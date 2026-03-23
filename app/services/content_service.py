import asyncio
from typing import List, Dict, Any, Optional
from sqlalchemy.ext.asyncio import AsyncSession
from datetime import date, datetime
from sqlalchemy import text, or_, and_
from uuid import UUID
from app.services.tmdb_client import TMDBClient
from app.services.mal_client import MALClient
from app.models.content import ContentResponse, HomeTrendingResponse
from app.core.logging import get_logger
from app.services.cache_service import cache, CacheKeys, CacheService

logger = get_logger('mambo.content')

class ContentService:
    def __init__(self, db: AsyncSession):
        self.db = db
        self.tmdb_client = TMDBClient()
        self.mal_client = MALClient()

    def _map_to_response(self, db_list: List[Dict[str, Any]]) -> List[ContentResponse]:
        results = []
        today = date.today()
        for d in db_list:
            rd = d.get('release_date')
            d['is_anticipated'] = bool(rd and rd > today)
            d['avg_star_rating'] = self._get_display_rating(d)
            d['cast'] = d.get('cast', [])
            try:
                results.append(ContentResponse.model_validate(d))
            except Exception as e: 
                logger.error(f"Validation error for content {d.get('id')}: {e}")
        return results

    async def get_home_trending(self, user_id: Optional[str] = None) -> HomeTrendingResponse:
        cache_key = CacheKeys.trending('all', date.today().isoformat())
        try:
            cached = await cache.get(cache_key)
            if cached:
                if cached.get('movies') or cached.get('series') or cached.get('anime'):
                    resp = HomeTrendingResponse.model_validate(cached)
                    if user_id:
                        all_items = resp.movies + resp.series + resp.anime
                        await self._populate_user_status(all_items, user_id)
                    return resp
        except Exception: pass

        async def _safe_fetch_movies() -> List[Dict[str, Any]]:
            db_fallback_sql = "SELECT * FROM content WHERE content_type = 'movie' ORDER BY external_rating DESC NULLS LAST, last_synced_at DESC NULLS LAST LIMIT 5"
            try:
                data = await self.tmdb_client.get_trending_movies(page=1)
                if data:
                    return await self._upsert_tmdb_content(data[:5])
            except Exception as e:
                logger.error('tmdb_trending_failed', extra={'content_type': 'movie', 'error': str(e)})
            res = await self.db.execute(text(db_fallback_sql))
            return [dict(r) for r in res.mappings()]

        async def _safe_fetch_series() -> List[Dict[str, Any]]:
            db_fallback_sql = "SELECT * FROM content WHERE content_type = 'series' ORDER BY external_rating DESC NULLS LAST, last_synced_at DESC NULLS LAST LIMIT 10"
            try:
                data = await self.tmdb_client.get_trending_series(page=1)
                if data:
                    return await self._upsert_tmdb_content(data[:5])
            except Exception as e:
                logger.error('tmdb_trending_failed', extra={'content_type': 'series', 'error': str(e)})
            res = await self.db.execute(text(db_fallback_sql))
            return [dict(r) for r in res.mappings()]

        async def _safe_fetch_anime() -> List[Dict[str, Any]]:
            db_fallback_sql = "SELECT * FROM content WHERE content_type = 'anime' ORDER BY external_rating DESC NULLS LAST, last_synced_at DESC NULLS LAST LIMIT 5"
            try:
                data = await self.mal_client.get_trending_anime()
                if data:
                    return await self._upsert_mal_content(data[:5])
            except Exception as e:
                logger.error('mal_trending_failed', extra={'error': str(e)})
            res = await self.db.execute(text(db_fallback_sql))
            return [dict(r) for r in res.mappings()]

        m_list = await _safe_fetch_movies()
        s_list = await _safe_fetch_series()
        a_list = await _safe_fetch_anime()

        all_items = m_list + s_list + a_list
        await self._populate_cast(all_items)
        if user_id:
            await self._populate_user_status(all_items, user_id)

        resp = HomeTrendingResponse(
            movies=self._map_to_response(m_list),
            series=self._map_to_response(s_list),
            anime=self._map_to_response(a_list)
        )
        try:
            await cache.set(cache_key, resp.model_dump(), ttl=CacheService.TTL_TRENDING)
        except Exception: pass
        return resp

    async def get_discover_content(self, mode: str, user_id: Optional[str] = None) -> Dict[str, Any]:
        content_type = {'movie': 'movie', 'series': 'series', 'anime': 'anime'}.get(mode, 'movie')
        GENRE_ACTION = (28 if content_type == 'movie' else 10759) if mode != 'anime' else 1
        GENRE_CRIME = 80 if mode != 'anime' else 7
        GENRE_COMEDY = 35 if mode != 'anime' else 4

        async def _db_fresh_count(genre_filter: Optional[str] = None, min_rating: Optional[float] = None, future_only: bool = False) -> int:
            sql = 'SELECT COUNT(*) FROM content WHERE content_type = :ct'
            params: Dict[str, Any] = {'ct': content_type}
            if genre_filter:
                sql += " AND genres @> ARRAY[:genre]::TEXT[]"
                params['genre'] = genre_filter
            if min_rating is not None:
                sql += " AND external_rating >= :r"
                params['r'] = min_rating
            if future_only:
                sql += " AND (release_date > CURRENT_DATE OR status = 'upcoming')"
            else:
                sql += " AND last_synced_at > NOW() - INTERVAL '24 hours'"
            res = await self.db.execute(text(sql), params)
            return res.scalar() or 0

        async def _db_query(limit=10, genre_filter=None, future_only=False, min_rating=None, sort='rating'):
            sql = 'SELECT * FROM content WHERE content_type = :ct'
            params: Dict[str, Any] = {'ct': content_type, 'limit': limit}
            if genre_filter:
                sql += " AND genres @> ARRAY[:genre]::TEXT[]"
                params['genre'] = genre_filter
            if future_only:
                sql += " AND (release_date > CURRENT_DATE OR status = 'upcoming')"
            if min_rating is not None:
                sql += ' AND external_rating >= :r'
                params['r'] = min_rating
            
            if future_only:
                sql += ' ORDER BY release_date ASC NULLS LAST, external_rating DESC NULLS LAST'
            elif sort == 'rating':
                sql += ' ORDER BY external_rating DESC NULLS LAST'
            else:
                sql += ' ORDER BY last_synced_at DESC NULLS LAST'
            sql += ' LIMIT :limit'
            res = await self.db.execute(text(sql), params)
            return [dict(row) for row in res.mappings()]

        CACHE_MIN = 8
        ANT_MIN = 5
        # Align thresholds: Use 7.2 here since the final query uses 7.2
        needs_popular = await _db_fresh_count() < CACHE_MIN
        needs_top     = await _db_fresh_count(min_rating=7.2) < CACHE_MIN
        needs_action  = await _db_fresh_count(genre_filter='Action') < CACHE_MIN
        needs_crime   = await _db_fresh_count(genre_filter='Crime') < CACHE_MIN
        needs_comedy  = await _db_fresh_count(genre_filter='Comedy') < CACHE_MIN
        needs_anti    = await _db_fresh_count(future_only=True) < ANT_MIN
        
        fetch_tasks = {}
        if needs_popular:
            if content_type == 'movie': fetch_tasks['popular'] = asyncio.gather(self.tmdb_client.get_popular_movies(1), self.tmdb_client.get_indian_movies(1), return_exceptions=True)
            elif content_type == 'series': fetch_tasks['popular'] = self.tmdb_client.get_popular_series(1)
            else: fetch_tasks['popular'] = self.mal_client.get_top_anime()
                
        if needs_top:
            if content_type == 'movie': fetch_tasks['top_rated'] = asyncio.gather(self.tmdb_client.get_top_rated_movies(1), self.tmdb_client.get_indian_movies(1), return_exceptions=True)
            elif content_type == 'series': fetch_tasks['top_rated'] = self.tmdb_client.get_top_rated_series(1)
            else: fetch_tasks['top_rated'] = self.mal_client.get_trending_anime()

        if needs_action:
            if content_type != 'anime':
                func = self.tmdb_client.get_movies_by_genre if content_type == 'movie' else self.tmdb_client.get_series_by_genre
                if content_type == 'movie':
                    fetch_tasks['action'] = asyncio.gather(func(GENRE_ACTION), self.tmdb_client.get_indian_movies_by_genre(GENRE_ACTION), return_exceptions=True)
                else:
                    fetch_tasks['action'] = func(GENRE_ACTION)
            else: fetch_tasks['action'] = self.mal_client.get_anime_by_genre(GENRE_ACTION)

        if needs_crime:
            if content_type != 'anime':
                func = self.tmdb_client.get_movies_by_genre if content_type == 'movie' else self.tmdb_client.get_series_by_genre
                if content_type == 'movie':
                    fetch_tasks['crime'] = asyncio.gather(func(GENRE_CRIME), self.tmdb_client.get_indian_movies_by_genre(GENRE_CRIME), return_exceptions=True)
                else:
                    fetch_tasks['crime'] = func(GENRE_CRIME)
            else: fetch_tasks['crime'] = self.mal_client.get_anime_by_genre(GENRE_CRIME)

        if needs_comedy:
            if content_type != 'anime':
                func = self.tmdb_client.get_movies_by_genre if content_type == 'movie' else self.tmdb_client.get_series_by_genre
                if content_type == 'movie':
                    fetch_tasks['comedy'] = asyncio.gather(func(GENRE_COMEDY), self.tmdb_client.get_indian_movies_by_genre(GENRE_COMEDY), return_exceptions=True)
                else:
                    fetch_tasks['comedy'] = func(GENRE_COMEDY)
            else: fetch_tasks['comedy'] = self.mal_client.get_anime_by_genre(GENRE_COMEDY)

        if needs_anti:
            if content_type == 'movie': fetch_tasks['anticipated'] = asyncio.gather(self.tmdb_client.get_upcoming_movies(1), self.tmdb_client.get_indian_upcoming_movies(1), return_exceptions=True)
            elif content_type == 'series': fetch_tasks['anticipated'] = self.tmdb_client.get_upcoming_series(1)
            else: fetch_tasks['anticipated'] = self.mal_client.get_upcoming_anime()

        if fetch_tasks:
            keys = list(fetch_tasks.keys())
            net_res = await asyncio.gather(*[fetch_tasks[k] for k in keys], return_exceptions=True)
            combined_tmdb = []
            combined_mal = []
            for i, val in enumerate(net_res):
                if isinstance(val, Exception): continue
                flat = []
                if isinstance(val, (list, tuple)) and any(isinstance(x, list) for x in val):
                    for sub in val:
                        if isinstance(sub, list): flat.extend(sub)
                elif isinstance(val, list): flat = val
                if mode == 'anime': combined_mal.extend(flat)
                else: combined_tmdb.extend(flat)
            if combined_tmdb: await self._upsert_tmdb_content(combined_tmdb, returning=False)
            if combined_mal: await self._upsert_mal_content(combined_mal, returning=False)

        pop_db = await _db_query(limit=10, sort='recent')
        top_db = await _db_query(limit=10, min_rating=7.2)
        act_db = await _db_query(limit=10, genre_filter='Action')
        cri_db = await _db_query(limit=10, genre_filter='Crime')
        com_db = await _db_query(limit=10, genre_filter='Comedy')
        ant_db = await _db_query(limit=5, future_only=True)

        resp = {
            "popular": self._map_to_response(pop_db),
            "top_rated": self._map_to_response(top_db),
            "action": self._map_to_response(act_db),
            "crime": self._map_to_response(cri_db),
            "comedy": self._map_to_response(com_db),
            "anticipated": self._map_to_response(ant_db),
        }
        
        if user_id:
            all_lists = [resp[k] for k in resp]
            flat_items = [item for sublist in all_lists for item in sublist]
            await self._populate_user_status(flat_items, user_id)
            
        return resp

    async def get_spotlight(self) -> List[Dict[str, Any]]:
        res = await self.db.execute(text('SELECT * FROM curated_content WHERE is_active = true AND category = \'spotlight\' ORDER BY priority DESC, created_at DESC LIMIT 5'))
        return [dict(row) for row in res.mappings()]

    async def search_content(self, query: str, limit: int = 20, content_type: str = "", user_id: Optional[str] = None) -> List[Dict[str, Any]]:
        # Normalize content_type: movies -> movie
        ct = {'movies': 'movie', 'movie': 'movie', 'series': 'series', 'anime': 'anime'}.get(content_type, content_type)
        
        # 1. Local Search (Broader wildcard)
        q = f"%{query}%"
        sql = 'SELECT * FROM content WHERE title ILIKE :q'
        params: Dict[str, Any] = {'q': q, 'limit': limit}
        if ct:
            sql += ' AND content_type = :ct'
            params['ct'] = ct
        sql += ' ORDER BY external_rating DESC NULLS LAST LIMIT :limit'
        
        result = await self.db.execute(text(sql), params)
        rows = [dict(row) for row in result.mappings()]
        
        # 2. Remote Fallback if no/low results
        if len(rows) < 5:
            remote_results = []
            try:
                if ct == 'movie':
                    remote_results = await self.tmdb_client.search_movies(query)
                elif ct == 'series':
                    remote_results = await self.tmdb_client.search_series(query)
                elif ct == 'anime':
                    remote_results = await self.mal_client.search_anime(query)
                elif not ct:
                    # Broad search across all if no type filtered
                    tm_m = await self.tmdb_client.search_movies(query)
                    tm_s = await self.tmdb_client.search_series(query)
                    ma_a = await self.mal_client.search_anime(query)
                    remote_results = tm_m + tm_s + ma_a
                
                if remote_results:
                    # Filter out what we already have to avoid redundant upserts
                    # (Though _upsert handles it, it's cleaner to just upsert everything and re-query)
                    t_res = [r for r in remote_results if r.get('content_type') != 'anime']
                    a_res = [r for r in remote_results if r.get('content_type') == 'anime']
                    
                    if t_res: await self._upsert_tmdb_content(t_res, returning=False)
                    if a_res: await self._upsert_mal_content(a_res, returning=False)
                    
                    # Re-run local search to get a clean unified list from DB
                    result = await self.db.execute(text(sql), params)
                    rows = [dict(row) for row in result.mappings()]
            except Exception as e:
                logger.error(f"Remote search fallback failed: {e}")

        await self._populate_cast(rows)
        today = date.today()
        for r in rows:
            rd = r.get('release_date')
            r['is_anticipated'] = bool(rd and rd > today)
            r['avg_star_rating'] = self._get_display_rating(r)
            
        if user_id:
            await self._populate_user_status(rows, user_id)
            
        return rows

    async def save_search_history(self, user_id: str, query: str, content_type: str = "") -> None:
        try:
            await self.db.execute(text('DELETE FROM search_history WHERE user_id = :uid AND query = :q'), {'uid': user_id, 'q': query})
            await self.db.execute(text('INSERT INTO search_history (user_id, query, content_type_filter, searched_at) VALUES (:uid, :q, :ct, now())'), {'uid': user_id, 'q': query, 'ct': content_type})
            await self.db.commit()
        except Exception: await self.db.rollback()

    async def get_search_history(self, user_id: str, limit: int = 10) -> List[Dict[str, Any]]:
        result = await self.db.execute(text('SELECT query, content_type_filter, searched_at FROM search_history WHERE user_id = :uid ORDER BY searched_at DESC LIMIT :limit'), {'uid': user_id, 'limit': limit})
        return [dict(row) for row in result.mappings()]

    async def clear_search_history(self, user_id: str, query: str = "") -> int:
        if query: res = await self.db.execute(text('DELETE FROM search_history WHERE user_id = :uid AND query = :q'), {'uid': user_id, 'q': query})
        else: res = await self.db.execute(text('DELETE FROM search_history WHERE user_id = :uid'), {'uid': user_id})
        await self.db.commit()
        return res.rowcount

    async def get_hot_reviews(self, limit: int = 10) -> List[Dict[str, Any]]:
        result = await self.db.execute(text('''
            SELECT r.id, r.star_rating, r.text_review, r.likes_count, r.created_at, c.id as content_id, c.title as content_title, c.poster_url, c.content_type, p.id as author_id, p.username, p.display_name, p.avatar_url, p.is_verified
            FROM reviews r JOIN content c ON c.id = r.content_id JOIN profiles p ON p.id = r.user_id
            WHERE r.is_deleted = false AND r.created_at > now() - interval '30 days' ORDER BY r.likes_count DESC, r.created_at DESC LIMIT :limit
        '''), {'limit': limit})
        return [dict(row) for row in result.mappings()]

    async def get_content_by_id(self, content_id: str, user_id: Optional[str] = None) -> Optional[ContentResponse]:
        cache_key = CacheKeys.content(content_id)
        if not user_id:
            cached = await cache.get(cache_key)
            if cached: return ContentResponse.model_validate(cached)
        
        try:
            # 1. Try local DB by UUID
            is_uuid = False
            try:
                UUID(content_id)
                is_uuid = True
            except ValueError: pass

            row = None
            if is_uuid:
                res = await self.db.execute(text("SELECT * FROM content WHERE id = CAST(:id AS UUID)"), {"id": content_id})
                row = res.mappings().one_or_none()
            
            # 2. Try external IDs if not found by UUID or if content_id is an external ID
            if not row:
                res = await self.db.execute(text("SELECT * FROM content WHERE tmdb_id = :id OR mal_id = :id"), {"id": content_id})
                row = res.mappings().one_or_none()

            # 3. If still not found, it's a 404
            if not row: return None
            
            d = dict(row)
            
            # 4. Check if sync is needed (stale or missing basic info)
            last_synced = d.get('last_synced_at')
            stale = not last_synced or (datetime.now() - last_synced.replace(tzinfo=None)).days > 7
            missing_info = not d.get('synopsis') or not d.get('genres')
            
            if (stale or missing_info) and d.get('tmdb_id'):
                try:
                    ext_data = await self.tmdb_client.get_movie_details(d['tmdb_id']) if d['content_type'] == 'movie' else await self.tmdb_client.get_series_details(d['tmdb_id'])
                    if ext_data:
                        updated = await self._upsert_tmdb_content([ext_data], returning=True)
                        if updated: d.update(updated[0])
                except Exception as sync_err:
                    logger.error(f"Sync failed for {content_id}: {sync_err}")

            d['is_anticipated'] = bool(d.get('release_date') and d.get('release_date') > date.today())
            d['avg_star_rating'] = self._get_display_rating(d)
            d['cast'] = d.get('cast', [])
            
            # Fetch user status if user_id is provided
            if user_id:
                status_res = await self.db.execute(text('''
                    SELECT is_watched, is_liked, is_dropped, is_interested, watch_count, rating
                    FROM user_content_status
                    WHERE user_id = CAST(:uid AS UUID) AND content_id = :cid
                '''), {'uid': user_id, 'cid': d['id']})
                status = status_res.mappings().one_or_none()
                if status:
                    d['is_watched'] = status.get('is_watched', False)
                    d['is_liked'] = status.get('is_liked', False)
                    d['is_dropped'] = status.get('is_dropped', False)
                    d['is_interested'] = status.get('is_interested', False)
                    d['watch_count'] = status.get('watch_count', 0)
                    d['user_rating'] = status.get('rating')

            try:
                resp = ContentResponse.model_validate(d)
            except Exception as ve:
                logger.error(f"Content validation failed for {content_id}: {ve}\nData: {d}")
                raise
            if not user_id:
                await cache.set(cache_key, resp.model_dump(), ttl=CacheService.TTL_CONTENT)
            return resp
        except Exception as e: 
            logger.error(f"Error getting content by id {content_id}: {e}")
            raise

    async def get_content_credits(self, content_id: str) -> List[Dict[str, Any]]:
        """Fetch cast and crew, using local DB if available, otherwise fallback to TMDB."""
        print(f"DEBUG: get_content_credits for {content_id}")
        try:
            # 1. Try local DB
            res = await self.db.execute(text('''
                SELECT p.id, p.name, p.profile_image_url as profile_url, cc.role, cc.character_name as character, cc.job, cc.department
                FROM content_credits cc
                JOIN persons p ON p.id = cc.person_id
                WHERE cc.content_id = :cid
                ORDER BY cc.display_order ASC
            '''), {'cid': content_id})
            rows = [dict(r) for r in res.mappings()]
            if rows: 
                print(f"DEBUG: Found {len(rows)} local credits")
                return rows

            # 2. If no local credits, fetch from TMDB
            print("DEBUG: Fetching from TMDB...")
            content_res = await self.db.execute(text("SELECT tmdb_id, content_type FROM content WHERE id = :id"), {"id": content_id})
            content = content_res.mappings().one_or_none()
            if not content or not content.get('tmdb_id'): 
                print(f"DEBUG: Content not found or no tmdb_id: {content}")
                return []

            tmdb_credits = await self.tmdb_client.get_credits(content['tmdb_id'], content['content_type'])
            
            # 3. Upsert persons and credits (Optimized: single transaction)
            all_credits = []
            
            # Cast - Limit to top 25
            tmdb_cast = tmdb_credits.get('cast', [])[:25]
            for i, p in enumerate(tmdb_cast):
                person_id = await self._upsert_person(p)
                if person_id:
                    await self._upsert_credit(content_id, person_id, 'cast', i, character=p.get('character'))
                    p['role'] = 'cast'
                    p['id'] = person_id
                    all_credits.append(p)
            
            # Crew - Filter by department and limit to 25
            tmdb_crew = tmdb_credits.get('crew', [])
            allowed_depts = {'Directing', 'Writing', 'Production'}
            crew_count = 0
            for i, p in enumerate(tmdb_crew):
                if p.get('department') in allowed_depts:
                    person_id = await self._upsert_person(p)
                    if person_id:
                        await self._upsert_credit(content_id, person_id, 'crew', i + 100, job=p.get('job'), department=p.get('department'))
                        p['role'] = 'crew'
                        p['id'] = person_id
                        all_credits.append(p)
                        crew_count += 1
                        if crew_count >= 25: break
            
            # Commit everything at once
            await self.db.commit()
            return all_credits
        except Exception as e:
            await self.db.rollback()
            logger.error(f"Error getting credits for {content_id}: {e}")
            return []

    async def get_similar_content(self, content_id: str) -> List[ContentResponse]:
        """Fetch similar content from TMDB and upsert basic info."""
        try:
            content_res = await self.db.execute(text("SELECT tmdb_id, content_type FROM content WHERE id = :id"), {"id": content_id})
            content = content_res.mappings().one_or_none()
            if not content or not content.get('tmdb_id'): return []

            similar_data = await self.tmdb_client.get_similar(content['tmdb_id'], content['content_type'])
            if not similar_data: return []

            # Limit to 10 items as requested
            upserted = await self._upsert_tmdb_content(similar_data[:10], is_permanent=False)
            return self._map_to_response(upserted)
        except Exception as e:
            await self.db.rollback()
            logger.error(f"Error getting similar for {content_id}: {e}")
            return []

    async def _upsert_person(self, p: Dict[str, Any]) -> Optional[str]:
        """Upsert a person into the persons table."""
        try:
            stmt = text('''
                INSERT INTO persons (tmdb_id, name, profile_image_url)
                VALUES (:tmdb_id, :name, :profile_image_url)
                ON CONFLICT (tmdb_id) DO UPDATE SET
                    name = EXCLUDED.name,
                    profile_image_url = EXCLUDED.profile_image_url,
                    updated_at = now()
                RETURNING id
            ''')
            res = await self.db.execute(stmt, {
                'tmdb_id': p.get('tmdb_id') or p.get('id'),
                'name': p.get('name'),
                'profile_image_url': p.get('profile_url') or p.get('profile_path') or p.get('image_url')
            })
            row = res.mappings().one_or_none()
            return str(row['id']) if row else None
        except Exception as e:
            await self.db.rollback()
            logger.error(f"Error upserting person {p.get('name')}: {e}")
            return None

    async def _upsert_credit(self, content_id: str, person_id: str, role: str, order: int, 
                             character: str = None, job: str = None, department: str = None):
        """Upsert a credit record."""
        try:
            stmt = text('''
                INSERT INTO content_credits (content_id, person_id, role, character_name, job, department, display_order)
                VALUES (:cid, :pid, :role, :char, :job, :dept, :order)
                ON CONFLICT (content_id, person_id, role) DO UPDATE SET
                    character_name = EXCLUDED.character_name,
                    job = EXCLUDED.job,
                    department = EXCLUDED.department,
                    display_order = EXCLUDED.display_order
            ''')
            await self.db.execute(stmt, {
                'cid': content_id,
                'pid': person_id,
                'role': role,
                'char': character,
                'job': job,
                'dept': department,
                'order': order
            })
            await self.db.commit()
        except Exception as e:
            await self.db.rollback()
            logger.error(f"Error upserting credit for {content_id}/{person_id}: {e}")


    async def _populate_cast(self, items: List[Dict[str, Any]]):
        if not items: return
        content_ids = []
        for item in items:
            cid = item.get('id')
            if cid:
                if isinstance(cid, UUID): content_ids.append(str(cid))
                else:
                    try: content_ids.append(str(UUID(str(cid))))
                    except ValueError: continue
        if not content_ids: return

        # Use proper UUID array comparison and correct column name (profile_image_url)
        result = await self.db.execute(text('''
            WITH cast_ranked AS (
                SELECT cc.content_id, p.id, p.name, p.profile_image_url, cc.character_name,
                       ROW_NUMBER() OVER(PARTITION BY cc.content_id ORDER BY cc.display_order ASC) as rank
                FROM content_credits cc JOIN persons p ON p.id = cc.person_id
                WHERE cc.content_id = ANY(CAST(:ids AS UUID[])) AND cc.role = 'cast'
            )
            SELECT content_id, id, name, profile_image_url, character_name as character FROM cast_ranked WHERE rank <= 4
        '''), {'ids': content_ids})
        
        cast_map = {}
        for row in result.mappings():
            cid = str(row['content_id'])
            if cid not in cast_map: cast_map[cid] = []
            cast_map[cid].append({
                'id': str(row['id']),
                'name': row['name'],
                'profile_url': row['profile_image_url'],
                'character': row['character'],
                'role': 'cast'
            })
        for item in items:
            cid_str = str(item.get('id', ''))
            item['cast'] = cast_map.get(cid_str, [])

    def _get_display_rating(self, d: dict) -> float:
        today = date.today()
        rd = d.get('release_date')
        if rd and rd > today: return 0.0
        ar = d.get('avg_star_rating')
        if ar is not None and ar > 0: return float(ar)
        er = d.get('external_rating')
        if er: return float(round(float(er) / 2.0, 1))
        return 0.0

    async def _populate_user_status(self, items: List[Any], user_id: str):
        if not items: return
        content_ids = []
        for it in items:
            cid = it.get('id') if isinstance(it, dict) else getattr(it, 'id', None)
            if cid: content_ids.append(str(cid))
        if not content_ids: return

        res = await self.db.execute(text('''
            SELECT content_id, is_watched, is_liked, is_dropped, is_interested, watch_count, rating
            FROM user_content_status
            WHERE user_id = :uid AND content_id = ANY(CAST(:ids AS UUID[]))
        '''), {'uid': user_id, 'ids': content_ids})
        
        status_map = {str(r['content_id']): r for r in res.mappings()}
        for it in items:
            cid = str(it.get('id', '')) if isinstance(it, dict) else str(getattr(it, 'id', ''))
            status = status_map.get(cid, {})
            
            def set_val(obj, key, val):
                if isinstance(obj, dict): obj[key] = val
                else: setattr(obj, key, val)

            set_val(it, 'is_watched', status.get('is_watched', False))
            set_val(it, 'is_liked', status.get('is_liked', False))
            set_val(it, 'is_dropped', status.get('is_dropped', False))
            set_val(it, 'is_interested', status.get('is_interested', False))
            set_val(it, 'watch_count', status.get('watch_count', 0))
            set_val(it, 'user_rating', status.get('rating'))

    async def _upsert_tmdb_content(self, items: List[Dict[str, Any]], returning: bool = True, is_permanent: bool = False) -> List[Dict[str, Any]]:
        if not items: return []
        seen = set()
        u_items = []
        for it in items:
            tid = it.get('tmdb_id')
            if tid and tid not in seen: seen.add(tid); u_items.append(it)
        stmt_text = '''
            INSERT INTO content (tmdb_id, content_type, title, original_title, synopsis, poster_url, backdrop_url, external_rating, external_rating_source, release_date, genres, is_permanent)
            VALUES (:tmdb_id, :content_type, :title, :original_title, :synopsis, :poster_url, :backdrop_url, :external_rating, :external_rating_source, :release_date, :genres, :is_permanent)
            ON CONFLICT (tmdb_id) DO UPDATE SET 
                title = EXCLUDED.title, 
                synopsis = EXCLUDED.synopsis, 
                poster_url = EXCLUDED.poster_url, 
                backdrop_url = EXCLUDED.backdrop_url, 
                external_rating = EXCLUDED.external_rating, 
                genres = EXCLUDED.genres, 
                last_synced_at = now(),
                is_permanent = content.is_permanent OR EXCLUDED.is_permanent
        '''
        if returning: stmt_text += ' RETURNING *'
        stmt = text(stmt_text)
        params = []
        for it in u_items:
            rd = it.get('release_date')
            if rd and isinstance(rd, str):
                try: rd = datetime.strptime(rd[:10], '%Y-%m-%d').date()
                except ValueError: rd = None
            params.append({ 
                'tmdb_id': it.get('tmdb_id'), 
                'content_type': it.get('content_type'), 
                'title': it.get('title'), 
                'original_title': it.get('original_title'), 
                'synopsis': it.get('synopsis'), 
                'poster_url': it.get('poster_url'), 
                'backdrop_url': it.get('backdrop_url'), 
                'external_rating': it.get('external_rating'), 
                'external_rating_source': it.get('external_rating_source'), 
                'release_date': rd, 
                'genres': it.get('genres', []),
                'is_permanent': is_permanent
            })
        try:
            results = []
            if returning:
                for p in params:
                    r = await self.db.execute(stmt, p)
                    results.append(dict(r.mappings().one()))
            else:
                if params: await self.db.execute(stmt, params)
            await self.db.commit()
            return results
        except Exception as e:
            await self.db.rollback(); logger.error(f"tmdb_upsert_failed: {e}"); return []

    async def _upsert_mal_content(self, items: List[Dict[str, Any]], returning: bool = True, is_permanent: bool = False) -> List[Dict[str, Any]]:
        if not items: return []
        seen = set()
        u_items = []
        for it in items:
            mid = it.get('mal_id')
            if mid and mid not in seen: seen.add(mid); u_items.append(it)
        stmt_text = '''
            INSERT INTO content (mal_id, content_type, title, synopsis, poster_url, external_rating, external_rating_source, total_episodes, release_date, status, anime_studio, genres, is_permanent)
            VALUES (:mal_id, :content_type, :title, :synopsis, :poster_url, :external_rating, :external_rating_source, :total_episodes, :release_date, :status, :anime_studio, :genres, :is_permanent)
            ON CONFLICT (mal_id) DO UPDATE SET 
                title = EXCLUDED.title, 
                synopsis = EXCLUDED.synopsis, 
                poster_url = EXCLUDED.poster_url, 
                external_rating = EXCLUDED.external_rating, 
                genres = EXCLUDED.genres, 
                last_synced_at = now(),
                is_permanent = content.is_permanent OR EXCLUDED.is_permanent
        '''
        if returning: stmt_text += ' RETURNING *'
        stmt = text(stmt_text)
        params = []
        for it in u_items:
            rd = it.get('release_date')
            if rd and isinstance(rd, str):
                try: rd = datetime.strptime(rd[:10], '%Y-%m-%d').date()
                except ValueError: rd = None
            params.append({ 
                'mal_id': it.get('mal_id'), 
                'content_type': it.get('content_type'), 
                'title': it.get('title'), 
                'synopsis': it.get('synopsis'), 
                'poster_url': it.get('poster_url'), 
                'external_rating': it.get('external_rating'), 
                'external_rating_source': it.get('external_rating_source'), 
                'total_episodes': it.get('total_episodes'), 
                'release_date': rd, 
                'status': it.get('status'), 
                'anime_studio': it.get('anime_studio'), 
                'genres': it.get('genres', []),
                'is_permanent': is_permanent
            })
        try:
            results = []
            if returning:
                for p in params:
                    r = await self.db.execute(stmt, p)
                    results.append(dict(r.mappings().one()))
            else:
                if params: await self.db.execute(stmt, params)
            await self.db.commit()
            return results
        except Exception as e:
            await self.db.rollback(); logger.error(f"mal_upsert_failed: {e}"); return []

    async def cleanup_stale_content(self, hours: int = 24) -> int:
        """
        Deletes content that is not permanent, was last synced more than X hours ago,
        and has no user activity (reviews, posts, collections, status, etc.)
        """
        logger.info(f"Starting stale content cleanup (older than {hours} hours)...")
        try:
            # We use a single query with NOT EXISTS for efficiency
            query = text(f"""
                DELETE FROM content
                WHERE is_permanent = false
                AND last_synced_at < now() - interval '{{hours}} hours'
                AND NOT EXISTS (SELECT 1 FROM reviews WHERE content_id = content.id)
                AND NOT EXISTS (SELECT 1 FROM posts WHERE content_id = content.id)
                AND NOT EXISTS (SELECT 1 FROM collection_items WHERE content_id = content.id)
                AND NOT EXISTS (
                    SELECT 1 FROM user_content_status 
                    WHERE content_id = content.id 
                    AND (is_watched = true OR is_liked = true OR is_dropped = true OR is_interested = true)
                )
                AND NOT EXISTS (SELECT 1 FROM watch_history WHERE content_id = content.id)
                AND NOT EXISTS (SELECT 1 FROM shares WHERE content_id = content.id)
                AND NOT EXISTS (SELECT 1 FROM trending_content WHERE content_id = content.id)
                AND NOT EXISTS (SELECT 1 FROM recommendations WHERE content_id = content.id)
                AND NOT EXISTS (SELECT 1 FROM favourites WHERE content_id = content.id)
            """.replace("{hours}", str(hours)))
            
            result = await self.db.execute(query)
            await self.db.commit()
            
            deleted_count = result.rowcount if result.rowcount is not None else 0
            logger.info(f"Cleanup finished. Deleted {deleted_count} stale items.")
            return deleted_count
        except Exception as e:
            logger.error(f"Cleanup failed: {e}")
            await self.db.rollback()
            return 0

    async def cleanup_old_activities(self, days: int = 7) -> int:
        """Deletes activity logs older than specified days."""
        logger.info(f"Starting activity log cleanup (older than {days} days)...")
        try:
            query = text(f"DELETE FROM activity_log WHERE created_at < now() - interval '{days} days'")
            result = await self.db.execute(query)
            await self.db.commit()
            
            deleted_count = result.rowcount if result.rowcount is not None else 0
            logger.info(f"Activity cleanup finished. Deleted {deleted_count} logs.")
            return deleted_count
        except Exception as e:
            logger.error(f"Activity cleanup failed: {e}")
            await self.db.rollback()
            return 0