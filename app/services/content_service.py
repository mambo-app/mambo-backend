import asyncio
import json
from typing import List, Dict, Any, Optional
from sqlalchemy.ext.asyncio import AsyncSession
from datetime import date, datetime
from sqlalchemy import text, or_, and_
from uuid import UUID
from app.services.tmdb_client import TMDBClient
from app.services.mal_client import MALClient
from app.models.content import ContentResponse, HomeTrendingResponse, ContentStatus, SeasonStatusResponse
from app.core.logger import get_logger
from app.services.cache_service import cache, CacheKeys, CacheService

logger = get_logger('mambo.content')

class ContentService:
    def __init__(self, db: AsyncSession):
        self.db = db
        self.tmdb_client = TMDBClient()
        self.mal_client = MALClient()

    def _calculate_distribution(self, rating: float) -> List[float]:
        """
        Simulate a realistic rating distribution based on the average star rating (0-5 scale).
        Using a simple normal-ish distribution model.
        """
        # Ensure rating is 1-5 for calculation
        r = max(1.0, min(5.0, rating))
        
        # We want to create 5 weights that sum to 1.0, with a peak near 'r'
        weights = []
        total = 0.0
        for i in range(1, 6):
            # Distance from target rating
            dist = abs(i - r)
            # Use an exponential decay based on distance
            # Closer to rating = higher weight
            w = 1.0 / (1.0 + (dist * 2.0)**2)
            weights.append(w)
            total += w
            
        # Normalize to 1.0
        return [round(w / total, 2) for w in weights]

    def _map_to_response(self, db_list: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        results = []
        today = date.today()
        for d in db_list:
            try:
                # 1. Prepare computed fields
                rd = d.get('release_date')
                d['is_anticipated'] = bool(rd and rd > today)
                d['avg_star_rating'] = self._get_display_rating(d)
                # Ensure no conflict between DB status (release status) and user tracking status
                if 'status' in d:
                    d['release_status'] = d.pop('status')
                
                # 2. Validate with Pydantic for schema correctness
                model = ContentResponse.model_validate(d)
                
                # 3. Convert back to dict and ensure ID is a string for the App
                safe_dict = model.model_dump()
                safe_dict['id'] = str(safe_dict['id'])
                safe_dict['description'] = safe_dict.get('synopsis')
                
                # 4. Distribution and Counts
                safe_dict['vote_count'] = d.get('vote_count') or 0
                safe_dict['rating_distribution'] = self._calculate_distribution(d['avg_star_rating'])

                # NEW: Ensure series metadata is passed to rows
                if d.get('content_type') in ['series', 'anime', 'tv']:
                    safe_dict['total_seasons'] = d.get('total_seasons') or 1
                    safe_dict['total_episodes'] = d.get('total_episodes') or 0

                if safe_dict.get('release_date'):
                    safe_dict['release_date'] = safe_dict['release_date'].isoformat()
                
                results.append(safe_dict)
            except Exception as e:
                logger.warning(f"Map failed for {d.get('id')}: {e}")
                # Ultimate fallback: Raw dict with string ID
                if 'id' in d: d['id'] = str(d['id'])
                results.append(d)
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

    async def get_continue_watching(self, user_id: str) -> List[Dict[str, Any]]:
        query = text("""
            SELECT c.*, s.status as user_status, s.progress_episodes, s.last_activity_at
            FROM content c
            JOIN user_content_status s ON c.id = s.content_id
            WHERE s.user_id = :uid AND s.status = 'watching'
            ORDER BY s.updated_at DESC
            LIMIT 10
        """)
        res = await self.db.execute(query, {'uid': UUID(user_id)})
        rows = [dict(r) for r in res.mappings()]
        return self._map_to_response(rows)

    async def get_discover_content(self, mode: str, user_id: Optional[str] = None) -> Dict[str, Any]:
        from datetime import date
        from app.services.cache_service import CacheKeys, cache, CacheService
        
        # 1. Query Cache (Bust with v2 prefix)
        cache_key = f"v2:{CacheKeys.discover(mode, user_id if user_id else 'guest', date.today().isoformat())}"
        cached = await cache.get(cache_key)
        if cached:
            # Hydrate user-specific status even on cache hits
            if user_id:
                all_items = []
                for k in ["popular", "top_rated", "anticipated"]:
                    if k in cached: all_items.extend(cached[k])
                for row in cached.get("genre_rows", []):
                    all_items.extend(row.get("items", []))
                
                if all_items:
                    await self._populate_user_status(all_items, user_id)
            return cached

        content_type = {'movie': 'movie', 'series': 'series', 'anime': 'anime'}.get(mode, 'movie')
        
        # 1. Get User Favorite Genres
        target_genres = ['Action', 'Crime', 'Comedy'] # Default
        if user_id:
            from app.repositories.user_repo import UserRepository
            repo = UserRepository(self.db)
            user_favs = await repo.get_favorite_genres(user_id)
            if user_favs:
                target_genres = user_favs[:3]

        # Genre mapping for TMDB/MAL
        # (Simplified mapping, ideally this would be a more robust lookup table)
        is_movie = (content_type == 'movie')
        genre_ids = {
            'Action': (28 if is_movie else 10759) if mode != 'anime' else 1,
            'Crime': 80 if mode != 'anime' else 7,
            'Comedy': 35 if mode != 'anime' else 4,
            'Drama': 18 if mode != 'anime' else 8,
            'Sci-Fi': (878 if is_movie else 10765) if mode != 'anime' else 24,
            'Horror': (27 if is_movie else 9648) if mode != 'anime' else 14,
            'Romance': 10749 if mode != 'anime' else 22,
            'Thriller': (53 if is_movie else 9648) if mode != 'anime' else 41,
            'Animation': 16 if mode != 'anime' else 1,
            'Fantasy': (14 if is_movie else 10765) if mode != 'anime' else 10,
            'Documentary': 99 if mode != 'anime' else 1,
            'Adventure': (12 if is_movie else 10759) if mode != 'anime' else 2,
            'Mystery': 9648 if mode != 'anime' else 7,
            'Family': 10751 if mode != 'anime' else 1,
        }

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

        CACHE_MIN = 100
        ANT_MIN = 50
        
        # 2. Check and Prepare Fetch Tasks
        fetch_tasks = {}
        
        # Standard rows
        if await _db_fresh_count() < CACHE_MIN:
            if content_type == 'movie': 
                fetch_tasks['popular'] = asyncio.gather(
                    self.tmdb_client.get_popular_movies(1), 
                    self.tmdb_client.get_indian_movies(1),
                    self.tmdb_client.get_indian_now_playing(1),
                    return_exceptions=True
                )
            elif content_type == 'series': fetch_tasks['popular'] = self.tmdb_client.get_popular_series(1)
            else: fetch_tasks['popular'] = self.mal_client.get_top_anime()
                
        if await _db_fresh_count(min_rating=7.2) < CACHE_MIN:
            if content_type == 'movie': fetch_tasks['top_rated'] = asyncio.gather(self.tmdb_client.get_top_rated_movies(1), self.tmdb_client.get_indian_movies(1), return_exceptions=True)
            elif content_type == 'series': fetch_tasks['top_rated'] = self.tmdb_client.get_top_rated_series(1)
            else: fetch_tasks['top_rated'] = self.mal_client.get_trending_anime()
 
        if await _db_fresh_count(future_only=True) < ANT_MIN:
            if content_type == 'movie': fetch_tasks['anticipated'] = asyncio.gather(self.tmdb_client.get_upcoming_movies(1), self.tmdb_client.get_indian_upcoming_movies(1), return_exceptions=True)
            elif content_type == 'series': fetch_tasks['anticipated'] = self.tmdb_client.get_upcoming_series(1)
            else: fetch_tasks['anticipated'] = self.mal_client.get_upcoming_anime()

        # Genre-specific rows
        for genre in target_genres:
            if await _db_fresh_count(genre_filter=genre) < CACHE_MIN:
                genre_id = genre_ids.get(genre, 1) # Fallback to 1 (General/Action)
                if content_type != 'anime':
                    func = self.tmdb_client.get_movies_by_genre if content_type == 'movie' else self.tmdb_client.get_series_by_genre
                    if content_type == 'movie':
                        fetch_tasks[f'genre_{genre}'] = asyncio.gather(func(genre_id), self.tmdb_client.get_indian_movies_by_genre(genre_id), return_exceptions=True)
                    else:
                        fetch_tasks[f'genre_{genre}'] = func(genre_id)
                else: 
                    fetch_tasks[f'genre_{genre}'] = self.mal_client.get_anime_by_genre(genre_id)
 
        # 3. BACKGROUND FETCH (if stale or empty)
        async def _background_fetch():
            from app.core.database import AsyncSessionLocal
            async with AsyncSessionLocal() as bg_db:
                # We need a new service instance with the new session
                bg_service = ContentService(bg_db)
                try:
                    logger.info(f"Discovery: BG Fetching {len(fetch_tasks)} tasks for {mode}...")
                    keys = list(fetch_tasks.keys())
                    # Use longer timeout for background work
                    net_res = await asyncio.wait_for(
                        asyncio.gather(*[fetch_tasks[k] for k in keys], return_exceptions=True),
                        timeout=15.0 
                    )
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
                    
                    if combined_tmdb: await bg_service._upsert_tmdb_content(combined_tmdb, returning=False, is_permanent=False)
                    if combined_mal: await bg_service._upsert_mal_content(combined_mal, returning=False, is_permanent=False)
                except Exception as e:
                    logger.error(f"Discovery BG fetch error: {e}")

        # If data is completely missing, we MUST wait for the first fetch to avoid returning empty
        pop_db = await _db_query(limit=50, sort='recent')
        if not pop_db and fetch_tasks:
            # First time load: wait for a sync fetch (3s)
            logger.info("Discovery: DB empty. Performing sync fetch...")
            # (Logic for sync fetch here if needed, but for now we just fire BG and proceed)
            # Actually, better to just call the background fetch logic synchronously once
            await _background_fetch()
            pop_db = await _db_query(limit=50, sort='recent')
        elif fetch_tasks:
            # Content exists but is stale: spawn background refresh
            logger.info("Discovery: Data stale. Spawning BG refresh.")
            asyncio.create_task(_background_fetch())

        # 4. Final DB Queries for Response
        # Re-fetch everything to ensure we have the latest (either from DB or the sync fetch above)
        top_db = await _db_query(limit=20, min_rating=7.2)
        ant_db = await _db_query(limit=50, future_only=True)

        if content_type == 'movie':
            # Priority: Recent + Popular mixing
            hollywood = [item for item in pop_db if item.get('original_language') == 'en'][:15]
            bollywood = [item for item in pop_db if item.get('original_language') in ['hi', 'ta', 'te', 'ml', 'kn']][:15]
            
            mixed = []
            max_len = max(len(hollywood), len(bollywood))
            for i in range(max_len):
                if i < len(hollywood): mixed.append(hollywood[i])
                if i < len(bollywood): mixed.append(bollywood[i])
                
            if len(mixed) < 12:
                for item in pop_db:
                    if item not in mixed:
                        mixed.append(item); 
                        if len(mixed) >= 12: break
            pop_db = mixed[:24]
        else:
            pop_db = pop_db[:12]

        genre_rows = []
        for genre in target_genres:
            # For Series, we might need to search by the mapped names
            search_genres = [genre]
            if content_type == 'series':
                if genre == 'Horror': search_genres.append('Mystery')
                elif genre == 'Thriller': search_genres.append('Mystery')
                elif genre == 'Sci-Fi': search_genres.append('Sci-Fi')
                elif genre == 'Fantasy': search_genres.append('Sci-Fi')
                elif genre == 'Action': search_genres.append('Action')
                elif genre == 'Adventure': search_genres.append('Action')

            genre_items = []
            for sg in search_genres:
                found = await _db_query(limit=10, genre_filter=sg)
                genre_items.extend([i for i in found if i not in genre_items])
                if len(genre_items) >= 10: break
            
            genre_rows.append({
                "genre": genre,
                "items": self._map_to_response(genre_items[:10])
            })

        resp = {
            "popular": self._map_to_response(pop_db),
            "top_rated": self._map_to_response(top_db),
            "anticipated": self._map_to_response(ant_db),
            "genre_rows": genre_rows
        }
        
        if user_id:
            all_lists = [resp[k] for k in ["popular", "top_rated", "anticipated"]]
            # Also add items from genre rows correctly
            flat_items = [item for sublist in all_lists for item in (sublist if isinstance(sublist, list) else [])]
            for row in genre_rows:
                flat_items.extend(row["items"])
                
            await self._populate_user_status(flat_items, user_id)
            
        # 5. Cache the Result
        try:
            await cache.set(cache_key, resp, ttl=CacheService.TTL_DISCOVER)
        except Exception:
            pass

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

    async def search_people(self, query: str, limit: int = 20) -> List[Dict[str, Any]]:
        """Search for people across local DB and TMDB fallback, filtered by Acting/Directing."""
        # 1. Local Search with Filtering
        q = f"%{query}%"
        res = await self.db.execute(text('''
            SELECT id, tmdb_id, name, profile_image_url as profile_url, known_for_department 
            FROM persons 
            WHERE name ILIKE :q 
            AND known_for_department IN ('Acting', 'Directing')
            ORDER BY last_synced_at DESC
            LIMIT :limit
        '''), {'q': q, 'limit': limit})
        rows = [dict(r) for r in res.mappings()]
        
        # 2. TMDB Fallback if few results
        if len(rows) < 5:
            tmdb_results = await self.tmdb_client.search_people(query)
            if tmdb_results:
                # Filter TMDB results before upsert
                filtered_tmdb = [
                    p for p in tmdb_results 
                    if p.get('known_for_department') in ('Acting', 'Directing')
                ]
                
                # Upsert findings to local DB
                for p in filtered_tmdb:
                    await self._upsert_person(p)
                await self.db.commit()
                
                # Re-query local for unified response
                res = await self.db.execute(text('''
                    SELECT id, tmdb_id, name, profile_image_url as profile_url, known_for_department 
                    FROM persons 
                    WHERE name ILIKE :q 
                    AND known_for_department IN ('Acting', 'Directing')
                    ORDER BY last_synced_at DESC
                    LIMIT :limit
                '''), {'q': q, 'limit': limit})
                rows = [dict(r) for r in res.mappings()]
        
        return rows

    async def get_person_profile(self, person_id: str) -> Dict[str, Any]:
        """Fetch full person details and filmography."""
        # Check if local ID or TMDB ID
        is_uuid = False
        try:
            from uuid import UUID
            UUID(person_id)
            is_uuid = True
        except (ValueError, ImportError): pass
        
        tmdb_id = None
        if is_uuid:
            res = await self.db.execute(text("SELECT tmdb_id FROM persons WHERE id::text = :id"), {"id": person_id})
            row = res.mappings().one_or_none()
            tmdb_id = row['tmdb_id'] if row else None
            
            # If not found in DB but is a valid integer string, fallback to treating as TMDB ID
            if not tmdb_id:
                try: tmdb_id = int(person_id)
                except ValueError: pass
        else:
            try:
                tmdb_id = int(person_id)
            except ValueError: pass
            
        if not tmdb_id:
            logger.error(f"DEBUG_PROFILE_FAIL: Could not resolve tmdb_id for {person_id}")
            logger.warning(f"Could not resolve TMDB ID for person_id: {person_id}")
            return {}
            
        logger.info(f"DEBUG_PROFILE_RESOLVED: person_id={person_id} -> tmdb_id={tmdb_id}")
        # Fetch from TMDB (always fresh for detail screen)
        details = await self.tmdb_client.get_person_details(tmdb_id)
        if not details: 
            logger.error(f"DEBUG_PROFILE_FAIL: TMDB returned empty for tmdb_id={tmdb_id}")
            logger.error(f"Failed to fetch TMDB details for person: {tmdb_id}")
            return {}
        
        credits = await self.tmdb_client.get_person_combined_credits(tmdb_id)
        
        # Ensure credits have 'id' for the UI routing if missing
        for c in credits:
            if 'id' not in c and 'tmdb_id' in c:
                c['id'] = str(c['tmdb_id'])
        
        # Separate Top Movies (popularity high) and Full Filmography
        details['top_credits'] = credits[:10]
        details['filmography'] = credits
        
        return details

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
            if not row and content_id.isdigit():
                res = await self.db.execute(text("SELECT * FROM content WHERE tmdb_id = :id OR mal_id = :id"), {"id": int(content_id)})
                row = res.mappings().one_or_none()

            # 3. If still not found, try live TMDB fetch if content_id is numeric
            if not row and content_id.isdigit():
                try:
                    # We don't know if it's a movie or series yet, so we try discovery or specific lookup.
                    # Usually, filmographies provide media_type, but here we only have the ID.
                    # We'll try a generic search or assume it's a movie first, then series.
                    logger.info(f"Content {content_id} not in DB. Attempting auto-import from TMDB.")
                    
                    # Try movie first
                    ext_data = await self.tmdb_client.get_movie_details(int(content_id))
                    content_type = 'movie'
                    
                    if not ext_data or not ext_data.get('title'):
                        # Try series
                        ext_data = await self.tmdb_client.get_series_details(int(content_id))
                        content_type = 'series'
                    
                    if ext_data and (ext_data.get('title') or ext_data.get('name')):
                        # Map and Upsert
                        upserted = await self._upsert_tmdb_content([ext_data], returning=True)
                        if upserted:
                            row = upserted[0]
                except Exception as e:
                    logger.error(f"Auto-import failed for TMDB ID {content_id}: {e}")

            if not row: return None
            
            d = dict(row)
            
            # 4. Check if sync is needed (stale or missing basic info)
            last_synced = d.get('last_synced_at')
            stale = not last_synced or (datetime.now() - last_synced.replace(tzinfo=None)).days > 7
            missing_info = not d.get('synopsis') or not d.get('genres')
            if d.get('content_type') in ['series', 'anime', 'tv']:
                if d.get('total_episodes') is None or d.get('total_seasons') is None:
                    missing_info = True
            
            if (stale or missing_info) and d.get('tmdb_id'):
                try:
                    ext_data = await self.tmdb_client.get_movie_details(d['tmdb_id']) if d['content_type'] == 'movie' else await self.tmdb_client.get_series_details(d['tmdb_id'])
                    if ext_data:
                        updated = await self._upsert_tmdb_content([ext_data], returning=True)
                        if updated: d.update(updated[0])
                except Exception as sync_err:
                    logger.error(f"Sync failed for {content_id}: {sync_err}")

            # Ensure description alias is present for the App
            d['description'] = d.get('synopsis')
            
            d['is_anticipated'] = bool(d.get('release_date') and d.get('release_date') > date.today())
            d['avg_star_rating'] = self._get_display_rating(d)
            d['cast'] = d.get('cast', [])
            
            # Dynamic Rating Stats
            d['vote_count'] = d.get('vote_count') or 0
            d['rating_distribution'] = self._calculate_distribution(d['avg_star_rating'])

            # Rename database 'status' (release status) to 'release_status' to avoid conflict with user tracking status
            if 'status' in d:
                d['release_status'] = d.pop('status')
            
            # Map to response model
            resp = ContentResponse.model_validate(d)
            
            # Add Series-specific metadata if available
            if d.get('content_type') in ['series', 'anime', 'tv']:
                resp.total_seasons = d.get('total_seasons') or d.get('number_of_seasons') or 1
                resp.total_episodes = d.get('total_episodes') or d.get('number_of_episodes') or 0
                # release_status is already set from pop above, but ensure it's not None
                if not resp.release_status:
                    resp.release_status = 'unknown'

            # 5. Fetch user status if user_id is provided
            if user_id:
                try:
                    # 5. Fetch main user status
                    uid_obj = user_id if isinstance(user_id, UUID) else UUID(user_id)
                    status_res = await self.db.execute(text('''
                        SELECT 
                            s.is_watched, s.is_liked, s.is_dropped, s.is_interested, s.watch_count, s.rating,
                            s.status, s.progress_episodes, s.rewatch_count, s.last_activity_at,
                            s.last_watched_season, s.last_watched_episode,
                            EXISTS (SELECT 1 FROM calendar_alerts ca WHERE ca.user_id = :uid AND ca.content_id = :cid) as is_notified
                        FROM user_content_status s
                        WHERE s.user_id = :uid AND s.content_id = :cid
                    '''), {'uid': uid_obj, 'cid': d['id']})
                    row = status_res.mappings().one_or_none()
                    
                    if row:
                        resp.is_watched = row['is_watched']
                        resp.is_liked = row['is_liked']
                        resp.is_dropped = row['is_dropped']
                        resp.is_interested = row['is_interested']
                        resp.is_notified = row['is_notified']
                        resp.watch_count = row['watch_count']
                        resp.user_rating = float(row['rating']) if row['rating'] is not None else None
                        
                        raw_status = row['status'] or 'none'
                        resp.status = ContentStatus.COMPLETED if (raw_status == 'none' and row['is_watched']) else ContentStatus(raw_status)
                        resp.progress_episodes = row['progress_episodes'] or 0
                        resp.last_watched_season = row['last_watched_season'] or 0
                        resp.last_watched_episode = row['last_watched_episode'] or 0
                        resp.rewatch_count = row['rewatch_count'] or 0
                        resp.last_activity_at = row['last_activity_at']
                    else:
                        # Check for notification even if no status row
                        ca_res = await self.db.execute(text(
                            "SELECT EXISTS (SELECT 1 FROM calendar_alerts WHERE user_id = :uid AND content_id = :cid)"
                        ), {'uid': uid_obj, 'cid': d['id']})
                        resp.is_notified = ca_res.scalar() or False

                    # 6. Fetch Per-Season Status
                    logger.info(f"DEBUG_SYNC: uid={user_id}, cid={d['id']}")

                    # Fetch rows normally to be safe
                    cid_obj = d['id'] if isinstance(d['id'], UUID) else UUID(d['id'])
                    
                    raw_rows_res = await self.db.execute(text("""
                        SELECT s.sn as season_number, 
                               COALESCE(uss.status, 'none') as status, 
                               COALESCE(uss.progress_episodes, 0) as progress_episodes,
                               COALESCE(
                                   uss.total_episodes,
                                   (
                                       SELECT (elem->>'episode_count')::int 
                                       FROM jsonb_array_elements(c.seasons) elem 
                                       WHERE (elem->>'season_number')::int = s.sn 
                                       LIMIT 1
                                   ),
                                   CASE 
                                       WHEN :total_seasons > 0 THEN ceil(CAST(:total_episodes AS FLOAT) / :total_seasons)::int
                                       ELSE 0 
                                   END
                               ) as total_episodes,
                               uss.updated_at
                        FROM (SELECT generate_series(1, COALESCE(:total_seasons, 1)) as sn) s
                        CROSS JOIN content c
                        LEFT JOIN user_season_status uss ON uss.content_id = c.id AND uss.user_id = :uid AND uss.season_number = s.sn
                        WHERE c.id = :cid
                        ORDER BY s.sn ASC
                    """), {
                        'uid': str(uid_obj), 
                        'cid': str(cid_obj),
                        'total_seasons': resp.total_seasons or 1,
                        'total_episodes': resp.total_episodes or 0
                    })
                    
                    rows = raw_rows_res.fetchall()
                    logger.info(f"DEBUG_SYNC: Found {len(rows)} status rows for {cid_obj}")
                    
                    season_statuses = []
                    for row in rows:
                        season_statuses.append(SeasonStatusResponse(
                            season_number=row[0],
                            status=row[1],
                            progress_episodes=row[2],
                            total_episodes=row[3],
                            updated_at=row[4]
                        ))
                    
                    resp.season_statuses = season_statuses

                except Exception as status_err:
                    logger.error(f"Error fetching user status for {content_id}: {status_err}")
                    import traceback
                    logger.error(traceback.format_exc())

            return resp
        except Exception as e:
            logger.error(f"Error getting content by id {content_id}: {e}")
            raise

    async def get_content_credits(self, content_id: str) -> List[Dict[str, Any]]:
        """Fetch cast and crew, using local DB if available, otherwise fallback to TMDB."""
        try:
            # 1. Resolve content first (ensure we have a UUID and TMDB ID)
            content = await self.get_content_by_id(content_id)
            if not content: return []
            
            cid_uuid = content.id
            tmdb_id = content.tmdb_id
            content_type = content.content_type

            # 2. Try local DB credits by UUID
            res = await self.db.execute(text('''
                SELECT p.id, p.name, p.profile_image_url as profile_url, cc.role, cc.character_name as character, cc.job, cc.department
                FROM content_credits cc
                JOIN persons p ON p.id = cc.person_id
                WHERE cc.content_id = :cid
                ORDER BY cc.display_order ASC
            '''), {'cid': cid_uuid})
            rows = [dict(r) for r in res.mappings()]
            if rows: return rows

            # 3. If no local credits, fetch from TMDB
            if not tmdb_id: return []

            tmdb_credits = await self.tmdb_client.get_credits(tmdb_id, content_type)
            
            # 4. Upsert persons and credits
            all_credits = []
            cid_str = str(cid_uuid)
            
            # Cast - Limit to top 25
            tmdb_cast = tmdb_credits.get('cast', [])[:25]
            for i, p in enumerate(tmdb_cast):
                try:
                    person_id = await self._upsert_person(p)
                    if person_id:
                        await self._upsert_credit(cid_str, person_id, 'cast', i, character=p.get('character'))
                        p['role'] = 'cast'
                        p['id'] = person_id
                        all_credits.append(p)
                except Exception as p_err:
                    logger.warning(f"Skipping cast member {p.get('name')} for {content_id}: {p_err}")
            
            # Crew - Filter by department and limit to 25
            tmdb_crew = tmdb_credits.get('crew', [])
            allowed_depts = {'Directing', 'Writing', 'Production'}
            crew_count = 0
            for i, p in enumerate(tmdb_crew):
                try:
                    if p.get('department') in allowed_depts:
                        person_id = await self._upsert_person(p)
                        if person_id:
                            await self._upsert_credit(cid_str, person_id, 'crew', i + 100, job=p.get('job'), department=p.get('department'))
                            p['role'] = 'crew'
                            p['id'] = person_id
                            all_credits.append(p)
                            crew_count += 1
                            if crew_count >= 25: break
                except Exception as c_err:
                    logger.warning(f"Skipping crew member {p.get('name')} for {content_id}: {c_err}")
            
            # 5. Commit everything at once
            await self.db.commit()
            return all_credits
        except Exception as e:
            await self.db.rollback()
            logger.error(f"Error getting credits for {content_id}: {e}")
            return []

    async def get_season_details(self, content_id: str, season_number: int) -> Dict[str, Any]:
        """Fetch episodes for a specific season, proxying to TMDB."""
        content = await self.get_content_by_id(content_id)
        if not content or not content.tmdb_id:
            return {"episodes": []}
        
        return await self.tmdb_client.get_season_details(content.tmdb_id, season_number)

    async def get_similar_content(self, content_id: str) -> List[Dict[str, Any]]:
        """Fetch similar content from TMDB and upsert basic info."""
        try:
            content = await self.get_content_by_id(content_id)
            if not content or not content.tmdb_id: return []

            similar_data = await self.tmdb_client.get_similar(content.tmdb_id, content.content_type)
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
        stmt = text('''
            INSERT INTO persons (tmdb_id, name, profile_image_url, known_for_department)
            VALUES (:tmdb_id, :name, :profile_image_url, :dept)
            ON CONFLICT (tmdb_id) DO UPDATE SET
                name = EXCLUDED.name,
                profile_image_url = EXCLUDED.profile_image_url,
                known_for_department = EXCLUDED.known_for_department,
                last_synced_at = now(),
                updated_at = now()
            RETURNING id
        ''')
        res = await self.db.execute(stmt, {
            'tmdb_id': p.get('tmdb_id') or p.get('id'),
            'name': p.get('name'),
            'profile_image_url': p.get('profile_url') or p.get('profile_path') or p.get('image_url'),
            'dept': p.get('known_for_department')
        })
        row = res.mappings().one_or_none()
        return str(row['id']) if row else None

    async def _upsert_credit(self, content_id: str, person_id: str, role: str, order: int, 
                             character: str = None, job: str = None, department: str = None):
        """Upsert a credit record."""
        stmt = text('''
            INSERT INTO content_credits (content_id, person_id, role, character_name, job, department, display_order)
            VALUES (CAST(:cid AS UUID), CAST(:pid AS UUID), :role, :char, :job, :dept, :order)
            ON CONFLICT ON CONSTRAINT unique_content_person_role DO UPDATE SET
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
            SELECT 
                c_ids.id as content_id,
                COALESCE(ucs.is_watched, false) as is_watched,
                COALESCE(ucs.is_liked, false) as is_liked,
                COALESCE(ucs.is_dropped, false) as is_dropped,
                COALESCE(ucs.is_interested, false) as is_interested,
                COALESCE(ucs.watch_count, 0) as watch_count,
                ucs.rating as user_rating,
                ucs.status,
                ucs.progress_episodes,
                ucs.rewatch_count,
                ucs.last_activity_at,
                (EXISTS (SELECT 1 FROM calendar_alerts ca WHERE ca.user_id = :uid AND ca.content_id = c_ids.id)) as is_notified
            FROM (SELECT unnest(CAST(:ids AS UUID[])) as id) c_ids
            LEFT JOIN user_content_status ucs ON ucs.content_id = c_ids.id AND ucs.user_id = :uid
        '''), {'uid': user_id, 'ids': content_ids})
        
        status_map = {str(r['content_id']): r for r in res.mappings()}

        for it in items:
            cid = str(it.get('id', '')) if isinstance(it, dict) else str(getattr(it, 'id', ''))
            status = status_map.get(cid, {})
            
            def set_val(obj, key, val):
                if isinstance(obj, dict): obj[key] = val
                else: setattr(obj, key, val)

            set_val(it, 'is_watched', status.get('is_watched') or False)
            set_val(it, 'is_liked', status.get('is_liked') or False)
            set_val(it, 'is_dropped', status.get('is_dropped') or False)
            set_val(it, 'is_interested', status.get('is_interested') or False)
            set_val(it, 'is_notified', status.get('is_notified') or False)
            set_val(it, 'watch_count', status.get('watch_count') or 0)
            
            # New Tracking Fields
            # Fallback to 'completed' if is_watched is true (legacy sync)
            raw_status = status.get('status') or 'none'
            if raw_status == 'none' and (status.get('is_watched') or False):
                raw_status = 'completed'
                
            set_val(it, 'status', ContentStatus(raw_status))
            set_val(it, 'progress_episodes', status.get('progress_episodes') or 0)
            set_val(it, 'rewatch_count', status.get('rewatch_count') or 0)
            set_val(it, 'last_activity_at', status.get('last_activity_at'))

            # Explicitly cast to float to avoid Decimal-as-string issues in JSON
            val = status.get('rating')
            set_val(it, 'user_rating', float(val) if val is not None else None)

    async def _upsert_tmdb_content(self, items: List[Dict[str, Any]], returning: bool = True, is_permanent: bool = False) -> List[Dict[str, Any]]:
        if not items: return []
        seen = set()
        u_items = []
        for it in items:
            tid = it.get('tmdb_id')
            if tid and tid not in seen: seen.add(tid); u_items.append(it)
        stmt_text = '''
            INSERT INTO content (tmdb_id, content_type, title, original_title, original_language, synopsis, poster_url, backdrop_url, external_rating, external_rating_source, vote_count, release_date, genres, is_permanent, total_episodes, total_seasons, seasons, last_synced_at)
            VALUES (:tmdb_id, :content_type, :title, :original_title, :original_language, :synopsis, :poster_url, :backdrop_url, :external_rating, :external_rating_source, :vote_count, :release_date, :genres, :is_permanent, :total_episodes, :total_seasons, CAST(:seasons AS JSONB), now())
            ON CONFLICT (tmdb_id) DO UPDATE SET 
                title = EXCLUDED.title, 
                content_type = EXCLUDED.content_type,
                synopsis = EXCLUDED.synopsis, 
                poster_url = EXCLUDED.poster_url, 
                backdrop_url = EXCLUDED.backdrop_url, 
                external_rating = EXCLUDED.external_rating, 
                vote_count = EXCLUDED.vote_count,
                original_language = EXCLUDED.original_language,
                genres = EXCLUDED.genres, 
                total_episodes = EXCLUDED.total_episodes,
                total_seasons = EXCLUDED.total_seasons,
                seasons = EXCLUDED.seasons,
                last_synced_at = now(),
                is_permanent = content.is_permanent OR EXCLUDED.is_permanent
        '''
        if returning: stmt_text += ' RETURNING *'
        stmt = text(stmt_text)
        params = []
        for it in u_items:
            rd = it.get('release_date')
            ctype = it.get('content_type')
            
            # Smart Re-classification: If it's a TMDB Series but is Japanese Animation, it's Anime.
            # (TMDB Genre 16 is Animation)
            genres = it.get('genres', [])
            if ctype == 'series' and it.get('original_language') == 'ja' and ('Animation' in genres or 16 in it.get('genre_ids', [])):
                ctype = 'anime'
                it['content_type'] = 'anime'

            if rd and isinstance(rd, str):
                try: rd = datetime.strptime(rd[:10], '%Y-%m-%d').date()
                except ValueError: rd = None
            params.append({ 
                'tmdb_id': it.get('tmdb_id'), 
                'content_type': ctype, 
                'title': it.get('title'), 
                'original_title': it.get('original_title'), 
                'original_language': it.get('original_language'),
                'synopsis': it.get('synopsis'), 
                'poster_url': it.get('poster_url'), 
                'backdrop_url': it.get('backdrop_url'), 
                'external_rating': it.get('external_rating'), 
                'external_rating_source': it.get('external_rating_source'), 
                'vote_count': it.get('vote_count', 0),
                'release_date': rd, 
                'genres': it.get('genres', []),
                'is_permanent': is_permanent,
                'total_episodes': it.get('total_episodes') or it.get('number_of_episodes'),
                'total_seasons': it.get('total_seasons') or it.get('number_of_seasons'),
                'seasons': json.dumps(it.get('seasons', []))
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

    async def cleanup_stale_persons(self, hours: int = 24) -> int:
        """
        Deletes persons that are not permanent, were last synced more than X hours ago,
        and are not referenced in any user favorites.
        """
        logger.info(f"Starting stale persons cleanup (older than {hours} hours)...")
        try:
            # We use a single query with NOT EXISTS for efficiency
            # TMDB ID in persons is currently an integer, cast to text to match repo references.
            query = text(f"""
                DELETE FROM persons
                WHERE is_permanent = false
                AND last_synced_at < now() - interval '{{hours}} hours'
                AND NOT EXISTS (SELECT 1 FROM user_person_favorites WHERE person_id = CAST(persons.tmdb_id AS TEXT))
                AND NOT EXISTS (SELECT 1 FROM user_actor_preferences WHERE CAST(person_id AS TEXT) = persons.id)
                AND NOT EXISTS (SELECT 1 FROM user_director_preferences WHERE CAST(person_id AS TEXT) = persons.id)
            """.replace("{hours}", str(hours)))
            
            result = await self.db.execute(query)
            await self.db.commit()
            
            deleted_count = result.rowcount if result.rowcount is not None else 0
            logger.info(f"Person cleanup finished. Deleted {deleted_count} stale persons.")
            return deleted_count
        except Exception as e:
            logger.error(f"Person cleanup failed: {e}")
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

    async def get_landing_posters(self, target_count: int = 12) -> List[str]:
        """
        Returns a stable list of poster URLs for the landing screen carousel.
        Strategy:
          1. If the DB already has >= target_count permanently saved posters, return them.
          2. Otherwise, fetch trending movies from TMDB, pick the first ones with a valid
             poster, save them permanently to the landing_posters table, and return them.
          3. If TMDB is unreachable, return whatever is in the DB (even if < target_count).
        """
        try:
            # Step 1: Check if we already have enough saved posters
            res = await self.db.execute(text(
                "SELECT poster_url FROM landing_posters ORDER BY id LIMIT :limit"
            ), {"limit": target_count})
            rows = [r["poster_url"] for r in res.mappings()]

            if len(rows) >= target_count:
                logger.info(f"Landing posters: serving {len(rows)} from DB cache.")
                return rows

            # Step 2: Fetch from TMDB
            logger.info("Landing posters: not enough in DB — fetching from TMDB...")
            try:
                trending = await self.tmdb_client.get_trending_movies(page=1)
                # Also grab popular for more variety
                popular = await self.tmdb_client.get_popular_movies(page=1)
                all_items = trending + popular
            except Exception as tmdb_err:
                logger.error(f"Landing posters: TMDB fetch failed: {tmdb_err}")
                # Return whatever we have in DB, even if empty
                return rows

            # Filter: only items with a valid poster
            new_posters = []
            for item in all_items:
                purl = item.get("poster_url")
                if purl and purl not in rows:
                    new_posters.append({
                        "poster_url": purl,
                        "tmdb_id": item.get("tmdb_id"),
                        "title": item.get("title"),
                    })
                if len(new_posters) >= (target_count - len(rows)):
                    break

            # Step 3: Save new posters permanently (ON CONFLICT DO NOTHING)
            for p in new_posters:
                try:
                    await self.db.execute(text(
                        """
                        INSERT INTO landing_posters (poster_url, tmdb_id, title, fetched_at)
                        VALUES (:url, :tmdb_id, :title, now())
                        ON CONFLICT (poster_url) DO NOTHING
                        """
                    ), {"url": p["poster_url"], "tmdb_id": p["tmdb_id"], "title": p["title"]})
                except Exception as insert_err:
                    logger.warning(f"Landing posters insert skipped: {insert_err}")
                    await self.db.rollback()

            try:
                await self.db.commit()
            except Exception as commit_err:
                logger.warning(f"Landing posters commit warning: {commit_err}")
                await self.db.rollback()

            # Re-fetch from DB for a clean, consistent response
            res2 = await self.db.execute(text(
                "SELECT poster_url FROM landing_posters ORDER BY id LIMIT :limit"
            ), {"limit": target_count})
            final_rows = [r["poster_url"] for r in res2.mappings()]

            logger.info(f"Landing posters: returning {len(final_rows)} posters.")
            return final_rows

        except Exception as e:
            logger.error(f"get_landing_posters failed: {e}")
            # Ultimate fallback — return empty list; client will use mock data
            return []