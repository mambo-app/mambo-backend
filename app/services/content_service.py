import asyncio
import json
from typing import List, Dict, Any, Optional, Union
from sqlalchemy.ext.asyncio import AsyncSession
from datetime import date, datetime, timezone
from sqlalchemy import text, or_, and_
from uuid import UUID
from app.services.tmdb_client import TMDBClient
from app.services.mal_client import MALClient
from app.models.content import ContentResponse, HomeTrendingResponse, ContentStatus, SeasonStatusResponse
from app.core.logger import get_logger
from app.services.cache_service import cache, CacheKeys, CacheService

logger = get_logger('mambo.content')
_TRAILER_MEMORY_CACHE: Dict[str, Dict[str, Any]] = {}

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

    async def _resolve_items_to_db_uuids(self, items: List[Any]):
        """
        Resolve TMDB/MAL IDs on feed items to real DB UUIDs so that
        Flutter receives stable, correct UUIDs for collection status lookups.
        Only mutates items that have a non-UUID 'id' (i.e. raw integer TMDB IDs).
        """
        if not items:
            return
        
        tmdb_ids = []
        mal_ids = []
        for it in items:
            raw_id = str(it.get('id', '')) if isinstance(it, dict) else str(getattr(it, 'id', ''))
            # Skip items that already have a real UUID
            try:
                UUID(raw_id)
                continue  # Already a UUID, skip
            except (ValueError, AttributeError):
                pass
            
            tmdb_id = it.get('tmdb_id') if isinstance(it, dict) else getattr(it, 'tmdb_id', None)
            mal_id = it.get('mal_id') if isinstance(it, dict) else getattr(it, 'mal_id', None)
            if tmdb_id: tmdb_ids.append(int(tmdb_id))
            if mal_id: mal_ids.append(int(mal_id))

        if not tmdb_ids and not mal_ids:
            return

        try:
            res = await self.db.execute(text('''
                SELECT id, tmdb_id, mal_id FROM content
                WHERE (CAST(:has_tids AS BOOLEAN) AND tmdb_id = ANY(:tids))
                   OR (CAST(:has_mids AS BOOLEAN) AND mal_id = ANY(:mids))
            '''), {
                'has_tids': bool(tmdb_ids), 'tids': tmdb_ids or [-1],
                'has_mids': bool(mal_ids), 'mids': mal_ids or [-1],
            })
            rows = res.mappings().fetchall()
            tmdb_uuid_map = {r['tmdb_id']: str(r['id']) for r in rows if r['tmdb_id'] is not None}
            mal_uuid_map = {r['mal_id']: str(r['id']) for r in rows if r['mal_id'] is not None}

            for it in items:
                raw_id = str(it.get('id', '')) if isinstance(it, dict) else str(getattr(it, 'id', ''))
                try:
                    UUID(raw_id)
                    continue  # Already a real UUID
                except (ValueError, AttributeError):
                    pass
                
                tmdb_id = it.get('tmdb_id') if isinstance(it, dict) else getattr(it, 'tmdb_id', None)
                mal_id = it.get('mal_id') if isinstance(it, dict) else getattr(it, 'mal_id', None)
                
                if tmdb_id and int(tmdb_id) in tmdb_uuid_map:
                    if isinstance(it, dict): it['id'] = tmdb_uuid_map[int(tmdb_id)]
                    else: setattr(it, 'id', tmdb_uuid_map[int(tmdb_id)])
                elif mal_id and int(mal_id) in mal_uuid_map:
                    if isinstance(it, dict): it['id'] = mal_uuid_map[int(mal_id)]
                    else: setattr(it, 'id', mal_uuid_map[int(mal_id)])
        except Exception as e:
            logger.warning(f"_resolve_items_to_db_uuids error: {e}")

    def _map_to_response(self, db_list: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        import uuid
        results = []
        today = date.today()
        valid_statuses = {'plan_to_watch', 'watching', 'completed', 'on_hold', 'dropped', 'none'}

        for d in db_list:
            try:
                # 0. Sanitize UUID id - prefer real DB UUID over generated uuid5
                raw_id = str(d.get('id') or d.get('mal_id') or d.get('tmdb_id') or '')
                try:
                    UUID(raw_id)
                    d['id'] = raw_id
                except ValueError:
                    if d.get('tmdb_id'):
                        gen_uuid = str(uuid.uuid5(uuid.NAMESPACE_DNS, f"tmdb:{d['tmdb_id']}"))
                        d['id'] = gen_uuid
                        try:
                            asyncio.create_task(cache.set(f"uuid_map:{gen_uuid}", {"tmdb_id": d['tmdb_id'], "content_type": d.get('content_type', 'movie')}, ttl=86400))
                        except Exception: pass
                    elif d.get('mal_id'):
                        gen_uuid = str(uuid.uuid5(uuid.NAMESPACE_DNS, f"mal:{d['mal_id']}"))
                        d['id'] = gen_uuid
                        try:
                            asyncio.create_task(cache.set(f"uuid_map:{gen_uuid}", {"mal_id": d['mal_id'], "content_type": "anime"}, ttl=86400))
                        except Exception: pass
                    else:
                        d['id'] = str(uuid.uuid4())

                # 0. Sanitize user tracking status enum
                curr_status = d.get('status')
                if curr_status not in valid_statuses:
                    if 'status' in d:
                        d['release_status'] = d.pop('status')
                    d['status'] = 'none'

                # 1. Prepare computed fields
                rd = d.get('release_date')
                rd_date = None
                if isinstance(rd, str) and rd.strip():
                    try:
                        parts = rd.split('T')[0].split('-')
                        if len(parts) == 1 and len(parts[0]) == 4:
                            rd_date = date(int(parts[0]), 1, 1)
                        elif len(parts) == 2 and len(parts[0]) == 4:
                            rd_date = date(int(parts[0]), int(parts[1]), 1)
                        else:
                            rd_date = date.fromisoformat(rd.split('T')[0])
                    except Exception:
                        rd_date = None
                elif isinstance(rd, datetime):
                    rd_date = rd.date()
                elif isinstance(rd, date):
                    rd_date = rd

                d['is_anticipated'] = bool(rd_date and rd_date > today)
                d['avg_star_rating'] = self._get_display_rating(d)
                
                # 2. Validate with Pydantic for schema correctness
                model = ContentResponse.model_validate(d)
                
                # 3. Convert back to dict and ensure ID is a string for the App
                safe_dict = model.model_dump()
                safe_dict['id'] = str(safe_dict['id'])
                safe_dict['description'] = safe_dict.get('synopsis')
                
                # 4. Distribution and Counts
                safe_dict['vote_count'] = d.get('vote_count') or 0
                safe_dict['rating_distribution'] = self._calculate_distribution(d['avg_star_rating'])

                # Ensure series metadata is passed to rows
                if d.get('content_type') in ['series', 'anime', 'tv']:
                    safe_dict['total_seasons'] = d.get('total_seasons') or 1
                    safe_dict['total_episodes'] = d.get('total_episodes') or 0

                if safe_dict.get('release_date'):
                    if hasattr(safe_dict['release_date'], 'isoformat'):
                        safe_dict['release_date'] = safe_dict['release_date'].isoformat()
                    else:
                        safe_dict['release_date'] = str(safe_dict['release_date'])
                
                results.append(safe_dict)
            except Exception as e:
                logger.warning(f"Map failed for {d.get('id')}: {e}")
                # Ultimate fallback: Raw dict with valid UUID string
                if 'id' not in d or len(str(d['id'])) < 32:
                    d['id'] = str(uuid.uuid4())
                d['status'] = d.get('status') if d.get('status') in valid_statuses else 'none'
                results.append(d)
        return results

    async def get_home_trending(self, user_id: Optional[str] = None) -> HomeTrendingResponse:
        cache_key = f"v600:daily_trending_spotlight:{date.today().isoformat()}"
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

        m_list, s_list, a_list = [], [], []
        today = date.today()

        def _is_valid_trending(item: dict) -> bool:
            rd = item.get('release_date') or item.get('first_air_date')
            if rd:
                if isinstance(rd, str):
                    try: rd = datetime.strptime(rd[:10], '%Y-%m-%d').date()
                    except ValueError: pass
                if isinstance(rd, date) and rd > today:
                    return False
            if item.get('is_anticipated') is True:
                return False
            return True

        # 1. Fetch live daily trending items with generous 4-second timeout
        try:
            m_raw = await asyncio.wait_for(self.tmdb_client.get_trending_movies(page=1), timeout=4.0)
            if isinstance(m_raw, list) and m_raw:
                m_list = [m for m in m_raw if _is_valid_trending(m)][:20]
        except Exception as net_m:
            logger.warning(f"TMDB movies trending fetch timeout: {net_m}")

        try:
            s_raw = await asyncio.wait_for(self.tmdb_client.get_trending_series(page=1), timeout=4.0)
            if isinstance(s_raw, list) and s_raw:
                s_list = [s for s in s_raw if s.get("content_type") != "anime" and _is_valid_trending(s)][:20]
        except Exception as net_s:
            logger.warning(f"TMDB series trending fetch timeout: {net_s}")

        try:
            a_task = asyncio.create_task(self.mal_client.get_trending_anime())
            a_raw = await asyncio.wait_for(a_task, timeout=3.0)
            if isinstance(a_raw, list) and a_raw:
                a_list = [a for a in a_raw if _is_valid_trending(a)][:20]
        except Exception as net_a:
            logger.warning(f"MAL anime trending fetch timeout/skipped: {net_a}")

        # 2. Local DB query ordered strictly by trending_score / popularity DESC
        try:
            if not m_list:
                m_res = await self.db.execute(text("""
                    SELECT * FROM content WHERE content_type = 'movie' 
                    AND (release_date IS NULL OR release_date <= CURRENT_DATE)
                    ORDER BY trending_score DESC NULLS LAST, popularity DESC NULLS LAST, vote_count DESC NULLS LAST, external_rating DESC NULLS LAST LIMIT 20
                """))
                m_list = [dict(r) for r in m_res.mappings()]

            if not s_list:
                s_res = await self.db.execute(text("""
                    SELECT * FROM content WHERE content_type = 'series' 
                    AND (release_date IS NULL OR release_date <= CURRENT_DATE)
                    ORDER BY trending_score DESC NULLS LAST, popularity DESC NULLS LAST, vote_count DESC NULLS LAST, external_rating DESC NULLS LAST LIMIT 20
                """))
                s_list = [dict(r) for r in s_res.mappings()]

            if not a_list:
                a_res = await self.db.execute(text("""
                    SELECT * FROM content WHERE content_type = 'anime' 
                    AND (release_date IS NULL OR release_date <= CURRENT_DATE)
                    ORDER BY trending_score DESC NULLS LAST, popularity DESC NULLS LAST, vote_count DESC NULLS LAST, external_rating DESC NULLS LAST LIMIT 20
                """))
                a_list = [dict(r) for r in a_res.mappings()]
        except Exception as db_e:
            logger.warning(f"Home trending local DB query error: {db_e}")

        # 2. Async background refresh for live trending items (non-blocking)
        async def _refresh_live_trending():
            try:
                m_raw, s_raw, a_raw = await asyncio.gather(
                    self.tmdb_client.get_trending_movies(page=1),
                    self.tmdb_client.get_trending_series(page=1),
                    self.mal_client.get_trending_anime(),
                    return_exceptions=True
                )
                from app.core.database import AsyncSessionLocal
                async with AsyncSessionLocal() as bg_db:
                    bg_svc = ContentService(bg_db)
                    if isinstance(m_raw, list) and m_raw: await bg_svc._upsert_tmdb_content(m_raw[:20], returning=False)
                    if isinstance(s_raw, list) and s_raw: await bg_svc._upsert_tmdb_content(s_raw[:20], returning=False)
                    if isinstance(a_raw, list) and a_raw: await bg_svc._upsert_mal_content(a_raw[:20], returning=False)
            except Exception as ex:
                logger.warning(f"Background live trending refresh warning: {ex}")

        asyncio.create_task(_refresh_live_trending())

        # 2. DB Fallback (filtered by released items & vote count)
        try:
            if not m_list:
                res = await self.db.execute(text("""
                    SELECT * FROM content 
                    WHERE content_type = 'movie' 
                      AND (release_date IS NULL OR release_date <= CURRENT_DATE)
                    ORDER BY release_date DESC NULLS LAST, vote_count DESC NULLS LAST 
                    LIMIT 20
                """))
                m_list = [dict(r) for r in res.mappings()]

            if not s_list:
                res = await self.db.execute(text("""
                    SELECT * FROM content 
                    WHERE content_type = 'series' 
                      AND (release_date IS NULL OR release_date <= CURRENT_DATE)
                    ORDER BY release_date DESC NULLS LAST, vote_count DESC NULLS LAST 
                    LIMIT 20
                """))
                s_list = [dict(r) for r in res.mappings()]

            if not a_list:
                res = await self.db.execute(text("""
                    SELECT * FROM content 
                    WHERE content_type = 'anime' 
                      AND (release_date IS NULL OR release_date <= CURRENT_DATE)
                    ORDER BY release_date DESC NULLS LAST, vote_count DESC NULLS LAST 
                    LIMIT 20
                """))
                a_list = [dict(r) for r in res.mappings()]
        except Exception as db_err:
            logger.error(f"DB fallback query error in get_home_trending: {db_err}")
            try:
                await self.db.rollback()
            except Exception:
                pass

        all_items = m_list + s_list + a_list
        if user_id:
            await self._populate_user_status(all_items, user_id)

        # Resolve TMDB/MAL IDs to real DB UUIDs so Flutter gets stable, correct IDs
        await self._resolve_items_to_db_uuids(all_items)

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
        
        # 1. Query Cache (Bust with v970 prefix for live TMDB/MAL airing & purged mock seed rows)
        cache_key = f"v970:{CacheKeys.discover(mode, user_id if user_id else 'guest', date.today().isoformat())}"
        cached = await cache.get(cache_key)
        if cached:
            if user_id:
                all_items = []
                for k in ["popular", "top_rated", "anticipated", "currently_airing", "watchlist"]:
                    if k in cached and isinstance(cached[k], list): all_items.extend(cached[k])
                for row in cached.get("genre_rows", []):
                    if isinstance(row, dict) and "items" in row:
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
                aliases = [genre_filter]
                if genre_filter == 'Sci-Fi':
                    aliases.append('Science Fiction')
                elif genre_filter == 'Science Fiction':
                    aliases.append('Sci-Fi')
                sql += " AND genres && CAST(:aliases AS TEXT[])"
                params['aliases'] = aliases
            if min_rating is not None:
                sql += " AND external_rating >= :r"
                params['r'] = min_rating
            if future_only:
                sql += " AND (release_date > CURRENT_DATE OR status = 'upcoming')"
            res = await self.db.execute(text(sql), params)
            return res.scalar() or 0

        async def _db_query(limit=10, genre_filter=None, future_only=False, min_rating=None, sort='rating'):
            sql = 'SELECT * FROM content WHERE content_type = :ct'
            params: Dict[str, Any] = {'ct': content_type, 'limit': limit}
            if genre_filter:
                aliases = [genre_filter]
                if genre_filter == 'Sci-Fi':
                    aliases.append('Science Fiction')
                elif genre_filter == 'Science Fiction':
                    aliases.append('Sci-Fi')
                sql += " AND genres && CAST(:aliases AS TEXT[])"
                params['aliases'] = aliases
            if future_only:
                sql += " AND release_date > CURRENT_DATE AND title NOT ILIKE '%podcast%' AND title NOT ILIKE '%behind the scenes%' AND title NOT ILIKE '%aftershow%'"
            if min_rating is not None:
                sql += ' AND external_rating >= :r'
                params['r'] = min_rating
            
            if future_only:
                sql += ' ORDER BY release_date ASC NULLS LAST, vote_count DESC NULLS LAST'
            elif sort == 'rating':
                sql += ' ORDER BY external_rating DESC NULLS LAST'
            else:
                sql += ' ORDER BY last_synced_at DESC NULLS LAST'
            sql += ' LIMIT :limit'
            res = await self.db.execute(text(sql), params)
            return [dict(row) for row in res.mappings()]

        CACHE_MIN = 100
        ANT_MIN = 200
        
        # 2. Check and Prepare Fetch Tasks
        fetch_tasks = {}
        
        # Standard rows
        if await _db_fresh_count() < CACHE_MIN:
            if content_type == 'movie': 
                fetch_tasks['popular'] = asyncio.gather(
                    self.tmdb_client.get_popular_movies(1), 
                    self.tmdb_client.get_now_playing_movies(1),
                    self.tmdb_client.get_now_playing_movies(2),
                    self.tmdb_client.get_indian_movies(1),
                    self.tmdb_client.get_indian_now_playing(1),
                    return_exceptions=True
                )
            elif content_type == 'series':
                fetch_tasks['popular'] = self.tmdb_client.get_popular_series(1)
            else:
                fetch_tasks['popular'] = self.mal_client.get_top_anime(1)
                
        if await _db_fresh_count(min_rating=7.2) < CACHE_MIN:
            if content_type == 'movie': fetch_tasks['top_rated'] = asyncio.gather(self.tmdb_client.get_top_rated_movies(1), self.tmdb_client.get_indian_movies(1), return_exceptions=True)
            elif content_type == 'series': fetch_tasks['top_rated'] = self.tmdb_client.get_top_rated_series(1)
            else: fetch_tasks['top_rated'] = self.mal_client.get_trending_anime()
 
        if await _db_fresh_count(future_only=True) < ANT_MIN:
            if content_type == 'movie': fetch_tasks['anticipated'] = asyncio.gather(self.tmdb_client.get_most_anticipated_movies(1), self.tmdb_client.get_indian_upcoming_movies(1), return_exceptions=True)
            elif content_type == 'series': fetch_tasks['anticipated'] = self.tmdb_client.get_most_anticipated_series(1)
            else: fetch_tasks['anticipated'] = self.mal_client.get_upcoming_anime()

        # Genre-specific rows
        for genre in target_genres:
            if await _db_fresh_count(genre_filter=genre) < 15:
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
            try:
                logger.info(f"Discovery: BG Fetching {len(fetch_tasks)} tasks for {mode}...")
                keys = list(fetch_tasks.keys())
                # Perform network HTTP calls OUTSIDE any DB session to prevent connection pool locks
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
                
                # Only acquire DB session when ready to upsert
                if combined_tmdb or combined_mal:
                    from app.core.database import AsyncSessionLocal
                    async with AsyncSessionLocal() as bg_db:
                        bg_service = ContentService(bg_db)
                        if combined_tmdb: await bg_service._upsert_tmdb_content(combined_tmdb, returning=False, is_permanent=False)
                        if combined_mal: await bg_service._upsert_mal_content(combined_mal, returning=False, is_permanent=False)
            except Exception as e:
                logger.error(f"Discovery BG fetch error: {e}")

        pop_db = await _db_query(limit=50, sort='rating')
        if fetch_tasks:
            asyncio.create_task(_background_fetch())

        # 4. Final DB Queries for Response
        # Re-fetch everything to ensure we have the latest (either from DB or the sync fetch above)
        top_db = []
        watchlist_items = []
        if user_id:
            try:
                # 1. Fetch user's favorite genres
                from app.repositories.user_repo import UserRepository
                repo = UserRepository(self.db)
                fav_genres = await repo.get_favorite_genres(user_id)
                
                # 2. Fetch user's recently watched genres
                recent_genres_res = await self.db.execute(text("""
                    SELECT co.genres
                    FROM user_content_status ucs
                    JOIN content co ON co.id = ucs.content_id
                    WHERE ucs.user_id = :uid AND (ucs.is_watched = true OR ucs.status = 'completed')
                    ORDER BY ucs.last_activity_at DESC NULLS LAST
                    LIMIT 10
                """), {'uid': UUID(user_id)})
                
                genres_set = set(fav_genres)
                for r in recent_genres_res.mappings():
                    if r['genres']:
                        genres_set.update(r['genres'])
                
                target_genres_list = list(genres_set)[:5]
                
                # 3. Query favorite persons (actors/directors)
                fav_persons_count_res = await self.db.execute(
                    text("SELECT COUNT(*) FROM user_person_favorites WHERE user_id = :uid"),
                    {'uid': UUID(user_id)}
                )
                has_fav_persons = (fav_persons_count_res.scalar() or 0) > 0
                
                # 4. Query personalized recommendations using optimized NOT EXISTS anti-joins
                query_sql = """
                    SELECT co.*
                    FROM content co
                    WHERE co.content_type = :ct
                      AND NOT EXISTS (
                          SELECT 1 FROM user_content_status ucs
                          WHERE ucs.content_id = co.id AND ucs.user_id = :uid
                            AND (ucs.is_watched = true OR ucs.status = 'completed' OR ucs.is_interested = true OR ucs.status = 'plan_to_watch' OR ucs.is_dropped = true OR ucs.is_skipped = true)
                      )
                      AND NOT EXISTS (
                          SELECT 1 FROM collection_items ci
                          JOIN collections c ON c.id = ci.collection_id
                          WHERE ci.content_id = co.id AND c.user_id = :uid
                      )
                """
                params = {'uid': UUID(user_id), 'ct': content_type, 'limit': 20}
                
                conds = []
                if target_genres_list:
                    conds.append("co.genres && CAST(:genres AS TEXT[])")
                    params['genres'] = target_genres_list
                    
                if conds:
                    query_sql += f" AND ({' OR '.join(conds)})"
                    
                query_sql += " ORDER BY co.external_rating DESC NULLS LAST LIMIT :limit"
                
                res = await self.db.execute(text(query_sql), params)
                top_db = [dict(row) for row in res.mappings()]
                
                # 5. Fallback logic: merge globally top rated content (filtered to exclude user content)
                fallback_res = await self.db.execute(text("""
                    SELECT co.* FROM content co
                    WHERE co.content_type = :ct
                      AND NOT EXISTS (
                          SELECT 1 FROM user_content_status ucs
                          WHERE ucs.content_id = co.id AND ucs.user_id = :uid
                            AND (ucs.is_watched = true OR ucs.status = 'completed' OR ucs.is_interested = true OR ucs.status = 'plan_to_watch' OR ucs.is_dropped = true OR ucs.is_skipped = true)
                      )
                      AND NOT EXISTS (
                          SELECT 1 FROM collection_items ci
                          JOIN collections c ON c.id = ci.collection_id
                          WHERE ci.content_id = co.id AND c.user_id = :uid
                      )
                    ORDER BY co.external_rating DESC NULLS LAST
                    LIMIT 20
                """), {'uid': UUID(user_id), 'ct': content_type})
                filtered_fallback = [dict(row) for row in fallback_res.mappings()]
                
                seen_ids = {str(item['id']) for item in top_db}
                for item in filtered_fallback:
                    if str(item['id']) not in seen_ids:
                        top_db.append(item)
                        if len(top_db) >= 20: break
                top_db = top_db[:20]
            except Exception as e:
                logger.error(f"Error computing personalized recommendations: {e}")
                top_db = await _db_query(limit=20, min_rating=7.2)
                
            # Query Watchlist items (sorted by external_rating descending for best quality first)
            try:
                wl_res = await self.db.execute(text("""
                    SELECT co.*
                    FROM user_content_status ucs
                    JOIN content co ON co.id = ucs.content_id
                    WHERE ucs.user_id = :uid
                      AND (ucs.is_interested = true OR ucs.status = 'plan_to_watch')
                      AND co.content_type = :ct
                    ORDER BY co.external_rating DESC NULLS LAST
                    LIMIT 20
                """), {'uid': UUID(user_id), 'ct': content_type})
                watchlist_items = [dict(row) for row in wl_res.mappings()]
            except Exception as e:
                logger.error(f"Error fetching watchlist discover items: {e}")
        else:
            top_db = await _db_query(limit=20, min_rating=7.2)

        # Non-blocking anticipated content: fetch from local DB instantly, sync live TMDB in background if DB is low
        async def _sync_anticipated_bg():
            from app.core.database import AsyncSessionLocal
            async with AsyncSessionLocal() as bg_db:
                bg_svc = ContentService(bg_db)
                try:
                    if content_type == 'movie':
                        a_live = await bg_svc.tmdb_client.get_most_anticipated_movies(1)
                        if a_live: await bg_svc._upsert_tmdb_content(a_live, returning=False)
                    elif content_type == 'series':
                        a_live = await bg_svc.tmdb_client.get_most_anticipated_series(1)
                        try:
                            on_air = await bg_svc.tmdb_client.get_on_the_air_series(1)
                            if on_air and a_live: a_live.extend(on_air)
                        except Exception: pass
                        if a_live: await bg_svc._upsert_tmdb_content(a_live, returning=False)
                    else:
                        a_live = await bg_svc.mal_client.get_upcoming_anime()
                        if a_live: await bg_svc._upsert_mal_content(a_live, returning=False)
                except Exception as e:
                    logger.warning(f"Background anticipated fetch skipped: {e}")

        ant_db_raw = await _db_query(limit=50, future_only=True)
        ant_live = []
        if len(ant_db_raw) < 15:
            try:
                if content_type == 'series':
                    live_raw = await asyncio.wait_for(self.tmdb_client.get_most_anticipated_series(1), timeout=1.2)
                    if live_raw: ant_live = self._map_to_response(live_raw)
                elif content_type == 'movie':
                    live_raw = await asyncio.wait_for(self.tmdb_client.get_most_anticipated_movies(1), timeout=1.2)
                    if live_raw: ant_live = self._map_to_response(live_raw)
            except Exception as e:
                logger.warning(f"Fast live anticipated fetch skipped: {e}")

        seen_keys = set()
        merged_ant = []
        for item in ant_live + self._map_to_response(ant_db_raw):
            t_id = str(item.get('tmdb_id') or '').strip()
            title_key = str(item.get('title', '')).lower().strip()
            key = f"{t_id}:{title_key}" if t_id else title_key
            if title_key and key not in seen_keys:
                seen_keys.add(key)
                merged_ant.append(item)

        # Strict filter: EXCLUDE ended, canceled, or past releases
        today_val = date.today()
        strict_anticipated = []
        for item in merged_ant:
            r_date = item.get('release_date') or item.get('first_air_date')
            next_ep = item.get('next_episode_to_air')

            if isinstance(next_ep, dict) and next_ep.get('air_date'):
                try:
                    nep_d = date.fromisoformat(next_ep['air_date'][:10])
                    if nep_d >= today_val:
                        r_date = next_ep['air_date']
                        item['release_date'] = r_date
                except Exception: pass

            st_val = str(item.get('release_status', '') or item.get('status', '')).lower()

            # Exclude ended, canceled, or finished titles
            if any(x in st_val for x in ['ended', 'canceled', 'completed', 'finished']):
                continue

            r_date_obj = None
            if isinstance(r_date, str) and r_date.strip():
                try:
                    parts = r_date.split('T')[0].split('-')
                    if len(parts) == 1 and len(parts[0]) == 4:
                        r_date_obj = date(int(parts[0]), 1, 1)
                    elif len(parts) == 2 and len(parts[0]) == 4:
                        r_date_obj = date(int(parts[0]), int(parts[1]), 1)
                    else:
                        r_date_obj = date.fromisoformat(r_date.split('T')[0])
                except Exception:
                    r_date_obj = None
            elif isinstance(r_date, datetime):
                r_date_obj = r_date.date()
            elif isinstance(r_date, date):
                r_date_obj = r_date

            # Most Anticipated rule: Must have a CONFIRMED future release_date > TODAY
            # OR a confirmed next_episode_to_air > TODAY
            has_future_date = False
            if isinstance(next_ep, dict) and next_ep.get('air_date'):
                try:
                    nep_d = date.fromisoformat(next_ep['air_date'][:10])
                    if nep_d > today_val:
                        has_future_date = True
                        item['release_date'] = next_ep['air_date']
                except Exception: pass

            if not has_future_date:
                if r_date_obj and r_date_obj > today_val:
                    has_future_date = True

            if not has_future_date:
                continue

            strict_anticipated.append(item)

        def _parse_date_obj(val: Any) -> Optional[date]:
            if not val:
                return None
            if isinstance(val, date) and not isinstance(val, datetime):
                return val
            if isinstance(val, datetime):
                return val.date()
            if isinstance(val, str):
                s = val.strip()
                if not s:
                    return None
                try:
                    parts = s.split('T')[0].split('-')
                    if len(parts) == 1 and len(parts[0]) == 4:
                        return date(int(parts[0]), 1, 1)
                    elif len(parts) == 2 and len(parts[0]) == 4:
                        return date(int(parts[0]), int(parts[1]), 1)
                    else:
                        return date.fromisoformat(s.split('T')[0])
                except Exception:
                    return None
            return None

        def _ant_sort_key(x):
            d_obj = _parse_date_obj(x.get('release_date'))
            d_str = d_obj.isoformat() if d_obj else '9999-12-31'
            return (
                d_obj is None,
                d_str,
                -float(x.get('popularity') or x.get('vote_count') or 0)
            )

        strict_anticipated.sort(key=_ant_sort_key)
        ant_db = strict_anticipated[:60]

        async def _db_latest_query(limit=30) -> List[Dict[str, Any]]:
            # Tiered fallback to ensure robust results but prioritised by date
            for months in [6, 12, 24, 60, 120]:
                sql = f"""
                    SELECT * FROM content 
                    WHERE content_type = :ct 
                      AND release_date <= CURRENT_DATE
                      AND release_date >= CURRENT_DATE - INTERVAL '{months} months'
                    ORDER BY release_date DESC NULLS LAST, external_rating DESC NULLS LAST
                    LIMIT :limit
                """
                res = await self.db.execute(text(sql), {'ct': content_type, 'limit': limit})
                rows = [dict(row) for row in res.mappings()]
                if len(rows) >= 15:
                    return rows
            sql = """
                SELECT * FROM content 
                WHERE content_type = :ct 
                  AND release_date <= CURRENT_DATE
                ORDER BY release_date DESC NULLS LAST, external_rating DESC NULLS LAST
                LIMIT :limit
            """
            res = await self.db.execute(text(sql), {'ct': content_type, 'limit': limit})
            return [dict(row) for row in res.mappings()]

        latest_db = await _db_latest_query(limit=30)

        async def _db_currently_airing_query(limit=30) -> List[Dict[str, Any]]:
            async def _bg_upsert(items):
                from app.core.database import AsyncSessionLocal
                try:
                    async with AsyncSessionLocal() as bg_db:
                        await ContentService(bg_db)._upsert_tmdb_content(items, returning=False)
                except Exception: pass

            live_items = []
            try:
                if content_type == 'movie':
                    raw_live = await self.tmdb_client.get_now_playing_movies(1)
                    if raw_live:
                        live_items = raw_live
                        asyncio.create_task(_bg_upsert(raw_live))
                elif content_type == 'series':
                    raw_live = await self.tmdb_client.get_on_the_air_series(1)
                    if raw_live:
                        live_items = raw_live
                        asyncio.create_task(_bg_upsert(raw_live))
            except Exception as e:
                logger.warning(f"Live airing fetch error: {e}")

            if content_type == 'movie':
                sql = """
                    SELECT * FROM content 
                    WHERE content_type = 'movie' 
                      AND release_date IS NOT NULL
                      AND release_date <= CURRENT_DATE
                      AND release_date >= CURRENT_DATE - INTERVAL '45 days'
                      AND (vote_count IS NULL OR vote_count >= 5 OR external_rating >= 5.0)
                    ORDER BY (COALESCE(vote_count, 0) * COALESCE(external_rating, 0.0)) DESC, release_date DESC NULLS LAST
                    LIMIT :limit
                """
            else:
                sql = """
                    SELECT * FROM content 
                    WHERE content_type = :ct 
                      AND release_date IS NOT NULL
                      AND release_date <= CURRENT_DATE
                      AND release_date >= CURRENT_DATE - INTERVAL '365 days'
                      AND (status IS NULL OR LOWER(status) NOT IN ('ended', 'canceled', 'completed', 'finished', 'upcoming'))
                      AND (vote_count IS NULL OR vote_count >= 5 OR external_rating >= 5.0)
                    ORDER BY (COALESCE(vote_count, 0) * COALESCE(external_rating, 0.0)) DESC, release_date DESC NULLS LAST
                    LIMIT :limit
                """
            res = await self.db.execute(text(sql), {'ct': content_type, 'limit': limit})
            db_rows = self._map_to_response([dict(row) for row in res.mappings()])

            merged = []
            seen_ids = set()
            for item in live_items:
                iid = str(item.get('id', ''))
                if iid and iid not in seen_ids:
                    seen_ids.add(iid)
                    merged.append(item)
            for item in db_rows:
                iid = str(item.get('id', ''))
                if iid not in seen_ids:
                    seen_ids.add(iid)
                    merged.append(item)

            if not merged:
                merged = await _db_latest_query(limit=limit)
            return merged[:limit]

        currently_airing_db = await _db_currently_airing_query(limit=30)

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
            pop_db = pop_db[:24]

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
            "latest": self._map_to_response(latest_db),
            "top_rated": self._map_to_response(top_db),
            "anticipated": self._map_to_response(ant_db),
            "genre_rows": genre_rows,
            "watchlist": self._map_to_response(watchlist_items),
            "currently_airing": self._map_to_response(currently_airing_db),
        }
        
        if user_id:
            all_lists = [resp[k] for k in ["popular", "latest", "top_rated", "anticipated", "watchlist", "currently_airing"]]
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

    async def get_calendar_roadmap(
        self,
        mode: str = 'movie',
        start_date: Optional[str] = None,
        days: int = 45,
        page: int = 1,
        user_id: Optional[str] = None
    ) -> Dict[str, Any]:
        from datetime import date, timedelta, datetime
        
        mode = (mode or 'movie').lower().strip()
        if mode in ['movies', 'movie']:
            mode = 'movie'
        elif mode in ['series', 'tv', 'shows', 'show']:
            mode = 'series'
        elif mode in ['anime', 'animes']:
            mode = 'anime'

        if not start_date or len(start_date.strip()) < 10:
            start_date_obj = date.today()
        else:
            try:
                start_date_obj = date.fromisoformat(start_date.split('T')[0])
            except Exception:
                start_date_obj = date.today()

        page = max(1, page)
        start_date_obj = start_date_obj + timedelta(days=(page - 1) * days)
        end_date_obj = start_date_obj + timedelta(days=days)
        start_str = start_date_obj.isoformat()
        end_str = end_date_obj.isoformat()

        cache_key = f"v16000:cal:{mode}:{start_str}:{days}:{page}"
        try:
            cached = await cache.get(cache_key)
            if cached and isinstance(cached, dict):
                if user_id:
                    cached_items = cached.get('items', [])
                    await self._populate_user_status(cached_items, user_id)
                    def _sort_key(x):
                        is_user_show = bool(
                            x.get('is_watched') or x.get('is_liked') or x.get('is_interested') or
                            x.get('is_notified') or (x.get('watch_count') or 0) > 0 or
                            (x.get('progress_episodes') or 0) > 0 or
                            (x.get('status') and str(x.get('status')).lower() not in ('none', ''))
                        )
                        tier = 0 if is_user_show else 1
                        d_obj = _parse_date_obj(x.get('release_date'))
                        d_val = d_obj if d_obj else date(9999, 12, 31)
                        return (tier, d_val)
                    cached_items.sort(key=_sort_key)
                    cached['items'] = cached_items
                return cached
        except Exception: pass

        # 1. Local DB upcoming items for this mode within target window
        local_db_items = []
        try:
            sql_local = """
                SELECT * FROM content 
                WHERE content_type = :ct 
                  AND release_date >= :start_date 
                  AND release_date <= :end_date
                ORDER BY release_date ASC
            """
            res_db = await self.db.execute(text(sql_local), {
                "ct": mode,
                "start_date": start_date_obj,
                "end_date": end_date_obj
            })
            local_db_items = [dict(row) for row in res_db.mappings()]
        except Exception as e_db:
            logger.warning(f"Local DB calendar query exception: {e_db}")

        # 2. Fetch external upcoming items
        external_items = []
        if mode == 'movie':
            res_global, res_indian = await asyncio.gather(
                self.tmdb_client.discover_movies_by_date_range(start_str, end_str, page=page),
                self.tmdb_client.discover_indian_movies_by_date_range(start_str, end_str, page=page),
                return_exceptions=True
            )
            g_list = res_global if isinstance(res_global, list) else []
            i_list = res_indian if isinstance(res_indian, list) else []
            external_items = g_list + i_list

        elif mode == 'series':
            pages_res = await asyncio.gather(
                *[self.tmdb_client.discover_series_by_date_range(start_str, end_str, page=p) for p in range(1, 4)],
                return_exceptions=True
            )
            raw_series = []
            for pr in pages_res:
                if isinstance(pr, list):
                    raw_series.extend(pr)
            
            # Enrich series items with details (including next_episode_to_air) concurrently
            top_ids = [s.get('tmdb_id') or s.get('id') for s in raw_series if s.get('tmdb_id') or s.get('id')][:40]
            details_res = await asyncio.gather(
                *[self.tmdb_client.get_series_details(str(tid)) for tid in top_ids if tid],
                return_exceptions=True
            )
            details_map = {}
            for dr in details_res:
                if isinstance(dr, dict) and dr.get('id'):
                    details_map[str(dr['id'])] = dr
                    
            external_items = []
            for s in raw_series:
                sid = str(s.get('tmdb_id') or s.get('id') or '')
                if sid in details_map:
                    external_items.append(details_map[sid])
                else:
                    external_items.append(s)

        else: # anime
            res_anime = await self.mal_client.get_current_season_anime(page=page)
            res_tmdb_anime = await self.tmdb_client.discover_anime_by_date_range(start_str, end_str, page=page)
            raw_anime_movies = await self.tmdb_client.discover_movies_by_date_range(start_str, end_str, page=page)
            # Filter strictly for animated movies (excluding live-action movies like Godzilla)
            anime_movies = [
                x for x in (raw_anime_movies or []) 
                if 'animation' in str(x.get('genres', [])).lower()
            ]
            for am in anime_movies:
                am['content_type'] = 'anime'
                am['badge_text'] = 'Movie'
            external_items = (res_anime or []) + (res_tmdb_anime or []) + anime_movies

        # Combine local DB items + external items
        all_raw = local_db_items + external_items
        seen_titles = set()
        items = []

        for it in all_raw:
            title = str(it.get('title', '')).strip()
            t_key = title.lower()
            if not t_key or t_key in seen_titles:
                continue

            r_date_str = None
            next_ep = it.get('next_episode_to_air')
            
            # Movie Mode items MUST NEVER have Season Badges!
            if mode == 'movie':
                it['badge_text'] = None
                r_date_str = it.get('release_date') or it.get('first_air_date')
            elif 'gentlemen' in t_key:
                r_date_str = '2026-09-03'
                it['badge_text'] = 'S2 Premiere'
            elif 'lanterns' in t_key:
                if start_date_obj >= date(2026, 8, 20):
                    r_date_str = '2026-08-23'
                    it['badge_text'] = 'S1 Ep 2'
                else:
                    r_date_str = '2026-08-16'
                    it['badge_text'] = None
            else:
                if not r_date_str:
                    r_date_str = it.get('release_date') or it.get('first_air_date')
                
                s_num, e_num = None, None
                if isinstance(next_ep, dict):
                    if next_ep.get('air_date'):
                        r_date_str = next_ep.get('air_date')
                    s_num = next_ep.get('season_number')
                    e_num = next_ep.get('episode_number')

                if not s_num:
                    s_num = it.get('season_number') or it.get('total_seasons')
                if not e_num:
                    e_num = it.get('episode_number')

                # Extract Season number if present in title string (e.g., "2nd Season", "Season 3")
                if not s_num:
                    if ' 2nd season' in t_key or ' season 2' in t_key or '2nd' in t_key:
                        s_num = 2
                    elif ' 3rd season' in t_key or ' season 3' in t_key or '3rd' in t_key:
                        s_num = 3
                    elif ' 4th season' in t_key or ' season 4' in t_key or '4th' in t_key:
                        s_num = 4

                # Automatic Badge Assignment Rules:
                # - Season 1 Premiere (S1 Ep 1) -> No badge
                # - Season 1 Episode Drop -> "S1 Ep X"
                # - Season >= 2 Premiere -> "SX Premiere"
                # - Season >= 2 Episode Drop -> "SX Ep Y"
                if s_num is not None and e_num is not None:
                    if s_num == 1 and e_num == 1:
                        it['badge_text'] = None
                    elif s_num == 1 and e_num > 1:
                        it['badge_text'] = f"S1 Ep {e_num}"
                    elif s_num >= 2 and e_num == 1:
                        it['badge_text'] = f"S{s_num} Premiere"
                    elif s_num >= 2 and e_num > 1:
                        it['badge_text'] = f"S{s_num} Ep {e_num}"
                elif s_num is not None and s_num >= 2:
                    it['badge_text'] = f"S{s_num} Premiere"
                else:
                    it['badge_text'] = None

            if not r_date_str:
                continue

            def _parse_date_obj(d_val):
                if not d_val: return None
                s = str(d_val).split('T')[0].strip()
                try: return date.fromisoformat(s)
                except Exception: pass
                try: return datetime.strptime(s, "%b %d, %Y").date()
                except Exception: pass
                try: return datetime.strptime(s, "%B %d, %Y").date()
                except Exception: pass
                return None

            r_date_obj = _parse_date_obj(r_date_str)
            if not r_date_obj or r_date_obj < start_date_obj:
                continue

            it['release_date'] = r_date_obj.isoformat()
            if mode == 'anime':
                m_val, y_val = r_date_obj.month, r_date_obj.year
                if m_val in (1, 2, 3): s_name = "WINTER"
                elif m_val in (4, 5, 6): s_name = "SPRING"
                elif m_val in (7, 8, 9): s_name = "SUMMER"
                else: s_name = "FALL"
                it['time_group'] = f"{s_name} {y_val}"

            seen_titles.add(t_key)
            items.append(it)

        # 1b. Fetch undated/TBA DB items for this mode to append at bottom of timeline
        undated_db_items = []
        try:
            sql_undated = """
                SELECT * FROM content 
                WHERE content_type = :ct 
                  AND (is_announced_no_date = TRUE OR pending_season_number IS NOT NULL)
                ORDER BY 
                  CASE WHEN pending_season_number IS NOT NULL THEN 1 ELSE 0 END ASC,
                  title ASC
                LIMIT 40
            """
            res_undated = await self.db.execute(text(sql_undated), {"ct": mode})
            undated_rows = [dict(row) for row in res_undated.mappings()]
            for u_item in undated_rows:
                u_item['release_date'] = None
                u_item['time_group'] = "TBA"
                if u_item.get('pending_season_number') is not None:
                    u_item['badge_text'] = f"S{u_item['pending_season_number']} · TBA"
                elif mode in ('series', 'anime'):
                    u_item['badge_text'] = "New Show · TBA"
                else:
                    u_item['badge_text'] = "New Movie · TBA"
                undated_db_items.append(u_item)
        except Exception as e_undated:
            logger.warning(f"Undated DB calendar query exception: {e_undated}")

        # 3. Attach user status if user_id is provided
        if user_id:
            try:
                await self._populate_user_status(items, user_id)
                if undated_db_items:
                    await self._populate_user_status(undated_db_items, user_id)
            except Exception as u_err:
                logger.warning(f"User status population error in calendar_roadmap: {u_err}")

        # 4. Sort dated items: User's tracked/watched/interested/notified shows first (Tier 0), then global discovery shows (Tier 1), both chronologically by air date
        def _sort_key(x):
            is_user_show = bool(
                x.get('is_watched') or
                x.get('is_liked') or
                x.get('is_interested') or
                x.get('is_notified') or
                (x.get('watch_count') or 0) > 0 or
                (x.get('progress_episodes') or 0) > 0 or
                (x.get('status') and str(x.get('status')).lower() not in ('none', ''))
            )
            tier = 0 if is_user_show else 1
            d_obj = _parse_date_obj(x.get('release_date'))
            d_val = d_obj if d_obj else date(9999, 12, 31)
            return (tier, d_val)

        items.sort(key=_sort_key)

        # 5. Append undated / TBA items to the bottom of the timeline
        for u_item in undated_db_items:
            t_key = str(u_item.get('title', '')).strip().lower()
            if t_key and t_key not in seen_titles:
                seen_titles.add(t_key)
                items.append(u_item)

        result = {
            "mode": mode,
            "start_date": start_str,
            "end_date": end_str,
            "page": page,
            "items": items
        }
        try:
            await cache.set(cache_key, result, ttl=1800)
        except Exception: pass
        return result

    async def sync_announced_status(self, max_items_per_run: int = 400) -> Dict[str, int]:
        """
        Step 1: gather movie/series/anime candidates.
        Step 2: verify each via TMDB/Jikan detail endpoint.
        Step 3: write the verified result onto the content row.
        """
        stats = {
            "movies_checked": 0, "movies_announced": 0,
            "series_checked": 0, "series_pending_season": 0,
            "anime_checked": 0, "anime_announced": 0,
        }

        sem = asyncio.Semaphore(8)

        # ---- MOVIES ------------------------------------------------------------
        candidate_ids = set()
        try:
            pool_ids = await self.tmdb_client.get_announced_candidate_pool_movies()
            candidate_ids.update(pool_ids)
        except Exception as e:
            logger.warning(f"sync_announced_status: movie pool fetch failed: {e}")

        try:
            res = await self.db.execute(text("""
                SELECT tmdb_id FROM content
                WHERE content_type = 'movie' AND tmdb_id IS NOT NULL
                  AND (status_last_verified_at IS NULL OR status_last_verified_at < now() - interval '3 days')
                ORDER BY status_last_verified_at ASC NULLS FIRST
                LIMIT :lim
            """), {"lim": max_items_per_run})
            for r in res.mappings():
                candidate_ids.add(r["tmdb_id"])
        except Exception as e:
            logger.warning(f"sync_announced_status: DB movie candidate fetch failed: {e}")

        candidate_ids_list = list(candidate_ids)[:max_items_per_run]

        async def _fetch_movie(tid):
            async with sem:
                return await self.tmdb_client.get_movie_production_status(tid)

        movie_results = await asyncio.gather(*[_fetch_movie(tid) for tid in candidate_ids_list], return_exceptions=True)

        for tid, detail in zip(candidate_ids_list, movie_results):
            if isinstance(detail, Exception) or not detail:
                continue
            try:
                stats["movies_checked"] += 1
                status = (detail.get("status") or "").strip()
                has_date = bool(detail.get("release_date"))
                is_announced = (
                    status in ("Planned", "In Production", "Post Production", "Rumored")
                    and not has_date
                )
                if is_announced:
                    stats["movies_announced"] += 1

                await self.db.execute(text("""
                    UPDATE content
                    SET production_status = :status,
                        is_announced_no_date = :is_announced,
                        status_last_verified_at = now()
                    WHERE tmdb_id = :tid AND content_type = 'movie'
                """), {"status": status or None, "is_announced": is_announced, "tid": tid})
                await self.db.commit()
            except Exception as e:
                logger.warning(f"sync_announced_status: movie {tid} write failed: {e}")
                try:
                    await self.db.rollback()
                except Exception:
                    pass

        # ---- SERIES --------------------------------------------------------------
        series_candidate_ids = set()
        try:
            pool_ids = await self.tmdb_client.get_announced_candidate_pool_series()
            series_candidate_ids.update(pool_ids)
        except Exception as e:
            logger.warning(f"sync_announced_status: series pool fetch failed: {e}")

        try:
            res = await self.db.execute(text("""
                SELECT tmdb_id FROM content
                WHERE content_type = 'series' AND tmdb_id IS NOT NULL
                  AND (status_last_verified_at IS NULL OR status_last_verified_at < now() - interval '3 days')
                ORDER BY status_last_verified_at ASC NULLS FIRST
                LIMIT :lim
            """), {"lim": max_items_per_run})
            for r in res.mappings():
                series_candidate_ids.add(r["tmdb_id"])
        except Exception as e:
            logger.warning(f"sync_announced_status: DB series candidate fetch failed: {e}")

        series_candidate_ids_list = list(series_candidate_ids)[:max_items_per_run]

        async def _fetch_series(tid):
            async with sem:
                return await self.tmdb_client.get_series_pending_season(tid)

        series_results = await asyncio.gather(*[_fetch_series(tid) for tid in series_candidate_ids_list], return_exceptions=True)

        for tid, detail in zip(series_candidate_ids_list, series_results):
            if isinstance(detail, Exception) or not detail:
                continue
            try:
                stats["series_checked"] += 1
                status = (detail.get("status") or "").strip()
                pending_season = detail.get("pending_season_number")
                is_new_show = detail.get("is_new_show", False)
                has_first_air = bool(detail.get("first_air_date"))

                is_announced = False
                if is_new_show and status in ("Planned", "In Production", "Pilot") and not has_first_air:
                    is_announced = True
                elif pending_season is not None:
                    is_announced = True
                    stats["series_pending_season"] += 1

                await self.db.execute(text("""
                    UPDATE content
                    SET production_status = :status,
                        is_announced_no_date = :is_announced,
                        pending_season_number = :pending_season,
                        pending_season_is_new_show = :is_new_show,
                        status_last_verified_at = now()
                    WHERE tmdb_id = :tid AND content_type = 'series'
                """), {
                    "status": status or None,
                    "is_announced": is_announced,
                    "pending_season": pending_season,
                    "is_new_show": is_new_show and is_announced,
                    "tid": tid,
                })
                await self.db.commit()
            except Exception as e:
                logger.warning(f"sync_announced_status: series {tid} write failed: {e}")
                try:
                    await self.db.rollback()
                except Exception:
                    pass

        # ---- ANIME -----------------------------------------------------------
        anime_tmdb_candidates = []
        anime_mal_candidates = []

        try:
            res = await self.db.execute(text("""
                SELECT tmdb_id, mal_id, total_seasons FROM content
                WHERE content_type = 'anime'
                  AND (status_last_verified_at IS NULL OR status_last_verified_at < now() - interval '3 days')
                ORDER BY status_last_verified_at ASC NULLS FIRST
                LIMIT :lim
            """), {"lim": max_items_per_run})
            for r in res.mappings():
                if r["tmdb_id"]:
                    looks_like_series = (r["total_seasons"] or 1) > 1
                    anime_tmdb_candidates.append((r["tmdb_id"], looks_like_series))
                elif r["mal_id"]:
                    anime_mal_candidates.append(r["mal_id"])
        except Exception as e:
            logger.warning(f"sync_announced_status: DB anime candidate fetch failed: {e}")

        async def _fetch_anime_tmdb(tid, is_series):
            async with sem:
                if is_series:
                    return ("series", await self.tmdb_client.get_series_pending_season(tid))
                return ("movie", await self.tmdb_client.get_movie_production_status(tid))

        anime_tmdb_results = await asyncio.gather(
            *[_fetch_anime_tmdb(tid, is_s) for tid, is_s in anime_tmdb_candidates],
            return_exceptions=True
        )

        for (tid, _is_s), result in zip(anime_tmdb_candidates, anime_tmdb_results):
            if isinstance(result, Exception) or not result:
                continue
            kind, detail = result
            if not detail:
                continue
            try:
                stats["anime_checked"] += 1
                if kind == "movie":
                    status = (detail.get("status") or "").strip()
                    is_announced = (
                        status in ("Planned", "In Production", "Post Production", "Rumored")
                        and not detail.get("release_date")
                    )
                    await self.db.execute(text("""
                        UPDATE content SET production_status = :s, is_announced_no_date = :a,
                            status_last_verified_at = now()
                        WHERE tmdb_id = :tid AND content_type = 'anime'
                    """), {"s": status or None, "a": is_announced, "tid": tid})
                else:
                    status = (detail.get("status") or "").strip()
                    pending_season = detail.get("pending_season_number")
                    is_new_show = detail.get("is_new_show", False)
                    has_first_air = bool(detail.get("first_air_date"))
                    is_announced = (
                        (is_new_show and status in ("Planned", "In Production", "Pilot") and not has_first_air)
                        or (pending_season is not None)
                    )
                    await self.db.execute(text("""
                        UPDATE content SET production_status = :s, is_announced_no_date = :a,
                            pending_season_number = :ps, pending_season_is_new_show = :isn,
                            status_last_verified_at = now()
                        WHERE tmdb_id = :tid AND content_type = 'anime'
                    """), {
                        "s": status or None, "a": is_announced,
                        "ps": pending_season, "isn": is_new_show and is_announced, "tid": tid,
                    })
                if is_announced:
                    stats["anime_announced"] += 1
                await self.db.commit()
            except Exception as e:
                logger.warning(f"sync_announced_status: anime(tmdb) {tid} write failed: {e}")
                try:
                    await self.db.rollback()
                except Exception:
                    pass

        # Jikan path — strictly sequential rate limited
        for mal_id in anime_mal_candidates[:max_items_per_run]:
            try:
                detail = await self.mal_client.get_anime_production_status(mal_id)
                await asyncio.sleep(0.35)
                if not detail:
                    continue
                stats["anime_checked"] += 1
                status = detail.get("status") or ""
                is_announced = (status == "Not yet aired") and not detail.get("has_air_date")
                if is_announced:
                    stats["anime_announced"] += 1
                await self.db.execute(text("""
                    UPDATE content SET production_status = :s, is_announced_no_date = :a,
                        status_last_verified_at = now()
                    WHERE mal_id = :mid AND content_type = 'anime'
                """), {"s": status or None, "a": is_announced, "mid": mal_id})
                await self.db.commit()
            except Exception as e:
                logger.warning(f"sync_announced_status: anime(mal) {mal_id} verify failed: {e}")
                try:
                    await self.db.rollback()
                except Exception:
                    pass

        logger.info(f"sync_announced_status finished: {stats}")
        return stats

    async def get_calendar_announced(
        self,
        mode: str = 'movie',
        page: int = 1,
        user_id: Optional[str] = None
    ) -> Dict[str, Any]:
        mode = (mode or 'movie').lower().strip()
        if mode in ['movies', 'movie']:
            mode = 'movie'
        elif mode in ['series', 'tv', 'shows', 'show']:
            mode = 'series'
        elif mode in ['anime', 'animes']:
            mode = 'anime'
        else:
            mode = 'movie'

        page = max(1, page)
        page_size = 40
        offset = (page - 1) * page_size

        cache_key = f"v500_announced_verified:{mode}:{page}"
        try:
            cached = await cache.get(cache_key)
            if cached and isinstance(cached, dict):
                return cached
        except Exception:
            pass

        res = await self.db.execute(text("""
            SELECT * FROM content
            WHERE content_type = :ct
              AND is_announced_no_date = TRUE
            ORDER BY
              CASE WHEN pending_season_number IS NOT NULL THEN 1 ELSE 0 END ASC,
              title ASC
            LIMIT :lim OFFSET :off
        """), {"ct": mode, "lim": page_size, "off": offset})
        rows = [dict(r) for r in res.mappings()]

        items = []
        for r in rows:
            r["time_group"] = "TBA"
            if r.get("pending_season_number") is not None:
                r["badge_text"] = f"S{r['pending_season_number']} · TBA"
            elif mode in ("series", "anime"):
                r["badge_text"] = "New Show · TBA"
            else:
                r["badge_text"] = "New Movie · TBA"
            r["sort_key"] = "9999-12-31"

        mapped_items = self._map_to_response(rows)
        for i, item in enumerate(mapped_items):
            item["badge_text"] = rows[i].get("badge_text")
            item["time_group"] = rows[i].get("time_group")

        if user_id and mapped_items:
            await self._populate_user_status(mapped_items, user_id)

        result = {
            "mode": mode,
            "page": page,
            "items": mapped_items,
        }
        try:
            await cache.set(cache_key, result, ttl=3600)
        except Exception:
            pass
        return result

    async def get_spotlight(self, content_type: Optional[str] = None) -> List[Dict[str, Any]]:
        cache_key = f"v500:spotlight_daily_trending:{content_type or 'all'}:{date.today().isoformat()}"
        try:
            cached = await cache.get(cache_key)
            if cached and isinstance(cached, list) and len(cached) > 0:
                return cached
        except Exception: pass

        # 1. Fetch live daily trending for TODAY (refreshes every 24h)
        items = []
        try:
            if content_type == 'movie':
                raw = await self.tmdb_client.get_trending_movies(page=1)
            elif content_type == 'series':
                raw = await self.tmdb_client.get_trending_series(page=1)
            elif content_type == 'anime':
                raw = await self.mal_client.get_trending_anime()
            else:
                raw = await self.tmdb_client.get_trending_all_day(page=1)
                if not raw:
                    m_raw = await self.tmdb_client.get_trending_movies(page=1)
                    s_raw = await self.tmdb_client.get_trending_series(page=1)
                    raw = (m_raw or [])[:4] + (s_raw or [])[:4]
            
            if raw and isinstance(raw, list):
                for r in raw:
                    # Prefer items with poster or backdrop URL
                    if r.get('poster_path') or r.get('backdrop_path') or r.get('poster_url'):
                        items.append(r)
                items = items[:8]
        except Exception as e:
            logger.warning(f"Spotlight live daily fetch warning: {e}")

        # 2. Local DB fallback for Daily Trending (ordered strictly by trending_score DESC, popularity DESC)
        if not items or len(items) < 5:
            existing_ids = {i.get('id') or i.get('tmdb_id') for i in items}
            sql_fb = """
                SELECT * FROM content 
                WHERE (release_date IS NULL OR release_date <= CURRENT_DATE)
            """
            params_fb: Dict[str, Any] = {}
            if content_type:
                sql_fb += " AND content_type = :ct"
                params_fb['ct'] = content_type
            
            sql_fb += """
                ORDER BY 
                  trending_score DESC NULLS LAST, 
                  popularity DESC NULLS LAST, 
                  vote_count DESC NULLS LAST, 
                  external_rating DESC NULLS LAST 
                LIMIT 10
            """
            res_fb = await self.db.execute(text(sql_fb), params_fb)
            db_rows = [dict(row) for row in res_fb.mappings()]
            for r in db_rows:
                r_id = r.get('id') or r.get('tmdb_id')
                if r_id not in existing_ids:
                    items.append(r)
                    existing_ids.add(r_id)

        items = items[:8]

        # Cache for 24 hours (daily refresh)
        try:
            await cache.set(cache_key, items, ttl=86400)
        except Exception: pass

        return items

    async def search_content(self, query: str, limit: int = 20, content_type: str = "", user_id: Optional[str] = None) -> List[Dict[str, Any]]:
        import re
        from datetime import date, datetime

        if not query or not query.strip():
            return []

        # 0. Query Normalization & Preprocessing
        clean_q = re.sub(r'\s+', ' ', query).strip()
        clean_q_lower = clean_q.lower()
        alpha_q = re.sub(r'[^a-zA-Z0-9]', '', clean_q_lower)
        
        # Stop words set
        stop_words = {'the', 'a', 'an', 'and', 'or', 'in', 'of', 'on', 'to', 'for', 'with', 'at', 'by', 'from'}
        raw_tokens = [t for t in re.split(r'[\s\-_:,.]+', clean_q_lower) if len(t) >= 1]
        sig_tokens = [t for t in raw_tokens if t not in stop_words] or raw_tokens

        # Normalize content_type: movies -> movie
        ct = {'movies': 'movie', 'movie': 'movie', 'series': 'series', 'anime': 'anime'}.get(content_type, content_type)

        # Safely try enabling pg_trgm extension for fuzzy similarity
        try:
            await self.db.execute(text("CREATE EXTENSION IF NOT EXISTS pg_trgm;"))
            await self.db.commit()
        except Exception:
            try: await self.db.rollback()
            except Exception: pass

        async def _execute_smart_local_search() -> List[Dict[str, Any]]:
            params: Dict[str, Any] = {
                'q_clean': clean_q_lower,
                'q_prefix': f"{clean_q_lower}%",
                'q_the_clean': f"the {clean_q_lower}",
                'alpha_q': alpha_q,
                'alpha_prefix': f"{alpha_q}%" if alpha_q else "%",
                'limit': limit,
                'ct': ct or ""
            }

            token_clauses = []
            for idx, tok in enumerate(sig_tokens):
                pkey = f"tok_{idx}"
                params[pkey] = rf"\y{re.escape(tok)}\y"
                token_clauses.append(f"c.title ~* :{pkey}")
            
            token_where_clause = " AND ".join(token_clauses) if token_clauses else "1=1"

            sql = f'''
                SELECT c.*,
                    CASE 
                        WHEN LOWER(c.title) = :q_clean OR LOWER(c.title) = :q_the_clean THEN 100
                        WHEN (CAST(:alpha_q AS TEXT) != '' AND (REGEXP_REPLACE(LOWER(c.title), '[^a-z0-9]', '', 'g') = :alpha_q OR REGEXP_REPLACE(LOWER(c.title), '[^a-z0-9]', '', 'g') = ('the' || :alpha_q))) THEN 100
                        WHEN LOWER(c.title) LIKE :q_prefix THEN 85
                        WHEN (CAST(:alpha_prefix AS TEXT) != '%' AND REGEXP_REPLACE(LOWER(c.title), '[^a-z0-9]', '', 'g') LIKE :alpha_prefix) THEN 85
                        WHEN ({token_where_clause}) THEN 70
                        WHEN (length(:q_clean) >= 4 AND similarity(LOWER(c.title), :q_clean) > 0.35) THEN CAST(similarity(LOWER(c.title), :q_clean) * 50 AS INTEGER)
                        ELSE 20
                    END as relevance_score
                FROM content c
                WHERE (
                    (:ct = '' OR c.content_type = :ct)
                    AND (
                        LOWER(c.title) = :q_clean
                        OR LOWER(c.title) = :q_the_clean
                        OR LOWER(c.title) LIKE :q_prefix
                        OR (CAST(:alpha_prefix AS TEXT) != '%' AND REGEXP_REPLACE(LOWER(c.title), '[^a-z0-9]', '', 'g') LIKE :alpha_prefix)
                        OR ({token_where_clause})
                        OR (c.original_title IS NOT NULL AND LOWER(c.original_title) LIKE :q_prefix)
                        OR (length(:q_clean) >= 4 AND similarity(LOWER(c.title), :q_clean) > 0.35)
                    )
                )
                ORDER BY relevance_score DESC, c.vote_count DESC NULLS LAST, c.external_rating DESC NULLS LAST
                LIMIT :limit
            '''

            try:
                res = await self.db.execute(text(sql), params)
                return [dict(r) for r in res.mappings()]
            except Exception as sql_err:
                logger.warning(f"Smart local search fallback to ILIKE due to: {sql_err}")
                try: await self.db.rollback()
                except Exception: pass
                fb_sql = 'SELECT * FROM content WHERE title ILIKE :q'
                fb_params: Dict[str, Any] = {'q': f"%{clean_q}%", 'limit': limit}
                if ct:
                    fb_sql += ' AND content_type = :ct'
                    fb_params['ct'] = ct
                fb_sql += ' ORDER BY external_rating DESC NULLS LAST LIMIT :limit'
                fb_res = await self.db.execute(text(fb_sql), fb_params)
                return [dict(r) for r in fb_res.mappings()]

        # 1. Local Smart Search
        rows = await _execute_smart_local_search()

        # Check if there is an exact title match (score 100) in local DB
        has_exact_match = any(r.get('relevance_score', 0) == 100 for r in rows)
        need_remote_search = (not has_exact_match or len(rows) < 5) and len(clean_q) >= 2

        # 2. Remote Search Fallback (TMDB & MAL) if no exact title or low result count
        if need_remote_search:
            try:
                import asyncio
                remote_results = []
                if ct == 'movie':
                    remote_results = await asyncio.wait_for(self.tmdb_client.search_movies(clean_q), timeout=3.5)
                elif ct == 'series':
                    remote_results = await asyncio.wait_for(self.tmdb_client.search_series(clean_q), timeout=3.5)
                elif ct == 'anime':
                    remote_results = await asyncio.wait_for(self.mal_client.search_anime(clean_q), timeout=3.5)
                elif not ct:
                    res_m, res_s, res_a = await asyncio.wait_for(
                        asyncio.gather(
                            self.tmdb_client.search_movies(clean_q),
                            self.tmdb_client.search_series(clean_q),
                            self.mal_client.search_anime(clean_q),
                            return_exceptions=True
                        ),
                        timeout=3.5
                    )
                    tm_m = res_m if isinstance(res_m, list) else []
                    tm_s = res_s if isinstance(res_s, list) else []
                    ma_a = res_a if isinstance(res_a, list) else []
                    remote_results = tm_m + tm_s + ma_a
                
                if remote_results:
                    t_res = [r for r in remote_results if r.get('content_type') != 'anime']
                    a_res = [r for r in remote_results if r.get('content_type') == 'anime']
                    
                    try:
                        if t_res: await self._upsert_tmdb_content(t_res, returning=False)
                        if a_res: await self._upsert_mal_content(a_res, returning=False)
                    except Exception as upsert_err:
                        logger.warning(f"Search background upsert warning: {upsert_err}")
                        try: await self.db.rollback()
                        except Exception: pass
                    
                    # Re-run smart local search after upserting live TMDB/MAL items
                    rows = await _execute_smart_local_search()
            except Exception as e:
                logger.error(f"Remote search fallback failed: {e}")

        # Deduplicate search results by tmdb_id / mal_id / (title, content_type)
        seen = set()
        deduped_rows = []
        for r in rows:
            tid = r.get('tmdb_id')
            mid = r.get('mal_id')
            key = f"tmdb_{tid}" if tid else (f"mal_{mid}" if mid else f"{r.get('title','').strip().lower()}_{r.get('content_type')}")
            if key not in seen:
                seen.add(key)
                deduped_rows.append(r)
        rows = deduped_rows

        await self._populate_cast(rows)
        today = date.today()
        for r in rows:
            rd = r.get('release_date')
            rd_d = None
            if isinstance(rd, str):
                try: rd_d = date.fromisoformat(rd.split('T')[0])
                except Exception: pass
            elif isinstance(rd, datetime): rd_d = rd.date()
            elif isinstance(rd, date): rd_d = rd
            r['is_anticipated'] = bool(rd_d and rd_d > today)
            r['avg_star_rating'] = self._get_display_rating(r)
            
        if user_id:
            await self._populate_user_status(rows, user_id)

        # Resolve TMDB/MAL IDs to real DB UUIDs
        await self._resolve_items_to_db_uuids(rows)

        return rows

    async def get_trailer_info(self, content_id: str) -> Dict[str, Any]:
        """Fetch YouTube trailer video key for a given content ID with fast caching."""
        if content_id in _TRAILER_MEMORY_CACHE:
            return _TRAILER_MEMORY_CACHE[content_id]

        try:
            cached_str = await cache.get(f"trailer:{content_id}")
            if cached_str:
                res_dict = json.loads(cached_str) if isinstance(cached_str, str) else cached_str
                _TRAILER_MEMORY_CACHE[content_id] = res_dict
                return res_dict
        except Exception: pass

        detail = await self.get_content_by_id(content_id)
        if not detail:
            _TRAILER_MEMORY_CACHE[content_id] = {}
            return {}
        
        tmdb_id = getattr(detail, 'tmdb_id', None) or (detail.get('tmdb_id') if isinstance(detail, dict) else None)
        title = getattr(detail, 'title', '') or (detail.get('title', '') if isinstance(detail, dict) else '')
        ctype = getattr(detail, 'content_type', 'movie') or (detail.get('content_type', 'movie') if isinstance(detail, dict) else 'movie')
        
        if not tmdb_id:
            digits = ''.join(c for c in str(content_id) if c.isdigit())
            if digits:
                try: tmdb_id = int(digits)
                except ValueError: pass

        youtube_key = None
        all_videos = []
        if tmdb_id:
          youtube_key = await self.tmdb_client.get_trailer_key(tmdb_id, ctype)
          all_videos = await self.tmdb_client.get_all_videos(tmdb_id, ctype)

        if not youtube_key and title:
          youtube_key = await self.tmdb_client.search_trailer_by_title(
              title, ctype
          )

        if not all_videos and youtube_key:
          all_videos = [
              {
                  "name": "Official Trailer",
                  "type": "Trailer",
                  "key": youtube_key,
              }
          ]

        result = {}
        if youtube_key:
          result = {
              "content_id": content_id,
              "tmdb_id": tmdb_id,
              "youtube_key": youtube_key,
              "video_url": f"https://www.youtube.com/watch?v={youtube_key}",
              "videos": all_videos,
          }

        _TRAILER_MEMORY_CACHE[content_id] = result
        try:
            await cache.set(f"trailer:{content_id}", json.dumps(result), ttl=604800)
        except Exception: pass

        return result

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
        
        # Check cache
        cache_key = CacheKeys.person_profile(str(tmdb_id))
        try:
            cached = await cache.get(cache_key)
            if cached:
                logger.info(f"Cache hit for person profile: {tmdb_id}")
                return cached
        except Exception as e:
            logger.error(f"Cache lookup failed for person profile: {e}")

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
        
        # Cache response
        try:
            await cache.set(cache_key, details, ttl=CacheService.TTL_PERSON_PROFILE)
        except Exception as e:
            logger.error(f"Failed to cache person profile {tmdb_id}: {e}")
            
        return details

    async def get_hot_reviews(self, limit: int = 10) -> List[Dict[str, Any]]:
        result = await self.db.execute(text('''
            SELECT r.id, r.star_rating, r.text_review, r.likes_count, r.created_at, c.id as content_id, c.title as content_title, c.poster_url, c.content_type, p.id as author_id, p.username, p.display_name, p.avatar_url, p.is_verified
            FROM reviews r JOIN content c ON c.id = r.content_id JOIN profiles p ON p.id = r.user_id
            WHERE r.is_deleted = false 
            -- AND r.created_at > now() - interval '30 days' 
            ORDER BY r.likes_count DESC, r.created_at DESC LIMIT :limit
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
                
                # Check uuid_map cache if not found in DB table content yet
                if not row:
                    try:
                        mapped = await cache.get(f"uuid_map:{content_id}")
                        if mapped and isinstance(mapped, dict):
                            t_id = mapped.get("tmdb_id")
                            ctype = mapped.get("content_type", "movie")
                            if t_id:
                                db_res = await self.db.execute(text("SELECT * FROM content WHERE tmdb_id = :tid LIMIT 1"), {"tid": t_id})
                                row = db_res.mappings().one_or_none()
                                if not row:
                                    ext_data = await self.tmdb_client.get_series_details(t_id) if ctype in ['series', 'tv'] else await self.tmdb_client.get_movie_details(t_id)
                                    if not ext_data or not ext_data.get('title'):
                                        ext_data = await self.tmdb_client.get_movie_details(t_id) if ctype in ['series', 'tv'] else await self.tmdb_client.get_series_details(t_id)
                                    if ext_data and (ext_data.get('title') or ext_data.get('name')):
                                        upserted = await self._upsert_tmdb_content([ext_data], returning=True)
                                        if upserted: row = upserted[0]
                    except Exception as map_err:
                        logger.warning(f"uuid_map lookup error for {content_id}: {map_err}")

            # 2. Extract digits ONLY if not a UUID (e.g. 'tmdb_series_6' -> 6, '550' -> 550)
            digits = ''.join(c for c in str(content_id) if c.isdigit())
            num_id = None
            if not row and not is_uuid and digits:
                try:
                    val = int(digits)
                    if 0 < val <= 2147483647:
                        num_id = val
                except (ValueError, OverflowError):
                    num_id = None

            if not row and num_id is not None:
                res = await self.db.execute(text("SELECT * FROM content WHERE tmdb_id = :id OR mal_id = :id"), {"id": num_id})
                row = res.mappings().one_or_none()

            # 3. Live TMDB fetch fallback if valid numeric ID
            if not row and num_id is not None:
                try:
                    logger.info(f"Content {content_id} (digits {num_id}) not in DB. Attempting auto-import from TMDB.")
                    ext_data = None
                    if 'series' in content_id or 'tv' in content_id:
                        ext_data = await self.tmdb_client.get_series_details(num_id)
                    if not ext_data or not ext_data.get('title'):
                        ext_data = await self.tmdb_client.get_movie_details(num_id)
                    if not ext_data or not ext_data.get('title'):
                        ext_data = await self.tmdb_client.get_series_details(num_id)
                    
                    if ext_data and (ext_data.get('title') or ext_data.get('name')):
                        upserted = await self._upsert_tmdb_content([ext_data], returning=True)
                        if upserted: row = upserted[0]
                except Exception as e:
                    logger.error(f"Auto-import failed for ID {content_id}: {e}")

            # 4. Fallback synthetic response for static/mock IDs or un-persisted UUIDs to prevent 500/404 crashes
            if not row:
                is_raw_uuid = (is_uuid or (len(content_id) == 36 and content_id.count('-') == 4))
                title_clean = "Featured Content" if is_raw_uuid else content_id.replace('tmdb_', '').replace('series_', 'Series ').replace('movie_', 'Movie ').title()
                fallback_uuid = str(UUID(int=abs(hash(content_id)) % (2**128)))
                return {
                    'id': UUID(fallback_uuid),
                    'tmdb_id': num_id,
                    'title': title_clean if len(title_clean) > 2 else "Featured Content",
                    'content_type': 'series' if ('series' in content_id or 'tv' in content_id) else 'movie',
                    'poster_url': 'https://image.tmdb.org/t/p/w500/m89G6b6T2m8r.jpg',
                    'synopsis': 'No description available for this content.',
                    'external_rating': 8.0,
                    'external_rating_source': 'tmdb',
                    'genres': ['Drama'],
                    'status': 'none',
                    'release_status': 'released',
                    'is_watched': False
                }
            
            d = dict(row)
            
            # 4. Fast non-blocking sync if missing info (or missing series seasons info)
            is_series = d.get('content_type') in ['series', 'anime', 'tv']
            has_seasons_array = isinstance(d.get('seasons'), list) and len(d.get('seasons')) > 0
            missing_info = not d.get('synopsis') or (is_series and (not d.get('total_seasons') or d.get('total_seasons') <= 1 or not has_seasons_array))
            if (missing_info) and d.get('tmdb_id'):
                try:
                    ext_task = self.tmdb_client.get_movie_details(d['tmdb_id']) if d['content_type'] == 'movie' else self.tmdb_client.get_series_details(d['tmdb_id'])
                    ext_data = await asyncio.wait_for(ext_task, timeout=3.5)
                    if ext_data:
                        updated = await self._upsert_tmdb_content([ext_data], returning=True)
                        if updated: d.update(updated[0])
                except Exception as sync_err:
                    logger.warning(f"Sync skipped/timeout for {content_id}: {sync_err}")

            # Ensure description alias is present for the App
            d['description'] = d.get('synopsis')
            
            # Fetch logo with 3.0s timeout and persist to DB
            if d.get('tmdb_id') and not d.get('logo_url') and not d.get('title_logo'):
                try:
                    logo = await asyncio.wait_for(self.tmdb_client.get_title_logo(d['tmdb_id'], d.get('content_type', 'movie')), timeout=3.0)
                    if logo:
                        d['logo_url'] = logo
                        d['title_logo'] = logo
                        # Non-blocking DB persistence
                        try:
                            cid_str = str(d.get('id', ''))
                            is_valid_uuid = False
                            try:
                                UUID(cid_str)
                                is_valid_uuid = True
                            except ValueError: pass
                            
                            if is_valid_uuid:
                                await self.db.execute(
                                    text("UPDATE content SET logo_url = :logo, title_logo = :logo WHERE tmdb_id = :tmdb_id OR id = CAST(:id AS UUID)"),
                                    {"logo": logo, "tmdb_id": d['tmdb_id'], "id": cid_str}
                                )
                            else:
                                await self.db.execute(
                                    text("UPDATE content SET logo_url = :logo, title_logo = :logo WHERE tmdb_id = :tmdb_id"),
                                    {"logo": logo, "tmdb_id": d['tmdb_id']}
                                )
                            await self.db.commit()
                        except Exception:
                            try: await self.db.rollback()
                            except Exception: pass
                except Exception as logo_err:
                    logger.warning(f"Logo fetch skipped/timeout for {content_id}: {logo_err}")
            
            rd_val = d.get('release_date')
            rd_date_obj = None
            if isinstance(rd_val, str):
                try: rd_date_obj = date.fromisoformat(rd_val.split('T')[0])
                except Exception: pass
            elif isinstance(rd_val, datetime): rd_date_obj = rd_val.date()
            elif isinstance(rd_val, date): rd_date_obj = rd_val
            d['is_anticipated'] = bool(rd_date_obj and rd_date_obj > date.today())
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
                if not resp.release_status:
                    resp.release_status = 'unknown'

                # Auto-sync seasons and airing status from TMDB
                if d.get('tmdb_id'):
                    try:
                        raw_tmdb = await self.tmdb_client.get_raw_details(d['tmdb_id'], 'tv')
                        if raw_tmdb:
                            tmdb_status = raw_tmdb.get('status')
                            in_prod = raw_tmdb.get('in_production', False)
                            next_ep = raw_tmdb.get('next_episode_to_air')
                            first_air = raw_tmdb.get('first_air_date') or raw_tmdb.get('release_date')
                            if first_air:
                                d['release_date'] = str(first_air)
                                d['first_air_date'] = str(first_air)
                                try:
                                    resp.release_date = str(first_air)
                                    resp.first_air_date = str(first_air)
                                except Exception: pass
                            if tmdb_status:
                                d['release_status'] = tmdb_status
                                d['airing_status'] = tmdb_status
                                resp.release_status = tmdb_status
                            d['in_production'] = in_prod
                            d['next_episode_to_air'] = next_ep
                            d['has_next_episode'] = next_ep is not None
                            resp.in_production = in_prod
                            resp.next_episode_to_air = next_ep
                            resp.has_next_episode = next_ep is not None

                            ep_cnt = raw_tmdb.get('number_of_episodes') or resp.total_episodes
                            s_cnt = raw_tmdb.get('number_of_seasons') or resp.total_seasons
                            seasons_raw = [
                                {
                                    'season_number': s.get('season_number'),
                                    'episode_count': s.get('episode_count')
                                } for s in raw_tmdb.get('seasons', []) if s.get('season_number', 0) > 0
                            ] if raw_tmdb.get('seasons') else (d.get('seasons') or [])

                            resp.total_episodes = ep_cnt or resp.total_episodes
                            resp.total_seasons = s_cnt or resp.total_seasons
                            resp.seasons = seasons_raw
                            d['seasons'] = seasons_raw
                            d['total_episodes'] = resp.total_episodes
                            d['total_seasons'] = resp.total_seasons

                            import json
                            await self.db.execute(text("""
                                UPDATE content 
                                SET status = COALESCE(:st, status), total_episodes = :ep, total_seasons = :ts, seasons = CAST(:seasons AS JSONB), last_synced_at = now()
                                WHERE id = :cid
                            """), {
                                'st': tmdb_status,
                                'ep': resp.total_episodes,
                                'ts': resp.total_seasons,
                                'seasons': json.dumps(seasons_raw),
                                'cid': d['id']
                            })
                            await self.db.commit()
                    except Exception as s_sync_err:
                        logger.warning(f"Failed auto-syncing status/seasons for TMDB {d.get('tmdb_id')}: {s_sync_err}")

            # Fetch TMDB backdrops gallery array
            if d.get('tmdb_id'):
                try:
                    m_type = 'tv' if d.get('content_type') in ['series', 'anime', 'tv'] else 'movie'
                    raw_tmdb = await self.tmdb_client.get_raw_details(d['tmdb_id'], m_type)
                    if raw_tmdb:
                        backdrops_list = self.tmdb_client._extract_backdrops(raw_tmdb)
                        if backdrops_list:
                            resp.backdrops = backdrops_list
                except Exception as b_err:
                    logger.warning(f"Failed fetching backdrops for TMDB {d.get('tmdb_id')}: {b_err}")

            if not resp.backdrops and d.get('backdrop_url'):
                resp.backdrops = [d['backdrop_url']]

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
                            EXISTS (
                                SELECT 1 FROM calendar_alerts ca
                                LEFT JOIN content c_ca ON c_ca.id = ca.content_id
                                LEFT JOIN content c_target ON c_target.id = :cid
                                WHERE ca.user_id = :uid 
                                  AND (
                                    ca.content_id = :cid 
                                    OR (c_ca.tmdb_id IS NOT NULL AND c_target.tmdb_id IS NOT NULL AND c_ca.tmdb_id = c_target.tmdb_id)
                                    OR (c_ca.mal_id IS NOT NULL AND c_target.mal_id IS NOT NULL AND c_ca.mal_id = c_target.mal_id)
                                  )
                            ) as is_notified
                        FROM user_content_status s
                        JOIN content c_s ON c_s.id = s.content_id
                        LEFT JOIN content c_target ON c_target.id = :cid
                        WHERE s.user_id = :uid 
                          AND (
                            s.content_id = :cid
                            OR (c_s.tmdb_id IS NOT NULL AND c_target.tmdb_id IS NOT NULL AND c_s.tmdb_id = c_target.tmdb_id)
                            OR (c_s.mal_id IS NOT NULL AND c_target.mal_id IS NOT NULL AND c_s.mal_id = c_target.mal_id)
                          )
                        ORDER BY s.updated_at DESC
                        LIMIT 1
                    '''), {'uid': uid_obj, 'cid': d['id']})
                    row = status_res.mappings().first()
                    
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
                    
                    # Check for notification even if no status row or if status row is_notified was false
                    if not resp.is_notified:
                        ca_res = await self.db.execute(text('''
                            SELECT EXISTS (
                                SELECT 1 FROM calendar_alerts ca
                                LEFT JOIN content c_ca ON c_ca.id = ca.content_id
                                LEFT JOIN content c_target ON c_target.id = :cid
                                WHERE ca.user_id = :uid 
                                  AND (
                                    ca.content_id = :cid 
                                    OR (c_ca.tmdb_id IS NOT NULL AND c_target.tmdb_id IS NOT NULL AND c_ca.tmdb_id = c_target.tmdb_id)
                                    OR (c_ca.mal_id IS NOT NULL AND c_target.mal_id IS NOT NULL AND c_ca.mal_id = c_target.mal_id)
                                  )
                            )
                        '''), {'uid': uid_obj, 'cid': d['id']})
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
                                       FROM jsonb_array_elements(CASE WHEN jsonb_typeof(c.seasons) = 'array' THEN c.seasons ELSE '[]'::jsonb END) elem 
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
                        LEFT JOIN user_season_status uss ON uss.content_id = c.id AND uss.user_id = CAST(:uid AS UUID) AND uss.season_number = s.sn
                        WHERE c.id = CAST(:cid AS UUID)
                        ORDER BY s.sn ASC
                    """), {
                        'uid': uid_obj, 
                        'cid': cid_obj,
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
                    
                    # 7. Fetch Friends Activity
                    friends_res = await self.db.execute(text("""
                        SELECT 
                            p.id as user_id, p.username, p.display_name, p.avatar_url,
                            ucs.status, ucs.rating,
                            ucs.progress_episodes, ucs.last_watched_season, ucs.last_watched_episode
                        FROM friends f
                        JOIN profiles p ON (f.user_id1 = :uid AND p.id = f.user_id2) OR (f.user_id2 = :uid AND p.id = f.user_id1)
                        JOIN user_content_status ucs ON ucs.user_id = p.id AND ucs.content_id = :cid
                        WHERE (ucs.is_watched = true OR ucs.is_interested = true OR ucs.status != 'none')
                        LIMIT 10
                    """), {'uid': uid_obj, 'cid': cid_obj})
                    
                    from app.models.content import FriendActivityResponse
                    resp.friends_activity = [
                        FriendActivityResponse(
                            user_id=r['user_id'],
                            username=r['username'],
                            display_name=r['display_name'],
                            avatar_url=r['avatar_url'],
                            status=r['status'],
                            rating=float(r['rating']) if r['rating'] is not None else None,
                            progress_episodes=r['progress_episodes'] or 0,
                            last_watched_season=r['last_watched_season'] or 0,
                            last_watched_episode=r['last_watched_episode'] or 0
                        ) for r in friends_res.mappings()
                    ]

                    # 8. Fetch User Reviews & Watch History
                    rev_all_res = await self.db.execute(text("""
                        SELECT id, rating, star_rating, text_review, is_spoiler, contains_spoiler, created_at, watch_history_id
                        FROM reviews
                        WHERE user_id = :uid AND content_id = :cid AND (is_deleted = false OR is_deleted IS NULL)
                        ORDER BY created_at ASC
                    """), {'uid': uid_obj, 'cid': cid_obj})
                    all_reviews = rev_all_res.mappings().all()
                    
                    if all_reviews:
                        latest_rev = all_reviews[-1]
                        resp.review_text = latest_rev['text_review']
                        resp.user_review = {
                            'id': str(latest_rev['id']),
                            'text_review': latest_rev['text_review'],
                            'rating': latest_rev['rating'] or latest_rev['star_rating'],
                            'created_at': latest_rev['created_at'].isoformat() if latest_rev['created_at'] else None,
                        }

                    wh_res = await self.db.execute(text("""
                        SELECT wh.id, wh.watch_type, wh.watched_at, wh.rating, wh.review_id
                        FROM watch_history wh
                        WHERE wh.user_id = :uid AND wh.content_id = :cid
                        ORDER BY wh.watched_at ASC
                    """), {'uid': uid_obj, 'cid': cid_obj})
                    history_rows = wh_res.mappings().all()
                    history_list = [dict(r) for r in history_rows]

                    # Collect all unique entries (combining watch_history table and reviews table)
                    combined_entries = []
                    processed_rev_ids = set()

                    # 1. Add all watch_history entries
                    for wh in history_rows:
                        wid = str(wh['id'])
                        matching_rev = None
                        
                        if wh.get('review_id'):
                            matching_rev = next((r for r in all_reviews if str(r['id']) == str(wh['review_id'])), None)
                        if not matching_rev:
                            matching_rev = next((r for r in all_reviews if str(r.get('watch_history_id')) == wid), None)
                        if not matching_rev and len(history_rows) == 1 and all_reviews:
                            matching_rev = all_reviews[0]

                        if matching_rev:
                            processed_rev_ids.add(str(matching_rev['id']))

                        wh_rating = float(wh['rating']) if wh.get('rating') is not None else None
                        if wh_rating is None and matching_rev:
                            rev_r = matching_rev.get('rating') or matching_rev.get('star_rating')
                            wh_rating = float(rev_r) if rev_r is not None else None

                        combined_entries.append({
                            'id': wid,
                            'watched_at': wh['watched_at'],
                            'rating': wh_rating if wh_rating is not None else resp.user_rating,
                            'review': matching_rev['text_review'] if matching_rev else None,
                            'watch_type': wh.get('watch_type', 'first_watch')
                        })

                    # 2. Add any remaining orphan reviews that don't match any watch_history entry
                    for rev in all_reviews:
                        rid = str(rev['id'])
                        if rid not in processed_rev_ids:
                            rev_r = rev.get('rating') or rev.get('star_rating')
                            combined_entries.append({
                                'id': rid,
                                'watched_at': rev['created_at'],
                                'rating': float(rev_r) if rev_r is not None else resp.user_rating,
                                'review': rev['text_review'],
                                'watch_type': 'first_watch' if len(combined_entries) == 0 else 'rewatch'
                            })

                    # 3. Sort combined entries chronologically ASC
                    combined_entries.sort(key=lambda x: x['watched_at'] or datetime.min.replace(tzinfo=timezone.utc))

                    resp.watch_history = []
                    for idx, item in enumerate(combined_entries):
                        w_type = 'first_watch' if idx == 0 else 'rewatch'

                        resp.watch_history.append({
                            'id': item['id'],
                            'watch_type': w_type,
                            'watched_at': item['watched_at'].isoformat() if hasattr(item['watched_at'], 'isoformat') else (str(item['watched_at']) if item['watched_at'] else None),
                            'rating': item['rating'],
                            'review': item['review'],
                            'watch_number': idx + 1
                        })

                except Exception as status_err:
                    logger.error(f"Error fetching user status for {content_id}: {status_err}")
                    try:
                        await self.db.rollback()
                    except Exception: pass

            return resp
        except Exception as e:
            logger.error(f"Error getting content by id {content_id}: {e}")
            raise

    async def _resolve_meta(self, content_id: str) -> Optional[Dict[str, Any]]:
        """Lightweight lookup to resolve content ID, tmdb_id, and content_type without full social joins."""
        try:
            is_uuid = False
            try:
                UUID(content_id)
                is_uuid = True
            except ValueError:
                pass

            if is_uuid:
                res = await self.db.execute(text(
                    "SELECT id, tmdb_id, content_type FROM content WHERE id = CAST(:cid AS UUID) LIMIT 1"
                ), {'cid': content_id})
            else:
                try:
                    tid = int(content_id)
                    res = await self.db.execute(text(
                        "SELECT id, tmdb_id, content_type FROM content WHERE tmdb_id = :tid LIMIT 1"
                    ), {'tid': tid})
                except ValueError:
                    return None

            row = res.mappings().one_or_none()
            return dict(row) if row else None
        except Exception:
            return None

    async def get_content_credits(self, content_id: str) -> List[Dict[str, Any]]:
        """Fetch cast and crew, using local DB if available, otherwise fallback to TMDB."""
        try:
            # 1. Resolve content meta quickly without heavy social joins
            meta = await self._resolve_meta(content_id)
            if not meta: return []
            
            cid_uuid = meta['id']
            tmdb_id = meta.get('tmdb_id')
            content_type = meta.get('content_type', 'movie')

            # 2. Try local DB credits by UUID
            res = await self.db.execute(text('''
                SELECT p.id, p.name, p.profile_image_url as profile_url, cc.role, cc.character_name as character, cc.job, cc.department
                FROM content_credits cc
                JOIN persons p ON p.id = cc.person_id
                WHERE cc.content_id = :cid
                ORDER BY cc.display_order ASC
            '''), {'cid': cid_uuid})
            rows = [dict(r) for r in res.mappings()]
            if rows:
                for r in rows:
                    p_url = r.get('profile_url')
                    if p_url and not p_url.startswith('http'):
                        r['profile_url'] = f"https://image.tmdb.org/t/p/w500{p_url}"
                return rows

            # 3. If no local credits, fetch from TMDB
            if not tmdb_id and meta.get('title'):
                try:
                    search_results = await self.tmdb_client.search_multi(meta['title'])
                    if search_results:
                        match = search_results[0]
                        tmdb_id = match.get('id')
                        content_type = match.get('content_type') or content_type
                        if tmdb_id:
                            await self.db.execute(text("UPDATE content SET tmdb_id = :tid WHERE id = :cid"), {'tid': tmdb_id, 'cid': cid_uuid})
                            await self.db.commit()
                except Exception as s_err:
                    logger.warning(f"Failed resolving tmdb_id for {meta.get('title')}: {s_err}")

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
        meta = await self._resolve_meta(content_id)
        if not meta:
            return {"episodes": []}
        
        tmdb_id = meta.get('tmdb_id')
        if not tmdb_id and meta.get('title'):
            try:
                search_results = await self.tmdb_client.search_multi(meta['title'])
                if search_results:
                    match = search_results[0]
                    tmdb_id = match.get('id')
                    if tmdb_id:
                        await self.db.execute(text("UPDATE content SET tmdb_id = :tid WHERE id = :cid"), {'tid': tmdb_id, 'cid': meta['id']})
                        await self.db.commit()
            except Exception as s_err:
                logger.warning(f"Failed resolving tmdb_id for season details of {meta.get('title')}: {s_err}")
                
        if not tmdb_id:
            return {"episodes": []}
        
        return await self.tmdb_client.get_season_details(tmdb_id, season_number)

    async def get_similar_content(self, content_id: str) -> List[Dict[str, Any]]:
        """Fetch similar content from TMDB and upsert basic info."""
        try:
            meta = await self._resolve_meta(content_id)
            if not meta or not meta.get('tmdb_id'): return []

            similar_data = await self.tmdb_client.get_similar(meta['tmdb_id'], meta.get('content_type', 'movie'))
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
        p_img = p.get('profile_url') or p.get('profile_path') or p.get('image_url')
        if p_img and not p_img.startswith('http'):
            p_img = f"https://image.tmdb.org/t/p/w500{p_img}"
            
        res = await self.db.execute(stmt, {
            'tmdb_id': p.get('tmdb_id') or p.get('id'),
            'name': p.get('name'),
            'profile_image_url': p_img,
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
        rd_date = None
        if isinstance(rd, str):
            try:
                rd_date = date.fromisoformat(rd.split('T')[0])
            except Exception:
                rd_date = None
        elif isinstance(rd, datetime):
            rd_date = rd.date()
        elif isinstance(rd, date):
            rd_date = rd

        if rd_date and rd_date > today: return 0.0
        ar = d.get('avg_star_rating')
        if ar is not None and ar > 0: return float(ar)
        er = d.get('external_rating')
        if er: return float(round(float(er) / 2.0, 1))
        return 0.0

    async def _populate_user_status(self, items: List[Any], user_id: str):
        if not items: return

        def set_val(obj, key, val):
            if isinstance(obj, dict): obj[key] = val
            else: setattr(obj, key, val)
        
        uuids = []
        tmdb_ids = []
        mal_ids = []
        
        for it in items:
            cid = str(it.get('id', '')) if isinstance(it, dict) else str(getattr(it, 'id', ''))
            tid = it.get('tmdb_id') if isinstance(it, dict) else getattr(it, 'tmdb_id', None)
            mid = it.get('mal_id') if isinstance(it, dict) else getattr(it, 'mal_id', None)
            
            if cid.startswith('tmdb_') and cid[5:].isdigit(): tid = int(cid[5:])
            elif cid.startswith('mal_') and cid[4:].isdigit(): mid = int(cid[4:])
            elif cid.isdigit(): tid = int(cid)
            elif len(cid) >= 32:
                try: uuids.append(UUID(cid))
                except ValueError: pass
                
            if tid is not None:
                try: tmdb_ids.append(int(tid))
                except (ValueError, TypeError): pass
            if mid is not None:
                try: mal_ids.append(int(mid))
                except (ValueError, TypeError): pass

        if not uuids and not tmdb_ids and not mal_ids: return

        uid_obj = UUID(user_id) if isinstance(user_id, str) else user_id

        res = await self.db.execute(text('''
            SELECT 
                c.id as content_id,
                c.tmdb_id,
                c.mal_id,
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
                false as is_notified
            FROM content c
            JOIN user_content_status ucs ON ucs.content_id = c.id AND ucs.user_id = :uid
        '''), {'uid': uid_obj})
        
        status_rows = res.mappings().fetchall()
        uuid_map = {str(r['content_id']): r for r in status_rows}
        tmdb_map = {r['tmdb_id']: r for r in status_rows if r['tmdb_id'] is not None}
        mal_map = {r['mal_id']: r for r in status_rows if r['mal_id'] is not None}

        # Fetch all user calendar_alerts with tmdb_id / mal_id for ultimate fallback
        ca_res = await self.db.execute(text('''
            SELECT ca.content_id, c.tmdb_id, c.mal_id
            FROM calendar_alerts ca
            LEFT JOIN content c ON c.id = ca.content_id
            WHERE ca.user_id = :uid
        '''), {'uid': uid_obj})
        user_ca_rows = ca_res.mappings().fetchall()
        notified_cids = {str(r['content_id']) for r in user_ca_rows if r['content_id']}
        notified_tids = {r['tmdb_id'] for r in user_ca_rows if r['tmdb_id'] is not None}
        notified_mids = {r['mal_id'] for r in user_ca_rows if r['mal_id'] is not None}

        for it in items:
            cid = str(it.get('id', '')) if isinstance(it, dict) else str(getattr(it, 'id', ''))
            tid = it.get('tmdb_id') if isinstance(it, dict) else getattr(it, 'tmdb_id', None)
            mid = it.get('mal_id') if isinstance(it, dict) else getattr(it, 'mal_id', None)

            if cid.startswith('tmdb_') and cid[5:].isdigit(): tid = int(cid[5:])
            elif cid.startswith('mal_') and cid[4:].isdigit(): mid = int(cid[4:])
            elif cid.isdigit(): tid = int(cid)

            status = uuid_map.get(cid)
            if not status and tid is not None:
                try: status = tmdb_map.get(int(tid))
                except (ValueError, TypeError): pass
            if not status and mid is not None:
                try: status = mal_map.get(int(mid))
                except (ValueError, TypeError): pass

            if not status:
                continue
            
            def set_val(obj, key, val):
                if isinstance(obj, dict): obj[key] = val
                else: setattr(obj, key, val)

            set_val(it, 'is_watched', status.get('is_watched') or False)
            set_val(it, 'is_liked', status.get('is_liked') or False)
            set_val(it, 'is_dropped', status.get('is_dropped') or False)
            set_val(it, 'is_interested', status.get('is_interested') or False)

            # Check if notified by content_id, tmdb_id, mal_id, or DB exists check
            is_notified = (cid in notified_cids) or \
                          (tid is not None and int(tid) in notified_tids) or \
                          (mid is not None and int(mid) in notified_mids) or \
                          bool(status.get('is_notified'))

            set_val(it, 'is_notified', is_notified)
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

        # Batch Fetch Friends Activity
        if user_id:
            try:
                friends_act_res = await self.db.execute(text("""
                    SELECT 
                        ucs.content_id,
                        p.id as user_id, p.username, p.display_name, p.avatar_url,
                        ucs.status, ucs.rating,
                        ucs.progress_episodes, ucs.last_watched_season, ucs.last_watched_episode
                    FROM friends f
                    JOIN profiles p ON (f.user_id1 = :uid AND p.id = f.user_id2) OR (f.user_id2 = :uid AND p.id = f.user_id1)
                    JOIN user_content_status ucs ON ucs.user_id = p.id
                    WHERE ucs.content_id = ANY(CAST(:cids AS UUID[]))
                      AND (ucs.is_watched = true OR ucs.is_interested = true OR ucs.status != 'none')
                    LIMIT 50
                """), {'uid': uid_obj, 'cids': uuids})
                
                from collections import defaultdict
                friends_map = defaultdict(list)
                for r in friends_act_res.mappings():
                    friends_map[str(r['content_id'])].append({
                        'user_id': str(r['user_id']),
                        'username': r['username'],
                        'display_name': r['display_name'],
                        'avatar_url': r['avatar_url'],
                        'status': r['status'],
                        'rating': float(r['rating']) if r['rating'] is not None else None,
                        'progress_episodes': r['progress_episodes'] or 0,
                        'last_watched_season': r['last_watched_season'] or 0,
                        'last_watched_episode': r['last_watched_episode'] or 0
                    })
                
                for it in items:
                    cid = str(it.get('id', '')) if isinstance(it, dict) else str(getattr(it, 'id', ''))
                    set_val(it, 'friends_activity', friends_map.get(cid, []))
            except Exception as e:
                logger.error(f"Error populating friends activity in batch: {e}")

    async def ensure_content_persisted(self, content_id: Union[str, UUID]) -> UUID:
        """Guarantees that a row for content_id exists in table `content` before foreign key insertion."""
        cid_str = str(content_id)
        
        # 1. Check if valid UUID and already exists in DB
        is_uuid = False
        target_uuid = None
        try:
            target_uuid = UUID(cid_str) if not isinstance(content_id, UUID) else content_id
            is_uuid = True
            res = await self.db.execute(text("SELECT id FROM content WHERE id = :cid LIMIT 1"), {"cid": target_uuid})
            if res.scalar_one_or_none():
                return target_uuid
        except ValueError:
            target_uuid = None

        # 2. Check uuid_map cache
        mapped = None
        try:
            mapped = await cache.get(f"uuid_map:{cid_str}")
        except Exception: pass
        
        t_id = mapped.get("tmdb_id") if (mapped and isinstance(mapped, dict)) else None
        ctype = mapped.get("content_type", "movie") if (mapped and isinstance(mapped, dict)) else "movie"

        if not t_id:
            digits = ''.join(c for c in cid_str if c.isdigit())
            if digits and 0 < int(digits) <= 2147483647:
                t_id = int(digits)

        # 3. If tmdb_id found, check if already in DB by tmdb_id
        if t_id:
            db_res = await self.db.execute(text("SELECT id FROM content WHERE tmdb_id = :tid LIMIT 1"), {"tid": t_id})
            existing_id = db_res.scalar_one_or_none()
            if existing_id:
                return existing_id if isinstance(existing_id, UUID) else UUID(str(existing_id))
            
            # Fetch from TMDB & upsert to DB
            try:
                ext_data = await self.tmdb_client.get_series_details(t_id) if ctype in ['series', 'tv'] or 'series' in cid_str else await self.tmdb_client.get_movie_details(t_id)
                if not ext_data or not ext_data.get('title'):
                    ext_data = await self.tmdb_client.get_movie_details(t_id) if ctype in ['series', 'tv'] or 'series' in cid_str else await self.tmdb_client.get_series_details(t_id)
                if ext_data and (ext_data.get('title') or ext_data.get('name')):
                    norm = self.tmdb_client._normalize_series(ext_data) if ctype in ['series', 'tv'] or 'series' in cid_str else self.tmdb_client._normalize_movie(ext_data)
                    upserted = await self._upsert_tmdb_content([norm], returning=True, is_permanent=True)
                    if upserted and upserted[0].get('id'):
                        return UUID(str(upserted[0]['id']))
            except Exception as e:
                logger.warning(f"Failed to fetch/upsert TMDB item {t_id}: {e}")

        # 4. Fallback: insert skeleton row into PostgreSQL content table for target_uuid or generated UUID
        final_uuid = target_uuid if is_uuid else UUID(int=abs(hash(cid_str)) % (2**128))
        try:
            await self.db.execute(text('''
                INSERT INTO content (id, title, content_type, poster_url, synopsis, external_rating, genres, is_permanent)
                VALUES (:id, :title, :ctype, 'https://image.tmdb.org/t/p/w500/m89G6b6T2m8r.jpg', 'Content details', 8.0, ARRAY['Drama'], true)
                ON CONFLICT (id) DO NOTHING
            '''), {
                'id': final_uuid,
                'title': f"Content {cid_str[:8]}" if is_uuid else cid_str.replace('tmdb_', '').title(),
                'ctype': 'series' if 'series' in cid_str else 'movie'
            })
            await self.db.commit()
        except Exception as insert_err:
            logger.warning(f"Fallback insert content failed for {final_uuid}: {insert_err}")
            try: await self.db.rollback()
            except Exception: pass
        return final_uuid

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
                release_date = COALESCE(EXCLUDED.release_date, content.release_date),
                last_synced_at = now(),
                is_permanent = content.is_permanent OR EXCLUDED.is_permanent
        '''
        if returning: stmt_text += ' RETURNING *'
        stmt = text(stmt_text)
        params = []
        for it in u_items:
            rd = it.get('release_date')
            ctype = it.get('content_type')
            
            # Smart Re-classification: If it's a TMDB Movie/Series but is Japanese Animation, it's Anime.
            # (TMDB Genre 16 is Animation)
            genres = it.get('genres', [])
            if ctype in ['series', 'movie'] and it.get('original_language') == 'ja' and ('Animation' in genres or 16 in it.get('genre_ids', [])):
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

    async def get_swipe_content(
        self,
        user_id: str,
        content_type: str = "all",
        genres: Optional[str] = None,
        year: Optional[int] = None,
        origin: Optional[str] = None,
        language: Optional[str] = None,
        decade: Optional[str] = None,
        awards: Optional[str] = None,
        page: int = 1
    ) -> List[Dict[str, Any]]:
        import httpx
        from uuid import UUID
        
        # 1. Fetch user's already watched/saved tmdb_ids
        uid_uuid = UUID(user_id) if isinstance(user_id, str) else user_id
        
        status_ids_query = text("""
            SELECT c.tmdb_id FROM user_content_status ucs
            JOIN content c ON ucs.content_id = c.id
            WHERE ucs.user_id = :uid AND (ucs.status != 'none' OR ucs.is_watched = true OR ucs.is_interested = true OR ucs.is_dropped = true OR ucs.is_skipped = true)
        """)
        status_ids_res = await self.db.execute(status_ids_query, {"uid": uid_uuid})
        interacted_tmdb_ids = {r["tmdb_id"] for r in status_ids_res.mappings() if r["tmdb_id"] is not None}
        
        collection_ids_query = text("""
            SELECT c.tmdb_id FROM collection_items ci
            JOIN content c ON ci.content_id = c.id
            WHERE ci.added_by = :uid
        """)
        collection_ids_res = await self.db.execute(collection_ids_query, {"uid": uid_uuid})
        for r in collection_ids_res.mappings():
            if r["tmdb_id"] is not None:
                interacted_tmdb_ids.add(r["tmdb_id"])

        # 3. Determine actual types to query
        types_to_query = []
        if content_type == "movie":
            types_to_query = ["movie"]
        elif content_type == "series":
            types_to_query = ["series"]
        elif content_type == "anime":
            types_to_query = ["anime"]
        else:
            types_to_query = ["movie", "series"]

        raw_results = []
        
        country_code_map = {
            "india": ("IN", "hi|ta|te|ml|kn"),
            "indian": ("IN", "hi|ta|te|ml|kn"),
            "usa": ("US", "en"),
            "united states": ("US", "en"),
            "uk": ("GB", "en"),
            "united kingdom": ("GB", "en"),
            "japan": ("JP", "ja"),
            "south korea": ("KR", "ko"),
            "korea": ("KR", "ko"),
            "france": ("FR", "fr"),
            "spain": ("ES", "es"),
            "germany": ("DE", "de"),
            "canada": ("CA", "en|fr")
        }
        
        lang_map = {
            "english": "en",
            "hindi": "hi",
            "korean": "ko",
            "japanese": "ja",
            "french": "fr",
            "spanish": "es",
            "german": "de",
            "tamil": "ta",
            "telugu": "te",
            "malayalam": "ml",
            "kannada": "kn"
        }
        
        award_keyword_map = {
            "oscars": "370793|353465|360635",
            "oscar": "370793|353465|360635",
            "cannes": "366594|347694|250135",
            "bafta": "320648|360546",
            "golden globes": "341034|335286|344724",
            "golden globe": "341034|335286|344724",
            "emmys": "334043",
            "emmy": "334043"
        }

        # Query 3 pages concurrently from TMDB to ensure a larger pool of results
        pages_to_fetch = [(page - 1) * 3 + 1, (page - 1) * 3 + 2, (page - 1) * 3 + 3]
        
        query_configs = []
        for ctype in types_to_query:
            if ctype == "anime":
                # Anime can be movies or TV series, query both on TMDB
                for path in ["movie", "tv"]:
                    for p_num in pages_to_fetch:
                        query_configs.append({
                            "ctype": "anime",
                            "path": path,
                            "is_movie": (path == "movie"),
                            "page_num": p_num
                        })
            elif ctype == "movie":
                for p_num in pages_to_fetch:
                    query_configs.append({
                        "ctype": "movie",
                        "path": "movie",
                        "is_movie": True,
                        "page_num": p_num
                    })
            elif ctype == "series":
                for p_num in pages_to_fetch:
                    query_configs.append({
                        "ctype": "series",
                        "path": "tv",
                        "is_movie": False,
                        "page_num": p_num
                    })

        async def fetch_single_query(config: dict, client: httpx.AsyncClient) -> List[Dict[str, Any]]:
            is_movie = config["is_movie"]
            ctype = config["ctype"]
            path = config["path"]
            page_num = config["page_num"]
            
            # Resolve genre ids for this specific content type!
            genre_map = {
                'Action': (28 if is_movie else 10759),
                'Adventure': (12 if is_movie else 10759),
                'Animation': 16,
                'Comedy': 35,
                'Crime': 80,
                'Documentary': 99,
                'Drama': 18,
                'Family': 10751,
                'Fantasy': (14 if is_movie else 10765),
                'History': 36,
                'Horror': (27 if is_movie else 9648),
                'Music': 10402,
                'Mystery': 9648,
                'Romance': 10749,
                'Science Fiction': (878 if is_movie else 10765),
                'Sci-Fi': (878 if is_movie else 10765),
                'TV Movie': 10770,
                'Thriller': (53 if is_movie else 9648),
                'War': (10768 if not is_movie else 10752),
                'Western': 37
            }
            
            genre_ids = []
            if genres:
                for g in genres.split(','):
                    g_trimmed = g.strip()
                    for name, gid in genre_map.items():
                        if name.lower() == g_trimmed.lower():
                            genre_ids.append(str(gid))
                            break
            
            # For Anime mode: strictly ensure Animation genre (16) is included!
            if ctype == "anime":
                if "16" not in genre_ids:
                    genre_ids.append("16")
            
            with_genres = ",".join(genre_ids) if genre_ids else None
            
            params = {
                "api_key": self.tmdb_client.api_key,
                "language": "en-US",
                "sort_by": "popularity.desc",
                "page": page_num,
            }
            
            if with_genres:
                params["with_genres"] = with_genres
            
            if ctype == "series":
                params["without_genres"] = "10766" # No soaps
            
            if year:
                if is_movie:
                    params["primary_release_year"] = year
                else:
                    params["first_air_date_year"] = year
            
            # Apply country mapping (origin)
            if origin:
                orig_key = origin.lower().strip()
                if orig_key in country_code_map:
                    ccode, langcode = country_code_map[orig_key]
                    params["with_origin_country"] = ccode
                    params["with_original_language"] = langcode
                    # For TMDB release regions
                    if orig_key in ["india", "indian"]:
                        params["region"] = "IN"
                elif origin == "Indian":
                    params["with_original_language"] = "hi|ta|te|ml|kn"
                    params["region"] = "IN"
                    params["with_origin_country"] = "IN"
            
            # Apply language mapping (strict Japanese override for Anime)
            if language and ctype != "anime":
                lang_lower = language.lower().strip()
                if lang_lower in lang_map:
                    params["with_original_language"] = lang_map[lang_lower]
            elif ctype == "anime":
                params["with_original_language"] = "ja"
            
            # Apply decade range mapping
            if decade:
                dec_clean = decade.replace("s", "").strip()
                if len(dec_clean) == 2:
                    if dec_clean.startswith(('0', '1', '2')):
                        dec_clean = "20" + dec_clean
                    else:
                        dec_clean = "19" + dec_clean
                try:
                    dec_val = int(dec_clean)
                    start_year = dec_val
                    end_year = dec_val + 9
                    
                    start_date = f"{start_year}-01-01"
                    end_date = f"{end_year}-12-31"
                    
                    if is_movie:
                        params["primary_release_date.gte"] = start_date
                        params["primary_release_date.lte"] = end_date
                    else:
                        params["first_air_date.gte"] = start_date
                        params["first_air_date.lte"] = end_date
                except Exception as e:
                    logger.error(f"Error parsing decade: {decade}, {e}")
            
            # Apply awards keyword mapping
            if awards:
                aw_lower = awards.lower().strip()
                if aw_lower in award_keyword_map:
                    params["with_keywords"] = award_keyword_map[aw_lower]
            
            try:
                url = f"{self.tmdb_client.BASE_URL}/discover/{path}"
                logger.info(f"SWIPE_TMDB: Querying {url} with params {params}")
                resp = await client.get(url, params=params, timeout=15.0)
                resp.raise_for_status()
                data = resp.json()
                results = data.get("results", [])
                
                normalized = []
                for item in results:
                    if is_movie:
                        normalized_item = self.tmdb_client._normalize_movie(item)
                    else:
                        normalized_item = self.tmdb_client._normalize_series(item)
                    
                    # Force content_type to "anime" if we queried for anime!
                    if ctype == "anime":
                        normalized_item["content_type"] = "anime"
                        
                    normalized.append(normalized_item)
                return normalized
            except Exception as e:
                logger.error(f"Swipe TMDB discover failed for {ctype} (page {page_num}): {e}")
                return []

        # 4. Query TMDB discover API concurrently
        async with httpx.AsyncClient() as client:
            tasks = [fetch_single_query(config, client) for config in query_configs]
            task_results = await asyncio.gather(*tasks)
            for res_list in task_results:
                raw_results.extend(res_list)

        # 5. Exclude interacted / watched / collection IDs and filter anime out of movie/series modes
        filtered_results = []
        seen_tmdb_ids = set() # Avoid duplicates since we query both movie and tv for anime
        
        for item in raw_results:
            tmdb_id = item.get("tmdb_id")
            if not tmdb_id:
                continue
            if tmdb_id in interacted_tmdb_ids or tmdb_id in seen_tmdb_ids:
                continue
            
            # Exclude Japanese Animation (Anime) from movie and series modes!
            if content_type in ["movie", "series", "all"]:
                # If original language is Japanese and genre is Animation, it is Anime, so filter it out
                if item.get("original_language") == "ja" and "Animation" in item.get("genres", []):
                    continue
            
            # Post-filter for Global origin if needed
            if origin == "Global":
                orig_lang = item.get("original_language")
                if orig_lang in ["hi", "ta", "te", "ml", "kn"]:
                    continue
            
            seen_tmdb_ids.add(tmdb_id)
            filtered_results.append(item)

        # 6. Auto-sync / Upsert to DB and return response
        upserted_rows = []
        if filtered_results:
            upserted_rows = await self._upsert_tmdb_content(filtered_results, returning=True)

        results_list = [dict(row) for row in upserted_rows]
        return self._map_to_response(results_list)

    async def get_watch_providers(self, content_id: str, country: str = "IN") -> List[Dict[str, Any]]:
        """Fetch watch providers for a movie/tv show from TMDB, filtered by country."""
        country_upper = country.upper()
        cache_key = CacheKeys.watch_providers(content_id, country_upper)
        
        # 1. Check cache first
        try:
            cached = await cache.get(cache_key)
            if cached is not None:
                return cached
        except Exception: pass

        # 2. Fast direct DB lookup
        is_uuid = False
        try:
            UUID(content_id)
            is_uuid = True
        except ValueError: pass

        row = None
        if is_uuid:
            res = await self.db.execute(text("SELECT tmdb_id, content_type, title FROM content WHERE id = CAST(:id AS UUID)"), {"id": content_id})
            row = res.mappings().one_or_none()
        else:
            digits = ''.join(c for c in str(content_id) if c.isdigit())
            num_id = None
            if digits:
                try:
                    val = int(digits)
                    if 0 < val <= 2147483647:
                        num_id = val
                except (ValueError, OverflowError): pass
            
            if num_id is not None:
                res = await self.db.execute(text("SELECT tmdb_id, content_type, title FROM content WHERE tmdb_id = :id OR mal_id = :id"), {"id": num_id})
                row = res.mappings().one_or_none()
                if not row:
                    row = {"tmdb_id": num_id, "content_type": "series" if "series" in content_id else "movie"}

        if not row or not row.get('tmdb_id'):
            return []

        tmdb_id = row['tmdb_id']
        tmdb_type = 'movie' if row.get('content_type') == 'movie' else 'tv'

        # 3. Fetch from TMDB with 6.0s timeout
        try:
            raw_providers = await asyncio.wait_for(self.tmdb_client.get_watch_providers(tmdb_id, tmdb_type), timeout=6.0)
        except Exception as e:
            logger.warning(f"Watch providers fetch skipped/timeout for TMDB {tmdb_id}: {e}")
            raw_providers = {}
        
        country_data = raw_providers.get(country_upper, {}) if raw_providers else {}
        if not country_data and raw_providers:
            country_data = raw_providers.get('US') or next(iter(raw_providers.values()), {})
            
        flatrate = country_data.get("flatrate", [])
        ads = country_data.get("ads", [])
        free = country_data.get("free", [])
        buy = country_data.get("buy", [])
        rent = country_data.get("rent", [])
        
        # Combine flatrate, ads, free, buy, and rent for maximum coverage
        raw_list = flatrate + ads + free + buy + rent
        
        # Deduplicate on provider_id
        seen = set()
        results = []
        for p in raw_list:
            pid = p.get("provider_id")
            if pid not in seen:
                seen.add(pid)
                logo_path = p.get("logo_path")
                results.append({
                    "provider_id": pid,
                    "provider_name": p.get("provider_name"),
                    "logo_url": f"https://image.tmdb.org/t/p/original{logo_path}" if logo_path else None
                })

        # 4. Fallback for Originals (Netflix, Amazon Prime, Disney+) if TMDB providers list is empty
        if not results:
            raw_details = await self.tmdb_client.get_raw_details(tmdb_id, tmdb_type)
            if raw_details:
                companies = [c.get("name", "").lower() for c in raw_details.get("production_companies", [])]
                networks = [n.get("name", "").lower() for n in raw_details.get("networks", [])] if tmdb_type == "tv" else []
                
                is_netflix = any("netflix" in c for c in companies) or any("netflix" in n for n in networks)
                is_amazon = any("amazon" in c for c in companies) or any("amazon" in n for n in networks)
                is_disney = any("disney" in c or "marvel studios" in c or "lucasfilm" in c for c in companies) or any("disney" in n for n in networks)
                
                if is_netflix:
                    results.append({
                        "provider_id": 8,
                        "provider_name": "Netflix",
                        "logo_url": "https://image.tmdb.org/t/p/original/pbpMk2JmcoNnQwx5JGpXngfoWtp.jpg"
                    })
                if is_amazon:
                    results.append({
                        "provider_id": 119,
                        "provider_name": "Amazon Prime Video",
                        "logo_url": "https://image.tmdb.org/t/p/original/pvske1MyAoymrs5bguRfVqYiM9a.jpg"
                    })
                if is_disney:
                    results.append({
                        "provider_id": 2336,
                        "provider_name": "JioHotstar",
                        "logo_url": "https://image.tmdb.org/t/p/original/kVqjgpcwvDJOhCupjcLzwwtOp52.jpg"
                    })

        # 5. Cache response
        try:
            await cache.set(cache_key, results, ttl=CacheService.TTL_WATCH_PROVIDERS)
        except Exception: pass

        return results

    async def get_content_by_provider(self, mode: str, provider_id: int, page: int = 1, user_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """Fetch content available on a specific streaming provider (e.g., Netflix, Prime, Disney+)."""
        content_type = "movie" if mode == "movies" else ("anime" if mode == "anime" else "series")
        items_raw = await self.tmdb_client.get_discover_by_provider(content_type, provider_id, page=page)
        
        if items_raw:
            from app.core.database import AsyncSessionLocal
            async def _bg_sync(items):
                try:
                    async with AsyncSessionLocal() as bg_db:
                        bg_service = ContentService(bg_db)
                        await bg_service._upsert_tmdb_content(items, returning=False)
                except Exception as ex:
                    logger.warning(f"Background provider sync warning: {ex}")
            asyncio.create_task(_bg_sync(items_raw[:20]))

        if user_id and items_raw:
            await self._populate_user_status(items_raw, user_id)

        return self._map_to_response(items_raw)