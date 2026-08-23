import httpx
from typing import List, Dict, Any, Optional
from app.core.config import settings
import logging
from tenacity import retry, stop_after_attempt, wait_exponential

logger = logging.getLogger('mambo.mal')

class MALClient:
    MAL_URL = "https://api.myanimelist.net/v2"
    JIKAN_URL = "https://api.jikan.moe/v4"

    def __init__(self):
        self.client_id = settings.mal_client_id

    async def get_trending_anime(self, page: int = 1) -> List[Dict[str, Any]]:
        # Try MAL first with 'airing' (currently trending seasonal anime)
        if self.client_id:
            try:
                res = await self._fetch_mal_ranking("airing", limit=20)
                if res:
                    return res
            except Exception as e:
                logger.warning(f"MAL trending failed: {e}. Falling back to Jikan.")
        
        # Fallback to Jikan airing
        try:
            return await self._fetch_jikan_trending()
        except Exception as e:
            logger.error(f"Jikan trending also failed: {e}")
            return []

    async def get_top_anime(self, page: int = 1) -> List[Dict[str, Any]]:
        if self.client_id:
            try:
                res = await self._fetch_mal_ranking("all", limit=20)
                if res:
                    return res
            except Exception as e:
                logger.warning(f"MAL top anime failed: {e}. Falling back to Jikan.")
        try:
            return await self._fetch_jikan_top()
        except Exception as e:
            logger.error(f"Jikan top anime failed: {e}")
            return []

    async def get_anime_by_genre(self, genre_id: int) -> List[Dict[str, Any]]:
        """Fetch anime by genre ID using Jikan fallback."""
        try:
            async with httpx.AsyncClient() as client:
                resp = await client.get(
                    f"{self.JIKAN_URL}/anime",
                    params={"genres": genre_id, "order_by": "popularity", "sort": "asc", "limit": 20},
                    timeout=10.0
                )
                resp.raise_for_status()
                data = resp.json()
                return [self._normalize_jikan(item) for item in data.get("data", [])]
        except Exception as e:
            logger.error(f"Error fetching anime by genre {genre_id}: {e}")
            return []

    async def get_current_season_anime(self, page: int = 1) -> List[Dict[str, Any]]:
        """Fetch anime airing in the current season."""
        try:
            async with httpx.AsyncClient() as client:
                resp = await client.get(
                    f"{self.JIKAN_URL}/seasons/now",
                    params={"page": page, "limit": 25},
                    timeout=2.5
                )
                resp.raise_for_status()
                data = resp.json()
                return [self._normalize_jikan(item) for item in data.get("data", [])]
        except Exception as e:
            logger.error(f"Jikan current season failed: {e}")
            return []

    async def get_upcoming_anime(self) -> List[Dict[str, Any]]:
        """Fetch seasonal/upcoming anime."""
        if self.client_id:
            try:
                # Ranking type upcoming for MAL
                res = await self._fetch_mal_ranking("upcoming", limit=10)
                if res: return res
            except Exception as e:
                logger.warning(f"MAL upcoming failed: {e}. Falling back to Jikan.")
        
        try:
            async with httpx.AsyncClient() as client:
                resp = await client.get(
                    f"{self.JIKAN_URL}/seasons/upcoming",
                    params={"limit": 10},
                    timeout=2.5
                )
                resp.raise_for_status()
                data = resp.json()
                return [self._normalize_jikan(item) for item in data.get("data", [])]
        except Exception as e:
            logger.error(f"Jikan upcoming failed: {e}")
            return []

    async def search_anime(self, query: str, limit: int = 20) -> List[Dict[str, Any]]:
        """Search for anime on MAL/Jikan."""
        if not query or len(query.strip()) < 3:
            return []

        if self.client_id:
            try:
                async with httpx.AsyncClient() as client:
                    resp = await client.get(
                        f"{self.MAL_URL}/anime",
                        headers={"X-MAL-CLIENT-ID": self.client_id},
                        params={
                            "q": query,
                            "limit": limit,
                            "fields": "title,synopsis,mean,genres,num_episodes,start_date,status,studios,main_picture"
                        },
                        timeout=3.0
                    )
                    resp.raise_for_status()
                    data = resp.json()
                    return [self._normalize_mal(item["node"]) for item in data.get("data", [])]
            except Exception as e:
                logger.warning(f"MAL search failed: {e}. Falling back to Jikan.")

        # Fallback to Jikan
        try:
            async with httpx.AsyncClient() as client:
                resp = await client.get(
                    f"{self.JIKAN_URL}/anime",
                    params={"q": query, "limit": limit},
                    timeout=3.0
                )
                resp.raise_for_status()
                data = resp.json()
                return [self._normalize_jikan(item) for item in data.get("data", [])]
        except Exception as e:
            logger.error(f"Jikan search failed: {e}")
            return []

    @retry(stop=stop_after_attempt(2), wait=wait_exponential(min=1, max=3))
    async def _fetch_mal_ranking(self, ranking_type: str, limit: int = 10) -> List[Dict[str, Any]]:
        async with httpx.AsyncClient() as client:
            resp = await client.get(
                f"{self.MAL_URL}/anime/ranking",
                headers={"X-MAL-CLIENT-ID": self.client_id},
                params={
                    "ranking_type": ranking_type,
                    "limit": limit,
                    "fields": "title,synopsis,mean,genres,num_episodes,start_date,status,studios,main_picture"
                },
                timeout=10.0
            )
            resp.raise_for_status()
            data = resp.json()
            return [self._normalize_mal(item["node"]) for item in data.get("data", [])]

    @retry(stop=stop_after_attempt(2), wait=wait_exponential(min=1, max=3))
    async def _fetch_jikan_trending(self) -> List[Dict[str, Any]]:
        async with httpx.AsyncClient() as client:
            # Jikan currently airing seasonal anime trending today
            resp = await client.get(
                f"{self.JIKAN_URL}/top/anime",
                params={"filter": "airing", "limit": 20},
                timeout=10.0
            )
            resp.raise_for_status()
            data = resp.json()
            return [self._normalize_jikan(item) for item in data.get("data", [])]

    @retry(stop=stop_after_attempt(2), wait=wait_exponential(min=1, max=3))
    async def _fetch_jikan_top(self) -> List[Dict[str, Any]]:
        async with httpx.AsyncClient() as client:
            resp = await client.get(
                f"{self.JIKAN_URL}/top/anime",
                params={"limit": 20},
                timeout=10.0
            )
            resp.raise_for_status()
            data = resp.json()
            return [self._normalize_jikan(item) for item in data.get("data", [])]

    def _clean_date(self, raw_date: Any) -> Any:
        if not raw_date or not isinstance(raw_date, str):
            return raw_date
        rd = raw_date.strip()
        if 'T' in rd:
            rd = rd.split('T')[0]
        parts = rd.split('-')
        if len(parts) == 2 and len(parts[0]) == 4:
            return f"{rd}-01"
        elif len(parts) == 1 and len(parts[0]) == 4:
            return f"{rd}-01-01"
        return rd

    def _normalize_mal(self, data: dict) -> dict:
        raw_genres = [g['name'] for g in data.get('genres', [])] if data.get('genres') else []
        # Normalize: Mystery/Police -> Crime
        genres = list(set(raw_genres))
        if "Mystery" in genres or "Police" in genres:
            if "Crime" not in genres: genres.append("Crime")
            
        return {
            'mal_id':        data.get('id'),
            'title':         data.get('title'),
            'synopsis':      data.get('synopsis'),
            'external_rating': data.get('mean'),
            'external_rating_source': 'mal',
            'total_episodes': data.get('num_episodes'),
            'release_date':  self._clean_date(data.get('start_date')),
            'status':        self._map_mal_status(data.get('status', '')),
            'poster_url':    data.get('main_picture', {}).get('large') if data.get('main_picture') else None,
            'anime_studio':  data.get('studios', [{}])[0].get('name') if data.get('studios') else None,
            'genres':        genres,
            'content_type':  'anime',
        }

    def _normalize_jikan(self, data: dict) -> dict:
        raw_genres = [g['name'] for g in data.get('genres', [])] if data.get('genres') else []
        genres = list(set(raw_genres))
        if "Mystery" in genres or "Police" in genres:
            if "Crime" not in genres: genres.append("Crime")

        # Jikan often returns ISO strings with time or YYYY-MM. We normalize date.
        rd = data.get('aired', {}).get('from', '')
        clean_rd = self._clean_date(rd)

        return {
            'mal_id':        data.get('mal_id'),
            'title':         data.get('title'),
            'synopsis':      data.get('synopsis'),
            'external_rating': data.get('score'),
            'external_rating_source': 'mal',
            'total_episodes': data.get('episodes'),
            'release_date':  clean_rd,
            'status':        self._map_jikan_status(data.get('status', '')),
            'poster_url':    data.get('images', {}).get('jpg', {}).get('large_image_url') if data.get('images') else None,
            'anime_studio':  data.get('studios', [{}])[0].get('name') if data.get('studios') else None,
            'genres':        genres,
            'content_type':  'anime',
        }

    def _map_mal_status(self, status: str) -> str:
        return {
            'finished_airing': 'released',
            'currently_airing': 'in_production',
            'not_yet_aired': 'upcoming',
        }.get(status, 'released')

    def _map_jikan_status(self, status: str) -> str:
        return {
            'Finished Airing': 'released',
            'Currently Airing': 'in_production',
            'Not yet aired': 'upcoming',
        }.get(status, 'released')

    @retry(stop=stop_after_attempt(2), wait=wait_exponential(min=1, max=3))
    async def get_anime_production_status(self, mal_id: int) -> Optional[Dict[str, Any]]:
        """Fetch production status for an anime from MAL/Jikan."""
        if self.client_id:
            try:
                async with httpx.AsyncClient() as client:
                    resp = await client.get(
                        f"{self.MAL_URL}/anime/{mal_id}",
                        headers={"X-MAL-CLIENT-ID": self.client_id},
                        params={"fields": "status,start_date"},
                        timeout=5.0
                    )
                    if resp.status_code == 200:
                        data = resp.json()
                        st = data.get("status")
                        mapped_st = "Not yet aired" if st == "not_yet_aired" else st
                        return {
                            "mal_id": mal_id,
                            "status": mapped_st,
                            "has_air_date": bool(data.get("start_date"))
                        }
            except Exception as e:
                logger.warning(f"MAL get_anime_production_status failed for {mal_id}: {e}")

        try:
            async with httpx.AsyncClient() as client:
                resp = await client.get(
                    f"{self.JIKAN_URL}/anime/{mal_id}",
                    timeout=5.0
                )
                if resp.status_code == 200:
                    data = resp.json().get("data", {})
                    st = data.get("status")
                    aired_from = data.get("aired", {}).get("from")
                    return {
                        "mal_id": mal_id,
                        "status": st,
                        "has_air_date": bool(aired_from)
                    }
        except Exception as e:
            logger.error(f"Jikan get_anime_production_status failed for {mal_id}: {e}")
        return None

