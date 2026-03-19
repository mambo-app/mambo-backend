import httpx
from typing import List, Dict, Any
from app.core.config import settings
import logging
from tenacity import retry, stop_after_attempt, wait_exponential

logger = logging.getLogger('mambo.mal')

class MALClient:
    MAL_URL = "https://api.myanimelist.net/v2"
    JIKAN_URL = "https://api.jikan.moe/v4"

    def __init__(self):
        self.client_id = settings.mal_client_id

    async def get_trending_anime(self) -> List[Dict[str, Any]]:
        # Try MAL first
        if self.client_id:
            try:
                # 'bypopularity' or 'all' depending on what is considered 'trending'
                res = await self._fetch_mal_ranking("bypopularity", limit=10)
                if res:
                    return res
            except Exception as e:
                logger.warning(f"MAL trending failed: {e}. Falling back to Jikan.")
        
        # Fallback to Jikan
        try:
            return await self._fetch_jikan_trending()
        except Exception as e:
            logger.error(f"Jikan trending also failed: {e}")
            return []

    async def get_top_anime(self) -> List[Dict[str, Any]]:
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
                    timeout=10.0
                )
                resp.raise_for_status()
                data = resp.json()
                return [self._normalize_jikan(item) for item in data.get("data", [])]
        except Exception as e:
            logger.error(f"Jikan upcoming failed: {e}")
            return []

    async def search_anime(self, query: str, limit: int = 20) -> List[Dict[str, Any]]:
        """Search for anime on MAL/Jikan."""
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
                        timeout=10.0
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
                    timeout=10.0
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
            # Jikan 'top anime' sorted by popularity
            resp = await client.get(
                f"{self.JIKAN_URL}/top/anime",
                params={"filter": "bypopularity", "limit": 10},
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
            'release_date':  data.get('start_date'),
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

        # Jikan often returns ISO strings with time. We only want the date.
        rd = data.get('aired', {}).get('from', '')
        if rd and 'T' in rd:
            rd = rd.split('T')[0]

        return {
            'mal_id':        data.get('mal_id'),
            'title':         data.get('title'),
            'synopsis':      data.get('synopsis'),
            'external_rating': data.get('score'),
            'external_rating_source': 'mal',
            'total_episodes': data.get('episodes'),
            'release_date':  rd,
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
