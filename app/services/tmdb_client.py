import httpx
from datetime import date, datetime, timedelta
from typing import List, Dict, Any, Optional
from app.core.config import settings
import logging
from tenacity import retry, stop_after_attempt, wait_exponential

import asyncio

logger = logging.getLogger('mambo.tmdb')

_HTTP_CLIENT: Optional[httpx.AsyncClient] = None
_TMDB_SEMAPHORE = asyncio.Semaphore(12)

def _get_shared_client() -> httpx.AsyncClient:
    global _HTTP_CLIENT
    if _HTTP_CLIENT is None or _HTTP_CLIENT.is_closed:
        _HTTP_CLIENT = httpx.AsyncClient(
            limits=httpx.Limits(max_keepalive_connections=30, max_connections=100),
            timeout=8.0
        )
    return _HTTP_CLIENT

class TMDBClient:
    BASE_URL = "https://api.themoviedb.org/3"
    IMAGE_BASE = "https://image.tmdb.org/t/p/w500"
    BACKDROP_BASE = "https://image.tmdb.org/t/p/w1280"

    def __init__(self):
        self.api_key = settings.tmdb_api_key
        # If API key is not set, we might mock or return empty
        self.headers = {
            "accept": "application/json",
            "Authorization": f"Bearer {self.api_key}" if self.api_key else ""
        }

    def _client(self) -> httpx.AsyncClient:
        return _get_shared_client()

    @retry(stop=stop_after_attempt(2), wait=wait_exponential(min=1, max=3))
    async def get_trending_movies(self, page: int = 1) -> List[Dict[str, Any]]:
        print(f"DEBUG_TMDB: api_key_set={bool(self.api_key)}")
        if not self.api_key:
            logger.warning("TMDB API Key missing")
            return []
            
        async with httpx.AsyncClient() as client:
            try:
                url = f"{self.BASE_URL}/trending/movie/day"
                print(f"DEBUG_TMDB: GET {url}")
                resp = await client.get(
                    url,
                    params={"api_key": self.api_key, "language": "en-US", "page": page},
                    timeout=10.0
                )
                print(f"DEBUG_TMDB: status={resp.status_code}")
                resp.raise_for_status()
                data = resp.json()
                results = data.get("results", [])
                print(f"DEBUG_TMDB: raw_results={len(results)}")
                return [self._normalize_movie(m) for m in results]
            except Exception as e:
                print(f"DEBUG_TMDB: error={e}")
                logger.error(f"Error fetching trending movies: {e}")
                return []

    @retry(stop=stop_after_attempt(2), wait=wait_exponential(min=1, max=3))
    async def get_popular_movies(self, page: int = 1) -> List[Dict[str, Any]]:
        if not self.api_key:
            return []
        async with httpx.AsyncClient() as client:
            try:
                resp = await client.get(
                    f"{self.BASE_URL}/movie/popular",
                    params={"api_key": self.api_key, "language": "en-US", "page": page},
                    timeout=10.0
                )
                resp.raise_for_status()
                data = resp.json()
                return [self._normalize_movie(m) for m in data.get("results", [])]
            except Exception as e:
                logger.error(f"Error fetching popular movies: {e}")
                return []

    @retry(stop=stop_after_attempt(2), wait=wait_exponential(min=1, max=3))
    async def get_trending_series(self, page: int = 1) -> List[Dict[str, Any]]:
        if not self.api_key:
            return []
            
        async with httpx.AsyncClient() as client:
            try:
                resp = await client.get(
                    f"{self.BASE_URL}/trending/tv/day",
                    params={"api_key": self.api_key, "language": "en-US", "page": page},
                    timeout=10.0
                )
                resp.raise_for_status()
                data = resp.json().get("results", [])
                normalized = [self._normalize_series(s) for s in data]
                return [s for s in normalized if s.get("content_type") != "anime"]
            except Exception as e:
                logger.error(f"Error fetching trending series: {e}")
                return []

    @retry(stop=stop_after_attempt(2), wait=wait_exponential(min=1, max=3))
    async def get_trending_all_day(self, page: int = 1) -> List[Dict[str, Any]]:
        if not self.api_key:
            return []
        async with httpx.AsyncClient() as client:
            try:
                resp = await client.get(
                    f"{self.BASE_URL}/trending/all/day",
                    params={"api_key": self.api_key, "language": "en-US", "page": page},
                    timeout=10.0
                )
                resp.raise_for_status()
                data = resp.json().get("results", [])
                results = []
                for item in data:
                    media_type = item.get("media_type")
                    if media_type == "movie":
                        results.append(self._normalize_movie(item))
                    elif media_type == "tv":
                        norm = self._normalize_series(item)
                        if norm.get("content_type") != "anime":
                            results.append(norm)
                return results
            except Exception as e:
                logger.error(f"Error fetching trending all day: {e}")
                return []

    @retry(stop=stop_after_attempt(2), wait=wait_exponential(min=1, max=3))
    async def get_popular_series(self, page: int = 1) -> List[Dict[str, Any]]:
        if not self.api_key:
            return []
        async with httpx.AsyncClient() as client:
            try:
                resp = await client.get(
                    f"{self.BASE_URL}/tv/popular",
                    params={
                        "api_key": self.api_key,
                        "language": "en-US",
                        "page": page,
                        "without_genres": "10766" # No soaps
                    },
                    timeout=10.0
                )
                resp.raise_for_status()
                data = resp.json().get("results", [])
                normalized = [self._normalize_series(s) for s in data]
                return [s for s in normalized if s.get("content_type") != "anime"]
            except Exception as e:
                logger.error(f"Error fetching popular series: {e}")
                return []

    @retry(stop=stop_after_attempt(2), wait=wait_exponential(min=1, max=3))
    async def get_on_the_air_series(self, page: int = 1) -> List[Dict[str, Any]]:
        """Fetch TV series currently airing episodes (on the air)."""
        if not self.api_key:
            return []
        async with httpx.AsyncClient() as client:
            try:
                resp = await client.get(
                    f"{self.BASE_URL}/tv/on_the_air",
                    params={
                        "api_key": self.api_key,
                        "language": "en-US",
                        "page": page,
                        "without_genres": "10766" # No soaps/talk shows
                    },
                    timeout=10.0
                )
                resp.raise_for_status()
                data = resp.json().get("results", [])
                normalized = [self._normalize_series(s) for s in data]
                return [s for s in normalized if s.get("content_type") != "anime"]
            except Exception as e:
                logger.error(f"Error fetching on the air series: {e}")
                return []

    @retry(stop=stop_after_attempt(2), wait=wait_exponential(min=1, max=3))
    async def get_now_playing_movies(self, page: int = 1) -> List[Dict[str, Any]]:
        """Fetch movies currently playing in theaters."""
        if not self.api_key:
            return []
        async with httpx.AsyncClient() as client:
            try:
                resp = await client.get(
                    f"{self.BASE_URL}/movie/now_playing",
                    params={
                        "api_key": self.api_key,
                        "language": "en-US",
                        "page": page
                    },
                    timeout=10.0
                )
                resp.raise_for_status()
                data = resp.json().get("results", [])
                return [self._normalize_movie(m) for m in data]
            except Exception as e:
                logger.error(f"Error fetching now playing movies: {e}")
                return []

    @retry(stop=stop_after_attempt(2), wait=wait_exponential(min=1, max=3))
    async def get_top_rated_movies(self, page: int = 1) -> List[Dict[str, Any]]:
        if not self.api_key:
            return []
        async with httpx.AsyncClient() as client:
            try:
                resp = await client.get(
                    f"{self.BASE_URL}/movie/top_rated",
                    params={"api_key": self.api_key, "language": "en-US", "page": page},
                    timeout=10.0
                )
                resp.raise_for_status()
                return [self._normalize_movie(m) for m in resp.json().get("results", [])]
            except Exception as e:
                logger.error(f"Error fetching top rated movies: {e}")
                return []

    @retry(stop=stop_after_attempt(2), wait=wait_exponential(min=1, max=3))
    async def get_top_rated_series(self, page: int = 1) -> List[Dict[str, Any]]:
        if not self.api_key:
            return []
        async with httpx.AsyncClient() as client:
            try:
                resp = await client.get(
                    f"{self.BASE_URL}/tv/top_rated",
                    params={
                        "api_key": self.api_key, 
                        "language": "en-US", 
                        "page": page,
                        "without_genres": "10766"
                    },
                    timeout=10.0
                )
                resp.raise_for_status()
                data = resp.json().get("results", [])
                normalized = [self._normalize_series(s) for s in data]
                return [s for s in normalized if s.get("content_type") != "anime"]
            except Exception as e:
                logger.error(f"Error fetching top rated series: {e}")
                return []

    @retry(stop=stop_after_attempt(2), wait=wait_exponential(min=1, max=3))
    async def get_movies_by_genre(self, genre_id: int, page: int = 1) -> List[Dict[str, Any]]:
        """Discover movies filtered by TMDB genre ID, sorted by popularity desc."""
        if not self.api_key:
            return []
        async with httpx.AsyncClient() as client:
            try:
                resp = await client.get(
                    f"{self.BASE_URL}/discover/movie",
                    params={
                        "api_key": self.api_key,
                        "language": "en-US",
                        "with_genres": str(genre_id),
                        "sort_by": "popularity.desc",
                        "page": page,
                    },
                    timeout=10.0
                )
                resp.raise_for_status()
                return [self._normalize_movie(m) for m in resp.json().get("results", [])]
            except Exception as e:
                logger.error(f"Error fetching movies by genre {genre_id}: {e}")
                return []

    @retry(stop=stop_after_attempt(2), wait=wait_exponential(min=1, max=3))
    async def get_series_by_genre(self, genre_id: int, page: int = 1) -> List[Dict[str, Any]]:
        """Discover TV shows filtered by TMDB genre ID, sorted by popularity desc."""
        if not self.api_key:
            return []
        async with httpx.AsyncClient() as client:
            try:
                resp = await client.get(
                    f"{self.BASE_URL}/discover/tv",
                    params={
                        "api_key": self.api_key,
                        "language": "en-US",
                        "with_genres": str(genre_id),
                        "without_genres": "10766",
                        "sort_by": "popularity.desc",
                        "page": page,
                    },
                    timeout=10.0
                )
                resp.raise_for_status()
                data = resp.json().get("results", [])
                normalized = [self._normalize_series(s) for s in data]
                return [s for s in normalized if s.get("content_type") != "anime"]
            except Exception as e:
                logger.error(f"Error fetching series by genre {genre_id}: {e}")
                return []

    async def discover_movies_by_date_range(self, start_date: str, end_date: str, page: int = 1) -> List[Dict[str, Any]]:
        if not self.api_key: return []
        async with httpx.AsyncClient() as client:
            try:
                resp = await client.get(
                    f"{self.BASE_URL}/discover/movie",
                    params={
                        "api_key": self.api_key,
                        "language": "en-US",
                        "primary_release_date.gte": start_date,
                        "primary_release_date.lte": end_date,
                        "sort_by": "popularity.desc",
                        "page": page
                    },
                    timeout=5.0
                )
                resp.raise_for_status()
                return [self._normalize_movie(m) for m in resp.json().get("results", [])]
            except Exception as e:
                logger.error(f"discover_movies_by_date_range failed: {e}")
                return []

    async def discover_indian_movies_by_date_range(self, start_date: str, end_date: str, page: int = 1) -> List[Dict[str, Any]]:
        if not self.api_key: return []
        async with httpx.AsyncClient() as client:
            try:
                resp = await client.get(
                    f"{self.BASE_URL}/discover/movie",
                    params={
                        "api_key": self.api_key,
                        "language": "en-US",
                        "with_original_language": "hi|ta|te|ml|kn",
                        "primary_release_date.gte": start_date,
                        "primary_release_date.lte": end_date,
                        "sort_by": "popularity.desc",
                        "page": page
                    },
                    timeout=5.0
                )
                resp.raise_for_status()
                return [self._normalize_movie(m) for m in resp.json().get("results", [])]
            except Exception as e:
                logger.error(f"discover_indian_movies_by_date_range failed: {e}")
                return []

    async def discover_series_by_date_range(self, start_date: str, end_date: str, page: int = 1) -> List[Dict[str, Any]]:
        if not self.api_key: return []
        async with httpx.AsyncClient() as client:
            try:
                resp = await client.get(
                    f"{self.BASE_URL}/discover/tv",
                    params={
                        "api_key": self.api_key,
                        "language": "en-US",
                        "air_date.gte": start_date,
                        "air_date.lte": end_date,
                        "without_genres": "10766",
                        "sort_by": "popularity.desc",
                        "page": page
                    },
                    timeout=5.0
                )
                resp.raise_for_status()
                results = resp.json().get("results", [])
                normalized = [self._normalize_series(s) for s in results]
                return [s for s in normalized if s.get("content_type") != "anime"]
            except Exception as e:
                logger.error(f"discover_series_by_date_range failed: {e}")
                return []

    async def discover_anime_by_date_range(self, start_date: str, end_date: str, page: int = 1) -> List[Dict[str, Any]]:
        if not self.api_key: return []
        async with httpx.AsyncClient() as client:
            try:
                resp = await client.get(
                    f"{self.BASE_URL}/discover/tv",
                    params={
                        "api_key": self.api_key,
                        "language": "en-US",
                        "with_genres": "16",
                        "with_original_language": "ja",
                        "air_date.gte": start_date,
                        "air_date.lte": end_date,
                        "sort_by": "popularity.desc",
                        "page": page
                    },
                    timeout=5.0
                )
                resp.raise_for_status()
                results = resp.json().get("results", [])
                
                if not results:
                    resp_fb = await client.get(
                        f"{self.BASE_URL}/discover/tv",
                        params={
                            "api_key": self.api_key,
                            "language": "en-US",
                            "with_genres": "16",
                            "with_original_language": "ja",
                            "sort_by": "popularity.desc",
                            "page": page
                        },
                        timeout=5.0
                    )
                    if resp_fb.status_code == 200:
                        results = resp_fb.json().get("results", [])

                normalized = [self._normalize_series(s) for s in results]
                for n in normalized:
                    n["content_type"] = "anime"
                return normalized
            except Exception as e:
                logger.error(f"discover_anime_by_date_range failed: {e}")
                return []

    @retry(stop=stop_after_attempt(2), wait=wait_exponential(min=1, max=3))
    async def search_movies(self, query: str, page: int = 1) -> List[Dict[str, Any]]:
        """Search for movies on TMDB."""
        if not self.api_key: return []
        async with httpx.AsyncClient() as client:
            try:
                resp = await client.get(
                    f"{self.BASE_URL}/search/movie",
                    params={
                        "api_key": self.api_key,
                        "language": "en-US",
                        "query": query,
                        "page": page,
                        "include_adult": "false",
                    },
                    timeout=10.0
                )
                resp.raise_for_status()
                return [self._normalize_movie(m) for m in resp.json().get("results", [])]
            except Exception as e:
                logger.error(f"TMDB search_movies failed: {e}")
                return []

    @retry(stop=stop_after_attempt(2), wait=wait_exponential(min=1, max=3))
    async def search_series(self, query: str, page: int = 1) -> List[Dict[str, Any]]:
        """Search for TV shows on TMDB."""
        if not self.api_key: return []
        async with httpx.AsyncClient() as client:
            try:
                resp = await client.get(
                    f"{self.BASE_URL}/search/tv",
                    params={
                        "api_key": self.api_key,
                        "language": "en-US",
                        "query": query,
                        "page": page,
                        "include_adult": "false",
                    },
                    timeout=10.0
                )
                resp.raise_for_status()
                return [self._normalize_series(s) for s in resp.json().get("results", [])]
            except Exception as e:
                logger.error(f"TMDB search_series failed: {e}")
                return []

    @retry(stop=stop_after_attempt(2), wait=wait_exponential(min=1, max=3))
    async def get_most_anticipated_movies(self, page: int = 1) -> List[Dict[str, Any]]:
        """Fetch the most anticipated (highest popularity) upcoming movies."""
        if not self.api_key:
            return []
        async with httpx.AsyncClient() as client:
            try:
                today_str = date.today().isoformat()
                all_results = []
                for p in [page, page + 1]:
                    resp = await client.get(
                        f"{self.BASE_URL}/discover/movie",
                        params={
                            "api_key": self.api_key,
                            "language": "en-US",
                            "sort_by": "popularity.desc",
                            "primary_release_date.gte": today_str,
                            "page": p
                        },
                        timeout=10.0
                    )
                    resp.raise_for_status()
                    all_results.extend(resp.json().get("results", []))
                return [self._normalize_movie(m) for m in all_results if m.get("poster_path")]
            except Exception as e:
                logger.error(f"Error fetching most anticipated movies: {e}")
                return []

    @retry(stop=stop_after_attempt(2), wait=wait_exponential(min=1, max=3))
    async def get_most_anticipated_series(self, page: int = 1) -> List[Dict[str, Any]]:
        """Fetch the most anticipated (highest popularity) upcoming TV series."""
        if not self.api_key:
            return []
        async with httpx.AsyncClient() as client:
            try:
                today_str = date.today().isoformat()
                all_results = []
                for p in [page, page + 1]:
                    resp = await client.get(
                        f"{self.BASE_URL}/discover/tv",
                        params={
                            "api_key": self.api_key,
                            "language": "en-US",
                            "sort_by": "popularity.desc",
                            "first_air_date.gte": today_str,
                            "page": p
                        },
                        timeout=10.0
                    )
                    resp.raise_for_status()
                    all_results.extend(resp.json().get("results", []))
                return [self._normalize_series(s) for s in all_results if s.get("poster_path")]
            except Exception as e:
                logger.error(f"Error fetching most anticipated series: {e}")
                return []

    @retry(stop=stop_after_attempt(2), wait=wait_exponential(min=1, max=3))
    async def get_announced_movies(self, page: int = 1) -> List[Dict[str, Any]]:
        """Fetch announced, upcoming, and in-production movies concurrently."""
        if not self.api_key:
            return []
        async with httpx.AsyncClient() as client:
            try:
                today_str = date.today().isoformat()
                
                async def fetch_url(endpoint: str, params: dict):
                    try:
                        r = await client.get(f"{self.BASE_URL}{endpoint}", params=params, timeout=5.0)
                        if r.status_code == 200:
                            return r.json().get("results", [])
                    except Exception:
                        pass
                    return []

                tasks = [
                    fetch_url("/movie/upcoming", {"api_key": self.api_key, "language": "en-US", "page": page}),
                    fetch_url("/movie/upcoming", {"api_key": self.api_key, "language": "en-US", "page": page + 1}),
                    fetch_url("/discover/movie", {"api_key": self.api_key, "language": "en-US", "sort_by": "popularity.desc", "primary_release_date.gte": today_str, "page": page}),
                    fetch_url("/discover/movie", {"api_key": self.api_key, "language": "en-US", "sort_by": "popularity.desc", "primary_release_date.gte": today_str, "page": page + 1}),
                ]

                results_list = await asyncio.gather(*tasks)
                all_results = []
                for res in results_list:
                    all_results.extend(res)
                        
                return [self._normalize_movie(m) for m in all_results if m.get("poster_path")]
            except Exception as e:
                logger.error(f"Error fetching announced movies: {e}")
                return []

    @retry(stop=stop_after_attempt(2), wait=wait_exponential(min=1, max=3))
    async def get_announced_series(self, page: int = 1) -> List[Dict[str, Any]]:
        """Fetch announced, in-production, and returning TV series."""
        if not self.api_key:
            return []
        async with httpx.AsyncClient() as client:
            try:
                all_results = []
                for p in range((page - 1) * 2 + 1, (page - 1) * 2 + 4):
                    resp = await client.get(
                        f"{self.BASE_URL}/tv/popular",
                        params={
                            "api_key": self.api_key,
                            "language": "en-US",
                            "page": p
                        },
                        timeout=10.0
                    )
                    resp.raise_for_status()
                    all_results.extend(resp.json().get("results", []))
                return [self._normalize_series(s) for s in all_results if s.get("poster_path")]
            except Exception as e:
                logger.error(f"Error fetching announced series: {e}")
                return []

    @retry(stop=stop_after_attempt(2), wait=wait_exponential(min=1, max=3))
    async def get_announced_anime(self, page: int = 1) -> List[Dict[str, Any]]:
        """Fetch upcoming announced anime from TMDB."""
        if not self.api_key:
            return []
        async with httpx.AsyncClient() as client:
            try:
                today_cutoff = date.today().isoformat()
                all_results = []
                for p in range((page - 1) * 2 + 1, (page - 1) * 2 + 4):
                    resp = await client.get(
                        f"{self.BASE_URL}/discover/tv",
                        params={
                            "api_key": self.api_key,
                            "language": "en-US",
                            "with_genres": 16,
                            "with_original_language": "ja",
                            "sort_by": "popularity.desc",
                            "first_air_date.gte": today_cutoff,
                            "page": p
                        },
                        timeout=10.0
                    )
                    resp.raise_for_status()
                    all_results.extend(resp.json().get("results", []))
                return [self._normalize_series(s) for s in all_results if s.get("poster_path")]
            except Exception as e:
                logger.error(f"Error fetching announced anime: {e}")
                return []

    # Covers Hindi (hi), Tamil (ta), Telugu (te), Malayalam (ml), Kannada (kn)
    _INDIAN_LANGS = "hi|ta|te|ml|kn"
    _INDIAN_LANGS_BROAD = "hi|ta|te|ml|kn|en"

    @retry(stop=stop_after_attempt(2), wait=wait_exponential(min=1, max=3))
    async def get_indian_movies(self, page: int = 1) -> List[Dict[str, Any]]:
        """Fetch popular Indian movies (released in last 6 months). Fetches 2 pages."""
        if not self.api_key:
            return []
        async with httpx.AsyncClient() as client:
            try:
                # Recency filter: released in last 180 days
                recent_cutoff = (date.today() - timedelta(days=180)).isoformat()
                all_results = []
                for p in [page, page + 1]:
                    resp = await client.get(
                        f"{self.BASE_URL}/discover/movie",
                        params={
                            "api_key": self.api_key,
                            "language": "en-US",
                            "with_original_language": self._INDIAN_LANGS_BROAD,
                            "region": "IN",
                            "primary_release_date.gte": recent_cutoff,
                            "sort_by": "popularity.desc",
                            "page": p,
                        },
                        timeout=10.0
                    )
                    resp.raise_for_status()
                    all_results.extend(resp.json().get("results", []))
                return [self._normalize_movie(m) for m in all_results]
            except Exception as e:
                logger.error(f"Error fetching Indian movies: {e}")
                return []

    # ── NEW: BOOKMYSHOW STYLE INGRESS ─────────────────────────────────────────

    @retry(stop=stop_after_attempt(2), wait=wait_exponential(min=1, max=3))
    async def get_indian_now_playing(self, page: int = 1) -> List[Dict[str, Any]]:
        """Fetch Indian movies currently in theaters (region=IN)."""
        if not self.api_key: return []
        async with httpx.AsyncClient() as client:
            try:
                resp = await client.get(
                    f"{self.BASE_URL}/movie/now_playing",
                    params={
                        "api_key": self.api_key,
                        "language": "en-US",
                        "region": "IN",
                        "page": page,
                    },
                    timeout=10.0
                )
                resp.raise_for_status()
                # Filter to only include Indian original languages
                results = [m for m in resp.json().get("results", []) 
                          if m.get("original_language") in self._INDIAN_LANGS_BROAD.split('|')]
                return [self._normalize_movie(m) for m in results]
            except Exception as e:
                logger.error(f"Error fetching Indian now playing: {e}")
                return []

    @retry(stop=stop_after_attempt(2), wait=wait_exponential(min=1, max=3))
    async def get_indian_series(self, page: int = 1) -> List[Dict[str, Any]]:
        """Fetch popular Indian series on OTT platforms."""
        if not self.api_key:
            return []
        async with httpx.AsyncClient() as client:
            try:
                # OTT Provider IDs for India: Netflix(8), Amazon(119), Disney+Hotstar(122), SonyLIV(237), Zee5(232)
                resp = await client.get(
                    f"{self.BASE_URL}/discover/tv",
                    params={
                        "api_key": self.api_key,
                        "language": "en-US",
                        "with_original_language": self._INDIAN_LANGS,
                        "without_genres": "10766", # No Soaps/Serials
                        "watch_region": "IN",
                        "with_watch_providers": "8|119|122|237|232",
                        "sort_by": "popularity.desc",
                        "page": page,
                    },
                    timeout=10.0
                )
                resp.raise_for_status()
                data = resp.json().get("results", [])
                normalized = [self._normalize_series(s) for s in data]
                return [s for s in normalized if s.get("content_type") != "anime"]
            except Exception as e:
                logger.error(f"Error fetching Indian series: {e}")
                return []

    @retry(stop=stop_after_attempt(2), wait=wait_exponential(min=1, max=3))
    async def get_indian_movies_by_genre(self, genre_id: int, page: int = 1) -> List[Dict[str, Any]]:
        """Fetch Indian movies filtered by genre."""
        if not self.api_key:
            return []
        async with httpx.AsyncClient() as client:
            try:
                resp = await client.get(
                    f"{self.BASE_URL}/discover/movie",
                    params={
                        "api_key": self.api_key,
                        "language": "en-US",
                        "with_original_language": self._INDIAN_LANGS,
                        "with_genres": str(genre_id),
                        "sort_by": "popularity.desc",
                        "page": page,
                    },
                    timeout=10.0
                )
                resp.raise_for_status()
                return [self._normalize_movie(m) for m in resp.json().get("results", [])]
            except Exception as e:
                logger.error(f"Error fetching Indian movies by genre {genre_id}: {e}")
                return []

    @retry(stop=stop_after_attempt(2), wait=wait_exponential(min=1, max=3))
    async def get_indian_series_by_genre(self, genre_id: int, page: int = 1) -> List[Dict[str, Any]]:
        """Fetch Indian TV shows filtered by genre on OTT platforms."""
        if not self.api_key:
            return []
        async with httpx.AsyncClient() as client:
            try:
                resp = await client.get(
                    f"{self.BASE_URL}/discover/tv",
                    params={
                        "api_key": self.api_key,
                        "language": "en-US",
                        "with_original_language": self._INDIAN_LANGS,
                        "with_genres": str(genre_id),
                        "without_genres": "10766",
                        "watch_region": "IN",
                        "with_watch_providers": "8|119|122|237|232",
                        "sort_by": "popularity.desc",
                        "page": page,
                    },
                    timeout=10.0
                )
                resp.raise_for_status()
                data = resp.json().get("results", [])
                normalized = [self._normalize_series(s) for s in data]
                return [s for s in normalized if s.get("content_type") != "anime"]
            except Exception as e:
                logger.error(f"Error fetching Indian series by genre {genre_id}: {e}")
                return []

    @retry(stop=stop_after_attempt(2), wait=wait_exponential(min=1, max=3))
    async def get_indian_upcoming_movies(self, page: int = 1) -> List[Dict[str, Any]]:
        """Fetch upcoming Indian movies. Deep fetch: 5 pages."""
        if not self.api_key:
            return []
        async with httpx.AsyncClient() as client:
            try:
                today = date.today().isoformat()
                all_results = []
                # BookMyShow style deeper fetch for roadmap reliability
                for p in range(page, page + 2):
                    resp = await client.get(
                        f"{self.BASE_URL}/discover/movie",
                        params={
                            "api_key": self.api_key,
                            "language": "en-US",
                            "with_original_language": self._INDIAN_LANGS_BROAD,
                            "primary_release_date.gte": today,
                            "sort_by": "popularity.desc",
                            "page": p,
                        },
                        timeout=10.0
                    )
                    resp.raise_for_status()
                    all_results.extend(resp.json().get("results", []))
                return [self._normalize_movie(m) for m in all_results]
            except Exception as e:
                logger.error(f"Error fetching Indian upcoming movies: {e}")
                return []

    @retry(stop=stop_after_attempt(2), wait=wait_exponential(min=1, max=3))
    async def get_movie_details(self, tmdb_id: int) -> Optional[Dict[str, Any]]:
        """Fetch full movie details including title logo image."""
        if not self.api_key: return None
        async with httpx.AsyncClient() as client:
            try:
                resp = await client.get(
                    f"{self.BASE_URL}/movie/{tmdb_id}",
                    params={
                        "api_key": self.api_key,
                        "language": "en-US",
                        "append_to_response": "images",
                        "include_image_language": "en,null"
                    },
                    timeout=10.0
                )
                resp.raise_for_status()
                return self._normalize_movie(resp.json())
            except Exception as e:
                logger.error(f"Error fetching movie details {tmdb_id}: {e}")
                return None

    @retry(stop=stop_after_attempt(2), wait=wait_exponential(min=1, max=3))
    async def get_series_details(self, tmdb_id: int) -> Optional[Dict[str, Any]]:
        """Fetch full TV series details including title logo image."""
        if not self.api_key: return None
        async with httpx.AsyncClient() as client:
            try:
                resp = await client.get(
                    f"{self.BASE_URL}/tv/{tmdb_id}",
                    params={
                        "api_key": self.api_key,
                        "language": "en-US",
                        "append_to_response": "images",
                        "include_image_language": "en,null"
                    },
                    timeout=10.0
                )
                resp.raise_for_status()
                return self._normalize_series(resp.json())
            except Exception as e:
                logger.error(f"Error fetching series details {tmdb_id}: {e}")
                return None

    # TMDB Genre Mappings
    _GENRE_MAP_MOVIE = {
        28: "Action", 12: "Adventure", 16: "Animation", 35: "Comedy", 80: "Crime",
        99: "Documentary", 18: "Drama", 10751: "Family", 14: "Fantasy", 36: "History",
        27: "Horror", 10402: "Music", 9648: "Mystery", 10749: "Romance", 878: "Science Fiction",
        10770: "TV Movie", 53: "Thriller", 10752: "War", 37: "Western"
    }
    _GENRE_MAP_TV = {
        10759: "Action", 16: "Animation", 35: "Comedy", 80: "Crime", 99: "Documentary",
        18: "Drama", 10751: "Family", 10762: "Kids", 9648: "Mystery", 10763: "News",
        10764: "Reality", 10765: "Sci-Fi", 10766: "Soap", 10767: "Talk", 10768: "War", 37: "Western"
    }

    def _extract_logo_url(self, item: dict) -> Optional[str]:
        images = item.get("images")
        if isinstance(images, dict):
            logos = images.get("logos", [])
            if isinstance(logos, list) and logos:
                en_logo = next((l for l in logos if isinstance(l, dict) and l.get("iso_639_1") == "en"), logos[0])
                if isinstance(en_logo, dict):
                    fp = en_logo.get("file_path")
                    if fp: return f"{self.IMAGE_BASE}{fp}"
        return None

    def _normalize_movie(self, item: dict) -> dict:
        poster = item.get("poster_path")
        backdrop = item.get("backdrop_path")
        logo_url = self._extract_logo_url(item)
        gids = list(item.get("genre_ids", []))
        if not gids and "genres" in item:
            raw_genres = item["genres"]
            if isinstance(raw_genres, list):
                for g in raw_genres:
                    if isinstance(g, dict) and "id" in g:
                        gids.append(g["id"])
                    elif isinstance(g, int):
                        gids.append(g)
                    elif isinstance(g, str):
                        for k, v in self._GENRE_MAP_MOVIE.items():
                            if v.lower() == g.lower():
                                gids.append(k)
                                break
        genres = [self._GENRE_MAP_MOVIE[gid] for gid in gids if gid in self._GENRE_MAP_MOVIE]
        if "genres" in item and isinstance(item["genres"], list):
            for g in item["genres"]:
                if isinstance(g, str) and g not in genres:
                    genres.append(g)
        rd = item.get("release_date")
        original_language = item.get("original_language")
        
        content_type = "movie"
        if original_language == "ja" and ("Animation" in genres or 16 in gids):
            content_type = "anime"
        
        return {
            "id": str(item.get("id")),
            "tmdb_id": item.get("id"),
            "content_type": content_type,
            "title": item.get("title") or item.get("original_title", ""),
            "original_title": item.get("original_title"),
            "original_language": original_language,
            "synopsis": item.get("overview"),
            "poster_url": f"{self.IMAGE_BASE}{poster}" if poster else None,
            "backdrop_url": f"{self.BACKDROP_BASE}{backdrop}" if backdrop else None,
            "logo_url": logo_url,
            "title_logo": logo_url,
            "external_rating": item.get("vote_average"),
            "external_rating_source": "tmdb",
            "vote_count": item.get("vote_count", 0),
            "release_date": rd if rd else None,
            "genres": genres,
            "genre_ids": gids,
            "production_status": item.get("status")
        }

    def _normalize_series(self, item: dict) -> dict:
        poster = item.get("poster_path")
        backdrop = item.get("backdrop_path")
        logo_url = self._extract_logo_url(item)
        gids = list(item.get("genre_ids", []))
        if not gids and "genres" in item:
            raw_genres = item["genres"]
            if isinstance(raw_genres, list):
                for g in raw_genres:
                    if isinstance(g, dict) and "id" in g:
                        gids.append(g["id"])
                    elif isinstance(g, int):
                        gids.append(g)
                    elif isinstance(g, str):
                        for k, v in self._GENRE_MAP_TV.items():
                            if v.lower() == g.lower():
                                gids.append(k)
                                break
        genres = [self._GENRE_MAP_TV[gid] for gid in gids if gid in self._GENRE_MAP_TV]
        if "genres" in item and isinstance(item["genres"], list):
            for g in item["genres"]:
                if isinstance(g, str) and g not in genres:
                    genres.append(g)
        rd = item.get("first_air_date")
        original_language = item.get("original_language")
        origin_countries = item.get("origin_country", [])
        
        content_type = "series"
        if ("Animation" in genres or 16 in gids) and (original_language == "ja" or (isinstance(origin_countries, list) and "JP" in origin_countries)):
            content_type = "anime"

        return {
            "id": str(item.get("id")),
            "tmdb_id": item.get("id"),
            "content_type": content_type,
            "title": item.get("name") or item.get("original_name", ""),
            "original_title": item.get("original_name"),
            "original_language": original_language,
            "synopsis": item.get("overview"),
            "poster_url": f"{self.IMAGE_BASE}{poster}" if poster else None,
            "backdrop_url": f"{self.BACKDROP_BASE}{backdrop}" if backdrop else None,
            "logo_url": logo_url,
            "title_logo": logo_url,
            "external_rating": item.get("vote_average"),
            "external_rating_source": "tmdb",
            "vote_count": item.get("vote_count", 0),
            "release_date": rd if rd else None,
            "genres": genres,
            "genre_ids": gids,
            "total_seasons": item.get("number_of_seasons", 1),
            "total_episodes": item.get("number_of_episodes", 0),
            "seasons": [
                {
                    "season_number": s.get("season_number"),
                    "episode_count": s.get("episode_count")
                } for s in item.get("seasons", []) if s.get("season_number", 0) > 0
            ],
            "status": item.get("status")
        }

    @retry(stop=stop_after_attempt(2), wait=wait_exponential(min=1, max=3))
    async def get_now_playing_movies(self, page: int = 1) -> List[Dict[str, Any]]:
        """Fetch movies currently in theaters globally."""
        if not self.api_key: return []
        async with httpx.AsyncClient() as client:
            try:
                resp = await client.get(
                    f"{self.BASE_URL}/movie/now_playing",
                    params={
                        "api_key": self.api_key,
                        "language": "en-US",
                        "page": page,
                    },
                    timeout=10.0
                )
                resp.raise_for_status()
                return [self._normalize_movie(m) for m in resp.json().get("results", [])]
            except Exception as e:
                logger.error(f"Error fetching now playing movies: {e}")
                return []

    @retry(stop=stop_after_attempt(2), wait=wait_exponential(min=1, max=3))
    async def get_on_the_air_series(self, page: int = 1) -> List[Dict[str, Any]]:
        """Fetch TV shows currently on the air (airing recently or soon)."""
        if not self.api_key: return []
        async with httpx.AsyncClient() as client:
            try:
                resp = await client.get(
                    f"{self.BASE_URL}/tv/on_the_air",
                    params={
                        "api_key": self.api_key,
                        "language": "en-US",
                        "page": page,
                    },
                    timeout=10.0
                )
                resp.raise_for_status()
                data = resp.json().get("results", [])
                normalized = [self._normalize_series(s) for s in data]
                return [s for s in normalized if s.get("content_type") != "anime"]
            except Exception as e:
                logger.error(f"Error fetching on the air series: {e}")
                return []

    @retry(stop=stop_after_attempt(2), wait=wait_exponential(min=1, max=3))
    async def get_upcoming_series(self, page: int = 1) -> List[Dict[str, Any]]:
        """Fetch upcoming TV shows. Fetches 2 pages."""
        if not self.api_key: return []
        async with httpx.AsyncClient() as client:
            try:
                today = date.today().isoformat()
                all_results = []
                for p in [page, page + 1]:
                    resp = await client.get(
                        f"{self.BASE_URL}/discover/tv",
                        params={
                            "api_key": self.api_key,
                            "language": "en-US",
                            "first_air_date.gte": today,
                            "sort_by": "popularity.desc",
                            "page": p,
                        },
                        timeout=10.0
                    )
                    resp.raise_for_status()
                    all_results.extend(resp.json().get("results", []))
                normalized = [self._normalize_series(s) for s in all_results]
                return [s for s in normalized if s.get("content_type") != "anime"]
            except Exception as e:
                logger.error(f"Error fetching upcoming series: {e}")
                return []

    @retry(stop=stop_after_attempt(2), wait=wait_exponential(min=1, max=3))
    async def get_indian_upcoming_series(self, page: int = 1) -> List[Dict[str, Any]]:
        """Fetch upcoming Indian TV shows on OTT platforms. Fetches 2 pages."""
        if not self.api_key: return []
        async with httpx.AsyncClient() as client:
            try:
                today = date.today().isoformat()
                all_results = []
                for p in [page, page + 1]:
                    resp = await client.get(
                        f"{self.BASE_URL}/discover/tv",
                        params={
                            "api_key": self.api_key,
                            "language": "en-US",
                            "with_original_language": self._INDIAN_LANGS,
                            "without_genres": "10766",
                            "watch_region": "IN",
                            "with_watch_providers": "8|119|122|237|232",
                            "first_air_date.gte": today,
                            "sort_by": "popularity.desc",
                            "page": p,
                        },
                        timeout=10.0
                    )
                    resp.raise_for_status()
                    all_results.extend(resp.json().get("results", []))
                return [self._normalize_series(s) for s in all_results]
            except Exception as e:
                logger.error(f"Error fetching Indian upcoming series: {e}")
                return []

    def _normalize_person(self, p: Dict[str, Any], role: str = "cast") -> Dict[str, Any]:
        profile = p.get("profile_path")
        return {
            "tmdb_id": p.get("id"),
            "name": p.get("name") or p.get("original_name", "Unknown"),
            "profile_url": f"{self.IMAGE_BASE}{profile}" if profile else None,
            "profile_path": profile,
            "character": p.get("character"),
            "job": p.get("job"),
            "department": p.get("known_for_department") or p.get("department"),
            "role": role,
            "display_order": p.get("order", 0)
        }

    @retry(stop=stop_after_attempt(2), wait=wait_exponential(min=1, max=3))
    async def get_credits(self, tmdb_id: int, content_type: str) -> Dict[str, Any]:
        """Fetch cast and crew for a movie or TV show."""
        if not self.api_key: return {"cast": [], "crew": []}
        path = "movie" if content_type == "movie" else "tv"
        async with httpx.AsyncClient() as client:
            try:
                resp = await client.get(
                    f"{self.BASE_URL}/{path}/{tmdb_id}/credits",
                    params={"api_key": self.api_key, "language": "en-US"},
                    timeout=10.0
                )
                if resp.status_code != 200:
                    return {"cast": [], "crew": []}
                data = resp.json()
                return {
                    "cast": [self._normalize_person(p, "cast") for p in data.get("cast", [])[:15]],
                    "crew": [self._normalize_person(p, "crew") for p in data.get("crew", []) if p.get("job") in ["Director", "Producer"]]
                }
            except Exception as e:
                logger.error(f"Error fetching credits for {content_type} {tmdb_id}: {e}")
                return {"cast": [], "crew": []}

    @retry(stop=stop_after_attempt(2), wait=wait_exponential(min=1, max=3))
    async def get_season_details(self, tmdb_id: int, season_number: int) -> Dict[str, Any]:
        """Fetch episode details for a TV season from TMDB."""
        if not self.api_key: return {"episodes": []}
        async with httpx.AsyncClient() as client:
            try:
                resp = await client.get(
                    f"{self.BASE_URL}/tv/{tmdb_id}/season/{season_number}",
                    params={"api_key": self.api_key, "language": "en-US"},
                    timeout=10.0
                )
                if resp.status_code != 200:
                    return {"episodes": []}
                data = resp.json()
                episodes = []
                for ep in data.get("episodes", []):
                    still = ep.get("still_path")
                    episodes.append({
                        "episode_number": ep.get("episode_number"),
                        "title": ep.get("name"),
                        "synopsis": ep.get("overview"),
                        "still_url": f"{self.IMAGE_BASE}{still}" if still else None,
                        "runtime_minutes": ep.get("runtime"),
                        "air_date": ep.get("air_date"),
                        "vote_average": ep.get("vote_average")
                    })
                return {
                    "season_number": season_number,
                    "episodes": episodes
                }
            except Exception as e:
                logger.error(f"Error fetching season {season_number} details for TV {tmdb_id}: {e}")
                return {"episodes": []}

    @retry(stop=stop_after_attempt(2), wait=wait_exponential(min=1, max=3))
    async def search_content(self, query: str, page: int = 1) -> List[Dict[str, Any]]:
        """Search multi (movies and TV shows) on TMDB."""
        if not self.api_key or not query: return []
        async with httpx.AsyncClient() as client:
            try:
                resp = await client.get(
                    f"{self.BASE_URL}/search/multi",
                    params={"api_key": self.api_key, "language": "en-US", "query": query, "page": page},
                    timeout=10.0
                )
                if resp.status_code != 200:
                    return []
                results = resp.json().get("results", [])
                normalized = []
                for item in results:
                    media_type = item.get("media_type")
                    if media_type == "movie":
                        normalized.append(self._normalize_movie(item))
                    elif media_type == "tv":
                        normalized.append(self._normalize_series(item))
                return normalized
            except Exception as e:
                logger.error(f"Error searching content for '{query}': {e}")
                return []

    async def search_multi(self, query: str, page: int = 1) -> List[Dict[str, Any]]:
        return await self.search_content(query, page)

    @retry(stop=stop_after_attempt(2), wait=wait_exponential(min=1, max=3))
    async def get_similar(self, tmdb_id: int, content_type: str) -> List[Dict[str, Any]]:
        """Fetch highly relevant recommended/similar movies or TV shows from TMDB."""
        if not self.api_key: return []
        path = "movie" if content_type == "movie" else "tv"
        async with httpx.AsyncClient() as client:
            try:
                # 1. Try recommendations endpoint first for curated high-relevance matches
                resp = await client.get(
                    f"{self.BASE_URL}/{path}/{tmdb_id}/recommendations",
                    params={"api_key": self.api_key, "language": "en-US", "page": 1},
                    timeout=10.0
                )
                if resp.status_code == 200:
                    results = resp.json().get("results", [])
                    if results:
                        if content_type == "movie":
                            return [self._normalize_movie(m) for m in results[:10]]
                        return [self._normalize_series(s) for s in results[:10]]

                # 2. Fallback to similar endpoint if recommendations is empty
                resp_sim = await client.get(
                    f"{self.BASE_URL}/{path}/{tmdb_id}/similar",
                    params={"api_key": self.api_key, "language": "en-US", "page": 1},
                    timeout=10.0
                )
                if resp_sim.status_code == 200:
                    results_sim = resp_sim.json().get("results", [])
                    if content_type == "movie":
                        return [self._normalize_movie(m) for m in results_sim[:10]]
                    return [self._normalize_series(s) for s in results_sim[:10]]

                return []
            except Exception as e:
                logger.error(f"Error fetching similar/recommendations for {content_type} {tmdb_id}: {e}")
                return []

    def _normalize_person(self, p: dict, role_type: str) -> dict:
        profile = p.get("profile_path")
        return {
            "id": str(p.get("id")),
            "tmdb_id": p.get("id"),
            "name": p.get("name"),
            "original_name": p.get("original_name"),
            "profile_url": f"{self.IMAGE_BASE}{profile}" if profile else None,
            "character": p.get("character") if role_type == "cast" else None,
            "job": p.get("job") if role_type == "crew" else None,
            "department": p.get("department"),
            "known_for_department": p.get("known_for_department")
        }

    @retry(stop=stop_after_attempt(2), wait=wait_exponential(min=1, max=3))
    async def search_people(self, query: str, page: int = 1) -> List[Dict[str, Any]]:
        """Search for people (actors, directors) on TMDB."""
        if not self.api_key: return []
        async with httpx.AsyncClient() as client:
            try:
                resp = await client.get(
                    f"{self.BASE_URL}/search/person",
                    params={
                        "api_key": self.api_key,
                        "language": "en-US",
                        "query": query,
                        "page": page,
                        "include_adult": "false",
                    },
                    timeout=10.0
                )
                resp.raise_for_status()
                results = resp.json().get("results", [])
                return [self._normalize_person(p, "search") for p in results]
            except Exception as e:
                logger.error(f"TMDB search_people failed: {e}")
                return []

    @retry(stop=stop_after_attempt(2), wait=wait_exponential(min=1, max=3))
    async def get_person_details(self, person_id: int) -> Dict[str, Any]:
        """Fetch detailed person profile."""
        if not self.api_key: return {}
        async with httpx.AsyncClient() as client:
            try:
                resp = await client.get(
                    f"{self.BASE_URL}/person/{person_id}",
                    params={"api_key": self.api_key, "language": "en-US"},
                    timeout=10.0
                )
                resp.raise_for_status()
                data = resp.json()
                profile = data.get("profile_path")
                return {
                    "tmdb_id": data.get("id"),
                    "name": data.get("name"),
                    "biography": data.get("biography"),
                    "birthday": data.get("birthday"),
                    "place_of_birth": data.get("place_of_birth"),
                    "profile_url": f"{self.IMAGE_BASE}{profile}" if profile else None,
                    "known_for_department": data.get("known_for_department"),
                }
            except Exception as e:
                logger.error(f"TMDB get_person_details failed: {e}")
                return {}

    @retry(stop=stop_after_attempt(2), wait=wait_exponential(min=1, max=3))
    async def get_person_combined_credits(self, person_id: int) -> List[Dict[str, Any]]:
        """Fetch all movie and TV credits for a person."""
        if not self.api_key: return []
        async with httpx.AsyncClient() as client:
            try:
                resp = await client.get(
                    f"{self.BASE_URL}/person/{person_id}/combined_credits",
                    params={"api_key": self.api_key, "language": "en-US"},
                    timeout=10.0
                )
                resp.raise_for_status()
                data = resp.json()
                
                # Combine cast and crew (Acting, Directing, Writing, Production focus)
                cast = data.get("cast", [])
                for c in cast:
                    c["credit_type"] = "cast"
                    c["department"] = "Acting"
                    c["job"] = "Actor"

                crew = [c for c in data.get("crew", []) if c.get("department") in ["Directing", "Writing", "Production"]]
                for c in crew:
                    c["credit_type"] = "crew"

                all_credits = cast + crew

                # Deduplicate per role and normalize
                seen = set()
                results = []
                for c in all_credits:
                    cid = c.get("id")
                    ctype = c.get("media_type") # movie or tv
                    dept = c.get("department", "")
                    job = c.get("job", "")
                    character = c.get("character", "")
                    key = f"{ctype}_{cid}_{dept}_{job}_{character}"
                    if key not in seen:
                        seen.add(key)
                        if ctype == "movie":
                            normalized = self._normalize_movie(c)
                        else:
                            normalized = self._normalize_series(c)

                        normalized["department"] = dept
                        normalized["job"] = job
                        normalized["character"] = character
                        normalized["credit_type"] = c.get("credit_type")
                        results.append(normalized)

                # Sort by popularity
                results.sort(key=lambda x: x.get("popularity", 0), reverse=True)
                return results
            except Exception as e:
                logger.error(f"TMDB get_person_combined_credits failed: {e}")
                return []
    @retry(stop=stop_after_attempt(2), wait=wait_exponential(min=1, max=3))
    async def get_season_details(self, tmdb_id: int, season_number: int) -> Dict[str, Any]:
        """Fetch all episodes for a specific season."""
        if not self.api_key: return {"episodes": []}
        async with httpx.AsyncClient() as client:
            try:
                resp = await client.get(
                    f"{self.BASE_URL}/tv/{tmdb_id}/season/{season_number}",
                    params={"api_key": self.api_key, "language": "en-US"},
                    timeout=10.0
                )
                resp.raise_for_status()
                data = resp.json()
                episodes = data.get("episodes", [])
                
                return {
                    "season_number": data.get("season_number"),
                    "name": data.get("name"),
                    "overview": data.get("overview"),
                    "episodes": [self._normalize_episode(e) for e in episodes]
                }
            except Exception as e:
                logger.error(f"Error fetching season details for {tmdb_id} S{season_number}: {e}")
                return {"episodes": []}

    def _normalize_episode(self, e: dict) -> dict:
        still = e.get("still_path")
        return {
            "episode_number": e.get("episode_number"),
            "season_number": e.get("season_number"),
            "title": e.get("name"),
            "synopsis": e.get("overview"),
            "air_date": e.get("air_date"),
            "runtime": e.get("runtime"),
            "thumbnail_url": f"{self.IMAGE_BASE}{still}" if still else None,
            "vote_average": e.get("vote_average")
        }

    @retry(stop=stop_after_attempt(2), wait=wait_exponential(min=1, max=3))
    async def get_watch_providers(self, tmdb_id: int, content_type: str) -> Dict[str, Any]:
        """Fetch watch providers (streaming platforms) for a movie or TV show."""
        if not self.api_key: return {}
        path = "movie" if content_type == "movie" else "tv"
        async with httpx.AsyncClient() as client:
            try:
                resp = await client.get(
                    f"{self.BASE_URL}/{path}/{tmdb_id}/watch/providers",
                    params={"api_key": self.api_key},
                    timeout=10.0
                )
                resp.raise_for_status()
                return resp.json().get("results", {})
            except Exception as e:
                logger.error(f"Error fetching watch providers for {content_type} {tmdb_id}: {e}")
                return {}

    @retry(stop=stop_after_attempt(2), wait=wait_exponential(min=1, max=3))
    async def get_discover_by_provider(self, content_type: str, provider_id: int, page: int = 1) -> List[Dict[str, Any]]:
        """Fetch movies or TV shows available on a specific streaming provider (e.g. Netflix, Prime, Apple TV+)."""
        if not self.api_key: return []
        path = "movie" if content_type == "movie" else "tv"
        
        # Provider ID Aliases & Expanded Channel Maps
        provider_map = {
            8: "8|175|1796",            # Netflix
            119: "119|9|2100",          # Prime Video
            122: "122|337|2336",        # JioHotstar
            232: "232",                 # Zee5
            237: "237|2180",            # SonyLIV
            350: "350",                 # Apple TV+ ONLY (Excludes iTunes Rental Store 2)
            561: "561|564",            # Lionsgate Play
            564: "561|564",            # Lionsgate Play
            386: "386|387",            # Peacock
            15: "15",                   # Hulu
            1899: "1899|384",          # Max / HBO Max
            531: "531",                 # Paramount+
            283: "283|1968",            # Crunchyroll
            201: "201",                 # MUBI
            315: "315",                 # Hoichoi
        }
        provider_query = provider_map.get(provider_id, str(provider_id))

        params = {
            "api_key": self.api_key,
            "language": "en-US",
            "page": page,
            "sort_by": "popularity.desc",
            "with_watch_providers": provider_query,
            "with_ott_monetization_types": "flatrate|free|ads",
            "watch_region": "IN",
        }
        if content_type == "series":
            params["without_genres"] = "10766" # No soaps
        elif content_type == "anime":
            params["with_original_language"] = "ja"
            params["with_genres"] = "16" # Animation

        async with httpx.AsyncClient() as client:
            try:
                resp = await client.get(
                    f"{self.BASE_URL}/discover/{path}",
                    params=params,
                    timeout=10.0
                )
                resp.raise_for_status()
                data = resp.json().get("results", [])

                # If region IN filter returns fewer than 15 items, query US region or global without watch_region restriction
                if len(data) < 15:
                    params_us = params.copy()
                    params_us["watch_region"] = "US"
                    resp_us = await client.get(
                        f"{self.BASE_URL}/discover/{path}",
                        params=params_us,
                        timeout=10.0
                    )
                    if resp_us.status_code == 200:
                        data_us = resp_us.json().get("results", [])
                        seen_ids = {m["id"] for m in data}
                        for g in data_us:
                            if g["id"] not in seen_ids:
                                data.append(g)

                # Fallback if still under 15 items: query page 2 or without watch_region
                if len(data) < 15:
                    params_global = params.copy()
                    params_global.pop("watch_region", None)
                    resp_g = await client.get(
                        f"{self.BASE_URL}/discover/{path}",
                        params=params_global,
                        timeout=10.0
                    )
                    if resp_g.status_code == 200:
                        data_g = resp_g.json().get("results", [])
                        seen_ids = {m["id"] for m in data}
                        for g in data_g:
                            if g["id"] not in seen_ids:
                                data.append(g)

                if content_type == "movie":
                    return [self._normalize_movie(m) for m in data]
                elif content_type == "anime":
                    res = [self._normalize_series(s) for s in data]
                    return [s for s in res if s.get("content_type") == "anime"]
                else: # series
                    res = [self._normalize_series(s) for s in data]
                    return [s for s in res if s.get("content_type") != "anime"]
            except Exception as e:
                logger.error(f"Error discovering by provider {provider_id} for {content_type}: {e}")
                return []

    @retry(stop=stop_after_attempt(2), wait=wait_exponential(min=1, max=3))
    async def get_raw_details(self, tmdb_id: int, content_type: str) -> Dict[str, Any]:
        """Fetch raw movie or TV details from TMDB."""
        if not self.api_key: return {}
        path = "movie" if content_type == "movie" else "tv"
        async with httpx.AsyncClient() as client:
            try:
                resp = await client.get(
                    f"{self.BASE_URL}/{path}/{tmdb_id}",
                    params={"api_key": self.api_key},
                    timeout=10.0
                )
                resp.raise_for_status()
                return resp.json()
            except Exception as e:
                logger.error(f"Error fetching raw details for {content_type} {tmdb_id}: {e}")
                return {}

    @retry(stop=stop_after_attempt(2), wait=wait_exponential(min=1, max=3))
    async def get_title_logo(self, tmdb_id: int, content_type: str) -> Optional[str]:
        """Fetch title logo PNG image URL directly from TMDB images endpoint."""
        if not self.api_key: return None
        path = "movie" if content_type == "movie" else "tv"
        async with httpx.AsyncClient() as client:
            try:
                resp = await client.get(
                    f"{self.BASE_URL}/{path}/{tmdb_id}/images",
                    params={"api_key": self.api_key},
                    timeout=10.0
                )
                resp.raise_for_status()
                logos = resp.json().get("logos", [])
                if logos:
                    # 1. English logo
                    en_logo = next((l for l in logos if isinstance(l, dict) and l.get("iso_639_1") == "en"), None)
                    if en_logo and en_logo.get("file_path"):
                        return f"{self.IMAGE_BASE}{en_logo['file_path']}"
                    
                    # 2. Null language logo (universal/textless logo)
                    null_logo = next((l for l in logos if isinstance(l, dict) and not l.get("iso_639_1")), None)
                    if null_logo and null_logo.get("file_path"):
                        return f"{self.IMAGE_BASE}{null_logo['file_path']}"
                    
                    # 3. Top voted logo regardless of language
                    valid_logos = [l for l in logos if isinstance(l, dict) and l.get("file_path")]
                    if valid_logos:
                        valid_logos.sort(key=lambda x: x.get("vote_count", 0), reverse=True)
                        return f"{self.IMAGE_BASE}{valid_logos[0]['file_path']}"
                return None
            except Exception as e:
                logger.error(f"Error fetching title logo for {content_type} {tmdb_id}: {e}")
                return None

    @retry(stop=stop_after_attempt(2), wait=wait_exponential(min=1, max=3))
    async def get_trailer_key(self, tmdb_id: int, content_type: str = "movie") -> Optional[str]:
        """Fetch YouTube trailer video key for a movie or TV show from TMDB."""
        if not self.api_key: return None
        path = "movie" if content_type == "movie" else "tv"
        async with httpx.AsyncClient() as client:
            try:
                resp = await client.get(
                    f"{self.BASE_URL}/{path}/{tmdb_id}/videos",
                    params={"api_key": self.api_key, "language": "en-US"},
                    timeout=10.0
                )
                resp.raise_for_status()
                videos = resp.json().get("results", [])
                
                # 1. Official YouTube Trailer
                official_trailer = next((v for v in videos if isinstance(v, dict) and v.get("site") == "YouTube" and v.get("type") == "Trailer" and v.get("official")), None)
                if official_trailer and official_trailer.get("key"):
                    return official_trailer["key"]

                # 2. Any YouTube Trailer
                yt_trailer = next((v for v in videos if isinstance(v, dict) and v.get("site") == "YouTube" and v.get("type") == "Trailer"), None)
                if yt_trailer and yt_trailer.get("key"):
                    return yt_trailer["key"]

                # 3. Any YouTube Teaser / Video
                any_yt = next((v for v in videos if isinstance(v, dict) and v.get("site") == "YouTube" and v.get("key")), None)
                if any_yt:
                    return any_yt["key"]

                return None
            except Exception as e:
                logger.error(f"Error fetching trailer for {content_type} {tmdb_id}: {e}")
                return None

    @retry(stop=stop_after_attempt(2), wait=wait_exponential(min=1, max=3))
    async def get_all_videos(self, tmdb_id: int, content_type: str = "movie") -> List[Dict[str, Any]]:
        """Fetch all YouTube videos (trailers, teasers, featurettes) for a content item."""
        if not self.api_key: return []
        path = "movie" if content_type == "movie" else "tv"
        async with httpx.AsyncClient() as client:
            try:
                resp = await client.get(
                    f"{self.BASE_URL}/{path}/{tmdb_id}/videos",
                    params={"api_key": self.api_key, "language": "en-US"},
                    timeout=10.0
                )
                resp.raise_for_status()
                videos = resp.json().get("results", [])
                yt_videos = []
                ignored_words = ["recap", "behind", "featurette", "interview", "blooper", "promo", "clip", "making"]
                for v in videos:
                    if isinstance(v, dict) and v.get("site") == "YouTube" and v.get("key"):
                        v_type = v.get("type", "")
                        v_name = (v.get("name") or "").lower()
                        # Only allow Trailer or Teaser types, ignoring promos/recaps
                        if v_type in ["Trailer", "Teaser"] and not any(w in v_name for w in ignored_words):
                            yt_videos.append({
                                "name": v.get("name") or v_type or "Trailer",
                                "type": v_type or "Trailer",
                                "key": v["key"]
                            })
                        if len(yt_videos) >= 4:
                            break
                return yt_videos
            except Exception as e:
                logger.error(f"Error fetching all videos for {content_type} {tmdb_id}: {e}")
                return []

    @retry(stop=stop_after_attempt(2), wait=wait_exponential(min=1, max=3))
    async def search_trailer_by_title(self, title: str, content_type: str = "movie") -> Optional[str]:
        """Search TMDB for title and return trailer key if direct ID had no videos."""
        if not self.api_key or not title: return None
        path = "search/movie" if content_type == "movie" else "search/tv"
        async with httpx.AsyncClient() as client:
            try:
                resp = await client.get(
                    f"{self.BASE_URL}/{path}",
                    params={"api_key": self.api_key, "query": title, "language": "en-US"},
                    timeout=10.0
                )
                if resp.status_code == 200:
                    results = resp.json().get("results", [])
                    for r in results[:3]:
                        found_id = r.get("id")
                        if found_id:
                            key = await self.get_trailer_key(found_id, content_type)
                            if key: return key
                return None
            except Exception as e:
                logger.error(f"Error searching trailer by title '{title}': {e}")
                return None

    @retry(stop=stop_after_attempt(2), wait=wait_exponential(min=1, max=3))
    async def get_movie_production_status(self, tmdb_id: int) -> Optional[Dict[str, Any]]:
        """Fetch just enough to classify announced/not for a movie."""
        if not self.api_key:
            return None
        client = self._client()
        try:
            resp = await client.get(
                f"{self.BASE_URL}/movie/{tmdb_id}",
                params={"api_key": self.api_key, "language": "en-US"},
                timeout=8.0
            )
            resp.raise_for_status()
            data = resp.json()
            return {
                "tmdb_id": tmdb_id,
                "status": data.get("status"),
                "release_date": data.get("release_date") or None,
                "title": data.get("title"),
                "poster_url": f"{self.IMAGE_BASE}{data['poster_path']}" if data.get("poster_path") else None,
            }
        except Exception as e:
            logger.error(f"get_movie_production_status failed for {tmdb_id}: {e}")
            return None

    @retry(stop=stop_after_attempt(2), wait=wait_exponential(min=1, max=3))
    async def get_series_pending_season(self, tmdb_id: int) -> Optional[Dict[str, Any]]:
        """Fetch series details to detect pending undated seasons or un-aired new shows."""
        if not self.api_key:
            return None
        client = self._client()
        try:
            resp = await client.get(
                f"{self.BASE_URL}/tv/{tmdb_id}",
                params={"api_key": self.api_key, "language": "en-US"},
                timeout=8.0
            )
            resp.raise_for_status()
            data = resp.json()

            status = data.get("status")
            in_production = data.get("in_production", False)
            first_air = data.get("first_air_date") or None
            seasons = data.get("seasons", [])

            today = date.today()
            aired_season_numbers = []
            undated_future_seasons = []
            for s in seasons:
                sn = s.get("season_number")
                if not sn or sn <= 0:
                    continue
                ad = s.get("air_date")
                if ad:
                    try:
                        ad_parsed = date.fromisoformat(ad[:10])
                        if ad_parsed <= today:
                            aired_season_numbers.append(sn)
                            continue
                    except Exception:
                        pass
                if not ad:
                    undated_future_seasons.append(sn)

            highest_aired = max(aired_season_numbers) if aired_season_numbers else 0
            real_pending = [sn for sn in undated_future_seasons if sn > highest_aired]

            pending_season_number = min(real_pending) if real_pending else None
            is_new_show = (highest_aired == 0 and not first_air)

            return {
                "tmdb_id": tmdb_id,
                "status": status,
                "in_production": in_production,
                "pending_season_number": pending_season_number,
                "is_new_show": is_new_show,
                "first_air_date": first_air,
            }
        except Exception as e:
            logger.error(f"get_series_pending_season failed for {tmdb_id}: {e}")
            return None

    @retry(stop=stop_after_attempt(2), wait=wait_exponential(min=1, max=3))
    async def get_announced_candidate_pool_movies(self) -> List[Dict[str, Any]]:
        """Collect movie candidate IDs for verification."""
        if not self.api_key:
            return []
        client = self._client()
        try:
            async def fetch_pages(endpoint: str, extra_params: dict, pages: int):
                out = []
                for p in range(1, pages + 1):
                    params = {"api_key": self.api_key, "language": "en-US", "page": p, **extra_params}
                    r = await client.get(f"{self.BASE_URL}{endpoint}", params=params, timeout=8.0)
                    if r.status_code == 200:
                        out.extend(r.json().get("results", []))
                return out

            upcoming = await fetch_pages("/movie/upcoming", {}, pages=3)
            popular = await fetch_pages("/movie/popular", {}, pages=2)
            trending_no_date = await fetch_pages(
                "/discover/movie", {"sort_by": "popularity.desc"}, pages=2
            )

            combined = upcoming + popular + trending_no_date
            seen = set()
            candidates = []
            for m in combined:
                mid = m.get("id")
                if mid and mid not in seen:
                    seen.add(mid)
                    candidates.append(mid)
            return candidates
        except Exception as e:
            logger.error(f"get_announced_candidate_pool_movies failed: {e}")
            return []

    @retry(stop=stop_after_attempt(2), wait=wait_exponential(min=1, max=3))
    async def get_announced_candidate_pool_series(self) -> List[Dict[str, Any]]:
        """Collect series candidate IDs for verification."""
        if not self.api_key:
            return []
        client = self._client()
        try:
            async def fetch_pages(endpoint: str, extra_params: dict, pages: int):
                out = []
                for p in range(1, pages + 1):
                    params = {"api_key": self.api_key, "language": "en-US", "page": p, **extra_params}
                    r = await client.get(f"{self.BASE_URL}{endpoint}", params=params, timeout=8.0)
                    if r.status_code == 200:
                        out.extend(r.json().get("results", []))
                return out

            popular = await fetch_pages("/tv/popular", {}, pages=3)
            on_air = await fetch_pages("/tv/on_the_air", {}, pages=2)
            top_rated = await fetch_pages("/tv/top_rated", {}, pages=1)

            combined = popular + on_air + top_rated
            seen = set()
            candidates = []
            for s in combined:
                sid = s.get("id")
                if sid and sid not in seen:
                    seen.add(sid)
                    candidates.append(sid)
            return candidates
        except Exception as e:
            logger.error(f"get_announced_candidate_pool_series failed: {e}")
            return []

