import httpx
from datetime import date, datetime
from typing import List, Dict, Any, Optional
from app.core.config import settings
import logging
from tenacity import retry, stop_after_attempt, wait_exponential

logger = logging.getLogger('mambo.tmdb')

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
                data = resp.json()
                return [self._normalize_series(s) for s in data.get("results", [])]
            except Exception as e:
                logger.error(f"Error fetching trending series: {e}")
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
                data = resp.json()
                return [self._normalize_series(s) for s in data.get("results", [])]
            except Exception as e:
                logger.error(f"Error fetching popular series: {e}")
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
                return [self._normalize_series(s) for s in resp.json().get("results", [])]
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
                return [self._normalize_series(s) for s in resp.json().get("results", [])]
            except Exception as e:
                logger.error(f"Error fetching series by genre {genre_id}: {e}")
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
    async def get_upcoming_movies(self, page: int = 1) -> List[Dict[str, Any]]:
        """Fetch upcoming movies (future release dates)."""
        if not self.api_key:
            return []
        async with httpx.AsyncClient() as client:
            try:
                resp = await client.get(
                    f"{self.BASE_URL}/movie/upcoming",
                    params={"api_key": self.api_key, "language": "en-US", "page": page},
                    timeout=10.0
                )
                resp.raise_for_status()
                return [self._normalize_movie(m) for m in resp.json().get("results", [])]
            except Exception as e:
                logger.error(f"Error fetching upcoming movies: {e}")
                return []

    # Covers Hindi (hi), Tamil (ta), Telugu (te), Malayalam (ml), Kannada (kn)
    _INDIAN_LANGS = "hi|ta|te|ml|kn"
    _INDIAN_LANGS_BROAD = "hi|ta|te|ml|kn|en"

    @retry(stop=stop_after_attempt(2), wait=wait_exponential(min=1, max=3))
    async def get_indian_movies(self, page: int = 1) -> List[Dict[str, Any]]:
        """Fetch popular Indian movies (Bollywood with major hits)."""
        if not self.api_key:
            return []
        async with httpx.AsyncClient() as client:
            try:
                resp = await client.get(
                    f"{self.BASE_URL}/discover/movie",
                    params={
                        "api_key": self.api_key,
                        "language": "en-US",
                        "with_original_language": self._INDIAN_LANGS_BROAD,
                        "region": "IN",
                        "sort_by": "popularity.desc",
                        "page": page,
                    },
                    timeout=10.0
                )
                resp.raise_for_status()
                return [self._normalize_movie(m) for m in resp.json().get("results", [])]
            except Exception as e:
                logger.error(f"Error fetching Indian movies: {e}")
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
                return [self._normalize_series(s) for s in resp.json().get("results", [])]
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
                return [self._normalize_series(s) for s in resp.json().get("results", [])]
            except Exception as e:
                logger.error(f"Error fetching Indian series by genre {genre_id}: {e}")
                return []

    @retry(stop=stop_after_attempt(2), wait=wait_exponential(min=1, max=3))
    async def get_indian_upcoming_movies(self, page: int = 1) -> List[Dict[str, Any]]:
        """Fetch upcoming Indian movies."""
        if not self.api_key:
            return []
        async with httpx.AsyncClient() as client:
            try:
                today = date.today().isoformat()
                resp = await client.get(
                    f"{self.BASE_URL}/discover/movie",
                    params={
                        "api_key": self.api_key,
                        "language": "en-US",
                        "with_original_language": self._INDIAN_LANGS_BROAD,
                        "primary_release_date.gte": today,
                        "sort_by": "popularity.desc",
                        "page": page,
                    },
                    timeout=10.0
                )
                resp.raise_for_status()
                return [self._normalize_movie(m) for m in resp.json().get("results", [])]
            except Exception as e:
                logger.error(f"Error fetching Indian upcoming movies: {e}")
                return []

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

    def _normalize_movie(self, item: dict) -> dict:
        poster = item.get("poster_path")
        backdrop = item.get("backdrop_path")
        gids = item.get("genre_ids", [])
        genres = [self._GENRE_MAP_MOVIE[gid] for gid in gids if gid in self._GENRE_MAP_MOVIE]
        rd = item.get("release_date")
        
        return {
            "tmdb_id": item.get("id"),
            "content_type": "movie",
            "title": item.get("title") or item.get("original_title", ""),
            "original_title": item.get("original_title"),
            "synopsis": item.get("overview"),
            "poster_url": f"{self.IMAGE_BASE}{poster}" if poster else None,
            "backdrop_url": f"{self.BACKDROP_BASE}{backdrop}" if backdrop else None,
            "external_rating": item.get("vote_average"),
            "external_rating_source": "tmdb",
            "release_date": rd if rd else None,
            "genres": genres
        }

    def _normalize_series(self, item: dict) -> dict:
        poster = item.get("poster_path")
        backdrop = item.get("backdrop_path")
        gids = item.get("genre_ids", [])
        genres = [self._GENRE_MAP_TV[gid] for gid in gids if gid in self._GENRE_MAP_TV]
        rd = item.get("first_air_date")

        return {
            "tmdb_id": item.get("id"),
            "content_type": "series",
            "title": item.get("name") or item.get("original_name", ""),
            "original_title": item.get("original_name"),
            "synopsis": item.get("overview"),
            "poster_url": f"{self.IMAGE_BASE}{poster}" if poster else None,
            "backdrop_url": f"{self.BACKDROP_BASE}{backdrop}" if backdrop else None,
            "external_rating": item.get("vote_average"),
            "external_rating_source": "tmdb",
            "release_date": rd if rd else None,
            "genres": genres
        }

    @retry(stop=stop_after_attempt(2), wait=wait_exponential(min=1, max=3))
    async def get_upcoming_series(self, page: int = 1) -> List[Dict[str, Any]]:
        """Fetch upcoming TV shows using discover (first_air_date.gte)."""
        if not self.api_key: return []
        async with httpx.AsyncClient() as client:
            try:
                today = date.today().isoformat()
                resp = await client.get(
                    f"{self.BASE_URL}/discover/tv",
                    params={
                        "api_key": self.api_key,
                        "language": "en-US",
                        "first_air_date.gte": today,
                        "sort_by": "popularity.desc",
                        "page": page,
                    },
                    timeout=10.0
                )
                resp.raise_for_status()
                return [self._normalize_series(s) for s in resp.json().get("results", [])]
            except Exception as e:
                logger.error(f"Error fetching upcoming series: {e}")
                return []

    @retry(stop=stop_after_attempt(2), wait=wait_exponential(min=1, max=3))
    async def get_indian_upcoming_series(self, page: int = 1) -> List[Dict[str, Any]]:
        """Fetch upcoming Indian TV shows on OTT platforms."""
        if not self.api_key: return []
        async with httpx.AsyncClient() as client:
            try:
                today = date.today().isoformat()
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
                        "page": page,
                    },
                    timeout=10.0
                )
                resp.raise_for_status()
                return [self._normalize_series(s) for s in resp.json().get("results", [])]
            except Exception as e:
                logger.error(f"Error fetching Indian upcoming series: {e}")
                return []

    # ── NEW: CREDITS & SIMILAR ────────────────────────────────────────────────

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
                resp.raise_for_status()
                data = resp.json()
                return {
                    "cast": [self._normalize_person(p, "cast") for p in data.get("cast", [])[:15]],
                    "crew": [self._normalize_person(p, "crew") for p in data.get("crew", []) if p.get("job") in ["Director", "Producer"]]
                }
            except Exception as e:
                logger.error(f"Error fetching credits for {content_type} {tmdb_id}: {e}")
                return {"cast": [], "crew": []}

    @retry(stop=stop_after_attempt(2), wait=wait_exponential(min=1, max=3))
    async def get_similar(self, tmdb_id: int, content_type: str) -> List[Dict[str, Any]]:
        """Fetch similar movies or TV shows."""
        if not self.api_key: return []
        path = "movie" if content_type == "movie" else "tv"
        async with httpx.AsyncClient() as client:
            try:
                resp = await client.get(
                    f"{self.BASE_URL}/{path}/{tmdb_id}/similar",
                    params={"api_key": self.api_key, "language": "en-US", "page": 1},
                    timeout=10.0
                )
                resp.raise_for_status()
                data = resp.json()
                results = data.get("results", [])
                if content_type == "movie":
                    return [self._normalize_movie(m) for m in results[:10]]
                return [self._normalize_series(s) for s in results[:10]]
            except Exception as e:
                logger.error(f"Error fetching similar for {content_type} {tmdb_id}: {e}")
                return []

    def _normalize_person(self, p: dict, role_type: str) -> dict:
        profile = p.get("profile_path")
        return {
            "tmdb_id": p.get("id"),
            "name": p.get("name"),
            "original_name": p.get("original_name"),
            "profile_url": f"{self.IMAGE_BASE}{profile}" if profile else None,
            "character": p.get("character") if role_type == "cast" else None,
            "job": p.get("job") if role_type == "crew" else None,
            "department": p.get("department")
        }
