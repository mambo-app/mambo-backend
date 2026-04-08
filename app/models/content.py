from pydantic import BaseModel, Field
from typing import Optional, List
from datetime import date, datetime
from uuid import UUID

class CastMemberResponse(BaseModel):
    id: Optional[str] = None
    name: str
    profile_url: Optional[str] = None
    role: Optional[str] = None
    character: Optional[str] = None
    job: Optional[str] = None

class ContentResponse(BaseModel):
    id: UUID
    tmdb_id: Optional[int] = None
    mal_id: Optional[int] = None
    content_type: str
    title: str
    synopsis: Optional[str] = None
    original_language: Optional[str] = None
    poster_url: Optional[str] = None
    backdrop_url: Optional[str] = None
    genres: List[str] = []
    release_date: Optional[date] = None
    status: Optional[str] = None
    external_rating: Optional[float] = None
    
    # Mode specific fields
    runtime_minutes: Optional[int] = None      # Movies
    total_episodes: Optional[int] = None       # Series/Anime
    seasons_count: Optional[int] = None        # Series
    anime_studio: Optional[str] = None         # Anime
    
    is_permanent: bool = False
    avg_star_rating: float = 0.0
    is_anticipated: bool = False
    cast: List[CastMemberResponse] = []
    
    # User-specific social status (populated if authenticated)
    is_watched: bool = False
    is_liked: bool = False
    is_dropped: bool = False
    is_interested: bool = False
    watch_count: int = 0
    user_rating: Optional[float] = None

    class Config:
        from_attributes = True

class HomeTrendingResponse(BaseModel):
    movies: List[ContentResponse]
    series: List[ContentResponse]
    anime: List[ContentResponse]

class CuratedContentResponse(BaseModel):
    id: UUID
    title: str
    description: Optional[str] = None
    image_url: Optional[str] = None
    link_url: Optional[str] = None
    content_id: Optional[UUID] = None
    category: str
    priority: int = 0

class NewsArticleResponse(BaseModel):
    id: UUID
    title: str
    description: Optional[str] = None
    image_url: Optional[str] = None
    source_name: Optional[str] = None
    category: str
    published_at: datetime
