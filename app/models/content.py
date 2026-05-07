from pydantic import BaseModel, Field
from typing import Optional, List
from datetime import date, datetime
from enum import Enum
from uuid import UUID

class ContentStatus(str, Enum):
    PLAN_TO_WATCH = 'plan_to_watch'
    WATCHING = 'watching'
    COMPLETED = 'completed'
    ON_HOLD = 'on_hold'
    DROPPED = 'dropped'
    NONE = 'none'

class CastMemberResponse(BaseModel):
    id: Optional[str] = None
    name: str
    profile_url: Optional[str] = None
    role: Optional[str] = None
    character: Optional[str] = None
    job: Optional[str] = None

class SeasonMetadata(BaseModel):
    season_number: int
    episode_count: int

class SeasonStatusResponse(BaseModel):
    season_number: int
    status: ContentStatus = ContentStatus.NONE
    progress_episodes: int = 0
    total_episodes: int = 0
    updated_at: Optional[datetime] = None

class ContentResponse(BaseModel):
    id: UUID
    tmdb_id: Optional[int] = None
    mal_id: Optional[int] = None
    content_type: str
    title: str
    synopsis: Optional[str] = None
    description: Optional[str] = None
    original_language: Optional[str] = None
    poster_url: Optional[str] = None
    backdrop_url: Optional[str] = None
    genres: List[str] = []
    release_date: Optional[date] = None
    release_status: Optional[str] = None
    external_rating: Optional[float] = None
    
    # Mode specific fields
    runtime_minutes: Optional[int] = None      # Movies
    total_episodes: Optional[int] = None       # Series/Anime
    total_seasons: Optional[int] = None        # Series
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
    
    # New Tracker Fields (Phase 1/2)
    status: ContentStatus = ContentStatus.NONE
    progress_episodes: int = 0
    last_watched_season: int = 0
    last_watched_episode: int = 0
    rewatch_count: int = 0
    last_activity_at: Optional[datetime] = None
    
    season_statuses: List[SeasonStatusResponse] = []
    seasons: List[SeasonMetadata] = []
    
    watch_count: int = 0
    user_rating: Optional[float] = None
    is_notified: bool = False
    
    # Rating Distribution (Real or Simulated)
    vote_count: int = 0
    rating_distribution: List[float] = [0.0, 0.0, 0.0, 0.0, 0.0] # 1 to 5 stars

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
