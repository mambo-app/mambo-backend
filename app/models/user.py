from pydantic import BaseModel
from uuid import UUID
from datetime import datetime

class UserProfile(BaseModel):
    id: UUID
    username: str
    display_name: str | None = None
    bio: str | None = None
    avatar_url: str | None = None
    is_verified: bool = False
    created_at: datetime
    
    # New fields
    birthday: datetime | None = None
    gender: str | None = None
    email: str | None = None
    phone_number: str | None = None
    
    activity_visibility: str = 'public'
    favourites_visibility: str = 'public'
    reviews_visibility: str = 'public'
    push_notifications_enabled: bool = True

    class Config:
        from_attributes = True

class PrivacyUpdateRequest(BaseModel):
    activity_visibility: str | None = None
    favourites_visibility: str | None = None
    reviews_visibility: str | None = None
    push_notifications_enabled: bool | None = None

class UserFavoriteGenre(BaseModel):
    genre_name: str

class GenrePreferenceRequest(BaseModel):
    genres: list[str]

class UserStats(BaseModel):
    total_watched: int = 0
    total_reviews: int = 0
    total_posts: int = 0
    followers_count: int = 0
    following_count: int = 0
    friends_count: int = 0