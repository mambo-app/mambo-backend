from pydantic import BaseModel, Field, field_validator
from uuid import UUID
from datetime import datetime

class ReviewCreate(BaseModel):
    content_id: UUID
    star_rating: float | None = Field(None, ge=1, le=10)
    text_review: str | None = Field(None, max_length=5000)
    contains_spoiler: bool = False
    tags: list[str] = []
    tagged_seasons: list[int] = []
    tagged_episodes: list[int] = []
    review_type: str = "overall" # overall, season, episode
    watch_history_id: UUID | None = None

    @field_validator('tags')
    @classmethod
    def validate_tags(cls, v):
        if len(v) > 5:
            raise ValueError('Maximum 5 tags allowed')
        return [tag.lower().strip() for tag in v]

class ReviewResponse(BaseModel):
    id: UUID
    user_id: UUID
    content_id: UUID
    star_rating: float | None = None
    text_review: str | None = None
    contains_spoiler: bool = False
    tags: list[str] = []
    tagged_seasons: list[int] = []
    tagged_episodes: list[int] = []
    review_type: str = "overall"
    watch_history_id: UUID | None = None
    likes_count: int = 0
    comments_count: int = 0
    saves_count: int = 0
    created_at: datetime
    username: str | None = None
    avatar_url: str | None = None
    is_liked: bool = False

    class Config:
        from_attributes = True