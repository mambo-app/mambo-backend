from pydantic import BaseModel, Field, field_validator
from uuid import UUID
from datetime import datetime

class ReviewCreate(BaseModel):
    content_id: UUID
    star_rating: int = Field(..., ge=1, le=10)
    text_review: str | None = Field(None, max_length=5000)
    contains_spoiler: bool = False
    tags: list[str] = []

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
    star_rating: int
    text_review: str | None
    contains_spoiler: bool
    tags: list[str]
    likes_count: int
    comments_count: int
    saves_count: int
    created_at: datetime
    username: str | None = None
    avatar_url: str | None = None

    class Config:
        from_attributes = True