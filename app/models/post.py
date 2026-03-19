from pydantic import BaseModel, Field
from uuid import UUID
from datetime import datetime

class PostCreate(BaseModel):
    body: str = Field(..., min_length=1, max_length=5000)
    title: str | None = Field(None, max_length=300)
    content_id: UUID | None = None
    visibility: str = 'public'

class PostResponse(BaseModel):
    id: UUID
    user_id: UUID
    body: str
    title: str | None
    upvotes_count: int
    comments_count: int
    saves_count: int
    created_at: datetime
    username: str | None = None
    avatar_url: str | None = None

    class Config:
        from_attributes = True