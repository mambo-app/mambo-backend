from pydantic import BaseModel, Field
from typing import Optional, List
from datetime import datetime
from uuid import UUID

class ReviewCreateRequest(BaseModel):
    content_id: UUID
    star_rating: float = Field(..., ge=1, le=10)
    text_review: Optional[str] = None
    contains_spoiler: bool = False
    tags: List[str] = []

class ReviewUpdateRequest(BaseModel):
    star_rating: Optional[float] = Field(None, ge=1, le=10)
    text_review: Optional[str] = None
    contains_spoiler: Optional[bool] = None
    tags: Optional[List[str]] = None

class ReviewResponse(BaseModel):
    id: UUID
    user_id: UUID
    content_id: UUID
    star_rating: int
    text_review: Optional[str] = None
    contains_spoiler: bool
    tags: List[str]
    likes_count: int
    comments_count: int
    shares_count: int
    saves_count: int
    created_at: datetime
    updated_at: datetime

class CommentCreateRequest(BaseModel):
    content: str
    parent_id: Optional[UUID] = None

class CommentResponse(BaseModel):
    id: UUID
    user_id: UUID
    content: str
    parent_id: Optional[UUID] = None
    upvotes_count: int
    created_at: datetime
    updated_at: datetime
    is_deleted: bool

class PostCreateRequest(BaseModel):
    title: str
    body: str
    content_id: Optional[UUID] = None
    image_url: Optional[str] = None
    media_urls: List[str] = []

class PostResponse(BaseModel):
    id: UUID
    user_id: UUID
    content_id: Optional[UUID] = None
    title: str
    body: str
    image_url: Optional[str] = None
    upvotes_count: int
    comments_count: int
    shares_count: int
    saves_count: int
    created_at: datetime
    updated_at: datetime
    media_urls: List[str] = []

class ShareRequest(BaseModel):
    conversation_id: Optional[UUID] = None
    recipient_id: Optional[UUID] = None

class FriendRequestCreate(BaseModel):
    receiver_id: UUID

class FriendRequestResponse(BaseModel):
    id: UUID
    sender_id: UUID
    receiver_id: UUID
    status: str
    created_at: datetime
    updated_at: datetime
    # Sender/Receiver info for UI
    sender_username: Optional[str] = None
    sender_avatar_url: Optional[str] = None

class FriendResponse(BaseModel):
    user_id: UUID
    username: str
    display_name: Optional[str] = None
    avatar_url: Optional[str] = None
    is_verified: bool
    friends_since: datetime

class UpvoteRequest(BaseModel):
    target_id: UUID
    target_type: str = Field(..., pattern="^(post|comment)$")

