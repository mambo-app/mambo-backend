from pydantic import BaseModel
from typing import Optional, List
from datetime import datetime
from uuid import UUID

class NotificationResponse(BaseModel):
    id: UUID
    user_id: UUID
    type: str
    title: Optional[str] = None
    message: Optional[str] = None
    is_read: bool
    created_at: datetime
    read_at: Optional[datetime] = None
    
    actor_id: Optional[UUID] = None
    first_actor_id: Optional[UUID] = None
    latest_actor_id: Optional[UUID] = None
    aggregate_count: int = 1
    
    related_content_id: Optional[UUID] = None
    related_review_id: Optional[UUID] = None
    related_post_id: Optional[UUID] = None
    related_collection_id: Optional[UUID] = None
    related_id: Optional[UUID] = None
    
    # Optional nested actor object
    actor: Optional[dict] = None

class NotificationListResponse(BaseModel):
    items: List[NotificationResponse]
    total: int
    has_more: bool
