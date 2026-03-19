from pydantic import BaseModel
from typing import Optional, List
from datetime import datetime
from uuid import UUID

class MessageResponse(BaseModel):
    id: UUID
    conversation_id: UUID
    sender_id: UUID
    receiver_id: Optional[UUID] = None
    body: Optional[str] = None
    image_url: Optional[str] = None
    message_type: str = "text"
    is_read: bool = False
    read_at: Optional[datetime] = None
    sent_at: datetime
    
    shared_content_id: Optional[UUID] = None
    shared_review_id: Optional[UUID] = None
    shared_post_id: Optional[UUID] = None
    shared_news_id: Optional[UUID] = None
    
    # Rich Metadata Preview
    shared_meta: Optional[dict] = None

class ConversationResponse(BaseModel):
    id: UUID
    name: Optional[str] = None
    conversation_type: str
    last_message_at: Optional[datetime] = None
    last_message: Optional[MessageResponse] = None
    created_at: datetime
    unread_count: int = 0
