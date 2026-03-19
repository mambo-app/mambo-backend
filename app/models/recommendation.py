from pydantic import BaseModel
from typing import Optional, List
from datetime import datetime
from uuid import UUID

class RecommendationCreateRequest(BaseModel):
    content_id: UUID
    recipient_id: UUID
    message: Optional[str] = None

class RecommendationResponse(BaseModel):
    id: UUID
    sender_id: UUID
    content_id: UUID
    sent_at: datetime
    message: Optional[str] = None
    recipient_id: UUID
    is_viewed: bool
    viewed_at: Optional[datetime] = None

class RecommendationListResponse(BaseModel):
    items: List[RecommendationResponse]
    total: int
