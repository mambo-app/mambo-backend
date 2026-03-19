from pydantic import BaseModel
from typing import Optional
from uuid import UUID

class ReportCreateRequest(BaseModel):
    report_type: str
    reason: str
    description: Optional[str] = None
    reported_user_id: Optional[UUID] = None
    post_id: Optional[UUID] = None
    review_id: Optional[UUID] = None
    message_id: Optional[UUID] = None
    news_id: Optional[UUID] = None
