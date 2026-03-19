from pydantic import BaseModel
from typing import Optional
from enum import Enum
from uuid import UUID

class ActionType(str, Enum):
    watch = 'watch'
    rewatch = 'rewatch'
    drop = 'drop'
    like = 'like'
    unlike = 'unlike'
    save = 'save'
    unsave = 'unsave'
    recommend = 'recommend'
    rate = 'rate'
    review = 'review'

class ContentActionRequest(BaseModel):
    action: ActionType
    # Optional metadata if needed for specific actions
    rating: Optional[float] = None

class ContentActionResponse(BaseModel):
    status: str
    action: ActionType
    content_id: UUID
    is_permanent: bool = True
