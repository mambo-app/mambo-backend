from pydantic import BaseModel
from typing import Optional, List
from datetime import datetime
from uuid import UUID

class CollectionResponse(BaseModel):
    id: UUID
    user_id: UUID
    name: str
    description: Optional[str] = None
    collection_type: str = 'custom' # 'custom', 'watchlist', 'favorites', etc.
    visibility: str = 'public' # 'public', 'private'
    is_default: bool = False
    is_deletable: bool = True
    is_pinned: bool = False
    pin_order: Optional[int] = None
    item_count: int = 0
    created_at: datetime
    updated_at: datetime

class CollectionCreateRequest(BaseModel):
    name: str
    description: Optional[str] = None
    visibility: str = 'public'

class CollectionItemRequest(BaseModel):
    content_id: UUID

class CollectionUpdateRequest(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    visibility: Optional[str] = None
    is_pinned: Optional[bool] = None
