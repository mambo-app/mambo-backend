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
    is_ranked: bool = False
    pin_order: Optional[int] = None
    item_count: int = 0
    created_at: datetime
    updated_at: datetime

class CollectionCreateRequest(BaseModel):
    name: str
    description: Optional[str] = None
    visibility: str = 'public'
    is_ranked: Optional[bool] = False

class CollectionReorderRequest(BaseModel):
    collection_ids: List[UUID]

class CollectionItemsReorderRequest(BaseModel):
    content_ids: List[UUID]

class CollectionItemRequest(BaseModel):
    content_id: str

class CollectionUpdateRequest(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    visibility: Optional[str] = None
    is_pinned: Optional[bool] = None
    is_ranked: Optional[bool] = None

class CollectionGroupCreateRequest(BaseModel):
    name: str
    content_ids: List[UUID] = []

class CollectionGroupUpdateRequest(BaseModel):
    name: Optional[str] = None
    content_ids: Optional[List[UUID]] = None
