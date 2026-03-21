from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from typing import List, Dict, Any, Optional
from app.core.database import get_db
from app.core.dependencies import get_current_user_id
from app.models.common import ok
from app.models.collection import CollectionResponse, CollectionCreateRequest, CollectionItemRequest, CollectionUpdateRequest
from app.services.collection_service import CollectionService
from uuid import UUID

router = APIRouter()

@router.get('/', response_model=Dict[str, Any])
async def get_collections(
    db: AsyncSession = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    service = CollectionService(db)
    items = await service.get_user_collections(UUID(user_id))
    return ok({"items": items})

@router.post('/', response_model=Dict[str, Any])
async def create_collection(
    req: CollectionCreateRequest,
    db: AsyncSession = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    service = CollectionService(db)
    collection = await service.create_collection(
        user_id=UUID(user_id),
        name=req.name,
        description=req.description,
        visibility=req.visibility
    )
    return ok(collection)

@router.patch('/{id}', response_model=Dict[str, Any])
async def update_collection(
    id: UUID,
    req: CollectionUpdateRequest,
    db: AsyncSession = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    service = CollectionService(db)
    updated = await service.update_collection(
        user_id=UUID(user_id),
        collection_id=id,
        **req.model_dump(exclude_unset=True)
    )
    if not updated:
        raise HTTPException(status_code=403, detail="Not authorized to modify this collection or collection not found")
    return ok(updated)

@router.delete('/{id}', response_model=Dict[str, Any])
async def delete_collection(
    id: UUID,
    db: AsyncSession = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    service = CollectionService(db)
    success = await service.delete_collection(UUID(user_id), id)
    if not success:
        raise HTTPException(status_code=403, detail="Not authorized to delete this collection or it is not deletable")
    return ok({"success": True})

@router.get('/{id}/items', response_model=Dict[str, Any])
async def get_collection_items(
    id: UUID,
    type: Optional[str] = Query(None, description="Filter by content type (movie, series, anime)"),
    genre: Optional[str] = Query(None, description="Filter by genre"),
    status: Optional[str] = Query(None, description="Filter by user status (watched/rewatched/dropped/plan_to_watch)"),
    platform: Optional[str] = Query(None, description="Filter by streaming platform"),
    db: AsyncSession = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    service = CollectionService(db)
    items = await service.get_collection_items(
        UUID(user_id), id,
        content_type=type, genre=genre, status=status, streaming_platform=platform
    )
    return ok({"items": items})

@router.post('/{id}/items', response_model=Dict[str, Any])
async def add_item_to_collection(
    id: UUID,
    req: CollectionItemRequest,
    db: AsyncSession = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    service = CollectionService(db)
    success = await service.add_item_to_collection(
        user_id=UUID(user_id),
        collection_id=id,
        content_id=req.content_id
    )
    if not success:
        raise HTTPException(status_code=403, detail="Not authorized to modify this collection or collection not found")
    return ok({"success": True})

@router.delete('/{id}/items/{content_id}', response_model=Dict[str, Any])
async def remove_item_from_collection(
    id: UUID,
    content_id: UUID,
    db: AsyncSession = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    service = CollectionService(db)
    success = await service.remove_item_from_collection(
        user_id=UUID(user_id),
        collection_id=id,
        content_id=content_id
    )
    if not success:
        raise HTTPException(status_code=403, detail="Not authorized to modify this collection or item not found")
    return ok({"success": True})

@router.get('/item-status/{content_id}', response_model=Dict[str, Any])
async def get_content_collection_status(
    content_id: UUID,
    db: AsyncSession = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    service = CollectionService(db)
    collection_ids = await service.get_content_collection_status(UUID(user_id), content_id)
    return ok({"collection_ids": collection_ids})
