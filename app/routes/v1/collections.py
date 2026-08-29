from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from typing import List, Dict, Any, Optional
from app.core.database import get_db
from app.core.dependencies import get_current_user_id
from app.models.common import ok
from app.models.collection import CollectionResponse, CollectionCreateRequest, CollectionItemRequest, CollectionUpdateRequest, CollectionReorderRequest, CollectionItemsReorderRequest, CollectionGroupCreateRequest, CollectionGroupUpdateRequest
from app.services.collection_service import CollectionService
from uuid import UUID

router = APIRouter()

@router.get('/public', response_model=Dict[str, Any])
async def get_public_collections(
    limit: int = 50,
    offset: int = 0,
    db: AsyncSession = Depends(get_db)
):
    service = CollectionService(db)
    items = await service.get_public_custom_collections(limit=limit, offset=offset)
    return ok({"items": items})

@router.get('/', response_model=Dict[str, Any])
async def get_collections(
    db: AsyncSession = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    service = CollectionService(db)
    items = await service.get_user_collections(UUID(user_id))
    return ok({"items": items})

@router.get('/item-status/{content_id}', response_model=Dict[str, Any])
async def get_content_collection_status(
    content_id: str,
    db: AsyncSession = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    content_uuid = await _resolve_content_id(content_id, db, auto_import=False)
    if not content_uuid:
        # If the content doesn't exist locally, it cannot be in any collection
        return ok({"collection_ids": []})

    service = CollectionService(db)
    collection_ids = await service.get_content_collection_status(UUID(user_id), content_uuid)
    return ok({"collection_ids": collection_ids})

@router.get('/{id}', response_model=Dict[str, Any])
async def get_collection(
    id: UUID,
    db: AsyncSession = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    service = CollectionService(db)
    coll = await service.get_collection_by_id(id, viewer_id=UUID(user_id))
    if not coll:
        raise HTTPException(status_code=404, detail="Collection not found")
    return ok(coll)

@router.post('/{id}/save', response_model=Dict[str, Any])
async def save_collection(
    id: UUID,
    db: AsyncSession = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    service = CollectionService(db)
    success = await service.save_collection(UUID(user_id), id)
    if not success:
        raise HTTPException(status_code=400, detail="Cannot save this collection (must be public and exist)")
    return ok({"success": True})

@router.delete('/{id}/save', response_model=Dict[str, Any])
async def unsave_collection(
    id: UUID,
    db: AsyncSession = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    service = CollectionService(db)
    await service.unsave_collection(UUID(user_id), id)
    return ok({"success": True})

@router.get('/{id}/savers', response_model=Dict[str, Any])
async def get_collection_savers(
    id: UUID,
    db: AsyncSession = Depends(get_db)
):
    service = CollectionService(db)
    savers = await service.get_collection_savers(id)
    return ok({"savers": savers})

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
        visibility=req.visibility,
        is_ranked=req.is_ranked or False
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

@router.put('/reorder', response_model=Dict[str, Any])
async def reorder_collections(
    req: CollectionReorderRequest,
    db: AsyncSession = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    service = CollectionService(db)
    await service.reorder_collections(UUID(user_id), req.collection_ids)
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

async def _resolve_content_id(content_id: str, db: AsyncSession, auto_import: bool = False) -> Optional[UUID]:
    if not content_id:
        return None
        
    raw_id = str(content_id).strip()
    
    target_uuid = None
    target_tmdb_id = None
    target_mal_id = None

    try:
        target_uuid = UUID(raw_id)
    except ValueError:
        pass

    if raw_id.startswith('tmdb_') and raw_id[5:].isdigit():
        target_tmdb_id = int(raw_id[5:])
    elif raw_id.startswith('mal_') and raw_id[4:].isdigit():
        target_mal_id = int(raw_id[4:])
    elif raw_id.isdigit():
        target_tmdb_id = int(raw_id)

    # 1. If target_uuid was parsed, check if content table has a row for this UUID
    if target_uuid:
        res = await db.execute(
            text("SELECT id, tmdb_id, mal_id FROM content WHERE id = :uid"),
            {"uid": target_uuid}
        )
        row = res.mappings().first()
        if row:
            if row['tmdb_id'] is not None:
                target_tmdb_id = row['tmdb_id']
            elif row['mal_id'] is not None:
                target_mal_id = row['mal_id']
            else:
                return target_uuid
        else:
            # Check redis cache for uuid_map:{gen_uuid}
            try:
                from app.core.redis import cache
                cached_map = await cache.get(f"uuid_map:{target_uuid}")
                if cached_map and isinstance(cached_map, dict):
                    if cached_map.get('tmdb_id'):
                        target_tmdb_id = int(cached_map['tmdb_id'])
                    elif cached_map.get('mal_id'):
                        target_mal_id = int(cached_map['mal_id'])
            except Exception:
                pass

    # 2. Resolve by tmdb_id or mal_id across content table to get existing DB row
    if target_tmdb_id is not None or target_mal_id is not None:
        res = await db.execute(
            text("""
                SELECT id FROM content 
                WHERE (CAST(:tid AS INTEGER) IS NOT NULL AND tmdb_id = CAST(:tid AS INTEGER)) 
                   OR (CAST(:mid AS INTEGER) IS NOT NULL AND mal_id = CAST(:mid AS INTEGER))
                ORDER BY created_at ASC
                LIMIT 1
            """),
            {"tid": target_tmdb_id, "mid": target_mal_id}
        )
        row = res.mappings().first()
        if row:
            return UUID(str(row['id']))

        # 3. If not found and auto_import is requested, import from TMDB/MAL
        if auto_import:
            from app.services.content_service import ContentService
            content_service = ContentService(db)
            try:
                imported = await content_service.get_content_by_id(raw_id)
                if imported:
                    return UUID(str(imported.id))
            except Exception as e:
                import logging
                logging.getLogger('mambo.collections').error(f"Auto-import in router failed: {e}")

    # Fallback to target_uuid
    if target_uuid:
        return target_uuid

    return None

@router.post('/{id}/items', response_model=Dict[str, Any])
async def add_item_to_collection(
    id: UUID,
    req: CollectionItemRequest,
    db: AsyncSession = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    content_uuid = await _resolve_content_id(req.content_id, db, auto_import=True)
    if not content_uuid:
        raise HTTPException(status_code=404, detail="Content not found and auto-import failed")

    service = CollectionService(db)
    success = await service.add_item_to_collection(
        user_id=UUID(user_id),
        collection_id=id,
        content_id=content_uuid
    )
    if not success:
        raise HTTPException(status_code=403, detail="Not authorized to modify this collection or collection not found")
    return ok({"success": True})

@router.delete('/{id}/items/{content_id}', response_model=Dict[str, Any])
async def remove_item_from_collection(
    id: UUID,
    content_id: str,
    db: AsyncSession = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    content_uuid = await _resolve_content_id(content_id, db, auto_import=False)
    if not content_uuid:
        # If the content doesn't exist locally, it's already not in the collection
        return ok({"success": True})

    service = CollectionService(db)
    success = await service.remove_item_from_collection(
        user_id=UUID(user_id),
        collection_id=id,
        content_id=content_uuid
    )
    if not success:
        raise HTTPException(status_code=403, detail="Not authorized to modify this collection or item not found")
    return ok({"success": True})



@router.put('/{id}/reorder-items', response_model=Dict[str, Any])
async def reorder_collection_items(
    id: UUID,
    req: CollectionItemsReorderRequest,
    db: AsyncSession = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    service = CollectionService(db)
    success = await service.reorder_collection_items(UUID(user_id), id, req.content_ids)
    if not success:
        raise HTTPException(status_code=403, detail="Not authorized to reorder items or collection not found")
    return ok({"success": True})

@router.get('/{id}/groups', response_model=Dict[str, Any])
async def get_collection_groups(
    id: UUID,
    db: AsyncSession = Depends(get_db)
):
    service = CollectionService(db)
    groups = await service.get_collection_groups(id)
    return ok({"items": groups})

@router.post('/{id}/groups', response_model=Dict[str, Any])
async def create_collection_group(
    id: UUID,
    req: CollectionGroupCreateRequest,
    db: AsyncSession = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    service = CollectionService(db)
    group = await service.create_collection_group(UUID(user_id), id, req.name, req.content_ids)
    return ok({"group": group})

@router.put('/{id}/groups/{group_id}', response_model=Dict[str, Any])
async def update_collection_group(
    id: UUID,
    group_id: UUID,
    req: CollectionGroupUpdateRequest,
    db: AsyncSession = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    service = CollectionService(db)
    success = await service.update_collection_group(UUID(user_id), group_id, req.name, req.content_ids)
    return ok({"success": success})

@router.delete('/{id}/groups/{group_id}', response_model=Dict[str, Any])
async def delete_collection_group(
    id: UUID,
    group_id: UUID,
    db: AsyncSession = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    service = CollectionService(db)
    success = await service.delete_collection_group(UUID(user_id), group_id)
    return ok({"success": success})
