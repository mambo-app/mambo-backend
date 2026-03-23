from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession
from app.core.database import get_db
from app.services.content_service import ContentService
from app.core.dependencies import get_current_user_id, get_current_user_id_optional
from typing import Dict, List, Any, Optional
from app.models.content import ContentResponse
from app.models.common import ok

router = APIRouter(tags=['discover'])

@router.get('/search', response_model=Dict[str, Any])
async def search_content(
    query: str,
    content_type: Optional[str] = Query(None, description="Filter: movie, series, anime"),
    limit: int = 20,
    db: AsyncSession = Depends(get_db),
    user_id: Optional[str] = Depends(get_current_user_id_optional),
):
    service = ContentService(db)
    items = await service.search_content(query, limit, content_type, user_id=user_id)
    # Persist search history for authenticated users
    if user_id and query.strip():
        await service.save_search_history(user_id, query, content_type)
    return ok({"items": items})

@router.get('/history', response_model=Dict[str, Any])
async def get_search_history(
    limit: int = 10,
    db: AsyncSession = Depends(get_db),
    user_id: str = Depends(get_current_user_id),
):
    service = ContentService(db)
    items = await service.get_search_history(user_id, limit)
    return ok({"items": items})

@router.delete('/history', response_model=Dict[str, Any])
async def clear_search_history(
    query: Optional[str] = Query(None, description="Specific query to remove; omit to clear all"),
    db: AsyncSession = Depends(get_db),
    user_id: str = Depends(get_current_user_id),
):
    service = ContentService(db)
    deleted = await service.clear_search_history(user_id, query)
    return ok({"deleted": deleted})

@router.get('/trending-creators', response_model=Dict[str, Any])
async def get_trending_creators(
    limit: int = 10,
    db: AsyncSession = Depends(get_db),
    user_id: Optional[str] = Depends(get_current_user_id_optional)
):
    from app.services.user_service import UserService
    service = UserService(db)
    items = await service.get_trending_creators(limit, viewer_id=user_id)
    return ok({"items": items})
    return ok({"items": items})

@router.get('/{mode}', response_model=Dict[str, Any])
async def get_discover(
    mode: str, 
    db: AsyncSession = Depends(get_db),
    user_id: Optional[str] = Depends(get_current_user_id_optional)
):
    # mode is one of: movie, series, anime
    service = ContentService(db)
    data = await service.get_discover_content(mode, user_id=user_id)
    return ok(data)
