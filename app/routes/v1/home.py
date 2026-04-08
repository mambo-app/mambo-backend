from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from app.core.database import get_db
from app.services.content_service import ContentService
from typing import List, Dict, Any
from app.models.content import HomeTrendingResponse, CuratedContentResponse
from app.models.common import ok

from app.core.dependencies import get_current_user_id_optional
from typing import List, Dict, Any, Optional

router = APIRouter(tags=['home'])

@router.get('/trending', response_model=Dict[str, Any])
async def get_trending(
    db: AsyncSession = Depends(get_db),
    user_id: Optional[str] = Depends(get_current_user_id_optional)
):
    service = ContentService(db)
    data = await service.get_home_trending(user_id)
    return ok(data.model_dump())

@router.get('/spotlight', response_model=Dict[str, Any])
async def get_spotlight(db: AsyncSession = Depends(get_db)):
    service = ContentService(db)
    items = await service.get_spotlight()
    return ok({"items": items})

@router.get('/hot-reviews', response_model=Dict[str, Any])
async def get_hot_reviews(
    limit: int = 10,
    db: AsyncSession = Depends(get_db)
):
    service = ContentService(db)
    items = await service.get_hot_reviews(limit)
    return ok({"items": items})

@router.get('/landing-posters', response_model=Dict[str, Any])
async def get_landing_posters(
    count: int = 12,
    db: AsyncSession = Depends(get_db)
):
    """
    Returns a stable list of movie poster URLs for the landing screen carousel.
    First call may be slightly slow (fetches from TMDB + saves to DB).
    Subsequent calls are fast (served from DB).
    No auth required.
    """
    service = ContentService(db)
    posters = await service.get_landing_posters(target_count=max(6, min(count, 20)))
    return ok({"posters": posters})

