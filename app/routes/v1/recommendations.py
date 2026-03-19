from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from typing import List, Dict, Any
from app.core.database import get_db
from app.core.dependencies import get_current_user_id
from app.models.common import ok
from app.models.recommendation import RecommendationCreateRequest, RecommendationResponse
from app.services.recommendation_service import RecommendationService
from uuid import UUID

router = APIRouter()

@router.get('/', response_model=Dict[str, Any])
async def get_received_recommendations(
    db: AsyncSession = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    service = RecommendationService(db)
    items = await service.get_received_recommendations(UUID(user_id))
    return ok({"items": items})

@router.post('/', response_model=Dict[str, Any])
async def create_recommendation(
    req: RecommendationCreateRequest,
    db: AsyncSession = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    service = RecommendationService(db)
    result = await service.create_recommendation(
        sender_id=UUID(user_id),
        content_id=req.content_id,
        recipient_id=req.recipient_id,
        message=req.message
    )
    return ok(result)
