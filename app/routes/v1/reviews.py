from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from typing import List, Dict, Any, Optional
from app.core.database import get_db
from app.core.dependencies import get_current_user_id
from app.models.common import ok
from app.models.social import ReviewCreateRequest, ReviewUpdateRequest, ReviewResponse, CommentCreateRequest, CommentResponse, ShareRequest
from app.services.social_service import SocialService
from uuid import UUID

router = APIRouter()

@router.get('/trending', response_model=Dict[str, Any])
async def get_trending_reviews(
    limit: int = 10,
    db: AsyncSession = Depends(get_db)
):
    service = SocialService(db)
    items = await service.get_trending_reviews(limit)
    return ok({"items": items})

@router.post('/', response_model=Dict[str, Any])
async def create_review(
    req: ReviewCreateRequest,
    db: AsyncSession = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    service = SocialService(db)
    result = await service.create_review(
        user_id=UUID(user_id),
        content_id=req.content_id,
        star_rating=req.star_rating,
        text_review=req.text_review,
        contains_spoiler=req.contains_spoiler,
        tags=req.tags
    )
    return ok(result)

@router.put('/{id}', response_model=Dict[str, Any])
async def update_review(
    id: UUID,
    req: ReviewUpdateRequest,
    db: AsyncSession = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    service = SocialService(db)
    result = await service.update_review(
        user_id=UUID(user_id),
        review_id=id,
        data=req.model_dump(exclude_unset=True)
    )
    return ok(result)

@router.delete('/{id}', response_model=Dict[str, Any])
async def delete_review(
    id: UUID,
    db: AsyncSession = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    from app.services.review_service import ReviewService
    svc = ReviewService(db)
    await svc.delete_review(str(id), user_id)
    return ok({"deleted": True})

@router.post('/{id}/like', response_model=Dict[str, Any])
async def toggle_like(
    id: UUID,
    db: AsyncSession = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    service = SocialService(db)
    liked = await service.toggle_review_like(UUID(user_id), id)
    return ok({"liked": liked})

@router.post('/{id}/comments', response_model=Dict[str, Any])
async def add_comment(
    id: UUID,
    req: CommentCreateRequest,
    db: AsyncSession = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    service = SocialService(db)
    comment = await service.create_comment(
        user_id=UUID(user_id),
        review_id=id,
        content=req.content,
        parent_id=req.parent_comment_id
    )
    return ok(comment)

@router.post('/{id}/share', response_model=Dict[str, Any])
async def share_review(
    id: UUID,
    req: ShareRequest,
    db: AsyncSession = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    service = SocialService(db)
    metadata = await service.share_review(UUID(user_id), id, req.conversation_id, req.recipient_id)
    return ok(metadata)


@router.get('/content/{content_id}', response_model=Dict[str, Any])
async def get_reviews_by_content(
    content_id: UUID,
    limit: int = 20,
    offset: int = 0,
    db: AsyncSession = Depends(get_db)
):
    service = SocialService(db)
    items = await service.get_content_reviews(content_id, limit, offset)
    return ok({"items": items})