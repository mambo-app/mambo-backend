from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from typing import List, Dict, Any, Optional
from app.core.database import get_db
from app.core.dependencies import get_current_user_id, get_current_user_id_optional
from app.models.common import ok
from app.models.social import ReviewCreateRequest, ReviewUpdateRequest, ReviewResponse, CommentCreateRequest, CommentResponse, ShareRequest
from app.services.social_service import SocialService
from app.core.logger import get_logger
from uuid import UUID

logger = get_logger('mambo.reviews')
router = APIRouter()

@router.get('/trending', response_model=Dict[str, Any])
async def get_trending_reviews(
    limit: int = Query(10, description="Number of items to fetch"),
    db: AsyncSession = Depends(get_db),
    current_user_id: Optional[str] = Depends(get_current_user_id_optional)
):
    service = SocialService(db)
    user_uuid = UUID(current_user_id) if current_user_id else None
    items = await service.get_trending_reviews(limit, current_user_id=user_uuid)
    return ok({"items": items})

@router.get('/of-the-day', response_model=Dict[str, Any])
async def get_review_of_the_day(
    db: AsyncSession = Depends(get_db),
    current_user_id: Optional[str] = Depends(get_current_user_id_optional)
):
    service = SocialService(db)
    user_uuid = UUID(current_user_id) if current_user_id else None
    item = await service.get_review_of_the_day(current_user_id=user_uuid)
    return ok({"item": item})

@router.get('/{id}', response_model=Dict[str, Any])
async def get_review(
    id: UUID,
    db: AsyncSession = Depends(get_db),
    current_user_id: Optional[str] = Depends(get_current_user_id_optional)
):
    service = SocialService(db)
    user_uuid = UUID(current_user_id) if current_user_id else None
    result = await service.get_review(id, current_user_id=user_uuid)
    return ok(result)

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
        tags=req.tags,
        tagged_seasons=req.tagged_seasons,
        tagged_episodes=req.tagged_episodes,
        review_type=req.review_type
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
    content_id: str,
    limit: int = 20,
    offset: int = 0,
    db: AsyncSession = Depends(get_db),
    current_user_id: Optional[str] = Depends(get_current_user_id_optional)
):
    from app.services.content_service import ContentService
    try:
        # Resolve content_id (could be UUID or TMDB numeric ID)
        content_svc = ContentService(db)
        content = await content_svc.get_content_by_id(content_id)
        if not content:
            return ok({"items": []})
        resolved_uuid = content.id
        tmdb_id = getattr(content, 'tmdb_id', None)
        title = getattr(content, 'title', None)
        service = SocialService(db)
        user_uuid = UUID(current_user_id) if current_user_id else None
        items = await service.get_content_reviews(
            content_id=resolved_uuid, 
            limit=limit, 
            offset=offset, 
            current_user_id=user_uuid,
            tmdb_id=tmdb_id,
            title=title
        )
        return ok({"items": items})
    except Exception as err:
        logger.warning(f"Error fetching reviews for content {content_id}: {err}")
        try:
            await db.rollback()
        except Exception: pass
        return ok({"items": []})