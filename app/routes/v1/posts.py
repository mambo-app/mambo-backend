from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from typing import List, Dict, Any, Optional
from app.core.database import get_db
from app.core.dependencies import get_current_user_id
from app.models.common import ok
from app.models.social import PostCreateRequest, PostResponse, CommentCreateRequest, CommentResponse, ShareRequest
from app.services.social_service import SocialService
from uuid import UUID

router = APIRouter()

@router.post('/', response_model=Dict[str, Any])
async def create_post(
    req: PostCreateRequest,
    db: AsyncSession = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    service = SocialService(db)
    result = await service.create_post(
        user_id=UUID(user_id),
        title=req.title,
        body=req.body,
        content_id=req.content_id,
        media_urls=req.media_urls
    )
    return ok(result)

@router.post('/{id}/upvote', response_model=Dict[str, Any])
async def toggle_upvote(
    id: UUID,
    db: AsyncSession = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    service = SocialService(db)
    upvoted = await service.toggle_post_upvote(UUID(user_id), id)
    return ok({"upvoted": upvoted})

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
        post_id=id,
        content=req.content,
        parent_id=req.parent_comment_id
    )
    return ok(comment)

@router.post('/{id}/share', response_model=Dict[str, Any])
async def share_post(
    id: UUID,
    req: ShareRequest,
    db: AsyncSession = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    service = SocialService(db)
    metadata = await service.share_post(UUID(user_id), id, req.conversation_id, req.recipient_id)
    return ok(metadata)

@router.post('/{id}/save', response_model=Dict[str, Any])
async def save_post(
    id: UUID,
    db: AsyncSession = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    """Save a post to the user's saved list."""
    service = SocialService(db)
    await service.save_post(UUID(user_id), id)
    return ok({"saved": True})

@router.delete('/{id}/save', response_model=Dict[str, Any])
async def unsave_post(
    id: UUID,
    db: AsyncSession = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    """Remove a post from the user's saved list."""
    service = SocialService(db)
    await service.unsave_post(UUID(user_id), id)
    return ok({"saved": False})

@router.get('/content/{content_id}', response_model=Dict[str, Any])
async def get_posts_by_content(
    content_id: UUID,
    limit: int = 20,
    offset: int = 0,
    db: AsyncSession = Depends(get_db)
):
    service = SocialService(db)
    items = await service.get_content_posts(content_id, limit, offset)
    return ok({"items": items})