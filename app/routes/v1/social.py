from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession
from uuid import UUID
from typing import List

from app.core.database import get_db
from app.core.dependencies import get_current_user_id, get_current_user_id_optional
from app.models.social import (
    FriendRequestCreate, FriendRequestResponse, FriendResponse,
    PostCreateRequest, PostResponse, CommentCreateRequest, CommentResponse, UpvoteRequest
)
from app.services.social_service import SocialService

router = APIRouter(tags=['Social'])

# --- Friend Requests ---
@router.post('/friend-requests', response_model=FriendRequestResponse)
async def send_friend_request(
    request: FriendRequestCreate,
    db: AsyncSession = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    service = SocialService(db)
    return await service.send_friend_request(UUID(user_id), request.receiver_id)

@router.put('/friend-requests/{request_id}', response_model=FriendRequestResponse)
async def respond_to_friend_request(
    request_id: UUID,
    status: str = Query(..., pattern="^(accepted|ignored)$"),
    db: AsyncSession = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    service = SocialService(db)
    return await service.respond_to_request(UUID(user_id), request_id, status)

@router.delete('/friend-requests/{receiver_id}')
async def cancel_friend_request(
    receiver_id: UUID,
    db: AsyncSession = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    service = SocialService(db)
    return await service.cancel_friend_request(UUID(user_id), receiver_id)

@router.get('/friends', response_model=List[FriendResponse])
async def get_friends(
    limit: int = 20,
    offset: int = 0,
    db: AsyncSession = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    service = SocialService(db)
    return await service.get_friends(UUID(user_id), limit, offset)

@router.get('/friend-requests/pending', response_model=List[FriendRequestResponse])
async def get_pending_requests(
    db: AsyncSession = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    service = SocialService(db)
    return await service.get_pending(UUID(user_id))

# --- Phase 3: Community Content ---

@router.post('/posts', response_model=PostResponse)
async def create_post(
    request: PostCreateRequest,
    db: AsyncSession = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    service = SocialService(db)
    return await service.create_post(UUID(user_id), request.dict())

@router.get('/posts', response_model=List[PostResponse])
async def get_posts(
    limit: int = 20,
    offset: int = 0,
    db: AsyncSession = Depends(get_db),
    viewer_id: str | None = Depends(get_current_user_id_optional)
):
    from app.core.dependencies import get_current_user_id_optional
    service = SocialService(db)
    vid = UUID(viewer_id) if viewer_id else None
    return await service.get_posts(limit, offset, vid)

@router.get('/posts/{post_id}', response_model=PostResponse)
async def get_post(
    post_id: UUID,
    db: AsyncSession = Depends(get_db)
):
    service = SocialService(db)
    return await service.get_post(post_id)

@router.post('/comments', response_model=CommentResponse)
async def create_comment(
    request: CommentCreateRequest,
    post_id: UUID | None = None,
    review_id: UUID | None = None,
    db: AsyncSession = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    service = SocialService(db)
    return await service.create_comment(
        UUID(user_id), 
        request.content, 
        post_id=post_id, 
        review_id=review_id, 
        parent_id=request.parent_id
    )

@router.get('/comments', response_model=List[CommentResponse])
async def get_comments(
    post_id: UUID | None = None,
    review_id: UUID | None = None,
    limit: int = 50,
    offset: int = 0,
    db: AsyncSession = Depends(get_db)
):
    service = SocialService(db)
    return await service.get_comments(post_id, review_id, limit, offset)

@router.post('/interactions/upvote')
async def toggle_upvote(
    request: UpvoteRequest,
    db: AsyncSession = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    service = SocialService(db)
    is_upvoted = await service.toggle_upvote(UUID(user_id), request.target_id, request.target_type)
    return {"is_upvoted": is_upvoted}
