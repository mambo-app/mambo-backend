from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from uuid import UUID
from app.core.database import get_db
from app.core.dependencies import get_current_user_id, get_current_user_id_optional
from app.models.action import ContentActionRequest, ContentActionResponse
from app.services.action_service import ActionService
import logging

logger = logging.getLogger('mambo.content_routes')

router = APIRouter(tags=['content'])

@router.get('/{content_id}')
async def get_content_details(
    content_id: str,
    db: AsyncSession = Depends(get_db),
    user_id: str = Depends(get_current_user_id_optional)
):
    from app.services.content_service import ContentService
    from app.models.common import ok
    service = ContentService(db)
    content = await service.get_content_by_id(content_id, user_id)
    if not content:
        raise HTTPException(status_code=404, detail="Content not found")
    return ok(content)

@router.post('/{content_id}/action', response_model=ContentActionResponse)
async def perform_content_action(
    content_id: str,
    req: ContentActionRequest,
    user_id_str: str = Depends(get_current_user_id),
    db: AsyncSession = Depends(get_db)
):
    try:
        user_id = UUID(user_id_str)
        content_uuid = UUID(content_id)
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid User ID or Content ID format")

    service = ActionService(db)
    
    try:
        return await service.handle_action(user_id, content_uuid, req)
    except Exception as e:
        logger.error(f"Error performing action {req.action} on {content_id} for user {user_id}: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@router.get('/{content_id}/credits')
async def get_content_credits(
    content_id: str,
    db: AsyncSession = Depends(get_db)
):
    from app.services.content_service import ContentService
    service = ContentService(db)
    credits = await service.get_content_credits(content_id)
    from app.models.common import ok
    return ok(credits)

@router.get('/{content_id}/similar')
async def get_similar_content(
    content_id: str,
    db: AsyncSession = Depends(get_db)
):
    from app.services.content_service import ContentService
    service = ContentService(db)
    similar = await service.get_similar_content(content_id)
    from app.models.common import ok
    return ok(similar)

@router.get('/{content_id}/rating-history')
async def get_content_rating_history(
    content_id: UUID,
    tab: str = 'all',
    limit: int = 20,
    offset: int = 0,
    db: AsyncSession = Depends(get_db),
    user_id: str = Depends(get_current_user_id_optional)
):
    from app.services.action_service import ActionService
    from app.models.common import ok
    service = ActionService(db)
    vid = UUID(user_id) if user_id else None
    items = await service.get_content_rating_history(content_id, viewer_id=vid, tab=tab, limit=limit, offset=offset)
    return ok(items)
