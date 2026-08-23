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

@router.get('/{content_id}/trailer')
async def get_content_trailer(
    content_id: str,
    db: AsyncSession = Depends(get_db)
):
    from app.services.content_service import ContentService
    from app.models.common import ok
    try:
        service = ContentService(db)
        trailer_info = await service.get_trailer_info(content_id)
        return ok(trailer_info)
    except Exception as err:
        logger.warning(f"Error fetching trailer for {content_id}: {err}")
        try: await db.rollback()
        except Exception: pass
        return ok({})

@router.post('/{content_id}/action', response_model=ContentActionResponse)
async def perform_content_action(
    content_id: str,
    req: ContentActionRequest,
    user_id_str: str = Depends(get_current_user_id),
    db: AsyncSession = Depends(get_db)
):
    from app.services.content_service import ContentService
    try:
        user_id = UUID(user_id_str)
        # Resolve content_id (could be UUID or TMDB/MAL ID)
        content_svc = ContentService(db)
        content = await content_svc.get_content_by_id(content_id, user_id_str)
        if not content:
            raise HTTPException(status_code=404, detail="Content not found")
        content_uuid = content.id
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid User ID or Content ID format")

    service = ActionService(db)
    
    try:
        return await service.handle_action(user_id, content_uuid, req)
    except Exception as e:
        logger.error(f"Error performing action {req.action} on {content_id} for user {user_id_str}: {e}")
        # Return the actual error message in dev mode to make debugging easier
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

@router.get('/{content_id}/credits')
async def get_content_credits(
    content_id: str,
    db: AsyncSession = Depends(get_db)
):
    from app.services.content_service import ContentService
    from app.models.common import ok
    try:
        service = ContentService(db)
        credits = await service.get_content_credits(content_id)
        return ok(credits)
    except Exception as err:
        logger.warning(f"Error fetching credits for {content_id}: {err}")
        try: await db.rollback()
        except Exception: pass
        return ok([])

@router.get('/{content_id}/similar')
async def get_similar_content(
    content_id: str,
    db: AsyncSession = Depends(get_db)
):
    from app.services.content_service import ContentService
    from app.models.common import ok
    try:
        service = ContentService(db)
        similar = await service.get_similar_content(content_id)
        return ok(similar)
    except Exception as err:
        logger.warning(f"Error fetching similar content for {content_id}: {err}")
        try: await db.rollback()
        except Exception: pass
        return ok([])

@router.get('/{content_id}/rating-history')
async def get_content_rating_history(
    content_id: str,
    tab: str = 'all',
    limit: int = 20,
    offset: int = 0,
    db: AsyncSession = Depends(get_db),
    user_id: str = Depends(get_current_user_id_optional)
):
    from app.services.action_service import ActionService
    from app.models.common import ok
    try:
        service = ActionService(db)
        vid = UUID(user_id) if user_id else None
        items = await service.get_content_rating_history(content_id, viewer_id=vid, tab=tab, limit=limit, offset=offset)
        return ok(items)
    except Exception as err:
        logger.warning(f"Error fetching rating history for {content_id}: {err}")
        try: await db.rollback()
        except Exception: pass
        return ok([])

@router.get('/{content_id}/season/{season_number}')
async def get_season_details(
    content_id: str,
    season_number: int,
    db: AsyncSession = Depends(get_db)
):
    from app.services.content_service import ContentService
    from app.models.common import ok
    try:
        service = ContentService(db)
        data = await service.get_season_details(content_id, season_number)
        return ok(data)
    except Exception as err:
        logger.warning(f"Error fetching season details for {content_id}: {err}")
        try: await db.rollback()
        except Exception: pass
        return ok({})

@router.get('/{content_id}/watch-providers')
async def get_watch_providers(
    content_id: str,
    country: str = "IN",
    db: AsyncSession = Depends(get_db)
):
    from app.services.content_service import ContentService
    from app.models.common import ok
    try:
        service = ContentService(db)
        providers = await service.get_watch_providers(content_id, country)
        return ok(providers)
    except Exception as err:
        logger.warning(f"Error fetching watch providers for {content_id}: {err}")
        try: await db.rollback()
        except Exception: pass
        return ok([])
