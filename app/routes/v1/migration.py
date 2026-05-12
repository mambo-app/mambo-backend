from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import StreamingResponse
from ...repositories.social_repo import SocialRepository
from ...services.migration_service import MigrationService
from ...core.database import get_db
from ...core.dependencies import get_current_user_id
from sqlalchemy.ext.asyncio import AsyncSession
from ...services.user_service import UserService
from datetime import datetime
from uuid import UUID

router = APIRouter(tags=["migration"])

@router.get("/export")
async def export_data(
    user_id: str = Depends(get_current_user_id),
    db: AsyncSession = Depends(get_db)
):
    """Export all user data as a ZIP file."""
    try:
        user_service = UserService(db)
        profile = await user_service.get_by_id(user_id)
        username = profile.get('username', 'user') if profile else 'user'

        social_repo = SocialRepository(db)
        migration_service = MigrationService(social_repo)
        
        zip_buffer = await migration_service.export_mambo_data(UUID(user_id))
        
        filename = f"mambo_export_{username}_{datetime.now().strftime('%Y%m%d')}.zip"
        
        return StreamingResponse(
            zip_buffer,
            media_type="application/x-zip-compressed",
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

