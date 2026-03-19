from fastapi import APIRouter, Depends, Query, HTTPException, Security
from sqlalchemy.ext.asyncio import AsyncSession
from app.core.database import get_db
from app.core.dependencies import get_current_user_id
from app.models.common import ok
from app.models.report import ReportCreateRequest
from app.services.report_service import ReportService
from fastapi.security.api_key import APIKeyHeader

router = APIRouter()

admin_scheme = APIKeyHeader(name="X-Admin-Secret", auto_error=False)

def verify_admin(api_key: str = Security(admin_scheme)):
    from app.core.config import settings
    if api_key != settings.admin_secret:
        raise HTTPException(status_code=403, detail="Not authorized")

@router.post('/')
async def submit_report(
    req: ReportCreateRequest,
    db: AsyncSession = Depends(get_db),
    user_id: str = Depends(get_current_user_id),
):
    service = ReportService(db)
    result = await service.create_report(user_id, req.model_dump(exclude_unset=True))
    return ok(result)

@router.get('/')
async def get_all_reports(
    page: int = Query(1, ge=1),
    limit: int = Query(50, ge=1, le=100),
    db: AsyncSession = Depends(get_db),
    _admin: None = Depends(verify_admin)
):
    service = ReportService(db)
    offset = (page - 1) * limit
    items = await service.get_reports(limit, offset)
    return ok({"items": items})
