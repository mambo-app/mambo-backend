from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from typing import List, Dict, Any
from app.core.database import get_db
from app.services.news_service import NewsService
from app.models.common import ok

from app.models.content import NewsArticleResponse

router = APIRouter(tags=['news'])

@router.get('/', response_model=Dict[str, Any])
async def get_news(
    category: str = 'all',
    limit: int = 20,
    db: AsyncSession = Depends(get_db)
):
    service = NewsService(db)
    articles = await service.get_latest_news(category, limit)
    return ok({"items": articles})
