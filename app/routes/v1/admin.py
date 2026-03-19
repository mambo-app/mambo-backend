from fastapi import APIRouter, Depends, HTTPException, Header
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from app.core.database import get_db
from app.core.config import settings

router = APIRouter(tags=['admin'])

def verify_admin(x_admin_secret: str = Header(...)):
    """Verify admin secret — separate from the user invite key."""
    if x_admin_secret != settings.admin_secret:
        raise HTTPException(status_code=403, detail="Unauthorized")
    return True

@router.delete('/cleanup', dependencies=[Depends(verify_admin)])
async def cleanup_temporary_data(db: AsyncSession = Depends(get_db)):
    """
    Cleans up content and news articles that are not marked as permanent
    and were last synced more than 24 hours ago.
    """
    try:
        # Delete temporary content older than 1 day
        res_content = await db.execute(text("""
            DELETE FROM content 
            WHERE is_permanent = false 
            AND last_synced_at < NOW() - INTERVAL '1 day'
        """))
        
        # Delete temporary news articles older than 1 day
        res_news = await db.execute(text("""
            DELETE FROM news_articles 
            WHERE is_permanent = false 
            AND fetched_at < NOW() - INTERVAL '1 day'
        """))
        
        await db.commit()
        return {
            "status": "success", 
            "deleted_content": res_content.rowcount,
            "deleted_news": res_news.rowcount
        }
    except Exception as e:
        await db.rollback()
        raise HTTPException(status_code=500, detail=str(e))
