from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from typing import List, Dict, Any, Optional
from app.core.database import get_db
from app.core.dependencies import get_current_user_id
from app.models.common import ok
from uuid import UUID

router = APIRouter()

@router.get('/', response_model=Dict[str, Any])
async def get_activity_feed(
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
    user_id: Optional[UUID] = None, # Filter by user
    db: AsyncSession = Depends(get_db),
    current_user_id: str = Depends(get_current_user_id)
):
    # Base query
    query_str = '''
        SELECT a.*, p.username, p.display_name, p.avatar_url
        FROM activity_log a
        JOIN profiles p ON p.id = a.user_id
        WHERE a.visibility = 'public'
    '''
    params = {'limit': limit, 'offset': offset}
    
    if user_id:
        query_str += " AND a.user_id = :uid"
        params['uid'] = user_id
        
    query_str += " ORDER BY a.created_at DESC LIMIT :limit OFFSET :offset"
    
    res = await db.execute(text(query_str), params)
    items = [dict(row) for row in res.mappings()]
    
    return ok({"items": items})

@router.get('/recently-watched', response_model=Dict[str, Any])
async def get_recently_watched(
    limit: int = Query(20, ge=1, le=100),
    db: AsyncSession = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    # Fetch from activity_log where type is watch/rewatch
    query = '''
        SELECT a.created_at as activity_at, c.*
        FROM activity_log a
        JOIN content c ON c.id = a.content_id
        WHERE a.user_id = :uid AND a.activity_type IN ('watched', 'rewatched')
        ORDER BY a.created_at DESC
        LIMIT :limit
    '''
    res = await db.execute(text(query), {'uid': UUID(user_id), 'limit': limit})
    return ok({"items": [dict(row) for row in res.mappings()]})

@router.get('/recommendations', response_model=Dict[str, Any])
async def get_received_recommendations(
    limit: int = Query(20, ge=1, le=100),
    db: AsyncSession = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    # Fetch from activity_log where type is receive_recommendation
    query = '''
        SELECT a.created_at as recommended_at, c.*, p.username as sender_username, p.display_name as sender_name
        FROM activity_log a
        JOIN content c ON c.id = a.content_id
        JOIN profiles p ON p.id = a.related_user_id
        WHERE a.user_id = :uid AND a.activity_type = 'receive_recommendation'
        ORDER BY a.created_at DESC
        LIMIT :limit
    '''
    res = await db.execute(text(query), {'uid': UUID(user_id), 'limit': limit})
    return ok({"items": [dict(row) for row in res.mappings()]})