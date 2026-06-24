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
    # Fetch a larger pool of activities from DB to allow clean deduplication in Python
    db_limit = 100
    
    # Base query joining content to get title and poster
    # Filter by high-value activity types only
    query_str = '''
        SELECT a.*, p.username, p.display_name, p.avatar_url,
               c.title as content_title, c.poster_url as content_poster,
               r.is_spoiler as contains_spoiler
        FROM activity_log a
        JOIN profiles p ON p.id = a.user_id
        LEFT JOIN reviews r ON r.id = a.review_id
        LEFT JOIN content c ON c.id = COALESCE(a.content_id, r.content_id)
        WHERE a.visibility = 'public'
          AND a.activity_type IN ('watched', 'rewatched', 'reviewed', 'post')
    '''
    params = {'limit': db_limit}
    
    if user_id:
        query_str += " AND a.user_id = :uid"
        params['uid'] = user_id
        
    query_str += " ORDER BY a.created_at DESC LIMIT :limit"
    
    res = await db.execute(text(query_str), params)
    
    raw_items = []
    for row in res.mappings():
        item_dict = dict(row)
        item_dict['metadata'] = {
            'content_title': item_dict.get('content_title') or 'Content',
            'poster_url': item_dict.get('content_poster'),
            'contains_spoiler': bool(item_dict.get('contains_spoiler')),
            **(item_dict.get('metadata') or {})
        }
        raw_items.append(item_dict)

    # Deduplicate activities by (user_id, content_id) to prevent layout spam
    # If the user has multiple activities, we keep the most recent or highest priority one
    best_activities = {}
    for item in raw_items:
        key = (item['user_id'], item['content_id']) if item['content_id'] else (item['user_id'], f"post_{item['id']}")
        
        if key not in best_activities:
            best_activities[key] = item
        else:
            existing = best_activities[key]
            priority = {'reviewed': 4, 'post': 3, 'rewatched': 2, 'watched': 1}
            p_existing = priority.get(existing['activity_type'], 0)
            p_new = priority.get(item['activity_type'], 0)
            if p_new > p_existing:
                best_activities[key] = item

    # Re-sort deduplicated activities by timestamp (newest first)
    deduplicated = list(best_activities.values())
    deduplicated.sort(key=lambda x: x['created_at'], reverse=True)

    # Slice list to exactly 8 items (user requested 5-10 items to prevent mess)
    target_limit = 8
    items = deduplicated[offset : offset + target_limit]
    
    return ok({"items": items})

@router.get('/recently-watched', response_model=Dict[str, Any])
async def get_recently_watched(
    limit: int = Query(20, ge=1, le=100),
    db: AsyncSession = Depends(get_db),
    user_id: str = Depends(get_current_user_id)
):
    # Fetch from activity_log where type is watch/rewatch
    query = '''
        WITH RankedActivities AS (
            SELECT a.created_at as activity_at, c.*,
                   ROW_NUMBER() OVER(PARTITION BY c.id ORDER BY a.created_at DESC) as rn
            FROM activity_log a
            JOIN content c ON c.id = a.content_id
            JOIN user_content_status ucs ON ucs.content_id = c.id AND ucs.user_id = a.user_id
            WHERE a.user_id = :uid 
              AND a.activity_type IN ('watched', 'rewatched', 'rated', 'reviewed', 'updated_review')
              AND ucs.status = 'completed'
        )
        SELECT * FROM RankedActivities 
        WHERE rn = 1
        ORDER BY activity_at DESC
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
        WITH RankedRecs AS (
            SELECT a.created_at as recommended_at, c.*, p.username as sender_username, p.display_name as sender_name,
                   ROW_NUMBER() OVER(PARTITION BY a.content_id ORDER BY a.created_at DESC) as rn
            FROM activity_log a
            JOIN content c ON c.id = a.content_id
            JOIN profiles p ON p.id = a.related_user_id
            WHERE a.user_id = :uid AND a.activity_type = 'receive_recommendation'
        )
        SELECT * FROM RankedRecs WHERE rn = 1
        ORDER BY recommended_at DESC
        LIMIT :limit
    '''
    res = await db.execute(text(query), {'uid': UUID(user_id), 'limit': limit})
    return ok({"items": [dict(row) for row in res.mappings()]})