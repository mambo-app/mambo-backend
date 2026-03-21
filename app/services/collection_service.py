import logging
from uuid import UUID
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from typing import List, Dict, Optional, Any
from app.services.action_service import ActionService

logger = logging.getLogger('mambo.collections')

# Only these status filters map to real boolean columns in user_content_status
_STATUS_COLUMN_MAP = {
    'watched': 'is_watched',
    'dropped': 'is_dropped',
    'interested': 'is_interested',
    'liked': 'is_liked',
}

class CollectionService:
    def __init__(self, db: AsyncSession):
        self.db = db
        self.action_service = ActionService(db)

    async def get_user_collections(self, user_id: UUID) -> List[Dict]:
        res = await self.db.execute(text('''
            SELECT * FROM collections 
            WHERE user_id = :user_id 
            ORDER BY is_pinned DESC, pin_order ASC, created_at DESC
        '''), {'user_id': user_id})
        return [dict(row) for row in res.mappings()]

    async def create_collection(self, user_id: UUID, name: str, description: str = None, 
                                visibility: str = 'public') -> Dict:
        is_public = visibility == 'public'
        stmt = text('''
            INSERT INTO collections (user_id, name, description, is_public, visibility, is_default, is_deletable)
            VALUES (:user_id, :name, :description, :is_public, :visibility, false, true)
            RETURNING *
        ''')
        res = await self.db.execute(stmt, {
            'user_id': user_id,
            'name': name,
            'description': description,
            'is_public': is_public,
            'visibility': visibility
        })
        collection = dict(res.mappings().first())
        await self.db.commit()
        
        # await self.action_service._log_activity(user_id, 'created_collection', collection_id=collection['id'])
        
        return collection

    async def update_collection(self, user_id: UUID, collection_id: UUID, **kwargs) -> Optional[Dict]:
        # Ownership check
        check = await self.db.execute(text("SELECT user_id FROM collections WHERE id = :cid"), {'cid': collection_id})
        row = check.mappings().first()
        if not row or row['user_id'] != user_id:
            return None

        if not kwargs:
            res = await self.db.execute(text("SELECT * FROM collections WHERE id = :cid"), {'cid': collection_id})
            return dict(res.mappings().first())

        cols = []
        params: Dict[str, Any] = {'cid': collection_id}
        for k, v in kwargs.items():
            cols.append(f"{k} = :{k}")
            params[k] = v
        
        # Sync is_public if visibility changed
        if 'visibility' in kwargs and 'is_public' not in kwargs:
            is_public = kwargs['visibility'] == 'public'
            cols.append("is_public = :is_public")
            params['is_public'] = is_public
        
        stmt = text(f"UPDATE collections SET {', '.join(cols)}, updated_at = now() WHERE id = :cid RETURNING *")
        res = await self.db.execute(stmt, params)
        updated = dict(res.mappings().first())
        await self.db.commit()
        return updated

    async def delete_collection(self, user_id: UUID, collection_id: UUID) -> bool:
        # Ownership check
        check = await self.db.execute(text("SELECT user_id, is_deletable FROM collections WHERE id = :cid"), {'cid': collection_id})
        row = check.mappings().first()
        if not row or row['user_id'] != user_id:
            return False
        
        if not row['is_deletable']:
            return False

        # Delete items first (though FK should handle it if set to cascade, but let's be safe if it's not)
        await self.db.execute(text("DELETE FROM collection_items WHERE collection_id = :cid"), {'cid': collection_id})
        
        # Delete collection
        await self.db.execute(text("DELETE FROM collections WHERE id = :cid"), {'cid': collection_id})
        await self.db.commit()
        return True

    async def add_item_to_collection(self, user_id: UUID, collection_id: UUID, content_id: UUID) -> bool:
        # Check if collection belongs to user
        coll_check = await self.db.execute(text("SELECT user_id, name FROM collections WHERE id = :cid"), {'cid': collection_id})
        coll_row = coll_check.mappings().first()
        if not coll_row:
            return False
            
        coll = dict(coll_row)
        if coll['user_id'] != user_id:
            return False

        # Add item
        stmt = text('''
            INSERT INTO collection_items (collection_id, content_id, added_by)
            VALUES (:collection_id, :content_id, :user_id)
            ON CONFLICT (collection_id, content_id) DO NOTHING
        ''')
        await self.db.execute(stmt, {
            'collection_id': collection_id,
            'content_id': content_id,
            'user_id': user_id
        })
        
        # Increment count
        await self.db.execute(text('''
            UPDATE collections SET item_count = item_count + 1, updated_at = now() WHERE id = :cid
        '''), {'cid': collection_id})
        
        # Special case: if adds to Watchlist/Favorites, update user_content_status
        if coll['name'].lower() == 'watchlist':
            await self.action_service._update_status_flag(user_id, content_id, 'is_interested', True)
        elif coll['name'].lower() == 'favorites':
            await self.action_service._update_status_flag(user_id, content_id, 'is_liked', True)

        await self.db.commit()
        
        # await self.action_service._log_activity(user_id, 'added_to_collection', content_id=content_id, collection_id=collection_id)
        
        return True

    async def remove_item_from_collection(self, user_id: UUID, collection_id: UUID, content_id: UUID) -> bool:
        coll_check = await self.db.execute(text("SELECT user_id, name FROM collections WHERE id = :cid"), {'cid': collection_id})
        coll_row = coll_check.mappings().first()
        if not coll_row:
            return False
            
        coll = dict(coll_row)
        if coll['user_id'] != user_id:
            return False

        res = await self.db.execute(text('''
            DELETE FROM collection_items WHERE collection_id = :cid AND content_id = :coid
        '''), {'cid': collection_id, 'coid': content_id})
        
        if res.rowcount > 0:
            await self.db.execute(text('''
                UPDATE collections SET item_count = GREATEST(0, item_count - 1), updated_at = now() WHERE id = :cid
            '''), {'cid': collection_id})
            
            # If removed from Watchlist, update status
            if coll['name'].lower() == 'watchlist':
                await self.action_service._update_status_flag(user_id, content_id, 'is_interested', False)
            
            await self.db.commit()
            return True
            
        return False

    async def get_collection_items(
        self, user_id: UUID, collection_id: UUID, 
        content_type: Optional[str] = None,
        genre: Optional[str] = None,
        status: Optional[str] = None,
        streaming_platform: Optional[str] = None
    ) -> List[Dict]:
        # Use ott_availability (jsonb) instead of streaming_platforms (doesn't exist)
        # Use boolean flags for status instead of non-existent ucs.status enum
        query = '''
            SELECT co.id, co.title, co.content_type, co.poster_url, co.backdrop_url,
                   co.external_rating, co.release_date, co.genres, co.status as content_status,
                   ci.added_at,
                   ucs.is_watched, ucs.is_dropped, ucs.is_interested, ucs.is_liked
            FROM collection_items ci
            JOIN content co ON co.id = ci.content_id
            LEFT JOIN user_content_status ucs ON ucs.content_id = ci.content_id AND ucs.user_id = :uid
            WHERE ci.collection_id = :cid
        '''
        params: Dict[str, Any] = {'cid': collection_id, 'uid': user_id}
        
        if content_type:
            query += " AND co.content_type = :ctype"
            params['ctype'] = content_type
            
        if genre:
            # genres stored as text[] array
            query += " AND :genre = ANY(co.genres)"
            params['genre'] = genre
            
        if status:
            # Map status string to the boolean column it represents
            bool_col = _STATUS_COLUMN_MAP.get(status)
            if bool_col:
                query += f" AND ucs.{bool_col} = true"
            # If unknown status just ignore rather than crash
            
            
        query += " ORDER BY ci.added_at DESC"
        
        res = await self.db.execute(text(query), params)
        return [dict(row) for row in res.mappings()]

    async def get_content_collection_status(self, user_id: UUID, content_id: UUID) -> List[UUID]:
        """Return a list of collection IDs that contain this content for the user."""
        res = await self.db.execute(text('''
            SELECT ci.collection_id 
            FROM collection_items ci
            JOIN collections c ON c.id = ci.collection_id
            WHERE c.user_id = :user_id AND ci.content_id = :content_id
        '''), {'user_id': user_id, 'content_id': content_id})
        return [row[0] for row in res.fetchall()]
