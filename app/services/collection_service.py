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

    async def get_user_collections(self, user_id: UUID, viewer_id: Optional[UUID] = None) -> List[Dict]:
        v_id = str(viewer_id) if viewer_id else str(user_id)
        res = await self.db.execute(text('''
            SELECT c.*, 
                   prof.username as creator_username,
                   prof.avatar_url as creator_avatar_url,
                   prof.display_name as creator_display_name,
                   COALESCE(p.posters, '[]'::json) as preview_posters,
                   COALESCE(tc.counts, '{"movies": 0, "series": 0, "anime": 0}'::json) as type_counts,
                   EXISTS (
                       SELECT 1 FROM saved_collections sc 
                       WHERE sc.collection_id = c.id AND sc.user_id = CAST(:viewer_id AS UUID)
                   ) as is_saved,
                   COALESCE((
                       SELECT json_agg(sub_p.avatar_url) FROM (
                           SELECT prof2.avatar_url
                           FROM saved_collections sc2
                           JOIN profiles prof2 ON prof2.id = sc2.user_id
                           WHERE sc2.collection_id = c.id AND prof2.avatar_url IS NOT NULL
                           ORDER BY sc2.saved_at DESC
                           LIMIT 4
                       ) sub_p
                   ), '[]'::json) as savers_avatars
            FROM collections c
            JOIN profiles prof ON prof.id = c.user_id
            LEFT JOIN LATERAL (
                SELECT 
                    json_agg(sub.poster_url) as posters,
                    json_agg(sub.backdrop_url) as backdrops
                FROM (
                    SELECT co.poster_url, co.backdrop_url
                    FROM collection_items ci
                    JOIN content co ON co.id = ci.content_id
                    WHERE ci.collection_id = c.id
                    AND (co.poster_url IS NOT NULL OR co.backdrop_url IS NOT NULL)
                    ORDER BY ci.added_at DESC
                    LIMIT 4
                ) sub
            ) p ON true
            LEFT JOIN LATERAL (
                SELECT json_build_object(
                    'movies', COUNT(*) FILTER (WHERE co.content_type = 'movie'),
                    'series', COUNT(*) FILTER (WHERE co.content_type = 'series'),
                    'anime', COUNT(*) FILTER (WHERE co.content_type = 'anime')
                ) as counts
                FROM collection_items ci
                JOIN content co ON co.id = ci.content_id
                WHERE ci.collection_id = c.id
            ) tc ON true
            WHERE (
                c.user_id = :user_id 
                OR EXISTS (
                    SELECT 1 FROM saved_collections sc_user 
                    WHERE sc_user.collection_id = c.id AND sc_user.user_id = :user_id
                )
            )
            AND (
                c.visibility = 'public' 
                OR c.user_id = CAST(:viewer_id AS UUID)
                OR (
                    c.visibility = 'friends_only' 
                    AND CAST(:viewer_id AS UUID) IS NOT NULL 
                    AND EXISTS (
                        SELECT 1 FROM friends 
                        WHERE (user_id1 = c.user_id AND user_id2 = CAST(:viewer_id AS UUID))
                           OR (user_id2 = c.user_id AND user_id1 = CAST(:viewer_id AS UUID))
                    )
                )
            )
            ORDER BY c.pin_order ASC NULLS LAST, c.created_at DESC
        '''), {'user_id': user_id, 'viewer_id': v_id})
        return [dict(row) for row in res.mappings()]

    async def get_collection_by_id(self, collection_id: UUID, viewer_id: Optional[UUID] = None) -> Optional[Dict]:
        v_id = str(viewer_id) if viewer_id else None
        res = await self.db.execute(text('''
            SELECT c.*, 
                   prof.username as creator_username,
                   prof.avatar_url as creator_avatar_url,
                   prof.display_name as creator_display_name,
                   COALESCE(p.posters, '[]'::json) as preview_posters,
                   COALESCE(tc.counts, '{"movies": 0, "series": 0, "anime": 0}'::json) as type_counts,
                   EXISTS (
                       SELECT 1 FROM saved_collections sc 
                       WHERE sc.collection_id = c.id AND sc.user_id = CAST(:viewer_id AS UUID)
                   ) as is_saved,
                   COALESCE((
                       SELECT json_agg(sub_p.avatar_url) FROM (
                           SELECT prof2.avatar_url
                           FROM saved_collections sc2
                           JOIN profiles prof2 ON prof2.id = sc2.user_id
                           WHERE sc2.collection_id = c.id AND prof2.avatar_url IS NOT NULL
                           ORDER BY sc2.saved_at DESC
                           LIMIT 4
                       ) sub_p
                   ), '[]'::json) as savers_avatars
            FROM collections c
            JOIN profiles prof ON prof.id = c.user_id
            LEFT JOIN LATERAL (
                SELECT 
                    json_agg(sub.poster_url) as posters,
                    json_agg(sub.backdrop_url) as backdrops
                FROM (
                    SELECT co.poster_url, co.backdrop_url
                    FROM collection_items ci
                    JOIN content co ON co.id = ci.content_id
                    WHERE ci.collection_id = c.id
                    AND (co.poster_url IS NOT NULL OR co.backdrop_url IS NOT NULL)
                    ORDER BY CASE WHEN c.is_ranked = true THEN ci.position END ASC, ci.added_at DESC
                    LIMIT 4
                ) sub
            ) p ON true
            LEFT JOIN LATERAL (
                SELECT json_build_object(
                    'movies', COUNT(*) FILTER (WHERE co.content_type = 'movie'),
                    'series', COUNT(*) FILTER (WHERE co.content_type = 'series'),
                    'anime', COUNT(*) FILTER (WHERE co.content_type = 'anime')
                ) as counts
                FROM collection_items ci
                JOIN content co ON co.id = ci.content_id
                WHERE ci.collection_id = c.id
            ) tc ON true
            WHERE c.id = :cid
        '''), {'cid': collection_id, 'viewer_id': v_id})
        row = res.mappings().first()
        return dict(row) if row else None

    async def save_collection(self, user_id: UUID, collection_id: UUID) -> bool:
        check = await self.db.execute(
            text("SELECT id, user_id, visibility FROM collections WHERE id = :cid"),
            {'cid': collection_id}
        )
        row = check.mappings().first()
        if not row or row['visibility'] != 'public':
            return False

        await self.db.execute(
            text('''
                INSERT INTO saved_collections (user_id, collection_id)
                VALUES (:uid, :cid)
                ON CONFLICT (user_id, collection_id) DO NOTHING
            '''),
            {'uid': user_id, 'cid': collection_id}
        )
        await self.db.execute(
            text('''
                UPDATE collections
                SET saves_count = (SELECT COUNT(*) FROM saved_collections WHERE collection_id = :cid)
                WHERE id = :cid
            '''),
            {'cid': collection_id}
        )
        await self.db.commit()
        return True

    async def unsave_collection(self, user_id: UUID, collection_id: UUID) -> bool:
        await self.db.execute(
            text("DELETE FROM saved_collections WHERE user_id = :uid AND collection_id = :cid"),
            {'uid': user_id, 'cid': collection_id}
        )
        await self.db.execute(
            text('''
                UPDATE collections
                SET saves_count = (SELECT COUNT(*) FROM saved_collections WHERE collection_id = :cid)
                WHERE id = :cid
            '''),
            {'cid': collection_id}
        )
        await self.db.commit()
        return True

    async def get_collection_savers(self, collection_id: UUID) -> List[Dict]:
        res = await self.db.execute(
            text('''
                SELECT p.id, p.username, p.display_name, p.avatar_url, p.is_verified, sc.saved_at
                FROM saved_collections sc
                JOIN profiles p ON p.id = sc.user_id
                WHERE sc.collection_id = :cid
                ORDER BY sc.saved_at DESC
            '''),
            {'cid': collection_id}
        )
        return [dict(row) for row in res.mappings()]

    async def create_collection(self, user_id: UUID, name: str, description: str = None, 
                                visibility: str = 'public', is_ranked: bool = False) -> Dict:
        is_public = visibility == 'public'
        stmt = text('''
            INSERT INTO collections (user_id, name, description, is_public, visibility, is_default, is_deletable, collection_type, is_ranked)
            VALUES (:user_id, :name, :description, :is_public, :visibility, false, true, 'custom', :is_ranked)
            RETURNING *
        ''')
        res = await self.db.execute(stmt, {
            'user_id': user_id,
            'name': name,
            'description': description,
            'is_public': is_public,
            'visibility': visibility,
            'is_ranked': is_ranked
        })
        collection = dict(res.mappings().first())
        await self.db.commit()
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

    async def reorder_collections(self, user_id: UUID, collection_ids: List[UUID]) -> bool:
        for idx, cid in enumerate(collection_ids):
            # Also verify ownership
            await self.db.execute(text('''
                UPDATE collections 
                SET pin_order = :order, updated_at = now() 
                WHERE id = :cid AND user_id = :uid
            '''), {'order': idx, 'cid': cid, 'uid': user_id})
        await self.db.commit()
        return True

    async def delete_collection(self, user_id: UUID, collection_id: UUID) -> bool:
        # Ownership check
        check = await self.db.execute(text("SELECT user_id, is_deletable FROM collections WHERE id = :cid"), {'cid': collection_id})
        row = check.mappings().first()
        if not row or row['user_id'] != user_id:
            return False
        
        if not row['is_deletable']:
            return False

        # Delete items first
        await self.db.execute(text("DELETE FROM collection_items WHERE collection_id = :cid"), {'cid': collection_id})
        
        # Delete collection
        await self.db.execute(text("DELETE FROM collections WHERE id = :cid"), {'cid': collection_id})
        await self.db.commit()
        return True

    async def add_item_to_collection(self, user_id: UUID, collection_id: UUID, content_id: UUID) -> bool:
        # Guarantee content row exists in PostgreSQL content table
        from app.services.content_service import ContentService
        content_svc = ContentService(self.db)
        content_id = await content_svc.ensure_content_persisted(content_id)

        # Check if collection belongs to user
        coll_check = await self.db.execute(text("SELECT user_id, name FROM collections WHERE id = :cid"), {'cid': collection_id})
        coll_row = coll_check.mappings().first()
        if not coll_row:
            return False
            
        coll = dict(coll_row)
        if coll['user_id'] != user_id:
            return False

        # Get the next position rank
        pos_check = await self.db.execute(text('''
            SELECT COALESCE(MAX(position), 0) + 1 as next_pos 
            FROM collection_items 
            WHERE collection_id = :cid
        '''), {'cid': collection_id})
        next_pos = pos_check.scalar() or 1

        # Add item
        stmt = text('''
            INSERT INTO collection_items (collection_id, content_id, added_by, position)
            VALUES (:collection_id, :content_id, :user_id, :position)
            ON CONFLICT (collection_id, content_id) DO NOTHING
        ''')
        await self.db.execute(stmt, {
            'collection_id': collection_id,
            'content_id': content_id,
            'user_id': user_id,
            'position': next_pos
        })
        
        # Increment count
        await self.db.execute(text('''
            UPDATE collections SET item_count = item_count + 1, updated_at = now() WHERE id = :cid
        '''), {'cid': collection_id})
        
        # Special case: if adds to Watchlist/Favorites, update user_content_status
        if coll['name'].lower() == 'watchlist':
            await self.action_service._update_status_flag(user_id, content_id, 'is_interested', True)
            await self.action_service._log_activity(user_id, 'interested', content_id=content_id)
        elif coll['name'].lower() == 'favorites':
            await self.action_service._update_status_flag(user_id, content_id, 'is_liked', True)

        await self.db.commit()
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
                await self.action_service._remove_activity(user_id, ['interested', 'saved', 'watchlist'], content_id=content_id)
            
        await self.db.commit()
        return True

    async def get_collection_items(
        self, user_id: UUID, collection_id: UUID, 
        content_type: Optional[str] = None,
        genre: Optional[str] = None,
        status: Optional[str] = None,
        streaming_platform: Optional[str] = None
    ) -> List[Dict]:
        query = '''
            SELECT co.id, co.title, co.content_type, co.poster_url, co.backdrop_url,
                   co.external_rating, co.release_date, co.genres, co.original_language, co.status as content_status,
                   ci.added_at, ci.position,
                   ucs.is_watched, ucs.is_dropped, ucs.is_interested, ucs.is_liked,
                   COALESCE(
                       ucs.rating,
                       (SELECT COALESCE(r.rating, r.star_rating) FROM reviews r WHERE r.user_id = c.user_id AND r.content_id = ci.content_id AND r.is_deleted = false ORDER BY r.created_at DESC LIMIT 1),
                       (SELECT wh.rating FROM watch_history wh WHERE wh.user_id = c.user_id AND wh.content_id = ci.content_id AND wh.rating IS NOT NULL ORDER BY wh.watched_at DESC LIMIT 1)
                   ) as user_rating,
                   (SELECT CAST(id AS VARCHAR) FROM reviews r WHERE r.user_id = c.user_id AND r.content_id = ci.content_id AND r.is_deleted = false AND (r.text_review IS NOT NULL AND TRIM(r.text_review) != '') ORDER BY r.created_at DESC LIMIT 1) as review_id,
                   (SELECT text_review FROM reviews r WHERE r.user_id = c.user_id AND r.content_id = ci.content_id AND r.is_deleted = false AND (r.text_review IS NOT NULL AND TRIM(r.text_review) != '') ORDER BY r.created_at DESC LIMIT 1) as review_text
            FROM collection_items ci
            JOIN content co ON co.id = ci.content_id
            JOIN collections c ON c.id = ci.collection_id
            LEFT JOIN user_content_status ucs ON ucs.content_id = ci.content_id AND ucs.user_id = c.user_id
            WHERE ci.collection_id = :cid
        '''
        params: Dict[str, Any] = {'cid': collection_id, 'uid': user_id}
        
        if content_type:
            query += " AND co.content_type = :ctype"
            params['ctype'] = content_type
            
        if genre:
            query += " AND :genre = ANY(co.genres)"
            params['genre'] = genre
            
        if status:
            bool_col = _STATUS_COLUMN_MAP.get(status)
            if bool_col:
                query += f" AND ucs.{bool_col} = true"
            
        query += " ORDER BY CASE WHEN c.is_ranked = true THEN ci.position END ASC, ci.added_at DESC"
        
        res = await self.db.execute(text(query), params)
        items = []
        for row in res.mappings():
            d = dict(row)
            if d.get('user_rating') is not None:
                try: d['user_rating'] = float(d['user_rating'])
                except Exception: pass
            if d.get('external_rating') is not None:
                try: d['external_rating'] = float(d['external_rating'])
                except Exception: pass
            items.append(d)
        return items

    async def get_content_collection_status(self, user_id: UUID, content_id: UUID) -> List[UUID]:
        res = await self.db.execute(text('''
            SELECT DISTINCT ci.collection_id 
            FROM collection_items ci
            JOIN collections c ON c.id = ci.collection_id
            LEFT JOIN content target_c ON target_c.id = :content_id
            LEFT JOIN content item_c ON item_c.id = ci.content_id
            WHERE c.user_id = :user_id 
              AND (
                ci.content_id = :content_id
                OR (target_c.tmdb_id IS NOT NULL AND item_c.tmdb_id = target_c.tmdb_id)
                OR (target_c.mal_id IS NOT NULL AND item_c.mal_id = target_c.mal_id)
              )

            UNION

            SELECT c.id as collection_id
            FROM collections c
            JOIN user_content_status ucs ON ucs.user_id = c.user_id
            LEFT JOIN content target_c ON target_c.id = :content_id
            LEFT JOIN content ucs_c ON ucs_c.id = ucs.content_id
            WHERE c.user_id = :user_id 
              AND (c.collection_type = 'watchlist' OR LOWER(c.name) = 'watchlist')
              AND ucs.is_interested = true
              AND (
                ucs.content_id = :content_id
                OR (target_c.tmdb_id IS NOT NULL AND ucs_c.tmdb_id = target_c.tmdb_id)
                OR (target_c.mal_id IS NOT NULL AND ucs_c.mal_id = target_c.mal_id)
              )
        '''), {'user_id': user_id, 'content_id': content_id})
        return [row[0] for row in res.fetchall()]

    async def reorder_collection_items(self, user_id: UUID, collection_id: UUID, content_ids: List[UUID]) -> bool:
        # Ownership check
        check = await self.db.execute(text("SELECT user_id FROM collections WHERE id = :cid"), {'cid': collection_id})
        row = check.mappings().first()
        if not row or row['user_id'] != user_id:
            return False

        # Update position for each item
        for idx, content_id in enumerate(content_ids):
            await self.db.execute(text('''
                UPDATE collection_items 
                SET position = :pos 
                WHERE collection_id = :cid AND content_id = :coid
            '''), {'pos': idx + 1, 'cid': collection_id, 'coid': content_id})
        
        await self.db.commit()
        return True

    async def get_public_custom_collections(self, limit: int = 50, offset: int = 0) -> List[Dict]:
        res = await self.db.execute(text('''
            SELECT c.*, 
                   prof.username as creator_username,
                   prof.avatar_url as creator_avatar_url,
                   prof.display_name as creator_display_name,
                   COALESCE(p.posters, '[]'::json) as preview_posters,
                   COALESCE(p.backdrops, '[]'::json) as preview_backdrops,
                   COALESCE(tc.counts, '{"movies": 0, "series": 0, "anime": 0}'::json) as type_counts,
                   COALESCE((
                       SELECT json_agg(sub_p.avatar_url) FROM (
                           SELECT prof2.avatar_url
                           FROM saved_collections sc2
                           JOIN profiles prof2 ON prof2.id = sc2.user_id
                           WHERE sc2.collection_id = c.id AND prof2.avatar_url IS NOT NULL
                           ORDER BY sc2.saved_at DESC
                           LIMIT 4
                       ) sub_p
                   ), '[]'::json) as savers_avatars
            FROM collections c
            JOIN profiles prof ON prof.id = c.user_id
            LEFT JOIN LATERAL (
                SELECT 
                    json_agg(sub.poster_url) as posters,
                    json_agg(sub.backdrop_url) as backdrops
                FROM (
                    SELECT co.poster_url, co.backdrop_url
                    FROM collection_items ci
                    JOIN content co ON co.id = ci.content_id
                    WHERE ci.collection_id = c.id
                    AND (co.poster_url IS NOT NULL OR co.backdrop_url IS NOT NULL)
                    ORDER BY CASE WHEN c.is_ranked = true THEN ci.position END ASC, ci.added_at DESC
                    LIMIT 4
                ) sub
            ) p ON true
            LEFT JOIN LATERAL (
                SELECT json_build_object(
                    'movies', COUNT(*) FILTER (WHERE co.content_type = 'movie'),
                    'series', COUNT(*) FILTER (WHERE co.content_type = 'series'),
                    'anime', COUNT(*) FILTER (WHERE co.content_type = 'anime')
                ) as counts
                FROM collection_items ci
                JOIN content co ON co.id = ci.content_id
                WHERE ci.collection_id = c.id
            ) tc ON true
            WHERE c.is_default = false
              AND LOWER(c.name) NOT IN ('watchlist', 'watched', 'dropped', 'favorites')
              AND LOWER(c.collection_type) NOT IN ('watchlist', 'watched', 'watched_films', 'dropped', 'favorites')
              AND (c.visibility = 'public' OR c.visibility = 'friends_only' OR c.is_public = true OR c.visibility IS NULL)
              AND prof.is_deleted = false
            ORDER BY c.created_at DESC
            LIMIT :limit OFFSET :offset
        '''), {'limit': limit, 'offset': offset})
        return [dict(row) for row in res.mappings()]

    async def get_collection_groups(self, collection_id: UUID) -> List[Dict]:
        res = await self.db.execute(text('''
            SELECT cg.*,
                   COALESCE(json_agg(cgi.content_id) FILTER (WHERE cgi.content_id IS NOT NULL), '[]'::json) as content_ids,
                   COALESCE(json_agg(co.poster_url) FILTER (WHERE co.poster_url IS NOT NULL), '[]'::json) as preview_posters
            FROM collection_groups cg
            LEFT JOIN collection_group_items cgi ON cgi.group_id = cg.id
            LEFT JOIN content co ON co.id = cgi.content_id
            WHERE cg.collection_id = :cid
            GROUP BY cg.id
            ORDER BY cg.created_at ASC
        '''), {'cid': collection_id})
        return [dict(row) for row in res.mappings()]

    async def create_collection_group(self, user_id: UUID, collection_id: UUID, name: str, content_ids: List[UUID]) -> Dict:
        res = await self.db.execute(text('''
            INSERT INTO collection_groups (collection_id, user_id, name)
            VALUES (:cid, :uid, :name)
            RETURNING *
        '''), {'cid': collection_id, 'uid': user_id, 'name': name})
        group = dict(res.mappings().first())

        for co_id in content_ids:
            await self.db.execute(text('''
                INSERT INTO collection_group_items (group_id, content_id)
                VALUES (:gid, :coid)
                ON CONFLICT DO NOTHING
            '''), {'gid': group['id'], 'coid': co_id})

        await self.db.commit()
        return group

    async def update_collection_group(self, user_id: UUID, group_id: UUID, name: Optional[str], content_ids: Optional[List[UUID]]) -> bool:
        if name:
            await self.db.execute(text('''
                UPDATE collection_groups SET name = :name WHERE id = :gid AND user_id = :uid
            '''), {'name': name, 'gid': group_id, 'uid': user_id})

        if content_ids is not None:
            await self.db.execute(text("DELETE FROM collection_group_items WHERE group_id = :gid"), {'gid': group_id})
            for co_id in content_ids:
                await self.db.execute(text('''
                    INSERT INTO collection_group_items (group_id, content_id)
                    VALUES (:gid, :coid)
                    ON CONFLICT DO NOTHING
                '''), {'gid': group_id, 'coid': co_id})

        await self.db.commit()
        return True

    async def delete_collection_group(self, user_id: UUID, group_id: UUID) -> bool:
        await self.db.execute(text("DELETE FROM collection_groups WHERE id = :gid AND user_id = :uid"), {'gid': group_id, 'uid': user_id})
        await self.db.commit()
        return True
