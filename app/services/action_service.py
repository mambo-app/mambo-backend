from app.core.logging import get_logger
from uuid import UUID
from typing import Optional
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from app.models.action import ActionType, ContentActionRequest, ContentActionResponse
from datetime import date
from fastapi import HTTPException

import uuid
logger = get_logger('mambo.action')

class ActionService:
    def __init__(self, db: AsyncSession):
        self.db = db

    async def handle_action(self, user_id: UUID, content_id: UUID, req: ContentActionRequest) -> ContentActionResponse:
        try:
            # 0. Fetch content details to check release date
            content_res = await self.db.execute(text(
                "SELECT release_date FROM content WHERE id = :id"
            ), {"id": content_id})
            content = content_res.mappings().one_or_none()
            
            if not content:
                raise HTTPException(status_code=404, detail="Content not found")
            
            release_date = content.get('release_date')
            is_future = bool(release_date and release_date > date.today())

            # 1. Block actions if content is not yet released
            restricted_actions = [
                ActionType.watch, ActionType.rewatch, ActionType.drop, 
                ActionType.rate, ActionType.review
            ]
            if is_future and req.action in restricted_actions:
                raise HTTPException(
                    status_code=400, 
                    detail=f"Action '{req.action}' is not available for unreleased content. Try marking as 'Interested' instead."
                )

            # 2. Update content to permanent
            await self._make_content_permanent(content_id)

            # 3. Handle specific action logic
            # Fetch current status to determine if we toggle
            status_res = await self.db.execute(text(
                "SELECT is_watched, is_liked, is_dropped, is_interested FROM user_content_status WHERE user_id = :uid AND content_id = :cid"
            ), {"uid": user_id, "cid": content_id})
            current_status = status_res.mappings().one_or_none() or {}

            if req.action == ActionType.watch:
                if current_status.get('is_watched'):
                    # REVERT WATCH (Toggle OFF)
                    # We set is_watched to false to clear the visual state immediately
                    await self._revert_watch(user_id, content_id)
                    await self._remove_activity(user_id, ['watched', 'rewatched'], content_id=content_id)
                    await self._remove_from_collection(user_id, content_id, 'Watched')
                else:
                    await self._handle_watch(user_id, content_id, req.action)
                    await self._sync_to_collection(user_id, content_id, 'Watched')
            elif req.action == ActionType.rewatch:
                await self._handle_watch(user_id, content_id, req.action)
                await self._sync_to_collection(user_id, content_id, 'Watched')  # idempotent, ON CONFLICT DO NOTHING
            elif req.action == ActionType.drop:
                if current_status.get('is_dropped'):
                    # REVERT DROP
                    await self._update_status_flag(user_id, content_id, 'is_dropped', False)
                    await self._remove_activity(user_id, 'dropped', content_id=content_id)
                    await self._remove_from_collection(user_id, content_id, 'Dropped')
                else:
                    await self._update_status_flag(user_id, content_id, 'is_dropped', True)
                    await self._log_activity(user_id, 'dropped', content_id=content_id)
                    await self._sync_to_collection(user_id, content_id, 'Dropped')
            elif req.action == ActionType.like:
                if current_status.get('is_liked'):
                    # REVERT LIKE
                    await self._update_status_flag(user_id, content_id, 'is_liked', False)
                    await self._remove_activity(user_id, 'liked_content', content_id=content_id)
                else:
                    await self._update_status_flag(user_id, content_id, 'is_liked', True)
                    await self._log_activity(user_id, 'liked_content', content_id=content_id)
            elif req.action == ActionType.unlike:
                await self._update_status_flag(user_id, content_id, 'is_liked', False)
                await self._remove_activity(user_id, 'liked_content', content_id=content_id)
            elif req.action == ActionType.save:
                if current_status.get('is_interested'):
                    # REVERT SAVE
                    await self._update_status_flag(user_id, content_id, 'is_interested', False)
                    await self._remove_activity(user_id, ['interested', 'saved'], content_id=content_id)
                    await self._remove_from_collection(user_id, content_id, 'Watchlist')
                else:
                    await self._update_status_flag(user_id, content_id, 'is_interested', True)
                    await self._log_activity(user_id, 'interested', content_id=content_id)
                    await self._sync_to_collection(user_id, content_id, 'Watchlist')
            elif req.action == ActionType.unsave:
                await self._update_status_flag(user_id, content_id, 'is_interested', False)
                await self._remove_activity(user_id, ['interested', 'saved'], content_id=content_id)
                await self._remove_from_collection(user_id, content_id, 'Watchlist')
            elif req.action == ActionType.recommend:
                # Recommendations are NOT irreversible as requested
                pass
            elif req.action == ActionType.rate:
                if req.rating is None:
                    raise HTTPException(status_code=400, detail="Rating value required for 'rate' action")
                await self._handle_rate(user_id, content_id, req.rating)

            await self.db.commit()
            return ContentActionResponse(
                status="success",
                action=req.action,
                content_id=content_id,
                is_permanent=True
            )
        except HTTPException:
            await self.db.rollback()
            raise
        except Exception as e:
            await self.db.rollback()
            logger.error(f"Error handling action {req.action} for {content_id}: {e}")
            raise HTTPException(status_code=500, detail=f"Internal server error: {e}")

    async def _make_content_permanent(self, content_id: UUID):
        await self.db.execute(text('''
            UPDATE content 
            SET is_permanent = true, made_permanent_at = COALESCE(made_permanent_at, now())
            WHERE id = :content_id
        '''), {'content_id': content_id})

    async def _update_status_flag(self, user_id: UUID, content_id: UUID, flag_name: str, flag_value: bool):
        stmt = text(f'''
            INSERT INTO user_content_status (user_id, content_id, {flag_name})
            VALUES (:user_id, :content_id, :flag_value)
            ON CONFLICT (user_id, content_id) DO UPDATE SET
                {flag_name} = :flag_value,
                updated_at = now()
        ''')
        await self.db.execute(stmt, {
            'user_id': user_id,
            'content_id': content_id,
            'flag_value': flag_value
        })

    async def _handle_watch(self, user_id: UUID, content_id: UUID, action: ActionType):
        # 1. Fetch current watch count to decide activity type
        status_res = await self.db.execute(text(
            "SELECT watch_count, is_watched FROM user_content_status WHERE user_id = :uid AND content_id = :cid"
        ), {"uid": user_id, "cid": content_id})
        current_status = status_res.mappings().one_or_none() or {}
        old_count = current_status.get('watch_count', 0)
        was_watched = current_status.get('is_watched', False)

        init_count = 2 if action == ActionType.rewatch and old_count == 0 else 1

        # 2. Update status
        stmt_status = text('''
            INSERT INTO user_content_status (user_id, content_id, is_watched, watch_count, first_watched_at, last_watched_at)
            VALUES (:user_id, :content_id, true, :init_count, now(), now())
            ON CONFLICT (user_id, content_id) DO UPDATE SET
                is_watched = true,
                watch_count = CASE 
                                WHEN user_content_status.is_watched = false THEN :init_count 
                                ELSE user_content_status.watch_count + 1 
                              END,
                last_watched_at = now(),
                updated_at = now()
        ''')
        await self.db.execute(stmt_status, {'user_id': user_id, 'content_id': content_id, 'init_count': init_count})

        # 3. Add to history
        watch_type = 'first_watch' if old_count == 0 else 'rewatch'
        stmt_history = text('''
            INSERT INTO watch_history (user_id, content_id, watch_type, watched_at)
            VALUES (:user_id, :content_id, :watch_type, now())
            ON CONFLICT (user_id, content_id) DO UPDATE SET 
                watch_type = EXCLUDED.watch_type,
                watched_at = EXCLUDED.watched_at
        ''')
        await self.db.execute(stmt_history, {'user_id': user_id, 'content_id': content_id, 'watch_type': watch_type})

        # 4. Update stats
        await self.db.execute(text('''
            INSERT INTO user_stats (user_id, total_watched)
            VALUES (:user_id, 1)
            ON CONFLICT (user_id) DO UPDATE SET
                total_watched = user_stats.total_watched + 1,
                updated_at = now()
        '''), {'user_id': user_id})

        # Invalidate profile cache
        from app.services.user_service import UserService
        u_svc = UserService(self.db)
        await u_svc.invalidate_profile_cache(str(user_id))

        # 5. Log activity
        # If it was 0, it's "watched", else it's "rewatched"
        activity_type = 'watched' if old_count == 0 else 'rewatched'
        # Check if a rating was provided in the context (though usually watch and rate are separate)
        await self._log_activity(user_id, activity_type, content_id=content_id)

    async def _handle_rate(self, user_id: UUID, content_id: UUID, rating: float):
        # 1. Update status flag and rating
        stmt = text('''
            INSERT INTO user_content_status (user_id, content_id, rating, updated_at)
            VALUES (:user_id, :content_id, :rating, now())
            ON CONFLICT (user_id, content_id) DO UPDATE SET
                rating = :rating,
                updated_at = now()
        ''')
        await self.db.execute(stmt, {'user_id': user_id, 'content_id': content_id, 'rating': rating})
        
        # 2. Log activity
        await self._log_activity(
            user_id=user_id, 
            activity_type='rated', 
            content_id=content_id,
            details={'rating': rating}
        )
        
        # 3. Invalidate cache
        from app.services.user_service import UserService
        u_svc = UserService(self.db)
        await u_svc.invalidate_profile_cache(str(user_id))

    async def _sync_to_collection(self, user_id: UUID, content_id: UUID, collection_name: str):
        # 1. Find or create default collection
        stmt_find = text("SELECT id FROM collections WHERE user_id = :uid AND name = :name")
        res = await self.db.execute(stmt_find, {'uid': user_id, 'name': collection_name})
        # Use .all() and pick the first one to avoid MultipleResultsFound error if duplicates exist
        colls = res.mappings().all()
        coll = colls[0] if colls else None
        
        if not coll:
            stmt_create = text('''
                INSERT INTO collections (user_id, name, collection_type, is_default, is_deletable)
                VALUES (:uid, :name, 'system', true, false)
                RETURNING id
            ''')
            res = await self.db.execute(stmt_create, {'uid': user_id, 'name': collection_name})
            coll = res.mappings().one()
        
        collection_id = coll['id']

        # 2. Add item
        stmt_item = text('''
            INSERT INTO collection_items (collection_id, content_id, added_by)
            VALUES (:cid, :coid, :uid)
            ON CONFLICT (collection_id, content_id) DO NOTHING
        ''')
        res_item = await self.db.execute(stmt_item, {'cid': collection_id, 'coid': content_id, 'uid': user_id})
        
        if res_item.rowcount > 0:
            await self.db.execute(text('''
                UPDATE collections SET item_count = item_count + 1, updated_at = now() WHERE id = :cid
            '''), {'cid': collection_id})

    async def _log_activity(self, user_id: UUID, activity_type: str, content_id: Optional[UUID] = None, 
                          review_id: Optional[UUID] = None, post_id: Optional[UUID] = None, 
                          collection_id: Optional[UUID] = None, news_id: Optional[UUID] = None, 
                          related_user_id: Optional[UUID] = None, details: Optional[dict] = None,
                          visibility: str = 'public'):
        import json
        stmt = text('''
            INSERT INTO activity_log (id, user_id, activity_type, content_id, review_id, post_id, 
                                   collection_id, news_id, related_user_id, details, visibility)
            VALUES (:id, :user_id, :activity_type, :content_id, :review_id, :post_id, 
                    :collection_id, :news_id, :related_user_id, :details, :visibility)
        ''')
        await self.db.execute(stmt, {
            'id': uuid.uuid4(),
            'user_id': user_id,
            'activity_type': activity_type,
            'content_id': content_id,
            'review_id': review_id,
            'post_id': post_id,
            'collection_id': collection_id,
            'news_id': news_id,
            'related_user_id': related_user_id,
            'details': json.dumps(details) if details else None,
            'visibility': visibility
        })

    async def _revert_watch(self, user_id: UUID, content_id: UUID):
        # 1. Fetch current watch count to decrement stats correctly
        res = await self.db.execute(text(
            "SELECT watch_count FROM user_content_status WHERE user_id = :uid AND content_id = :cid"
        ), {'uid': user_id, 'cid': content_id})
        row = res.mappings().one_or_none()
        count_to_remove = row.get('watch_count', 0) if row else 0

        # 2. Reset status completely
        await self.db.execute(text('''
            UPDATE user_content_status 
            SET watch_count = 0,
                is_watched = false,
                updated_at = now()
            WHERE user_id = :uid AND content_id = :cid
        '''), {'uid': user_id, 'cid': content_id})
        
        # 3. Delete ALL watch history for this content
        await self.db.execute(text('''
            DELETE FROM watch_history 
            WHERE user_id = :uid AND content_id = :cid
        '''), {'uid': user_id, 'cid': content_id})

        # 4. Decrement user_stats by the FULL count
        await self.db.execute(text('''
            UPDATE user_stats 
            SET total_watched = GREATEST(0, total_watched - :count),
                updated_at = now()
            WHERE user_id = :uid
        '''), {'uid': user_id, 'count': count_to_remove})

        # Invalidate profile cache
        from app.services.user_service import UserService
        u_svc = UserService(self.db)
        await u_svc.invalidate_profile_cache(str(user_id))

    async def _remove_activity(self, user_id: UUID, activity_type: any, content_id: UUID):
        # activity_type can be a string or a list of strings
        if isinstance(activity_type, str):
            types = [activity_type]
        else:
            types = activity_type

        # We delete ALL activity logs of these types for this content (full cleanup)
        # to ensure the feed gets cleared as requested.
        await self.db.execute(text('''
            DELETE FROM activity_log 
            WHERE user_id = :uid 
              AND content_id = :cid 
              AND activity_type = ANY(:types)
        '''), {'uid': user_id, 'cid': content_id, 'types': types})

    async def _remove_from_collection(self, user_id: UUID, content_id: UUID, collection_name: str):
        # 1. Find collection
        res = await self.db.execute(text(
            "SELECT id FROM collections WHERE user_id = :uid AND name = :name"
        ), {'uid': user_id, 'name': collection_name})
        coll = res.mappings().one_or_none()
        
        if coll:
            coll_id = coll['id']
            # 2. Remove item
            delete_res = await self.db.execute(text('''
                DELETE FROM collection_items 
                WHERE collection_id = :cid AND content_id = :coid
            '''), {'cid': coll_id, 'coid': content_id})
            
            # 3. Decrement count if something was deleted
            if delete_res.rowcount > 0:
                await self.db.execute(text('''
                    UPDATE collections SET item_count = GREATEST(0, item_count - 1), updated_at = now() 
                    WHERE id = :cid
                '''), {'cid': coll_id})

