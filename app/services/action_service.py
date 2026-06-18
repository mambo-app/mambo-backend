from app.core.logger import get_logger
from uuid import UUID
from typing import Optional
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from app.models.action import ActionType, ContentActionRequest, ContentActionResponse
from datetime import date, datetime
from fastapi import HTTPException
import uuid
import math
import json
logger = get_logger('mambo.action')

class ActionService:
    def __init__(self, db: AsyncSession):
        self.db = db

    async def handle_action(self, user_id: UUID, content_id: UUID, req: ContentActionRequest) -> ContentActionResponse:
        try:
            # 0. Fetch content details to check release date
            content_res = await self.db.execute(text(
                "SELECT title, release_date FROM content WHERE id = :id"
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
            elif req.action == ActionType.unrate:
                await self._handle_unrate(user_id, content_id)
            elif req.action == ActionType.notify:
                await self.db.execute(text('''
                    INSERT INTO calendar_alerts (user_id, content_id)
                    VALUES (:uid, :cid) ON CONFLICT DO NOTHING
                '''), {'uid': user_id, 'cid': content_id})

                # Create an actual notification so user sees it in their tray
                from app.services.notification_service import NotificationService
                ns = NotificationService(self.db)
                await ns.create_notification({
                    'user_id': user_id,
                    'type': 'calendar_alert',
                    'title': content['title'],
                    'message': "is now tracked for release alerts.",
                    'related_id': content_id
                })
            elif req.action == ActionType.unnotify:
                await self.db.execute(text('''
                    DELETE FROM calendar_alerts WHERE user_id = :uid AND content_id = :cid
                '''), {'uid': user_id, 'cid': content_id})
            elif req.action == ActionType.untrack:
                # FULL RESET: Clear status, collections, and history
                await self._revert_watch(user_id, content_id)
                await self._remove_activity(user_id, ['watched', 'rewatched', 'dropped', 'interested'], content_id=content_id)

            elif req.action == ActionType.set_status:
                if not req.status:
                    raise HTTPException(status_code=400, detail="Status value required for 'set_status' action")
                
                if req.status == 'none':
                    # Perform full reset
                    await self._revert_watch(user_id, content_id)
                    await self._remove_activity(user_id, ['watched', 'rewatched', 'dropped', 'interested'], content_id=content_id)
                else:
                    # 1. Update primary status
                    await self.db.execute(text('''
                        INSERT INTO user_content_status (user_id, content_id, status, last_activity_at, updated_at)
                        VALUES (:user_id, :content_id, :status, now(), now())
                        ON CONFLICT (user_id, content_id) DO UPDATE SET
                            status = EXCLUDED.status,
                            last_activity_at = now(),
                            updated_at = now()
                    '''), {'user_id': user_id, 'content_id': content_id, 'status': req.status})

                # 2. Sync legacy flags for backward compatibility
                if req.status == 'completed':
                    await self._update_status_flag(user_id, content_id, 'is_watched', True)
                    await self._update_status_flag(user_id, content_id, 'is_dropped', False)
                    await self._sync_to_collection(user_id, content_id, 'Watched')
                elif req.status == 'dropped':
                    await self._update_status_flag(user_id, content_id, 'is_dropped', True)
                    await self._update_status_flag(user_id, content_id, 'is_watched', False)
                    await self._sync_to_collection(user_id, content_id, 'Dropped')
                elif req.status == 'plan_to_watch':
                    await self._update_status_flag(user_id, content_id, 'is_interested', True)
                    await self._sync_to_collection(user_id, content_id, 'Watchlist')
                
                # 3. Log activity based on status
                activity_map = {
                    'completed': 'watched',
                    'dropped': 'dropped',
                    'plan_to_watch': 'interested',
                    'watching': 'watched'
                }
                act_type = activity_map.get(req.status, 'watched')
                await self._log_activity(user_id, act_type, content_id=content_id, details={'status': req.status})
            
            elif req.action == ActionType.watch_episode:
                if req.season_number is None or req.episode_number is None:
                    raise HTTPException(status_code=400, detail="Season and Episode numbers required")
                
                # 1. Log episode watch
                await self.db.execute(text('''
                    INSERT INTO episode_watch_history (user_id, content_id, season_number, episode_number)
                    VALUES (:uid, :cid, :sn, :en)
                    ON CONFLICT (user_id, content_id, season_number, episode_number) DO UPDATE SET
                        watched_at = now()
                '''), {'uid': user_id, 'cid': content_id, 'sn': req.season_number, 'en': req.episode_number})

                # 2. Update progress count (MAX of current progress or this episode)
                # Season 0 Rule: Ignore Season 0 (Specials) for main progression
                if req.season_number != 0:
                    logger.info(f"Saving watch_episode for user {user_id}, content {content_id}: S{req.season_number} E{req.episode_number}")
                    # 1. Update overall status
                    logger.info(f"Saving watch_episode for user {user_id}, content {content_id}: S{req.season_number} E{req.episode_number}")
                    await self.db.execute(text('''
                        INSERT INTO user_content_status (user_id, content_id, progress_episodes, last_watched_season, last_watched_episode, status, last_activity_at, updated_at)
                        VALUES (:uid, :cid, :en, :sn, :en, 'watching', now(), now())
                        ON CONFLICT (user_id, content_id) DO UPDATE SET
                            progress_episodes = GREATEST(user_content_status.progress_episodes, :en),
                            last_watched_season = :sn,
                            last_watched_episode = :en,
                            status = CASE 
                                        WHEN user_content_status.status IN ('none', 'plan_to_watch') THEN 'watching'
                                        ELSE user_content_status.status
                                     END,
                            last_activity_at = now(),
                            updated_at = now()
                    '''), {'uid': user_id, 'cid': content_id, 'sn': req.season_number, 'en': req.episode_number})
                    
                    # Update PER-SEASON status (for Liquid Tracker)
                    await self.db.execute(text('''
                        INSERT INTO user_season_status (user_id, content_id, season_number, progress_episodes, status, updated_at)
                        VALUES (:uid, :cid, :sn, :en, 'watching', now())
                        ON CONFLICT (user_id, content_id, season_number) DO UPDATE SET
                            progress_episodes = GREATEST(user_season_status.progress_episodes, :en),
                            status = CASE 
                                        WHEN user_season_status.status = 'completed' THEN 'completed'
                                        ELSE 'watching'
                                     END,
                            updated_at = now()
                    '''), {'uid': user_id, 'cid': content_id, 'sn': req.season_number, 'en': req.episode_number})

                    # 3. Mark previous seasons as 'completed' if they aren't already
                    if req.season_number > 1:
                        await self.db.execute(text('''
                             INSERT INTO user_season_status (user_id, content_id, season_number, status, updated_at)
                             SELECT :uid, :cid, s.sn, 'completed', now()
                             FROM (SELECT generate_series(1, :sn - 1) as sn) s
                             ON CONFLICT (user_id, content_id, season_number) DO UPDATE SET
                                 status = 'completed',
                                 updated_at = now()
                             WHERE user_season_status.status != 'completed'
                         '''), {'uid': user_id, 'cid': content_id, 'sn': req.season_number})
                    
                    # 4. Trigger progression recalculation (Auto-completes if needed)
                    await self._recalculate_series_progression(user_id, content_id)
                else:
                    # For Season 0, we still ensure status is 'watching' if it was 'none'
                    await self.db.execute(text('''
                        INSERT INTO user_content_status (user_id, content_id, status, last_activity_at, updated_at)
                        VALUES (:uid, :cid, 'watching', now(), now())
                        ON CONFLICT (user_id, content_id) DO UPDATE SET
                            status = CASE 
                                        WHEN user_content_status.status IN ('none', 'plan_to_watch') THEN 'watching'
                                        ELSE user_content_status.status
                                     END,
                            last_activity_at = now(),
                            updated_at = now()
                    '''), {'uid': user_id, 'cid': content_id})

                await self._log_activity(user_id, 'watched', content_id=content_id, 
                                        details={'season': req.season_number, 'episode': req.episode_number})
                
                # Update Streak
                await self._update_streak(user_id)

            elif req.action == ActionType.increment_progress:
                # Increment current progress by 1
                # We join with content to check total_episodes for auto-completion
                res = await self.db.execute(text('''
                    WITH updated AS (
                        UPDATE user_content_status
                        SET 
                            progress_episodes = user_content_status.progress_episodes + 1,
                            last_watched_episode = user_content_status.last_watched_episode + 1,
                            last_activity_at = now(),
                            updated_at = now()
                        WHERE user_id = :uid AND content_id = :cid
                        RETURNING progress_episodes, content_id
                    )
                    SELECT u.progress_episodes, c.total_episodes
                    FROM updated u
                    JOIN content c ON c.id = u.content_id
                '''), {'uid': user_id, 'cid': content_id})
                
                row = res.mappings().one_or_none()
                if not row:
                    # If no row exists yet, create one
                    res = await self.db.execute(text('''
                        INSERT INTO user_content_status (user_id, content_id, progress_episodes, last_watched_season, last_watched_episode, status, last_activity_at, updated_at)
                        VALUES (:uid, :cid, 1, 1, 1, 'watching', now(), now())
                        ON CONFLICT (user_id, content_id) DO UPDATE SET
                            progress_episodes = user_content_status.progress_episodes + 1,
                            last_watched_episode = user_content_status.last_watched_episode + 1,
                            last_activity_at = now(),
                            updated_at = now()
                        RETURNING progress_episodes
                    '''), {'uid': user_id, 'cid': content_id})
                    new_progress = res.scalar()
                    total_ep = 0 # unknown here, but will be caught in subsequent updates
                else:
                    new_progress = row['progress_episodes']
                    total_ep = row['total_episodes'] or 0

                # Auto-complete if reached total
                if total_ep > 0 and new_progress >= total_ep:
                    await self.db.execute(text('''
                        UPDATE user_content_status SET status = 'completed'
                        WHERE user_id = :uid AND content_id = :cid
                    '''), {'uid': user_id, 'cid': content_id})
                    act_type = 'watched' # Log as full watch
                else:
                    # Ensure status is 'watching'
                    await self.db.execute(text('''
                        UPDATE user_content_status SET status = 'watching'
                        WHERE user_id = :uid AND content_id = :cid AND status != 'watching'
                    '''), {'uid': user_id, 'cid': content_id})
                    act_type = 'watched' # episode watched

                # Update Streak
                await self._update_streak(user_id)
                
                # Recalculate series progression
                await self._recalculate_series_progression(user_id, content_id)

            elif req.action == ActionType.complete_season:
                if req.season_number is None:
                    raise HTTPException(status_code=400, detail="Season number required")
                
                # We assume the UI sends the last episode number of the season if available
                # or we just update the season marker
                last_ep = req.episode_number or 1 # fallback
                
                await self.db.execute(text('''
                    INSERT INTO user_content_status (user_id, content_id, last_watched_season, last_watched_episode, status, last_activity_at, updated_at)
                    VALUES (:uid, :cid, :sn, :en, 'watching', now(), now())
                    ON CONFLICT (user_id, content_id) DO UPDATE SET
                        last_watched_season = GREATEST(user_content_status.last_watched_season, :sn),
                        last_watched_episode = CASE 
                                                WHEN EXCLUDED.last_watched_season > user_content_status.last_watched_season THEN :en
                                                ELSE GREATEST(user_content_status.last_watched_episode, :en)
                                               END,
                        updated_at = now()
                '''), {'uid': user_id, 'cid': content_id, 'sn': req.season_number, 'en': last_ep})
                
                # Update PER-SEASON status to COMPLETED
                await self.db.execute(text('''
                    INSERT INTO user_season_status (user_id, content_id, season_number, progress_episodes, status, updated_at)
                    VALUES (:uid, :cid, :sn, :en, 'completed', now())
                    ON CONFLICT (user_id, content_id, season_number) DO UPDATE SET
                        progress_episodes = GREATEST(user_season_status.progress_episodes, :en),
                        status = 'completed',
                        updated_at = now()
                '''), {'uid': user_id, 'cid': content_id, 'sn': req.season_number, 'en': last_ep})
                
                await self._log_activity(user_id, 'watched', content_id=content_id, 
                                        details={'season': req.season_number, 'action': 'season_completed'})
                
                # Recalculate series progression
                await self._recalculate_series_progression(user_id, content_id)

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
        # Auto-clear conflicting flags to satisfy DB check constraints
        extra_sets = ""
        if flag_value:
            if flag_name == 'is_watched':
                extra_sets = ", is_dropped = false"
            elif flag_name == 'is_dropped':
                extra_sets = ", is_watched = false"
            elif flag_name == 'is_interested':
                pass # No conflicting flag for interest currently exists in schema

        stmt = text(f'''
            INSERT INTO user_content_status (user_id, content_id, {flag_name})
            VALUES (:user_id, :content_id, :flag_value)
            ON CONFLICT (user_id, content_id) DO UPDATE SET
                {flag_name} = :flag_value,
                updated_at = now()
                {extra_sets}
        ''')
        await self.db.execute(stmt, {
            'user_id': user_id,
            'content_id': content_id,
            'flag_value': flag_value
        })

    async def get_content_rating_history(self, content_id: UUID, viewer_id: Optional[UUID] = None, tab: str = 'all', limit: int = 50, offset: int = 0):
        """
        Fetches community rating history for a content item.
        Tabs: 'all', 'friends', 'you'
        """
        params = {"cid": content_id, "limit": limit, "offset": offset}
        
        base_query = '''
            SELECT 
                wh.id, wh.rating, wh.watched_at, wh.watch_type,
                p.id as user_id, p.username, p.display_name, p.avatar_url, p.is_verified
            FROM watch_history wh
            JOIN profiles p ON wh.user_id = p.id
            WHERE wh.content_id = :cid AND wh.rating IS NOT NULL
        '''

        if tab == 'you' and viewer_id:
            base_query += " AND wh.user_id = :vid"
            params["vid"] = viewer_id
        elif tab == 'friends' and viewer_id:
            base_query += '''
                AND wh.user_id IN (
                    SELECT receiver_id FROM friend_requests WHERE sender_id = :vid AND status = 'accepted'
                    UNION
                    SELECT sender_id FROM friend_requests WHERE receiver_id = :vid AND status = 'accepted'
                )
            '''
            params["vid"] = viewer_id

        base_query += " ORDER BY wh.watched_at DESC LIMIT :limit OFFSET :offset"
        
        res = await self.db.execute(text(base_query), params)
        return res.mappings().all()

    async def get_user_watch_history(self, user_id: UUID, limit: int = 50, offset: int = 0):
        """
        Fetches a user's lifelong watch and rating history.
        """
        query = '''
            SELECT 
                wh.id, wh.rating, wh.watched_at, wh.watch_type,
                c.id as content_id, c.title, c.poster_url, c.content_type
            FROM watch_history wh
            JOIN content c ON wh.content_id = c.id
            WHERE wh.user_id = :uid
            ORDER BY wh.watched_at DESC
            LIMIT :limit OFFSET :offset
        '''
        res = await self.db.execute(text(query), {"uid": user_id, "limit": limit, "offset": offset})
        return res.mappings().all()

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
            INSERT INTO user_content_status (user_id, content_id, is_watched, is_dropped, status, watch_count, first_watched_at, last_watched_at)
            VALUES (:user_id, :content_id, true, false, 'completed', :init_count, now(), now())
            ON CONFLICT (user_id, content_id) DO UPDATE SET
                is_watched = true,
                is_dropped = false,
                status = 'completed',
                watch_count = CASE 
                                WHEN user_content_status.is_watched = false THEN :init_count 
                                ELSE user_content_status.watch_count + 1 
                              END,
                last_watched_at = now(),
                updated_at = now()
        ''')
        await self.db.execute(stmt_status, {'user_id': user_id, 'content_id': content_id, 'init_count': init_count})

        # 3. Add to history (ALWAYS INSERT NEW)
        watch_type = 'first_watch' if old_count == 0 else 'rewatch'
        stmt_history = text('''
            INSERT INTO watch_history (id, user_id, content_id, watch_type, watched_at)
            VALUES (:id, :user_id, :content_id, :watch_type, now())
            RETURNING id
        ''')
        watch_id = (await self.db.execute(stmt_history, {
            'id': uuid.uuid4(),
            'user_id': user_id, 
            'content_id': content_id, 
            'watch_type': watch_type
        })).scalar()

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

        # 6. Update Streak
        await self._update_streak(user_id)

    async def _handle_rate(self, user_id: UUID, content_id: UUID, rating: float):
        # 1. Update status flag and rating (Latest Status)
        stmt = text('''
            INSERT INTO user_content_status (user_id, content_id, rating, updated_at)
            VALUES (:user_id, :content_id, :rating, now())
            ON CONFLICT (user_id, content_id) DO UPDATE SET
                rating = :rating,
                updated_at = now()
        ''')
        await self.db.execute(stmt, {'user_id': user_id, 'content_id': content_id, 'rating': rating})
        
        # 2. Update the MOST RECENT watch history entry with this rating
        # If no entry exists (user rated without watching), CREATE ONE with type 'rating_only'
        res = await self.db.execute(text('''
            UPDATE watch_history 
            SET rating = :rating 
            WHERE id = (
                SELECT id FROM watch_history 
                WHERE user_id = :uid AND content_id = :cid 
                ORDER BY watched_at DESC LIMIT 1
            )
            RETURNING id
        '''), {'uid': user_id, 'cid': content_id, 'rating': rating})
        
        updated_row = res.mappings().one_or_none()
        
        if not updated_row:
            # Create a "rating only" history entry so it shows in the Rating History screen
            import uuid
            await self.db.execute(text('''
                INSERT INTO watch_history (id, user_id, content_id, rating, watch_type, watched_at)
                VALUES (:id, :user_id, :content_id, :rating, 'rating_only', now())
            '''), {
                'id': uuid.uuid4(),
                'user_id': user_id,
                'content_id': content_id,
                'rating': rating
            })

        # 3. Log activity
        # Check if this content was already watched (to decide if rewatch badge)
        status_res = await self.db.execute(text(
            "SELECT watch_count FROM user_content_status WHERE user_id = :uid AND content_id = :cid"
        ), {"uid": user_id, "cid": content_id})
        row_dict = status_res.mappings().one_or_none() or {}
        old_count = row_dict.get('watch_count') or 0
        is_rewatch = old_count > 1

        await self._log_activity(
            user_id=user_id, 
            activity_type='rated', 
            content_id=content_id,
            details={'rating': rating},
            is_rewatch=is_rewatch
        )
        
        # 4. Invalidate cache
        from app.services.user_service import UserService
        u_svc = UserService(self.db)
        await u_svc.invalidate_profile_cache(str(user_id))

    async def _handle_unrate(self, user_id: UUID, content_id: UUID):
        # 1. Clear rating from user_content_status
        await self.db.execute(text('''
            UPDATE user_content_status
            SET rating = NULL, updated_at = now()
            WHERE user_id = :uid AND content_id = :cid
        '''), {'uid': user_id, 'cid': content_id})
        
        # 2. Clear rating from watch history
        await self.db.execute(text('''
            UPDATE watch_history 
            SET rating = NULL 
            WHERE user_id = :uid AND content_id = :cid
        '''), {'uid': user_id, 'cid': content_id})
        
        # Remove rating-only entries
        await self.db.execute(text('''
            DELETE FROM watch_history
            WHERE user_id = :uid AND content_id = :cid AND watch_type = 'rating_only'
        '''), {'uid': user_id, 'cid': content_id})
        
        # 3. Remove rating activity from activity_log
        await self._remove_activity(user_id, 'rated', content_id=content_id)
        
        # 4. Invalidate cache
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
                          visibility: str = 'public', is_rewatch: bool = False):
        import json
        stmt = text('''
            INSERT INTO activity_log (id, user_id, activity_type, content_id, review_id, post_id, 
                                   collection_id, news_id, related_user_id, details, visibility, is_rewatch)
            VALUES (:id, :user_id, :activity_type, :content_id, :review_id, :post_id, 
                    :collection_id, :news_id, :related_user_id, :details, :visibility, :is_rewatch)
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
            'visibility': visibility,
            'is_rewatch': is_rewatch
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
                status = 'none',
                progress_episodes = 0,
                last_watched_season = 0,
                last_watched_episode = 0,
                updated_at = now()
            WHERE user_id = :uid AND content_id = :cid
        '''), {'uid': user_id, 'cid': content_id})
        
        # 2b. Clear season and episode progress
        await self.db.execute(text('''
            DELETE FROM user_season_status WHERE user_id = :uid AND content_id = :cid
        '''), {'uid': user_id, 'cid': content_id})
        
        await self.db.execute(text('''
            DELETE FROM episode_watch_history WHERE user_id = :uid AND content_id = :cid
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

    async def _check_badges(self, user_id: UUID):
        """Evaluates user milestones and awards badges."""
        import json
        # 1. Fetch user stats
        res = await self.db.execute(text('''
            SELECT total_watched, badges FROM user_stats WHERE user_id = :uid
        '''), {'uid': user_id})
        row = res.mappings().one_or_none()
        if not row: return
        
        current_badges = row['badges'] or []
        if isinstance(current_badges, str):
            current_badges = json.loads(current_badges)
            
        new_badges = list(current_badges)
        earned_new = False
        
        # BADGE: Binge Monster (Watch 5 episodes in a day)
        if "Binge Monster" not in [b.get('name') for b in new_badges]:
            ep_res = await self.db.execute(text('''
                SELECT count(*) FROM episode_watch_history 
                WHERE user_id = :uid AND watched_at > now() - interval '24 hours'
            '''), {'uid': user_id})
            if ep_res.scalar() >= 5:
                new_badges.append({
                    "name": "Binge Monster",
                    "description": "Watched 5+ episodes in 24 hours!",
                    "earned_at": datetime.now().isoformat()
                })
                earned_new = True

        # BADGE: Anime Addict (Complete 10 anime)
        if "Anime Addict" not in [b.get('name') for b in new_badges]:
            anime_res = await self.db.execute(text('''
                SELECT count(*) FROM user_content_status ucs
                JOIN content c ON c.id = ucs.content_id
                WHERE ucs.user_id = :uid AND ucs.status = 'completed' AND c.content_type = 'anime'
            '''), {'uid': user_id})
            if anime_res.scalar() >= 10:
                new_badges.append({
                    "name": "Anime Addict",
                    "description": "Completed 10+ Anime series!",
                    "earned_at": datetime.now().isoformat()
                })
                earned_new = True

        if earned_new:
            await self.db.execute(text('''
                UPDATE user_stats SET badges = :badges WHERE user_id = :uid
            '''), {'badges': json.dumps(new_badges), 'uid': user_id})

    async def _update_streak(self, user_id: UUID):
        """Updates the user's daily watching streak with a 12-hour grace period."""
        from uuid import UUID
        from datetime import datetime, timezone, date, timedelta
        
        # 1. Fetch current streak info
        res = await self.db.execute(text('''
            SELECT current_streak, max_streak, last_streak_at 
            FROM user_stats WHERE user_id = :uid
        '''), {'uid': user_id})
        stats = res.mappings().one_or_none()
        
        if not stats:
            # Initialize stats if not present
            await self.db.execute(text('''
                INSERT INTO user_stats (user_id, current_streak, max_streak, last_streak_at)
                VALUES (:uid, 1, 1, now())
            '''), {'uid': user_id})
            return

        curr = stats['current_streak'] or 0
        max_s = stats['max_streak'] or 0
        last = stats['last_streak_at']
        
        now_dt = datetime.now(timezone.utc)
        today = now_dt.date()
        
        if last:
            if last.tzinfo is None:
                last = last.replace(tzinfo=timezone.utc)
            last_utc = last.astimezone(timezone.utc)
            last_date = last_utc.date()
            
            if today == last_date:
                return # Already updated today
            elif today == last_date + timedelta(days=1):
                # Streak continued next day!
                curr += 1
            elif today == last_date + timedelta(days=2):
                # Day after tomorrow: check 12-hour grace period (before noon UTC)
                deadline = datetime(now_dt.year, now_dt.month, now_dt.day, 12, 0, 0, tzinfo=timezone.utc)
                if now_dt <= deadline:
                    # Saved within grace period!
                    curr += 1
                else:
                    # Streak broken
                    curr = 1
            else:
                # Streak broken
                curr = 1
        else:
            curr = 1
            
        new_max = max(max_s, curr)
        
        # 2. Update DB
        await self.db.execute(text('''
            UPDATE user_stats SET 
                current_streak = :curr, 
                max_streak = :max_s, 
                last_streak_at = now(),
                updated_at = now()
            WHERE user_id = :uid
        '''), {'curr': curr, 'max_s': new_max, 'uid': user_id})

        # 3. Check for badges
        await self._check_badges(user_id)

    async def _recalculate_series_progression(self, user_id: UUID, content_id: UUID):
        """
        Check if all seasons are completed and update main series status.
        Also handles auto-completion of seasons if progress matches total.
        """
        try:
            # 1. Fetch seasons metadata and user's per-season progress
            res = await self.db.execute(text('''
                SELECT c.total_seasons, c.total_episodes, c.seasons,
                       jsonb_agg(jsonb_build_object(
                           'season_number', uss.season_number,
                           'progress_episodes', uss.progress_episodes,
                           'status', uss.status,
                           'total_episodes', uss.total_episodes
                       )) as user_statuses
                FROM content c
                LEFT JOIN user_season_status uss ON uss.content_id = c.id AND uss.user_id = :uid
                WHERE c.id = :cid
                GROUP BY c.id
            '''), {'uid': user_id, 'cid': content_id})
            
            row = res.mappings().one_or_none()
            if not row: return

            total_seasons = row['total_seasons'] or 1
            seasons_meta = row['seasons'] or [] 
            user_statuses = row['user_statuses'] or []
            
            # Map metadata for quick lookup
            meta_map = {s['season_number']: s['episode_count'] for s in seasons_meta if isinstance(s, dict)}
            user_map = {s['season_number']: s for s in user_statuses if s and s.get('season_number') is not None}
            
            has_tracked_seasons = False
            all_tracked_completed = True
            max_tracked_season = 0
            max_watched_episode = 0
            
            for sn in range(1, total_seasons + 1):
                s_meta_total = meta_map.get(sn)
                u_status = user_map.get(sn)
                
                s_progress = u_status['progress_episodes'] if u_status else 0
                s_status = u_status['status'] if u_status else 'none'
                s_total = u_status['total_episodes'] if (u_status and u_status.get('total_episodes')) else s_meta_total
                
                # Fallback to ceil(total/seasons) if no metadata
                if s_total is None:
                    s_total = math.ceil(row['total_episodes'] / total_seasons) if total_seasons > 0 else 0
                
                # Auto-complete season if progress >= total
                if s_total > 0 and s_progress >= s_total and s_status != 'completed':
                    await self.db.execute(text('''
                        UPDATE user_season_status SET status = 'completed', updated_at = now()
                        WHERE user_id = :uid AND content_id = :cid AND season_number = :sn
                    '''), {'uid': user_id, 'cid': content_id, 'sn': sn})
                    s_status = 'completed'

                # If this season has been tracked by the user
                if s_status not in ('none', None) or s_progress > 0:
                    has_tracked_seasons = True
                    max_tracked_season = max(max_tracked_season, sn)
                    if s_status != 'completed':
                        all_tracked_completed = False
                
                if s_progress > 0:
                    if sn == max_tracked_season:
                        max_watched_episode = max(max_watched_episode, s_progress)

            # 2. Update main status
            if has_tracked_seasons:
                if all_tracked_completed:
                    # Check if it was ALREADY completed to avoid duplicate activity logs
                    status_check = await self.db.execute(text(
                        "SELECT status FROM user_content_status WHERE user_id = :uid AND content_id = :cid"
                    ), {'uid': user_id, 'cid': content_id})
                    old_status = status_check.scalar()

                    await self.db.execute(text('''
                        UPDATE user_content_status 
                        SET status = 'completed', is_watched = true, is_dropped = false, updated_at = now()
                        WHERE user_id = :uid AND content_id = :cid
                    '''), {'uid': user_id, 'cid': content_id})
                    
                    # Log activity if it JUST became completed
                    if old_status != 'completed':
                        await self._log_activity(user_id, 'watched', content_id=content_id, details={'status': 'completed', 'trigger': 'auto_recalculation'})
                    
                    # Sync to collection
                    await self._sync_to_collection(user_id, content_id, 'Watched')
                else:
                     # Only update to 'watching' if not already 'dropped'
                     await self.db.execute(text('''
                        UPDATE user_content_status 
                        SET status = 'watching', updated_at = now()
                        WHERE user_id = :uid AND content_id = :cid AND status != 'dropped'
                    '''), {'uid': user_id, 'cid': content_id})
        except Exception as e:
            logger.error(f"Error recalculating progression for {content_id}: {e}")
