from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from typing import List, Dict, Any, Optional
from uuid import UUID
from app.core.exceptions import NotFoundError
from app.services.cache_service import cache, CacheKeys, CacheService
from app.core.supabase import supabase_admin
from fastapi import HTTPException
import logging

logger = logging.getLogger('mambo.users')

class UserService:
    def __init__(self, db: AsyncSession):
        self.db = db


    async def upload_avatar(self, user_id: str, file_data: bytes, filename: str) -> str:
        import uuid
        file_ext = filename.split('.')[-1] if '.' in filename else 'jpg'
        # Unique name to avoid caching issues
        storage_path = f"{user_id}/{uuid.uuid4()}.{file_ext}"
        
        try:
            # Upload to Supabase Storage "avatars" bucket
            supabase_admin.storage.from_("avatars").upload(
                path=storage_path,
                file=file_data,
                file_options={
                    "content-type": f"image/{file_ext}",
                    "upsert": "true"
                }
            )
            
            # Get public URL
            public_url = supabase_admin.storage.from_("avatars").get_public_url(storage_path)
            
            # Update profile in Neon
            await self.update_profile(user_id, {"avatar_url": public_url})
            
            return public_url
        except Exception as e:
            logger.error(f"Avatar upload failed for user {user_id}: {e}")
            raise HTTPException(status_code=500, detail=f"Failed to upload avatar: {str(e)}")

    async def invalidate_profile_cache(self, user_id: str):
        cache_key = CacheKeys.user_profile(user_id)
        await cache.delete(cache_key)
        logger.debug(f'Invalidated profile cache for user {user_id}')

    async def get_by_username(self, username: str, viewer_id: str | None) -> dict:
        try:
            result = await self.db.execute(text('''
                SELECT p.*,
                       us.followers_count, us.following_count, us.friends_count, us.total_posts,
                       us.total_watched, us.total_reviews,
                       COALESCE(
                           CASE 
                               WHEN us.last_streak_at IS NULL THEN 0
                               WHEN now() AT TIME ZONE 'UTC' < date_trunc('day', us.last_streak_at AT TIME ZONE 'UTC') + interval '60 hours' THEN us.current_streak
                               ELSE 0
                           END, 
                           0
                       ) AS current_streak,
                       COALESCE(
                           CASE 
                               WHEN us.last_streak_at IS NULL THEN false
                               WHEN now() AT TIME ZONE 'UTC' >= date_trunc('day', us.last_streak_at AT TIME ZONE 'UTC') + interval '48 hours'
                                AND now() AT TIME ZONE 'UTC' < date_trunc('day', us.last_streak_at AT TIME ZONE 'UTC') + interval '60 hours' THEN true
                               ELSE false
                           END, 
                           false
                       ) AS is_in_grace_period,
                       (SELECT COUNT(DISTINCT content_id) FROM public.watch_history WHERE user_id = p.id AND EXTRACT(year FROM watched_at) = EXTRACT(year FROM now())) AS watched_this_year,
                       (SELECT COUNT(*) FROM public.reviews WHERE user_id = p.id AND is_deleted = false AND EXTRACT(year FROM created_at) = EXTRACT(year FROM now())) AS reviews_this_year
                FROM profiles p
                LEFT JOIN user_stats us ON us.user_id = p.id
                WHERE p.username = :username
                AND p.is_deleted = false
            '''), {'username': username})
            profile = result.mappings().first()
        except Exception as db_err:
            try:
                await self.db.rollback()
            except Exception:
                pass
            raise db_err

        if not profile:
            raise NotFoundError('User')
        
        profile_dict = dict(profile)
        owner_id = str(profile_dict['id'])
        
        # Get social links
        links_res = await self.db.execute(text('''
            SELECT platform, url FROM social_links WHERE user_id = CAST(:id AS UUID)
        '''), {'id': owner_id})
        profile_dict['social_links'] = [dict(row) for row in links_res.mappings()]

        # Get badges
        badges_res = await self.db.execute(text('''
            SELECT b.id, b.name, b.description, b.image_url, ub.earned_at
            FROM user_badges ub
            JOIN badges b ON b.id = ub.badge_id
            WHERE ub.user_id = CAST(:id AS UUID) AND b.is_active = true
            ORDER BY ub.earned_at DESC
        '''), {'id': owner_id})
        profile_dict['badges'] = [dict(row) for row in badges_res.mappings()]

        # Get favorite actors
        actors_res = await self.db.execute(text('''
            SELECT name 
            FROM user_person_favorites
            WHERE user_id = CAST(:id AS UUID) AND is_actor = true
            ORDER BY favorite_order ASC NULLS LAST, created_at DESC
        '''), {'id': owner_id})
        profile_dict['favorite_actors'] = [row[0] for row in actors_res]

        # Get favorite directors
        directors_res = await self.db.execute(text('''
            SELECT name 
            FROM user_person_favorites
            WHERE user_id = CAST(:id AS UUID) AND is_actor = false
            ORDER BY favorite_order ASC NULLS LAST, created_at DESC
        '''), {'id': owner_id})
        profile_dict['favorite_directors'] = [row[0] for row in directors_res]

        # Get favorite genres
        genres_res = await self.db.execute(text('''
            SELECT genre_name FROM user_favorite_genres WHERE user_id = CAST(:id AS UUID)
        '''), {'id': owner_id})
        profile_dict['favorite_genres'] = [row[0] for row in genres_res]
        
        # Privacy Enforcement
        if viewer_id != owner_id:
            # Hide sensitive fields from public
            profile_dict['birthday'] = None
            profile_dict['gender'] = None
            profile_dict['email'] = None
            profile_dict['phone_number'] = None
            
        # Social Status (if viewer is present)
        profile_dict['is_following'] = False
        profile_dict['is_friend'] = False
        profile_dict['friend_request_sent_id'] = None
        profile_dict['friend_request_received_id'] = None

        if viewer_id and viewer_id != owner_id:
            from app.repositories.user_repo import UserRepository
            from app.repositories.social_repo import SocialRepository
            u_repo = UserRepository(self.db)
            s_repo = SocialRepository(self.db)
            
            profile_dict['is_following'] = await u_repo.is_following(viewer_id, owner_id)
            profile_dict['is_friend'] = await s_repo.check_is_friend(UUID(owner_id), UUID(viewer_id))
            
            # Also check if a request is pending (in either direction)
            req = await s_repo.check_request_exists(UUID(viewer_id), UUID(owner_id))
            if req and req['status'] == 'pending':
                if str(req['sender_id']) == viewer_id:
                    profile_dict['friend_request_sent_id'] = str(req['id'])
                else:
                    profile_dict['friend_request_received_id'] = str(req['id'])
                
        # Sync if owner and missing data
        if viewer_id == owner_id:
            if not profile_dict.get('email') or not profile_dict.get('phone_number'):
                synced_data = await self.sync_auth_data(owner_id)
                if synced_data:
                    profile_dict.update(synced_data)
                    # Note: get_by_username isn't currently cached like get_by_id
                
        return profile_dict

    async def update_profile(self, user_id: str, data: dict) -> dict:
        from datetime import datetime, timezone, timedelta, date
        
        # 1. Define editable fields
        allowed_fields = {'display_name', 'bio', 'gender', 'birthday', 'avatar_url', 'username'}
        updates = {k: v for k, v in data.items() if k in allowed_fields}

        if not updates:
            return await self.get_by_id(user_id)

        # 2. Normalize and Parse
        if 'gender' in updates and updates['gender']:
            updates['gender'] = updates['gender'].lower().strip()
            
        if 'birthday' in updates and updates['birthday']:
            try:
                updates['birthday'] = date.fromisoformat(updates['birthday'])
            except ValueError:
                from fastapi import HTTPException
                raise HTTPException(status_code=400, detail="Invalid birthday format. Use YYYY-MM-DD")

        # 3. Username specific logic (Constraint: 15 days, Unique)
        if 'username' in updates:
            new_username = updates['username'].strip().lower()
            current_profile = await self.get_by_id(user_id)
            
            if new_username != current_profile['username']:
                # I. Uniqueness Check
                existing = await self.db.execute(
                    text("SELECT 1 FROM profiles WHERE username = :un AND id != :uid"), 
                    {'un': new_username, 'uid': user_id}
                )
                if existing.mappings().first():
                    raise HTTPException(status_code=400, detail="This username is already taken.")

                # II. 15-Day Restriction Check
                last_update = current_profile.get('username_updated_at')
                if last_update:
                    # If coming from cache, it might be a string
                    if isinstance(last_update, str):
                        try:
                            last_update = datetime.fromisoformat(last_update.replace('Z', '+00:00'))
                        except ValueError:
                            pass # Fallback to normal check if it fails
                    
                    # Ensure last_update is timezone-aware
                    if isinstance(last_update, datetime):
                        if last_update.tzinfo is None:
                            last_update = last_update.replace(tzinfo=timezone.utc)
                        
                        if datetime.now(timezone.utc) - last_update < timedelta(days=15):
                            raise HTTPException(
                                status_code=400, 
                                detail="You can only change your username once every 15 days. Please try again later."
                            )
                
                updates['username'] = new_username
                updates['username_updated_at'] = datetime.now(timezone.utc)
            else:
                # Remove if same to avoid unnecessary update
                del updates['username']

        if not updates:
             return await self.get_by_id(user_id)

        # 4. Build query
        set_params = []
        for k in updates.keys():
            set_params.append(f"{k} = :{k}")
        
        set_clause = ", ".join(set_params)
        query = f"UPDATE profiles SET {set_clause}, updated_at = now() WHERE id = :id RETURNING *"
        params = {**updates, "id": user_id}

        try:
            result = await self.db.execute(text(query), params)
            await self.db.commit()
            updated_profile = result.mappings().first()
            if updated_profile:
                await self.invalidate_profile_cache(user_id)
            return dict(updated_profile)
        except Exception as e:
            await self.db.rollback()
            from fastapi import HTTPException
            if 'profiles_gender_check' in str(e):
                raise HTTPException(status_code=400, detail="Invalid gender choice.")
            raise HTTPException(status_code=500, detail=str(e))

    async def get_by_id(self, user_id: str) -> dict:
        # Try cache
        cache_key = CacheKeys.user_profile(user_id)
        cached = await cache.get(cache_key)
        if cached:
            return cached

        result = await self.db.execute(text('''
            SELECT p.*, 
                   us.followers_count, us.following_count, us.friends_count, us.total_posts,
                   us.total_watched, us.total_reviews,
                   COALESCE(
                       CASE 
                           WHEN us.last_streak_at IS NULL THEN 0
                           WHEN now() AT TIME ZONE 'UTC' < date_trunc('day', us.last_streak_at AT TIME ZONE 'UTC') + interval '60 hours' THEN us.current_streak
                           ELSE 0
                       END, 
                       0
                   ) AS current_streak,
                   COALESCE(
                       CASE 
                           WHEN us.last_streak_at IS NULL THEN false
                           WHEN now() AT TIME ZONE 'UTC' >= date_trunc('day', us.last_streak_at AT TIME ZONE 'UTC') + interval '48 hours'
                            AND now() AT TIME ZONE 'UTC' < date_trunc('day', us.last_streak_at AT TIME ZONE 'UTC') + interval '60 hours' THEN true
                           ELSE false
                       END, 
                       false
                   ) AS is_in_grace_period,
                   (SELECT COUNT(DISTINCT content_id) FROM public.watch_history WHERE user_id = p.id AND EXTRACT(year FROM watched_at) = EXTRACT(year FROM now())) AS watched_this_year,
                   (SELECT COUNT(*) FROM public.reviews WHERE user_id = p.id AND is_deleted = false AND EXTRACT(year FROM created_at) = EXTRACT(year FROM now())) AS reviews_this_year
            FROM profiles p
            LEFT JOIN user_stats us ON us.user_id = p.id
            WHERE p.id = CAST(:id AS UUID)
            AND p.is_deleted = false
        '''), {'id': user_id})
        profile = result.mappings().first()
        if not profile:
            raise NotFoundError('User')
        profile_dict = dict(profile)
        
        # Get social links
        links_res = await self.db.execute(text('''
            SELECT platform, url FROM social_links WHERE user_id = CAST(:id AS UUID)
        '''), {'id': user_id})
        profile_dict['social_links'] = [dict(row) for row in links_res.mappings()]

        # Get badges
        badges_res = await self.db.execute(text('''
            SELECT b.id, b.name, b.description, b.image_url, ub.earned_at
            FROM user_badges ub
            JOIN badges b ON b.id = ub.badge_id
            WHERE ub.user_id = CAST(:id AS UUID) AND b.is_active = true
            ORDER BY ub.earned_at DESC
        '''), {'id': user_id})
        profile_dict['badges'] = [dict(row) for row in badges_res.mappings()]

        # Get favorite actors
        actors_res = await self.db.execute(text('''
            SELECT name 
            FROM user_person_favorites
            WHERE user_id = CAST(:id AS UUID) AND is_actor = true
            ORDER BY favorite_order ASC NULLS LAST, created_at DESC
        '''), {'id': user_id})
        profile_dict['favorite_actors'] = [row[0] for row in actors_res]

        # Get favorite directors
        directors_res = await self.db.execute(text('''
            SELECT name 
            FROM user_person_favorites
            WHERE user_id = CAST(:id AS UUID) AND is_actor = false
            ORDER BY favorite_order ASC NULLS LAST, created_at DESC
        '''), {'id': user_id})
        profile_dict['favorite_directors'] = [row[0] for row in directors_res]

        # Get favorite genres
        genres_res = await self.db.execute(text('''
            SELECT genre_name FROM user_favorite_genres WHERE user_id = CAST(:id AS UUID)
        '''), {'id': user_id})
        profile_dict['favorite_genres'] = [row[0] for row in genres_res]

        # Cache response
        await cache.set(cache_key, profile_dict, ttl=CacheService.TTL_USER_PROFILE)

        # Trigger background sync if email/phone are missing
        if not profile_dict.get('email') or not profile_dict.get('phone_number'):
            # Fetch fresh data from Supabase and update DB
            synced_data = await self.sync_auth_data(user_id)
            if synced_data:
                profile_dict.update(synced_data)
                # Re-cache with fresh data
                await cache.set(cache_key, profile_dict, ttl=CacheService.TTL_USER_PROFILE)

        return profile_dict

    async def sync_auth_data(self, user_id: str) -> dict:
        """Fetch email and phone from Supabase Auth and update Neon profiles."""
        try:
            # supabase_admin uses synchronous auth methods usually
            # or we can check if it has async. Given AuthService usage, it's likely sync.
            res = supabase_admin.auth.admin.get_user_by_id(user_id)
            if not res or not res.user:
                return {}
            
            user = res.user
            email = user.email
            phone = user.phone
            
            # Update Neon - Only update phone if it's not null from Supabase
            # to avoid overwriting existing local data with nulls.
            if phone:
                await self.db.execute(text('''
                    UPDATE profiles 
                    SET email = :email, phone_number = :phone, updated_at = now()
                    WHERE id = CAST(:id AS UUID)
                '''), {'email': email, 'phone': phone, 'id': user_id})
            else:
                await self.db.execute(text('''
                    UPDATE profiles 
                    SET email = :email, updated_at = now()
                    WHERE id = CAST(:id AS UUID)
                '''), {'email': email, 'id': user_id})

            await self.db.commit()
            
            logger.info(f"Synced auth data for user {user_id}")
            return {'email': email, 'phone_number': phone}
        except Exception as e:
            logger.error(f"Failed to sync auth data for {user_id}: {e}")
            return {}

    async def get_activity(self, username: str, viewer_id: str | None = None) -> list[dict]:
        # 0. Fetch profile and visibility
        profile = await self.get_by_username(username, viewer_id)
        owner_id = str(profile['id'])
        
        # Privacy Check
        is_owner = viewer_id == owner_id
        visibility_setting = profile.get('activity_visibility', 'public')
        
        if not is_owner:
            if visibility_setting == 'private':
                return []
        
        # 1. Fetch recent activity directly (distinct per content)
        result = await self.db.execute(text('''
            WITH ranked_activities AS (
                SELECT DISTINCT ON (COALESCE(al.content_id, r.content_id))
                    al.activity_type, 
                    al.created_at as watched_at,
                    COALESCE(c.title, 'Deleted Content') as title, 
                    c.poster_url, 
                    COALESCE(c.content_type, 'movie') as content_type, 
                    COALESCE(CAST(c.id AS TEXT), '') as content_id,
                    COALESCE(CAST(al.review_id AS TEXT), CAST(r2.id AS TEXT)) as review_id,
                    CAST(al.post_id AS TEXT) as post_id,
                    CASE 
                        WHEN c.content_type IN ('series', 'anime', 'tv', 'tv_show') THEN
                            COALESCE(al.details, '{}'::jsonb) || jsonb_strip_nulls(jsonb_build_object(
                                'season', COALESCE(
                                    CASE WHEN al.details->>'season' ~ '^[0-9]+$' THEN (al.details->>'season')::int ELSE NULL END,
                                    CASE WHEN al.details->>'season_number' ~ '^[0-9]+$' THEN (al.details->>'season_number')::int ELSE NULL END,
                                    CASE WHEN al.details->>'seasons_watched' ~ '^[0-9]+$' THEN (al.details->>'seasons_watched')::int ELSE NULL END,
                                    ucs.last_watched_season
                                ),
                                'season_number', COALESCE(
                                    CASE WHEN al.details->>'season' ~ '^[0-9]+$' THEN (al.details->>'season')::int ELSE NULL END,
                                    CASE WHEN al.details->>'season_number' ~ '^[0-9]+$' THEN (al.details->>'season_number')::int ELSE NULL END,
                                    CASE WHEN al.details->>'seasons_watched' ~ '^[0-9]+$' THEN (al.details->>'seasons_watched')::int ELSE NULL END,
                                    ucs.last_watched_season
                                ),
                                'seasons_watched', COALESCE(
                                    CASE WHEN al.details->>'season' ~ '^[0-9]+$' THEN (al.details->>'season')::int ELSE NULL END,
                                    CASE WHEN al.details->>'season_number' ~ '^[0-9]+$' THEN (al.details->>'season_number')::int ELSE NULL END,
                                    CASE WHEN al.details->>'seasons_watched' ~ '^[0-9]+$' THEN (al.details->>'seasons_watched')::int ELSE NULL END,
                                    ucs.last_watched_season
                                ),
                                'episode', COALESCE(
                                    CASE WHEN al.details->>'episode' ~ '^[0-9]+$' THEN (al.details->>'episode')::int ELSE NULL END,
                                    CASE WHEN al.details->>'episode_number' ~ '^[0-9]+$' THEN (al.details->>'episode_number')::int ELSE NULL END,
                                    CASE WHEN al.details->>'episodes_watched' ~ '^[0-9]+$' THEN (al.details->>'episodes_watched')::int ELSE NULL END,
                                    CASE WHEN al.details->>'progress_episodes' ~ '^[0-9]+$' THEN (al.details->>'progress_episodes')::int ELSE NULL END,
                                    ucs.last_watched_episode,
                                    ucs.progress_episodes
                                ),
                                'episode_number', COALESCE(
                                    CASE WHEN al.details->>'episode' ~ '^[0-9]+$' THEN (al.details->>'episode')::int ELSE NULL END,
                                    CASE WHEN al.details->>'episode_number' ~ '^[0-9]+$' THEN (al.details->>'episode_number')::int ELSE NULL END,
                                    CASE WHEN al.details->>'episodes_watched' ~ '^[0-9]+$' THEN (al.details->>'episodes_watched')::int ELSE NULL END,
                                    CASE WHEN al.details->>'progress_episodes' ~ '^[0-9]+$' THEN (al.details->>'progress_episodes')::int ELSE NULL END,
                                    ucs.last_watched_episode,
                                    ucs.progress_episodes
                                ),
                                'episodes_watched', COALESCE(
                                    CASE WHEN al.details->>'episode' ~ '^[0-9]+$' THEN (al.details->>'episode')::int ELSE NULL END,
                                    CASE WHEN al.details->>'episode_number' ~ '^[0-9]+$' THEN (al.details->>'episode_number')::int ELSE NULL END,
                                    CASE WHEN al.details->>'episodes_watched' ~ '^[0-9]+$' THEN (al.details->>'episodes_watched')::int ELSE NULL END,
                                    CASE WHEN al.details->>'progress_episodes' ~ '^[0-9]+$' THEN (al.details->>'progress_episodes')::int ELSE NULL END,
                                    ucs.last_watched_episode,
                                    ucs.progress_episodes
                                ),
                                'start_episode', CASE WHEN al.details->>'start_episode' ~ '^[0-9]+$' THEN (al.details->>'start_episode')::int ELSE NULL END
                            ))
                        ELSE al.details
                    END as details,
                    p.username as actor_username,
                    p.display_name as actor_display_name,
                    ucs.status as user_content_status,
                    COALESCE(r.text_review, r2.text_review) as review_text,
                    COALESCE(r.rating, r.star_rating, r2.rating, r2.star_rating, CASE WHEN c.content_type = 'movie' THEN ucs.rating ELSE NULL END, (al.details->>'rating')::numeric) as rating,
                    CASE WHEN (r2.text_review IS NOT NULL AND r2.text_review != '') OR (r.text_review IS NOT NULL AND r.text_review != '') THEN true ELSE false END as has_review,
                    p.avatar_url as actor_avatar_url
                FROM activity_log al
                JOIN profiles p ON p.id = al.user_id
                LEFT JOIN reviews r ON r.id = al.review_id
                LEFT JOIN content c ON c.id = COALESCE(al.content_id, r.content_id)
                LEFT JOIN reviews r2 ON r2.content_id = c.id 
                    AND r2.user_id = al.user_id 
                    AND r2.is_deleted = false
                    AND (
                        r2.review_type = 'overall'
                        OR (r2.review_type = 'season' AND (CASE WHEN al.details->>'season' ~ '^[0-9]+$' THEN (al.details->>'season')::int ELSE NULL END) = ANY(r2.tagged_seasons))
                        OR (r2.review_type = 'episode' AND (CASE WHEN al.details->>'episode' ~ '^[0-9]+$' THEN (al.details->>'episode')::int ELSE NULL END) = ANY(r2.tagged_episodes))
                    )
                LEFT JOIN user_content_status ucs ON ucs.content_id = c.id AND ucs.user_id = al.user_id
                WHERE p.username = :username
                AND c.id IS NOT NULL
                AND (al.visibility = 'public' OR :is_owner = true)
                AND (al.review_id IS NULL OR (r.id IS NOT NULL AND r.is_deleted = false))
                AND (ucs.status IS NULL OR ucs.status NOT IN ('dropped', 'on_hold'))
                AND NOT (al.activity_type IN ('watched', 'rewatched', 'watching') AND (ucs.status IS NULL OR ucs.status = 'none' OR (c.content_type = 'movie' AND ucs.is_watched = false)))
                ORDER BY COALESCE(al.content_id, r.content_id), al.created_at DESC
            )
            SELECT * FROM ranked_activities
            ORDER BY watched_at DESC
            LIMIT 30
        '''), {'username': username, 'is_owner': is_owner})
        activities = [dict(row) for row in result.mappings()]
        activities.sort(key=lambda x: str(x.get('watched_at') or ''), reverse=True)
        
        # Fallback: if we have fewer than 5 unique watched/rewatched activities, query user_content_status
        unique_watched_count = len({act['content_id'] for act in activities if act['activity_type'] in ('watched', 'rewatched') and act['content_id']})
        if unique_watched_count < 5:
            ucs_res = await self.db.execute(text('''
                SELECT 
                    'watched' as activity_type,
                    GREATEST(ucs.last_watched_at, ucs.updated_at, ucs.created_at) as watched_at,
                    COALESCE(c.title, 'Deleted Content') as title,
                    c.poster_url,
                    COALESCE(c.content_type, 'movie') as content_type,
                    COALESCE(CAST(c.id AS TEXT), '') as content_id,
                    CAST(r.id AS TEXT) as review_id,
                    NULL as post_id,
                    CASE 
                        WHEN c.content_type IN ('series', 'anime', 'tv', 'tv_show') AND ucs.last_watched_season IS NOT NULL THEN
                            jsonb_build_object(
                                'season', ucs.last_watched_season, 
                                'season_number', ucs.last_watched_season, 
                                'seasons_watched', ucs.last_watched_season,
                                'episode', COALESCE(ucs.last_watched_episode, 0),
                                'episode_number', COALESCE(ucs.last_watched_episode, 0),
                                'episodes_watched', COALESCE(ucs.last_watched_episode, 0),
                                'progress_episodes', COALESCE(ucs.progress_episodes, 0)
                            )
                        ELSE NULL 
                    END as details,
                    :username as actor_username,
                    :display_name as actor_display_name,
                    ucs.status as user_content_status,
                    r.text_review as review_text,
                    COALESCE(ucs.rating, r.rating, r.star_rating) as rating,
                    CASE WHEN r.text_review IS NOT NULL AND r.text_review != '' THEN true ELSE false END as has_review,
                    :avatar_url as actor_avatar_url
                FROM user_content_status ucs
                JOIN content c ON c.id = ucs.content_id
                LEFT JOIN watch_history wh ON wh.user_id = ucs.user_id AND wh.content_id = c.id
                LEFT JOIN reviews r ON r.content_id = c.id AND r.user_id = ucs.user_id AND r.is_deleted = false
                WHERE ucs.user_id = CAST(:owner_id AS UUID)
                  AND (ucs.status = 'completed' OR (ucs.status = 'watching' AND COALESCE(ucs.progress_episodes, 0) > 0))
                ORDER BY COALESCE(ucs.last_watched_at, ucs.updated_at, ucs.created_at, wh.watched_at) DESC NULLS LAST
                LIMIT :fallback_limit
            '''), {
                'owner_id': owner_id,
                'username': username,
                'display_name': profile.get('display_name'),
                'avatar_url': profile.get('avatar_url'),
                'fallback_limit': 15
            })
            
            fallback_items = [dict(row) for row in ucs_res.mappings()]
            existing_cids = {act['content_id'] for act in activities if act['content_id']}
            for item in fallback_items:
                if item['content_id'] not in existing_cids:
                    activities.append(item)
                    existing_cids.add(item['content_id'])

        from datetime import datetime, timezone
        def get_watched_at(x):
            w = x.get('watched_at')
            if w is None:
                return datetime.min.replace(tzinfo=timezone.utc)
            if not isinstance(w, datetime):
                try:
                    from dateutil import parser
                    w = parser.parse(str(w))
                except Exception:
                    return datetime.min.replace(tzinfo=timezone.utc)
            if w.tzinfo is None:
                w = w.replace(tzinfo=timezone.utc)
            return w

        activities.sort(key=get_watched_at, reverse=True)
        activities = activities[:30]
            
        return activities

    async def get_library(self, username: str, viewer_id: str | None = None, status: str | None = None, limit: int = 500, offset: int = 0) -> list[dict]:
        """Returns all actively-tracked content (watching + on_hold) from user_content_status.
        This is the correct data source for the Library screen — permanent, never expires."""
        profile = await self.get_by_username(username, viewer_id)
        owner_id = str(profile['id'])

        # Privacy Check
        is_owner = viewer_id == owner_id
        visibility_setting = profile.get('library_visibility', 'public')
        
        if not is_owner:
            if visibility_setting == 'private':
                return []

        # Build status filter — default to watching + on_hold only
        allowed_statuses = ['watching', 'on_hold']
        if status and status in allowed_statuses:
            status_filter = f"AND ucs.status = '{status}'"
        else:
            status_filter = "AND ucs.status IN ('watching', 'on_hold')"

        result = await self.db.execute(text(f'''
            SELECT
                ucs.status as user_content_status,
                ucs.status as activity_type,
                ucs.last_activity_at as watched_at,
                ucs.progress_episodes as episodes_watched,
                CAST(c.id AS TEXT) as content_id,
                c.title,
                c.poster_url,
                COALESCE(c.content_type, 'movie') as content_type,
                c.total_episodes
            FROM user_content_status ucs
            JOIN content c ON c.id = ucs.content_id
            WHERE ucs.user_id = CAST(:owner_id AS UUID)
            {status_filter}
            ORDER BY ucs.last_activity_at DESC NULLS LAST
            LIMIT :limit OFFSET :offset
        '''), {'owner_id': owner_id, 'limit': limit, 'offset': offset})
        return [dict(row) for row in result.mappings()]


    async def get_liked_content(self, username: str, viewer_id: str | None = None) -> list[dict]:
        from app.repositories.user_repo import UserRepository
        repo = UserRepository(self.db)
        profile = await self.get_by_username(username, viewer_id)
        owner_id = str(profile['id'])
        
        if viewer_id != owner_id and profile.get('favourites_visibility') == 'private':
            return []
            
        return await repo.get_liked_content(owner_id)

    async def set_top_favorites(self, user_id: str, content_ids: list[str]) -> None:
        from app.repositories.user_repo import UserRepository
        repo = UserRepository(self.db)
        await repo.set_top_favorites(user_id, content_ids)

    async def get_prompt_picks(self, user_id_or_username: str) -> list[dict]:
        from app.repositories.user_repo import UserRepository
        repo = UserRepository(self.db)
        # Check if argument is UUID or username
        user = await repo.get_by_username(user_id_or_username)
        target_id = str(user['id']) if user else user_id_or_username
        return await repo.get_prompt_picks(target_id)

    async def set_prompt_picks(self, user_id: str, picks: list[str]) -> None:
        from app.repositories.user_repo import UserRepository
        from app.services.content_service import ContentService
        content_svc = ContentService(self.db)
        
        resolved_picks = []
        for p in picks:
            if p and str(p).strip():
                try:
                    uuid_val = await content_svc.ensure_content_persisted(str(p).strip())
                    resolved_picks.append(str(uuid_val))
                except Exception:
                    resolved_picks.append("")
            else:
                resolved_picks.append("")

        repo = UserRepository(self.db)
        await repo.set_prompt_picks(user_id, resolved_picks)


    async def get_received_recommendations(self, username: str) -> list[dict]:
        # Helper to bridge to RecommendationService or use direct query for speed
        result = await self.db.execute(text('''
            WITH RankedRecs AS (
                SELECT 
                    CAST(r.id AS TEXT) as recommendation_id, r.message, r.sent_at,
                    CAST(c.id AS TEXT) as content_id, c.title, c.poster_url, c.content_type, c.external_rating,
                    p_sender.username as actor_username, p_sender.display_name as actor_display_name,
                    p_sender.avatar_url as actor_avatar_url,
                    ROW_NUMBER() OVER(PARTITION BY c.id ORDER BY r.sent_at DESC) as rn
                FROM recommendations r
                JOIN recommendation_recipients rr ON rr.recommendation_id = r.id
                JOIN content c ON c.id = r.content_id
                JOIN profiles p_recipient ON p_recipient.id = rr.recipient_id
                JOIN profiles p_sender ON p_sender.id = r.sender_id
                WHERE p_recipient.username = :username
            )
            SELECT * FROM RankedRecs WHERE rn = 1
            ORDER BY sent_at DESC
            LIMIT 20
        '''), {'username': username})
        return [dict(row) for row in result.mappings()]

    async def get_collections(self, username: str, viewer_id: str | None = None) -> list[dict]:
        # Fast indexed resolution of username to UUID without heavy subqueries
        res = await self.db.execute(text("SELECT id FROM profiles WHERE username = :u AND is_deleted = false LIMIT 1"), {"u": username})
        row = res.mappings().one_or_none()
        if not row:
            raise NotFoundError('User')
        target_user_id = row['id']
        
        # 2. Use CollectionService for the optimized data
        from app.services.collection_service import CollectionService
        col_service = CollectionService(self.db)
        
        # Parse viewer_id safely
        v_id = None
        if viewer_id:
            try:
                v_id = UUID(str(viewer_id))
            except (ValueError, TypeError):
                pass
                
        return await col_service.get_user_collections(target_user_id, v_id)

    async def update_privacy(self, user_id: str, data: dict) -> dict:
        from app.repositories.user_repo import UserRepository
        repo = UserRepository(self.db)
        result = await repo.update_privacy(user_id, data)
        await self.invalidate_profile_cache(user_id)
        return result

    async def update_genres(self, user_id: str, genres: list[str]) -> list[str]:
        # Enforce 3-genre limit
        if len(genres) > 3:
            genres = genres[:3]
            
        from app.repositories.user_repo import UserRepository
        repo = UserRepository(self.db)
        await repo.set_favorite_genres(user_id, genres)
        await self.invalidate_profile_cache(user_id)
        return await repo.get_favorite_genres(user_id)

    async def get_trending_creators(self, limit: int = 10, viewer_id: Optional[str] = None) -> list[dict]:
        from app.repositories.user_repo import UserRepository
        repo = UserRepository(self.db)
        return await repo.get_trending_creators(limit, viewer_id)

    async def delete_account(self, user_id: str) -> None:
        from app.repositories.user_repo import UserRepository
        repo = UserRepository(self.db)
        await repo.delete_account(user_id)
        await self.invalidate_profile_cache(user_id)
        await self.db.commit()

    async def update_social_links(self, user_id: str, social_links: list[dict]) -> list[dict]:
        await self.db.execute(text('DELETE FROM social_links WHERE user_id = CAST(:uid AS UUID)'), {'uid': user_id})
        if social_links:
            for link in social_links:
                if link.get("platform") and link.get("url"):
                    await self.db.execute(text('''
                        INSERT INTO social_links (user_id, platform, url)
                        VALUES (CAST(:uid AS UUID), :platform, :url)
                    '''), {
                        'uid': user_id, 
                        'platform': link['platform'][:50], 
                        'url': link['url'][:500]
                    })
        await self.invalidate_profile_cache(user_id)
        await self.db.commit()
        
        res = await self.db.execute(text('SELECT platform, url FROM social_links WHERE user_id = CAST(:uid AS UUID)'), {'uid': user_id})
        return [dict(r) for r in res.mappings()]

    async def get_stats(self, user_id: str) -> dict:
        result = await self.db.execute(text('''
            SELECT 
                user_id,
                total_watched, total_reviews, total_posts,
                followers_count, following_count, friends_count,
                COALESCE(
                    CASE 
                        WHEN last_streak_at IS NULL THEN 0
                        WHEN now() AT TIME ZONE 'UTC' < date_trunc('day', last_streak_at AT TIME ZONE 'UTC') + interval '60 hours' THEN current_streak
                        ELSE 0
                    END, 
                    0
                ) AS current_streak,
                max_streak, last_streak_at,
                COALESCE(
                    CASE 
                        WHEN last_streak_at IS NULL THEN false
                        WHEN now() AT TIME ZONE 'UTC' >= date_trunc('day', last_streak_at AT TIME ZONE 'UTC') + interval '48 hours'
                         AND now() AT TIME ZONE 'UTC' < date_trunc('day', last_streak_at AT TIME ZONE 'UTC') + interval '60 hours' THEN true
                        ELSE false
                    END, 
                    false
                ) AS is_in_grace_period,
                (SELECT COUNT(DISTINCT content_id) FROM public.watch_history WHERE user_id = CAST(:user_id AS UUID) AND EXTRACT(year FROM watched_at) = EXTRACT(year FROM now())) AS watched_this_year,
                (SELECT COUNT(*) FROM public.reviews WHERE user_id = CAST(:user_id AS UUID) AND is_deleted = false AND EXTRACT(year FROM created_at) = EXTRACT(year FROM now())) AS reviews_this_year
            FROM user_stats
            WHERE user_id = CAST(:user_id AS UUID)
        '''), {'user_id': user_id})
        stats = result.mappings().first()
        if not stats:
            return {
                "user_id": user_id,
                "total_watched": 0,
                "total_reviews": 0,
                "total_posts": 0,
                "followers_count": 0,
                "following_count": 0,
                "friends_count": 0,
                "current_streak": 0,
                "max_streak": 0,
                "last_streak_at": None,
                "is_in_grace_period": False,
                "watched_this_year": 0,
                "reviews_this_year": 0
            }
        return dict(stats)

    async def get_wrapped_stats(self, username: str, timeframe: str, viewer_id: str | None = None) -> dict:
        import datetime
        profile = await self.get_by_username(username, viewer_id)
        owner_id = str(profile['id'])
        
        # Privacy Check
        is_owner = viewer_id == owner_id
        visibility_setting = profile.get('stats_visibility', profile.get('activity_visibility', 'public'))
        
        if not is_owner and visibility_setting == 'private':
            return {}
            
        now = datetime.datetime.now(datetime.timezone.utc)
        
        date_filter = ""
        params = {"uid": owner_id}
        
        if timeframe == "day":
            start_date = now - datetime.timedelta(hours=24)
        elif timeframe == "week":
            start_date = now - datetime.timedelta(days=7)
        elif timeframe == "month":
            start_date = now - datetime.timedelta(days=30)
        elif timeframe == "year":
            start_date = datetime.datetime(now.year, 1, 1, tzinfo=datetime.timezone.utc)
        else:
            start_date = None

        if start_date:
            params['start_date'] = start_date
            date_filter = """
                AND (
                    wh.watched_at >= :start_date 
                    OR EXISTS (
                        SELECT 1 FROM activity_log al 
                        WHERE al.user_id = wh.user_id 
                        AND al.content_id = wh.content_id 
                        AND al.activity_type IN ('watched', 'rewatched', 'rated', 'reviewed')
                        AND al.created_at >= :start_date
                    )
                )
            """
        else:
            date_filter = ""
            
        # 1. Total watches
        query = f'''
            SELECT 
                c.content_type, 
                c.total_episodes,
                wh.watched_at,
                wh.rating,
                wh.watch_type,
                c.id as content_id,
                c.title,
                c.poster_url,
                c.genres,
                c.release_date
            FROM watch_history wh
            JOIN content c ON wh.content_id = c.id
            WHERE wh.user_id = CAST(:uid AS UUID) {date_filter}
        '''
        
        res = await self.db.execute(text(query), params)
        watches = res.mappings().all()
        
        movies_count = 0
        series_count = 0
        anime_count = 0
        total_minutes = 0
        rewatch_count = 0
        
        genre_counts = {}
        rating_counts = [0, 0, 0, 0, 0] # 1 to 5 stars
        all_user_ratings = []
        decade_counts = {}
        day_of_week_counts = {}
        
        content_ratings = {} 
        content_info = {} 
        
        timeline_intensity = {}
        daily_watches = {}
        
        # 0. Fetch episode-level watches for precise series/anime time
        ep_date_filter = ""
        if start_date:
            ep_date_filter = "AND ewh.watched_at >= :start_date"
            
        ep_res = await self.db.execute(text(f'''
            SELECT ewh.content_id, c.content_type
            FROM episode_watch_history ewh
            JOIN content c ON c.id = ewh.content_id
            WHERE ewh.user_id = CAST(:uid AS UUID) {ep_date_filter}
        '''), params)
        episodes = ep_res.mappings().all()
        
        # Track which content IDs have episode-level data in this period
        contents_with_episodes = {} # cid -> count
        for ep in episodes:
            cid = str(ep['content_id'])
            contents_with_episodes[cid] = contents_with_episodes.get(cid, 0) + 1

        for w in watches:
            cid = str(w['content_id'])
            ctype = w['content_type'] or 'movie'
            
            if w.get('watch_type') == 'rewatch':
                rewatch_count += 1

            if ctype == 'movie':
                movies_count += 1
                total_minutes += 120
            elif ctype == 'series' or ctype == 'anime':
                if ctype == 'series': series_count += 1
                else: anime_count += 1
                
                # If we have individual episode logs, use them!
                if cid in contents_with_episodes:
                    ep_count = contents_with_episodes[cid]
                    total_minutes += ep_count * (45 if ctype == 'series' else 24)
                else:
                    # Fallback: Treat as a full series watch if they marked it completed
                    total_minutes += (w['total_episodes'] or 10) * (45 if ctype == 'series' else 24)
                
            # Genres
            genres = w['genres'] or []
            if isinstance(genres, str):
                import json
                try: genres = json.loads(genres)
                except: genres = []
                
            for g in genres:
                genre_counts[g] = genre_counts.get(g, 0) + 1
                
            # Decades
            rd_val = w.get('release_date')
            if rd_val:
                try:
                    year = int(str(rd_val)[:4])
                    decade = f"{(year // 10) * 10}s"
                    decade_counts[decade] = decade_counts.get(decade, 0) + 1
                except (ValueError, TypeError): pass

            # Ratings
            if w['rating'] is not None:
                r = float(w['rating'])
                r_5star = r / 2.0 if r > 5.0 else r
                all_user_ratings.append(r_5star)
                idx = max(0, min(4, int(r_5star - 0.001)))
                rating_counts[idx] += 1
                
                cid = w['content_id']
                if cid not in content_ratings or r_5star > content_ratings[cid]:
                    content_ratings[cid] = r_5star
                    content_info[cid] = {
                        "content_id": str(cid),
                        "title": w['title'],
                        "poster_url": w['poster_url'],
                        "rating": r_5star
                    }
            
            # Timeline Intensity & Day of Week
            dt = w['watched_at']
            if dt:
                day_name = dt.strftime('%A')
                day_of_week_counts[day_name] = day_of_week_counts.get(day_name, 0) + 1

                if timeframe in ['week', 'month']:
                    key = dt.strftime('%Y-%m-%d')
                elif timeframe == 'year':
                    key = dt.strftime('%b') # Month abbrev
                else:
                    key = dt.strftime('%Y')
                timeline_intensity[key] = timeline_intensity.get(key, 0) + 1
                
                day_key = dt.strftime('%Y-%m-%d')
                daily_watches[day_key] = daily_watches.get(day_key, 0) + 1
                
        # Aggregate Top Rated (sort by rating DESC)
        top_rated = sorted(content_info.values(), key=lambda x: x['rating'], reverse=True)[:10]
        
        # Aggregate Top Genres
        top_genres = [{"genre": k, "count": v} for k, v in sorted(genre_counts.items(), key=lambda item: item[1], reverse=True)[:5]]
        
        # Average Rating
        avg_rating = round(sum(all_user_ratings) / len(all_user_ratings), 1) if all_user_ratings else 0.0

        # Most Active Day
        most_active_day = max(day_of_week_counts, key=day_of_week_counts.get) if day_of_week_counts else "Weekends"

        # Decades formatted with percentages
        total_decade_watches = sum(decade_counts.values()) or 1
        decades = [
            {"decade": k, "percentage": round((v / total_decade_watches) * 100)}
            for k, v in sorted(decade_counts.items(), reverse=True)
        ]

        # Top Directors & Top Actors from DB credits
        top_directors = []
        top_actors = []
        try:
            dir_res = await self.db.execute(text('''
                SELECT p.name, p.profile_image_url, COUNT(DISTINCT wh.content_id) as count
                FROM watch_history wh
                JOIN content_credits cc ON cc.content_id = wh.content_id
                JOIN persons p ON p.id = cc.person_id
                WHERE wh.user_id = CAST(:uid AS UUID) AND (cc.job = 'Director' OR cc.role = 'director')
                GROUP BY p.id, p.name, p.profile_image_url
                ORDER BY count DESC LIMIT 5
            '''), {'uid': user_id})
            top_directors = [dict(r) for r in dir_res.mappings()]

            act_res = await self.db.execute(text('''
                SELECT p.name, p.profile_image_url, COUNT(DISTINCT wh.content_id) as count
                FROM watch_history wh
                JOIN content_credits cc ON cc.content_id = wh.content_id
                JOIN persons p ON p.id = cc.person_id
                WHERE wh.user_id = CAST(:uid AS UUID) AND cc.role = 'cast'
                GROUP BY p.id, p.name, p.profile_image_url
                ORDER BY count DESC LIMIT 5
            '''), {'uid': user_id})
            top_actors = [dict(r) for r in act_res.mappings()]
        except Exception as cred_err:
            logger.warning(f"Failed fetching directors/actors stats: {cred_err}")

        biggest_binge_date = None
        biggest_binge_count = 0
        if daily_watches:
            biggest_binge_date = max(daily_watches, key=daily_watches.get)
            biggest_binge_count = daily_watches[biggest_binge_date]
            
        if timeframe == 'year':
            months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
            timeline_formatted = [{"label": m, "count": timeline_intensity.get(m, 0)} for m in months]
        elif timeframe == 'all':
            years = sorted(timeline_intensity.keys())
            timeline_formatted = [{"label": str(y), "count": timeline_intensity[y]} for y in years]
        else:
            timeline_formatted = [{"label": k, "count": v} for k, v in sorted(timeline_intensity.items())]

        return {
            "total_minutes": total_minutes,
            "movies_count": movies_count,
            "series_count": series_count,
            "anime_count": anime_count,
            "avg_rating": avg_rating,
            "rewatch_count": rewatch_count,
            "most_active_day": most_active_day,
            "decades": decades,
            "top_directors": top_directors,
            "top_actors": top_actors,
            "top_rated": top_rated,
            "timeline_intensity": timeline_formatted,
            "rating_distribution": rating_counts,
            "top_genres": top_genres,
            "biggest_binge_date": biggest_binge_date,
            "biggest_binge_count": biggest_binge_count
        }

    async def get_followers(self, username: str, limit: int = 20, offset: int = 0) -> list[dict]:
        from app.repositories.user_repo import UserRepository
        user = await self.get_by_username(username, viewer_id=None)
        repo = UserRepository(self.db)
        return await repo.get_followers(str(user['id']), limit, offset)

    async def get_following(self, username: str, limit: int = 20, offset: int = 0) -> list[dict]:
        from app.repositories.user_repo import UserRepository
        user = await self.get_by_username(username, viewer_id=None)
        repo = UserRepository(self.db)
        return await repo.get_following(str(user['id']), limit, offset)

    async def get_friends(self, username: str, limit: int = 20, offset: int = 0) -> list[dict]:
        from app.repositories.social_repo import SocialRepository
        user = await self.get_by_username(username, viewer_id=None)
        repo = SocialRepository(self.db)
        return await repo.get_friends_list(user['id'], limit, offset)

    async def search_users(self, query: str, limit: int = 20, viewer_id: Optional[str] = None) -> list[dict]:
        """Search for users by username or display name."""
        from app.repositories.user_repo import UserRepository
        repo = UserRepository(self.db)
        
        # Handle @ prefix for specific username matching
        if query.startswith('@'):
            prefix = query[1:].strip()
            if not prefix:
                # Return suggested users if only '@' is typed
                return await repo.get_trending_creators(limit=6, viewer_id=viewer_id)
            # Use specific username prefix search
            return await repo.search_by_username_prefix(prefix, limit)
            
        # Default full-text search
        return await repo.search(query, limit, 0)

    async def toggle_person_favorite(self, user_id: str, person_id: str, name: str, profile_url: Optional[str], is_actor: bool) -> bool:
        from app.repositories.user_repo import UserRepository
        repo = UserRepository(self.db)
        return await repo.toggle_person_favorite(user_id, person_id, name, profile_url, is_actor)

    async def get_favorite_persons(self, user_id: str, is_actor: bool) -> list[dict]:
        from app.repositories.user_repo import UserRepository
        repo = UserRepository(self.db)
        return await repo.get_favorite_persons(user_id, is_actor)

    async def is_person_favorite(self, user_id: str, person_id: str) -> bool:
        from app.repositories.user_repo import UserRepository
        repo = UserRepository(self.db)
        return await repo.is_person_favorite(user_id, person_id)

    async def set_top_favorite_persons(self, user_id: str, person_ids: list[str], is_actor: bool) -> None:
        from app.repositories.user_repo import UserRepository
        repo = UserRepository(self.db)
        await repo.set_top_favorite_persons(user_id, person_ids, is_actor)
        await self.invalidate_profile_cache(user_id)
