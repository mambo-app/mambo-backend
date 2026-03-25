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
        result = await self.db.execute(text('''
            SELECT p.*,
                   us.followers_count, us.following_count, us.friends_count, us.total_posts
            FROM profiles p
            LEFT JOIN user_stats us ON us.user_id = p.id
            WHERE p.username = :username
            AND p.is_deleted = false
        '''), {'username': username})
        profile = result.mappings().first()
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
            SELECT p.name 
            FROM user_actor_preferences uap
            JOIN persons p ON p.id = CAST(uap.person_id AS TEXT)
            WHERE uap.user_id = CAST(:id AS UUID)
            ORDER BY uap.preference_order ASC
        '''), {'id': owner_id})
        profile_dict['favorite_actors'] = [row[0] for row in actors_res]

        # Get favorite directors
        directors_res = await self.db.execute(text('''
            SELECT p.name 
            FROM user_director_preferences udp
            JOIN persons p ON p.id = CAST(udp.person_id AS TEXT)
            WHERE udp.user_id = CAST(:id AS UUID)
            ORDER BY udp.preference_order ASC
        '''), {'id': owner_id})
        profile_dict['favorite_directors'] = [row[0] for row in directors_res]
        
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
        # 1. Define editable fields
        allowed_fields = {'display_name', 'bio', 'gender', 'birthday', 'avatar_url'}
        updates = {k: v for k, v in data.items() if k in allowed_fields}

        if not updates:
            return await self.get_by_id(user_id)

        # 2. Normalize and Parse
        if 'gender' in updates and updates['gender']:
            updates['gender'] = updates['gender'].lower().strip()
            
        if 'birthday' in updates and updates['birthday']:
            from datetime import date
            try:
                updates['birthday'] = date.fromisoformat(updates['birthday'])
            except ValueError:
                from fastapi import HTTPException
                raise HTTPException(status_code=400, detail="Invalid birthday format. Use YYYY-MM-DD")

        # 3. Build query
        set_clause = ", ".join([f"{k} = :{k}" for k in updates.keys()])
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
                   us.followers_count, us.following_count, us.friends_count, us.total_posts
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
            SELECT p.name 
            FROM user_actor_preferences uap
            JOIN persons p ON p.id = CAST(uap.person_id AS TEXT)
            WHERE uap.user_id = CAST(:id AS UUID)
            ORDER BY uap.preference_order ASC
        '''), {'id': user_id})
        profile_dict['favorite_actors'] = [row[0] for row in actors_res]

        # Get favorite directors
        directors_res = await self.db.execute(text('''
            SELECT p.name 
            FROM user_director_preferences udp
            JOIN persons p ON p.id = CAST(udp.person_id AS TEXT)
            WHERE udp.user_id = CAST(:id AS UUID)
            ORDER BY udp.preference_order ASC
        '''), {'id': user_id})
        profile_dict['favorite_directors'] = [row[0] for row in directors_res]

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
            # TODO: Handle 'friends' visibility if SocialService is available here
        
        # 1. Cleanup old activity (> 7 days) as requested: "completely gone"
        await self.db.execute(text('''
            DELETE FROM activity_log 
            WHERE user_id = CAST(:owner_id AS UUID) 
            AND created_at < now() - interval '7 days'
        '''), {'owner_id': owner_id})
        await self.db.commit()

        # 2. Fetch recent activity
        result = await self.db.execute(text('''
            SELECT 
                al.activity_type, 
                al.created_at as watched_at,
                c.title, 
                c.poster_url, 
                c.content_type, 
                c.id as content_id,
                al.review_id,
                al.post_id,
                al.details,
                p.username as actor_username,
                p.display_name as actor_display_name
            FROM activity_log al
            JOIN profiles p ON p.id = al.user_id
            LEFT JOIN content c ON c.id = al.content_id
            LEFT JOIN reviews r ON r.id = al.review_id
            WHERE p.username = :username
            AND (al.visibility = 'public' OR :is_owner = true)
            AND (al.review_id IS NULL OR (r.id IS NOT NULL AND r.is_deleted = false))
            ORDER BY al.created_at DESC
            LIMIT 30
        '''), {'username': username, 'is_owner': is_owner})
        return [dict(row) for row in result.mappings()]

    async def get_liked_content(self, username: str, viewer_id: str | None = None) -> list[dict]:
        profile = await self.get_by_username(username, viewer_id)
        owner_id = str(profile['id'])
        
        if viewer_id != owner_id and profile.get('favourites_visibility') == 'private':
            return []

        result = await self.db.execute(text('''
            SELECT c.id as content_id, c.title, c.poster_url, c.content_type, ucs.updated_at as liked_at
            FROM user_content_status ucs
            JOIN profiles p ON p.id = ucs.user_id
            JOIN content c ON c.id = ucs.content_id
            WHERE p.username = :username AND ucs.is_liked = true
            ORDER BY ucs.updated_at DESC
        '''), {'username': username})
        return [dict(row) for row in result.mappings()]

    async def get_received_recommendations(self, username: str) -> list[dict]:
        # Helper to bridge to RecommendationService or use direct query for speed
        result = await self.db.execute(text('''
            SELECT 
                r.id as recommendation_id, r.message, r.sent_at,
                c.id as content_id, c.title, c.poster_url, c.content_type, c.external_rating,
                p_sender.username as actor_username, p_sender.display_name as actor_display_name,
                p_sender.avatar_url as actor_avatar_url
            FROM recommendations r
            JOIN recommendation_recipients rr ON rr.recommendation_id = r.id
            JOIN content c ON c.id = r.content_id
            JOIN profiles p_recipient ON p_recipient.id = rr.recipient_id
            JOIN profiles p_sender ON p_sender.id = r.sender_id
            WHERE p_recipient.username = :username
            ORDER BY r.sent_at DESC
            LIMIT 20
        '''), {'username': username})
        return [dict(row) for row in result.mappings()]

    async def get_collections(self, username: str, viewer_id: str | None = None) -> list[dict]:
        # Fetch collections and apply privacy logic
        result = await self.db.execute(text('''
            SELECT c.id, c.user_id, c.name, c.description, c.is_public, 
                   c.is_pinned, c.pin_order, c.is_default, c.is_deletable,
                   c.created_at, c.visibility,
                   COUNT(ci.content_id) as item_count
            FROM collections c
            JOIN profiles p ON p.id = c.user_id
            LEFT JOIN collection_items ci ON ci.collection_id = c.id
            WHERE p.username = :username 
            AND (c.visibility = 'public' OR c.user_id = CAST(:viewer_id AS UUID))
            GROUP BY c.id, c.user_id, c.name, c.description, c.is_public, 
                     c.is_pinned, c.pin_order, c.is_default, c.is_deletable,
                     c.created_at, c.visibility
            ORDER BY c.is_pinned DESC, c.pin_order ASC, c.created_at DESC
        '''), {'username': username, 'viewer_id': viewer_id})
        return [dict(row) for row in result.mappings()]

    async def update_privacy(self, user_id: str, data: dict) -> dict:
        from app.repositories.user_repo import UserRepository
        repo = UserRepository(self.db)
        return await repo.update_privacy(user_id, data)

    async def update_genres(self, user_id: str, genres: list[str]) -> list[str]:
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
                followers_count, following_count, friends_count
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
                "friends_count": 0
            }
        return dict(stats)

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
