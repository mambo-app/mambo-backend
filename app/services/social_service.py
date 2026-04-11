from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from uuid import UUID
from fastapi import HTTPException
from app.repositories.social_repo import SocialRepository
from app.services.notification_service import NotificationService
from app.services.chat_service import ChatService

class SocialService:
    def __init__(self, db: AsyncSession):
        self.db = db
        self.repo = SocialRepository(db)
        self.notif_service = NotificationService(db)

    async def send_friend_request(self, sender_id: UUID, receiver_id: UUID) -> dict:
        if sender_id == receiver_id:
            raise HTTPException(status_code=400, detail="Cannot send friend request to yourself")
            
        # Check if already friends or request pending
        existing = await self.repo.check_request_exists(sender_id, receiver_id)
        if existing:
            if existing['status'] == 'accepted':
                raise HTTPException(status_code=400, detail="Already friends")
            if existing['status'] == 'pending':
                raise HTTPException(status_code=400, detail="Request already pending")
        
        request = await self.repo.create_friend_request(sender_id, receiver_id)
        
        # Notify
        await self.notif_service.create_notification({
            'user_id': receiver_id,
            'type': 'friend_request',
            'title': 'Friend Request',
            'actor_id': sender_id,
            'message': 'sent you a friend request',
            'related_id': request['id'] # Include request ID for easy accept/ignore
        })
        
        return request

    async def cancel_friend_request(self, sender_id: UUID, receiver_id: UUID) -> dict:
        deleted = await self.repo.delete_friend_request(sender_id, receiver_id)
        if not deleted:
            raise HTTPException(status_code=404, detail="Friend request not found")
        
        # Optionally remove notification? 
        # For now just return success
        return {"status": "success", "message": "Friend request cancelled"}

    async def respond_to_request(self, user_id: UUID, request_id: UUID, status: str) -> dict:
        if status not in ['accepted', 'ignored']:
            raise HTTPException(status_code=400, detail="Invalid status")
            
        request = await self.repo.get_friend_request(request_id)
        if not request:
            raise HTTPException(status_code=404, detail="Request not found")
        
        if request['receiver_id'] != user_id:
            raise HTTPException(status_code=403, detail="Not authorized to respond to this request")
            
        if request['status'] != 'pending':
            raise HTTPException(status_code=400, detail="Request already processed")

        updated = await self.repo.update_request_status(request_id, status)
        
        if status == 'accepted':
            # Create mutual friendship
            await self.repo.add_friend(request['sender_id'], request['receiver_id'])
            
            # Create mutual follows (Friendship = Mutual Follow)
            from app.repositories.user_repo import UserRepository
            u_repo = UserRepository(self.db)
            await u_repo.follow(str(request['sender_id']), str(request['receiver_id']))
            await u_repo.follow(str(request['receiver_id']), str(request['sender_id']))
            
            # Update stats - UserRepository.follow already increments followers/following
            # but we also need to increment friends_count specifically.
            await self.repo.increment_friends_count(request['sender_id'])
            await self.repo.increment_friends_count(request['receiver_id'])

            # Invalidate caches for both
            from app.services.user_service import UserService
            u_svc = UserService(self.db)
            await u_svc.invalidate_profile_cache(str(request['sender_id']))
            await u_svc.invalidate_profile_cache(str(request['receiver_id']))
            
            # Notify sender
            await self.notif_service.create_notification({
                'user_id': request['sender_id'],
                'type': 'friend_accepted',
                'actor_id': user_id,
                'message': 'accepted your friend request'
            })
            
        return updated

    async def get_friends(self, user_id: UUID, limit: int = 20, offset: int = 0) -> list[dict]:
        return await self.repo.get_friends_list(user_id, limit, offset)

    async def get_pending(self, user_id: UUID) -> list[dict]:
        return await self.repo.get_pending_requests(user_id)

    # --- Phase 3: Community Methods ---
    async def create_post(self, user_id: UUID, title: str, body: str, content_id: UUID | None = None, media_urls: list[str] = [], **kwargs) -> dict:
        data = {
            'title': title,
            'body': body,
            'content_id': content_id,
            'media_urls': media_urls
        }
        post = await self.repo.create_post(user_id, data)
        # Update stats
        await self.repo.increment_posts_count(user_id)
        # Invalidate profile cache
        from app.services.user_service import UserService
        u_svc = UserService(self.db)
        await u_svc.invalidate_profile_cache(str(user_id))
        return post

    async def get_posts(self, limit: int = 20, offset: int = 0, viewer_id: UUID | None = None) -> list[dict]:
        return await self.repo.get_posts(limit, offset, viewer_id)

    async def get_post(self, post_id: UUID) -> dict:
        post = await self.repo.get_post(post_id)
        if not post:
            raise HTTPException(status_code=404, detail="Post not found")
        return post

    async def create_comment(self, user_id: UUID, content: str, post_id: UUID | None = None, review_id: UUID | None = None, parent_id: UUID | None = None) -> dict:
        if not post_id and not review_id:
            raise HTTPException(status_code=400, detail="Either post_id or review_id must be provided")
        return await self.repo.create_comment(user_id, content, post_id, review_id, parent_id)

    async def add_post_comment(self, user_id: UUID, post_id: UUID, content: str, parent_comment_id: UUID | None = None) -> dict:
        return await self.create_comment(user_id=user_id, content=content, post_id=post_id, parent_id=parent_comment_id)
        
    async def add_review_comment(self, user_id: UUID, review_id: UUID, content: str, parent_comment_id: UUID | None = None) -> dict:
        return await self.create_comment(user_id=user_id, content=content, review_id=review_id, parent_id=parent_comment_id)

    async def get_comments(self, post_id: UUID | None = None, review_id: UUID | None = None, limit: int = 50, offset: int = 0) -> list[dict]:
        return await self.repo.get_comments(post_id, review_id, limit, offset)

    async def toggle_upvote(self, user_id: UUID, target_id: UUID, target_type: str) -> bool:
        return await self.repo.toggle_upvote(user_id, target_id, target_type)

    async def toggle_review_like(self, user_id: UUID, review_id: UUID) -> bool:
        """Toggle a like on a review."""
        return await self.repo.toggle_review_like(user_id, review_id)

    async def toggle_post_upvote(self, user_id: UUID, post_id: UUID) -> bool:
        """Toggle an upvote on a post."""
        return await self.repo.toggle_upvote(user_id, post_id, 'post')


    async def save_post(self, user_id: UUID, post_id: UUID) -> bool:
        """Save a post."""
        from sqlalchemy import text
        await self.db.execute(text('''
            INSERT INTO post_saves (user_id, post_id)
            VALUES (:user_id, :post_id)
            ON CONFLICT (user_id, post_id) DO NOTHING
        '''), {'user_id': user_id, 'post_id': post_id})
        await self.db.commit()
        return True

    async def unsave_post(self, user_id: UUID, post_id: UUID) -> bool:
        """Unsave a post."""
        from sqlalchemy import text
        await self.db.execute(text('''
            DELETE FROM post_saves WHERE user_id = :user_id AND post_id = :post_id
        '''), {'user_id': user_id, 'post_id': post_id})
        await self.db.commit()
        return True

    async def get_share_metadata(self, user_id: UUID, target_id: UUID, target_type: str, conversation_id: UUID | None = None, recipient_id: UUID | None = None) -> dict:
        if target_type == 'post':
            item = await self.repo.get_post(target_id)
        elif target_type == 'review':
            item = await self.repo.get_review(target_id)
        else:
            raise HTTPException(status_code=400, detail="Invalid target type for metadata")
            
        if not item:
            raise HTTPException(status_code=404, detail="Item not found")
            
        if not conversation_id and not recipient_id:
            raise HTTPException(status_code=400, detail="Either conversation_id or recipient_id is required")

        # Implementation for sharing: send a chat message
        chat_svc = ChatService(self.db)
        
        cid = str(conversation_id) if conversation_id else None
        rid = str(recipient_id) if recipient_id else None
            
        if not cid and rid:
            # Fallback: get or create 1:1 conversation
            cid = await chat_svc.get_or_create_direct_conversation(str(user_id), rid)
            
        if cid:
            await chat_svc.send_message(
                user_id=str(user_id),
                conversation_id=cid,
                body=f"Shared a {target_type}",
                receiver_id=rid,
                shared_post_id=target_id if target_type == 'post' else None,
                shared_review_id=target_id if target_type == 'review' else None
            )
            # Increment share count
            table = "posts" if target_type == 'post' else "reviews"
            from sqlalchemy import text
            await self.db.execute(text(f"UPDATE {table} SET shares_count = shares_count + 1 WHERE id = :tid"), {'tid': target_id})
            await self.db.commit()

        return {
            'title': item.get('title') or (item.get('text_review')[:50] + '...' if item.get('text_review') else 'Review'),
            'poster_url': item.get('poster_url') if target_type == 'post' else item.get('image_url'),
            'type': target_type,
            'target_id': target_id,
            'success': True
        }

    async def share_post(self, user_id: UUID, post_id: UUID, conversation_id: UUID | None = None, recipient_id: UUID | None = None) -> dict:
        return await self.get_share_metadata(user_id, post_id, 'post', conversation_id, recipient_id)

    async def share_review(self, user_id: UUID, review_id: UUID, conversation_id: UUID | None = None, recipient_id: UUID | None = None) -> dict:
        return await self.get_share_metadata(user_id, review_id, 'review', conversation_id, recipient_id)

    async def get_user_reviews(self, user_id: UUID, viewer_id: str | None = None, limit: int = 20, offset: int = 0) -> list[dict]:
        # Privacy Check
        from app.services.user_service import UserService
        u_svc = UserService(self.db)
        profile = await u_svc.get_by_id(str(user_id))
        
        if str(viewer_id) != str(user_id) and profile.get('reviews_visibility') == 'private':
            return []

        return await self.repo.get_reviews_by_user(user_id, limit, offset)

    async def create_review(self, user_id: UUID, content_id: UUID, star_rating: float, text_review: str | None = None, contains_spoiler: bool = False, tags: list[str] = []) -> dict:
        # 1. Create review
        review = await self.repo.create_review(
            user_id=user_id,
            content_id=content_id,
            star_rating=star_rating,
            text_review=text_review,
            is_spoiler=contains_spoiler
        )

        # 2. Automatic watch/rewatch logic
        from app.services.action_service import ActionService
        from app.models.action import ActionType
        action_svc = ActionService(self.db)
        
        # Trigger watch logic
        await action_svc._handle_watch(user_id, content_id, ActionType.watch)
        # Sync to Watched collection
        await action_svc._sync_to_collection(user_id, content_id, 'Watched')

        # 3. Log activity for the review itself
        # Change type based on whether text was provided as requested
        activity_type = 'reviewed' if text_review and text_review.strip() else 'rated'
        
        await action_svc._log_activity(
            user_id=user_id,
            activity_type=activity_type,
            content_id=content_id,
            review_id=review['id'],
            details={'rating': star_rating}
        )

        # 4. Update user stats
        await self.db.execute(text('''
            INSERT INTO user_stats (user_id, total_reviews)
            VALUES (:user_id, 1)
            ON CONFLICT (user_id) DO UPDATE SET
                total_reviews = user_stats.total_reviews + 1,
                updated_at = now()
        '''), {'user_id': user_id})

        # Invalidate profile cache to reflect new review count
        from app.services.user_service import UserService
        u_svc = UserService(self.db)
        await u_svc.invalidate_profile_cache(str(user_id))

        await self.db.commit()
        return review

    async def get_trending_reviews(self, limit: int = 5) -> list[dict]:
        return await self.repo.get_trending_reviews(limit)

    async def get_content_reviews(self, content_id: UUID, limit: int = 20, offset: int = 0) -> list[dict]:
        return await self.repo.get_reviews_by_content(content_id, limit, offset)

    async def get_content_posts(self, content_id: UUID, limit: int = 20, offset: int = 0) -> list[dict]:
        """Fetch posts (discussions) related to a specific content item."""
        return await self.repo.get_posts_by_content(content_id, limit, offset)

    async def mute_user(self, user_id: UUID, target_id: UUID) -> dict:
        if user_id == target_id:
            raise HTTPException(status_code=400, detail="Cannot mute yourself")
        await self.repo.mute_user(user_id, target_id)
        return {"message": "User muted successfully"}

    async def unmute_user(self, user_id: UUID, target_id: UUID) -> dict:
        await self.repo.unmute_user(user_id, target_id)
        return {"message": "User unmuted successfully"}

    async def block_user(self, user_id: UUID, target_id: UUID) -> dict:
        if user_id == target_id:
            raise HTTPException(status_code=400, detail="Cannot block yourself")
        await self.repo.block_user(user_id, target_id)
        return {"message": "User blocked successfully"}

    async def unblock_user(self, user_id: UUID, target_id: UUID) -> dict:
        await self.repo.unblock_user(user_id, target_id)
        return {"message": "User unblocked successfully"}

    async def update_review(self, user_id: UUID, review_id: UUID, data: dict) -> dict:
        result = await self.repo.update_review(review_id, user_id, data)
        if not result:
            raise HTTPException(status_code=404, detail="Review not found or not authorized")
        
        # Log activity
        from app.services.action_service import ActionService
        action_svc = ActionService(self.db)
        
        # Check if text exists in the original result or new data
        text_content = data.get('text_review') or result.get('text_review')
        activity_type = 'updated_review' if text_content and text_content.strip() else 'updated_rating'

        await action_svc._log_activity(
            user_id=user_id,
            activity_type=activity_type,
            content_id=result['content_id'],
            review_id=review_id,
            details={'rating': data.get('star_rating')}
        )
        
        await self.db.commit()
        return result
