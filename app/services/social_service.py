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

            # Create mutual direct conversation (Requirement 5)
            try:
                from app.services.chat_service import ChatService
                chat_svc = ChatService(self.db)
                await chat_svc.get_or_create_direct_conversation(
                    str(request['sender_id']),
                    str(request['receiver_id']),
                    bypass_friendship_check=True
                )
            except Exception as ce:
                import logging
                logging.getLogger('mambo.social').error(f"Failed to auto-create direct conversation: {ce}")
            
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

    async def remove_friend(self, user_id: UUID, friend_id: UUID) -> dict:
        # Delete friendship from repo
        deleted = await self.repo.delete_friend(user_id, friend_id)
        if not deleted:
            raise HTTPException(status_code=404, detail="Friendship not found")
            
        # Mutual unfollow
        from app.repositories.user_repo import UserRepository
        u_repo = UserRepository(self.db)
        await u_repo.unfollow(str(user_id), str(friend_id))
        await u_repo.unfollow(str(friend_id), str(user_id))
        
        # Decrement friends count
        await self.repo.decrement_friends_count(user_id)
        await self.repo.decrement_friends_count(friend_id)
        
        # Delete friend request record between them so they can send requests again
        await self.db.execute(text('''
            DELETE FROM friend_requests
            WHERE (sender_id = :u1 AND receiver_id = :u2)
               OR (sender_id = :u2 AND receiver_id = :u1)
        '''), {'u1': user_id, 'u2': friend_id})
        await self.db.commit()
        
        # Invalidate caches
        from app.services.user_service import UserService
        u_svc = UserService(self.db)
        await u_svc.invalidate_profile_cache(str(user_id))
        await u_svc.invalidate_profile_cache(str(friend_id))
        
        return {"status": "success", "message": "Friend removed successfully"}

    async def get_pending(self, user_id: UUID) -> list[dict]:
        return await self.repo.get_pending_requests(user_id)

    async def create_post(self, user_id: UUID, title: str, body: str, content_id: UUID | None = None, media_urls: list[str] = [], **kwargs) -> dict:
        if content_id:
            from app.services.content_service import ContentService
            content_svc = ContentService(self.db)
            content_id = await content_svc.ensure_content_persisted(content_id)

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

    async def delete_post(self, user_id: UUID, post_id: UUID) -> bool:
        post = await self.repo.get_post(post_id)
        if not post:
            return True
        if str(post.get('user_id')) != str(user_id):
            raise HTTPException(status_code=403, detail="Not authorized to delete this post")
        deleted = await self.repo.delete_post(user_id, post_id)
        if deleted:
            await self.repo.decrement_posts_count(user_id)
            from app.services.user_service import UserService
            u_svc = UserService(self.db)
            await u_svc.invalidate_profile_cache(str(user_id))
        return True

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

    async def delete_comment(self, user_id: UUID, comment_id: UUID) -> dict:
        deleted = await self.repo.delete_comment(user_id, comment_id)
        if not deleted:
            raise HTTPException(status_code=404, detail="Comment not found or not authorized")
        return {"status": "success"}

    async def toggle_upvote(self, user_id: UUID, target_id: UUID, target_type: str) -> bool:
        return await self.repo.toggle_upvote(user_id, target_id, target_type)

    async def toggle_review_like(self, user_id: UUID, review_id: UUID) -> bool:
        """Toggle a like on a review."""
        liked = await self.repo.toggle_review_like(user_id, review_id)
        if liked:
            try:
                # Query review author and content title
                res = await self.db.execute(text('''
                    SELECT r.user_id as author_id, c.title as content_title
                    FROM reviews r
                    LEFT JOIN content c ON c.id = r.content_id
                    WHERE r.id = :rid
                '''), {'rid': review_id})
                row = res.mappings().one_or_none()
                if row and row['author_id'] != user_id:
                    await self.notif_service.create_notification({
                        'user_id': row['author_id'],
                        'type': 'review_liked',
                        'actor_id': user_id,
                        'message': f"liked your review of {row['content_title'] or 'Content'}",
                        'related_id': review_id
                    })
            except Exception as e:
                import logging
                logging.getLogger('mambo.social').error(f"Failed to create like notification: {e}")
        return liked

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

    async def get_user_reviews(self, user_id: UUID, viewer_id: str | None = None, limit: int = 20, offset: int = 0, sort_order: str = 'desc', item_type: str | None = None) -> list[dict]:
        # Privacy Check
        from app.services.user_service import UserService
        u_svc = UserService(self.db)
        profile = await u_svc.get_by_id(str(user_id))
        
        if str(viewer_id) != str(user_id) and profile.get('reviews_visibility') == 'private':
            return []

        viewer_uuid = UUID(viewer_id) if viewer_id else None
        return await self.repo.get_reviews_by_user(user_id, limit, offset, sort_order, current_user_id=viewer_uuid, item_type=item_type)

    async def create_review(
        self,
        user_id: UUID,
        content_id: UUID,
        star_rating: float,
        text_review: str | None = None,
        contains_spoiler: bool = False,
        tags: list[str] | None = None,
        tagged_seasons: list[int] | None = None,
        tagged_episodes: list[int] | None = None,
        review_type: str = "overall",
        **kwargs
    ) -> dict:
        # 0. Guarantee content row exists in PostgreSQL content table
        from app.services.content_service import ContentService
        content_svc = ContentService(self.db)
        content_id = await content_svc.ensure_content_persisted(content_id)

        # 1. Enforce mandatory text
        if not text_review or not text_review.strip():
            raise HTTPException(status_code=400, detail="Review text is mandatory. Use 'Rate' for star-only ratings.")

        # Check if the latest watch session is unreviewed
        session_res = await self.db.execute(text('''
            SELECT id, review_id FROM watch_history 
            WHERE user_id = :uid AND content_id = :cid 
            ORDER BY watched_at DESC LIMIT 1
        '''), {'uid': user_id, 'cid': content_id})
        latest_session = session_res.mappings().one_or_none()

        # 2. Find the most recent watch session to link (ONLY if unreviewed)
        from app.services.action_service import ActionService
        from app.models.action import ActionType
        action_svc = ActionService(self.db)

        # Fetch content type
        content_res = await self.db.execute(text(
            "SELECT content_type FROM content WHERE id = :id"
        ), {"id": content_id})
        content_row = content_res.mappings().one_or_none()
        content_type = content_row.get('content_type') if content_row else None

        # Fetch watch status
        status_res = await self.db.execute(text(
            "SELECT is_watched FROM user_content_status WHERE user_id = :uid AND content_id = :cid"
        ), {"uid": user_id, "cid": content_id})
        status_row = status_res.mappings().one_or_none() or {}
        is_watched = status_row.get('is_watched') or False

        # Only use latest session if it doesn't already have a review attached!
        unreviewed_session = latest_session if (latest_session and latest_session.get('review_id') is None) else None

        watch_history_id = None
        if unreviewed_session:
            watch_history_id = unreviewed_session['id']
            # Update the rating for this session to match the review
            await self.db.execute(text('''
                UPDATE watch_history SET rating = :r WHERE id = :wid
            '''), {'r': star_rating, 'wid': watch_history_id})
            
            # Only auto-complete for MOVIES — TV shows require explicit episode tracking
            if content_type == 'movie' and not is_watched:
                await self.db.execute(text('''
                    UPDATE user_content_status 
                    SET is_watched = true, is_dropped = false, status = 'completed', watch_count = COALESCE(watch_count, 0) + 1, last_watched_at = now(), updated_at = now()
                    WHERE user_id = :uid AND content_id = :cid
                '''), {'uid': user_id, 'cid': content_id})
                await action_svc._sync_to_collection(user_id, content_id, 'Watched')
                await self.db.execute(text('''
                    UPDATE user_stats SET total_watched = COALESCE(total_watched, 0) + 1, updated_at = now() WHERE user_id = :uid
                '''), {'uid': user_id})
                await action_svc._update_streak(user_id)
        else:
            # Create a NEW watch history session for this review
            if content_type == 'movie':
                watch_history_id = await action_svc._handle_watch(user_id, content_id, ActionType.watch)
            else:
                import uuid as _uuid
                wh_res = await self.db.execute(text('''
                    INSERT INTO watch_history (id, user_id, content_id, watch_type, watched_at, rating)
                    VALUES (:id, :uid, :cid, 'review_only', now(), :r)
                    RETURNING id
                '''), {'id': _uuid.uuid4(), 'uid': user_id, 'cid': content_id, 'r': star_rating})
                watch_history_id = wh_res.scalar()

            await self.db.execute(text('''
                UPDATE watch_history SET rating = :r WHERE id = :wid
            '''), {'r': star_rating, 'wid': watch_history_id})

        # 3. Create review record
        review = await self.repo.create_review(
            user_id=user_id,
            content_id=content_id,
            star_rating=star_rating,
            text_review=text_review,
            is_spoiler=contains_spoiler,
            tagged_seasons=tagged_seasons,
            tagged_episodes=tagged_episodes,
            review_type=review_type
        )

        # 4. Link the session to the review
        await self.db.execute(text('''
            UPDATE reviews SET watch_history_id = :wid WHERE id = :rid
        '''), {'wid': watch_history_id, 'rid': review['id']})
        
        await self.db.execute(text('''
            UPDATE watch_history SET review_id = :rid WHERE id = :wid
        '''), {'rid': review['id'], 'wid': watch_history_id})

        # 5. Log activity (Always 'reviewed' here because text is mandatory)
        await action_svc._log_activity(
            user_id=user_id,
            activity_type='reviewed',
            content_id=content_id,
            review_id=review['id'],
            details={
                'rating': star_rating,
                'tagged_seasons': tagged_seasons,
                'tagged_episodes': tagged_episodes,
                'review_type': review_type
            }
        )

        # 6. Update user stats
        await self.db.execute(text('''
            INSERT INTO user_stats (user_id, total_reviews)
            VALUES (:user_id, 1)
            ON CONFLICT (user_id) DO UPDATE SET
                total_reviews = user_stats.total_reviews + 1,
                updated_at = now()
        '''), {'user_id': user_id})

        # Invalidate profile cache
        from app.services.user_service import UserService
        u_svc = UserService(self.db)
        await u_svc.invalidate_profile_cache(str(user_id))

        await self.db.commit()
        return review

    async def get_review(self, review_id: UUID, current_user_id: UUID | None = None) -> dict:
        review = await self.repo.get_review(review_id, current_user_id)
        if not review:
            raise HTTPException(status_code=404, detail="Review not found")
        return review

    async def get_trending_reviews(self, limit: int = 5, current_user_id: UUID | None = None) -> list[dict]:
        return await self.repo.get_trending_reviews(limit, current_user_id)

    async def get_review_of_the_day(self, current_user_id: UUID | None = None) -> dict | None:
        """Picks a random trending review that changes every 24 hours."""
        from datetime import datetime
        trending = await self.repo.get_trending_reviews(limit=10, current_user_id=current_user_id)
        if not trending:
            return None
            
        # Daily seed: days since epoch
        seed = int(datetime.now().timestamp() // 86400)
        index = seed % len(trending)
        return trending[index]

    async def get_content_reviews(self, content_id: UUID, limit: int = 20, offset: int = 0, current_user_id: UUID | None = None, tmdb_id: int | None = None, title: str | None = None) -> list[dict]:
        return await self.repo.get_reviews_by_content(content_id, limit, offset, current_user_id, tmdb_id=tmdb_id, title=title)

    async def get_content_posts(self, content_id: UUID, limit: int = 20, offset: int = 0, tmdb_id: int | None = None, title: str | None = None) -> list[dict]:
        """Fetch posts (discussions) related to a specific content item."""
        return await self.repo.get_posts_by_content(content_id, limit, offset, tmdb_id=tmdb_id, title=title)

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
