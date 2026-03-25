from .base import BaseRepository
from uuid import UUID
from sqlalchemy import text

class SocialRepository(BaseRepository):
    async def create_friend_request(self, sender_id: UUID, receiver_id: UUID) -> dict:
        return await self.execute_returning('''
            INSERT INTO friend_requests (sender_id, receiver_id, status)
            VALUES (:sender_id, :receiver_id, 'pending')
            RETURNING *
        ''', {'sender_id': sender_id, 'receiver_id': receiver_id})

    async def get_friend_request(self, request_id: UUID) -> dict | None:
        return await self.fetch_one('''
            SELECT * FROM friend_requests WHERE id = :id
        ''', {'id': request_id})

    async def delete_friend_request(self, sender_id: UUID, receiver_id: UUID) -> bool:
        res = await self.db.execute(text('''
            DELETE FROM friend_requests
            WHERE ((sender_id = :sender_id AND receiver_id = :receiver_id)
               OR (sender_id = :receiver_id AND receiver_id = :sender_id))
            AND status = 'pending'
        '''), {'sender_id': sender_id, 'receiver_id': receiver_id})
        await self.db.commit()
        return res.rowcount > 0

    async def update_request_status(self, request_id: UUID, status: str) -> dict:
        return await self.execute_returning('''
            UPDATE friend_requests
            SET status = :status, updated_at = now()
            WHERE id = :id
            RETURNING *
        ''', {'id': request_id, 'status': status})

    async def add_friend(self, user_id1: UUID, user_id2: UUID) -> None:
        # Ensure user_id1 < user_id2 for consistency
        u1, u2 = sorted([user_id1, user_id2])
        await self.execute('''
            INSERT INTO friends (user_id1, user_id2)
            VALUES (:u1, :u2)
            ON CONFLICT DO NOTHING
        ''', {'u1': u1, 'u2': u2})

    async def get_friends_list(self, user_id: UUID, limit: int = 20, offset: int = 0) -> list[dict]:
        return await self.fetch_many('''
            SELECT p.id AS user_id, p.username, p.display_name, p.avatar_url, p.is_verified, f.created_at as friends_since
            FROM friends f
            JOIN profiles p ON (p.id = f.user_id1 OR p.id = f.user_id2)
            WHERE (f.user_id1 = :user_id OR f.user_id2 = :user_id)
            AND p.id != :user_id
            AND p.is_deleted = false
            ORDER BY f.created_at DESC
            LIMIT :limit OFFSET :offset
        ''', {'user_id': user_id, 'limit': limit, 'offset': offset})

    async def check_is_friend(self, owner_id: UUID, viewer_id: UUID) -> bool:
        """O(1) EXISTS check — replaces the O(n) friends-list privacy check."""
        u1, u2 = sorted([owner_id, viewer_id])
        result = await self.fetch_one('''
            SELECT 1 FROM friends WHERE user_id1 = :u1 AND user_id2 = :u2
        ''', {'u1': u1, 'u2': u2})
        return result is not None

    async def get_pending_requests(self, user_id: UUID, limit: int = 50, offset: int = 0) -> list[dict]:
        return await self.fetch_many('''
            SELECT fr.*, p.username as sender_username, p.avatar_url as sender_avatar_url
            FROM friend_requests fr
            JOIN profiles p ON p.id = fr.sender_id
            WHERE fr.receiver_id = :user_id AND fr.status = 'pending'
            ORDER BY fr.created_at DESC
            LIMIT :limit OFFSET :offset
        ''', {'user_id': user_id, 'limit': limit, 'offset': offset})

    async def increment_friends_count(self, user_id: UUID) -> None:
        await self.execute('''
            UPDATE user_stats SET friends_count = COALESCE(friends_count, 0) + 1 WHERE user_id = :user_id
        ''', {'user_id': user_id})

    async def increment_followers_count(self, user_id: UUID) -> None:
        await self.execute('''
            UPDATE user_stats SET followers_count = COALESCE(followers_count, 0) + 1 WHERE user_id = :user_id
        ''', {'user_id': user_id})

    async def increment_following_count(self, user_id: UUID) -> None:
        await self.execute('''
            UPDATE user_stats SET following_count = COALESCE(following_count, 0) + 1 WHERE user_id = :user_id
        ''', {'user_id': user_id})

    async def increment_posts_count(self, user_id: UUID) -> None:
        await self.execute('''
            UPDATE user_stats SET total_posts = COALESCE(total_posts, 0) + 1 WHERE user_id = :user_id
        ''', {'user_id': user_id})

    async def check_request_exists(self, sender_id: UUID, receiver_id: UUID) -> dict | None:
        return await self.fetch_one('''
            SELECT * FROM friend_requests 
            WHERE (sender_id = :sender_id AND receiver_id = :receiver_id)
            OR (sender_id = :receiver_id AND receiver_id = :sender_id)
        ''', {'sender_id': sender_id, 'receiver_id': receiver_id})

    async def create_post(self, user_id: UUID, data: dict) -> dict:
        post = await self.execute_returning('''
            INSERT INTO posts (user_id, title, body, content_id, media_urls)
            VALUES (:user_id, :title, :body, :content_id, :media_urls)
            RETURNING *
        ''', {
            'user_id': user_id, 
            'title': data.get('title'),
            'body': data.get('body'),
            'content_id': data.get('content_id'),
            'media_urls': data.get('media_urls', [])
        })
        if post:
            await self.increment_posts_count(user_id)
        return post

    async def get_post(self, post_id: UUID) -> dict | None:
        return await self.fetch_one('''
            SELECT p.*, pr.username, pr.avatar_url, pr.is_verified
            FROM posts p
            JOIN profiles pr ON pr.id = p.user_id
            WHERE p.id = :id
        ''', {'id': post_id})

    async def get_posts(self, limit: int = 20, offset: int = 0, viewer_id: UUID | None = None) -> list[dict]:
        where_clause = ""
        params: dict[str, Any] = {'limit': limit, 'offset': offset}
        if viewer_id:
            where_clause = '''
            WHERE p.user_id NOT IN (
                SELECT muted_id FROM muted_users WHERE muter_id = :vid
            ) AND p.user_id NOT IN (
                SELECT blocked_id FROM blocked_users WHERE blocker_id = :vid
            ) AND p.user_id NOT IN (
                SELECT blocker_id FROM blocked_users WHERE blocked_id = :vid
            )
            '''
            params['vid'] = viewer_id

        return await self.fetch_many(f'''
            SELECT p.*, pr.username, pr.avatar_url, pr.is_verified
            FROM posts p
            JOIN profiles pr ON pr.id = p.user_id
            {where_clause}
            ORDER BY p.created_at DESC
            LIMIT :limit OFFSET :offset
        ''', params)

    async def get_posts_by_content(self, content_id: UUID, limit: int = 20, offset: int = 0) -> list[dict]:
        return await self.fetch_many('''
            SELECT p.*, pr.username, pr.avatar_url, pr.is_verified
            FROM posts p
            JOIN profiles pr ON pr.id = p.user_id
            WHERE p.content_id = :content_id
            ORDER BY p.created_at DESC
            LIMIT :limit OFFSET :offset
        ''', {'content_id': content_id, 'limit': limit, 'offset': offset})

    # --- Comments (use specialized tables) ---
    async def create_comment(self, user_id: UUID, content: str, post_id: UUID | None = None, review_id: UUID | None = None, parent_id: UUID | None = None) -> dict:
        """Route to the correct comments table based on the target."""
        if post_id:
            # post_comments: (user_id, post_id, body, parent_id)
            return await self.execute_returning('''
                INSERT INTO post_comments (user_id, post_id, body, parent_id)
                VALUES (:user_id, :post_id, :body, :parent_id)
                RETURNING id, user_id, post_id, body as content, parent_id, created_at, updated_at, is_deleted
            ''', {'user_id': user_id, 'post_id': post_id, 'body': content, 'parent_id': parent_id})
        elif review_id:
            # review_comments: (user_id, review_id, body, parent_id)
            return await self.execute_returning('''
                INSERT INTO review_comments (user_id, review_id, body, parent_id)
                VALUES (:user_id, :review_id, :body, :parent_id)
                RETURNING id, user_id, review_id, body as content, parent_id, created_at, updated_at, is_deleted
            ''', {'user_id': user_id, 'review_id': review_id, 'body': content, 'parent_id': parent_id})
        else:
            raise ValueError("Either post_id or review_id must be provided")

    async def get_comments(self, post_id: UUID | None = None, review_id: UUID | None = None, limit: int = 50, offset: int = 0) -> list[dict]:
        """Fetch comments from the appropriate table."""
        if post_id:
            return await self.fetch_many('''
                SELECT c.id, c.user_id, c.post_id, c.body as content, c.parent_id, c.created_at, c.updated_at, c.is_deleted,
                       p.username, p.avatar_url as user_avatar, p.is_verified
                FROM post_comments c
                JOIN profiles p ON p.id = c.user_id
                WHERE c.post_id = :post_id AND c.is_deleted = false
                ORDER BY c.created_at ASC
                LIMIT :limit OFFSET :offset
            ''', {'post_id': post_id, 'limit': limit, 'offset': offset})
        else:
            return await self.fetch_many('''
                SELECT c.id, c.user_id, c.review_id, c.body as content, c.parent_id, c.created_at, c.updated_at, c.is_deleted,
                       p.username, p.avatar_url as user_avatar, p.is_verified
                FROM review_comments c
                JOIN profiles p ON p.id = c.user_id
                JOIN reviews r ON r.id = c.review_id
                WHERE c.review_id = :review_id AND c.is_deleted = false AND r.is_deleted = false
                ORDER BY c.created_at ASC
                LIMIT :limit OFFSET :offset
            ''', {'review_id': review_id, 'limit': limit, 'offset': offset})

    # --- Upvotes ---
    async def toggle_upvote(self, user_id: UUID, target_id: UUID, target_type: str) -> bool:
        """Toggle upvote in a single atomic operation (one commit)."""
        # Only support post upvotes
        table_map = {
            'post': ('post_upvotes', 'post_id', 'posts'),
        }
        if target_type not in table_map:
            raise ValueError(f"Unsupported upvote target_type: {target_type}")

        table, col, parent_table = table_map[target_type]

        # Single atomic query: attempt delete, if 0 rows affected then insert
        # Use raw db.execute() to batch into one transaction, commit once.
        async with self.db.begin_nested():
            exists = await self.db.execute(
                text(f"SELECT 1 FROM {table} WHERE user_id = :uid AND {col} = :tid"),
                {'uid': user_id, 'tid': target_id}
            )
            row = exists.fetchone()

            if row:
                await self.db.execute(
                    text(f"DELETE FROM {table} WHERE user_id = :uid AND {col} = :tid"),
                    {'uid': user_id, 'tid': target_id}
                )
                await self.db.execute(
                    text(f"UPDATE {parent_table} SET upvotes_count = GREATEST(0, upvotes_count - 1) WHERE id = :tid"),
                    {'tid': target_id}
                )
                result = False
            else:
                await self.db.execute(
                    text(f"INSERT INTO {table} (user_id, {col}) VALUES (:uid, :tid)"),
                    {'uid': user_id, 'tid': target_id}
                )
                await self.db.execute(
                    text(f"UPDATE {parent_table} SET upvotes_count = upvotes_count + 1 WHERE id = :tid"),
                    {'tid': target_id}
                )
                result = True

        await self.db.commit()
        return result

    async def toggle_review_like(self, user_id: UUID, review_id: UUID) -> bool:
        """Toggle like on a review."""
        async with self.db.begin_nested():
            exists = await self.db.execute(
                text("SELECT 1 FROM review_likes WHERE user_id = :uid AND review_id = :rid"),
                {'uid': user_id, 'rid': review_id}
            )
            row = exists.fetchone()

            if row:
                await self.db.execute(
                    text("DELETE FROM review_likes WHERE user_id = :uid AND review_id = :rid"),
                    {'uid': user_id, 'rid': review_id}
                )
                await self.db.execute(
                    text("UPDATE reviews SET likes_count = GREATEST(0, likes_count - 1) WHERE id = :rid"),
                    {'rid': review_id}
                )
                result = False
            else:
                await self.db.execute(
                    text("INSERT INTO review_likes (user_id, review_id) VALUES (:uid, :rid)"),
                    {'uid': user_id, 'rid': review_id}
                )
                await self.db.execute(
                    text("UPDATE reviews SET likes_count = likes_count + 1 WHERE id = :rid"),
                    {'rid': review_id}
                )
                result = True

        await self.db.commit()
        return result

    # --- Reviews (Additional) ---
    async def create_review(self, user_id: UUID, content_id: UUID, star_rating: float, text_review: str | None, is_spoiler: bool) -> dict:
        review = await self.execute_returning('''
            INSERT INTO reviews (user_id, content_id, rating, text_review, is_spoiler)
            VALUES (:user_id, :content_id, :rating, :text_review, :is_spoiler)
            RETURNING id, user_id, content_id, rating as star_rating, text_review, is_spoiler as contains_spoiler, created_at
        ''', {
            'user_id': user_id,
            'content_id': content_id,
            'rating': star_rating,
            'text_review': text_review,
            'is_spoiler': is_spoiler
        })
        if review:
            # Increment both total_reviews and total_posts
            await self.execute('''
                INSERT INTO user_stats (user_id, total_reviews, total_posts)
                VALUES (:uid, 1, 1)
                ON CONFLICT (user_id) DO UPDATE SET
                    total_reviews = COALESCE(user_stats.total_reviews, 0) + 1,
                    total_posts = COALESCE(user_stats.total_posts, 0) + 1,
                    updated_at = now()
            ''', {'uid': user_id})
        return review

    async def update_review(self, review_id: UUID, user_id: UUID, data: dict) -> dict | None:
        allowed_fields = {'star_rating': 'rating', 'text_review': 'text_review', 'contains_spoiler': 'is_spoiler'}
        updates = []
        params = {'id': review_id, 'user_id': user_id}
        
        for k, db_col in allowed_fields.items():
            if k in data and data[k] is not None:
                updates.append(f"{db_col} = :{k}")
                params[k] = data[k]
        
        if not updates:
            return await self.get_review(review_id)
            
        query = f'''
            UPDATE reviews 
            SET {", ".join(updates)}, updated_at = now()
            WHERE id = :id AND user_id = :user_id
            RETURNING id, user_id, content_id, rating as star_rating, text_review, is_spoiler as contains_spoiler, created_at
        '''
        return await self.execute_returning(query, params)

    async def get_review(self, review_id: UUID) -> dict | None:
        return await self.fetch_one('''
            SELECT r.*, pr.username, pr.avatar_url, pr.is_verified
            FROM reviews r
            JOIN profiles pr ON pr.id = r.user_id
            WHERE r.id = :id AND r.is_deleted = false
        ''', {'id': review_id})

    async def get_reviews_by_user(self, user_id: UUID, limit: int = 20, offset: int = 0) -> list[dict]:
        return await self.fetch_many('''
            SELECT r.*, 
                   r.rating as star_rating, 
                   r.is_spoiler as contains_spoiler,
                   pr.username, pr.avatar_url, pr.is_verified,
                   c.title as content_title, 
                   c.poster_url as content_poster
            FROM reviews r
            JOIN profiles pr ON pr.id = r.user_id
            LEFT JOIN content c ON c.id = r.content_id
            WHERE r.user_id = :user_id AND r.is_deleted = false
            ORDER BY r.created_at DESC
            LIMIT :limit OFFSET :offset
        ''', {'user_id': user_id, 'limit': limit, 'offset': offset})

    async def get_trending_reviews(self, limit: int = 5) -> list[dict]:
        return await self.fetch_many('''
            SELECT r.*, 
                   r.rating as star_rating, 
                   r.is_spoiler as contains_spoiler,
                   p.username, p.avatar_url, p.is_verified, 
                   c.title as content_title, 
                   c.poster_url as content_poster
            FROM reviews r
            JOIN profiles p ON p.id = r.user_id
            LEFT JOIN content c ON c.id = r.content_id
            WHERE r.is_deleted = false
            ORDER BY (r.likes_count * 2 + r.comments_count) DESC, r.created_at DESC
            LIMIT :limit
        ''', {'limit': limit})

    async def get_reviews_by_content(self, content_id: UUID, limit: int = 20, offset: int = 0) -> list[dict]:
        return await self.fetch_many('''
            SELECT r.*, 
                   r.rating as star_rating, 
                   r.is_spoiler as contains_spoiler,
                   pr.username, pr.avatar_url, pr.is_verified,
                   c.title as content_title, 
                   c.poster_url as content_poster
            FROM reviews r
            JOIN profiles pr ON pr.id = r.user_id
            LEFT JOIN content c ON c.id = r.content_id
            WHERE r.content_id = :content_id AND r.is_deleted = false
            ORDER BY r.created_at DESC
            LIMIT :limit OFFSET :offset
        ''', {'content_id': content_id, 'limit': limit, 'offset': offset})

    async def mute_user(self, user_id: UUID, target_id: UUID) -> None:
        await self.execute('''
            INSERT INTO muted_users (muter_id, muted_id)
            VALUES (:user_id, :target_id)
            ON CONFLICT DO NOTHING
        ''', {'user_id': user_id, 'target_id': target_id})

    async def unmute_user(self, user_id: UUID, target_id: UUID) -> None:
        await self.execute('''
            DELETE FROM muted_users
            WHERE muter_id = :user_id AND muted_id = :target_id
        ''', {'user_id': user_id, 'target_id': target_id})

    async def block_user(self, user_id: UUID, target_id: UUID) -> None:
        await self.execute('''
            INSERT INTO blocked_users (blocker_id, blocked_id)
            VALUES (:user_id, :target_id)
            ON CONFLICT DO NOTHING
        ''', {'user_id': user_id, 'target_id': target_id})
        # If blocked, remove from friends
        u1, u2 = sorted([user_id, target_id])
        await self.execute('''
            DELETE FROM friends WHERE user_id1 = :u1 AND user_id2 = :u2
        ''', {'u1': u1, 'u2': u2})
        # And delete any pending friend requests
        await self.execute('''
            DELETE FROM friend_requests
            WHERE (sender_id = :user_id AND receiver_id = :target_id)
               OR (sender_id = :target_id AND receiver_id = :user_id)
        ''', {'user_id': user_id, 'target_id': target_id})

    async def unblock_user(self, user_id: UUID, target_id: UUID) -> None:
        await self.execute('''
            DELETE FROM blocked_users
            WHERE blocker_id = :user_id AND blocked_id = :target_id
        ''', {'user_id': user_id, 'target_id': target_id})
