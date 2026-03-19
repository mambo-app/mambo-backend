from .base import BaseRepository

class ReviewRepository(BaseRepository):

    async def create(self, user_id: str, content_id: str,
                     star_rating: int, text_review: str | None,
                     contains_spoiler: bool, tags: list) -> dict:
        from sqlalchemy import text as _text
        review = await self.execute_returning('''
            INSERT INTO reviews
                (user_id, content_id, star_rating,
                 text_review, contains_spoiler, tags)
            VALUES
                (:user_id, :content_id, :star_rating,
                 :text_review, :contains_spoiler, :tags)
            ON CONFLICT (user_id, content_id)
            DO UPDATE SET
                star_rating = EXCLUDED.star_rating,
                text_review = EXCLUDED.text_review,
                contains_spoiler = EXCLUDED.contains_spoiler,
                tags = EXCLUDED.tags,
                updated_at = now()
            RETURNING *, (xmax = 0) AS is_insert
        ''', {
            'user_id': user_id,
            'content_id': content_id,
            'star_rating': star_rating,
            'text_review': text_review,
            'contains_spoiler': contains_spoiler,
            'tags': tags,
        })

        # Only increment total_reviews for genuinely new reviews, not updates
        if review.get('is_insert'):
            await self.db.execute(_text('''
                INSERT INTO user_stats (user_id, total_reviews)
                VALUES (:user_id, 1)
                ON CONFLICT (user_id) DO UPDATE SET
                    total_reviews = COALESCE(user_stats.total_reviews, 0) + 1,
                    updated_at = now()
            '''), {'user_id': user_id})
            await self.db.commit()

        return review

    async def get_by_id(self, review_id: str) -> dict | None:
        return await self.fetch_one('''
            SELECT r.*, p.username, p.avatar_url
            FROM reviews r
            JOIN profiles p ON p.id = r.user_id
            WHERE r.id = :review_id AND r.is_deleted = false
        ''', {'review_id': review_id})

    async def get_by_user_and_content(self, user_id: str,
                                       content_id: str) -> dict | None:
        return await self.fetch_one('''
            SELECT * FROM reviews
            WHERE user_id = :user_id
            AND content_id = :content_id
            AND is_deleted = false
        ''', {'user_id': user_id, 'content_id': content_id})

    async def get_by_content(self, content_id: str,
                              limit: int, offset: int) -> list[dict]:
        return await self.fetch_many('''
            SELECT r.*, p.username, p.avatar_url
            FROM reviews r
            JOIN profiles p ON p.id = r.user_id
            WHERE r.content_id = :content_id
            AND r.is_deleted = false
            ORDER BY r.created_at DESC
            LIMIT :limit OFFSET :offset
        ''', {'content_id': content_id,
               'limit': limit, 'offset': offset})

    async def get_by_user(self, user_id: str,
                           limit: int, offset: int) -> list[dict]:
        return await self.fetch_many('''
            SELECT r.*, c.title as content_title,
                   c.poster_url, c.content_type
            FROM reviews r
            JOIN content c ON c.id = r.content_id
            WHERE r.user_id = :user_id
            AND r.is_deleted = false
            ORDER BY r.created_at DESC
            LIMIT :limit OFFSET :offset
        ''', {'user_id': user_id,
               'limit': limit, 'offset': offset})

    async def soft_delete(self, review_id: str, user_id: str) -> bool:
        from sqlalchemy import text
        result = await self.db.execute(text('''
            UPDATE reviews
            SET is_deleted = true, deleted_at = now()
            WHERE id = :review_id AND user_id = :user_id
            RETURNING id
        '''), {'review_id': review_id, 'user_id': user_id})
        await self.db.commit()
        return result.rowcount > 0

    async def like(self, review_id: str, user_id: str) -> None:
        await self.execute('''
            INSERT INTO review_likes (review_id, user_id)
            VALUES (:review_id, :user_id)
            ON CONFLICT DO NOTHING
        ''', {'review_id': review_id, 'user_id': user_id})

    async def unlike(self, review_id: str, user_id: str) -> None:
        await self.execute('''
            DELETE FROM review_likes
            WHERE review_id = :review_id AND user_id = :user_id
        ''', {'review_id': review_id, 'user_id': user_id})

    async def save(self, review_id: str, user_id: str) -> None:
        await self.execute('''
            INSERT INTO review_saves (review_id, user_id)
            VALUES (:review_id, :user_id)
            ON CONFLICT DO NOTHING
        ''', {'review_id': review_id, 'user_id': user_id})

    async def unsave(self, review_id: str, user_id: str) -> None:
        await self.execute('''
            DELETE FROM review_saves
            WHERE review_id = :review_id AND user_id = :user_id
        ''', {'review_id': review_id, 'user_id': user_id})

    async def count_by_content(self, content_id: str) -> int:
        from sqlalchemy import text
        result = await self.db.execute(text('''
            SELECT COUNT(*) FROM reviews
            WHERE content_id = :content_id AND is_deleted = false
        '''), {'content_id': content_id})
        return result.scalar()