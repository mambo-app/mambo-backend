from .base import BaseRepository

class FeedRepository(BaseRepository):

    async def get_feed(self, user_id: str,
                       limit: int, offset: int) -> list[dict]:
        return await self.fetch_many('''
            SELECT f.*,
                   p.username as actor_username,
                   p.avatar_url as actor_avatar,
                   CASE
                       WHEN f.review_id IS NOT NULL THEN r.star_rating
                       ELSE NULL
                   END as star_rating,
                   CASE
                       WHEN f.review_id IS NOT NULL THEN r.text_review
                       WHEN f.post_id IS NOT NULL THEN po.body
                       ELSE NULL
                   END as content_body
            FROM feeds f
            JOIN profiles p ON p.id = f.actor_id
            LEFT JOIN reviews r ON r.id = f.review_id
            LEFT JOIN posts po ON po.id = f.post_id
            WHERE f.user_id = :user_id
            ORDER BY f.rank_score DESC, f.created_at DESC
            LIMIT :limit OFFSET :offset
        ''', {'user_id': user_id,
               'limit': limit, 'offset': offset})

    async def count_feed(self, user_id: str) -> int:
        from sqlalchemy import text
        result = await self.db.execute(text('''
            SELECT COUNT(*) FROM feeds WHERE user_id = :user_id
        '''), {'user_id': user_id})
        return result.scalar()

    async def add_to_feed(self, user_id: str, actor_id: str,
                           review_id: str = None,
                           post_id: str = None) -> None:
        if review_id:
            await self.execute('''
                INSERT INTO feeds (user_id, review_id, actor_id)
                VALUES (:user_id, :review_id, :actor_id)
                ON CONFLICT DO NOTHING
            ''', {'user_id': user_id,
                   'review_id': review_id,
                   'actor_id': actor_id})
        elif post_id:
            await self.execute('''
                INSERT INTO feeds (user_id, post_id, actor_id)
                VALUES (:user_id, :post_id, :actor_id)
                ON CONFLICT DO NOTHING
            ''', {'user_id': user_id,
                   'post_id': post_id,
                   'actor_id': actor_id})

    async def remove_from_feed(self, review_id: str = None,
                                post_id: str = None) -> None:
        if review_id:
            await self.execute('''
                DELETE FROM feeds WHERE review_id = :review_id
            ''', {'review_id': review_id})
        elif post_id:
            await self.execute('''
                DELETE FROM feeds WHERE post_id = :post_id
            ''', {'post_id': post_id})