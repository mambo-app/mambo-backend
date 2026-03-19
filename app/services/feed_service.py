from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
import logging

logger = logging.getLogger('mambo.feed')

class FeedService:
    def __init__(self, db: AsyncSession):
        self.db = db

    async def get_feed(self, user_id: str, limit: int, offset: int) -> tuple[list, int]:
        where_clause = '''
            WHERE f.user_id = :user_id 
            AND f.actor_id NOT IN (SELECT muted_id FROM muted_users WHERE muter_id = :uid::uuid)
            AND f.actor_id NOT IN (SELECT blocked_id FROM blocked_users WHERE blocker_id = :uid::uuid)
            AND f.actor_id NOT IN (SELECT blocker_id FROM blocked_users WHERE blocked_id = :uid::uuid)
        '''
        
        rows = await self.db.execute(text(f'''
            SELECT f.*, 
                   p.username as actor_username,
                   p.avatar_url as actor_avatar
            FROM feeds f
            JOIN profiles p ON p.id = f.actor_id
            {where_clause}
            ORDER BY f.rank_score DESC, f.created_at DESC
            LIMIT :limit OFFSET :offset
        '''), {'user_id': user_id, 'uid': user_id, 'limit': limit, 'offset': offset})

        count = await self.db.execute(text(f'''
            SELECT COUNT(*) FROM feeds f {where_clause}
        '''), {'user_id': user_id, 'uid': user_id})

        return [dict(r) for r in rows.mappings()], count.scalar()

    async def fan_out_review(self, review_id: str, actor_id: str) -> None:
        try:
            await self.db.execute(text('''
                INSERT INTO feeds (user_id, review_id, actor_id, rank_score)
                SELECT f.follower_id, :review_id, :actor_id, 1.0
                FROM follows f
                WHERE f.following_id = :actor_id
                ON CONFLICT DO NOTHING
            '''), {'review_id': review_id, 'actor_id': actor_id})
            await self.db.commit()
        except Exception as e:
            logger.error(f'Fan out failed for review {review_id}: {e}')

    async def fan_out_post(self, post_id: str, actor_id: str) -> None:
        try:
            await self.db.execute(text('''
                INSERT INTO feeds (user_id, post_id, actor_id, rank_score)
                SELECT f.follower_id, :post_id, :actor_id, 1.0
                FROM follows f
                WHERE f.following_id = :actor_id
                ON CONFLICT DO NOTHING
            '''), {'post_id': post_id, 'actor_id': actor_id})
            await self.db.commit()
        except Exception as e:
            logger.error(f'Fan out failed for post {post_id}: {e}')