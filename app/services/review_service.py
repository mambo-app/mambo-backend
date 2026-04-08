from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text
from app.core.exceptions import NotFoundError, ForbiddenError

class ReviewService:
    def __init__(self, db: AsyncSession):
        self.db = db

    async def get_by_content(self, content_id: str,
                              limit: int, offset: int) -> tuple[list, int]:
        rows = await self.db.execute(text('''
            SELECT r.*, p.username, p.avatar_url
            FROM reviews r
            JOIN profiles p ON p.id = r.user_id
            WHERE r.content_id = :content_id
            AND r.is_deleted = false
            ORDER BY r.created_at DESC
            LIMIT :limit OFFSET :offset
        '''), {'content_id': content_id, 'limit': limit, 'offset': offset})

        count = await self.db.execute(text('''
            SELECT COUNT(*) FROM reviews
            WHERE content_id = :content_id AND is_deleted = false
        '''), {'content_id': content_id})

        return [dict(r) for r in rows.mappings()], count.scalar()

    async def update_review(self, review_id: str, user_id: str, data: dict) -> dict:
        from app.repositories.social_repo import SocialRepository
        repo = SocialRepository(self.db)
        from uuid import UUID
        res = await repo.update_review(UUID(review_id), UUID(user_id), data)
        if not res:
            raise ForbiddenError('Review not found or not yours.')
        await self.db.commit()
        return res

    async def delete_review(self, review_id: str, user_id: str) -> None:
        result = await self.db.execute(text('''
            UPDATE reviews
            SET is_deleted = true, deleted_at = now()
            WHERE id = :review_id AND user_id = :user_id
            RETURNING id, user_id
        '''), {'review_id': review_id, 'user_id': user_id})
        
        row = result.mappings().first()
        if not row:
            await self.db.commit()
            raise ForbiddenError('Review not found or not yours.')
            
        # Update stats
        await self.db.execute(text('''
            UPDATE user_stats
            SET total_reviews = GREATEST(0, COALESCE(total_reviews, 0) - 1),
                total_posts = GREATEST(0, COALESCE(total_posts, 0) - 1),
                updated_at = now()
            WHERE user_id = :uid
        '''), {'uid': row['user_id']})
        
        await self.db.commit()