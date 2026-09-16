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
        from uuid import UUID
        try:
            r_uuid = UUID(review_id) if isinstance(review_id, str) else review_id
            u_uuid = UUID(user_id) if isinstance(user_id, str) else user_id
        except Exception:
            raise ForbiddenError('Invalid review ID format')

        # Fetch review info before deleting (need content_id)
        review_row = (await self.db.execute(text('''
            SELECT content_id, user_id FROM reviews WHERE id = :rid
        '''), {'rid': r_uuid})).mappings().first()

        if not review_row:
            raise NotFoundError('Review not found')

        actual_user_id = review_row['user_id']
        content_id = review_row['content_id']

        if str(actual_user_id) != str(u_uuid):
            raise ForbiddenError('Not authorized to delete this review')

        # Clear review_id link in watch_history if present (Option A: retains watch event and score)
        await self.db.execute(text('''
            UPDATE watch_history SET review_id = NULL WHERE review_id = :rid
        '''), {'rid': r_uuid})

        # Hard delete from reviews table
        await self.db.execute(text('''
            DELETE FROM reviews WHERE id = :rid AND user_id = :uid
        '''), {'rid': r_uuid, 'uid': actual_user_id})

        # Clean up activity log rows for ONLY this specific review
        await self.db.execute(text('''
            DELETE FROM activity_log 
            WHERE review_id = :rid 
               OR (user_id = :uid AND content_id = :cid AND activity_type IN ('reviewed', 'updated_review'))
        '''), {'rid': r_uuid, 'uid': actual_user_id, 'cid': content_id})

        # Set user_content_status.rating to latest watch_history rating if present (Option A)
        latest_r = (await self.db.execute(text('''
            SELECT rating FROM watch_history 
            WHERE user_id = :uid AND content_id = :cid AND rating IS NOT NULL 
            ORDER BY watched_at DESC LIMIT 1
        '''), {'uid': actual_user_id, 'cid': content_id})).scalar()

        await self.db.execute(text('''
            UPDATE user_content_status
            SET rating = :r, updated_at = now()
            WHERE user_id = :uid AND content_id = :cid
        '''), {'r': latest_r, 'uid': actual_user_id, 'cid': content_id})

        # Update stats
        await self.db.execute(text('''
            UPDATE user_stats
            SET total_reviews = GREATEST(0, COALESCE(total_reviews, 0) - 1),
                updated_at = now()
            WHERE user_id = :uid
        '''), {'uid': actual_user_id})
        
        await self.db.commit()