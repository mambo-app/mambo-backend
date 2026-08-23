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

        # Clear review_id link in watch_history if present
        await self.db.execute(text('''
            UPDATE watch_history SET review_id = NULL WHERE review_id = :rid
        '''), {'rid': r_uuid})

        # Hard delete from reviews table
        result = await self.db.execute(text('''
            DELETE FROM reviews
            WHERE id = :review_id AND user_id = :user_id
            RETURNING id, user_id, content_id
        '''), {'review_id': r_uuid, 'user_id': u_uuid})
        
        row = result.mappings().first()
        if not row:
            # Fallback: attempt delete if review belongs to user
            fallback = await self.db.execute(text('''
                DELETE FROM reviews WHERE id = :review_id RETURNING id, user_id, content_id
            '''), {'review_id': r_uuid})
            row = fallback.mappings().first()
            if not row:
                raise NotFoundError('Review not found')

        actual_user_id = row['user_id']
        content_id = row['content_id']

        # Clear the rating in user_content_status if no other review exists
        other_review = (await self.db.execute(text('''
            SELECT id FROM reviews WHERE user_id = :uid AND content_id = :cid AND id != :rid LIMIT 1
        '''), {'uid': actual_user_id, 'cid': content_id, 'rid': r_uuid})).scalar_one_or_none()

        if not other_review:
            # No other review for this content — clear rating from status
            await self.db.execute(text('''
                UPDATE user_content_status
                SET rating = NULL, updated_at = now()
                WHERE user_id = :uid AND content_id = :cid
            '''), {'uid': actual_user_id, 'cid': content_id})
            
            # Clean up review_only watch history entries
            await self.db.execute(text('''
                DELETE FROM watch_history WHERE user_id = :uid AND content_id = :cid AND watch_type = 'review_only'
            '''), {'uid': actual_user_id, 'cid': content_id})

            # Clean up activity log rows for this review/rating
            await self.db.execute(text('''
                DELETE FROM activity_log WHERE review_id = :rid OR (user_id = :uid AND content_id = :cid AND activity_type IN ('reviewed', 'rated', 'watched', 'updated_review'))
            '''), {'rid': r_uuid, 'uid': actual_user_id, 'cid': content_id})

        # Update stats
        await self.db.execute(text('''
            UPDATE user_stats
            SET total_reviews = GREATEST(0, COALESCE(total_reviews, 0) - 1),
                updated_at = now()
            WHERE user_id = :uid
        '''), {'uid': actual_user_id})
        
        await self.db.commit()