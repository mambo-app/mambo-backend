from .base import BaseRepository

class PostRepository(BaseRepository):

    async def create(self, user_id: str, body: str,
                     title: str | None, content_id: str | None,
                     visibility: str) -> dict:
        return await self.execute_returning('''
            INSERT INTO posts
                (user_id, body, title, content_id, visibility)
            VALUES
                (:user_id, :body, :title, :content_id, :visibility)
            RETURNING *
        ''', {
            'user_id': user_id,
            'body': body,
            'title': title,
            'content_id': content_id,
            'visibility': visibility,
        })

    async def get_by_id(self, post_id: str) -> dict | None:
        return await self.fetch_one('''
            SELECT po.*, p.username, p.avatar_url
            FROM posts po
            JOIN profiles p ON p.id = po.user_id
            WHERE po.id = :post_id
            AND po.is_deleted = false
            AND po.is_hidden = false
        ''', {'post_id': post_id})

    async def get_by_user(self, user_id: str,
                           limit: int, offset: int) -> list[dict]:
        return await self.fetch_many('''
            SELECT po.*, p.username, p.avatar_url
            FROM posts po
            JOIN profiles p ON p.id = po.user_id
            WHERE po.user_id = :user_id
            AND po.is_deleted = false
            AND po.is_hidden = false
            ORDER BY po.created_at DESC
            LIMIT :limit OFFSET :offset
        ''', {'user_id': user_id,
               'limit': limit, 'offset': offset})

    async def soft_delete(self, post_id: str, user_id: str) -> bool:
        from sqlalchemy import text
        result = await self.db.execute(text('''
            UPDATE posts
            SET is_deleted = true, deleted_at = now()
            WHERE id = :post_id AND user_id = :user_id
            RETURNING id
        '''), {'post_id': post_id, 'user_id': user_id})
        await self.db.commit()
        return result.rowcount > 0

    async def upvote(self, post_id: str, user_id: str) -> None:
        await self.execute('''
            INSERT INTO post_upvotes (post_id, user_id)
            VALUES (:post_id, :user_id)
            ON CONFLICT DO NOTHING
        ''', {'post_id': post_id, 'user_id': user_id})

    async def remove_upvote(self, post_id: str, user_id: str) -> None:
        await self.execute('''
            DELETE FROM post_upvotes
            WHERE post_id = :post_id AND user_id = :user_id
        ''', {'post_id': post_id, 'user_id': user_id})

    async def save(self, post_id: str, user_id: str) -> None:
        await self.execute('''
            INSERT INTO post_saves (post_id, user_id)
            VALUES (:post_id, :user_id)
            ON CONFLICT DO NOTHING
        ''', {'post_id': post_id, 'user_id': user_id})

    async def unsave(self, post_id: str, user_id: str) -> None:
        await self.execute('''
            DELETE FROM post_saves
            WHERE post_id = :post_id AND user_id = :user_id
        ''', {'post_id': post_id, 'user_id': user_id})