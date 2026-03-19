from .base import BaseRepository
from uuid import UUID
from sqlalchemy import text

class UserRepository(BaseRepository):

    async def get_by_id(self, user_id: str) -> dict | None:
        return await self.fetch_one('''
            SELECT p.*, us.total_watched, us.total_reviews,
                   us.total_posts, us.followers_count, 
                   us.following_count, us.friends_count
            FROM profiles p
            LEFT JOIN user_stats us ON us.user_id = p.id
            WHERE p.id = CAST(:user_id AS UUID) AND p.is_deleted = false
        ''', {'user_id': user_id})

    async def get_by_username(self, username: str) -> dict | None:
        return await self.fetch_one('''
            SELECT p.*, us.total_watched, us.total_reviews,
                   us.total_posts, us.followers_count, 
                   us.following_count, us.friends_count
            FROM profiles p
            LEFT JOIN user_stats us ON us.user_id = p.id
            WHERE p.username = :username AND p.is_deleted = false
        ''', {'username': username})

    async def update_profile(self, user_id: str, fields: dict) -> dict:
        set_clause = ', '.join([f'{k} = :{k}' for k in fields.keys()])
        fields['user_id'] = user_id
        return await self.execute_returning(f'''
            UPDATE profiles
            SET {set_clause}, updated_at = now()
            WHERE id = CAST(:user_id AS UUID)
            RETURNING *
        ''', fields)

    async def search(self, query: str, limit: int, offset: int) -> list[dict]:
        return await self.fetch_many('''
            SELECT id, username, display_name, avatar_url, is_verified
            FROM profiles
            WHERE search_vector @@ plainto_tsquery('simple', :query)
            AND is_deleted = false
            ORDER BY ts_rank(search_vector, plainto_tsquery('simple', :query)) DESC
            LIMIT :limit OFFSET :offset
        ''', {'query': query, 'limit': limit, 'offset': offset})

    async def follow(self, follower_id: str, following_id: str) -> None:
        # 1. Insert follow relationship
        res = await self.db.execute(text('''
            INSERT INTO follows (follower_id, following_id)
            VALUES (CAST(:follower_id AS UUID), CAST(:following_id AS UUID))
            ON CONFLICT DO NOTHING
            RETURNING id
        '''), {'follower_id': follower_id, 'following_id': following_id})
        
        # 2. Only update stats if the follow was actually created (not already following)
        if res.rowcount > 0:
            # Increment following for the follower
            await self.execute('''
                INSERT INTO user_stats (user_id, following_count)
                VALUES (CAST(:uid AS UUID), 1)
                ON CONFLICT (user_id) DO UPDATE SET
                    following_count = COALESCE(user_stats.following_count, 0) + 1,
                    updated_at = now()
            ''', {'uid': follower_id})
            
            # Increment followers for the target
            await self.execute('''
                INSERT INTO user_stats (user_id, followers_count)
                VALUES (CAST(:uid AS UUID), 1)
                ON CONFLICT (user_id) DO UPDATE SET
                    followers_count = COALESCE(user_stats.followers_count, 0) + 1,
                    updated_at = now()
            ''', {'uid': following_id})
            await self.db.commit()

    async def unfollow(self, follower_id: str, following_id: str) -> None:
        # 1. Delete follow relationship
        res = await self.db.execute(text('''
            DELETE FROM follows
            WHERE follower_id = CAST(:follower_id AS UUID)
            AND following_id = CAST(:following_id AS UUID)
            RETURNING id
        '''), {'follower_id': follower_id, 'following_id': following_id})
        
        # 2. Only update stats if a row was actually deleted
        if res.rowcount > 0:
            # Decrement following for the ex-follower
            await self.execute('''
                UPDATE user_stats 
                SET following_count = GREATEST(0, COALESCE(following_count, 0) - 1),
                    updated_at = now()
                WHERE user_id = CAST(:uid AS UUID)
            ''', {'uid': follower_id})
            
            # Decrement followers for the ex-target
            await self.execute('''
                UPDATE user_stats 
                SET followers_count = GREATEST(0, COALESCE(followers_count, 0) - 1),
                    updated_at = now()
                WHERE user_id = CAST(:uid AS UUID)
            ''', {'uid': following_id})
            await self.db.commit()

    async def get_followers(self, user_id: str,
                             limit: int, offset: int) -> list[dict]:
        return await self.fetch_many('''
            SELECT p.id, p.username, p.display_name,
                   p.avatar_url, p.is_verified
            FROM follows f
            JOIN profiles p ON p.id = f.follower_id
            WHERE f.following_id = CAST(:user_id AS UUID)
            AND p.is_deleted = false
            ORDER BY f.created_at DESC
            LIMIT :limit OFFSET :offset
        ''', {'user_id': user_id, 'limit': limit, 'offset': offset})

    async def get_following(self, user_id: str,
                             limit: int, offset: int) -> list[dict]:
        return await self.fetch_many('''
            SELECT p.id, p.username, p.display_name,
                   p.avatar_url, p.is_verified
            FROM follows f
            JOIN profiles p ON p.id = f.following_id
            WHERE f.follower_id = CAST(:user_id AS UUID)
            AND p.is_deleted = false
            ORDER BY f.created_at DESC
            LIMIT :limit OFFSET :offset
        ''', {'user_id': user_id, 'limit': limit, 'offset': offset})

    async def is_following(self, follower_id: str,
                            following_id: str) -> bool:
        result = await self.fetch_one('''
            SELECT id FROM follows
            WHERE follower_id = CAST(:follower_id AS UUID)
            AND following_id = CAST(:following_id AS UUID)
        ''', {'follower_id': follower_id, 'following_id': following_id})
        return result is not None

    async def update_privacy(self, user_id: str, fields: dict) -> dict:
        set_clause = ', '.join([f'{k} = :{k}' for k in fields.keys()])
        fields['user_id'] = user_id
        return await self.execute_returning(f'''
            UPDATE profiles
            SET {set_clause}, updated_at = now()
            WHERE id = CAST(:user_id AS UUID)
            RETURNING *
        ''', fields)

    async def set_favorite_genres(self, user_id: str, genres: list[str]) -> None:
        await self.execute('DELETE FROM user_favorite_genres WHERE user_id = CAST(:user_id AS UUID)', {'user_id': user_id})
        if genres:
            for genre in genres:
                await self.execute('''
                    INSERT INTO user_favorite_genres (user_id, genre_name)
                    VALUES (:user_id, :genre)
                    ON CONFLICT DO NOTHING
                ''', {'user_id': user_id, 'genre': genre})

    async def get_favorite_genres(self, user_id: str) -> list[str]:
        rows = await self.fetch_many('SELECT genre_name FROM user_favorite_genres WHERE user_id = CAST(:user_id AS UUID)', {'user_id': user_id})
        return [row['genre_name'] for row in rows]

    async def get_trending_creators(self, limit: int = 10) -> list[dict]:
        return await self.fetch_many('''
            SELECT p.id, p.username, p.display_name, p.avatar_url, p.is_verified,
                   COUNT(f.id) as recent_followers
            FROM profiles p
            JOIN follows f ON f.following_id = p.id
            WHERE f.created_at > now() - interval '7 days'
            AND p.is_deleted = false
            GROUP BY p.id
            ORDER BY recent_followers DESC
            LIMIT :limit
        ''', {'limit': limit})

    async def delete_account(self, user_id: str) -> None:
        await self.execute('DELETE FROM profiles WHERE id = :user_id', {'user_id': user_id})