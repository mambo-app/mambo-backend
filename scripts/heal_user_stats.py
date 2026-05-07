import asyncio
import sys
import os

# Add the project root to the python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import text
from app.core.database import AsyncSessionLocal

async def heal_user_stats():
    print("Connecting to database...")
    async with AsyncSessionLocal() as db:
        # 1. Get all user IDs
        result = await db.execute(text("SELECT id, username FROM profiles WHERE is_deleted = false"))
        users = result.mappings().all()
        
        print(f"Found {len(users)} users. Starting sync...")
        
        for user in users:
            uid = user['id']
            username = user['username']
            
            # Count actual followers
            followers_res = await db.execute(
                text("SELECT COUNT(*) FROM follows WHERE following_id = :uid"),
                {"uid": uid}
            )
            actual_followers = followers_res.scalar() or 0
            
            # Count actual following
            following_res = await db.execute(
                text("SELECT COUNT(*) FROM follows WHERE follower_id = :uid"),
                {"uid": uid}
            )
            actual_following = following_res.scalar() or 0
            
            # Update user_stats
            # We use INSERT ... ON CONFLICT to ensure the row exists
            await db.execute(
                text('''
                    INSERT INTO user_stats (user_id, followers_count, following_count, updated_at)
                    VALUES (:uid, :followers, :following, now())
                    ON CONFLICT (user_id) DO UPDATE SET
                        followers_count = :followers,
                        following_count = :following,
                        updated_at = now()
                '''),
                {"uid": uid, "followers": actual_followers, "following": actual_following}
            )
            
            print(f"Synced @{username}: Followers={actual_followers}, Following={actual_following}")
        
        await db.commit()
        print("Sync complete!")

if __name__ == "__main__":
    asyncio.run(heal_user_stats())
