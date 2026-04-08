import asyncio
import os
import sys

sys.path.append(os.getcwd())
from app.core.database import AsyncSessionLocal
from sqlalchemy import text

async def cleanup_activity():
    async with AsyncSessionLocal() as db:
        print("Cleaning up duplicate activity log recommendations...")
        query = text("""
            DELETE FROM activity_log
            WHERE id IN (
                SELECT id
                FROM (
                    SELECT id,
                           ROW_NUMBER() OVER(
                               PARTITION BY user_id, content_id 
                               ORDER BY created_at DESC
                           ) as rn
                    FROM activity_log
                    WHERE activity_type = 'receive_recommendation'
                ) t
                WHERE t.rn > 1
            )
            RETURNING id;
        """)
        
        result = await db.execute(query)
        deleted_ids = result.scalars().all()
        await db.commit()
        print(f"Deleted {len(deleted_ids)} duplicate activity_log records.")

if __name__ == '__main__':
    asyncio.run(cleanup_activity())
