import asyncio
import os
import sys

sys.path.append(os.getcwd())
from app.core.database import AsyncSessionLocal
from sqlalchemy import text

async def cleanup_duplicates():
    async with AsyncSessionLocal() as db:
        print("Cleaning up duplicate friends recommendations...")
        query = text("""
            DELETE FROM recommendation_recipients
            WHERE recommendation_id IN (
                SELECT recommendation_id
                FROM (
                    SELECT rr.recommendation_id,
                           ROW_NUMBER() OVER(
                               PARTITION BY rr.recipient_id, r.content_id 
                               ORDER BY r.sent_at DESC, rr.recommendation_id DESC
                           ) as rn
                    FROM recommendation_recipients rr
                    JOIN recommendations r ON r.id = rr.recommendation_id
                ) t
                WHERE t.rn > 1
            )
            RETURNING recommendation_id;
        """)
        
        result = await db.execute(query)
        deleted_ids = result.scalars().all()
        await db.commit()
        print(f"Deleted {len(deleted_ids)} duplicate recommendation recipient records.")

if __name__ == '__main__':
    asyncio.run(cleanup_duplicates())
