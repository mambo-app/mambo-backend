import asyncio
from sqlalchemy import text
from app.core.database import AsyncSessionLocal

async def add_pin_indexes():
    async with AsyncSessionLocal() as db:
        await db.execute(text("CREATE INDEX IF NOT EXISTS idx_collections_user_pin ON collections (user_id, pin_order ASC NULLS LAST, created_at DESC);"))
        await db.execute(text("CREATE INDEX IF NOT EXISTS idx_collections_vis ON collections (visibility);"))
        await db.commit()
        print("Successfully created collection pin & visibility indexes!")

if __name__ == '__main__':
    asyncio.run(add_pin_indexes())
