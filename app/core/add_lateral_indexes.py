import asyncio
from sqlalchemy import text
from app.core.database import AsyncSessionLocal

async def add_lat_indexes():
    async with AsyncSessionLocal() as db:
        await db.execute(text("CREATE INDEX IF NOT EXISTS idx_collection_items_col_added ON collection_items (collection_id, added_at DESC);"))
        await db.execute(text("CREATE INDEX IF NOT EXISTS idx_sc_col_saved ON saved_collections (collection_id, saved_at DESC);"))
        await db.commit()
        print("Successfully created LATERAL join indexes!")

if __name__ == '__main__':
    asyncio.run(add_lat_indexes())
