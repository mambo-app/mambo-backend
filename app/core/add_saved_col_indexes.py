import asyncio
from sqlalchemy import text
from app.core.database import AsyncSessionLocal

async def add_sc_indexes():
    async with AsyncSessionLocal() as db:
        await db.execute(text("CREATE INDEX IF NOT EXISTS idx_sc_user_col ON saved_collections (user_id, collection_id);"))
        await db.execute(text("CREATE INDEX IF NOT EXISTS idx_sc_col_user ON saved_collections (collection_id, user_id);"))
        await db.commit()
        print("Successfully created saved_collections indexes!")

if __name__ == '__main__':
    asyncio.run(add_sc_indexes())
