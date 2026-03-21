import asyncio
from app.core.database import AsyncSessionLocal
from sqlalchemy import text

async def main():
    async with AsyncSessionLocal() as db:
        res = await db.execute(text('SELECT id, title, content_type FROM content LIMIT 10'))
        print(res.mappings().all())

asyncio.run(main())
