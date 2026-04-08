import asyncio
import os
import sys
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

DATABASE_URL = "postgresql+asyncpg://neondb_owner:npg_W5YfRlyT9LhX@ep-crimson-sun-a5sh9kiz-pooler.us-east-2.aws.neon.tech/neondb?ssl=require"

async def run_migration():
    engine = create_async_engine(DATABASE_URL)
    async with engine.begin() as conn:
        try:
            # Check if column exists first to avoid error
            check_sql = text("SELECT column_name FROM information_schema.columns WHERE table_name='user_content_status' AND column_name='favorite_order';")
            res = await conn.execute(check_sql)
            if not res.first():
                print("Adding column 'favorite_order' to 'user_content_status'...")
                await conn.execute(text("ALTER TABLE user_content_status ADD COLUMN favorite_order INTEGER DEFAULT NULL;"))
                print("Migration complete!")
            else:
                print("Column 'favorite_order' already exists.")
        except Exception as e:
            print(f"Migration failed: {e}")
            sys.exit(1)
    await engine.dispose()

if __name__ == "__main__":
    asyncio.run(run_migration())
