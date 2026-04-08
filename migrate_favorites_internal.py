import asyncio
from sqlalchemy import text
from app.core.database import engine

async def run_migration():
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

if __name__ == "__main__":
    asyncio.run(run_migration())
