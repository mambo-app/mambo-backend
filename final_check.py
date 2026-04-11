
import asyncio
from sqlalchemy import text
from app.core.database import AsyncSessionLocal

async def check_db():
    async with AsyncSessionLocal() as db:
        print("\n--- DB Counts ---")
        tables = ['content', 'persons', 'profiles', 'trending_content', 'landing_posters']
        for table in tables:
            try:
                res = await db.execute(text(f"SELECT COUNT(*) FROM {table}"))
                print(f"{table}: {res.scalar()}")
            except Exception as e:
                print(f"{table}: Error {e}")
        
        print("\n--- Recent Logs ---")
        try:
            res = await db.execute(text("SELECT id, title, last_synced_at FROM content ORDER BY last_synced_at DESC LIMIT 1"))
            row = res.mappings().one_or_none()
            if row:
                print(f"Latest Content: {row['title']} (Synced: {row['last_synced_at']})")
        except Exception as e:
            print(f"Error checking content: {e}")

if __name__ == "__main__":
    asyncio.run(check_db())
