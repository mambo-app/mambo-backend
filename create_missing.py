import asyncio
from app.core.database import AsyncSessionLocal
from sqlalchemy import text

async def fix():
    async with AsyncSessionLocal() as db:
        # Get all users
        res = await db.execute(text("SELECT id FROM profiles"))
        users = res.fetchall()

        for (uid,) in users:
            # Stats
            await db.execute(text("INSERT INTO user_stats (user_id) VALUES (:id) ON CONFLICT DO NOTHING"), {'id': uid})
            # Privacy
            await db.execute(text("INSERT INTO privacy_settings (user_id) VALUES (:id) ON CONFLICT DO NOTHING"), {'id': uid})
            
            # Watchlist
            await db.execute(text("""
                INSERT INTO collections (user_id, name, description, is_public, collection_type, is_default, is_deletable, is_pinned, pin_order)
                VALUES (:uid, 'Watchlist', 'My watchlist of movies and shows', false, 'system', true, false, true, 1)
                ON CONFLICT DO NOTHING
            """), {'uid': uid})
            
            # Dropped
            await db.execute(text("""
                INSERT INTO collections (user_id, name, description, is_public, collection_type, is_default, is_deletable, is_pinned, pin_order)
                VALUES (:uid, 'Dropped', 'Content I stopped watching', false, 'system', true, false, true, 2)
                ON CONFLICT DO NOTHING
            """), {'uid': uid})
            
        await db.commit()
    print("All missing related tables (stats, privacy, collections) created.")

import sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
asyncio.run(fix())
