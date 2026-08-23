import asyncio
from sqlalchemy import text
from app.core.database import AsyncSessionLocal
from app.core.logger import get_logger

logger = get_logger('mambo.db_all_indexes')

INDEXES = [
    ("idx_profiles_username", "CREATE INDEX IF NOT EXISTS idx_profiles_username ON profiles (username);"),
    ("idx_profiles_is_deleted", "CREATE INDEX IF NOT EXISTS idx_profiles_is_deleted ON profiles (is_deleted);"),
    ("idx_watch_history_user_date", "CREATE INDEX IF NOT EXISTS idx_watch_history_user_date ON watch_history (user_id, watched_at);"),
    ("idx_watch_history_content", "CREATE INDEX IF NOT EXISTS idx_watch_history_content ON watch_history (content_id);"),
    ("idx_reviews_user_deleted", "CREATE INDEX IF NOT EXISTS idx_reviews_user_deleted ON reviews (user_id, is_deleted);"),
    ("idx_reviews_content_deleted", "CREATE INDEX IF NOT EXISTS idx_reviews_content_deleted ON reviews (content_id, is_deleted);"),
    ("idx_collections_user_id", "CREATE INDEX IF NOT EXISTS idx_collections_user_id ON collections (user_id);"),
    ("idx_collection_items_cid", "CREATE INDEX IF NOT EXISTS idx_collection_items_cid ON collection_items (collection_id);"),
    ("idx_collection_items_content", "CREATE INDEX IF NOT EXISTS idx_collection_items_content ON collection_items (content_id);"),
    ("idx_friends_users", "CREATE INDEX IF NOT EXISTS idx_friends_users ON friends (user_id1, user_id2);"),
    ("idx_user_stats_uid", "CREATE INDEX IF NOT EXISTS idx_user_stats_uid ON user_stats (user_id);"),
]

async def apply_all():
    logger.info("Applying full suite of database performance indexes...")
    async with AsyncSessionLocal() as db:
        for idx_name, sql in INDEXES:
            try:
                logger.info(f"Creating index {idx_name}...")
                await db.execute(text(sql))
                await db.commit()
                logger.info(f"Successfully created index {idx_name}")
            except Exception as e:
                logger.warning(f"Notice creating {idx_name}: {e}")
                try: await db.rollback()
                except Exception: pass

if __name__ == '__main__':
    asyncio.run(apply_all())
