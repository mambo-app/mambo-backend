import asyncio
from sqlalchemy import text
from app.core.database import AsyncSessionLocal
from app.core.logger import get_logger

logger = get_logger('mambo.db_indexes')

INDEXES = [
    ("idx_content_type_rating", "CREATE INDEX IF NOT EXISTS idx_content_type_rating ON content (content_type, external_rating DESC NULLS LAST);"),
    ("idx_content_type_release", "CREATE INDEX IF NOT EXISTS idx_content_type_release ON content (content_type, release_date DESC NULLS LAST);"),
    ("idx_content_type_status", "CREATE INDEX IF NOT EXISTS idx_content_type_status ON content (content_type, status);"),
    ("idx_content_tmdb_id", "CREATE INDEX IF NOT EXISTS idx_content_tmdb_id ON content (tmdb_id);"),
    ("idx_ucs_user_status", "CREATE INDEX IF NOT EXISTS idx_ucs_user_status ON user_content_status (user_id, status);"),
    ("idx_ucs_user_watched", "CREATE INDEX IF NOT EXISTS idx_ucs_user_watched ON user_content_status (user_id, is_watched);"),
    ("idx_content_credits_lookup", "CREATE INDEX IF NOT EXISTS idx_content_credits_lookup ON content_credits (content_id, person_id);"),
]

async def apply_indexes():
    logger.info("Applying performance database indexes...")
    async with AsyncSessionLocal() as db:
        for idx_name, sql in INDEXES:
            try:
                logger.info(f"Creating index {idx_name}...")
                await db.execute(text(sql))
                await db.commit()
                logger.info(f"Successfully created index {idx_name}")
            except Exception as e:
                logger.warning(f"Index {idx_name} creation notice: {e}")
                try:
                    await db.rollback()
                except Exception:
                    pass

if __name__ == '__main__':
    asyncio.run(apply_indexes())
