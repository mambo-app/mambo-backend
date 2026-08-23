from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from app.core.config import settings
import asyncio
import logging

logger = logging.getLogger("mambo.database")

DATABASE_URL = settings.database_pool_url.replace(
    'postgresql://', 'postgresql+asyncpg://'
).replace('sslmode=require', 'ssl=require').replace('&channel_binding=require', '').replace('?channel_binding=require', '?')

engine = create_async_engine(
    DATABASE_URL,
    pool_size=30,
    max_overflow=30,
    pool_timeout=15,
    pool_pre_ping=True, # Stabilize connection health
    pool_recycle=300,   # Recycle connections every 5 minutes
    echo=False,
    connect_args={
        "statement_cache_size": 0,
        "prepared_statement_cache_size": 0,
        "timeout": 30,
        "command_timeout": 30,
    },
)

AsyncSessionLocal = async_sessionmaker(
    engine,
    class_=AsyncSession,
    expire_on_commit=False,
)

async def get_db():
    async with AsyncSessionLocal() as session:
        try:
            yield session
        except Exception as e:
            logger.warning(f"Database session exception caught, performing rollback: {e}")
            try:
                await session.rollback()
            except Exception:
                pass
            raise
        finally:
            try:
                await session.close()
            except Exception:
                pass