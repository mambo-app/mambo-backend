from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from app.core.config import settings

DATABASE_URL = settings.database_pool_url.replace(
    'postgresql://', 'postgresql+asyncpg://'
).replace('sslmode=require', 'ssl=require').replace('&channel_binding=require', '').replace('?channel_binding=require', '?')

engine = create_async_engine(
    DATABASE_URL,
    pool_size=10,
    max_overflow=5,
    pool_timeout=30,
    pool_pre_ping=True,
    pool_recycle=1800,
    echo=False,
    # Disable prepared statement caching to prevent stale plan errors
    # after ALTER TABLE schema changes (InvalidCachedStatementError)
    connect_args={"statement_cache_size": 0},
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
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()