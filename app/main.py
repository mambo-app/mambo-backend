import sentry_sdk
from sentry_sdk.integrations.fastapi import FastApiIntegration
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import ORJSONResponse, Response
from contextlib import asynccontextmanager

from app.core.config import settings
from app.core.database import engine
from app.core.exceptions import register_exception_handlers
from app.core.logging import configure_logging
from app.middleware.request_id import RequestIDMiddleware
from app.middleware.timing import TimingMiddleware
from app.middleware.security_headers import SecurityHeadersMiddleware
from app.middleware.rate_limit import RateLimitMiddleware

# Initialize structured logging before anything else
configure_logging(level='DEBUG' if not settings.is_production else 'INFO')

from app.routes.v1 import auth, users, reviews, posts, feed, notifications, home, admin, discover, content, news, chat, reports, collections, recommendations, social, media

# if settings.sentry_dsn:
#     import sentry_sdk
#     from sentry_sdk.integrations.fastapi import FastApiIntegration
#     sentry_sdk.init(
#         dsn=settings.sentry_dsn,
#         integrations=[FastApiIntegration(
#             transaction_style="endpoint",
#         )],
#         traces_sample_rate=0.1,
#         profiles_sample_rate=0.1,
#     )

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Start background news fetcher
    from app.services.chat_service import ChatService
    from app.services.news_service import NewsService
    from app.core.database import AsyncSessionLocal
    import asyncio
    import logging

    logger = logging.getLogger('mambo.scheduler')

    # 1. Temporarily disabling schema init to bypass Render timeouts/locks
    # from app.core.init_db import init_db, init_db_data_healing
    # async with AsyncSessionLocal() as db:
    #     logger.info("Initializing critical schemas at startup")
    #     await init_db(db)

    # 2. Define background startup tasks
    async def run_global_healing():
        # Temporarily skipping healing as well to ensure clean start
        pass

    async def run_news_scheduler():
        await asyncio.sleep(5)  # Short delay to allow port binding
        while True:
            try:
                logger.info("Starting background news fetch cycle")
                async with AsyncSessionLocal() as db:
                    service = NewsService(db)
                    await service.fetch_and_store_news()
                logger.info("News fetch cycle completed")
            except Exception as e:
                logger.error(f"News scheduler error: {e}")
            await asyncio.sleep(6 * 3600)  # 6 hours

    async def run_content_cleanup_scheduler():
        await asyncio.sleep(5)  # Short delay to allow port binding
        from app.services.content_service import ContentService
        while True:
            try:
                logger.info("Starting background content cleanup cycle")
                async with AsyncSessionLocal() as db:
                    service = ContentService(db)
                    deleted_stale = await service.cleanup_stale_content(hours=24)
                    deleted_persons = await service.cleanup_stale_persons(hours=24)
                    deleted_activities = await service.cleanup_old_activities(days=7)
                logger.info(f"Content cleanup cycle completed. Deleted {deleted_stale} stale items, {deleted_persons} stale persons, and {deleted_activities} old activities.")
            except Exception as e:
                logger.error(f"Content cleanup scheduler error: {e}")
            await asyncio.sleep(12 * 3600)  # 12 hours
            
    # scheduler_task = asyncio.create_task(run_news_scheduler())
    # cleanup_task = asyncio.create_task(run_content_cleanup_scheduler())
    # healing_task = asyncio.create_task(run_global_healing())
    
    yield
    
    # Cleanup
    # try:
    #     scheduler_task.cancel()
    #     cleanup_task.cancel()
    #     healing_task.cancel()
    #     await asyncio.gather(scheduler_task, cleanup_task, healing_task, return_exceptions=True)
    # except Exception:
    #     pass
    await engine.dispose()

app = FastAPI(
    title='Mambo API',
    version='1.0.0',
    docs_url='/docs' if not settings.is_production else None,
    lifespan=lifespan,
    # default_response_class=ORJSONResponse,
)

# Middleware
# app.add_middleware(SecurityHeadersMiddleware)
app.add_middleware(TimingMiddleware)
app.add_middleware(RequestIDMiddleware)
# app.add_middleware(RateLimitMiddleware)
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.origins_list,
    allow_credentials=True,
    allow_methods=['*'],
    allow_headers=['*'],
)

register_exception_handlers(app)

# Routes
app.include_router(auth.router,          prefix='/v1/auth')
app.include_router(users.router,         prefix='/v1/users')
app.include_router(reviews.router,       prefix='/v1/reviews')
app.include_router(posts.router,         prefix='/v1/posts')
app.include_router(feed.router,          prefix='/v1/feed')
app.include_router(notifications.router, prefix='/v1/notifications')
app.include_router(home.router,          prefix='/v1/home')
app.include_router(discover.router,      prefix='/v1/discover')
app.include_router(content.router,       prefix='/v1/content')
app.include_router(news.router,          prefix='/v1/news')
app.include_router(chat.router,          prefix='/v1/chat')
app.include_router(reports.router,       prefix='/v1/reports')
app.include_router(collections.router,   prefix='/v1/collections')
app.include_router(recommendations.router, prefix='/v1/recommendations')
app.include_router(social.router,          prefix='/v1/social')
app.include_router(admin.router,         prefix='/v1/admin')
app.include_router(media.router,         prefix='/v1/media')

# Backward-compatible aliases (no /v1 prefix)
# Handles apps built with base URL missing the /v1 path
app.include_router(auth.router,          prefix='/auth')
app.include_router(users.router,         prefix='/users')
app.include_router(reviews.router,       prefix='/reviews')
app.include_router(posts.router,         prefix='/posts')
app.include_router(feed.router,          prefix='/feed')
app.include_router(notifications.router, prefix='/notifications')
app.include_router(home.router,          prefix='/home')
app.include_router(discover.router,      prefix='/discover')
app.include_router(content.router,       prefix='/content')
app.include_router(news.router,          prefix='/news')
app.include_router(chat.router,          prefix='/chat')
app.include_router(collections.router,   prefix='/collections')
app.include_router(recommendations.router, prefix='/recommendations')
app.include_router(social.router,        prefix='/social')
app.include_router(media.router,         prefix='/media')


@app.api_route("/", methods=["GET", "HEAD"])
async def root():
    return {
        "message": "Welcome to Mambo API",
        "version": "1.0.0",
        "docs": "/docs" if not settings.is_production else "Contact admin for docs",
        "health": "/health"
    }

@app.api_route('/health', methods=['GET', 'HEAD'])
async def health():
    """Health check: pings the DB to verify connectivity. Supports GET and HEAD."""
    from sqlalchemy import text as _text
    from app.core.database import get_db as _get_db

    db_status = 'ok'
    try:
        async for db in _get_db():
            # Use a very fast check
            await db.execute(_text('SELECT 1'))
            break
    except Exception as e:
        db_status = f'down: {str(e)}'

    return {
        'status': 'ok' if db_status == 'ok' else 'degraded',
        'db': db_status,
        'env': settings.app_env
    }
