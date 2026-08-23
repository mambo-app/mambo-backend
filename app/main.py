import sentry_sdk
from sentry_sdk.integrations.fastapi import FastApiIntegration
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, Response
from contextlib import asynccontextmanager

from app.core.config import settings
from app.core.database import engine
from app.core.exceptions import register_exception_handlers
from app.core.logger import configure_logging, get_logger
from app.middleware.request_id import RequestIDMiddleware
from app.middleware.timing import TimingMiddleware
from app.middleware.security_headers import SecurityHeadersMiddleware
from app.middleware.rate_limit import RateLimitMiddleware

# Initialize structured logging before anything else
configure_logging(level='INFO')

from app.routes.v1 import auth, users, reviews, posts, feed, notifications, home, admin, discover, content, news, chat, reports, collections, recommendations, social, media, migration, import_letterboxd

if settings.sentry_dsn:
    sentry_sdk.init(
        dsn=settings.sentry_dsn,
        integrations=[FastApiIntegration(
            transaction_style="endpoint",
        )],
        traces_sample_rate=0.1,
        profiles_sample_rate=0.1,
    )

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Start background news fetcher
    from app.services.chat_service import ChatService
    from app.services.news_service import NewsService
    from app.core.database import AsyncSessionLocal
    import asyncio
    import logging

    logger = logging.getLogger('mambo.scheduler')

    # 1. Fast seed purge & date cleanup at startup
    from app.core.init_db import init_db
    async with AsyncSessionLocal() as db:
        try:
            await init_db(db)
            await db.execute(text("ALTER TABLE profiles ADD COLUMN IF NOT EXISTS letterboxd_username TEXT;"))
            await db.execute(text("ALTER TABLE profiles ADD COLUMN IF NOT EXISTS letterboxd_import_status TEXT DEFAULT 'idle';"))
            await db.execute(text("ALTER TABLE content ADD COLUMN IF NOT EXISTS next_episode_to_air JSONB;"))
            await db.execute(text("ALTER TABLE content ADD COLUMN IF NOT EXISTS production_status TEXT;"))
            await db.execute(text("ALTER TABLE content ADD COLUMN IF NOT EXISTS is_announced_no_date BOOLEAN DEFAULT FALSE;"))
            await db.execute(text("ALTER TABLE content ADD COLUMN IF NOT EXISTS pending_season_number INTEGER;"))
            await db.execute(text("ALTER TABLE content ADD COLUMN IF NOT EXISTS pending_season_is_new_show BOOLEAN DEFAULT FALSE;"))
            await db.execute(text("ALTER TABLE content ADD COLUMN IF NOT EXISTS status_last_verified_at TIMESTAMP;"))
            await db.execute(text("CREATE INDEX IF NOT EXISTS idx_content_announced ON content (content_type, is_announced_no_date) WHERE is_announced_no_date = TRUE;"))
            await db.execute(text("CREATE INDEX IF NOT EXISTS idx_content_status_verify ON content (status_last_verified_at) WHERE is_permanent = TRUE OR status_last_verified_at IS NOT NULL;"))
            await db.execute(text("""
                DELETE FROM content 
                WHERE title IN (
                    'Spider-Man: Brand New Day', 'Evil Dead Burn', 'The Devil''s Mouth', '72 HOURS', 
                    'Avatar Aang: The Last Airbender', 'The Last House', 'The End of Oak Street', 'Soulm8te', 'Heartstopper Forever',
                    'IT: Welcome to Derry', 'A Knight of the Seven Kingdoms', 'Pluribus', 'Teach You a Lesson',
                    'Off Campus', 'Spider-Noir', 'Heated Rivalry', 'Dutton Ranch', 'HIS & HERS', 'Marvel Zombies',
                    'Tulsa King', 'Stranger Things Season 5', 'Invincible Season 3', 'The Witcher Season 4', 'Euphoria Season 3',
                    'The Odyssey', 'Toy Story 5', 'Rage of Stars', 'Colony', 'Minions & Monsters', 'Obsession', 'The Death of Robin Hood'
                ) OR tmdb_id IS NULL;
            """))
            await db.commit()
        except Exception as e:
            logger.warning(f"Startup schema init error: {e}")
            await db.rollback()

    # 2. Define background startup tasks
    async def run_global_healing():
        await asyncio.sleep(1200) # Start 20 minutes after boot
        async with AsyncSessionLocal() as db:
            await init_db_data_healing(db)
        logger.info("Global data healing task completed")

    async def run_news_scheduler():
        await asyncio.sleep(900)
        while True:
            try:
                logger.info("Starting background news fetch cycle")
                async with AsyncSessionLocal() as db:
                    service = NewsService(db)
                    await service.fetch_and_store_news(limit=5)
                logger.info("News fetch cycle completed")
            except Exception as e:
                logger.error(f"News scheduler error: {e}")
            await asyncio.sleep(6 * 3600)  # 6 hours

    async def run_content_cleanup_scheduler():
        await asyncio.sleep(600)  # Start 10 minutes after boot
        from app.services.content_service import ContentService
        while True:
            try:
                logger.info("Starting background content cleanup cycle")
                async with AsyncSessionLocal() as db:
                    service = ContentService(db)
                    deleted_stale = await service.cleanup_stale_content(hours=24)
                    deleted_persons = await service.cleanup_stale_persons(hours=24)
                    deleted_activities = await service.cleanup_old_activities(days=7)
                    
                    # 4. Cleanup old notifications
                    from app.services.notification_service import NotificationService
                    notif_service = NotificationService(db)
                    deleted_notifs = await notif_service.cleanup_old_notifications(days=30)
                    
                logger.info(f"Content cleanup cycle completed. Deleted {deleted_stale} stale items, {deleted_persons} stale persons, {deleted_activities} old activities, and {deleted_notifs} old notifications.")
            except Exception as e:
                logger.error(f"Content cleanup scheduler error: {e}")
            await asyncio.sleep(12 * 3600)  # 12 hours

    async def run_release_checker_scheduler():
        await asyncio.sleep(10)  # Check 10 seconds after boot
        from app.tasks.release_checker import check_and_notify_releases
        while True:
            try:
                logger.info("Starting background release check cycle")
                await check_and_notify_releases()
                logger.info("Release check cycle completed")
            except Exception as e:
                logger.error(f"Release checker error: {e}")
            await asyncio.sleep(3600)  # 1 hour

    async def run_announced_sync_scheduler():
        await asyncio.sleep(60)  # Start 1 minute after boot
        from app.services.content_service import ContentService
        while True:
            try:
                logger.info("Starting background announced status sync cycle")
                async with AsyncSessionLocal() as db:
                    service = ContentService(db)
                    stats = await service.sync_announced_status(max_items_per_run=400)
                logger.info(f"Announced status sync cycle completed: {stats}")
            except Exception as e:
                logger.error(f"Announced sync scheduler error: {e}")
            await asyncio.sleep(6 * 3600)  # 6 hours

    # 3. Start background schedulers
    release_task = asyncio.create_task(run_release_checker_scheduler())
    announced_task = asyncio.create_task(run_announced_sync_scheduler())
    if settings.app_env != 'development':
        logger.info(f"Starting background maintenance tasks in {settings.app_env} mode")
        scheduler_task = asyncio.create_task(run_news_scheduler())
        cleanup_task = asyncio.create_task(run_content_cleanup_scheduler())
        healing_task = asyncio.create_task(run_global_healing())
    else:
        logger.info("Maintenance tasks are FORCED OFF for local development except Release Checker & Announced Sync")
        scheduler_task = None
        cleanup_task = None
        healing_task = None
    
    yield
    
    # Cleanup
    try:
        if release_task: release_task.cancel()
        if announced_task: announced_task.cancel()
        if scheduler_task: scheduler_task.cancel()
        if cleanup_task: cleanup_task.cancel()
        if healing_task: healing_task.cancel()
        await asyncio.gather(
            *[t for t in [release_task, announced_task, scheduler_task, cleanup_task, healing_task] if t], 
            return_exceptions=True
        )
    except Exception:
        pass
    await engine.dispose()

app = FastAPI(
    title='Mambo API',
    version='1.0.0',
    docs_url='/docs' if not settings.is_production else None,
    lifespan=lifespan,
    # Standard JSONResponse is more stable and faster in FastAPI 0.100+
    default_response_class=JSONResponse,
)

# Middleware
app.add_middleware(SecurityHeadersMiddleware)
app.add_middleware(TimingMiddleware)
app.add_middleware(RequestIDMiddleware)
app.add_middleware(RateLimitMiddleware)
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
app.include_router(migration.router,     prefix='/v1/migration')
app.include_router(import_letterboxd.router, prefix='/v1/import/letterboxd')

# /api/v1 Aliases
app.include_router(auth.router,          prefix='/api/v1/auth')
app.include_router(users.router,         prefix='/api/v1/users')
app.include_router(reviews.router,       prefix='/api/v1/reviews')
app.include_router(posts.router,         prefix='/api/v1/posts')
app.include_router(feed.router,          prefix='/api/v1/feed')
app.include_router(notifications.router, prefix='/api/v1/notifications')
app.include_router(home.router,          prefix='/api/v1/home')
app.include_router(discover.router,      prefix='/api/v1/discover')
app.include_router(content.router,       prefix='/api/v1/content')
app.include_router(news.router,          prefix='/api/v1/news')
app.include_router(chat.router,          prefix='/api/v1/chat')
app.include_router(reports.router,       prefix='/api/v1/reports')
app.include_router(collections.router,   prefix='/api/v1/collections')
app.include_router(recommendations.router, prefix='/api/v1/recommendations')
app.include_router(social.router,          prefix='/api/v1/social')
app.include_router(admin.router,         prefix='/api/v1/admin')
app.include_router(media.router,         prefix='/api/v1/media')
app.include_router(migration.router,     prefix='/api/v1/migration')
app.include_router(import_letterboxd.router, prefix='/api/v1/import/letterboxd')

# Backward-compatible aliases
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
app.include_router(import_letterboxd.router, prefix='/import/letterboxd')


@app.api_route("/", methods=["GET", "HEAD"])
async def root():
    return {
        "message": "Welcome to Mambo API",
        "version": "1.0.0",
        "docs": "/docs" if not settings.is_production else "Contact admin for docs",
        "health": "/health"
    }

# Force reload trigger


@app.api_route('/health', methods=['GET', 'HEAD'])
async def health():
    """Lightweight health check for Render/Cloudflare liveness probes."""
    return {
        'status': 'ok',
        'env': settings.app_env,
        'version': '1.0.1' 
    }
# Reload Trigger
