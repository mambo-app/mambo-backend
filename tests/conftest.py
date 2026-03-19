"""
Shared fixtures for MAMBO integration tests.

Tests run against the LIVE app using httpx.AsyncClient.
They require the server to be running (uvicorn app.main:app) or use
the ASGI transport directly for isolated testing.
"""
import os
import asyncio
import pytest
import pytest_asyncio
from httpx import AsyncClient, ASGITransport

# Set APP_ENV=test early before any app imports to ensure consistent config
os.environ['APP_ENV'] = 'test'
os.environ['ADMIN_SECRET'] = 'test_admin_secret'
os.environ['INVITE_KEY'] = 'test_invite_key'

SCHEMA_SQL = """
CREATE EXTENSION IF NOT EXISTS pgcrypto;

DROP TABLE IF EXISTS public.post_comments CASCADE;
DROP TABLE IF EXISTS public.review_comments CASCADE;
DROP TABLE IF EXISTS public.post_upvotes CASCADE;
DROP TABLE IF EXISTS public.review_upvotes CASCADE;
DROP TABLE IF EXISTS public.posts CASCADE;
DROP TABLE IF EXISTS public.reviews CASCADE;
DROP TABLE IF EXISTS public.friend_requests CASCADE;
DROP TABLE IF EXISTS public.friends CASCADE;
DROP TABLE IF EXISTS public.muted_users CASCADE;
DROP TABLE IF EXISTS public.blocked_users CASCADE;
DROP TABLE IF EXISTS public.social_links CASCADE;
DROP TABLE IF EXISTS public.watch_history CASCADE;
DROP TABLE IF EXISTS public.follows CASCADE;
DROP TABLE IF EXISTS public.collection_items CASCADE;
DROP TABLE IF EXISTS public.collections CASCADE;
DROP TABLE IF EXISTS public.user_stats CASCADE;
DROP TABLE IF EXISTS public.profiles CASCADE;
DROP TABLE IF EXISTS public.content CASCADE;

CREATE TABLE IF NOT EXISTS public.profiles (
  id           uuid PRIMARY KEY,
  username     text NOT NULL UNIQUE,
  display_name text,
  avatar_url   text,
  is_verified  boolean DEFAULT false,
  is_deleted   boolean DEFAULT false,
  created_at   timestamptz DEFAULT now(),
  updated_at   timestamptz DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.friend_requests (
  id          uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  sender_id   uuid NOT NULL REFERENCES profiles(id) ON DELETE CASCADE,
  receiver_id uuid NOT NULL REFERENCES profiles(id) ON DELETE CASCADE,
  status      text NOT NULL DEFAULT 'pending',
  created_at  timestamptz DEFAULT now(),
  updated_at  timestamptz DEFAULT now(),
  UNIQUE(sender_id, receiver_id)
);

CREATE TABLE IF NOT EXISTS public.friends (
  id         uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id1   uuid NOT NULL REFERENCES profiles(id) ON DELETE CASCADE,
  user_id2   uuid NOT NULL REFERENCES profiles(id) ON DELETE CASCADE,
  created_at timestamptz DEFAULT now(),
  UNIQUE(user_id1, user_id2),
  CHECK(user_id1 < user_id2)
);

CREATE TABLE IF NOT EXISTS public.user_stats (
  user_id       uuid PRIMARY KEY REFERENCES profiles(id) ON DELETE CASCADE,
  friends_count int DEFAULT 0,
  total_watched int DEFAULT 0,
  total_reviews int DEFAULT 0,
  total_posts   int DEFAULT 0,
  followers_count int DEFAULT 0,
  following_count int DEFAULT 0,
  updated_at    timestamptz DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.content (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  title text NOT NULL
);

CREATE TABLE IF NOT EXISTS public.posts (
  id            uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id       uuid NOT NULL REFERENCES profiles(id) ON DELETE CASCADE,
  content_id    uuid REFERENCES content(id) ON DELETE SET NULL,
  title         text,
  body          text NOT NULL,
  media_urls    text[] DEFAULT '{}',
  upvotes_count int DEFAULT 0,
  comments_count int DEFAULT 0,
  created_at    timestamptz DEFAULT now(),
  updated_at    timestamptz DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.reviews (
  id            uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id       uuid NOT NULL REFERENCES profiles(id) ON DELETE CASCADE,
  content_id    uuid REFERENCES content(id) ON DELETE CASCADE,
  upvotes_count int DEFAULT 0,
  comments_count int DEFAULT 0,
  created_at    timestamptz DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.post_upvotes (
  id         uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  post_id    uuid NOT NULL REFERENCES posts(id) ON DELETE CASCADE,
  user_id    uuid NOT NULL REFERENCES profiles(id) ON DELETE CASCADE,
  created_at timestamptz DEFAULT now(),
  UNIQUE(post_id, user_id)
);

CREATE TABLE IF NOT EXISTS public.review_upvotes (
  id         uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  review_id  uuid NOT NULL REFERENCES reviews(id) ON DELETE CASCADE,
  user_id    uuid NOT NULL REFERENCES profiles(id) ON DELETE CASCADE,
  created_at timestamptz DEFAULT now(),
  UNIQUE(review_id, user_id)
);

CREATE TABLE IF NOT EXISTS public.post_comments (
  id                uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  post_id           uuid NOT NULL REFERENCES posts(id) ON DELETE CASCADE,
  user_id           uuid NOT NULL REFERENCES profiles(id) ON DELETE CASCADE,
  body              text NOT NULL,
  parent_id         uuid REFERENCES post_comments(id),
  is_deleted        boolean DEFAULT false,
  created_at        timestamptz DEFAULT now(),
  updated_at        timestamptz DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.review_comments (
  id                uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  review_id         uuid NOT NULL REFERENCES reviews(id) ON DELETE CASCADE,
  user_id           uuid NOT NULL REFERENCES profiles(id) ON DELETE CASCADE,
  body              text NOT NULL,
  parent_id         uuid REFERENCES review_comments(id),
  is_deleted        boolean DEFAULT false,
  created_at        timestamptz DEFAULT now(),
  updated_at        timestamptz DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.muted_users (
  id       uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  muter_id uuid NOT NULL REFERENCES profiles(id) ON DELETE CASCADE,
  muted_id uuid NOT NULL REFERENCES profiles(id) ON DELETE CASCADE,
  muted_at timestamptz DEFAULT now(),
  UNIQUE(muter_id, muted_id)
);

CREATE TABLE IF NOT EXISTS public.blocked_users (
  id         uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  blocker_id uuid NOT NULL REFERENCES profiles(id) ON DELETE CASCADE,
  blocked_id uuid NOT NULL REFERENCES profiles(id) ON DELETE CASCADE,
  blocked_at timestamptz DEFAULT now(),
  UNIQUE(blocker_id, blocked_id)
);

CREATE TABLE IF NOT EXISTS public.follows (
  id           uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  follower_id  uuid NOT NULL REFERENCES profiles(id) ON DELETE CASCADE,
  following_id uuid NOT NULL REFERENCES profiles(id) ON DELETE CASCADE,
  created_at   timestamptz DEFAULT now(),
  UNIQUE(follower_id, following_id)
);

CREATE TABLE IF NOT EXISTS public.collections (
  id          uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id     uuid NOT NULL REFERENCES profiles(id) ON DELETE CASCADE,
  name        text NOT NULL,
  description text,
  is_public   boolean DEFAULT true,
  created_at  timestamptz DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.social_links (
  id         uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id    uuid NOT NULL REFERENCES profiles(id) ON DELETE CASCADE,
  platform   text NOT NULL,
  url        text NOT NULL,
  created_at timestamptz DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.collection_items (
  collection_id uuid NOT NULL REFERENCES collections(id) ON DELETE CASCADE,
  content_id    uuid NOT NULL REFERENCES content(id) ON DELETE CASCADE,
  added_at      timestamptz DEFAULT now(),
  PRIMARY KEY (collection_id, content_id)
);

CREATE TABLE IF NOT EXISTS public.watch_history (
  user_id    uuid NOT NULL REFERENCES profiles(id) ON DELETE CASCADE,
  content_id uuid NOT NULL REFERENCES content(id) ON DELETE CASCADE,
  watched_at timestamptz DEFAULT now(),
  PRIMARY KEY (user_id, content_id)
);
"""

@pytest_asyncio.fixture(scope='session', autouse=True)
async def init_test_db():
    """Ensure the test database has all required tables."""
    from app.core.database import engine, AsyncSessionLocal
    from sqlalchemy import text
    async with AsyncSessionLocal() as db:
        for statement in SCHEMA_SQL.split(';'):
            stmt = statement.strip()
            if stmt:
                # print(f"Executing: {stmt[:50]}...")
                await db.execute(text(stmt))
        await db.commit()
    
    yield
    
    # Session teardown
    await engine.dispose()


@pytest_asyncio.fixture(scope='session')
async def client():
    """Create an ASGI test client that talks directly to the app."""
    from app.main import app as fastapi_app
    async with AsyncClient(
        transport=ASGITransport(app=fastapi_app),
        base_url='http://testserver',
    ) as c:
        yield c


@pytest.fixture(scope='session')
def auth_headers():
    """
    Return Authorization headers for authenticated requests.
    Set TEST_AUTH_TOKEN env var to use a pre-existing token.
    """
    token = os.environ.get('TEST_AUTH_TOKEN', '')
    if token:
        return {'Authorization': f'Bearer {token}'}
    return {}


@pytest_asyncio.fixture(autouse=True)
async def clean_db():
    """Clean up the database before each test."""
    from app.core.database import AsyncSessionLocal
    from sqlalchemy import text
    async with AsyncSessionLocal() as session:
        tables = [
            'friend_requests', 'friends', 'post_upvotes', 'review_upvotes',
            'post_comments', 'review_comments', 'posts', 'reviews',
            'muted_users', 'blocked_users', 'follows', 'collections',
            'collection_items', 'social_links', 'watch_history', 'user_stats', 'profiles', 'content'
        ]
        for table in tables:
            await session.execute(text(f"TRUNCATE TABLE public.{table} RESTART IDENTITY CASCADE"))
        await session.commit()
    yield

@pytest_asyncio.fixture
async def db():
    """Provide a real AsyncSession for repository testing."""
    from app.core.database import AsyncSessionLocal
    async with AsyncSessionLocal() as session:
        yield session
        await session.rollback()
