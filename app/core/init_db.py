import logging
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text

logger = logging.getLogger('mambo.db')

SCHEMA_SQL = """
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- 1. Core / Independent Tables
CREATE TABLE IF NOT EXISTS public.profiles (
  id           uuid PRIMARY KEY,
  username     text NOT NULL UNIQUE,
  display_name text,
  email        text,
  phone_number text,
  avatar_url   text,
  bio          text,
  gender       text,
  birthday     date,
  is_verified  boolean DEFAULT false,
  is_deleted   boolean DEFAULT false,
  created_at   timestamptz DEFAULT now(),
  updated_at   timestamptz DEFAULT now(),
  activity_visibility text DEFAULT 'public',
  favourites_visibility text DEFAULT 'public',
  reviews_visibility text DEFAULT 'public'
);

CREATE TABLE IF NOT EXISTS public.content (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  title text NOT NULL,
  poster_url text,
  content_type text,
  release_date date,
  external_rating float,
  is_permanent boolean DEFAULT false,
  made_permanent_at timestamptz,
  created_at timestamptz DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.persons (
  id text PRIMARY KEY,
  name text NOT NULL
);

CREATE TABLE IF NOT EXISTS public.badges (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  name text NOT NULL,
  description text,
  image_url text,
  is_active boolean DEFAULT true
);

-- 2. User Stats and Settings
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

CREATE TABLE IF NOT EXISTS public.privacy_settings (
  user_id uuid PRIMARY KEY REFERENCES profiles(id) ON DELETE CASCADE,
  profile_visibility text DEFAULT 'public',
  activity_visibility text DEFAULT 'public',
  updated_at timestamptz DEFAULT now()
);

-- 3. Social Relations
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

CREATE TABLE IF NOT EXISTS public.follows (
  id           uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  follower_id  uuid NOT NULL REFERENCES profiles(id) ON DELETE CASCADE,
  following_id uuid NOT NULL REFERENCES profiles(id) ON DELETE CASCADE,
  created_at   timestamptz DEFAULT now(),
  UNIQUE(follower_id, following_id)
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

-- 4. Content Interactions
CREATE TABLE IF NOT EXISTS public.user_content_status (
  user_id uuid NOT NULL REFERENCES profiles(id) ON DELETE CASCADE,
  content_id uuid NOT NULL REFERENCES content(id) ON DELETE CASCADE,
  is_watched boolean DEFAULT false,
  is_liked boolean DEFAULT false,
  is_dropped boolean DEFAULT false,
  is_interested boolean DEFAULT false,
  watch_count int DEFAULT 0,
  rating float,
  first_watched_at timestamptz,
  last_watched_at timestamptz,
  updated_at timestamptz DEFAULT now(),
  PRIMARY KEY (user_id, content_id)
);

CREATE TABLE IF NOT EXISTS public.watch_history (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id    uuid NOT NULL REFERENCES profiles(id) ON DELETE CASCADE,
  content_id uuid NOT NULL REFERENCES content(id) ON DELETE CASCADE,
  watch_type text, -- 'first_watch', 'rewatch'
  watched_at timestamptz DEFAULT now()
);

-- 5. Collections
CREATE TABLE IF NOT EXISTS public.collections (
  id          uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id     uuid NOT NULL REFERENCES profiles(id) ON DELETE CASCADE,
  name        text NOT NULL,
  description text,
  is_public   boolean DEFAULT true,
  is_pinned   boolean DEFAULT false,
  pin_order   int DEFAULT 0,
  is_default  boolean DEFAULT false,
  is_deletable boolean DEFAULT true,
  visibility  text DEFAULT 'public',
  item_count  int DEFAULT 0,
  created_at  timestamptz DEFAULT now(),
  updated_at  timestamptz DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.collection_items (
  collection_id uuid NOT NULL REFERENCES collections(id) ON DELETE CASCADE,
  content_id    uuid NOT NULL REFERENCES content(id) ON DELETE CASCADE,
  added_by      uuid REFERENCES profiles(id) ON DELETE SET NULL,
  added_at      timestamptz DEFAULT now(),
  PRIMARY KEY (collection_id, content_id)
);

-- 6. Posts and Reviews
CREATE TABLE IF NOT EXISTS public.posts (
  id            uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id       uuid NOT NULL REFERENCES profiles(id) ON DELETE CASCADE,
  content_id    uuid REFERENCES content(id) ON DELETE SET NULL,
  title         text,
  body          text NOT NULL,
  media_urls    text[] DEFAULT '{}',
  upvotes_count int DEFAULT 0,
  comments_count int DEFAULT 0,
  shares_count  int DEFAULT 0,
  created_at    timestamptz DEFAULT now(),
  updated_at    timestamptz DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.reviews (
  id            uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id       uuid NOT NULL REFERENCES profiles(id) ON DELETE CASCADE,
  content_id    uuid NOT NULL REFERENCES content(id) ON DELETE CASCADE,
  star_rating   float NOT NULL DEFAULT 0,
  text_review   text,
  is_spoiler    boolean DEFAULT false,
  is_deleted    boolean DEFAULT false,
  upvotes_count int DEFAULT 0,
  comments_count int DEFAULT 0,
  shares_count  int DEFAULT 0,
  created_at    timestamptz DEFAULT now(),
  updated_at    timestamptz DEFAULT now()
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

CREATE TABLE IF NOT EXISTS public.post_saves (
  user_id uuid NOT NULL REFERENCES profiles(id) ON DELETE CASCADE,
  post_id uuid NOT NULL REFERENCES posts(id) ON DELETE CASCADE,
  created_at timestamptz DEFAULT now(),
  PRIMARY KEY (user_id, post_id)
);

-- 7. Chat System
CREATE TABLE IF NOT EXISTS public.conversations (
  id               uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  conversation_type text NOT NULL, -- 'direct', 'group'
  direct_pair_key  text UNIQUE,
  created_by       uuid REFERENCES profiles(id) ON DELETE SET NULL,
  last_message_id  uuid,
  last_message_at  timestamptz,
  created_at       timestamptz DEFAULT now(),
  updated_at       timestamptz DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.conversation_members (
  conversation_id uuid NOT NULL REFERENCES conversations(id) ON DELETE CASCADE,
  user_id         uuid NOT NULL REFERENCES profiles(id) ON DELETE CASCADE,
  joined_at       timestamptz DEFAULT now(),
  PRIMARY KEY (conversation_id, user_id)
);

CREATE TABLE IF NOT EXISTS public.messages (
  id               uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  conversation_id  uuid NOT NULL REFERENCES conversations(id) ON DELETE CASCADE,
  sender_id        uuid NOT NULL REFERENCES profiles(id) ON DELETE CASCADE,
  receiver_id      uuid REFERENCES profiles(id) ON DELETE SET NULL,
  body             text NOT NULL,
  message_type     text NOT NULL DEFAULT 'text',
  shared_post_id   uuid REFERENCES posts(id) ON DELETE SET NULL,
  shared_review_id uuid REFERENCES reviews(id) ON DELETE SET NULL,
  shared_content_id uuid REFERENCES content(id) ON DELETE SET NULL,
  is_read          boolean DEFAULT false,
  read_at          timestamptz,
  sent_at          timestamptz DEFAULT now()
);

-- 8. Activity and Notifications
CREATE TABLE IF NOT EXISTS public.activity_log (
  id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id         uuid NOT NULL REFERENCES profiles(id) ON DELETE CASCADE,
  activity_type   text NOT NULL,
  content_id      uuid REFERENCES content(id) ON DELETE SET NULL,
  review_id       uuid REFERENCES reviews(id) ON DELETE SET NULL,
  post_id         uuid REFERENCES posts(id) ON DELETE SET NULL,
  collection_id   uuid REFERENCES collections(id) ON DELETE SET NULL,
  news_id         uuid, -- No news table in conftest yet
  related_user_id uuid REFERENCES profiles(id) ON DELETE SET NULL,
  details         jsonb,
  visibility      text DEFAULT 'public',
  created_at      timestamptz DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.notifications (
  id            uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id       uuid NOT NULL REFERENCES profiles(id) ON DELETE CASCADE,
  actor_id      uuid REFERENCES profiles(id) ON DELETE SET NULL,
  related_id    uuid,
  type          varchar(50) NOT NULL,
  title         text,
  message       text NOT NULL,
  is_read       boolean DEFAULT false,
  is_deleted    boolean DEFAULT false,
  read_at       timestamptz,
  created_at    timestamptz DEFAULT now(),
  last_updated_at timestamptz
);

-- 9. External & Utils
CREATE TABLE IF NOT EXISTS public.social_links (
  id         uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  user_id    uuid NOT NULL REFERENCES profiles(id) ON DELETE CASCADE,
  platform   text NOT NULL,
  url        text NOT NULL,
  created_at timestamptz DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.news_articles (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    title TEXT NOT NULL,
    description TEXT,
    url TEXT UNIQUE NOT NULL,
    external_url TEXT NOT NULL,
    image_url TEXT,
    source_name TEXT,
    category TEXT DEFAULT 'all',
    is_active BOOLEAN DEFAULT true,
    published_at TIMESTAMP WITH TIME ZONE,
    fetched_at TIMESTAMP WITH TIME ZONE DEFAULT now()
);

-- 10. Preferences & Badges
CREATE TABLE IF NOT EXISTS public.user_badges (
  user_id  uuid NOT NULL REFERENCES profiles(id) ON DELETE CASCADE,
  badge_id uuid NOT NULL REFERENCES badges(id) ON DELETE CASCADE,
  earned_at timestamptz DEFAULT now(),
  PRIMARY KEY (user_id, badge_id)
);

CREATE TABLE IF NOT EXISTS public.user_actor_preferences (
  user_id uuid NOT NULL REFERENCES profiles(id) ON DELETE CASCADE,
  person_id text NOT NULL REFERENCES persons(id) ON DELETE CASCADE,
  preference_order int DEFAULT 0,
  PRIMARY KEY (user_id, person_id)
);

CREATE TABLE IF NOT EXISTS public.user_director_preferences (
  user_id uuid NOT NULL REFERENCES profiles(id) ON DELETE CASCADE,
  person_id text NOT NULL REFERENCES persons(id) ON DELETE CASCADE,
  preference_order int DEFAULT 0,
  PRIMARY KEY (user_id, person_id)
);

CREATE TABLE IF NOT EXISTS public.recommendations (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  sender_id uuid NOT NULL REFERENCES profiles(id) ON DELETE CASCADE,
  content_id uuid NOT NULL REFERENCES content(id) ON DELETE CASCADE,
  message text,
  sent_at timestamptz DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.recommendation_recipients (
  recommendation_id uuid NOT NULL REFERENCES recommendations(id) ON DELETE CASCADE,
  recipient_id uuid NOT NULL REFERENCES profiles(id) ON DELETE CASCADE,
  PRIMARY KEY (recommendation_id, recipient_id)
);
"""

async def init_db(db: AsyncSession):
    """Run the consolidated schema SQL."""
    try:
        logger.info("Starting database schema initialization...")
        # execute handles multi-statement if string contains them
        # but to be safe with async, we split
        for statement in SCHEMA_SQL.split(';'):
            stmt = statement.strip()
            if stmt:
                await db.execute(text(stmt))
        
        await db.commit()
        logger.info("Database schema initialized successfully.")
    except Exception as e:
        await db.rollback()
        logger.error(f"Failed to initialize database schema: {e}")
        raise e
