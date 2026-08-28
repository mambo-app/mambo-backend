import logging
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('mambo.db_init')

async def init_db(db: AsyncSession):
    """
    Critical schema initialization. 
    Runs synchronously at startup to ensure tables and columns exist.
    """
    logger.info("Running critical database schema initialization...")

    try:
        await db.execute(text("CREATE EXTENSION IF NOT EXISTS pgcrypto"))
        await db.commit()
    except Exception as e:
        logger.warning(f"pgcrypto extension: {e}")
        await db.rollback()

    async def add_col(table: str, col: str, dtype: str):
        try:
            await db.execute(text(
                f"ALTER TABLE public.{table} ADD COLUMN IF NOT EXISTS {col} {dtype}"
            ))
            await db.commit()
        except Exception as e:
            logger.warning(f"add_col {table}.{col}: {e}")
            await db.rollback()

    # --- Structure Changes (SCHEMA) ---
    await add_col("profiles", "bio",                         "TEXT")
    await add_col("profiles", "birthday",                    "DATE")
    await add_col("profiles", "gender",                      "TEXT")
    await add_col("profiles", "email",                       "TEXT")
    await add_col("profiles", "phone_number",                "TEXT")
    await add_col("profiles", "activity_visibility",         "TEXT DEFAULT 'public'")
    await add_col("profiles", "favourites_visibility",       "TEXT DEFAULT 'public'")
    await add_col("profiles", "reviews_visibility",          "TEXT DEFAULT 'public'")
    await add_col("profiles", "library_visibility",          "TEXT DEFAULT 'public'")
    await add_col("profiles", "push_notifications_enabled",  "BOOLEAN DEFAULT true")
    await add_col("profiles", "search_vector",               "TSVECTOR")
    await add_col("profiles", "is_deleted",                  "BOOLEAN DEFAULT false")
    await add_col("profiles", "updated_at",                  "TIMESTAMPTZ DEFAULT now()")
    await add_col("profiles", "letterboxd_username",          "TEXT")
    await add_col("profiles", "letterboxd_import_status",    "TEXT DEFAULT 'idle'")
    
    # --- Data Migration ---
    try:
        # Migration: set existing users to private library if not already set
        await db.execute(text("UPDATE profiles SET library_visibility = 'public' WHERE library_visibility IS NULL"))
        # Cleanup fake/tentative future release dates stored on returning series in PostgreSQL content table
        await db.execute(text("""
            UPDATE content 
            SET release_date = NULL 
            WHERE (LOWER(status) LIKE '%returning%' OR LOWER(status) LIKE '%in production%') 
              AND release_date > CURRENT_DATE
        """))
        # Purge fake/mock seed rows inserted during earlier testing
        await db.execute(text("""
            DELETE FROM content 
            WHERE title IN (
                'Spider-Man: Brand New Day', 'Evil Dead Burn', 'The Devil''s Mouth', '72 HOURS', 
                'Avatar Aang: The Last Airbender', 'The Last House', 'The End of Oak Street', 'Soulm8te', 'Heartstopper Forever',
                'IT: Welcome to Derry', 'A Knight of the Seven Kingdoms', 'Pluribus', 'Teach You a Lesson',
                'Off Campus', 'Spider-Noir', 'Heated Rivalry', 'Dutton Ranch', 'HIS & HERS', 'Marvel Zombies',
                'Tulsa King', 'Stranger Things Season 5', 'Invincible Season 3', 'The Witcher Season 4', 'Euphoria Season 3'
            ) OR tmdb_id IS NULL;
        """))
        # Merge duplicate watch_history rows generated from review attachments
        await db.execute(text("""
            WITH duplicates AS (
                SELECT id, ROW_NUMBER() OVER (
                    PARTITION BY user_id, content_id 
                    ORDER BY 
                        CASE WHEN review_id IS NOT NULL THEN 1 WHEN watch_type = 'first_watch' THEN 2 ELSE 3 END,
                        watched_at ASC
                ) as rn
                FROM watch_history
            )
            DELETE FROM watch_history WHERE id IN (SELECT id FROM duplicates WHERE rn > 1);
        """))
        # Recalculate watch_count in user_content_status
        await db.execute(text("""
            UPDATE user_content_status ucs
            SET watch_count = COALESCE((
                SELECT COUNT(*) FROM watch_history wh WHERE wh.user_id = ucs.user_id AND wh.content_id = ucs.content_id
            ), 1)
            WHERE ucs.is_watched = true;
        """))
        # Clean stale JSON details in activity_log where watch_count <= 1
        await db.execute(text("""
            UPDATE activity_log
            SET details = details - 'is_rewatch' - 'watch_count'
            WHERE details IS NOT NULL AND ((details->>'watch_count')::int <= 1 OR details->>'is_rewatch' = 'false');
        """))
        await db.commit()
    except Exception as e:
        logger.warning(f"Migration error: {e}")
        await db.rollback()

    await add_col("user_stats", "friends_count",   "INTEGER DEFAULT 0")
    await add_col("user_stats", "followers_count", "INTEGER DEFAULT 0")
    await add_col("user_stats", "following_count", "INTEGER DEFAULT 0")
    await add_col("user_stats", "total_watched",   "INTEGER DEFAULT 0")
    await add_col("user_stats", "total_reviews",   "INTEGER DEFAULT 0")
    await add_col("user_stats", "total_posts",     "INTEGER DEFAULT 0")
    await add_col("user_stats", "updated_at",      "TIMESTAMPTZ DEFAULT now()")

    await add_col("collection_items", "imported_from", "VARCHAR(50)")

    await add_col("content", "content_type",           "TEXT NOT NULL DEFAULT 'movie'")
    await add_col("content", "tmdb_id",                "INTEGER")
    await add_col("content", "mal_id",                 "INTEGER")
    await add_col("content", "original_title",         "TEXT")
    await add_col("content", "original_language",      "TEXT")
    await add_col("content", "synopsis",               "TEXT")
    await add_col("content", "poster_url",             "TEXT")
    await add_col("content", "backdrop_url",           "TEXT")
    await add_col("content", "external_rating",        "FLOAT")
    await add_col("content", "external_rating_source", "TEXT")
    await add_col("content", "vote_count",             "INTEGER DEFAULT 0")
    await add_col("content", "release_date",           "DATE")
    await add_col("content", "status",                 "TEXT")
    await add_col("content", "anime_studio",           "TEXT")
    await add_col("content", "total_episodes",         "INTEGER")
    await add_col("content", "total_seasons",          "INTEGER DEFAULT 1")
    await add_col("content", "seasons",                "JSONB DEFAULT '[]'")
    await add_col("content", "next_episode_to_air",     "JSONB")
    await add_col("content", "genres",                 "TEXT[] DEFAULT '{}'")
    await add_col("content", "is_permanent",           "BOOLEAN DEFAULT false")
    await add_col("content", "made_permanent_at",      "TIMESTAMPTZ")
    await add_col("content", "last_synced_at",         "TIMESTAMPTZ DEFAULT now()")
    await add_col("content", "created_at",             "TIMESTAMPTZ DEFAULT now()")

    try:
        await db.execute(text(
            "ALTER TABLE public.content ADD CONSTRAINT content_tmdb_id_key UNIQUE (tmdb_id)"
        ))
    except Exception:
        await db.rollback()

    try:
        await db.execute(text(
            "ALTER TABLE public.content ADD CONSTRAINT content_mal_id_key UNIQUE (mal_id)"
        ))
    except Exception:
        await db.rollback()

    await add_col("reviews", "rating",            "FLOAT")
    await add_col("reviews", "star_rating",       "INTEGER")
    await add_col("reviews", "text_review",       "TEXT")
    await add_col("reviews", "contains_spoiler",  "BOOLEAN DEFAULT false")
    await add_col("reviews", "is_spoiler",        "BOOLEAN DEFAULT false")
    await add_col("reviews", "tags",              "TEXT[] DEFAULT '{}'")
    await add_col("reviews", "likes_count",       "INTEGER DEFAULT 0")
    await add_col("reviews", "comments_count",    "INTEGER DEFAULT 0")
    await add_col("reviews", "shares_count",      "INTEGER DEFAULT 0")
    await add_col("reviews", "saves_count",       "INTEGER DEFAULT 0")
    await add_col("reviews", "upvotes_count",     "INTEGER DEFAULT 0")
    await add_col("reviews", "is_deleted",        "BOOLEAN DEFAULT false")
    await add_col("reviews", "deleted_at",        "TIMESTAMPTZ")
    await add_col("reviews", "updated_at",        "TIMESTAMPTZ DEFAULT now()")
    await add_col("reviews", "tagged_seasons",    "INTEGER[] DEFAULT '{}'")
    await add_col("reviews", "tagged_episodes",   "INTEGER[] DEFAULT '{}'")
    await add_col("reviews", "review_type",       "TEXT DEFAULT 'overall'")

    try:
        await db.execute(text(
            "ALTER TABLE public.reviews ADD CONSTRAINT reviews_user_content_unique "
            "UNIQUE (user_id, content_id)"
        ))
    except Exception:
        await db.rollback()

    await add_col("posts", "upvotes_count",   "INTEGER DEFAULT 0")
    await add_col("posts", "comments_count",  "INTEGER DEFAULT 0")
    await add_col("posts", "shares_count",    "INTEGER DEFAULT 0")
    await add_col("posts", "saves_count",     "INTEGER DEFAULT 0")
    await add_col("posts", "updated_at",      "TIMESTAMPTZ DEFAULT now()")

    await add_col("collections", "collection_type",  "TEXT DEFAULT 'user'")
    await add_col("collections", "is_default",       "BOOLEAN DEFAULT false")
    await add_col("collections", "is_deletable",     "BOOLEAN DEFAULT true")
    await add_col("collections", "item_count",       "INTEGER DEFAULT 0")
    await add_col("collections", "visibility",       "TEXT DEFAULT 'public'")
    await add_col("collections", "is_pinned",        "BOOLEAN DEFAULT false")
    await add_col("collections", "is_ranked",        "BOOLEAN DEFAULT false")
    await add_col("collections", "saves_count",       "INTEGER DEFAULT 0")
    await add_col("collections", "updated_at",       "TIMESTAMPTZ DEFAULT now()")
    
    await db.execute(text('''
        CREATE TABLE IF NOT EXISTS public.saved_collections (
            user_id uuid NOT NULL REFERENCES public.profiles(id) ON DELETE CASCADE,
            collection_id uuid NOT NULL REFERENCES public.collections(id) ON DELETE CASCADE,
            saved_at timestamptz DEFAULT now(),
            PRIMARY KEY (user_id, collection_id)
        )
    '''))
    
    # Migration: Set existing private collections to public (as per new policy)
    try:
        await db.execute(text("UPDATE collections SET visibility = 'public', is_public = true WHERE visibility = 'private'"))
        await db.commit()
    except Exception as e:
        logger.warning(f"Collection migration error: {e}")
        await db.rollback()

    await add_col("collection_items", "added_by",  "UUID")
    await add_col("collection_items", "position",  "INTEGER DEFAULT 0")

    await add_col("conversations", "direct_pair_key",  "TEXT")
    await add_col("conversations", "title",            "TEXT")
    await add_col("conversations", "last_message_id",  "UUID")
    await add_col("conversations", "last_message_at",  "TIMESTAMPTZ")
    await add_col("conversations", "updated_at",       "TIMESTAMPTZ DEFAULT now()")
    try:
        await db.execute(text(
            "CREATE UNIQUE INDEX IF NOT EXISTS uq_direct_pair ON public.conversations(direct_pair_key)"
        ))
    except Exception:
        await db.rollback()

    await add_col("messages", "shared_content_id",      "UUID")
    await add_col("messages", "shared_review_id",       "UUID")
    await add_col("messages", "shared_post_id",         "UUID")
    await add_col("messages", "shared_news_id",         "UUID")
    await add_col("messages", "message_type",           "TEXT DEFAULT 'text'")
    await add_col("messages", "image_url",              "TEXT")
    await add_col("messages", "is_read",                "BOOLEAN DEFAULT false")
    await add_col("messages", "read_at",                "TIMESTAMPTZ")
    await add_col("messages", "deleted_by_sender",      "BOOLEAN DEFAULT false")
    await add_col("messages", "deleted_by_receiver",    "BOOLEAN DEFAULT false")

    await add_col("notifications", "actor_id",             "UUID")
    await add_col("notifications", "related_id",           "UUID")
    await add_col("notifications", "image_url",            "TEXT")
    await add_col("notifications", "poster_url",           "TEXT")
    await add_col("notifications", "aggregate_key",        "TEXT")
    await add_col("notifications", "aggregate_count",      "INTEGER DEFAULT 1")
    await add_col("notifications", "first_actor_id",       "UUID")
    await add_col("notifications", "latest_actor_id",      "UUID")
    await add_col("notifications", "last_updated_at",      "TIMESTAMPTZ DEFAULT now()")
    await add_col("notifications", "is_deleted",           "BOOLEAN DEFAULT false")
    await add_col("notifications", "deleted_at",           "TIMESTAMPTZ")
    await add_col("notifications", "related_content_id",   "UUID")
    await add_col("notifications", "related_review_id",    "UUID")
    await add_col("notifications", "related_post_id",      "UUID")
    await add_col("notifications", "related_collection_id","UUID")

    await add_col("user_content_status", "favorite_order", "INTEGER")
    await add_col("user_person_favorites", "favorite_order", "INTEGER")
    await add_col("user_content_status", "status",           "TEXT DEFAULT 'none'")
    await add_col("user_content_status", "progress_episodes","INTEGER DEFAULT 0")
    await add_col("user_content_status", "rewatch_count",    "INTEGER DEFAULT 0")
    await add_col("user_content_status", "last_activity_at", "TIMESTAMPTZ DEFAULT now()")
    await add_col("user_content_status", "is_skipped",        "BOOLEAN DEFAULT false")

    await add_col("user_stats", "current_streak", "INTEGER DEFAULT 0")
    await add_col("user_stats", "max_streak",     "INTEGER DEFAULT 0")
    await add_col("user_stats", "last_streak_at", "TIMESTAMPTZ")
    await add_col("user_stats", "badges",          "JSONB DEFAULT '[]'::jsonb")

    await db.execute(text('''
        CREATE TABLE IF NOT EXISTS public.collection_groups (
            id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
            collection_id uuid NOT NULL REFERENCES public.collections(id) ON DELETE CASCADE,
            user_id uuid NOT NULL REFERENCES public.profiles(id) ON DELETE CASCADE,
            name text NOT NULL,
            color text,
            created_at timestamptz DEFAULT now()
        );
    '''))

    await db.execute(text('''
        CREATE TABLE IF NOT EXISTS public.collection_group_items (
            group_id uuid NOT NULL REFERENCES public.collection_groups(id) ON DELETE CASCADE,
            content_id uuid NOT NULL REFERENCES public.content(id) ON DELETE CASCADE,
            added_at timestamptz DEFAULT now(),
            PRIMARY KEY (group_id, content_id)
        );
    '''))

    await db.execute(text('''
        CREATE TABLE IF NOT EXISTS public.episode_watch_history (
            id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
            user_id uuid NOT NULL REFERENCES public.profiles(id) ON DELETE CASCADE,
            content_id uuid NOT NULL REFERENCES public.content(id) ON DELETE CASCADE,
            season_number integer NOT NULL,
            episode_number integer NOT NULL,
            rating float,
            note text,
            watched_at timestamptz DEFAULT now(),
            UNIQUE(user_id, content_id, season_number, episode_number)
        );
    '''))

    await db.execute(text('''
        CREATE TABLE IF NOT EXISTS public.user_prompt_picks (
            id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
            user_id uuid NOT NULL REFERENCES public.profiles(id) ON DELETE CASCADE,
            slot_index integer NOT NULL,
            content_id uuid NOT NULL REFERENCES public.content(id) ON DELETE CASCADE,
            created_at timestamptz DEFAULT now(),
            updated_at timestamptz DEFAULT now(),
            UNIQUE(user_id, slot_index)
        );
    '''))

    await add_col("episode_watch_history", "rating", "FLOAT")
    await add_col("episode_watch_history", "note",   "TEXT")

    try:
        await db.execute(text("ALTER TABLE public.notifications ALTER COLUMN title DROP NOT NULL"))
        # Drop the restrictive type check that blocks new notification types
        await db.execute(text("ALTER TABLE public.notifications DROP CONSTRAINT IF EXISTS notifications_type_check"))
    except Exception:
        await db.rollback()

    await add_col("news_articles", "external_url",     "TEXT")
    await add_col("news_articles", "is_permanent",     "BOOLEAN DEFAULT false")
    await add_col("news_articles", "likes_count",      "INTEGER DEFAULT 0")
    await add_col("news_articles", "comments_count",   "INTEGER DEFAULT 0")
    await add_col("news_articles", "shares_count",     "INTEGER DEFAULT 0")
    await add_col("news_articles", "content",          "TEXT")
    await add_col("news_articles", "is_active",        "BOOLEAN DEFAULT true")
    await add_col("news_articles", "fetched_at",       "TIMESTAMPTZ DEFAULT now()")

    await add_col("persons", "mal_id",          "INTEGER")
    await add_col("persons", "bio",             "TEXT")
    await add_col("persons", "birth_date",      "DATE")
    await add_col("persons", "death_date",      "DATE")
    await add_col("persons", "place_of_birth",  "VARCHAR")
    await add_col("persons", "known_for",       "TEXT")
    await add_col("persons", "known_for_department", "TEXT")
    await add_col("persons", "is_permanent",           "BOOLEAN DEFAULT false")
    await add_col("persons", "last_synced_at",         "TIMESTAMPTZ DEFAULT now()")
    await add_col("persons", "updated_at",      "TIMESTAMPTZ DEFAULT now()")

    await db.execute(text('''
        CREATE TABLE IF NOT EXISTS public.privacy_settings (
            id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
            user_id uuid NOT NULL UNIQUE REFERENCES public.profiles(id) ON DELETE CASCADE,
            activity_visibility text DEFAULT 'public',
            reviews_visibility text DEFAULT 'public',
            posts_visibility text DEFAULT 'public',
            favourites_visibility text DEFAULT 'public',
            stats_visibility text DEFAULT 'public',
            watchlist_visibility text DEFAULT 'private',
            watched_visibility text DEFAULT 'private',
            liked_visibility text DEFAULT 'private',
            dropped_visibility text DEFAULT 'private',
            custom_collections_visibility text DEFAULT 'friends',
            show_birthday boolean DEFAULT false,
            show_gender boolean DEFAULT false,
            created_at timestamptz DEFAULT now(),
            updated_at timestamptz DEFAULT now()
        )
    '''))

    await db.execute(text('''
        CREATE TABLE IF NOT EXISTS public.push_tokens (
            id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
            user_id uuid NOT NULL REFERENCES public.profiles(id) ON DELETE CASCADE,
            token text NOT NULL UNIQUE,
            platform text NOT NULL,
            updated_at timestamptz DEFAULT now(),
            UNIQUE(user_id, token)
        )
    '''))

    await db.execute(text('''
        CREATE TABLE IF NOT EXISTS public.search_history (
            id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
            user_id uuid NOT NULL REFERENCES public.profiles(id) ON DELETE CASCADE,
            query text NOT NULL,
            content_type_filter text,
            searched_at timestamptz DEFAULT now()
        )
    '''))

    await db.execute(text('''
        CREATE TABLE IF NOT EXISTS public.reported_content (
            id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
            reported_by uuid NOT NULL,
            report_type text NOT NULL,
            review_id uuid,
            post_id uuid,
            reported_user_id uuid,
            message_id uuid,
            news_id uuid,
            reason text NOT NULL,
            description text,
            status text DEFAULT 'pending',
            reviewed_by uuid,
            reviewed_at timestamptz,
            reported_at timestamptz DEFAULT now()
        )
    '''))

    await db.execute(text('''
        CREATE TABLE IF NOT EXISTS public.user_favorite_genres (
            user_id uuid NOT NULL REFERENCES public.profiles(id) ON DELETE CASCADE,
            genre_name text NOT NULL,
            created_at timestamptz DEFAULT now(),
            PRIMARY KEY (user_id, genre_name)
        )
    '''))

    await add_col("curated_content", "content_type", "TEXT")
    await add_col("curated_content", "source_id",    "TEXT")
    await add_col("curated_content", "link_url",     "TEXT")
    await add_col("curated_content", "content_id",   "UUID")

    # [REWATCH SUPPORT] Remove unique constraint on watch_history (user_id, content_id)
    # This allows multiple "Watch" events for the same content.
    try:
        # 1. Drop the composite primary key
        await db.execute(text("ALTER TABLE public.watch_history DROP CONSTRAINT IF EXISTS watch_history_pkey"))
        await db.commit()
    except Exception as e:
        logger.warning(f"watch_history pkey drop: {e}")
        await db.rollback()

    await add_col("watch_history", "id",          "UUID PRIMARY KEY DEFAULT gen_random_uuid()")
    await add_col("watch_history", "watch_type",  "TEXT DEFAULT 'first_watch'")
    await add_col("watch_history", "rating",      "FLOAT")
    await add_col("watch_history", "review_id",   "UUID")

    try:
        # Note: We try to drop by name or by signature. 
        # Standard Postgres name for unique constraint is '{table}_{col1}_{col2}_key'
        await db.execute(text("ALTER TABLE public.watch_history DROP CONSTRAINT IF EXISTS watch_history_user_id_content_id_key"))
        await db.commit()
    except Exception:
        await db.rollback()

    await add_col("reviews", "watch_history_id", "UUID")

    await add_col("activity_log", "news_id",          "UUID")
    await add_col("activity_log", "details",          "JSONB DEFAULT '{}'")
    await add_col("activity_log", "related_user_id",  "UUID")
    await add_col("activity_log", "is_rewatch",       "BOOLEAN DEFAULT false")

    try:
        # Drop restrictive activity_type check that blocks new activity types like on_hold and dropped
        await db.execute(text("ALTER TABLE public.activity_log DROP CONSTRAINT IF EXISTS activity_log_activity_type_check"))
    except Exception as e:
        logger.warning(f"Failed to drop activity_log_activity_type_check constraint: {e}")
        await db.rollback()


    await db.execute(text('''
        CREATE TABLE IF NOT EXISTS public.error_logs (
            id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
            event_name text NOT NULL,
            message text NOT NULL,
            stack_trace text,
            request_id text,
            path text,
            method text,
            status_code integer,
            metadata jsonb,
            created_at timestamptz DEFAULT now()
        )
    '''))

    await db.execute(text('''
        CREATE TABLE IF NOT EXISTS public.landing_posters (
            id          serial PRIMARY KEY,
            poster_url  TEXT NOT NULL UNIQUE,
            tmdb_id     INTEGER,
            title       TEXT,
            fetched_at  TIMESTAMPTZ DEFAULT now()
        )
    '''))

    await db.execute(text('''
        CREATE TABLE IF NOT EXISTS public.calendar_alerts (
            id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
            user_id uuid NOT NULL REFERENCES public.profiles(id) ON DELETE CASCADE,
            content_id uuid NOT NULL REFERENCES public.content(id) ON DELETE CASCADE,
            created_at timestamptz DEFAULT now(),
            UNIQUE(user_id, content_id)
        )
    '''))

    await add_col("collections", "pin_order", "INTEGER DEFAULT 0")

    # Optimize profiles email lookup
    try:
        await db.execute(text("CREATE INDEX IF NOT EXISTS idx_profiles_email ON public.profiles(email)"))
    except Exception as e:
        logger.warning(f"idx_profiles_email error: {e}")
        await db.rollback()

    # Optimize content queries
    try:
        await db.execute(text("CREATE INDEX IF NOT EXISTS idx_content_type_rating_sync ON public.content(content_type, external_rating DESC NULLS LAST, last_synced_at DESC NULLS LAST)"))
        await db.execute(text("CREATE INDEX IF NOT EXISTS idx_content_type_synced ON public.content(content_type, last_synced_at DESC NULLS LAST)"))
        await db.execute(text("CREATE INDEX IF NOT EXISTS idx_content_type_release ON public.content(content_type, release_date DESC NULLS LAST)"))
    except Exception as e:
        logger.warning(f"content index error: {e}")
        await db.rollback()

    # Optimize user_content_status query
    try:
        await db.execute(text("CREATE INDEX IF NOT EXISTS idx_user_content_status_user_status_updated ON public.user_content_status(user_id, status, updated_at DESC)"))
        await db.execute(text("CREATE INDEX IF NOT EXISTS idx_reviews_created ON public.reviews(created_at DESC)"))
        await db.execute(text("CREATE INDEX IF NOT EXISTS idx_reviews_user_created ON public.reviews(user_id, created_at DESC)"))
        await db.execute(text("CREATE INDEX IF NOT EXISTS idx_reviews_content ON public.reviews(content_id)"))
        await db.execute(text("CREATE INDEX IF NOT EXISTS idx_watch_history_user_watched ON public.watch_history(user_id, watched_at DESC)"))
        await db.execute(text("CREATE INDEX IF NOT EXISTS idx_messages_conv_sent ON public.messages(conversation_id, sent_at DESC)"))
        await db.execute(text("CREATE INDEX IF NOT EXISTS idx_review_likes_review_user ON public.review_likes(review_id, user_id)"))
        await db.execute(text("CREATE INDEX IF NOT EXISTS idx_content_credits_cid_role ON public.content_credits(content_id, role)"))
    except Exception as e:
        logger.warning(f"user_content_status index error: {e}")
        await db.rollback()

    await add_col("reviews", "imported_from", "TEXT")
    await add_col("watch_history", "imported_from", "TEXT")
    await add_col("user_content_status", "imported_from", "TEXT")
    await add_col("messages", "shared_meta", "JSONB")
    try:
        await db.execute(text("ALTER TABLE public.messages DROP CONSTRAINT IF EXISTS messages_message_type_check"))
    except Exception as e:
        logger.warning(f"Failed to drop messages_message_type_check constraint: {e}")

    await db.execute(text('''
        CREATE TABLE IF NOT EXISTS public.letterboxd_slug_map (
            slug TEXT PRIMARY KEY,
            content_id uuid NOT NULL REFERENCES public.content(id) ON DELETE CASCADE
        )
    '''))

    await db.commit()
    logger.info("Critical schema initialization completed.")

async def init_db_data_healing(db: AsyncSession):
    """
    HEAVY tasks (Global re-indexing, backfills).
    Runs in background AFTER startup to avoid Render timeouts.
    """
    logger.info("Starting background database data healing...")

    # 1. Watched collection backfill
    try:
        await db.execute(text('''
            INSERT INTO collections (user_id, name, description, is_public, collection_type,
                                    is_default, is_deletable, is_pinned, pin_order)
            SELECT p.id, 'Watched', 'All content I have watched', false, 'watched', true, false, true, 3
            FROM profiles p
            WHERE p.is_deleted = false
              AND NOT EXISTS (
                  SELECT 1 FROM collections c WHERE c.user_id = p.id AND c.name = 'Watched'
              )
        '''))
        await db.execute(text("UPDATE collections SET collection_type = 'watchlist' WHERE name = 'Watchlist' AND collection_type NOT IN ('watchlist')"))
        await db.execute(text("UPDATE collections SET collection_type = 'dropped' WHERE name = 'Dropped' AND collection_type NOT IN ('dropped')"))
        await db.execute(text("UPDATE collections SET collection_type = 'watched' WHERE name = 'Watched' AND collection_type NOT IN ('watched')"))
        await db.execute(text("UPDATE collections SET collection_type = 'custom' WHERE collection_type = 'user'"))
        await db.execute(text("UPDATE collections SET is_public = true, visibility = 'public' WHERE is_default = true OR collection_type IN ('watchlist', 'watched', 'dropped', 'favorites')"))
        await db.execute(text('''
            INSERT INTO collection_items (collection_id, content_id, added_by)
            SELECT c.id, ucs.content_id, ucs.user_id
            FROM user_content_status ucs
            JOIN collections c ON c.user_id = ucs.user_id AND c.name = 'Watched'
            WHERE ucs.is_watched = true
            ON CONFLICT (collection_id, content_id) DO NOTHING
        '''))
        await db.execute(text('''
            UPDATE collections c
            SET item_count = (SELECT COUNT(*) FROM collection_items ci WHERE ci.collection_id = c.id),
                updated_at = now()
            WHERE c.name = 'Watched'
        '''))
        logger.info("Watched collection backfill completed.")
        await db.commit()
    except Exception as e:
        logger.warning(f"Backfill warning: {e}")
    # 2. Tracking Status Migration (Phase 1)
    try:
        # Migrate Completed
        await db.execute(text('''
            UPDATE user_content_status 
            SET status = 'completed', updated_at = now()
            WHERE is_watched = true AND (status IS NULL OR status = 'none')
        '''))
        # Migrate Dropped
        await db.execute(text('''
            UPDATE user_content_status 
            SET status = 'dropped', updated_at = now()
            WHERE is_dropped = true AND (status IS NULL OR status = 'none')
        '''))
        # Migrate Plan to Watch (Interested)
        await db.execute(text('''
            UPDATE user_content_status 
            SET status = 'plan_to_watch', updated_at = now()
            WHERE is_interested = true AND (status IS NULL OR status = 'none')
        '''))
        logger.info("Tracking status migration completed.")
        await db.commit()
    except Exception as e:
        logger.warning(f"Status migration warning: {e}")
        await db.rollback()

    # 3. Global Stats Healing
    try:
        await db.execute(text('''
            INSERT INTO user_stats (user_id)
            SELECT id FROM profiles WHERE is_deleted = false
            ON CONFLICT (user_id) DO NOTHING
        '''))
        
        posts_exist = (await db.execute(text("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'posts')"))).scalar()

        sql = '''
            UPDATE user_stats us
            SET total_reviews = (SELECT COUNT(*) FROM reviews r WHERE r.user_id = us.user_id AND r.is_deleted = false),
                total_posts = (SELECT COUNT(*) FROM reviews r WHERE r.user_id = us.user_id AND r.is_deleted = false)
        '''
        if posts_exist:
             sql += ' + (SELECT COUNT(*) FROM posts p WHERE p.user_id = us.user_id)'
        sql += ", updated_at = now()"
        
        await db.execute(text(sql))
        logger.info("Global user stats healing completed.")
        await db.commit()
    except Exception as e:
        logger.warning(f"Stats healing warning: {e}")
        await db.rollback()

    logger.info("Background database data healing completed successfully.")

async def main():
    from app.core.database import AsyncSessionLocal
    async with AsyncSessionLocal() as session:
        await init_db(session)
        await init_db_data_healing(session)

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
