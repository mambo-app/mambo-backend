import logging
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text

logger = logging.getLogger('mambo.db_init')

async def init_db(db: AsyncSession):
    """
    Heals the existing Neon database by adding any missing columns to existing tables.
    Uses ALTER TABLE ... ADD COLUMN IF NOT EXISTS so it's fully safe to run repeatedly.
    The actual table structure was defined via the Neon dashboard DDL.
    """
    logger.info("Running database schema healing...")

    try:
        await db.execute(text("CREATE EXTENSION IF NOT EXISTS pgcrypto"))
        await db.commit()
    except Exception as e:
        logger.warning(f"pgcrypto extension: {e}")
        await db.rollback()

    # Helper to run ALTER TABLE safely
    async def add_col(table: str, col: str, dtype: str):
        try:
            await db.execute(text(
                f"ALTER TABLE public.{table} ADD COLUMN IF NOT EXISTS {col} {dtype}"
            ))
        except Exception as e:
            logger.warning(f"add_col {table}.{col}: {e}")
            await db.rollback()

    # ── profiles ───────────────────────────────────────────────────────────────
    await add_col("profiles", "bio",                         "TEXT")
    await add_col("profiles", "birthday",                    "DATE")
    await add_col("profiles", "gender",                      "TEXT")
    await add_col("profiles", "email",                       "TEXT")
    await add_col("profiles", "phone_number",                "TEXT")
    await add_col("profiles", "activity_visibility",         "TEXT DEFAULT 'public'")
    await add_col("profiles", "favourites_visibility",       "TEXT DEFAULT 'public'")
    await add_col("profiles", "reviews_visibility",          "TEXT DEFAULT 'public'")
    await add_col("profiles", "push_notifications_enabled",  "BOOLEAN DEFAULT true")
    await add_col("profiles", "search_vector",               "TSVECTOR")
    await add_col("profiles", "is_deleted",                  "BOOLEAN DEFAULT false")
    await add_col("profiles", "updated_at",                  "TIMESTAMPTZ DEFAULT now()")

    # ── user_stats ─────────────────────────────────────────────────────────────
    await add_col("user_stats", "friends_count",   "INTEGER DEFAULT 0")
    await add_col("user_stats", "followers_count", "INTEGER DEFAULT 0")
    await add_col("user_stats", "following_count", "INTEGER DEFAULT 0")
    await add_col("user_stats", "total_watched",   "INTEGER DEFAULT 0")
    await add_col("user_stats", "total_reviews",   "INTEGER DEFAULT 0")
    await add_col("user_stats", "total_posts",     "INTEGER DEFAULT 0")
    await add_col("user_stats", "updated_at",      "TIMESTAMPTZ DEFAULT now()")

    # ── content ────────────────────────────────────────────────────────────────
    await add_col("content", "content_type",           "TEXT NOT NULL DEFAULT 'movie'")
    await add_col("content", "tmdb_id",                "INTEGER")
    await add_col("content", "mal_id",                 "INTEGER")
    await add_col("content", "original_title",         "TEXT")
    await add_col("content", "synopsis",               "TEXT")
    await add_col("content", "poster_url",             "TEXT")
    await add_col("content", "backdrop_url",           "TEXT")
    await add_col("content", "external_rating",        "FLOAT")
    await add_col("content", "external_rating_source", "TEXT")
    await add_col("content", "release_date",           "DATE")
    await add_col("content", "status",                 "TEXT")
    await add_col("content", "anime_studio",           "TEXT")
    await add_col("content", "total_episodes",         "INTEGER")
    await add_col("content", "genres",                 "TEXT[] DEFAULT '{}'")
    await add_col("content", "is_permanent",           "BOOLEAN DEFAULT false")
    await add_col("content", "made_permanent_at",      "TIMESTAMPTZ")
    await add_col("content", "last_synced_at",         "TIMESTAMPTZ DEFAULT now()")
    await add_col("content", "created_at",             "TIMESTAMPTZ DEFAULT now()")

    # ── reviews ────────────────────────────────────────────────────────────────
    # The actual reviews table may be missing these if created from old schema
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

    # Unique constraint for upsert
    try:
        await db.execute(text(
            "ALTER TABLE public.reviews ADD CONSTRAINT reviews_user_content_unique "
            "UNIQUE (user_id, content_id)"
        ))
    except Exception:
        pass  # Already exists

    # ── posts ──────────────────────────────────────────────────────────────────
    await add_col("posts", "upvotes_count",   "INTEGER DEFAULT 0")
    await add_col("posts", "comments_count",  "INTEGER DEFAULT 0")
    await add_col("posts", "shares_count",    "INTEGER DEFAULT 0")
    await add_col("posts", "saves_count",     "INTEGER DEFAULT 0")
    await add_col("posts", "updated_at",      "TIMESTAMPTZ DEFAULT now()")

    # ── collections ────────────────────────────────────────────────────────────
    await add_col("collections", "collection_type",  "TEXT DEFAULT 'user'")
    await add_col("collections", "is_default",       "BOOLEAN DEFAULT false")
    await add_col("collections", "is_deletable",     "BOOLEAN DEFAULT true")
    await add_col("collections", "item_count",       "INTEGER DEFAULT 0")
    await add_col("collections", "visibility",       "TEXT DEFAULT 'private'")
    await add_col("collections", "is_pinned",        "BOOLEAN DEFAULT false")
    await add_col("collections", "pin_order",        "INTEGER DEFAULT 0")
    await add_col("collections", "updated_at",       "TIMESTAMPTZ DEFAULT now()")

    # ── collection_items ───────────────────────────────────────────────────────
    await add_col("collection_items", "added_by",  "UUID")

    # ── conversations ──────────────────────────────────────────────────────────
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
        pass

    # ── messages ───────────────────────────────────────────────────────────────
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

    # ── notifications ──────────────────────────────────────────────────────────
    await add_col("notifications", "actor_id",             "UUID")
    await add_col("notifications", "related_id",           "UUID")
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
    try:
        await db.execute(text(
            "ALTER TABLE public.notifications ALTER COLUMN title DROP NOT NULL"
        ))
    except Exception:
        pass

    # ── news_articles ──────────────────────────────────────────────────────────
    await add_col("news_articles", "external_url",     "TEXT")
    await add_col("news_articles", "is_permanent",     "BOOLEAN DEFAULT false")
    await add_col("news_articles", "likes_count",      "INTEGER DEFAULT 0")
    await add_col("news_articles", "comments_count",   "INTEGER DEFAULT 0")
    await add_col("news_articles", "shares_count",     "INTEGER DEFAULT 0")
    await add_col("news_articles", "content",          "TEXT")
    await add_col("news_articles", "is_active",        "BOOLEAN DEFAULT true")
    await add_col("news_articles", "fetched_at",       "TIMESTAMPTZ DEFAULT now()")

    # ── persons ────────────────────────────────────────────────────────────────
    await add_col("persons", "mal_id",          "INTEGER")
    await add_col("persons", "bio",             "TEXT")
    await add_col("persons", "birth_date",      "DATE")
    await add_col("persons", "death_date",      "DATE")
    await add_col("persons", "place_of_birth",  "VARCHAR")
    await add_col("persons", "known_for",       "TEXT")
    await add_col("persons", "updated_at",      "TIMESTAMPTZ DEFAULT now()")

    # ── privacy_settings (create if missing) ───────────────────────────────────
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

    # ── push_tokens (create if missing) ────────────────────────────────────────
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

    # ── search_history ─────────────────────────────────────────────────────────
    await db.execute(text('''
        CREATE TABLE IF NOT EXISTS public.search_history (
            id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
            user_id uuid NOT NULL REFERENCES public.profiles(id) ON DELETE CASCADE,
            query text NOT NULL,
            content_type_filter text,
            searched_at timestamptz DEFAULT now()
        )
    '''))

    # ── reported_content (create if missing) ───────────────────────────────────
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

    # ── user_favorite_genres ───────────────────────────────────────────────────
    await db.execute(text('''
        CREATE TABLE IF NOT EXISTS public.user_favorite_genres (
            user_id uuid NOT NULL REFERENCES public.profiles(id) ON DELETE CASCADE,
            genre_name text NOT NULL,
            created_at timestamptz DEFAULT now(),
            PRIMARY KEY (user_id, genre_name)
        )
    '''))

    # ── curated_content ────────────────────────────────────────────────────────
    await add_col("curated_content", "content_type", "TEXT")
    await add_col("curated_content", "source_id",    "TEXT")
    await add_col("curated_content", "link_url",     "TEXT")
    await add_col("curated_content", "content_id",   "UUID")

    # ── watch_history ──────────────────────────────────────────────────────────
    await add_col("watch_history", "watch_type",  "TEXT DEFAULT 'first_watch'")

    # ── activity_log ───────────────────────────────────────────────────────────
    await add_col("activity_log", "news_id",          "UUID")
    await add_col("activity_log", "details",          "JSONB DEFAULT '{}'")
    await add_col("activity_log", "related_user_id",  "UUID")

    # ── Watched collection backfill ────────────────────────────────────────────
    # For every user that has watched content but is missing a "Watched" collection:
    # 1. Create the collection
    # 2. Populate it from user_content_status (is_watched = true)
    # 3. Fix item_count
    # This block is 100% idempotent — safe to run on every startup.
    try:
        # Step 1: Create missing "Watched" collections for all existing users
        await db.execute(text('''
            INSERT INTO collections (user_id, name, description, is_public, collection_type,
                                    is_default, is_deletable, is_pinned, pin_order)
            SELECT p.id,
                   'Watched',
                   'All content I have watched',
                   false,
                   'watched',
                   true,
                   false,
                   true,
                   3
            FROM profiles p
            WHERE p.is_deleted = false
              AND NOT EXISTS (
                  SELECT 1 FROM collections c
                  WHERE c.user_id = p.id AND c.name = 'Watched'
              )
        '''))

        # Step 1b: Fix collection_type for existing default collections
        await db.execute(text('''
            UPDATE collections SET collection_type = 'watchlist' WHERE name = 'Watchlist' AND collection_type NOT IN ('watchlist');
            UPDATE collections SET collection_type = 'dropped' WHERE name = 'Dropped' AND collection_type NOT IN ('dropped');
            UPDATE collections SET collection_type = 'watched' WHERE name = 'Watched' AND collection_type NOT IN ('watched');
        '''))

        # Step 2: Backfill collection_items from user_content_status for all "Watched" collections
        await db.execute(text('''
            INSERT INTO collection_items (collection_id, content_id, added_by)
            SELECT c.id, ucs.content_id, ucs.user_id
            FROM user_content_status ucs
            JOIN collections c ON c.user_id = ucs.user_id AND c.name = 'Watched'
            WHERE ucs.is_watched = true
            ON CONFLICT (collection_id, content_id) DO NOTHING
        '''))

        # Step 3: Sync item_count to the actual number of items in each "Watched" collection
        await db.execute(text('''
            UPDATE collections c
            SET item_count = (
                SELECT COUNT(*) FROM collection_items ci WHERE ci.collection_id = c.id
            ),
            updated_at = now()
            WHERE c.name = 'Watched'
        '''))

        logger.info("Watched collection backfill completed successfully.")
    except Exception as e:
        logger.warning(f"Watched collection backfill warning (non-fatal): {e}")
        # Dont rollback the whole thing if just collection sync failed
        # await db.rollback() 

    # ── Global Stats Healing ──────────────────────────────────────────────────
    try:
        logger.info("Starting global user stats healing (total_reviews, total_posts)...")
        # Ensure all users have a user_stats entry
        await db.execute(text('''
            INSERT INTO user_stats (user_id)
            SELECT id FROM profiles
            WHERE is_deleted = false
            ON CONFLICT (user_id) DO NOTHING
        '''))

        # Check if 'posts' table exists before trying to query it
        posts_exist = await db.execute(text("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'posts')"))
        posts_exist = posts_exist.scalar()

        # Recalculate total_reviews and total_posts for all users
        # This update ensures existing reviews are counted as posts
        sql = '''
            UPDATE user_stats us
            SET total_reviews = (
                SELECT COUNT(*) FROM reviews r 
                WHERE r.user_id = us.user_id AND r.is_deleted = false
            ),
            total_posts = (
                SELECT COUNT(*) FROM reviews r 
                WHERE r.user_id = us.user_id AND r.is_deleted = false
            )
        '''
        if posts_exist:
             sql += ''' + (
                SELECT COUNT(*) FROM posts p 
                WHERE p.user_id = us.user_id
            )'''
        
        sql += ", updated_at = now()"
        
        res = await db.execute(text(sql))
        logger.info(f"Global user stats healing completed. Updated {res.rowcount} user_stats rows.")
        await db.commit() # Commit this part specifically
    except Exception as e:
        logger.warning(f"Global stats healing warning (non-fatal): {e}")
        await db.rollback()

    try:
        await db.commit()
        logger.info("Database schema healing completed successfully.")
    except Exception as e:
        await db.rollback()
        logger.error(f"Database schema healing commit failed: {e}")
        raise e
