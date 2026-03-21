import logging
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text

logger = logging.getLogger('mambo.db_init')

async def init_db(db: AsyncSession):
    """
    Ensures all tables exist and have the correct columns.
    This script handles both fresh installations and "healing" existing databases
    by adding missing columns to tables that already exist.
    """
    logger.info("Initializing database schema...")
    
    try:
        # 0. Core Extensions
        await db.execute(text("CREATE EXTENSION IF NOT EXISTS pgcrypto"))
        await db.commit()

        # 1. Profiles & Stats
        await db.execute(text('''
            CREATE TABLE IF NOT EXISTS public.profiles (
                id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
                username text UNIQUE NOT NULL,
                display_name text,
                avatar_url text,
                bio text,
                birthday date,
                gender text,
                email text,
                phone_number text,
                activity_visibility text DEFAULT 'public',
                favourites_visibility text DEFAULT 'public',
                reviews_visibility text DEFAULT 'public',
                push_notifications_enabled boolean DEFAULT true,
                is_verified boolean DEFAULT false,
                is_deleted boolean DEFAULT false,
                created_at timestamptz DEFAULT now(),
                updated_at timestamptz DEFAULT now()
            )
        '''))
        
        # Heal profiles
        cols_profiles = {
            "birthday": "DATE",
            "gender": "TEXT",
            "email": "TEXT",
            "phone_number": "TEXT",
            "activity_visibility": "TEXT DEFAULT 'public'",
            "favourites_visibility": "TEXT DEFAULT 'public'",
            "reviews_visibility": "TEXT DEFAULT 'public'",
            "push_notifications_enabled": "BOOLEAN DEFAULT true",
            "search_vector": "TSVECTOR"
        }
        for col, dtype in cols_profiles.items():
            await db.execute(text(f"ALTER TABLE public.profiles ADD COLUMN IF NOT EXISTS {col} {dtype}"))
        
        await db.execute(text("CREATE INDEX IF NOT EXISTS idx_profiles_search_vector ON profiles USING gin(search_vector)"))

        await db.execute(text('''
            CREATE TABLE IF NOT EXISTS public.user_stats (
                user_id uuid PRIMARY KEY REFERENCES public.profiles(id) ON DELETE CASCADE,
                friends_count integer DEFAULT 0,
                followers_count integer DEFAULT 0,
                following_count integer DEFAULT 0,
                total_watched integer DEFAULT 0,
                total_reviews integer DEFAULT 0,
                total_posts integer DEFAULT 0,
                updated_at timestamptz DEFAULT now()
            )
        '''))

        # 2. Content & Credits
        await db.execute(text('''
            CREATE TABLE IF NOT EXISTS public.content (
                id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
                tmdb_id integer UNIQUE,
                mal_id integer UNIQUE,
                content_type text NOT NULL,
                title text NOT NULL,
                original_title text,
                synopsis text,
                poster_url text,
                backdrop_url text,
                external_rating float,
                external_rating_source text,
                release_date date,
                status text,
                anime_studio text,
                total_episodes integer,
                genres text[] DEFAULT '{}',
                is_permanent boolean DEFAULT false,
                made_permanent_at timestamptz,
                last_synced_at timestamptz DEFAULT now(),
                created_at timestamptz DEFAULT now()
            )
        '''))
        
        # Heal content
        cols_content = {
            "tmdb_id": "INTEGER UNIQUE",
            "mal_id": "INTEGER UNIQUE",
            "original_title": "TEXT",
            "synopsis": "TEXT",
            "backdrop_url": "TEXT",
            "external_rating_source": "TEXT",
            "status": "TEXT",
            "anime_studio": "TEXT",
            "total_episodes": "INTEGER",
            "genres": "TEXT[] DEFAULT '{}'",
            "is_permanent": "BOOLEAN DEFAULT false",
            "made_permanent_at": "TIMESTAMPTZ",
            "last_synced_at": "TIMESTAMPTZ DEFAULT now()"
        }
        for col, dtype in cols_content.items():
            await db.execute(text(f"ALTER TABLE public.content ADD COLUMN IF NOT EXISTS {col} {dtype}"))

        await db.execute(text('''
            CREATE TABLE IF NOT EXISTS public.persons (
                id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
                tmdb_id integer UNIQUE,
                name text NOT NULL,
                profile_image_url text,
                created_at timestamptz DEFAULT now(),
                updated_at timestamptz DEFAULT now()
            )
        '''))

        await db.execute(text('''
            CREATE TABLE IF NOT EXISTS public.content_credits (
                id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
                content_id uuid NOT NULL REFERENCES public.content(id) ON DELETE CASCADE,
                person_id uuid NOT NULL REFERENCES public.persons(id) ON DELETE CASCADE,
                role text NOT NULL,
                character_name text,
                job text,
                department text,
                display_order integer DEFAULT 0,
                created_at timestamptz DEFAULT now(),
                UNIQUE(content_id, person_id, role)
            )
        '''))

        # 3. Social & Discussions
        await db.execute(text('''
            CREATE TABLE IF NOT EXISTS public.posts (
                id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
                user_id uuid NOT NULL REFERENCES public.profiles(id) ON DELETE CASCADE,
                content_id uuid REFERENCES public.content(id) ON DELETE SET NULL,
                title text,
                body text NOT NULL,
                media_urls text[] DEFAULT '{}',
                upvotes_count integer DEFAULT 0,
                comments_count integer DEFAULT 0,
                shares_count integer DEFAULT 0,
                saves_count integer DEFAULT 0,
                created_at timestamptz DEFAULT now(),
                updated_at timestamptz DEFAULT now()
            )
        '''))
        
        # Heal posts
        cols_posts = {
            "upvotes_count": "INTEGER DEFAULT 0",
            "comments_count": "INTEGER DEFAULT 0",
            "shares_count": "INTEGER DEFAULT 0",
            "saves_count": "INTEGER DEFAULT 0",
            "updated_at": "TIMESTAMPTZ DEFAULT now()"
        }
        for col, dtype in cols_posts.items():
            await db.execute(text(f"ALTER TABLE public.posts ADD COLUMN IF NOT EXISTS {col} {dtype}"))

        await db.execute(text('''
            CREATE TABLE IF NOT EXISTS public.reviews (
                id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
                user_id uuid NOT NULL REFERENCES public.profiles(id) ON DELETE CASCADE,
                content_id uuid NOT NULL REFERENCES public.content(id) ON DELETE CASCADE,
                rating float NOT NULL,
                text_review text,
                is_spoiler boolean DEFAULT false,
                likes_count integer DEFAULT 0,
                comments_count integer DEFAULT 0,
                shares_count integer DEFAULT 0,
                saves_count integer DEFAULT 0,
                is_deleted boolean DEFAULT false,
                created_at timestamptz DEFAULT now(),
                updated_at timestamptz DEFAULT now()
            )
        '''))
        
        # Heal reviews
        cols_reviews = {
            "likes_count": "INTEGER DEFAULT 0",
            "comments_count": "INTEGER DEFAULT 0",
            "shares_count": "INTEGER DEFAULT 0",
            "saves_count": "INTEGER DEFAULT 0",
            "is_deleted": "BOOLEAN DEFAULT false",
            "updated_at": "TIMESTAMPTZ DEFAULT now()"
        }
        for col, dtype in cols_reviews.items():
            await db.execute(text(f"ALTER TABLE public.reviews ADD COLUMN IF NOT EXISTS {col} {dtype}"))

        # Comment Tables
        await db.execute(text('''
            CREATE TABLE IF NOT EXISTS public.post_comments (
                id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
                post_id uuid NOT NULL REFERENCES public.posts(id) ON DELETE CASCADE,
                user_id uuid NOT NULL REFERENCES public.profiles(id) ON DELETE CASCADE,
                body text NOT NULL,
                parent_id uuid REFERENCES public.post_comments(id) ON DELETE CASCADE,
                is_deleted boolean DEFAULT false,
                created_at timestamptz DEFAULT now(),
                updated_at timestamptz DEFAULT now()
            )
        '''))

        await db.execute(text('''
            CREATE TABLE IF NOT EXISTS public.review_comments (
                id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
                review_id uuid NOT NULL REFERENCES public.reviews(id) ON DELETE CASCADE,
                user_id uuid NOT NULL REFERENCES public.profiles(id) ON DELETE CASCADE,
                body text NOT NULL,
                parent_id uuid REFERENCES public.review_comments(id) ON DELETE CASCADE,
                is_deleted boolean DEFAULT false,
                created_at timestamptz DEFAULT now(),
                updated_at timestamptz DEFAULT now()
            )
        '''))

        # Like/Upvote Tables
        await db.execute(text('''
            CREATE TABLE IF NOT EXISTS public.post_upvotes (
                user_id uuid NOT NULL REFERENCES public.profiles(id) ON DELETE CASCADE,
                post_id uuid NOT NULL REFERENCES public.posts(id) ON DELETE CASCADE,
                created_at timestamptz DEFAULT now(),
                PRIMARY KEY (user_id, post_id)
            )
        '''))

        await db.execute(text('''
            CREATE TABLE IF NOT EXISTS public.review_likes (
                user_id uuid NOT NULL REFERENCES public.profiles(id) ON DELETE CASCADE,
                review_id uuid NOT NULL REFERENCES public.reviews(id) ON DELETE CASCADE,
                created_at timestamptz DEFAULT now(),
                PRIMARY KEY (user_id, review_id)
            )
        '''))

        # 4. Social Relationships
        await db.execute(text('''
            CREATE TABLE IF NOT EXISTS public.friends (
                user_id1 uuid NOT NULL REFERENCES public.profiles(id) ON DELETE CASCADE,
                user_id2 uuid NOT NULL REFERENCES public.profiles(id) ON DELETE CASCADE,
                created_at timestamptz DEFAULT now(),
                PRIMARY KEY (user_id1, user_id2),
                CHECK (user_id1 < user_id2)
            )
        '''))

        await db.execute(text('''
            CREATE TABLE IF NOT EXISTS public.friend_requests (
                id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
                sender_id uuid NOT NULL REFERENCES public.profiles(id) ON DELETE CASCADE,
                receiver_id uuid NOT NULL REFERENCES public.profiles(id) ON DELETE CASCADE,
                status text DEFAULT 'pending',
                created_at timestamptz DEFAULT now(),
                updated_at timestamptz DEFAULT now(),
                UNIQUE(sender_id, receiver_id)
            )
        '''))

        await db.execute(text('''
            CREATE TABLE IF NOT EXISTS public.follows (
                follower_id uuid NOT NULL REFERENCES public.profiles(id) ON DELETE CASCADE,
                following_id uuid NOT NULL REFERENCES public.profiles(id) ON DELETE CASCADE,
                created_at timestamptz DEFAULT now(),
                PRIMARY KEY (follower_id, following_id)
            )
        '''))

        await db.execute(text('''
            CREATE TABLE IF NOT EXISTS public.muted_users (
                muter_id uuid NOT NULL REFERENCES public.profiles(id) ON DELETE CASCADE,
                muted_id uuid NOT NULL REFERENCES public.profiles(id) ON DELETE CASCADE,
                created_at timestamptz DEFAULT now(),
                PRIMARY KEY (muter_id, muted_id)
            )
        '''))

        await db.execute(text('''
            CREATE TABLE IF NOT EXISTS public.blocked_users (
                blocker_id uuid NOT NULL REFERENCES public.profiles(id) ON DELETE CASCADE,
                blocked_id uuid NOT NULL REFERENCES public.profiles(id) ON DELETE CASCADE,
                created_at timestamptz DEFAULT now(),
                PRIMARY KEY (blocker_id, blocked_id)
            )
        '''))

        # 5. User Activity & Status
        await db.execute(text('''
            CREATE TABLE IF NOT EXISTS public.user_content_status (
                user_id uuid NOT NULL REFERENCES public.profiles(id) ON DELETE CASCADE,
                content_id uuid NOT NULL REFERENCES public.content(id) ON DELETE CASCADE,
                is_watched boolean DEFAULT false,
                is_liked boolean DEFAULT false,
                is_dropped boolean DEFAULT false,
                is_interested boolean DEFAULT false,
                watch_count integer DEFAULT 0,
                rating float,
                first_watched_at timestamptz,
                last_watched_at timestamptz,
                updated_at timestamptz DEFAULT now(),
                PRIMARY KEY (user_id, content_id)
            )
        '''))

        await db.execute(text('''
            CREATE TABLE IF NOT EXISTS public.watch_history (
                id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
                user_id uuid NOT NULL REFERENCES public.profiles(id) ON DELETE CASCADE,
                content_id uuid NOT NULL REFERENCES public.content(id) ON DELETE CASCADE,
                watch_type text DEFAULT 'first_watch',
                watched_at timestamptz DEFAULT now()
            )
        '''))

        await db.execute(text('''
            CREATE TABLE IF NOT EXISTS public.activity_log (
                id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
                user_id uuid NOT NULL REFERENCES public.profiles(id) ON DELETE CASCADE,
                activity_type text NOT NULL,
                content_id uuid REFERENCES public.content(id) ON DELETE SET NULL,
                review_id uuid,
                post_id uuid,
                collection_id uuid,
                news_id uuid,
                related_user_id uuid,
                details jsonb,
                visibility text DEFAULT 'public',
                created_at timestamptz DEFAULT now()
            )
        '''))

        # 6. Collections
        await db.execute(text('''
            CREATE TABLE IF NOT EXISTS public.collections (
                id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
                user_id uuid NOT NULL REFERENCES public.profiles(id) ON DELETE CASCADE,
                name text NOT NULL,
                description text,
                collection_type text DEFAULT 'user',
                is_default boolean DEFAULT false,
                is_deletable boolean DEFAULT true,
                item_count integer DEFAULT 0,
                is_public boolean DEFAULT true,
                created_at timestamptz DEFAULT now(),
                updated_at timestamptz DEFAULT now()
            )
        '''))

        await db.execute(text('''
            CREATE TABLE IF NOT EXISTS public.collection_items (
                collection_id uuid NOT NULL REFERENCES public.collections(id) ON DELETE CASCADE,
                content_id uuid NOT NULL REFERENCES public.content(id) ON DELETE CASCADE,
                added_by uuid REFERENCES public.profiles(id) ON DELETE SET NULL,
                added_at timestamptz DEFAULT now(),
                PRIMARY KEY (collection_id, content_id)
            )
        '''))

        # 7. Communications
        await db.execute(text('''
            CREATE TABLE IF NOT EXISTS public.conversations (
                id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
                conversation_type text DEFAULT 'direct',
                direct_pair_key text UNIQUE,
                title text,
                last_message_id uuid,
                last_message_at timestamptz,
                created_by uuid REFERENCES public.profiles(id),
                created_at timestamptz DEFAULT now(),
                updated_at timestamptz DEFAULT now()
            )
        '''))
        
        # Heal conversations
        await db.execute(text("ALTER TABLE public.conversations ADD COLUMN IF NOT EXISTS direct_pair_key TEXT UNIQUE"))
        await db.execute(text("CREATE UNIQUE INDEX IF NOT EXISTS uq_direct_pair ON public.conversations(direct_pair_key)"))

        await db.execute(text('''
            CREATE TABLE IF NOT EXISTS public.conversation_members (
                conversation_id uuid NOT NULL REFERENCES public.conversations(id) ON DELETE CASCADE,
                user_id uuid NOT NULL REFERENCES public.profiles(id) ON DELETE CASCADE,
                joined_at timestamptz DEFAULT now(),
                PRIMARY KEY (conversation_id, user_id)
            )
        '''))

        await db.execute(text('''
            CREATE TABLE IF NOT EXISTS public.messages (
                id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
                conversation_id uuid NOT NULL REFERENCES public.conversations(id) ON DELETE CASCADE,
                sender_id uuid NOT NULL REFERENCES public.profiles(id),
                receiver_id uuid,
                body text,
                message_type text DEFAULT 'text',
                shared_post_id uuid,
                shared_review_id uuid,
                shared_content_id uuid,
                is_read boolean DEFAULT false,
                read_at timestamptz,
                sent_at timestamptz DEFAULT now()
            )
        '''))
        
        # Heal messages
        await db.execute(text("ALTER TABLE public.messages ADD COLUMN IF NOT EXISTS shared_content_id UUID"))

        await db.execute(text('''
            CREATE TABLE IF NOT EXISTS public.notifications (
                id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
                user_id uuid NOT NULL REFERENCES public.profiles(id) ON DELETE CASCADE,
                actor_id uuid REFERENCES public.profiles(id) ON DELETE SET NULL,
                related_id uuid,
                title text,
                message text NOT NULL,
                type varchar(50) NOT NULL,
                is_read boolean DEFAULT false,
                read_at timestamptz,
                is_deleted boolean DEFAULT false,
                last_updated_at timestamptz,
                created_at timestamptz DEFAULT now()
            )
        '''))
        
        # Heal notifications (from NotificationService logic)
        await db.execute(text("ALTER TABLE public.notifications ADD COLUMN IF NOT EXISTS actor_id UUID"))
        await db.execute(text("ALTER TABLE public.notifications ADD COLUMN IF NOT EXISTS related_id UUID"))
        await db.execute(text("ALTER TABLE public.notifications ALTER COLUMN title DROP NOT NULL"))
        await db.execute(text("ALTER TABLE public.notifications ALTER COLUMN type TYPE VARCHAR(50)"))

        # 8. Others
        await db.execute(text('''
            CREATE TABLE IF NOT EXISTS public.news_articles (
                id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
                title text NOT NULL,
                description text,
                image_url text,
                source_name text,
                url text UNIQUE,
                category text,
                likes_count integer DEFAULT 0,
                published_at timestamptz,
                created_at timestamptz DEFAULT now()
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
            CREATE TABLE IF NOT EXISTS public.curated_content (
                id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
                title text NOT NULL,
                description text,
                image_url text,
                link_url text,
                content_id uuid REFERENCES public.content(id),
                category text NOT NULL,
                priority integer DEFAULT 0,
                is_active boolean DEFAULT true,
                created_at timestamptz DEFAULT now()
            )
        '''))

        await db.execute(text('''
            CREATE TABLE IF NOT EXISTS public.user_favorite_genres (
                user_id uuid NOT NULL REFERENCES public.profiles(id) ON DELETE CASCADE,
                genre_name text NOT NULL,
                PRIMARY KEY (user_id, genre_name)
            )
        '''))

        await db.commit()
        logger.info("Database initialization and healing completed successfully.")
        
    except Exception as e:
        await db.rollback()
        logger.error(f"Database initialization failed: {e}")
        # We don't raise here to allow the app to attempt starting anyway,
        # but in production, this might lead to downstream errors.
        raise e
