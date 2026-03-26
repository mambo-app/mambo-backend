import asyncio
import sys
import os

# Add parent directory to path to allow importing app modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import text
from app.core.database import AsyncSessionLocal
from app.core.supabase import supabase_admin
from uuid import UUID

async def wipe_user(user_identifier: str):
    """
    Completely removes a user from the system:
    1. Finds User ID from Neon (profiles table)
    2. Deletes from all auxiliary tables in Neon
    3. Deletes from Supabase Auth
    4. Deletes from Supabase Storage (avatars & posts)
    """
    async with AsyncSessionLocal() as db:
        # 1. Find the user
        print(f"Searching for user: {user_identifier}...")
        result = await db.execute(text('''
            SELECT id, username, email FROM profiles 
            WHERE username = :id OR email = :id OR id::text = :id
        '''), {'id': user_identifier})
        user = result.mappings().first()
        
        if not user:
            print(f"Error: User '{user_identifier}' not found in profiles.")
            return

        user_id = str(user['id'])
        username = user['username']
        print(f"Found User: {username} ({user_id})")

        # 2. Delete from Neon Tables (ORDER MATTERS due to potential FKs)
        # We delete from dependent tables first.
        tables_to_clean = [
            # Social / Relationships
            ("friend_requests", "sender_id"), ("friend_requests", "receiver_id"),
            ("friends", "user_id1"), ("friends", "user_id2"),
            ("follows", "follower_id"), ("follows", "following_id"),
            ("muted_users", "muter_id"), ("muted_users", "muted_id"),
            ("blocked_users", "blocker_id"), ("blocked_users", "blocked_id"),
            
            # Interactions
            ("post_upvotes", "user_id"), ("review_likes", "user_id"),
            ("post_comments", "user_id"), ("review_comments", "user_id"),
            
            # Content (posts/reviews themselves)
            ("posts", "user_id"), ("reviews", "user_id"),
            
            # Preferences / Logs
            ("activity_log", "user_id"), ("user_content_status", "user_id"),
            ("notifications", "user_id"), ("push_tokens", "user_id"),
            ("social_links", "user_id"), ("user_badges", "user_id"),
            ("user_actor_preferences", "user_id"), ("user_director_preferences", "user_id"),
            ("user_favorite_genres", "user_id"),
            
            # Recommendations
            ("recommendation_recipients", "recipient_id"),
            ("recommendations", "sender_id"),
            
            # Messaging
            ("messages", "sender_id"), ("messages", "receiver_id"),
            
            # Stats & Profile
            ("user_stats", "user_id"),
            ("profiles", "id")
        ]

        print("Wiping data from Neon database...")
        try:
            # A. Handle collection items first because they depend on collections
            await db.execute(text('''
                DELETE FROM collection_items 
                WHERE collection_id IN (SELECT id FROM collections WHERE user_id = :uid)
            '''), {'uid': user_id})
            await db.execute(text("DELETE FROM collections WHERE user_id = :uid"), {'uid': user_id})

            # B. Bulk delete from the list
            for table_info in tables_to_clean:
                table, col = table_info
                res = await db.execute(text(f"DELETE FROM {table} WHERE {col} = :uid"), {'uid': user_id})
                if res.rowcount > 0:
                    print(f"  - Deleted {res.rowcount} rows from {table}")
            
            await db.commit()
            print("Neon data wipe successful.")
        except Exception as e:
            await db.rollback()
            print(f"Neon data wipe failed: {e}")

        # 3. Delete from Supabase Storage
        print("Cleaning up storage...")
        for bucket in ["avatars", "posts"]:
            try:
                # List all files in user's folder (Supabase storage usually organizes by user_id/filename)
                files = supabase_admin.storage.from_(bucket).list(user_id)
                if files:
                    file_paths = [f"{user_id}/{f['name']}" for f in files]
                    supabase_admin.storage.from_(bucket).remove(file_paths)
                    print(f"  - Deleted {len(file_paths)} files from {bucket} bucket.")
            except Exception as e:
                print(f"  - {bucket} cleanup failed: {e}")

        # 4. Delete from Supabase Auth
        print("Deleting from Supabase Auth...")
        try:
            supabase_admin.auth.admin.delete_user(user_id)
            print("Supabase Auth deletion successful.")
        except Exception as e:
            print(f"Supabase Auth deletion failed: {e}")

        print("\n=== SYSTEM WIPE COMPLETE ===")
        print(f"User '{username}' has been completely removed from Mambo.")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python wipe_user.py <username_or_email_or_id>")
        sys.exit(1)
    
    asyncio.run(wipe_user(sys.argv[1]))
