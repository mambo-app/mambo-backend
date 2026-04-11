import asyncio
import sys
import os
from uuid import UUID
from sqlalchemy import text

# Add parent directory to path so we can import app modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.core.database import AsyncSessionLocal
from app.core.supabase import supabase_admin

USER_1_ID = "6ab7ed7a-a7ed-4ac8-bc31-2f3c111219ae"
USER_2_ID = "c03b7cf1-57cb-4761-b32e-b21f0989de2e"

TARGET_IDS = [USER_1_ID, USER_2_ID]

async def purge_user_data():
    async with AsyncSessionLocal() as db:
        try:
            # Get existing tables to avoid transaction abortion
            res = await db.execute(text("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"))
            existing_tables = {row[0] for row in res.fetchall()}
            
            for uid_str in TARGET_IDS:
                uid = UUID(uid_str)
                print(f"\n--- PURGING USER: {uid_str} ---")

                # --- PART 1: SUPABASE AUTH (NEW) ---
                print("  > Deleting Supabase Auth account")
                try:
                    # Using the admin API to permanently delete the auth record
                    auth_res = supabase_admin.auth.admin.delete_user(uid_str)
                    print(f"    Supabase Auth Result: Success")
                except Exception as se:
                    print(f"    Supabase Auth Result: Already gone or error ({se})")

                # --- PART 2: NEON DATABASE (IDEMPOTENT) ---
                # Find all conversations the user is part of
                if 'conversation_members' in existing_tables:
                    res = await db.execute(text('''
                        SELECT conversation_id 
                        FROM conversation_members 
                        WHERE user_id = :uid
                    '''), {'uid': uid})
                    conv_ids = [row[0] for row in res.fetchall()]
                    
                    for cid in conv_ids:
                        count_res = await db.execute(text('SELECT COUNT(*) FROM conversation_members WHERE conversation_id = :cid'), {'cid': cid})
                        member_count = count_res.scalar()
                        
                        if member_count <= 2:
                            print(f"  > Deleting 1:1 conversation {cid}")
                            if 'conversations' in existing_tables:
                                await db.execute(text('UPDATE conversations SET last_message_id = NULL WHERE id = :cid'), {'cid': cid})
                            if 'messages' in existing_tables:
                                await db.execute(text('DELETE FROM messages WHERE conversation_id = :cid'), {'cid': cid})
                            await db.execute(text('DELETE FROM conversation_members WHERE conversation_id = :cid'), {'cid': cid})
                            if 'conversations' in existing_tables:
                                await db.execute(text('DELETE FROM conversations WHERE id = :cid'), {'cid': cid})
                        else:
                            print(f"  > Removing membership from group conversation {cid}")
                            if 'messages' in existing_tables:
                                await db.execute(text('DELETE FROM messages WHERE conversation_id = :cid AND sender_id = :uid'), {'cid': cid, 'uid': uid})
                            await db.execute(text('DELETE FROM conversation_members WHERE conversation_id = :cid AND user_id = :uid'), {'cid': cid, 'uid': uid})

                # General Data Cleanup
                if 'notifications' in existing_tables:
                    await db.execute(text('DELETE FROM notifications WHERE user_id = :uid OR actor_id = :uid'), {'uid': uid})
                if 'friend_requests' in existing_tables:
                    await db.execute(text('DELETE FROM friend_requests WHERE sender_id = :uid OR receiver_id = :uid'), {'uid': uid})
                if 'friends' in existing_tables:
                    await db.execute(text('DELETE FROM friends WHERE user_id1 = :uid OR user_id2 = :uid'), {'uid': uid})
                if 'posts' in existing_tables:
                    await db.execute(text('DELETE FROM posts WHERE user_id = :uid'), {'uid': uid})
                if 'reviews' in existing_tables:
                    await db.execute(text('DELETE FROM reviews WHERE user_id = :uid'), {'uid': uid})

                fav_tables = ['user_favorites', 'favorite_persons', 'favourite_genres', 'content_saves']
                for tbl in fav_tables:
                    if tbl in existing_tables:
                        await db.execute(text(f'DELETE FROM {tbl} WHERE user_id = :uid'), {'uid': uid})

                if 'user_stats' in existing_tables:
                    await db.execute(text('DELETE FROM user_stats WHERE user_id = :uid'), {'uid': uid})
                if 'profiles' in existing_tables:
                    await db.execute(text('DELETE FROM profiles WHERE id = :uid'), {'uid': uid})
                
                print(f"--- PURGE COMPLETE FOR {uid_str} ---")

            await db.commit()
            print("\nALL PURGES COMMITTED SUCCESSFULLY.")
        except Exception as e:
            await db.rollback()
            print(f"\nCRITICAL ERROR: {e}")
            raise e

if __name__ == "__main__":
    asyncio.run(purge_user_data())
