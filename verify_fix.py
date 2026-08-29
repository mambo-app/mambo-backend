import asyncio
import uuid
from datetime import date, datetime
from sqlalchemy import text
from app.core.database import AsyncSessionLocal
from app.services.action_service import ActionService
from app.services.social_service import SocialService
from app.repositories.social_repo import SocialRepository
from app.routes.v1.feed import get_activity_feed
from app.models.action import ContentActionRequest, ActionType

async def run_verification():
    user_a_id = uuid.uuid4()
    user_b_id = uuid.uuid4()
    content_id = uuid.uuid4()
    review_id = uuid.uuid4()
    activity_id = uuid.uuid4()
    
    username_a = f"test_user_a_{uuid.uuid4().hex[:6]}"
    username_b = f"test_user_b_{uuid.uuid4().hex[:6]}"
    
    print("=== STARTING BACKEND FIX VERIFICATION ===")
    
    async with AsyncSessionLocal() as db:
        try:
            # 1. Setup Mock Profiles
            print("Creating test profiles...")
            await db.execute(text("""
                INSERT INTO profiles (id, username, display_name, created_at, updated_at)
                VALUES (:uid, :username, :display_name, now(), now())
            """), {'uid': user_a_id, 'username': username_a, 'display_name': 'Test User A'})
            
            await db.execute(text("""
                INSERT INTO profiles (id, username, display_name, created_at, updated_at)
                VALUES (:uid, :username, :display_name, now(), now())
            """), {'uid': user_b_id, 'username': username_b, 'display_name': 'Test User B'})
            
            # Setup User Stats
            await db.execute(text("""
                INSERT INTO user_stats (user_id, total_watched, total_reviews, total_posts, followers_count, following_count, friends_count, updated_at)
                VALUES (:uid, 0, 0, 0, 0, 0, 0, now())
            """), {'uid': user_a_id})
            await db.execute(text("""
                INSERT INTO user_stats (user_id, total_watched, total_reviews, total_posts, followers_count, following_count, friends_count, updated_at)
                VALUES (:uid, 0, 0, 0, 0, 0, 0, now())
            """), {'uid': user_b_id})
            
            # 2. Setup Mock Content
            print("Creating test content...")
            await db.execute(text("""
                INSERT INTO content (id, title, content_type, release_date, created_at)
                VALUES (:cid, :title, 'movie', :release_date, now())
            """), {'cid': content_id, 'title': 'Test Verification Movie', 'release_date': date.today()})
            
            await db.commit()
            print("Setup successful.")
            
        except Exception as e:
            print(f"ERROR DURING SETUP: {e}")
            await db.rollback()
            return

    # --- TEST 1: Watchlist Auto-Removal ---
    print("\n--- Running Test 1: Watchlist Auto-Removal ---")
    async with AsyncSessionLocal() as db:
        try:
            action_svc = ActionService(db)
            
            # Add content to user's Watchlist (plan_to_watch)
            print("Adding content to Watchlist...")
            await action_svc._sync_to_collection(user_a_id, content_id, 'Watchlist')
            await action_svc._update_status_flag(user_a_id, content_id, 'is_interested', True)
            
            await db.execute(text("""
                INSERT INTO user_content_status (user_id, content_id, status, is_interested, last_activity_at, updated_at)
                VALUES (:uid, :cid, 'plan_to_watch', true, now(), now())
                ON CONFLICT (user_id, content_id) DO UPDATE SET
                    status = 'plan_to_watch',
                    is_interested = true,
                    updated_at = now()
            """), {'uid': user_a_id, 'cid': content_id})
            await db.commit()
            
            # Verify Watchlist state
            col_res = await db.execute(text("""
                SELECT count(*) FROM collection_items ci
                JOIN collections c ON c.id = ci.collection_id
                WHERE c.user_id = :uid AND c.name = 'Watchlist' AND ci.content_id = :cid
            """), {'uid': user_a_id, 'cid': content_id})
            assert col_res.scalar() == 1, "Content should be in Watchlist collection initially"
            
            status_res = await db.execute(text("""
                SELECT is_interested, status FROM user_content_status WHERE user_id = :uid AND content_id = :cid
            """), {'uid': user_a_id, 'cid': content_id})
            status_row = status_res.mappings().one()
            assert status_row['is_interested'] is True, "is_interested should be True initially"
            print("Initial Watchlist state confirmed.")
            
            # Change status to 'watching'
            print("Calling handle_action to set status to 'watching'...")
            req = ContentActionRequest(action=ActionType.set_status, status='watching')
            await action_svc.handle_action(user_a_id, content_id, req)
            await db.commit()
            
            # Verify auto-removal
            col_res_after = await db.execute(text("""
                SELECT count(*) FROM collection_items ci
                JOIN collections c ON c.id = ci.collection_id
                WHERE c.user_id = :uid AND c.name = 'Watchlist' AND ci.content_id = :cid
            """), {'uid': user_a_id, 'cid': content_id})
            assert col_res_after.scalar() == 0, "Content should be removed from Watchlist collection"
            
            status_res_after = await db.execute(text("""
                SELECT is_interested, status FROM user_content_status WHERE user_id = :uid AND content_id = :cid
            """), {'uid': user_a_id, 'cid': content_id})
            status_row_after = status_res_after.mappings().one()
            assert status_row_after['is_interested'] is False, "is_interested should now be False"
            assert status_row_after['status'] == 'watching', "status should be 'watching'"
            
            print("Watchlist Auto-Removal verified successfully.")
            
        except Exception as e:
            print(f"FAIL: Test 1 failed: {e}")
            import traceback
            traceback.print_exc()

    # --- TEST 2: Review Like Notification ---
    print("\n--- Running Test 2: Review Like Notification ---")
    async with AsyncSessionLocal() as db:
        try:
            # 1. User A writes a review containing a spoiler
            print("User A creating a review...")
            social_repo = SocialRepository(db)
            review = await social_repo.create_review(
                user_id=user_a_id,
                content_id=content_id,
                star_rating=9,
                text_review="A masterpiece with a shocking twist!",
                is_spoiler=True,
                tagged_seasons=[],
                tagged_episodes=[],
                review_type="overall"
            )
            # Fetch review ID
            res = await db.execute(text("SELECT id FROM reviews WHERE user_id = :uid AND content_id = :cid"), {'uid': user_a_id, 'cid': content_id})
            review_id = res.scalar()
            assert review_id is not None, "Review should be saved successfully"
            print(f"Review created: {review_id}")
            
            # 2. User B likes User A's review
            print("User B toggling like on User A's review...")
            social_svc = SocialService(db)
            liked = await social_svc.toggle_review_like(user_id=user_b_id, review_id=review_id)
            assert liked is True, "Review like should toggle ON"
            
            # 3. Verify User A received a notification
            notif_res = await db.execute(text("""
                SELECT * FROM notifications 
                WHERE user_id = :uid AND type = 'review_liked' AND actor_id = :actor_id
            """), {'uid': user_a_id, 'actor_id': user_b_id})
            notif = notif_res.mappings().one_or_none()
            
            assert notif is not None, "Notification should be generated for review author"
            assert notif['related_id'] == review_id, "Notification related_id should match review_id"
            print(f"Notification message verified: '{notif['message']}'")
            print("Review Like Notification verified successfully.")
            
        except Exception as e:
            print(f"FAIL: Test 2 failed: {e}")
            import traceback
            traceback.print_exc()

    # --- TEST 3: Spoiler Feed Filter ---
    print("\n--- Running Test 3: Spoiler Feed Filter ---")
    async with AsyncSessionLocal() as db:
        try:
            # 1. Log activity for the review
            print("Logging review activity...")
            await db.execute(text("""
                INSERT INTO activity_log (id, user_id, activity_type, content_id, review_id, visibility, created_at)
                VALUES (:aid, :uid, 'reviewed', :cid, :rid, 'public', now())
            """), {'aid': activity_id, 'uid': user_a_id, 'cid': content_id, 'rid': review_id})
            await db.commit()
            
            # 2. Call get_activity_feed directly
            print("Calling get_activity_feed endpoint function...")
            feed_res = await get_activity_feed(
                limit=10,
                offset=0,
                db=db,
                current_user_id=str(user_a_id)
            )
            
            # 3. Verify contains_spoiler in metadata
            items = feed_res['data']['items']
            test_items = [i for i in items if i['id'] == activity_id]
            assert len(test_items) == 1, "Logged activity should be in the returned feed items"
            item = test_items[0]
            assert item['metadata']['contains_spoiler'] is True, "contains_spoiler should be True in metadata"
            
            print("Spoiler Feed Filter verified successfully.")
            
        except Exception as e:
            print(f"FAIL: Test 3 failed: {e}")
            import traceback
            traceback.print_exc()

    # --- TEST 4: ON HOLD and DROPPED Status Updates ---
    print("\n--- Running Test 4: ON HOLD & DROPPED Status Updates ---")
    async with AsyncSessionLocal() as db:
        try:
            # 1. Run init_db to ensure constraints are dropped
            from app.core.init_db import init_db
            await init_db(db)

            action_svc = ActionService(db)

            # 2. Test setting status to 'on_hold'
            print("Testing handle_action with status='on_hold'...")
            res_on_hold = await action_svc.handle_action(
                user_a_id,
                content_id,
                ContentActionRequest(action=ActionType.set_status, status='on_hold')
            )
            assert res_on_hold.status == 'success', f"on_hold status action should return status success, got {res_on_hold.status}"

            ucs_res = await db.execute(text("SELECT status FROM user_content_status WHERE user_id = :uid AND content_id = :cid"), {'uid': user_a_id, 'cid': content_id})
            ucs_row = ucs_res.mappings().one()
            assert ucs_row['status'] == 'on_hold', "user_content_status should be 'on_hold'"
            print("Status 'on_hold' verified successfully!")

            # 3. Test setting status to 'dropped'
            print("Testing handle_action with status='dropped'...")
            res_dropped = await action_svc.handle_action(
                user_a_id,
                content_id,
                ContentActionRequest(action=ActionType.set_status, status='dropped')
            )
            assert res_dropped.status == 'success', f"dropped status action should return status success, got {res_dropped.status}"

            ucs_res2 = await db.execute(text("SELECT status FROM user_content_status WHERE user_id = :uid AND content_id = :cid"), {'uid': user_a_id, 'cid': content_id})
            ucs_row2 = ucs_res2.mappings().one()
            assert ucs_row2['status'] == 'dropped', "user_content_status should be 'dropped'"
            print("Status 'dropped' verified successfully!")

            # 4. Test resuming a dropped show by watching episode 1
            print("Testing watch_episode transition from 'dropped' -> 'watching'...")
            res_watch = await action_svc.handle_action(
                user_a_id,
                content_id,
                ContentActionRequest(action=ActionType.watch_episode, season_number=1, episode_number=1)
            )
            assert res_watch.status == 'success', f"watch_episode should return success, got {res_watch.status}"
            ucs_res3 = await db.execute(text("SELECT status FROM user_content_status WHERE user_id = :uid AND content_id = :cid"), {'uid': user_a_id, 'cid': content_id})
            ucs_row3 = ucs_res3.mappings().one()
            assert ucs_row3['status'] == 'watching', f"user_content_status should transition to 'watching', got '{ucs_row3['status']}'"
            print("Status transition 'dropped' -> 'watching' verified successfully!")

        except Exception as e:
            print(f"FAIL: Test 4 failed: {e}")
            import traceback
            traceback.print_exc()

    # --- TEST 5: Discover Anime Mode (_parse_date_obj verification) ---
    print("\n--- Running Test 5: Discover Anime Mode (_parse_date_obj verification) ---")
    async with AsyncSessionLocal() as db:
        try:
            from app.services.content_service import ContentService
            cs = ContentService(db)
            print("Testing get_discover_content('anime')...")
            res_anime = await cs.get_discover_content('anime')
            assert res_anime is not None, "get_discover_content('anime') should return a result dict"
            print("Discover Anime Mode verified successfully!")
        except Exception as e:
            print(f"FAIL: Test 5 failed: {e}")
            import traceback
            traceback.print_exc()

    # --- CLEANUP ---
    print("\n--- Cleaning up test data ---")
    async with AsyncSessionLocal() as db:
        try:
            await db.execute(text("DELETE FROM activity_log WHERE user_id IN (:ua, :ub)"), {'ua': user_a_id, 'ub': user_b_id})
            await db.execute(text("DELETE FROM notifications WHERE user_id IN (:ua, :ub) OR actor_id IN (:ua, :ub)"), {'ua': user_a_id, 'ub': user_b_id})
            await db.execute(text("DELETE FROM review_likes WHERE user_id IN (:ua, :ub)"), {'ua': user_a_id, 'ub': user_b_id})
            await db.execute(text("DELETE FROM reviews WHERE user_id IN (:ua, :ub)"), {'ua': user_a_id, 'ub': user_b_id})
            await db.execute(text("DELETE FROM collection_items WHERE added_by IN (:ua, :ub)"), {'ua': user_a_id, 'ub': user_b_id})
            await db.execute(text("DELETE FROM collections WHERE user_id IN (:ua, :ub)"), {'ua': user_a_id, 'ub': user_b_id})
            await db.execute(text("DELETE FROM user_content_status WHERE user_id IN (:ua, :ub)"), {'ua': user_a_id, 'ub': user_b_id})
            await db.execute(text("DELETE FROM user_stats WHERE user_id IN (:ua, :ub)"), {'ua': user_a_id, 'ub': user_b_id})
            await db.execute(text("DELETE FROM profiles WHERE id IN (:ua, :ub)"), {'ua': user_a_id, 'ub': user_b_id})
            await db.execute(text("DELETE FROM content WHERE id = :cid"), {'cid': content_id})
            await db.commit()
            print("Cleanup complete. Database is clean.")
        except Exception as e:
            print(f"Cleanup failed: {e}")
            await db.rollback()

if __name__ == "__main__":
    asyncio.run(run_verification())
