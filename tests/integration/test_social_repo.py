import pytest
import uuid
from sqlalchemy import text
from app.repositories.social_repo import SocialRepository

@pytest.mark.asyncio
async def test_create_friend_request_success(db):
    """
    Behavior: successful record creation with RETURNING *
    Bug Protection: Verify that all expected fields are returned after insertion.
    """
    repo = SocialRepository(db)
    u1, u2 = uuid.uuid4(), uuid.uuid4()
    
    # Setup: Create profiles first due to FK constraints
    await db.execute(text("INSERT INTO profiles (id, username) VALUES (:u1, 'user1'), (:u2, 'user2')"), {'u1': u1, 'u2': u2})
    await db.commit()
    
    res = await repo.create_friend_request(u1, u2)
    assert res['sender_id'] == u1
    assert res['receiver_id'] == u2
    assert res['status'] == 'pending'
    assert 'created_at' in res

@pytest.mark.asyncio
async def test_add_friend_idempotency(db):
    """
    Behavior: ON CONFLICT DO NOTHING
    Bug Protection: Ensure that calling add_friend twice for the same pair doesn't raise a UniqueViolation.
    """
    repo = SocialRepository(db)
    u1, u2 = uuid.uuid4(), uuid.uuid4()
    await db.execute(text("INSERT INTO profiles (id, username) VALUES (:u1, 'u1'), (:u2, 'u2')"), {'u1': u1, 'u2': u2})
    await db.commit()
    
    # First call
    await repo.add_friend(u1, u2)
    
    # Second call (duplicate) - should not raise exception
    await repo.add_friend(u1, u2)
    
    # Verify exactly one record exists
    pair = sorted([u1, u2])
    res = await db.execute(text("SELECT COUNT(*) FROM friends WHERE user_id1 = :u1 AND user_id2 = :u2"), 
                          {'u1': pair[0], 'u2': pair[1]})
    assert res.scalar() == 1

@pytest.mark.asyncio
async def test_get_friends_list_aliasing(db):
    """
    Behavior: Correct SQL aliasing (p.id AS user_id)
    Bug Protection: Verify that the frontend receives 'user_id' instead of 'id', 
    which is a common source of bugs when joining tables with conflicting column names.
    """
    repo = SocialRepository(db)
    u1, u2 = uuid.uuid4(), uuid.uuid4()
    await db.execute(text("INSERT INTO profiles (id, username) VALUES (:u1, 'u1'), (:u2, 'u2')"), {'u1': u1, 'u2': u2})
    await db.commit()
    await repo.add_friend(u1, u2)
    await db.commit()
    
    friends = await repo.get_friends_list(u1)
    assert len(friends) == 1
    friend = friends[0]
    
    # The CRITICAL check: aliasing should provide user_id and NOT cause ambiguity
    assert 'user_id' in friend
    assert friend['user_id'] == u2
    assert friend['username'] == 'u2'
    # We care that user_id is correct; if 'id' exists it might be from a join we don't control,
    # but we must ensure it doesn't shadow or break the expected user_id.

@pytest.mark.asyncio
async def test_get_friend_request_missing_returns_none(db):
    """
    Behavior: get returning None when not found
    Bug Protection: Handle non-existent request IDs gracefully without raising errors.
    """
    repo = SocialRepository(db)
    res = await repo.get_friend_request(uuid.uuid4())
    assert res is None

@pytest.mark.asyncio
async def test_mute_user_idempotency(db):
    """
    Behavior: ON CONFLICT DO NOTHING for muting
    Bug Protection: Ensure duplicate mute actions are handled silently and idempotently.
    """
    repo = SocialRepository(db)
    u1, u2 = uuid.uuid4(), uuid.uuid4()
    await db.execute(text("INSERT INTO profiles (id, username) VALUES (:u1, 'u1'), (:u2, 'u2')"), {'u1': u1, 'u2': u2})
    await db.commit()
    
    await repo.mute_user(u1, u2)
    await repo.mute_user(u1, u2) # Duplicate
    
    res = await db.execute(text("SELECT COUNT(*) FROM muted_users WHERE muter_id = :u1 AND muted_id = :u2"), 
                          {'u1': u1, 'u2': u2})
    assert res.scalar() == 1

@pytest.mark.asyncio
async def test_get_post_not_found(db):
    """
    Behavior: Return None for non-existent posts
    Bug Protection: Prevent 500 errors if a client requests a deleted or non-existent post.
    """
    repo = SocialRepository(db)
    res = await repo.get_post(uuid.uuid4())
    assert res is None

@pytest.mark.asyncio
async def test_create_comment_success(db):
    """
    Behavior: Branching logic for post vs review comments
    Bug Protection: Verify that comments are correctly routed to the post_comments table.
    """
    repo = SocialRepository(db)
    u1 = uuid.uuid4()
    p1 = uuid.uuid4()
    
    # Setup
    await db.execute(text("INSERT INTO profiles (id, username) VALUES (:u1, 'u1')"), {'u1': u1})
    await db.execute(text("INSERT INTO posts (id, user_id, title, body) VALUES (:p1, :u1, 'Title', 'Body')"), 
                    {'p1': p1, 'u1': u1})
    await db.commit()
    
    res = await repo.create_comment(u1, "Great post!", post_id=p1)
    assert res['body'] == "Great post!"
    assert res['post_id'] == p1
    assert res['user_id'] == u1

@pytest.mark.asyncio
async def test_increment_friends_count_behavior(db):
    """
    Behavior: Atomic increment of friends_count
    Bug Protection: Ensure that the friends_count is actually updated in user_stats.
    """
    repo = SocialRepository(db)
    u1 = uuid.uuid4()
    await db.execute(text("INSERT INTO profiles (id, username) VALUES (:u1, 'u1')"), {'u1': u1})
    await db.execute(text("INSERT INTO user_stats (user_id, friends_count) VALUES (:u1, 0)"), {'u1': u1})
    await db.commit()
    
    await repo.increment_friends_count(u1)
    
    res = await db.execute(text("SELECT friends_count FROM user_stats WHERE user_id = :u1"), {'u1': u1})
    assert res.scalar() == 1

@pytest.mark.asyncio
async def test_block_user_cleanup(db):
    """
    Behavior: Blocking removes friendship and pending requests
    Bug Protection: Critical security behavior. Ensure that blocking 
    someone actually severs all social ties in the database.
    """
    repo = SocialRepository(db)
    u1, u2 = uuid.uuid4(), uuid.uuid4()
    
    # Setup profiles
    await db.execute(text("INSERT INTO profiles (id, username) VALUES (:u1, 'u1'), (:u2, 'u2')"), {'u1': u1, 'u2': u2})
    await db.commit()
    
    # Setup: Existing friendship and a request
    await repo.add_friend(u1, u2)
    await repo.create_friend_request(u1, u2)
    await db.commit()
    
    # Action: Block
    await repo.block_user(u1, u2)
    
    # Verify friendship deleted
    u_low, u_high = sorted([u1, u2])
    res_f = await db.execute(text("SELECT COUNT(*) FROM friends WHERE user_id1 = :u1 AND user_id2 = :u2"), 
                          {'u1': u_low, 'u2': u_high})
    assert res_f.scalar() == 0
    
    # Verify requests deleted
    res_r = await db.execute(text("SELECT COUNT(*) FROM friend_requests WHERE (sender_id = :u1 AND receiver_id = :u2) OR (sender_id = :u2 AND receiver_id = :u1)"), 
                          {'u1': u1, 'u2': u2})
    assert res_r.scalar() == 0
    
    # Verify block exists
    res_b = await db.execute(text("SELECT COUNT(*) FROM blocked_users WHERE blocker_id = :u1 AND blocked_id = :u2"), 
                          {'u1': u1, 'u2': u2})
    assert res_b.scalar() == 1

@pytest.mark.asyncio
async def test_get_posts_mute_filtering(db):
    """
    Behavior: viewer_id filters out muted users' posts
    Bug Protection: Ensure that the 'where_clause' in get_posts correctly respects mutes.
    """
    repo = SocialRepository(db)
    u1, u2 = uuid.uuid4(), uuid.uuid4()
    await db.execute(text("INSERT INTO profiles (id, username) VALUES (:u1, 'u1'), (:u2, 'u2')"), {'u1': u1, 'u2': u2})
    await db.commit()
    
    # U2 creates a post
    await repo.create_post(u2, {'body': 'Muted message'})
    await db.commit()
    
    # U1 mutes U2
    await repo.mute_user(u1, u2)
    await db.commit()
    
    # Verify U1 doesn't see it, but a guest/other user does
    posts_u1 = await repo.get_posts(viewer_id=u1)
    assert len(posts_u1) == 0
    
    posts_guest = await repo.get_posts(viewer_id=None)
    assert len(posts_guest) == 1

@pytest.mark.asyncio
async def test_create_friend_request_duplicate_raises(db):
    """
    DB constraint: duplicate requests should raise, not silently succeed.
    Bug Protection: Verify that the UNIQUE(sender_id, receiver_id) constraint is enforced at the DB level.
    """
    repo = SocialRepository(db)
    u1, u2 = uuid.uuid4(), uuid.uuid4()
    await db.execute(text("INSERT INTO profiles (id, username) VALUES (:u1, 'u1'), (:u2, 'u2')"), {'u1': u1, 'u2': u2})
    await db.commit()
    
    await repo.create_friend_request(u1, u2)
    await db.commit()
    
    with pytest.raises(Exception):  # IntegrityError
        await repo.create_friend_request(u1, u2)
