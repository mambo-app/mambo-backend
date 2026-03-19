import pytest
import uuid
from httpx import AsyncClient
from app.main import app
from app.core.dependencies import get_current_user_id
from sqlalchemy import text

from unittest.mock import patch, AsyncMock

# Setup a test user ID
TEST_USER_ID = str(uuid.uuid4())
TEST_USERNAME = "testuser"

# Mock dependency for authentication
async def mocked_get_current_user_id():
    return TEST_USER_ID

@pytest.fixture(autouse=True)
def mock_cache():
    with patch("app.services.cache_service.CacheService.get", new_callable=AsyncMock) as mock_get, \
         patch("app.services.cache_service.CacheService.set", new_callable=AsyncMock) as mock_set:
        mock_get.return_value = None
        yield mock_get, mock_set

@pytest.mark.asyncio
async def test_unauthenticated_request_returns_401(client: AsyncClient):
    """
    PROTECTS AGAINST: Security regression where protected routes are accidentally made public.
    """
    # Attempting to access /me without auth headers should fail
    r = await client.get("/v1/users/me")
    assert r.status_code == 401

@pytest.mark.asyncio
async def test_get_me_correct_shape(client: AsyncClient, db):
    """
    PROTECTS AGAINST: 1) Breaking the frontend expectation of the 'ok()' response wrapper.
    2) Ensuring all required profile fields are present.
    """
    # Setup test user in DB
    await db.execute(
        text("INSERT INTO profiles (id, username, display_name) VALUES (:id, :username, :name)"),
        {"id": TEST_USER_ID, "username": TEST_USERNAME, "name": "Test User"}
    )
    await db.commit()

    # Override dependency for this test
    app.dependency_overrides[get_current_user_id] = mocked_get_current_user_id
    
    try:
        r = await client.get("/v1/users/me")
        assert r.status_code == 200
        data = r.json()
        
        # Check 'ok()' wrapper structure
        assert data["success"] is True
        assert "data" in data
        
        # Check actual profile shape
        profile = data["data"]
        assert profile["id"] == TEST_USER_ID
        assert profile["username"] == TEST_USERNAME
        assert "display_name" in profile
    finally:
        app.dependency_overrides = {}

@pytest.mark.asyncio
async def test_route_shadowing_prevention(client: AsyncClient, db):
    """
    PROTECTS AGAINST: Route shadowing where /{username} (wildcard) might swallow 
    specific routes like /{username}/follow or /{username}/activity if defined 
    in the wrong order in the APIRouter.
    """
    target_id = str(uuid.uuid4())
    target_username = "target_dev"
    
    # Setup users
    await db.execute(
        text("INSERT INTO profiles (id, username) VALUES (:u1, :un1), (:u2, :un2)"),
        {"u1": TEST_USER_ID, "un1": TEST_USERNAME, "u2": target_id, "un2": target_username}
    )
    await db.commit()

    app.dependency_overrides[get_current_user_id] = mocked_get_current_user_id

    try:
        # 1. Test /activity (specific route)
        r_act = await client.get(f"/v1/users/{target_username}/activity")
        # If shadowed, might return profile object from /{username} instead of success wrapper with list/empty
        assert r_act.status_code == 200
        assert r_act.json()["success"] is True
        
        # 2. Test /follow (POST)
        r_follow = await client.post(f"/v1/users/{target_username}/follow")
        assert r_follow.status_code == 200
        assert "Successfully followed" in r_follow.json()["data"]["message"]

    finally:
        app.dependency_overrides = {}

@pytest.mark.asyncio
async def test_follow_unfollow_messages(client: AsyncClient, db):
    """
    PROTECTS AGAINST: Regressions in success messages that the UI might rely on 
    for user feedback or toast notifications.
    """
    target_id = str(uuid.uuid4())
    target_username = "friendly_jake"
    
    await db.execute(
        text("INSERT INTO profiles (id, username) VALUES (:u1, :un1), (:u2, :un2)"),
        {"u1": TEST_USER_ID, "un1": TEST_USERNAME, "u2": target_id, "un2": target_username}
    )
    await db.commit()

    app.dependency_overrides[get_current_user_id] = mocked_get_current_user_id

    try:
        # Follow message
        r_f = await client.post(f"/v1/users/{target_username}/follow")
        assert r_f.json()["data"]["message"] == f"Successfully followed {target_username}"
        
        # Unfollow message
        r_u = await client.delete(f"/v1/users/{target_username}/follow")
        assert r_u.json()["data"]["message"] == f"Successfully unfollowed {target_username}"
    finally:
        app.dependency_overrides = {}
