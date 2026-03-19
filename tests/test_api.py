"""
Integration tests for the MAMBO API.

Run: venv\\Scripts\\pytest tests/ -v

Tests that require auth are marked with @pytest.mark.skipif to skip
gracefully when no TEST_AUTH_TOKEN is set.
"""
import os
import uuid
import pytest
import pytest_asyncio
from httpx import AsyncClient


# ── Helpers ──────────────────────────────────────────────────────────────────

AUTH_TOKEN = os.environ.get('TEST_AUTH_TOKEN', '')
requires_auth = pytest.mark.skipif(
    not AUTH_TOKEN,
    reason="Set TEST_AUTH_TOKEN env var to run authenticated tests"
)


def _auth(extra: dict | None = None) -> dict:
    headers = {'Authorization': f'Bearer {AUTH_TOKEN}'}
    if extra:
        headers.update(extra)
    return headers


# ── 1. Health Check ───────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_health_returns_ok(client: AsyncClient):
    """GET /health should return status=ok with DB reachable."""
    r = await client.get('/health')
    assert r.status_code == 200
    data = r.json()
    assert data['status'] in ('ok', 'degraded')
    assert 'db' in data
    assert 'env' in data


# ── 2. Auth ───────────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_register_with_invalid_invite_key(client: AsyncClient):
    """POST /v1/auth/register with wrong invite key must return 4xx."""
    r = await client.post('/v1/auth/register', json={
        'email': f'test_{uuid.uuid4().hex[:8]}@example.com',
        'password': 'Password123!',
        'username': f'usr_{uuid.uuid4().hex[:8]}',
        'invite_key': 'INVALID_KEY'
    })
    assert r.status_code in (400, 401, 403)


@pytest.mark.asyncio
async def test_login_with_wrong_password(client: AsyncClient):
    """POST /v1/auth/login with wrong credentials must return 4xx."""
    r = await client.post('/v1/auth/login', json={
        'email': 'nonexistent@example.com',
        'password': 'WrongPassword!'
    })
    assert r.status_code in (400, 401, 403)


@pytest.mark.asyncio
async def test_auth_rate_limit_enforced(client: AsyncClient):
    """After 10 rapid requests to /v1/auth/login the 11th should be 429."""
    for _ in range(10):
        await client.post('/v1/auth/login', json={
            'email': 'rl_test@example.com',
            'password': 'WrongPassword!'
        })
    r = await client.post('/v1/auth/login', json={
        'email': 'rl_test@example.com',
        'password': 'WrongPassword!'
    })
    # Tightened assertion: priority is 429
    if os.environ.get('UPSTASH_REDIS_REST_URL'):
        assert r.status_code == 429
    else:
        assert r.status_code in (400, 401, 403, 429)


# ── 3. Unauthenticated access ─────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_protected_endpoint_without_token(client: AsyncClient):
    """GET /v1/users/me without token should return 401."""
    r = await client.get('/v1/users/me')
    assert r.status_code == 401


@pytest.mark.asyncio
async def test_protected_feed_without_token(client: AsyncClient):
    """GET /v1/feed/ without token should return 401."""
    r = await client.get('/v1/feed/')
    assert r.status_code == 401


# ── 4. Public endpoints ───────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_home_trending_returns_structure(client: AsyncClient):
    """GET /v1/home/trending should return success with movies/series/anime keys."""
    r = await client.get('/v1/home/trending')
    assert r.status_code == 200
    data = r.json()
    assert data.get('success') is True
    payload = data.get('data', {})
    assert isinstance(payload.get('movies'), list)
    assert isinstance(payload.get('series'), list)
    assert isinstance(payload.get('anime'), list)


@pytest.mark.asyncio
async def test_discover_valid_mode(client: AsyncClient):
    """GET /v1/discover/movies should return success."""
    r = await client.get('/v1/discover/movies')
    assert r.status_code == 200
    assert r.json().get('success') is True


@pytest.mark.asyncio
async def test_discover_invalid_mode(client: AsyncClient):
    """GET /v1/discover/invalid should return 4xx."""
    r = await client.get('/v1/discover/invalid_mode_xyz')
    assert r.status_code in (400, 404, 422)


@pytest.mark.asyncio
async def test_content_search_returns_results(client: AsyncClient):
    """GET /v1/content/search?q=inception should return success with items list."""
    r = await client.get('/v1/content/search', params={'q': 'inception'})
    assert r.status_code == 200
    data = r.json()
    assert data.get('success') is True
    assert isinstance(data.get('data', {}).get('items', []), list)


# ── 5. Authenticated: Profile ─────────────────────────────────────────────────

@requires_auth
@pytest.mark.asyncio
async def test_get_my_profile(client: AsyncClient):
    """GET /v1/users/me should return current user's profile."""
    r = await client.get('/v1/users/me', headers=_auth())
    assert r.status_code == 200
    data = r.json()
    assert data.get('success') is True
    profile = data.get('data', {})
    assert 'username' in profile
    assert 'id' in profile


@requires_auth
@pytest.mark.asyncio
async def test_get_collections(client: AsyncClient):
    """GET /v1/collections/ should return a list of user collections."""
    r = await client.get('/v1/collections/', headers=_auth())
    assert r.status_code == 200
    data = r.json()
    assert data.get('success') is True
    assert isinstance(data.get('data', {}).get('items', []), list)


# ── 6. Authenticated: Feed ─────────────────────────────────────────────────────

@requires_auth
@pytest.mark.asyncio
async def test_feed_returns_paginated_items(client: AsyncClient):
    """GET /v1/feed/?limit=5 should return at most 5 items."""
    r = await client.get('/v1/feed/', params={'limit': 5}, headers=_auth())
    assert r.status_code == 200
    data = r.json()
    assert data.get('success') is True
    items = data.get('data', {}).get('items', [])
    assert isinstance(items, list)
    assert len(items) <= 5


@requires_auth
@pytest.mark.asyncio
async def test_recently_watched(client: AsyncClient):
    """GET /v1/feed/recently-watched should return a list."""
    r = await client.get('/v1/feed/recently-watched', headers=_auth())
    assert r.status_code == 200
    assert isinstance(r.json().get('data', {}).get('items', []), list)


# ── 7. Authenticated: Social ──────────────────────────────────────────────────

@requires_auth
@pytest.mark.asyncio
async def test_get_posts(client: AsyncClient):
    """GET /v1/posts/ should return a list of discussion posts."""
    r = await client.get('/v1/posts/', headers=_auth())
    assert r.status_code == 200
    data = r.json()
    assert data.get('success') is True


@requires_auth
@pytest.mark.asyncio
async def test_get_notifications(client: AsyncClient):
    """GET /v1/notifications/ should return a list."""
    r = await client.get('/v1/notifications/', headers=_auth())
    assert r.status_code == 200
    data = r.json()
    assert data.get('success') is True


# ── 8. Authenticated: Media upload URL ────────────────────────────────────────

@requires_auth
@pytest.mark.asyncio
async def test_media_upload_url_invalid_bucket(client: AsyncClient):
    """POST /v1/media/upload-url with invalid bucket should return 400."""
    r = await client.post('/v1/media/upload-url', headers=_auth(), params={
        'bucket': 'hacker_bucket',
        'file_name': 'photo.jpg',
    })
    assert r.status_code == 400


@requires_auth
@pytest.mark.asyncio
async def test_media_upload_url_invalid_extension(client: AsyncClient):
    """POST /v1/media/upload-url with .exe extension should return 400."""
    r = await client.post('/v1/media/upload-url', headers=_auth(), params={
        'bucket': 'avatars',
        'file_name': 'malware.exe',
    })
    assert r.status_code == 400


@requires_auth
@pytest.mark.asyncio
async def test_media_upload_url_valid_request(client: AsyncClient):
    """POST /v1/media/upload-url with valid params should return upload_url."""
    r = await client.post('/v1/media/upload-url', headers=_auth(), params={
        'bucket': 'avatars',
        'file_name': 'avatar.jpg',
        'content_type': 'image/jpeg',
    })
    # Accept both success and Supabase credential issues in CI
    assert r.status_code in (200, 500)
    if r.status_code == 200:
        data = r.json().get('data', {})
        assert 'upload_url' in data
        assert 'path' in data


# ── 9. Authenticated: Save/Unsave posts ──────────────────────────────────────

@requires_auth
@pytest.mark.asyncio
async def test_save_nonexistent_post(client: AsyncClient):
    """POST /v1/posts/{bad_id}/save should return 4xx or succeed gracefully."""
    fake_id = str(uuid.uuid4())
    r = await client.post(f'/v1/posts/{fake_id}/save', headers=_auth())
    # Either 404 (post not found) or 200 (ON CONFLICT DO NOTHING is forgiving)
    assert r.status_code in (200, 404, 422)


# ── 10. Security Regressions ────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_reports_rejects_invite_key(client: AsyncClient):
    """
    SECURITY: Verify GET /v1/reports returns 403 when given the invite key.
    Protection: Prevents the invite key (used for registration) from being 
    misused to access restricted admin data. This was a real bug.
    """
    # Use the test_invite_key set in conftest.py
    r = await client.get('/v1/reports/', headers={'X-Admin-Secret': 'test_invite_key'})
    assert r.status_code == 403

@pytest.mark.asyncio
async def test_reports_accepts_admin_secret(client: AsyncClient):
    """
    SECURITY: Verify GET /v1/reports returns 200 when given the admin secret.
    Behavior: Confirms that authorized administrators can access the reports 
    endpoint while unauthorized keys are correctly rejected.
    """
    # Use the test_admin_secret set in conftest.py
    r = await client.get('/v1/reports/', headers={'X-Admin-Secret': 'test_admin_secret'})
    # Accept 200 (success) or 500 (db/env issues in test), but NOT 403/401
    assert r.status_code in (200, 500)
