import asyncio
import os
import sys

sys.path.append(os.getcwd())
import uuid
from httpx import AsyncClient
from app.main import app
from app.core.dependencies import get_current_user_id

# Mock the dependency
async def mock_get_user():
    return "b59ae105-7b44-4eef-9e16-09ccdf3fbc90" # Use a known user ID

app.dependency_overrides[get_current_user_id] = mock_get_user

async def test_endpoint():
    conn_id = "d3e9f9bb-4d3b-4e8e-a3a6-d758a4e53ffc"
    from httpx import ASGITransport
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        try:
            response = await ac.post(f"/v1/content/{conn_id}/action", json={"action": "rewatch"})
            print("Status:", response.status_code)
            print("Response:", response.text)
        except Exception as e:
            import traceback
            traceback.print_exc()

if __name__ == '__main__':
    asyncio.run(test_endpoint())
