from fastapi import Depends, WebSocket
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from sqlalchemy.ext.asyncio import AsyncSession
from app.core.database import get_db
from app.core.security import verify_supabase_jwt, extract_user_id

bearer_scheme = HTTPBearer()
bearer_scheme_optional = HTTPBearer(auto_error=False)

async def get_current_user_id(
    credentials: HTTPAuthorizationCredentials = Depends(bearer_scheme),
) -> str:
    payload = verify_supabase_jwt(credentials.credentials)
    return extract_user_id(payload)

async def get_current_user_id_optional(
    credentials: HTTPAuthorizationCredentials = Depends(bearer_scheme_optional),
) -> str | None:
    if not credentials:
        return None
    payload = verify_supabase_jwt(credentials.credentials)
    return extract_user_id(payload)

async def get_current_user_token_payload(
    credentials: HTTPAuthorizationCredentials = Depends(bearer_scheme),
) -> dict:
    return verify_supabase_jwt(credentials.credentials)

async def get_current_user_token_payload_optional(
    credentials: HTTPAuthorizationCredentials = Depends(bearer_scheme_optional),
) -> dict | None:
    if not credentials:
        return None
    return verify_supabase_jwt(credentials.credentials)

async def get_ws_user_id(websocket: WebSocket) -> str:
    token = websocket.query_params.get("token")
    if not token:
        auth = websocket.headers.get("Authorization")
        if auth and auth.startswith("Bearer "):
            token = auth.split(" ")[1]
    
    if not token:
        from fastapi import WebSocketException
        raise WebSocketException(code=1008, reason="Missing token")
        
    try:
        payload = verify_supabase_jwt(token)
        return extract_user_id(payload)
    except Exception:
        from fastapi import WebSocketException
        raise WebSocketException(code=1008, reason="Invalid token")