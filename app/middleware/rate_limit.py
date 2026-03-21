from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse
from upstash_redis import Redis
from app.core.config import settings
from app.core.security import verify_supabase_jwt, extract_user_id

redis = Redis(
    url=settings.upstash_redis_rest_url,
    token=settings.upstash_redis_rest_token,
)

GENERAL_LIMIT = 120   # requests per minute per authenticated user
AUTH_LIMIT    = 100   # Elevated for testing/debugging
AUTH_PATHS    = {'/v1/auth/login', '/v1/auth/register'}

class RateLimitMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):

        # ── 1. Auth endpoints: rate-limit by IP (no token required) ──────────
        if request.url.path in AUTH_PATHS:
            ip = request.client.host or 'unknown'
            key = f'rl:auth:{ip}'
            try:
                count = redis.incr(key)
                if count == 1:
                    redis.expire(key, 60)
                if count > AUTH_LIMIT:
                    import logging
                    logger = logging.getLogger('mambo.rate_limit')
                    logger.warning(f"RATE LIMIT HIT: {ip} for {request.url.path}")
                    return JSONResponse(
                        status_code=429,
                        content={
                            'success': False, 
                            'error': {
                                'code': 'RATE_LIMITED',
                                'message': 'Too many attempts. Please wait before trying again.'
                            },
                            # Add detail field for standard FastAPI client compatibility
                            'detail': 'Too many attempts. Please wait before trying again.'
                        }
                    )
            except Exception:
                pass  # If Redis is down, allow the request through
            return await call_next(request)

        # ── 2. All other routes: rate-limit by user ID (requires Bearer) ─────
        auth = request.headers.get('Authorization', '')
        if not auth.startswith('Bearer '):
            return await call_next(request)

        try:
            token = auth.split(' ')[1]
            payload = verify_supabase_jwt(token)
            user_id = extract_user_id(payload)
        except Exception:
            return await call_next(request)

        key = f'rl:global:{user_id}'
        try:
            count = redis.incr(key)
            if count == 1:
                redis.expire(key, 60)
            if count > GENERAL_LIMIT:
                return JSONResponse(
                    status_code=429,
                    content={'success': False, 'error': {
                        'code': 'RATE_LIMITED',
                        'message': 'Too many requests. Wait a moment.'
                    }}
                )
        except Exception:
            pass  # If Redis is down, allow the request through

        return await call_next(request)