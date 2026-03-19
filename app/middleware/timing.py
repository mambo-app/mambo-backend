import time, logging
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request

logger = logging.getLogger('mambo.timing')

class TimingMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        start = time.perf_counter()
        response = await call_next(request)
        duration_ms = (time.perf_counter() - start) * 1000
        if duration_ms > 500:
            logger.warning(f'SLOW: {request.method} {request.url.path} took {duration_ms:.1f}ms')
        response.headers['X-Response-Time'] = f'{duration_ms:.1f}ms'
        return response