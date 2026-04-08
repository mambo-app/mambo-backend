try:
    from upstash_redis.asyncio import Redis as AsyncRedis
    redis = AsyncRedis(
        url=__import__('app.core.config', fromlist=['settings']).settings.upstash_redis_rest_url,
        token=__import__('app.core.config', fromlist=['settings']).settings.upstash_redis_rest_token,
    )
    _REDIS_ASYNC = True
except ImportError:
    from upstash_redis import Redis
    redis = Redis(
        url=__import__('app.core.config', fromlist=['settings']).settings.upstash_redis_rest_url,
        token=__import__('app.core.config', fromlist=['settings']).settings.upstash_redis_rest_token,
    )
    _REDIS_ASYNC = False

import json
import logging
from app.core.config import settings

logger = logging.getLogger('mambo.cache')

_sync_redis = None
_async_redis = None

try:
    from upstash_redis.asyncio import Redis as _AsyncRedis
    _async_redis = _AsyncRedis(
        url=settings.upstash_redis_rest_url,
        token=settings.upstash_redis_rest_token,
    )
    _is_async = True
except (ImportError, AttributeError):
    from upstash_redis import Redis as _SyncRedis
    _sync_redis = _SyncRedis(
        url=settings.upstash_redis_rest_url,
        token=settings.upstash_redis_rest_token,
    )
    _is_async = False


class CacheKeys:
    @staticmethod
    def content(content_id: str) -> str:
        return f'content:{content_id}'

    @staticmethod
    def user_profile(user_id: str) -> str:
        return f'user:{user_id}:profile'

    @staticmethod
    def user_stats(user_id: str) -> str:
        return f'user:{user_id}:stats'

    @staticmethod
    def trending(content_type: str, date: str) -> str:
        return f'trending:{content_type}:{date}'

    @staticmethod
    def search(query_hash: str) -> str:
        return f'search:{query_hash}'

    @staticmethod
    def discover(mode: str, identifier: str, date_str: str) -> str:
        return f'discover:{mode}:{identifier}:{date_str}'

class CacheService:
    TTL_CONTENT      = 3600
    TTL_USER_PROFILE = 300
    TTL_USER_STATS   = 300
    TTL_TRENDING     = 86400
    TTL_DISCOVER     = 86400
    TTL_SEARCH       = 600

    @staticmethod
    async def get(key: str):
        try:
            if _is_async:
                val = await _async_redis.get(key)
            else:
                # Fallback: run sync in a thread pool to avoid blocking event loop
                import asyncio
                val = await asyncio.get_event_loop().run_in_executor(None, _sync_redis.get, key)
            if val is None:
                return None
            return json.loads(val)
        except Exception as e:
            logger.error(f'Cache get failed for {key}: {e}')
            return None

    @staticmethod
    async def set(key: str, value, ttl: int):
        try:
            serialized = json.dumps(value, default=str)
            if _is_async:
                await _async_redis.setex(key, ttl, serialized)
            else:
                import asyncio
                await asyncio.get_event_loop().run_in_executor(None, _sync_redis.setex, key, ttl, serialized)
        except Exception as e:
            logger.error(f'Cache set failed for {key}: {e}')

    @staticmethod
    async def delete(key: str):
        try:
            if _is_async:
                await _async_redis.delete(key)
            else:
                import asyncio
                await asyncio.get_event_loop().run_in_executor(None, _sync_redis.delete, key)
        except Exception as e:
            logger.error(f'Cache delete failed for {key}: {e}')

cache = CacheService()