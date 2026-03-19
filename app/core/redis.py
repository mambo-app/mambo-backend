import logging
import redis.asyncio as redis
from app.core.config import settings

logger = logging.getLogger('mambo.redis')

class RedisClient:
    _instance: redis.Redis = None

    @classmethod
    def get_client(cls) -> redis.Redis | None:
        if cls._instance is None:
            if settings.redis_url:
                try:
                    cls._instance = redis.from_url(settings.redis_url, decode_responses=True)
                except Exception as e:
                    logger.error(f"Failed to connect to Redis: {e}")
            else:
                logger.info("No REDIS_URL provided. Operating in in-memory mode for WebSockets.")
        return cls._instance

redis_client = RedisClient()
