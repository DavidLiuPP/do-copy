import redis.asyncio as redis
from settings import settings

redis_client = redis.Redis(
    host=settings.REDIS_URL,
    port="6379",
    decode_responses=True,
    socket_connect_timeout=30,
    socket_timeout=30,
) if settings.REDIS_URL else None

