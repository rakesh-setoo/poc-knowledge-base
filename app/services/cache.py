import json
from typing import Optional, Any
import redis

from app.core.config import settings
from app.logging import logger


# Configuration
METADATA_CACHE_TTL_SECONDS = 300  

_redis_client: Optional[redis.Redis] = None


def _get_redis_client() -> redis.Redis:
    global _redis_client
    
    if _redis_client is not None:
        return _redis_client
    
    if not settings.redis_url:
        raise ConnectionError("REDIS_URL not configured")
    
    _redis_client = redis.from_url(
        settings.redis_url,
        decode_responses=True,
        socket_connect_timeout=5,
        socket_timeout=5
    )
    _redis_client.ping()
    return _redis_client


def _get_table_info_key(table_name: str) -> str:
    return f"excel_ai:table_info:{table_name}"


def get_cached_table_info(table_name: str) -> Optional[dict]:
    try:
        client = _get_redis_client()
        key = _get_table_info_key(table_name)
        cached = client.get(key)
        
        if cached:
            logger.debug(f"Cache HIT for table_info: {table_name}")
            return json.loads(cached)
        
        logger.debug(f"Cache MISS for table_info: {table_name}")
        return None
        
    except Exception as e:
        logger.warning(f"Redis cache read error: {e}")
        return None


def set_cached_table_info(table_name: str, table_info: dict) -> None:
    try:
        client = _get_redis_client()
        key = _get_table_info_key(table_name)
        
        serializable_info = {
            "column_types": table_info["column_types"],
            "sample_data": table_info["sample_data"],
            "distinct_values": table_info["distinct_values"]
        }
        
        client.setex(key, METADATA_CACHE_TTL_SECONDS, json.dumps(serializable_info, default=str))
        logger.debug(f"Cached table_info for: {table_name} (TTL: {METADATA_CACHE_TTL_SECONDS}s)")
        
    except Exception as e:
        logger.warning(f"Redis cache write error: {e}")


def invalidate_table_cache(table_name: str) -> None:
    try:
        client = _get_redis_client()
        key = _get_table_info_key(table_name)
        client.delete(key)
        logger.info(f"Invalidated cache for table: {table_name}")
    except Exception as e:
        logger.warning(f"Redis cache invalidation error: {e}")


def invalidate_all_table_caches() -> None:
    try:
        client = _get_redis_client()
        pattern = "excel_ai:table_info:*"
        keys = client.keys(pattern)
        if keys:
            client.delete(*keys)
            logger.info(f"Invalidated {len(keys)} table cache entries")
    except Exception as e:
        logger.warning(f"Redis cache invalidation error: {e}")


def get_cache_stats() -> dict:
    try:
        client = _get_redis_client()
        pattern = "excel_ai:table_info:*"
        keys = client.keys(pattern)
        
        return {
            "cached_tables": len(keys),
            "table_names": [k.split(":")[-1] for k in keys],
            "ttl_seconds": METADATA_CACHE_TTL_SECONDS
        }
    except Exception as e:
        return {"error": str(e)}
