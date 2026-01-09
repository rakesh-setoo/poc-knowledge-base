from fastapi import APIRouter

from app.db import check_database_health
from app.core.config import settings
from app.schemas import HealthResponse
from app.services.conversation import check_redis_connection
from app.services.cache import get_cache_stats


router = APIRouter(tags=["Health"])


@router.get("/health", response_model=HealthResponse)
def health_check():
    db_healthy = check_database_health()
    return HealthResponse(
        status="healthy" if db_healthy else "degraded",
        database="connected" if db_healthy else "disconnected",
        version=settings.app_version
    )


@router.get("/health/redis")
def redis_health_check():
    return check_redis_connection()


@router.get("/health/cache")
def cache_stats():
    return get_cache_stats()
