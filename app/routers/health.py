from fastapi import APIRouter

from app.db import check_database_health
from app.core.config import settings
from app.schemas import HealthResponse


router = APIRouter(tags=["Health"])


@router.get("/health", response_model=HealthResponse)
def health_check():
    db_healthy = check_database_health()
    return HealthResponse(
        status="healthy" if db_healthy else "degraded",
        database="connected" if db_healthy else "disconnected",
        version=settings.app_version
    )
