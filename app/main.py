import time
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, JSONResponse

from app.core.config import settings
from app.db import init_metadata_table, load_all_datasets
from app.logging import logger, AppException
from app.routers import health, datasets, query, chat, settings as settings_router

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info(f"Starting {settings.app_name} v{settings.app_version}")
    init_metadata_table()
    
    datasets.refresh_datasets()
    logger.info(f"Loaded {len(datasets.get_datasets())} datasets")
    
    yield
    
    logger.info("Shutting down...")


# =============================================================================
# Create FastAPI App
# =============================================================================

app = FastAPI(
    title=settings.app_name,
    version=settings.app_version,
    description="Query your Excel data using natural language powered by AI",
    lifespan=lifespan
)


# =============================================================================
# Middleware
# =============================================================================

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins_list,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
async def log_requests(request: Request, call_next):
    """Log slow requests and errors."""
    start = time.time()
    response = await call_next(request)
    elapsed = time.time() - start
    
    if elapsed > 1.0:
        logger.warning(f"Slow request: {request.method} {request.url.path} ({elapsed:.2f}s)")
    
    if response.status_code >= 400:
        logger.error(f"Error response: {request.method} {request.url.path} -> {response.status_code}")
    
    return response


# =============================================================================
# Exception Handlers
# =============================================================================

@app.exception_handler(AppException)
async def app_exception_handler(request: Request, exc: AppException):
    """Handle application-specific exceptions."""
    return JSONResponse(
        status_code=exc.status_code,
        content={"error": exc.message}
    )


@app.exception_handler(Exception)
async def general_exception_handler(request: Request, exc: Exception):
    """Handle unexpected exceptions."""
    logger.error(f"Unexpected error: {exc}")
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={"error": "An unexpected error occurred"}
    )


# =============================================================================
# Static Files & Frontend
# =============================================================================

app.mount("/static", StaticFiles(directory="static"), name="static")


@app.api_route("/", methods=["GET", "HEAD"], include_in_schema=False)
def serve_frontend():
    """Serve the main frontend HTML."""
    return FileResponse("static/index.html")


# =============================================================================
# Include Routers
# =============================================================================

# Health check
app.include_router(health.router)

# Dataset management
app.include_router(datasets.router)
app.include_router(datasets.upload_router)

# Query endpoint
app.include_router(query.router)

# Chat management
app.include_router(chat.router)

# Settings
app.include_router(settings_router.router)
