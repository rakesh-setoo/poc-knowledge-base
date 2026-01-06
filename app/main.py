"""
Millenium Semiconductors AI - Main Application
FastAPI application for querying Excel data using natural language.
"""
from fastapi import FastAPI, UploadFile, Body, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, JSONResponse
from contextlib import asynccontextmanager
import pandas as pd
import uuid
import json
import time

from app.config import settings
from app.db import engine, init_metadata_table, save_dataset_metadata, load_all_datasets, delete_dataset_metadata, check_database_health
from app.llm import llm_call
from app.schema_selector import select_schema
from app.sql_utils import validate_sql, run_sql, extract_sql
from app.logging_config import logger
from app.schemas import (
    DatasetListResponse, DatasetDeleteResponse, SyncResponse,
    AskResponse, ErrorResponse, HealthResponse
)
from app.exceptions import (
    AppException, SQLValidationError, SQLExecutionError, LLMError,
    NoDatasetError, DatasetNotFoundError
)


# In-memory dataset cache
DATASETS: list[dict] = []


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan handler for startup and shutdown."""
    global DATASETS
    
    # Startup
    logger.info(f"Starting {settings.app_name} v{settings.app_version}")
    logger.info(f"Environment: {settings.environment}")
    
    init_metadata_table()
    DATASETS = load_all_datasets()
    logger.info(f"Loaded {len(DATASETS)} datasets")
    
    yield
    
    # Shutdown
    logger.info("Shutting down application")


# Create FastAPI app with metadata
app = FastAPI(
    title=settings.app_name,
    version=settings.app_version,
    description="Query your Excel data using natural language powered by AI",
    lifespan=lifespan
)


# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins_list,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Exception handlers
@app.exception_handler(AppException)
async def app_exception_handler(request: Request, exc: AppException):
    """Handle application-specific exceptions."""
    logger.error(f"Application error: {exc.message}", extra={"details": exc.details})
    return JSONResponse(
        status_code=status.HTTP_400_BAD_REQUEST,
        content={"error": exc.message, **exc.details}
    )


@app.exception_handler(Exception)
async def general_exception_handler(request: Request, exc: Exception):
    """Handle unexpected exceptions."""
    logger.error(f"Unexpected error: {str(exc)}")
    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={"error": "An unexpected error occurred. Please try again."}
    )


# Request logging middleware
@app.middleware("http")
async def log_requests(request: Request, call_next):
    """Log slow requests and errors."""
    start_time = time.time()
    response = await call_next(request)
    duration = time.time() - start_time
    
    # Only log slow requests (>2s) or errors
    if duration > 2 or response.status_code >= 400:
        logger.info(
            f"{request.method} {request.url.path} - {response.status_code} - {duration:.2f}s"
        )
    return response


# Static files and frontend
app.mount("/static", StaticFiles(directory="static"), name="static")


@app.get("/", include_in_schema=False)
async def serve_frontend():
    """Serve the main frontend HTML."""
    return FileResponse("static/index.html")


# Health check endpoint
@app.get("/health", response_model=HealthResponse, tags=["System"])
async def health_check():
    """Check application and database health."""
    db_healthy = check_database_health()
    return HealthResponse(
        status="healthy" if db_healthy else "degraded",
        database="connected" if db_healthy else "disconnected",
        version=settings.app_version
    )


# Dataset endpoints
@app.get("/datasets", response_model=DatasetListResponse, tags=["Datasets"])
async def list_datasets():
    """List all uploaded datasets."""
    return DatasetListResponse(datasets=DATASETS, count=len(DATASETS))


@app.post("/sync-datasets", response_model=SyncResponse, tags=["Datasets"])
async def sync_datasets():
    """Discover and register existing excel_* tables that aren't in metadata."""
    global DATASETS
    from sqlalchemy import text, inspect
    
    inspector = inspect(engine)
    existing_tables = inspector.get_table_names()
    
    registered_tables = {d["table_name"] for d in DATASETS}
    new_tables = [t for t in existing_tables if t.startswith("excel_") and t not in registered_tables]
    
    synced = []
    for table_name in new_tables:
        columns = [col["name"] for col in inspector.get_columns(table_name)]
        
        with engine.connect() as conn:
            result = conn.execute(text(f'SELECT COUNT(*) FROM "{table_name}"'))
            row_count = result.scalar()
        
        metadata = {
            "table_name": table_name,
            "file_name": f"{table_name} (synced)",
            "columns": columns,
            "row_count": row_count
        }
        save_dataset_metadata(metadata)
        synced.append(metadata)
    
    DATASETS = load_all_datasets()
    logger.info(f"Synced {len(synced)} new tables")
    
    return SyncResponse(synced=synced, total_datasets=len(DATASETS))


@app.delete("/datasets/{table_name}", response_model=DatasetDeleteResponse, tags=["Datasets"])
async def delete_dataset(table_name: str):
    """Delete a dataset by table name."""
    global DATASETS
    
    dataset = next((d for d in DATASETS if d["table_name"] == table_name), None)
    if not dataset:
        raise DatasetNotFoundError(f"Dataset '{table_name}' not found")
    
    from sqlalchemy import text
    with engine.connect() as conn:
        conn.execute(text(f'DROP TABLE IF EXISTS "{table_name}"'))
        conn.commit()
    
    delete_dataset_metadata(table_name)
    DATASETS = [d for d in DATASETS if d["table_name"] != table_name]
    
    logger.info(f"Deleted dataset: {table_name}")
    return DatasetDeleteResponse(message=f"Dataset '{table_name}' deleted", file_name=dataset["file_name"])


@app.post("/upload-excel", tags=["Datasets"])
async def upload_excel(file: UploadFile):
    """Upload an Excel file and create a queryable dataset."""
    global DATASETS
    
    logger.info(f"Uploading file: {file.filename}")
    
    try:
        df = pd.read_excel(file.file)
    except Exception as e:
        logger.error(f"Failed to read Excel file: {e}")
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content={"error": f"Failed to read Excel file: {str(e)}"}
        )

    # Clean column names
    df.columns = (
        df.columns.astype(str)
        .str.lower()
        .str.replace(" ", "_")
        .str.replace(r"[^a-z0-9_]", "", regex=True)
    )

    table_name = f"excel_{uuid.uuid4().hex[:8]}"
    df.to_sql(table_name, engine, index=False, if_exists="replace")

    metadata = {
        "table_name": table_name,
        "file_name": file.filename,
        "columns": list(df.columns),
        "row_count": len(df)
    }

    save_dataset_metadata(metadata)
    DATASETS = load_all_datasets()
    
    logger.info(f"Created table {table_name} with {len(df)} rows")
    return metadata


def get_table_info(table_name: str) -> dict:
    """Get column types, sample data, and distinct values for text columns."""
    from sqlalchemy import text
    
    with engine.connect() as conn:
        # Get column types
        type_query = text("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = :table_name
            ORDER BY ordinal_position
        """)
        type_result = conn.execute(type_query, {"table_name": table_name})
        column_types = {row[0]: row[1] for row in type_result}
        
        # Get sample data (first 5 rows)
        sample_query = text(f'SELECT * FROM "{table_name}" LIMIT 5')
        sample_result = conn.execute(sample_query)
        sample_rows = [dict(row._mapping) for row in sample_result]
        
        # Get distinct values for text columns
        distinct_values = {}
        for col_name, col_type in column_types.items():
            if col_type in ('text', 'character varying', 'varchar'):
                if any(keyword in col_name.lower() for keyword in ['month', 'date', 'year', 'category', 'type', 'status', 'region', 'city']):
                    distinct_query = text(f'SELECT DISTINCT "{col_name}" FROM "{table_name}" ORDER BY "{col_name}" LIMIT 20')
                    distinct_result = conn.execute(distinct_query)
                    distinct_values[col_name] = [row[0] for row in distinct_result if row[0] is not None]
        
    return {
        "column_types": column_types, 
        "sample_data": sample_rows,
        "distinct_values": distinct_values
    }


@app.post("/ask", tags=["Query"])
async def ask_question(question: str = Body(...)):
    """Ask a natural language question about your data."""
    start_time = time.time()
    
    if not DATASETS:
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content={"error": "No datasets uploaded. Please upload an Excel file first."}
        )

    try:
        # 1. Select table - skip LLM call if only 1 table exists
        if len(DATASETS) == 1:
            table = DATASETS[0]["table_name"]
        else:
            schema = select_schema(question, DATASETS)
            table = schema["table_name"]
        
        # 2. Get table info (fast DB query)
        table_info = get_table_info(table)

        # 3. Generate SQL with clear context
        distinct_section = ""
        if table_info.get('distinct_values'):
            distinct_section = f"\nDistinct values: {json.dumps(table_info['distinct_values'])}"
        
        sql_prompt = f"""Generate a PostgreSQL SELECT query for this question.

Table: {table}
Columns: {json.dumps(list(table_info['column_types'].keys()))}
Sample data: {json.dumps(table_info['sample_data'][:5], default=str)}{distinct_section}

Question: {question}

Return ONLY the SQL query, no explanation."""
        
        sql = extract_sql(llm_call(sql_prompt, max_tokens=500))
        
        # 4. Validate & run SQL
        validate_sql(sql)
        rows, cols = run_sql(sql)

        # 5. Prepare results
        columns_list = list(cols)
        result_data = [dict(zip(columns_list, row)) for row in rows]
        
        # 6. Generate answer from query results
        sample_for_llm = result_data[:10] if len(result_data) > 10 else result_data
        
        answer_prompt = f"""Based on the database query results below, answer the user's question.

Question: {question}

Query Results ({len(result_data)} rows):
{json.dumps(sample_for_llm, default=str)}

Provide a direct answer using the actual values from the results. Format large numbers in Cr/Lakhs."""
        
        answer = llm_call(answer_prompt, temperature=0.2, max_tokens=500)
        
        duration = time.time() - start_time

        return {
            "table_used": table,
            "generated_sql": sql,
            "answer": answer,
            "columns": columns_list,
            "data": result_data[:50],
            "row_count": len(result_data),
            "response_time_seconds": round(duration, 2)
        }
        
    except (SQLValidationError, SQLExecutionError) as e:
        logger.error(f"SQL error: {e.message}")
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content={"error": e.message, "generated_sql": getattr(e, 'details', {}).get('sql', '')}
        )
    except LLMError as e:
        logger.error(f"LLM error: {e.message}")
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"error": f"AI processing failed: {e.message}"}
        )
    except Exception as e:
        logger.error(f"Unexpected error in ask: {e}")
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"error": "An unexpected error occurred. Please try again."}
        )
