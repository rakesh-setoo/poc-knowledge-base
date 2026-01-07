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
    """Upload an Excel or CSV file and create a queryable dataset with progress streaming."""
    from fastapi.responses import StreamingResponse
    import asyncio
    
    global DATASETS
    
    async def process_with_progress():
        """Generator that yields dynamic progress updates as SSE events."""
        import io
        try:
            filename = file.filename.lower()
            
            # Phase 1: Read file in chunks (0-30%)
            yield f"data: {json.dumps({'progress': 1, 'status': '1% - Starting upload...'})}\n\n"
            await asyncio.sleep(0.05)
            
            chunks = []
            total_size = 0
            chunk_size = 64 * 1024
            
            while True:
                chunk = await file.read(chunk_size)
                if not chunk:
                    break
                chunks.append(chunk)
                total_size += len(chunk)
                read_progress = min(28, int((total_size / (1024 * 1024)) * 5) + 2)
                yield f"data: {json.dumps({'progress': read_progress, 'status': f'{read_progress}% - Reading file... ({total_size // 1024} KB)'})}\n\n"
                await asyncio.sleep(0.02)
            
            content = b''.join(chunks)
            
            # Phase 2: Parse file (30-55%)
            yield f"data: {json.dumps({'progress': 30, 'status': '30% - File read complete, parsing...'})}\n\n"
            await asyncio.sleep(0.05)
            
            try:
                if filename.endswith('.csv'):
                    yield f"data: {json.dumps({'progress': 35, 'status': '35% - Detecting encoding...'})}\n\n"
                    await asyncio.sleep(0.05)
                    
                    df = None
                    for idx, encoding in enumerate(['utf-8', 'latin1', 'cp1252', 'iso-8859-1']):
                        try:
                            df = pd.read_csv(io.BytesIO(content), encoding=encoding)
                            progress = 40 + (idx * 3)
                            yield f"data: {json.dumps({'progress': progress, 'status': f'{progress}% - Parsing CSV...'})}\n\n"
                            break
                        except UnicodeDecodeError:
                            continue
                    if df is None:
                        yield f"data: {json.dumps({'progress': 0, 'status': 'Error', 'error': 'Could not read CSV file. Please ensure it is a valid CSV with UTF-8 or Latin encoding.'})}\n\n"
                        return
                        
                elif filename.endswith(('.xlsx', '.xls')):
                    yield f"data: {json.dumps({'progress': 35, 'status': '35% - Reading Excel structure...'})}\n\n"
                    await asyncio.sleep(0.05)
                    
                    df_raw = pd.read_excel(io.BytesIO(content), header=None)
                    
                    yield f"data: {json.dumps({'progress': 42, 'status': '42% - Detecting header row...'})}\n\n"
                    await asyncio.sleep(0.05)
                    
                    header_row = 0
                    max_valid_cols = 0
                    for i in range(min(10, len(df_raw))):
                        row = df_raw.iloc[i]
                        valid_cols = sum(1 for v in row if pd.notna(v) and isinstance(v, str) and len(str(v)) > 1)
                        if valid_cols > max_valid_cols:
                            max_valid_cols = valid_cols
                            header_row = i
                    
                    yield f"data: {json.dumps({'progress': 48, 'status': '48% - Parsing Excel data...'})}\n\n"
                    await asyncio.sleep(0.05)
                    
                    df = pd.read_excel(io.BytesIO(content), header=header_row)
                    df = df.loc[:, ~df.columns.astype(str).str.contains('Unnamed')]
                    df = df.dropna(axis=1, how='all')
                else:
                    yield f"data: {json.dumps({'progress': 0, 'status': 'Error', 'error': 'Unsupported file type. Please upload .xlsx, .xls, or .csv files.'})}\n\n"
                    return
                    
            except Exception as e:
                yield f"data: {json.dumps({'progress': 0, 'status': 'Error', 'error': f'Failed to parse file: {str(e)}'})}\n\n"
                return
            
            # Phase 3: Clean data (55-60%)
            yield f"data: {json.dumps({'progress': 55, 'status': '55% - Cleaning column names...'})}\n\n"
            await asyncio.sleep(0.05)
            
            df.columns = (
                df.columns.astype(str)
                .str.lower()
                .str.replace(" ", "_")
                .str.replace(r"[^a-z0-9_]", "", regex=True)
            )
            
            total_rows = len(df)
            yield f"data: {json.dumps({'progress': 60, 'status': f'60% - Data cleaned ({total_rows:,} rows)'})}\n\n"
            await asyncio.sleep(0.05)
            
            # Phase 4: Save to database (60-95%)
            file_type = "csv" if filename.endswith('.csv') else "excel"
            table_name = f"dataset_{uuid.uuid4().hex[:8]}"
            
            if total_rows <= 1000:
                yield f"data: {json.dumps({'progress': 75, 'status': '75% - Saving to database...'})}\n\n"
                await asyncio.sleep(0.05)
                df.to_sql(table_name, engine, index=False, if_exists="replace")
                yield f"data: {json.dumps({'progress': 92, 'status': '92% - Database save complete'})}\n\n"
            else:
                chunk_size = max(100, total_rows // 20)
                rows_inserted = 0
                
                for i in range(0, total_rows, chunk_size):
                    chunk_df = df.iloc[i:i + chunk_size]
                    if_exists = "replace" if i == 0 else "append"
                    chunk_df.to_sql(table_name, engine, index=False, if_exists=if_exists)
                    
                    rows_inserted += len(chunk_df)
                    insert_progress = 60 + int((rows_inserted / total_rows) * 32)
                    yield f"data: {json.dumps({'progress': insert_progress, 'status': f'{insert_progress}% - Saving rows {rows_inserted:,}/{total_rows:,}...'})}\n\n"
                    await asyncio.sleep(0.02)
            
            # Phase 5: Finalize (95-100%)
            yield f"data: {json.dumps({'progress': 95, 'status': '95% - Saving metadata...'})}\n\n"
            await asyncio.sleep(0.05)
            
            metadata = {
                "table_name": table_name,
                "file_name": file.filename,
                "file_type": file_type,
                "columns": list(df.columns),
                "row_count": total_rows
            }
            
            save_dataset_metadata(metadata)
            
            yield f"data: {json.dumps({'progress': 98, 'status': '98% - Refreshing datasets...'})}\n\n"
            await asyncio.sleep(0.05)
            
            global DATASETS
            DATASETS = load_all_datasets()
            
            yield f"data: {json.dumps({'progress': 100, 'status': '100% - Upload complete!', 'result': metadata})}\n\n"
            
        except Exception as e:
            logger.error(f"Upload error: {str(e)}")
            yield f"data: {json.dumps({'progress': 0, 'status': 'Error', 'error': str(e)})}\n\n"
    
    return StreamingResponse(
        process_with_progress(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no"
        }
    )


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
async def ask_question(
    question: str = Body(...),
    dataset_id: int = Body(None, description="Optional dataset ID to query. If not provided, uses the first available dataset.")
):
    """Ask a natural language question about your data."""
    start_time = time.time()
    step_times = {}
    
    if not DATASETS:
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content={"error": "No datasets uploaded. Please upload a file first."}
        )

    try:
        # 1. Select table based on dataset_id or use first/only dataset
        step_start = time.time()
        if dataset_id is not None:
            dataset = next((d for d in DATASETS if d.get("id") == dataset_id), None)
            if not dataset:
                return JSONResponse(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    content={"error": f"Dataset with ID {dataset_id} not found."}
                )
            table = dataset["table_name"]
            logger.info(f"Using dataset ID {dataset_id}, table: {table}")
        elif len(DATASETS) == 1:
            table = DATASETS[0]["table_name"]
            logger.info(f"Single dataset, table: {table}")
        else:
            schema = select_schema(question, DATASETS)
            table = schema["table_name"]
            logger.info(f"Schema selected, table: {table}")
        step_times["table_selection"] = round(time.time() - step_start, 3)
        logger.info(f"⏱️ Step 1 - Table Selection: {step_times['table_selection']}s")
        
        # 2. Get table info (fast DB query)
        step_start = time.time()
        table_info = get_table_info(table)
        step_times["table_info_retrieval"] = round(time.time() - step_start, 3)
        logger.info(f"⏱️ Step 2 - Table Info Retrieval: {step_times['table_info_retrieval']}s")

        # 3. Generate SQL with comprehensive context
        step_start = time.time()
        distinct_section = ""
        if table_info.get('distinct_values'):
            distinct_section = f"\nKey column values: {json.dumps(table_info['distinct_values'])}"
        
        sql_prompt = f"""You are a PostgreSQL expert. Generate an accurate SQL query for this question.

TABLE: {table}
COLUMNS: {json.dumps(list(table_info['column_types'].keys()))}
SAMPLE DATA: {json.dumps(table_info['sample_data'][:5], default=str)}{distinct_section}

QUERY PATTERNS (use the appropriate pattern):

1. RANKING ("what rank is X", "position of X"):
   CRITICAL: Calculate rank for ALL rows first, then filter OUTSIDE the CTE!
   WITH ranked AS (
     SELECT entity, SUM(metric) as total, 
            ROW_NUMBER() OVER (ORDER BY SUM(metric) DESC) as rank
     FROM table 
     GROUP BY entity  -- NO WHERE clause here!
   ) 
   SELECT * FROM ranked WHERE entity ILIKE '%search%'  -- Filter AFTER ranking!

2. TOP N ("top 5", "best 10"):
   SELECT entity, SUM(metric) as total FROM table 
   GROUP BY entity ORDER BY total DESC LIMIT N

3. COMPARISON ("X vs Y", "compare"):
   SELECT entity, SUM(metric) as total FROM table 
   WHERE entity ILIKE '%X%' OR entity ILIKE '%Y%' GROUP BY entity

4. PERCENTAGE ("% of total", "share"):
   SELECT entity, SUM(metric) as value,
          ROUND((100.0 * SUM(metric) / (SELECT SUM(metric) FROM table))::numeric, 2) as percentage
   FROM table GROUP BY entity

5. FILTERING ("in region X", "where"):
   Use ILIKE '%value%' for text columns, = for exact matches

6. AGGREGATION ("total", "sum", "average", "count"):
   Use SUM(), AVG(), COUNT(), MIN(), MAX() with GROUP BY

7. TREND ("by month", "over time"):
   GROUP BY time_column ORDER BY time_column

IMPORTANT PostgreSQL Rules:
- ROUND with decimals MUST cast to numeric: ROUND(value::numeric, 2) NOT ROUND(value, 2)
- Always use ::numeric before ROUND when rounding to decimal places

QUESTION: {question}

OUTPUT: Only the SQL query, nothing else."""
        
        sql = extract_sql(llm_call(sql_prompt, max_tokens=500))
        step_times["sql_generation"] = round(time.time() - step_start, 3)
        logger.info(f"⏱️ Step 3 - SQL Generation (LLM): {step_times['sql_generation']}s")
        
        # 4. Validate & run SQL
        step_start = time.time()
        validate_sql(sql)
        step_times["sql_validation"] = round(time.time() - step_start, 3)
        logger.info(f"⏱️ Step 4a - SQL Validation: {step_times['sql_validation']}s")
        
        step_start = time.time()
        rows, cols = run_sql(sql)
        step_times["sql_execution"] = round(time.time() - step_start, 3)
        logger.info(f"⏱️ Step 4b - SQL Execution: {step_times['sql_execution']}s")

        # 5. Prepare results
        step_start = time.time()
        columns_list = list(cols)
        result_data = [dict(zip(columns_list, row)) for row in rows]
        step_times["result_preparation"] = round(time.time() - step_start, 3)
        logger.info(f"⏱️ Step 5 - Result Preparation: {step_times['result_preparation']}s")
        
        # 6. Generate answer from query results
        step_start = time.time()
        sample_for_llm = result_data[:20]
        
        answer_prompt = f"""Answer the question in natural language based on the query results below.

Question: {question}

Data ({len(result_data)} rows):
{json.dumps(sample_for_llm, default=str)}

RESPONSE GUIDELINES:
1. Start with a brief, friendly sentence answering the question directly
2. Present data as a simple numbered or bulleted list - DO NOT use markdown tables
3. Each list item should be clear and readable, like: "April: 87.23 days"
4. Format values for readability:
   - Currency/Sales/Revenue: Use Indian format - ₹38.85 Cr (crores), ₹19.49 L (lakhs)
   - Round decimals to 2 places
5. Keep the response concise and easy to scan
6. Do not use markdown table syntax (no | or --- characters)"""
        
        answer = llm_call(answer_prompt, temperature=0.1, max_tokens=1000)
        step_times["answer_generation"] = round(time.time() - step_start, 3)
        logger.info(f"⏱️ Step 6 - Answer Generation (LLM): {step_times['answer_generation']}s")
        
        duration = time.time() - start_time
        logger.info(f"⏱️ TOTAL TIME: {round(duration, 2)}s | Breakdown: {step_times}")

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
