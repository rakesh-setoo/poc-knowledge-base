import json
from fastapi import APIRouter, UploadFile
from fastapi.responses import StreamingResponse
from sqlalchemy import text, inspect

from app.db import (
    engine, save_dataset_metadata, load_all_datasets, 
    delete_dataset_metadata
)
from app.schemas import (
    DatasetListResponse, DatasetInfo, SyncResponse, DatasetDeleteResponse
)
from app.logging import DatasetNotFoundError, logger
from app.services.upload import process_upload_with_progress
from app.services.cache import invalidate_table_cache


router = APIRouter(prefix="/datasets", tags=["Datasets"])


DATASETS: list[dict] = []


def get_datasets() -> list[dict]:
    global DATASETS
    return DATASETS


def refresh_datasets():
    global DATASETS
    DATASETS = load_all_datasets()
    return DATASETS


@router.get("", response_model=DatasetListResponse)
def list_datasets():
    return DatasetListResponse(datasets=get_datasets(), count=len(get_datasets()))


@router.post("/sync", response_model=SyncResponse)
def sync_datasets():
    inspector = inspect(engine)
    existing_tables = inspector.get_table_names()
    
    known_tables = {d["table_name"] for d in get_datasets()}
    synced = []
    
    for table in existing_tables:
        if table.startswith("dataset_") and table not in known_tables:
            try:
                with engine.connect() as conn:
                    columns_query = text(f"""
                        SELECT column_name FROM information_schema.columns 
                        WHERE table_name = :table_name
                    """)
                    columns_result = conn.execute(columns_query, {"table_name": table})
                    columns = [row[0] for row in columns_result]
                    
                    count_result = conn.execute(text(f'SELECT COUNT(*) FROM "{table}"'))
                    row_count = count_result.scalar()
                
                metadata = {
                    "table_name": table,
                    "file_name": f"{table} (synced)",
                    "file_type": "synced",
                    "columns": columns,
                    "row_count": row_count
                }
                save_dataset_metadata(metadata)
                synced.append(DatasetInfo(**metadata))
                
            except Exception as e:
                logger.warning(f"Failed to sync table {table}: {e}")
    
    refresh_datasets()
    return SyncResponse(synced=synced, total_datasets=len(get_datasets()))


@router.delete("/{table_name}", response_model=DatasetDeleteResponse)
def delete_dataset(table_name: str):
    dataset = next((d for d in get_datasets() if d["table_name"] == table_name), None)
    if not dataset:
        raise DatasetNotFoundError(table_name)
    
    try:
        with engine.connect() as conn:
            conn.execute(text(f'DROP TABLE IF EXISTS "{table_name}"'))
            conn.commit()
        delete_dataset_metadata(table_name)
        
        # Invalidate Redis cache for this table
        invalidate_table_cache(table_name)
        
        refresh_datasets()
        logger.info(f"Deleted dataset: {table_name}")
    except Exception as e:
        logger.error(f"Failed to delete dataset: {e}")
        raise
    
    return DatasetDeleteResponse(
        message=f"Dataset '{table_name}' deleted", 
        file_name=dataset["file_name"]
    )


upload_router = APIRouter(tags=["Datasets"])


@upload_router.post("/upload-excel")
async def upload_excel(file: UploadFile):
    import asyncio
    
    async def process_with_progress():
        chunks = []
        total_size = 0
        chunk_size = 64 * 1024 
        
        yield f"data: {json.dumps({'progress': 1, 'status': '1% - Starting upload...'})}\\n\\n"
        await asyncio.sleep(0.05)
        
        while True:
            chunk = await file.read(chunk_size)
            if not chunk:
                break
            chunks.append(chunk)
            total_size += len(chunk)
            
            # Calculate progress (0-30% for reading)
            read_progress = min(28, int((total_size / (1024 * 1024)) * 5) + 2)
            yield f"data: {json.dumps({'progress': read_progress, 'status': f'{read_progress}% - Reading file... ({total_size // 1024} KB)'})}\\n\\n"
            await asyncio.sleep(0.02)
        
        content = b''.join(chunks)
        
        # Use upload service for the rest
        async for event in process_upload_with_progress(
            content, 
            file.filename.lower(), 
            file.filename
        ):
            yield event
            
            if '"progress": 100' in event:
                refresh_datasets()
    
    return StreamingResponse(
        process_with_progress(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no"
        }
    )
