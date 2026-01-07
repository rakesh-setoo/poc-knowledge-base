import uuid
import json
import asyncio
from typing import AsyncGenerator
import pandas as pd

from app.db import engine, save_dataset_metadata
from app.parsers import get_parser, ParserRegistry
from app.logging import logger


async def process_upload_with_progress(
    file_content: bytes,
    filename: str,
    original_filename: str
) -> AsyncGenerator[str, None]:
    try:
        # Phase 1: Get parser (1%)
        yield _sse_event(1, "1% - Starting upload...")
        await asyncio.sleep(0.05)
        
        parser = get_parser(filename)
        if not parser:
            supported = ParserRegistry.get_supported_extensions_display()
            yield _sse_event(0, "Error", error=f"Unsupported file type. Supported: {supported}")
            return
        
        # Phase 2: Parse file (30-55%)
        yield _sse_event(30, f"30% - Parsing {parser.name} file...")
        await asyncio.sleep(0.05)
        
        try:
            df = await parser.parse(
                file_content, 
                filename,
                progress_callback=_create_progress_callback()
            )
        except ValueError as e:
            yield _sse_event(0, "Error", error=str(e))
            return
        
        # Phase 3: Clean column names (55-60%)
        yield _sse_event(55, "55% - Cleaning column names...")
        await asyncio.sleep(0.05)
        
        df = _clean_column_names(df)
        total_rows = len(df)
        
        yield _sse_event(60, f"60% - Data cleaned ({total_rows:,} rows)")
        await asyncio.sleep(0.05)
        
        # Phase 4: Save to database (60-92%)
        file_type = parser.name.lower()
        table_name = f"dataset_{uuid.uuid4().hex[:8]}"
        
        async for event in _save_to_database(df, table_name, total_rows):
            yield event
        
        # Phase 5: Save metadata (95-100%)
        yield _sse_event(95, "95% - Saving metadata...")
        await asyncio.sleep(0.05)
        
        metadata = {
            "table_name": table_name,
            "file_name": original_filename,
            "file_type": file_type,
            "columns": list(df.columns),
            "row_count": total_rows
        }
        
        save_dataset_metadata(metadata)
        
        yield _sse_event(98, "98% - Refreshing datasets...")
        await asyncio.sleep(0.05)
        
        # Reload datasets (handled by caller updating DATASETS)
        yield _sse_event(100, "100% - Upload complete!", result=metadata)
        
    except Exception as e:
        logger.error(f"Upload error: {str(e)}")
        yield _sse_event(0, "Error", error=str(e))


def _clean_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize column names to lowercase snake_case."""
    df.columns = (
        df.columns.astype(str)
        .str.lower()
        .str.replace(" ", "_")
        .str.replace(r"[^a-z0-9_]", "", regex=True)
    )
    return df


async def _save_to_database(
    df: pd.DataFrame, 
    table_name: str, 
    total_rows: int
) -> AsyncGenerator[str, None]:
    if total_rows <= 1000:
        yield _sse_event(75, "75% - Saving to database...")
        await asyncio.sleep(0.05)
        df.to_sql(table_name, engine, index=False, if_exists="replace")
        yield _sse_event(92, "92% - Database save complete")
    else:
        chunk_size = max(100, total_rows // 20)
        rows_inserted = 0
        
        for i in range(0, total_rows, chunk_size):
            chunk_df = df.iloc[i:i + chunk_size]
            if_exists = "replace" if i == 0 else "append"
            chunk_df.to_sql(table_name, engine, index=False, if_exists=if_exists)
            
            rows_inserted += len(chunk_df)
            progress = 60 + int((rows_inserted / total_rows) * 32)
            yield _sse_event(
                progress, 
                f"{progress}% - Saving rows {rows_inserted:,}/{total_rows:,}..."
            )
            await asyncio.sleep(0.02)


def _sse_event(progress: int, status: str, error: str = None, result: dict = None) -> str:
    data = {"progress": progress, "status": status}
    if error:
        data["error"] = error
    if result:
        data["result"] = result
    return f"data: {json.dumps(data)}\n\n"


def _create_progress_callback():
    async def callback(progress: int, status: str):
        pass  
    return callback
