import time
import json
from fastapi import APIRouter, Body
from fastapi.responses import JSONResponse, StreamingResponse

from app.routers.datasets import get_datasets
from app.services.query import (
    get_table_info, build_sql_prompt, build_answer_prompt, select_table
)
from app.core.llm import llm_call, llm_call_stream
from app.utils.sql_utils import validate_sql, run_sql, extract_sql
from app.schemas import AskResponse
from app.logging import NoDatasetError, SQLValidationError, SQLExecutionError, LLMError, logger


router = APIRouter(tags=["Query"])


def error_response(error: str, generated_sql: str = None, table_used: str = None):
    # Log the actual error to terminal for debugging
    logger.error(f"Backend error: {error}")
    if generated_sql:
        logger.error(f"Generated SQL: {generated_sql}")
    if table_used:
        logger.error(f"Table used: {table_used}")
    
    # Return generic message to frontend
    return JSONResponse(
        status_code=400,
        content={
            "error": "Something went wrong. Please try again."
        }
    )


@router.post("/ask", response_model=AskResponse)
async def ask_question(
    question: str = Body(...),
    dataset_id: int = Body(None, description="Optional dataset ID to query")
):
    start_time = time.time()
    generated_sql = None
    table_used = None
    
    try:
        # Phase 1: Fetch datasets
        phase_start = time.time()
        datasets = get_datasets()
        logger.info(f"[TIMING] Phase 1 - Dataset fetch: {(time.time() - phase_start):.2f}s")
        logger.info(f"Received query: '{question}' for dataset_id: {dataset_id}")
        
        if not datasets:
            raise NoDatasetError()
        
        # Phase 2: Table selection
        phase_start = time.time()
        table_used = select_table(question, datasets, dataset_id)
        logger.info(f"[TIMING] Phase 2 - Table selection: {(time.time() - phase_start):.2f}s")
        
        # Phase 3: Get table info
        phase_start = time.time()
        table_info = get_table_info(table_used)
        logger.info(f"[TIMING] Phase 3 - Table info retrieval: {(time.time() - phase_start):.2f}s")
        
        # Phase 4: Build SQL prompt and generate SQL
        phase_start = time.time()
        sql_prompt = build_sql_prompt(question, table_used, table_info)
        generated_sql = llm_call(sql_prompt)
        logger.info(f"[TIMING] Phase 4 - SQL generation (LLM): {(time.time() - phase_start):.2f}s")
        
        # Phase 5: Extract SQL
        phase_start = time.time()
        generated_sql = extract_sql(generated_sql)
        logger.info(f"[TIMING] Phase 5 - SQL extraction: {(time.time() - phase_start):.2f}s")
        
        # Phase 6: Validate SQL
        phase_start = time.time()
        try:
            validated_sql = validate_sql(generated_sql)
            logger.info(f"[TIMING] Phase 6 - SQL validation: {(time.time() - phase_start):.2f}s")
        except SQLValidationError as e:
            return error_response(str(e), generated_sql, table_used)
        
        # Phase 7: Execute SQL
        phase_start = time.time()
        try:
            rows, columns = run_sql(validated_sql)
            logger.info(f"[TIMING] Phase 7 - SQL execution: {(time.time() - phase_start):.2f}s")
        except SQLExecutionError as e:
            return error_response(str(e), generated_sql, table_used)
        
        result_data = [dict(zip(columns, row)) for row in rows]
        
        # Phase 8: Generate answer
        phase_start = time.time()
        answer_prompt = build_answer_prompt(question, result_data)
        answer = llm_call(answer_prompt)
        logger.info(f"[TIMING] Phase 8 - Answer generation (LLM): {(time.time() - phase_start):.2f}s")
        
        elapsed = time.time() - start_time
        logger.info(f"[TIMING] TOTAL: {elapsed:.2f}s for query: {question[:50]}...")
        
        return AskResponse(
            table_used=table_used,
            generated_sql=validated_sql,
            answer=answer,
            columns=columns,
            data=result_data,
            row_count=len(result_data)
        )
        
    except NoDatasetError:
        raise
    except ValueError as e:
        return error_response(str(e), generated_sql, table_used)
    except SQLValidationError as e:
        return error_response(str(e), generated_sql, table_used)
    except SQLExecutionError as e:
        return error_response(str(e), generated_sql, table_used)
    except LLMError as e:
        return error_response(str(e), generated_sql, table_used)
    except Exception as e:
        logger.exception(f"Unexpected query error: {str(e)}")
        return error_response(str(e), generated_sql, table_used)


@router.post("/ask-stream")
async def ask_question_stream(
    question: str = Body(...),
    dataset_id: int = Body(None, description="Optional dataset ID to query")
):
    """Streaming endpoint that sends data immediately, then streams the answer."""
    
    def generate():
        start_time = time.time()
        generated_sql = None
        table_used = None
        
        try:
            # Phase 1-7: Same as non-streaming endpoint
            datasets = get_datasets()
            if not datasets:
                yield f"data: {json.dumps({'error': 'No datasets available'})}\n\n"
                return
            
            table_used = select_table(question, datasets, dataset_id)
            table_info = get_table_info(table_used)
            
            sql_prompt = build_sql_prompt(question, table_used, table_info)
            generated_sql = llm_call(sql_prompt)
            generated_sql = extract_sql(generated_sql)
            
            try:
                validated_sql = validate_sql(generated_sql)
            except SQLValidationError as e:
                logger.error(f"SQL validation error: {e}")
                yield f"data: {json.dumps({'error': 'Something went wrong. Please try again.'})}\n\n"
                return
            
            try:
                rows, columns = run_sql(validated_sql)
            except SQLExecutionError as e:
                logger.error(f"SQL execution error: {e}")
                yield f"data: {json.dumps({'error': 'Something went wrong. Please try again.'})}\n\n"
                return
            
            result_data = [dict(zip(columns, row)) for row in rows]
            
            # Send metadata immediately (table, SQL, columns, data)
            metadata = {
                "type": "metadata",
                "table_used": table_used,
                "generated_sql": validated_sql,
                "columns": columns,
                "data": result_data,
                "row_count": len(result_data)
            }
            yield f"data: {json.dumps(metadata, default=str)}\n\n"
            
            # Phase 8: Stream answer generation
            answer_prompt = build_answer_prompt(question, result_data)
            
            for token in llm_call_stream(answer_prompt):
                yield f"data: {json.dumps({'type': 'token', 'content': token})}\n\n"
            
            # Signal completion
            elapsed = time.time() - start_time
            logger.info(f"[TIMING] STREAM TOTAL: {elapsed:.2f}s for query: {question[:50]}...")
            yield f"data: {json.dumps({'type': 'done', 'elapsed': round(elapsed, 2)})}\n\n"
            
        except Exception as e:
            logger.exception(f"Stream error: {str(e)}")
            yield f"data: {json.dumps({'error': 'Something went wrong. Please try again.'})}\n\n"
    
    return StreamingResponse(
        generate(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no"
        }
    )
