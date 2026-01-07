import time
from fastapi import APIRouter, Body
from fastapi.responses import JSONResponse

from app.routers.datasets import get_datasets
from app.services.query import (
    get_table_info, build_sql_prompt, build_answer_prompt, select_table
)
from app.core.llm import llm_call
from app.utils.sql_utils import validate_sql, run_sql, extract_sql
from app.schemas import AskResponse
from app.logging import NoDatasetError, SQLValidationError, SQLExecutionError, LLMError, logger


router = APIRouter(tags=["Query"])


def error_response(error: str, generated_sql: str = None, table_used: str = None):
    return JSONResponse(
        status_code=400,
        content={
            "error": error,
            "generated_sql": generated_sql,
            "table_used": table_used
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
        datasets = get_datasets()
        logger.info(f"Received query: '{question}' for dataset_id: {dataset_id}")
        
        if not datasets:
            raise NoDatasetError()
        
        table_used = select_table(question, datasets, dataset_id)
        
        table_info = get_table_info(table_used)
        
        sql_prompt = build_sql_prompt(question, table_used, table_info)
        generated_sql = llm_call(sql_prompt)
        generated_sql = extract_sql(generated_sql)
        
        try:
            validated_sql = validate_sql(generated_sql)
        except SQLValidationError as e:
            return error_response(str(e), generated_sql, table_used)
        
        try:
            rows, columns = run_sql(validated_sql)
        except SQLExecutionError as e:
            return error_response(str(e), generated_sql, table_used)
        
        result_data = [dict(zip(columns, row)) for row in rows]
        
        answer_prompt = build_answer_prompt(question, result_data)
        answer = llm_call(answer_prompt)
        
        elapsed = time.time() - start_time
        logger.info(f"Query completed in {elapsed:.2f}s: {question[:50]}...")
        
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
        logger.error(f"Query error: {str(e)}")
        return error_response(f"An unexpected error occurred: {str(e)}", generated_sql, table_used)
