import time
import json
from decimal import Decimal
from fastapi import APIRouter, Body
from fastapi.responses import JSONResponse, StreamingResponse

from app.routers.datasets import get_datasets
from app.services.query import (
    get_table_info, build_sql_prompt, build_answer_prompt, select_table
)
from app.services.conversation import add_to_history, format_history_for_prompt
from app.services.visualization import detect_visualization_type
from app.services.chat import add_message, get_messages, create_chat, auto_generate_title, get_chat
from app.services.settings import get_global_system_prompt
from app.core.llm import llm_call, llm_call_stream
from app.utils.sql_utils import validate_sql, run_sql, extract_sql
from app.schemas import AskResponse
from app.logging import NoDatasetError, SQLValidationError, SQLExecutionError, LLMError, logger


router = APIRouter(tags=["Query"])


def error_response(error: str, generated_sql: str = None, table_used: str = None):
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



@router.post("/ask-stream")
async def ask_question_stream(
    question: str = Body(...),
    dataset_id: int = Body(None, description="Optional dataset ID to query"),
    chat_id: int = Body(None, description="Optional chat ID for conversation")
):
    """Streaming endpoint that sends data immediately, then streams the answer."""
    
    def generate():
        nonlocal chat_id
        start_time = time.time()
        generated_sql = None
        table_used = None
        is_first_message = False
        
        try:
            # Create new chat if not provided
            chat_system_prompt = None
            if not chat_id:
                chat = create_chat(dataset_id=dataset_id)
                chat_id = chat["id"]
                is_first_message = True
                chat_system_prompt = chat.get("system_prompt")
                logger.info(f"[STREAM] Created new chat: {chat_id}")
            else:
                # Fetch existing chat to get system_prompt
                chat = get_chat(chat_id)
                if chat:
                    chat_system_prompt = chat.get("system_prompt")
            
            # Fetch global system prompt and combine
            global_prompt = get_global_system_prompt()
            
            # Combine prompts: Global first, then Chat specific (which can override)
            system_prompt = ""
            if global_prompt:
                system_prompt += f"GLOBAL INSTRUCTIONS:\n{global_prompt}\n\n"
            if chat_system_prompt:
                system_prompt += f"CHAT SPECIFIC INSTRUCTIONS:\n{chat_system_prompt}"
            
            system_prompt = system_prompt.strip() or None
            
            # Save user message to chat
            add_message(chat_id, "user", question)
            
            # Phase 1: Fetch datasets
            phase_start = time.time()
            datasets = get_datasets()
            logger.info(f"[STREAM TIMING] Phase 1 - Dataset fetch: {(time.time() - phase_start):.2f}s")
            logger.info(f"[STREAM] Received query: '{question}' for dataset_id: {dataset_id}, chat_id: {chat_id}")
            
            if not datasets:
                yield f"data: {json.dumps({'error': 'No datasets available'})}\n\n"
                return
            
            # Phase 2: Table selection
            phase_start = time.time()
            table_used = select_table(question, datasets, dataset_id)
            logger.info(f"[STREAM TIMING] Phase 2 - Table selection: {(time.time() - phase_start):.2f}s")
            
            # Phase 3: Get table info
            phase_start = time.time()
            table_info = get_table_info(table_used)
            logger.info(f"[STREAM TIMING] Phase 3 - Table info retrieval: {(time.time() - phase_start):.2f}s")
            
            # Get conversation history for context (uses chat_id for isolation)
            history_context = format_history_for_prompt(chat_id) if chat_id else ""
            
            # Phase 4: Build SQL prompt and generate SQL (with conversation context for follow-up questions)
            phase_start = time.time()
            sql_prompt = build_sql_prompt(question, table_used, table_info, history_context)
            generated_sql = llm_call(sql_prompt, max_tokens=1500)
            logger.info(f"[STREAM TIMING] Phase 4 - SQL generation (LLM): {(time.time() - phase_start):.2f}s")
            
            # Phase 5: Extract SQL
            phase_start = time.time()
            generated_sql = extract_sql(generated_sql)
            logger.info(f"[STREAM TIMING] Phase 5 - SQL extraction: {(time.time() - phase_start):.2f}s")
            
            # Phase 6: Validate SQL
            phase_start = time.time()
            try:
                validated_sql = validate_sql(generated_sql)
                logger.info(f"[STREAM TIMING] Phase 6 - SQL validation: {(time.time() - phase_start):.2f}s")
            except SQLValidationError as e:
                logger.error(f"SQL validation error: {e}")
                yield f"data: {json.dumps({'error': 'Something went wrong. Please try again.'})}\n\n"
                return
            
            # Phase 7: Execute SQL
            phase_start = time.time()
            try:
                rows, columns = run_sql(validated_sql)
                logger.info(f"[STREAM TIMING] Phase 7 - SQL execution: {(time.time() - phase_start):.2f}s")
            except SQLExecutionError as e:
                logger.error(f"SQL execution error: {e}")
                yield f"data: {json.dumps({'error': 'Something went wrong. Please try again.'})}\n\n"
                return
            
            result_data = [dict(zip(columns, row)) for row in rows]
            
            # DEBUG: Log the SQL and raw results for troubleshooting
            logger.info(f"[DEBUG SQL] Generated: {validated_sql}")
            logger.info(f"[DEBUG DATA] Columns: {columns}")
            logger.info(f"[DEBUG DATA] First 3 rows: {result_data[:3]}")
            
            # Convert non-JSON-serializable types (Decimal, datetime, etc.)
            for row_dict in result_data:
                for key, value in row_dict.items():
                    if isinstance(value, Decimal):
                        row_dict[key] = float(value)
                    elif hasattr(value, 'isoformat'):  # datetime, date, time
                        row_dict[key] = value.isoformat()
            
            # Detect best visualization type for this result
            viz_type = detect_visualization_type(question, columns, result_data)
            
            # Send metadata immediately (table, SQL, columns, data, chat_id, viz_type)
            metadata = {
                "type": "metadata",
                "chat_id": chat_id,
                "table_used": table_used,
                "generated_sql": validated_sql,
                "columns": columns,
                "data": result_data,
                "row_count": len(result_data),
                "viz_type": viz_type
            }
            yield f"data: {json.dumps(metadata, default=str)}\n\n"
            logger.info(f"[STREAM] Metadata sent - {len(result_data)} rows, viz_type: {viz_type}")
            
            # Phase 8: Stream answer generation with conversation history
            phase_start = time.time()
            
            # Build answer prompt with optional custom system instructions
            answer_prompt = build_answer_prompt(question, result_data, history_context, system_prompt)
            
            token_count = 0
            full_answer = []  # Collect answer for history storage
            for token in llm_call_stream(answer_prompt):
                yield f"data: {json.dumps({'type': 'token', 'content': token})}\n\n"
                full_answer.append(token)
                token_count += 1
            
            # Store this Q&A in conversation history with viz data
            answer_text = "".join(full_answer)
            if chat_id:
                add_to_history(
                    chat_id, question, answer_text,
                    columns=columns, data=result_data, viz_type=viz_type
                )
            
            # Save assistant message to database (include viz_type)
            add_message(chat_id, "assistant", answer_text, {
                "table_used": table_used,
                "generated_sql": validated_sql,
                "row_count": len(result_data),
                "viz_type": viz_type,
                "columns": columns,
                "data": result_data[:100]  # Limit stored data
            })
            
            # Auto-generate title from first question
            if is_first_message:
                auto_generate_title(chat_id, question)
            
            logger.info(f"[STREAM TIMING] Phase 8 - Answer generation (LLM): {(time.time() - phase_start):.2f}s ({token_count} tokens)")
            
            # Signal completion
            elapsed = time.time() - start_time
            logger.info(f"[STREAM TIMING] TOTAL: {elapsed:.2f}s for query: {question[:50]}...")
            yield f"data: {json.dumps({'type': 'done', 'chat_id': chat_id, 'elapsed': round(elapsed, 2)})}\n\n"
            
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
