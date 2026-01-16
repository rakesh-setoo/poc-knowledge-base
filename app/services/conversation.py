"""
Conversation History Manager
Stores recent Q&A pairs in Redis for context-aware responses.
Uses chat_id as the conversation key for proper isolation between chats.

Redis-only implementation (no in-memory fallback).
"""
import json
from typing import List, Optional
import redis

from app.core.config import settings
from app.logging import logger


# Configuration
MAX_HISTORY_LENGTH = 20  
CONVERSATION_TTL_SECONDS = 2592000 

_redis_client: Optional[redis.Redis] = None


def _get_redis_client() -> redis.Redis:
    global _redis_client
    
    if _redis_client is not None:
        return _redis_client
    
    if not settings.redis_url:
        raise ConnectionError("REDIS_URL not configured in .env file")
    
    _redis_client = redis.from_url(
        settings.redis_url,
        decode_responses=True,
        socket_connect_timeout=5,
        socket_timeout=5
    )
    
    _redis_client.ping()
    logger.info(f"Redis connected: {settings.redis_url}")
    
    return _redis_client


def check_redis_connection() -> dict:
    """Check if Redis is connected and return status info."""
    try:
        client = _get_redis_client()
        info = client.info("server")
        return {
            "status": "connected",
            "redis_version": info.get("redis_version"),
            "connected_clients": client.info("clients").get("connected_clients"),
            "used_memory": client.info("memory").get("used_memory_human")
        }
    except ConnectionError as e:
        return {"status": "error", "message": str(e)}
    except redis.ConnectionError as e:
        return {"status": "error", "message": f"Redis connection failed: {e}"}
    except Exception as e:
        return {"status": "error", "message": str(e)}


def _get_redis_key(chat_id: int) -> str:
    return f"excel_ai:conv:history:chat:{chat_id}"


def add_to_history(
    chat_id: int,
    question: str,
    answer: str,
    columns: list = None,
    data: list = None,
    viz_type: str = None
) -> None:
    """Add a Q&A pair to conversation history with optional visualization data."""
    client = _get_redis_client()
    
    answer_summary = answer[:200] + "..." if len(answer) > 200 else answer
    
    new_entry = {
        "question": question,
        "answer": answer_summary
    }
    
    # Store visualization metadata for chart recreation
    if viz_type:
        new_entry["viz_type"] = viz_type
    if columns:
        new_entry["columns"] = columns
    if data:
        # Limit stored data to 100 rows for performance
        new_entry["data"] = data[:100] if len(data) > 100 else data
    
    key = _get_redis_key(chat_id)
    history = client.get(key)
    
    if history:
        history_list = json.loads(history)
    else:
        history_list = []
    
    history_list.append(new_entry)
    
    # Keep only last MAX_HISTORY_LENGTH
    if len(history_list) > MAX_HISTORY_LENGTH:
        history_list = history_list[-MAX_HISTORY_LENGTH:]
    
    client.setex(key, CONVERSATION_TTL_SECONDS, json.dumps(history_list))
    logger.debug(f"Added to Redis history for chat {chat_id}. Length: {len(history_list)}")


def get_history(chat_id: int) -> List[dict]:
    """Get the conversation history for a chat."""
    client = _get_redis_client()
    
    key = _get_redis_key(chat_id)
    history = client.get(key)
    
    if history:
        return json.loads(history)
    return []


def clear_history(chat_id: int) -> None:
    client = _get_redis_client()
    
    key = _get_redis_key(chat_id)
    client.delete(key)
    logger.debug(f"Cleared Redis history for chat {chat_id}")


def get_last_result(chat_id: int) -> dict:
    """
    Get the last query's data for follow-up visualization requests.
    Returns dict with 'columns', 'data', 'viz_type', 'question' if available.
    """
    if not chat_id:
        return {}
    
    try:
        history = get_history(chat_id)
        if history:
            last_entry = history[-1]
            if last_entry.get('data') and last_entry.get('columns'):
                return {
                    'columns': last_entry['columns'],
                    'data': last_entry['data'],
                    'viz_type': last_entry.get('viz_type'),
                    'question': last_entry.get('question', '')
                }
    except Exception as e:
        logger.warning(f"Failed to get last result: {e}")
    
    return {}


def format_history_for_prompt(chat_id: int) -> str:
    if not chat_id:
        return ""
    
    try:
        history = get_history(chat_id)
    except Exception as e:
        logger.warning(f"Failed to get history from Redis: {e}")
        return ""
    
    if not history:
        return ""
    
    if len(history) == 1:
        item = history[0]
        return f"""
CURRENT CONTEXT (use this for follow-up questions like "the 9th one", "that customer", "more details"):
Q: {item['question']}
A: {item['answer']}
"""
    
    background = history[:-1]  
    current = history[-1]      
    
    formatted = ""
    
    if background:
        formatted += "\nBACKGROUND CONTEXT (older conversation, for general reference only):\n"
        for i, item in enumerate(background, 1):
            q_brief = item['question'][:80] + "..." if len(item['question']) > 80 else item['question']
            a_brief = item['answer'][:500] + "..." if len(item['answer']) > 500 else item['answer']
            formatted += f"  {i}. Q: {q_brief}\n     A: {a_brief}\n"
    
    formatted += f"""
CURRENT CONTEXT (use this for follow-up questions like "the 9th one", "that customer", "more details"):
Q: {current['question']}
A: {current['answer']}
"""
    
    return formatted
