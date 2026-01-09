import json
from typing import List, Optional
from datetime import datetime
from sqlalchemy import text

from app.db import engine
from app.logging import logger


def create_chat(dataset_id: Optional[int] = None, title: str = "New Chat", system_prompt: Optional[str] = None) -> dict:
    try:
        with engine.connect() as conn:
            result = conn.execute(text("""
                INSERT INTO chats (title, dataset_id, system_prompt)
                VALUES (:title, :dataset_id, :system_prompt)
                RETURNING id, title, dataset_id, system_prompt, created_at, updated_at
            """), {"title": title, "dataset_id": dataset_id, "system_prompt": system_prompt})
            
            row = result.fetchone()
            conn.commit()
            
            return {
                "id": row[0],
                "title": row[1],
                "dataset_id": row[2],
                "system_prompt": row[3],
                "created_at": str(row[4]),
                "updated_at": str(row[5]),
                "message_count": 0
            }
    except Exception as e:
        logger.error(f"Failed to create chat: {e}")
        raise


def get_chats(limit: int = 50) -> List[dict]:
    try:
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT c.id, c.title, c.dataset_id, c.created_at, c.updated_at,
                       COUNT(m.id) as message_count
                FROM chats c
                LEFT JOIN messages m ON c.id = m.chat_id
                GROUP BY c.id
                ORDER BY c.updated_at DESC
                LIMIT :limit
            """), {"limit": limit})
            
            chats = []
            for row in result:
                chats.append({
                    "id": row[0],
                    "title": row[1],
                    "dataset_id": row[2],
                    "created_at": str(row[3]),
                    "updated_at": str(row[4]),
                    "message_count": row[5]
                })
            return chats
    except Exception as e:
        logger.error(f"Failed to get chats: {e}")
        return []


def get_chat(chat_id: int) -> Optional[dict]:
    try:
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT id, title, dataset_id, system_prompt, created_at, updated_at
                FROM chats WHERE id = :chat_id
            """), {"chat_id": chat_id})
            
            row = result.fetchone()
            if not row:
                return None
                
            return {
                "id": row[0],
                "title": row[1],
                "dataset_id": row[2],
                "system_prompt": row[3],
                "created_at": str(row[4]),
                "updated_at": str(row[5])
            }
    except Exception as e:
        logger.error(f"Failed to get chat: {e}")
        return None


def update_system_prompt(chat_id: int, system_prompt: str) -> bool:
    try:
        with engine.connect() as conn:
            conn.execute(text("""
                UPDATE chats SET system_prompt = :system_prompt, updated_at = NOW()
                WHERE id = :chat_id
            """), {"chat_id": chat_id, "system_prompt": system_prompt})
            conn.commit()
            return True
    except Exception as e:
        logger.error(f"Failed to update system prompt: {e}")
        return False


def delete_chat(chat_id: int) -> bool:
    try:
        with engine.connect() as conn:
            conn.execute(text("DELETE FROM chats WHERE id = :chat_id"), {"chat_id": chat_id})
            conn.commit()
            logger.info(f"Deleted chat {chat_id}")
            return True
    except Exception as e:
        logger.error(f"Failed to delete chat: {e}")
        return False


def update_chat_title(chat_id: int, title: str) -> bool:
    try:
        with engine.connect() as conn:
            conn.execute(text("""
                UPDATE chats SET title = :title, updated_at = NOW()
                WHERE id = :chat_id
            """), {"chat_id": chat_id, "title": title})
            conn.commit()
            return True
    except Exception as e:
        logger.error(f"Failed to update chat title: {e}")
        return False


def add_message(chat_id: int, role: str, content: str, metadata: dict = None) -> dict:
    try:
        with engine.connect() as conn:
            result = conn.execute(text("""
                INSERT INTO messages (chat_id, role, content, metadata)
                VALUES (:chat_id, :role, :content, :metadata)
                RETURNING id, chat_id, role, content, metadata, created_at
            """), {
                "chat_id": chat_id,
                "role": role,
                "content": content,
                "metadata": json.dumps(metadata) if metadata else None
            })
            
            row = result.fetchone()
            
            conn.execute(text("""
                UPDATE chats SET updated_at = NOW() WHERE id = :chat_id
            """), {"chat_id": chat_id})
            
            conn.commit()
            
            return {
                "id": row[0],
                "chat_id": row[1],
                "role": row[2],
                "content": row[3],
                "metadata": row[4] if row[4] else None,
                "created_at": str(row[5])
            }
    except Exception as e:
        logger.error(f"Failed to add message: {e}")
        raise


def get_messages(chat_id: int, limit: int = 100) -> List[dict]:
    try:
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT id, chat_id, role, content, metadata, created_at
                FROM messages
                WHERE chat_id = :chat_id
                ORDER BY created_at ASC
                LIMIT :limit
            """), {"chat_id": chat_id, "limit": limit})
            
            messages = []
            for row in result:
                messages.append({
                    "id": row[0],
                    "chat_id": row[1],
                    "role": row[2],
                    "content": row[3],
                    "metadata": row[4] if row[4] else None,
                    "created_at": str(row[5])
                })
            return messages
    except Exception as e:
        logger.error(f"Failed to get messages: {e}")
        return []


def auto_generate_title(chat_id: int, first_question: str) -> str:
    title = first_question[:50].strip()
    if len(first_question) > 50:
        title += "..."
    
    update_chat_title(chat_id, title)
    return title
