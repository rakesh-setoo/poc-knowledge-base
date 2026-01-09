from typing import Optional
from sqlalchemy import text

from app.db import engine
from app.logging import logger


def get_setting(key: str) -> Optional[str]:
    """Get a setting value by key."""
    try:
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT value FROM settings WHERE key = :key
            """), {"key": key})
            row = result.fetchone()
            return row[0] if row else None
    except Exception as e:
        logger.error(f"Failed to get setting '{key}': {e}")
        return None


def set_setting(key: str, value: str) -> bool:
    """Set a setting value (upsert)."""
    try:
        with engine.connect() as conn:
            conn.execute(text("""
                INSERT INTO settings (key, value, updated_at)
                VALUES (:key, :value, NOW())
                ON CONFLICT (key) DO UPDATE SET value = :value, updated_at = NOW()
            """), {"key": key, "value": value})
            conn.commit()
            return True
    except Exception as e:
        logger.error(f"Failed to set setting '{key}': {e}")
        return False


def get_global_system_prompt() -> Optional[str]:
    return get_setting("global_system_prompt")


def set_global_system_prompt(prompt: str) -> bool:
    return set_setting("global_system_prompt", prompt)
