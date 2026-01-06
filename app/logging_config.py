"""
Logging configuration for the application.
Provides structured logging with minimal output for production.
"""
import logging
import sys
from datetime import datetime
from app.config import settings


class SimpleFormatter(logging.Formatter):
    """Simple, clean log formatter."""
    
    def format(self, record: logging.LogRecord) -> str:
        timestamp = datetime.now().strftime("%H:%M:%S")
        return f"{timestamp} | {record.levelname:5} | {record.getMessage()}"


def setup_logging() -> logging.Logger:
    """Configure and return the application logger."""
    log_level = getattr(logging, settings.log_level.upper(), logging.INFO)
    
    # Create logger
    logger = logging.getLogger("app")
    logger.setLevel(log_level)
    logger.handlers.clear()
    
    # Console handler with simple format
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(log_level)
    console_handler.setFormatter(SimpleFormatter())
    logger.addHandler(console_handler)
    
    # Reduce noise from other libraries
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("openai").setLevel(logging.WARNING)
    logging.getLogger("sqlalchemy").setLevel(logging.WARNING)
    
    return logger


# Create and export logger
logger = setup_logging()
