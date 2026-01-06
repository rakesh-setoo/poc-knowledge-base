"""
Database configuration and operations.
Provides connection pooling, health checks, and CRUD operations for datasets.
"""
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.pool import QueuePool
import json

from app.config import settings
from app.logging_config import logger


# Create engine with connection pooling for production
engine = create_engine(
    settings.database_url,
    poolclass=QueuePool,
    pool_size=settings.db_pool_size,
    max_overflow=settings.db_max_overflow,
    pool_pre_ping=True,  # Verify connections before use
    pool_recycle=3600,   # Recycle connections after 1 hour
)


def check_database_health() -> bool:
    """Check if database is accessible."""
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return True
    except Exception as e:
        logger.error(f"Database health check failed: {e}")
        return False


def init_metadata_table():
    """Create the dataset_metadata table if it doesn't exist."""
    try:
        with engine.connect() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS dataset_metadata (
                    id SERIAL PRIMARY KEY,
                    table_name VARCHAR(255) UNIQUE NOT NULL,
                    file_name VARCHAR(255) NOT NULL,
                    columns TEXT NOT NULL,
                    row_count INTEGER NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """))
            conn.commit()
        logger.info("Metadata table initialized")
    except Exception as e:
        logger.error(f"Failed to initialize metadata table: {e}")
        raise


def save_dataset_metadata(metadata: dict):
    """Save dataset metadata to the database."""
    try:
        with engine.connect() as conn:
            conn.execute(text("""
                INSERT INTO dataset_metadata (table_name, file_name, columns, row_count)
                VALUES (:table_name, :file_name, :columns, :row_count)
                ON CONFLICT (table_name) DO UPDATE SET
                    file_name = EXCLUDED.file_name,
                    columns = EXCLUDED.columns,
                    row_count = EXCLUDED.row_count
            """), {
                "table_name": metadata["table_name"],
                "file_name": metadata["file_name"],
                "columns": json.dumps(metadata["columns"]),
                "row_count": metadata["row_count"]
            })
            conn.commit()
        logger.info(f"Saved metadata for table: {metadata['table_name']}")
    except Exception as e:
        logger.error(f"Failed to save dataset metadata: {e}")
        raise


def load_all_datasets() -> list:
    """Load all dataset metadata from the database."""
    inspector = inspect(engine)
    if "dataset_metadata" not in inspector.get_table_names():
        return []
    
    try:
        with engine.connect() as conn:
            result = conn.execute(text("""
                SELECT table_name, file_name, columns, row_count 
                FROM dataset_metadata
                ORDER BY created_at DESC
            """))
            datasets = []
            for row in result:
                datasets.append({
                    "table_name": row[0],
                    "file_name": row[1],
                    "columns": json.loads(row[2]),
                    "row_count": row[3]
                })
            return datasets
    except Exception as e:
        logger.error(f"Failed to load datasets: {e}")
        return []


def delete_dataset_metadata(table_name: str):
    """Delete dataset metadata from the database."""
    try:
        with engine.connect() as conn:
            conn.execute(text("""
                DELETE FROM dataset_metadata WHERE table_name = :table_name
            """), {"table_name": table_name})
            conn.commit()
        logger.info(f"Deleted metadata for table: {table_name}")
    except Exception as e:
        logger.error(f"Failed to delete dataset metadata: {e}")
        raise
