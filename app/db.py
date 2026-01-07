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
    pool_pre_ping=True,
    pool_recycle=3600,
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
    """Create the datasets table with full metadata."""
    try:
        with engine.connect() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS datasets (
                    id SERIAL PRIMARY KEY,
                    table_name VARCHAR(255) UNIQUE NOT NULL,
                    file_name VARCHAR(255) NOT NULL,
                    file_type VARCHAR(50) NOT NULL,
                    columns TEXT NOT NULL,
                    row_count INTEGER NOT NULL,
                    uploaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """))
            conn.commit()
    except Exception as e:
        logger.error(f"Failed to initialize datasets table: {e}")
        raise


def save_dataset_metadata(metadata: dict):
    """Save dataset metadata to the database."""
    try:
        with engine.connect() as conn:
            conn.execute(text("""
                INSERT INTO datasets (table_name, file_name, file_type, columns, row_count)
                VALUES (:table_name, :file_name, :file_type, :columns, :row_count)
                ON CONFLICT (table_name) DO UPDATE SET
                    file_name = EXCLUDED.file_name,
                    file_type = EXCLUDED.file_type,
                    columns = EXCLUDED.columns,
                    row_count = EXCLUDED.row_count
            """), {
                "table_name": metadata["table_name"],
                "file_name": metadata["file_name"],
                "file_type": metadata.get("file_type", "unknown"),
                "columns": json.dumps(metadata["columns"]),
                "row_count": metadata["row_count"]
            })
            conn.commit()
    except Exception as e:
        logger.error(f"Failed to save dataset metadata: {e}")
        raise


def load_all_datasets() -> list:
    """Load all dataset metadata from the database."""
    inspector = inspect(engine)
    
    # Check for new table first, fall back to old table
    if "datasets" in inspector.get_table_names():
        table_name = "datasets"
        query = """
            SELECT id, table_name, file_name, file_type, columns, row_count, uploaded_at
            FROM datasets ORDER BY uploaded_at DESC
        """
    elif "dataset_metadata" in inspector.get_table_names():
        table_name = "dataset_metadata"
        query = """
            SELECT id, table_name, file_name, 'excel' as file_type, columns, row_count, created_at
            FROM dataset_metadata ORDER BY created_at DESC
        """
    else:
        return []
    
    try:
        with engine.connect() as conn:
            result = conn.execute(text(query))
            datasets = []
            for row in result:
                datasets.append({
                    "id": row[0],
                    "table_name": row[1],
                    "file_name": row[2],
                    "file_type": row[3],
                    "columns": json.loads(row[4]) if isinstance(row[4], str) else row[4],
                    "row_count": row[5],
                    "uploaded_at": str(row[6]) if row[6] else None
                })
            return datasets
    except Exception as e:
        logger.error(f"Failed to load datasets: {e}")
        return []


def delete_dataset_metadata(table_name: str):
    """Delete dataset metadata from the database."""
    try:
        with engine.connect() as conn:
            # Try new table first
            conn.execute(text("DELETE FROM datasets WHERE table_name = :table_name"), {"table_name": table_name})
            conn.commit()
    except Exception:
        try:
            with engine.connect() as conn:
                conn.execute(text("DELETE FROM dataset_metadata WHERE table_name = :table_name"), {"table_name": table_name})
                conn.commit()
        except Exception as e:
            logger.error(f"Failed to delete dataset metadata: {e}")
            raise
