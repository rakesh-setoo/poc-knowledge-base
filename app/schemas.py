"""
Pydantic schemas for request/response validation.
Provides type-safe API contracts with proper documentation.
"""
from typing import Optional, Any
from pydantic import BaseModel, Field


# =============================================================================
# Dataset Schemas
# =============================================================================

class DatasetInfo(BaseModel):
    """Information about an uploaded dataset."""
    table_name: str = Field(..., description="Database table name")
    file_name: str = Field(..., description="Original file name")
    columns: list[str] = Field(..., description="Column names")
    row_count: int = Field(..., description="Number of rows")
    
    class Config:
        json_schema_extra = {
            "example": {
                "table_name": "excel_abc12345",
                "file_name": "sales_data.xlsx",
                "columns": ["date", "product", "revenue"],
                "row_count": 1500
            }
        }


class DatasetListResponse(BaseModel):
    """Response for listing datasets."""
    datasets: list[DatasetInfo] = Field(default_factory=list)
    count: int = Field(..., description="Total number of datasets")


class DatasetDeleteResponse(BaseModel):
    """Response for dataset deletion."""
    message: str
    file_name: str


class SyncResponse(BaseModel):
    """Response for sync operation."""
    synced: list[DatasetInfo] = Field(default_factory=list)
    total_datasets: int


# =============================================================================
# Question/Answer Schemas
# =============================================================================

class AskRequest(BaseModel):
    """Request body for asking a question."""
    question: str = Field(..., min_length=1, max_length=1000, description="Natural language question about the data")
    
    class Config:
        json_schema_extra = {
            "example": {
                "question": "What are the top 10 customers by sales?"
            }
        }


class AskResponse(BaseModel):
    """Response for a question."""
    table_used: str = Field(..., description="Table used for the query")
    generated_sql: str = Field(..., description="Generated SQL query")
    answer: str = Field(..., description="Natural language answer")
    columns: list[str] = Field(..., description="Result column names")
    data: list[dict[str, Any]] = Field(..., description="Query result data")
    row_count: int = Field(..., description="Total number of rows")
    
    class Config:
        json_schema_extra = {
            "example": {
                "table_used": "excel_abc12345",
                "generated_sql": "SELECT customer, SUM(revenue) FROM excel_abc12345 GROUP BY customer ORDER BY 2 DESC LIMIT 10",
                "answer": "The top customer is Acme Corp with ₹50 Lakhs in sales.",
                "columns": ["customer", "total_revenue"],
                "data": [{"customer": "Acme Corp", "total_revenue": 5000000}],
                "row_count": 10
            }
        }


class ErrorResponse(BaseModel):
    """Error response format."""
    error: str = Field(..., description="Error message")
    generated_sql: Optional[str] = Field(None, description="SQL that caused the error")
    table_used: Optional[str] = Field(None, description="Table that was used")


# =============================================================================
# Health Check Schemas
# =============================================================================

class HealthResponse(BaseModel):
    """Health check response."""
    status: str = Field(..., description="Overall health status")
    database: str = Field(..., description="Database connection status")
    version: str = Field(..., description="Application version")
