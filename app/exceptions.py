"""
Custom exception classes for the application.
Provides structured error handling with proper HTTP status codes.
"""
from fastapi import HTTPException, status


class AppException(Exception):
    """Base exception for all application errors."""
    
    def __init__(self, message: str, details: dict = None):
        self.message = message
        self.details = details or {}
        super().__init__(message)


class DatasetNotFoundError(AppException):
    """Raised when a requested dataset doesn't exist."""
    pass


class NoDatasetError(AppException):
    """Raised when no datasets are available."""
    pass


class SQLGenerationError(AppException):
    """Raised when SQL generation fails."""
    pass


class SQLExecutionError(AppException):
    """Raised when SQL execution fails."""
    pass


class SQLValidationError(AppException):
    """Raised when SQL validation fails."""
    pass


class FileUploadError(AppException):
    """Raised when file upload fails."""
    pass


class LLMError(AppException):
    """Raised when LLM call fails."""
    pass


# HTTP Exception helpers
def not_found(message: str) -> HTTPException:
    """Return a 404 Not Found exception."""
    return HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=message)


def bad_request(message: str) -> HTTPException:
    """Return a 400 Bad Request exception."""
    return HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=message)


def server_error(message: str) -> HTTPException:
    """Return a 500 Internal Server Error exception."""
    return HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=message)
