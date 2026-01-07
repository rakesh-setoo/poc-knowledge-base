from fastapi import HTTPException, status


class AppException(Exception):
    status_code: int = 500
    
    def __init__(self, message: str, details: dict = None):
        self.message = message
        self.details = details or {}
        super().__init__(message)


class DatasetNotFoundError(AppException):
    status_code = 404


class NoDatasetError(AppException):
    status_code = 400


class SQLGenerationError(AppException):
    status_code = 400


class SQLExecutionError(AppException):
    status_code = 400


class SQLValidationError(AppException):
    status_code = 400


class FileUploadError(AppException):
    status_code = 400


class LLMError(AppException):
    status_code = 500


# HTTP Exception helpers
def not_found(message: str) -> HTTPException:
    return HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=message)


def bad_request(message: str) -> HTTPException:
    return HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=message)


def server_error(message: str) -> HTTPException:
    return HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=message)
