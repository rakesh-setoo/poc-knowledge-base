from functools import lru_cache
from typing import Optional
from pydantic_settings import BaseSettings
from pydantic import Field, AliasChoices


class Settings(BaseSettings):
    # Application
    app_name: str = Field(default="Millenium Semiconductors AI", description="Application name")
    app_version: str = Field(default="1.0.0", description="Application version")
    debug: bool = Field(default=False, description="Debug mode")
    environment: str = Field(default="development", description="Environment (development/staging/production)")
    
    # Server
    host: str = Field(default="0.0.0.0", description="Server host")
    port: int = Field(default=8005, description="Server port")
    
    # Database - accept both uppercase (Render) and lowercase
    database_url: str = Field(
        ..., 
        validation_alias=AliasChoices('database_url', 'DATABASE_URL'),
        description="PostgreSQL connection URL"
    )
    db_pool_size: int = Field(default=5, description="Database connection pool size")
    db_max_overflow: int = Field(default=10, description="Max overflow connections")
    
    # OpenAI - accept both uppercase and lowercase
    openai_api_key: str = Field(
        default="",  # Empty default - will fail gracefully if not set
        validation_alias=AliasChoices('openai_api_key', 'OPENAI_API_KEY'),
        description="OpenAI API key"
    )
    openai_model: str = Field(default="gpt-4o", description="OpenAI model to use")
    
    # Security
    cors_origins: str = Field(default="*", description="Comma-separated list of allowed CORS origins")
    max_upload_size_mb: int = Field(default=10, description="Maximum file upload size in MB")
    
    # Logging
    log_level: str = Field(default="INFO", description="Logging level")
    log_format: str = Field(default="json", description="Log format (json/text)")
    
    # Rate Limiting
    rate_limit_enabled: bool = Field(default=False, description="Enable rate limiting")
    rate_limit_requests: int = Field(default=100, description="Max requests per minute")
    
    # Redis (for conversation history) - accept both uppercase and lowercase
    redis_url: Optional[str] = Field(
        default=None, 
        validation_alias=AliasChoices('redis_url', 'REDIS_URL'),
        description="Redis connection URL (optional)"
    )
    
    @property
    def cors_origins_list(self) -> list[str]:
        """Parse CORS origins from comma-separated string."""
        if self.cors_origins == "*":
            return ["*"]
        return [origin.strip() for origin in self.cors_origins.split(",")]
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False
        # Support both uppercase (Render) and lowercase env vars
        extra = "ignore"


@lru_cache
def get_settings() -> Settings:
    return Settings()


# Convenience access
settings = get_settings()
