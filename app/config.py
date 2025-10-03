import os
from typing import Optional

from dotenv import load_dotenv
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


# Load from a chosen env file (default .env). Example: ENV_FILE=.env.prod
load_dotenv(os.getenv("ENV_FILE", ".env"))


class Settings(BaseSettings):
    """Application configuration settings.
    
    All settings can be overridden via environment variables.
    Load from .env file by default, or specify ENV_FILE=.env.prod to use a different file.
    """
    model_config = SettingsConfigDict(env_file=None, extra="ignore")

    # API Configuration
    benzinga_api_key: str = Field(
        alias="BENZINGA_API_KEY",
        description="Benzinga API key (required)"
    )
    ws_url: str = Field(
        default="wss://api.benzinga.com/api/v1/news/stream",
        alias="BENZINGA_WS_URL",
        description="Benzinga WebSocket URL"
    )

    # Sink selection
    sink: str = Field(
        default="file",
        alias="SINK",
        description="Output sink type: 'file' or 's3'"
    )
    file_dir: str = Field(
        default="./data",
        alias="FILE_DIR",
        description="Base directory for file sink output"
    )

    # S3 options
    s3_bucket: Optional[str] = Field(
        default=None,
        alias="S3_BUCKET",
        description="S3 bucket name (required when sink=s3)"
    )
    s3_prefix: str = Field(
        default="benzinga/news",
        alias="S3_PREFIX",
        description="S3 key prefix for output objects"
    )
    aws_region_name: Optional[str] = Field(
        default=None,
        alias="AWS_REGION",
        description="AWS region (optional, uses default credential chain if not set)"
    )

    # Windowed writer settings
    window_minutes: int = Field(
        default=30,
        alias="WINDOW_MINUTES",
        description="Time window size in minutes (typically 15, 30, or 60)"
    )
    max_object_bytes: int = Field(
        default=512_000_000,
        alias="MAX_OBJECT_BYTES",
        description="Maximum size per output file/object before rotation (default 512MB)"
    )
    part_size_bytes: int = Field(
        default=16_777_216,
        alias="PART_SIZE_BYTES",
        description="S3 multipart upload part size (default 16MB, min 5MB)"
    )
    use_marker_files: bool = Field(
        default=True,
        alias="USE_MARKER_FILES",
        description="Create uploading.marker files to track in-progress uploads"
    )

    # AWS Bedrock (for article summarization)
    bedrock_model_id: str = Field(
        default="us.anthropic.claude-3-5-sonnet-20241022-v2:0",
        alias="BEDROCK_MODEL_ID",
        description="Claude model ID for summarization (inference profile for on-demand access)"
    )
    bedrock_fallback_model_id: str = Field(
        default="anthropic.claude-3-haiku-20240307-v1:0",
        alias="BEDROCK_FALLBACK_MODEL_ID",
        description="Fallback Claude model ID if primary model fails"
    )
    summary_max_words: int = Field(
        default=200,
        alias="SUMMARY_MAX_WORDS",
        description="Maximum words in article summary (shorter is fine)"
    )
    bedrock_max_retries: int = Field(
        default=3,
        alias="BEDROCK_MAX_RETRIES",
        description="Maximum retry attempts for Bedrock API calls"
    )

    # WebSocket heartbeat
    ping_interval: int = Field(
        default=30,
        alias="PING_INTERVAL",
        description="WebSocket ping interval in seconds"
    )
    ping_timeout: int = Field(
        default=10,
        alias="PING_TIMEOUT",
        description="WebSocket ping timeout in seconds"
    )

    # Reconnect backoff
    reconnect_base_delay: float = Field(
        default=1.0,
        alias="RECONNECT_BASE_DELAY",
        description="Initial reconnection delay in seconds"
    )
    reconnect_max_delay: float = Field(
        default=60.0,
        alias="RECONNECT_MAX_DELAY",
        description="Maximum reconnection delay in seconds"
    )

    # Logging
    log_level: str = Field(
        default="INFO",
        alias="LOG_LEVEL",
        description="Logging level: DEBUG, INFO, WARNING, ERROR, CRITICAL"
    )
    log_format: str = Field(
        default="text",
        alias="LOG_FORMAT",
        description="Log format: 'text' (human-readable) or 'json' (structured)"
    )


settings = Settings()
