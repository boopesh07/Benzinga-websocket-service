import os
from typing import Optional

from dotenv import load_dotenv
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


# Load from a chosen env file (default .env). Example: ENV_FILE=.env.prod
load_dotenv(os.getenv("ENV_FILE", ".env"))


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=None, extra="ignore")

    benzinga_api_key: str = Field(alias="BENZINGA_API_KEY")
    ws_url: str = Field(default="wss://api.benzinga.com/api/v1/news/stream", alias="BENZINGA_WS_URL")

    # Sink selection
    sink: str = Field(default="file", alias="SINK")  # "file" or "s3"
    file_dir: str = Field(default="./data", alias="FILE_DIR")

    # S3 options
    s3_bucket: Optional[str] = Field(default=None, alias="S3_BUCKET")
    s3_prefix: str = Field(default="benzinga/news", alias="S3_PREFIX")
    aws_region_name: Optional[str] = Field(default=None, alias="AWS_REGION")

    # Rotation and flush (only used by legacy paths; windowed writers manage time/size internally)
    rotation_max_bytes: int = Field(default=50_000_000, alias="ROTATION_MAX_BYTES")
    rotation_max_seconds: int = Field(default=300, alias="ROTATION_MAX_SECONDS")
    flush_interval_seconds: int = Field(default=5, alias="FLUSH_INTERVAL_SECONDS")

    # WebSocket heartbeat
    ping_interval: int = Field(default=30, alias="PING_INTERVAL")
    ping_timeout: int = Field(default=10, alias="PING_TIMEOUT")

    # Reconnect backoff
    reconnect_base_delay: float = Field(default=1.0, alias="RECONNECT_BASE_DELAY")
    reconnect_max_delay: float = Field(default=60.0, alias="RECONNECT_MAX_DELAY")

    # Logging
    log_level: str = Field(default="INFO", alias="LOG_LEVEL")
    log_format: str = Field(default="text", alias="LOG_FORMAT")  # text|json


settings = Settings()
