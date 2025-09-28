"""Public API for the Benzinga WebSocket sink package.

Re-exports key utilities for convenient importing.
"""

from .config import settings, Settings
from .logging_setup import setup_logging
from .models import (
    StreamMessage,
    DataMessage,
    Content,
    Security,
    OutputRecord,
    try_extract_output,
)
from .file_writer import FileWindowedWriter
from .s3_writer import WindowedS3Writer
from .ws_client import main_async

__all__ = [
    "settings",
    "Settings",
    "setup_logging",
    "StreamMessage",
    "DataMessage",
    "Content",
    "Security",
    "OutputRecord",
    "try_extract_output",
    "FileWindowedWriter",
    "WindowedS3Writer",
    "main_async",
]
