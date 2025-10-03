import asyncio
import logging
import random
import ssl
from typing import Union

import certifi
import orjson
import websockets
from websockets.asyncio.client import ClientConnection

from app.config import settings
from app.logging_setup import setup_logging
from app.models import StreamMessage, extract_all_outputs
from app.s3_writer import WindowedS3Writer
from app.file_writer import FileWindowedWriter


logger = logging.getLogger(__name__)


def _build_ws_url() -> str:
    """Build WebSocket URL with authentication token.
    
    Returns:
        Complete WebSocket URL with token query parameter
    """
    url = settings.ws_url
    token = settings.benzinga_api_key
    sep = "&" if ("?" in url) else "?"
    return f"{url}{sep}token={token}"


async def _consume_messages(
    conn: ClientConnection,
    writer: Union[WindowedS3Writer, FileWindowedWriter],
    summarizer,
) -> None:
    """Consume messages from WebSocket connection and write to sink.
    
    Args:
        conn: Active WebSocket connection
        writer: Output writer (S3 or file-based)
        summarizer: BedrockSummarizer instance for generating article summaries
    """
    async for raw in conn:
        try:
            payload = orjson.loads(raw)
        except Exception:
            logger.exception("decode-failed: invalid JSON from socket")
            continue
        try:
            msg = StreamMessage.model_validate(payload)
        except Exception:
            logger.exception("validation-failed: payload did not match expected schema")
            continue
        try:
            logger.debug("received message id=%s ts=%s", msg.data.id, msg.data.timestamp.isoformat())
        except Exception:
            pass
        
        # Extract all ticker records with summarization (one record per ticker)
        records = extract_all_outputs(msg, summarizer, settings.summary_max_words)
        if not records:
            try:
                logger.debug("ignored message id=%s reason=no-securities-or-ticker", msg.data.id)
            except Exception:
                logger.debug("ignored message reason=no-securities-or-ticker")
            continue
        
        # Write each ticker record
        for record in records:
            try:
                logger.debug("writing record news_id=%s ticker=%s", record.news_id, record.ticker)
                writer.write_line(record.to_ndjson())
            except Exception:
                logger.exception("write-failed news_id=%s ticker=%s", getattr(record, "news_id", None), getattr(record, "ticker", None))
                continue


async def run_stream(
    writer: Union[WindowedS3Writer, FileWindowedWriter],
    summarizer,
    stop: asyncio.Event
) -> None:
    """Run WebSocket stream with automatic reconnection.
    
    Args:
        writer: Output writer (S3 or file-based)
        summarizer: BedrockSummarizer instance for generating summaries
        stop: Event to signal graceful shutdown
    """
    backoff = settings.reconnect_base_delay
    while not stop.is_set():
        url = _build_ws_url()
        try:
            ssl_context = ssl.create_default_context(cafile=certifi.where())
            logger.info("connecting to benzinga ws with url=%s", url)
            async with websockets.connect(
                url,
                ssl=ssl_context,
                ping_interval=settings.ping_interval,
                ping_timeout=settings.ping_timeout,
                max_queue=1024
            ) as conn:
                logger.info("connected to benzinga ws")
                backoff = settings.reconnect_base_delay
                await _consume_messages(conn, writer, summarizer)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            jitter = random.uniform(0, backoff)
            delay = min(settings.reconnect_max_delay, backoff + jitter)
            logger.warning("ws-error error=%s reconnecting_in=%.1fs", exc, delay)
            try:
                await asyncio.wait_for(stop.wait(), timeout=delay)
            except asyncio.TimeoutError:
                pass
            backoff = min(settings.reconnect_max_delay, max(settings.reconnect_base_delay, backoff * 2))


async def main_async() -> None:
    """Main async entry point for the WebSocket client.
    
    Sets up logging, initializes Bedrock summarizer, writer (file or S3),
    and runs the WebSocket stream with graceful shutdown handling.
    """
    setup_logging(level=settings.log_level, log_format=settings.log_format)
    
    # Initialize Bedrock summarizer
    from app.bedrock_summarizer import BedrockSummarizer
    summarizer = BedrockSummarizer(
        region_name=settings.aws_region_name,
        model_id=settings.bedrock_model_id,
        max_retries=settings.bedrock_max_retries,
    )
    logger.info(
        "initialized bedrock summarizer model=%s region=%s max_words=%d",
        settings.bedrock_model_id,
        settings.aws_region_name or "default",
        settings.summary_max_words,
    )
    
    # Initialize writer based on sink configuration
    if (settings.sink or "file").lower() == "s3":
        if not settings.s3_bucket:
            raise ValueError("S3_BUCKET environment variable is required when SINK=s3")
        
        logger.info(
            "initializing S3 writer bucket=%s prefix=%s window_minutes=%d",
            settings.s3_bucket,
            settings.s3_prefix,
            settings.window_minutes,
        )
        writer = WindowedS3Writer(
            bucket=str(settings.s3_bucket),
            base_prefix=settings.s3_prefix,
            window_minutes=settings.window_minutes,
            max_object_bytes=settings.max_object_bytes,
            part_size_bytes=settings.part_size_bytes,
            aws_region_name=settings.aws_region_name,
            use_marker=settings.use_marker_files,
        )
    else:
        logger.info(
            "initializing file writer dir=%s window_minutes=%d",
            settings.file_dir,
            settings.window_minutes,
        )
        writer = FileWindowedWriter(
            base_dir=settings.file_dir,
            window_minutes=settings.window_minutes,
            max_object_bytes=settings.max_object_bytes,
            use_marker=settings.use_marker_files,
        )

    stop = asyncio.Event()

    loop = asyncio.get_running_loop()

    async def _graceful_shutdown(_: object = None) -> None:
        stop.set()

    for signame in ("SIGINT", "SIGTERM"):
        try:
            loop.add_signal_handler(getattr(__import__("signal"), signame), lambda: asyncio.create_task(_graceful_shutdown()))
        except Exception:
            pass

    try:
        await run_stream(writer, summarizer, stop)
    finally:
        writer.close()


if __name__ == "__main__":
    asyncio.run(main_async())
