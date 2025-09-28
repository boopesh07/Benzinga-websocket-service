import asyncio
import logging
import random
import ssl
from typing import Dict

import certifi
import orjson
import websockets
from websockets.client import WebSocketClientProtocol

from app.config import settings
from app.logging_setup import setup_logging
from app.models import StreamMessage, try_extract_output
from app.s3_writer import WindowedS3Writer
from app.file_writer import FileWindowedWriter


logger = logging.getLogger(__name__)


def _build_ws_url() -> str:
    url = settings.ws_url
    token = settings.benzinga_api_key
    sep = "&" if ("?" in url) else "?"
    return f"{url}{sep}token={token}"


async def _consume_messages(conn: WebSocketClientProtocol, writer) -> None:
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
        record = try_extract_output(msg)
        if record is None:
            try:
                logger.debug("ignored message id=%s reason=no-securities-or-ticker", msg.data.id)
            except Exception:
                logger.debug("ignored message reason=no-securities-or-ticker")
            continue
        try:
            logger.debug("writing record news_id=%s ticker=%s", record.news_id, record.ticker)
            writer.write_line(record.to_ndjson())
        except Exception:
            logger.exception("write-failed news_id=%s ticker=%s", getattr(record, "news_id", None), getattr(record, "ticker", None))
            continue


async def run_stream(writer, stop: asyncio.Event) -> None:
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
                await _consume_messages(conn, writer)
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
    setup_logging(level=settings.log_level, log_format=settings.log_format)
    if (settings.sink or "file").lower() == "s3":
        writer = WindowedS3Writer(
            bucket=str(settings.s3_bucket),
            base_prefix=settings.s3_prefix,
            window_minutes=30,
            max_object_bytes=512_000_000,
            part_size_bytes=16_777_216,
            aws_region_name=settings.aws_region_name,
            use_marker=True,
        )
    else:
        writer = FileWindowedWriter(
            base_dir=settings.file_dir,
            window_minutes=30,
            max_object_bytes=512_000_000,
            use_marker=True,
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
        await run_stream(writer, stop)
    finally:
        writer.close()


if __name__ == "__main__":
    asyncio.run(main_async())
