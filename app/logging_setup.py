import logging
import os
from typing import Literal

import orjson


def setup_logging(level: str = None, log_format: Literal["text", "json"] = None) -> None:
    lvl = (level or os.getenv("LOG_LEVEL") or "INFO").upper()
    fmt = (log_format or os.getenv("LOG_FORMAT") or "text").lower()

    class JsonFormatter(logging.Formatter):
        def format(self, record: logging.LogRecord) -> str:
            payload = {
                "level": record.levelname,
                "logger": record.name,
                "message": record.getMessage(),
                "time": self.formatTime(record, "%Y-%m-%dT%H:%M:%S%z"),
            }
            if record.exc_info:
                payload["exc_info"] = self.formatException(record.exc_info)
            return orjson.dumps(payload).decode()

    handlers = [logging.StreamHandler()]
    logging.root.handlers.clear()
    logging.root.setLevel(lvl)

    if fmt == "json":
        handler = handlers[0]
        handler.setFormatter(JsonFormatter())
    else:
        logging.basicConfig(level=lvl, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
        return

    for h in handlers:
        logging.root.addHandler(h)

