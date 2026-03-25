"""Centralized logging configuration.

Call ``configure_logging()`` once at app startup (before any loggers are used).
Produces structured JSON log lines on stdout suitable for ELK/Loki/CloudWatch.
"""

import json
import logging
import sys
from datetime import datetime, timezone

from app.config import settings


class JSONFormatter(logging.Formatter):
    """Emit one JSON object per log line."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict = {
            "ts": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }

        # Merge structured extras (skip internal logging keys)
        _SKIP = {
            "name", "msg", "args", "created", "relativeCreated",
            "thread", "threadName", "msecs", "filename", "funcName",
            "levelno", "lineno", "module", "exc_info", "exc_text",
            "stack_info", "pathname", "processName", "process",
            "message", "taskName", "levelname",
        }
        for key, value in record.__dict__.items():
            if key.startswith("_") or key in _SKIP:
                continue
            payload[key] = value

        if record.exc_info and record.exc_info[1] is not None:
            payload["exception"] = self.formatException(record.exc_info)

        return json.dumps(payload, default=str, ensure_ascii=False)


def configure_logging() -> None:
    """Set up root logger with JSON output to stdout."""
    log_level = getattr(settings, "log_level", "INFO").upper()

    root = logging.getLogger()
    root.setLevel(log_level)

    # Remove any pre-existing handlers (e.g. from basicConfig)
    for handler in root.handlers[:]:
        root.removeHandler(handler)

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JSONFormatter())
    root.addHandler(handler)

    # Quiet noisy third-party loggers
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)
