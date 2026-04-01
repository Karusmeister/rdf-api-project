"""Fire-and-forget activity logging service."""

from __future__ import annotations

import json
import logging
from typing import Any

from app.config import settings
from app.db.connection import get_db

logger = logging.getLogger(__name__)


class ActivityLogger:
    """Append-only activity log writer.

    Uses ``get_db()`` context manager so each write borrows its own pooled
    connection (or falls back to the shared connection in scripts/tests).
    This is safe for background tasks that run after the request-scoped
    connection has been released.

    All exceptions are caught and logged — callers are never impacted.
    Disabled entirely when ``settings.activity_logging_enabled`` is False.
    """

    def log(
        self,
        user_id: str | None,
        action: str,
        krs_number: str | None = None,
        detail: dict[str, Any] | None = None,
        ip_address: str | None = None,
    ) -> None:
        if not settings.activity_logging_enabled:
            return
        try:
            with get_db() as conn:
                conn.execute(
                    """
                    INSERT INTO activity_log (user_id, action, krs_number, detail, ip_address)
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    (
                        user_id,
                        action,
                        krs_number,
                        json.dumps(detail) if detail is not None else None,
                        ip_address,
                    ),
                )
        except Exception:
            logger.exception("activity_log_write_failed")


activity_logger = ActivityLogger()
