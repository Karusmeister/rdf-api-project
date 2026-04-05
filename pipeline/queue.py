"""Pipeline queue management.

`pipeline_queue` lives on the pipeline database. This module reads from the
scraper database (read-only) to discover changed documents and writes to the
pipeline_queue on the pipeline database.
"""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Optional

from app.db.connection import ConnectionWrapper

logger = logging.getLogger(__name__)


def enqueue_krs(
    pipeline_conn: ConnectionWrapper,
    krs: str,
    reason: str,
    document_id: str | None = None,
) -> None:
    """Idempotently add a KRS to the pipeline queue.

    A (krs, document_id) pair is unique. If the row already exists with
    status != 'pending' we still reset it to 'pending' so the next run picks
    it up (this is what you want for manual re-queue).
    """
    key = document_id or "__none__"
    pipeline_conn.execute(
        """
        INSERT INTO pipeline_queue (krs, document_key, trigger_reason, document_id,
                                    queued_at, status)
        VALUES (%s, %s, %s, %s, now(), 'pending')
        ON CONFLICT (krs, document_key) DO UPDATE SET
            trigger_reason = excluded.trigger_reason,
            queued_at = now(),
            status = 'pending',
            completed_at = NULL,
            error_message = NULL
        """,
        [krs, key, reason, document_id],
    )


def enqueue_changed_since(
    scraper_conn: ConnectionWrapper,
    pipeline_conn: ConnectionWrapper,
    since: datetime,
) -> int:
    """Discover documents created in the scraper DB since a cutoff and enqueue them.

    Reads `krs_document_versions` (append-only) from the scraper DB. Returns
    number of rows inserted/updated.
    """
    rows = scraper_conn.execute(
        """
        SELECT DISTINCT krs, document_id
        FROM krs_document_versions
        WHERE created_at >= %s
        """,
        [since],
    ).fetchall()

    count = 0
    for krs, document_id in rows:
        enqueue_krs(pipeline_conn, krs, reason="new_document", document_id=document_id)
        count += 1
    logger.info(
        "pipeline_queue_enqueued",
        extra={"event": "pipeline_queue_enqueued", "count": count,
               "since": since.isoformat()},
    )
    return count


def claim_pending(
    pipeline_conn: ConnectionWrapper,
    run_id: int,
    limit: int | None = None,
) -> list[dict]:
    """Atomically move pending items to 'processing' and return the claimed set."""
    limit_clause = f"LIMIT {int(limit)}" if limit else ""
    rows = pipeline_conn.execute(
        f"""
        WITH claimed AS (
            SELECT krs, document_key
            FROM pipeline_queue
            WHERE status = 'pending'
            ORDER BY queued_at
            {limit_clause}
            FOR UPDATE SKIP LOCKED
        )
        UPDATE pipeline_queue q
        SET status = 'processing',
            pipeline_run_id = %s
        FROM claimed c
        WHERE q.krs = c.krs AND q.document_key = c.document_key
        RETURNING q.krs, q.document_id, q.trigger_reason
        """,
        [run_id],
    ).fetchall()
    return [{"krs": r[0], "document_id": r[1], "trigger_reason": r[2]} for r in rows]


def mark_completed(pipeline_conn: ConnectionWrapper, run_id: int) -> int:
    row = pipeline_conn.execute(
        """
        UPDATE pipeline_queue
        SET status = 'completed', completed_at = now()
        WHERE pipeline_run_id = %s AND status = 'processing'
        RETURNING krs
        """,
        [run_id],
    ).fetchall()
    return len(row)


def mark_failed(
    pipeline_conn: ConnectionWrapper,
    krs: str,
    document_id: str | None,
    run_id: int,
    error: str,
) -> None:
    key = document_id or "__none__"
    pipeline_conn.execute(
        """
        UPDATE pipeline_queue
        SET status = 'failed',
            completed_at = now(),
            error_message = %s
        WHERE krs = %s AND document_key = %s AND pipeline_run_id = %s
        """,
        [error[:2000], krs, key, run_id],
    )


def get_queue_stats(pipeline_conn: ConnectionWrapper) -> dict:
    row = pipeline_conn.execute(
        """
        SELECT
            count(*) FILTER (WHERE status = 'pending')     AS pending,
            count(*) FILTER (WHERE status = 'processing')  AS processing,
            count(*) FILTER (WHERE status = 'completed')   AS completed,
            count(*) FILTER (WHERE status = 'failed')      AS failed,
            min(queued_at) FILTER (WHERE status = 'pending') AS oldest_pending
        FROM pipeline_queue
        """
    ).fetchone()
    return {
        "pending": int(row[0] or 0),
        "processing": int(row[1] or 0),
        "completed": int(row[2] or 0),
        "failed": int(row[3] or 0),
        "oldest_pending": str(row[4]) if row[4] else None,
    }
