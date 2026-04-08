"""PostgreSQL-backed progress store for the batch KRS scanner.

The batch_progress table tracks which KRS numbers have been processed.
"""

import time

import psycopg2

from app.db.connection import make_connection


class ProgressStore:
    """Track which KRS numbers have been processed (found / not_found / error)."""

    def __init__(self, dsn: str, *, init_schema: bool = True):
        self._dsn = dsn
        self._conn = None
        if init_schema:
            self._init_schema()

    def _get_conn(self):
        """Return a cached connection, reconnecting if needed."""
        if self._conn is not None and not self._conn.closed:
            return self._conn
        self._conn = make_connection(self._dsn)
        return self._conn

    def _with_conn(self, fn):
        """Execute fn(conn) using a persistent connection with retry on failure."""
        for attempt in range(3):
            try:
                return fn(self._get_conn())
            except psycopg2.OperationalError:
                self._close_stale()
                if attempt == 2:
                    raise
                time.sleep(1.0 * (2 ** attempt))

    def _close_stale(self):
        """Close a potentially stale connection before reconnect."""
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception:
                pass
            self._conn = None

    def close(self):
        """Close the persistent connection."""
        if self._conn is not None and not self._conn.closed:
            try:
                self._conn.close()
            except Exception:
                pass
            self._conn = None

    def _init_schema(self):
        def _do(conn):
            conn.execute("""
                CREATE TABLE IF NOT EXISTS batch_progress (
                    krs          BIGINT PRIMARY KEY,
                    status       VARCHAR NOT NULL,
                    worker_id    INTEGER,
                    processed_at TIMESTAMP DEFAULT now()
                )
            """)
        self._with_conn(_do)

    def is_done(self, krs: int) -> bool:
        def _do(conn):
            row = conn.execute(
                "SELECT 1 FROM batch_progress WHERE krs = %s", [krs]
            ).fetchone()
            return row is not None
        return self._with_conn(_do)

    def mark(self, krs: int, status: str, worker_id: int):
        def _do(conn):
            conn.execute("""
                INSERT INTO batch_progress (krs, status, worker_id)
                VALUES (%s, %s, %s)
                ON CONFLICT (krs) DO UPDATE SET
                    status = excluded.status,
                    worker_id = excluded.worker_id,
                    processed_at = now()
            """, [krs, status, worker_id])
        self._with_conn(_do)

    def summary(self) -> dict:
        """Count by status — useful for monitoring."""
        def _do(conn):
            rows = conn.execute(
                "SELECT status, COUNT(*) FROM batch_progress GROUP BY status"
            ).fetchall()
            return {row[0]: row[1] for row in rows}
        return self._with_conn(_do)

    def save_cursor(self, next_krs: int) -> None:
        """Persist the next KRS number to probe so future runs can resume."""
        def _do(conn):
            conn.execute("""
                INSERT INTO krs_scan_cursor (id, next_krs_int)
                VALUES (TRUE, %s)
                ON CONFLICT (id) DO UPDATE SET next_krs_int = %s
            """, [next_krs, next_krs])
        self._with_conn(_do)

    def load_cursor(self) -> int | None:
        """Load the saved cursor position. Returns None if no cursor exists."""
        def _do(conn):
            row = conn.execute(
                "SELECT next_krs_int FROM krs_scan_cursor WHERE id = TRUE"
            ).fetchone()
            return row[0] if row else None
        return self._with_conn(_do)
