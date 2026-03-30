"""PostgreSQL-backed progress store for the batch KRS scanner.

The batch_progress table tracks which KRS numbers have been processed.
"""

import psycopg2

from app.db.connection import make_connection


class ProgressStore:
    """Track which KRS numbers have been processed (found / not_found / error)."""

    def __init__(self, dsn: str, *, init_schema: bool = True):
        self._dsn = dsn
        if init_schema:
            self._init_schema()

    def _with_conn(self, fn):
        """Open a short-lived connection, call fn(conn), close, return result."""
        conn = make_connection(self._dsn)
        try:
            result = fn(conn)
        finally:
            conn.close()
        return result

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
