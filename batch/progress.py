"""DuckDB-backed progress store for the batch KRS scanner.

The batch_progress table lives in the same DuckDB file as financial data,
keeping everything queryable in one place.

DuckDB uses an exclusive file lock per connection, so we use short-lived
connections (connect -> execute -> close) with retry-on-lock-contention
to support multiple worker processes writing to the same file.
"""

import random
import time

import duckdb

_MAX_LOCK_RETRIES = 20
_BASE_LOCK_DELAY = 0.05  # 50ms base, jittered


class ProgressStore:
    """Track which KRS numbers have been processed (found / not_found / error)."""

    def __init__(self, db_path: str, *, init_schema: bool = True):
        self._db_path = db_path
        if init_schema:
            self._init_schema()

    def _with_conn(self, fn):
        """Open a short-lived connection, call fn(conn), close, return result.

        Retries on DuckDB lock contention with jittered backoff.
        """
        for attempt in range(_MAX_LOCK_RETRIES):
            try:
                conn = duckdb.connect(self._db_path)
                try:
                    result = fn(conn)
                finally:
                    conn.close()
                return result
            except duckdb.IOException:
                if attempt == _MAX_LOCK_RETRIES - 1:
                    raise
                delay = min(_BASE_LOCK_DELAY * (2 ** attempt), 5.0) + random.uniform(0, 0.05)
                time.sleep(delay)

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
                "SELECT 1 FROM batch_progress WHERE krs = ?", [krs]
            ).fetchone()
            return row is not None
        return self._with_conn(_do)

    def mark(self, krs: int, status: str, worker_id: int):
        def _do(conn):
            conn.execute("""
                INSERT INTO batch_progress (krs, status, worker_id)
                VALUES (?, ?, ?)
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
