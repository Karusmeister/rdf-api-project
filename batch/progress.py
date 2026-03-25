"""DuckDB-backed progress store for the batch KRS scanner.

The batch_progress table lives in the same DuckDB file as financial data,
keeping everything queryable in one place. DuckDB serializes writes internally —
at ~1 write/second from 3 workers the overhead is negligible.
"""

import threading

import duckdb


class ProgressStore:
    """Track which KRS numbers have been processed (found / not_found / error)."""

    def __init__(self, db_path: str):
        self._db_path = db_path
        self._local = threading.local()
        self._init_schema()

    def _conn(self) -> duckdb.DuckDBPyConnection:
        if not hasattr(self._local, "conn"):
            self._local.conn = duckdb.connect(self._db_path)
        return self._local.conn

    def _init_schema(self):
        self._conn().execute("""
            CREATE TABLE IF NOT EXISTS batch_progress (
                krs          BIGINT PRIMARY KEY,
                status       VARCHAR NOT NULL,
                worker_id    INTEGER,
                processed_at TIMESTAMP DEFAULT now()
            )
        """)

    def is_done(self, krs: int) -> bool:
        row = self._conn().execute(
            "SELECT 1 FROM batch_progress WHERE krs = ?", [krs]
        ).fetchone()
        return row is not None

    def mark(self, krs: int, status: str, worker_id: int):
        self._conn().execute("""
            INSERT INTO batch_progress (krs, status, worker_id)
            VALUES (?, ?, ?)
            ON CONFLICT (krs) DO UPDATE SET
                status = excluded.status,
                worker_id = excluded.worker_id,
                processed_at = now()
        """, [krs, status, worker_id])

    def summary(self) -> dict:
        """Count by status — useful for monitoring."""
        rows = self._conn().execute(
            "SELECT status, COUNT(*) FROM batch_progress GROUP BY status"
        ).fetchall()
        return {row[0]: row[1] for row in rows}
