"""DuckDB-backed progress store for the batch RDF document discovery.

Tracks which KRS numbers have had their RDF documents fetched.
Uses a separate table (batch_rdf_progress) so it does not interfere
with the KRS entity scanner's batch_progress table.

Same short-lived connection + retry pattern as batch/progress.py.
"""

import random
import time

import duckdb

_MAX_LOCK_RETRIES = 10
_BASE_LOCK_DELAY = 0.05


class RdfProgressStore:
    """Track which KRS numbers have had their RDF documents discovered."""

    def __init__(self, db_path: str):
        self._db_path = db_path
        self._init_schema()

    def _with_conn(self, fn):
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
                delay = _BASE_LOCK_DELAY * (2 ** attempt) + random.uniform(0, 0.05)
                time.sleep(delay)

    def _init_schema(self):
        def _do(conn):
            conn.execute("""
                CREATE TABLE IF NOT EXISTS batch_rdf_progress (
                    krs             VARCHAR(10) PRIMARY KEY,
                    status          VARCHAR NOT NULL,
                    documents_found INTEGER DEFAULT 0,
                    worker_id       INTEGER,
                    processed_at    TIMESTAMP DEFAULT now()
                )
            """)
        self._with_conn(_do)

    def is_done(self, krs: str) -> bool:
        def _do(conn):
            row = conn.execute(
                "SELECT 1 FROM batch_rdf_progress WHERE krs = ?", [krs]
            ).fetchone()
            return row is not None
        return self._with_conn(_do)

    def mark(self, krs: str, status: str, documents_found: int, worker_id: int):
        def _do(conn):
            conn.execute("""
                INSERT INTO batch_rdf_progress (krs, status, documents_found, worker_id)
                VALUES (?, ?, ?, ?)
                ON CONFLICT (krs) DO UPDATE SET
                    status = excluded.status,
                    documents_found = excluded.documents_found,
                    worker_id = excluded.worker_id,
                    processed_at = now()
            """, [krs, status, documents_found, worker_id])
        self._with_conn(_do)

    def get_pending_krs(self, worker_id: int, total_workers: int) -> list[str]:
        """Fetch KRS numbers from batch_progress (status='found') that haven't
        been processed yet, partitioned by worker_id modulo total_workers.

        Returns zero-padded 10-char KRS strings.
        """
        def _do(conn):
            rows = conn.execute("""
                SELECT LPAD(CAST(bp.krs AS VARCHAR), 10, '0') AS krs_str
                FROM batch_progress bp
                LEFT JOIN batch_rdf_progress rp
                    ON LPAD(CAST(bp.krs AS VARCHAR), 10, '0') = rp.krs
                WHERE bp.status = 'found'
                  AND rp.krs IS NULL
                  AND bp.krs % ? = ?
                ORDER BY bp.krs
            """, [total_workers, worker_id]).fetchall()
            return [row[0] for row in rows]
        return self._with_conn(_do)

    def get_needs_download_krs(self, worker_id: int, total_workers: int) -> list[str]:
        """Fetch KRS numbers that were discovered (in batch_rdf_progress) but
        still have undownloaded documents in krs_documents.

        Returns zero-padded 10-char KRS strings.
        """
        def _do(conn):
            rows = conn.execute("""
                SELECT DISTINCT rp.krs
                FROM batch_rdf_progress rp
                JOIN krs_document_versions kd ON kd.krs = rp.krs AND kd.is_current = true
                WHERE rp.status IN ('done', 'partial')
                  AND (kd.is_downloaded = false OR kd.is_downloaded IS NULL)
                  AND kd.download_error IS NULL
                  AND CAST(rp.krs AS BIGINT) % ? = ?
                ORDER BY rp.krs
            """, [total_workers, worker_id]).fetchall()
            return [row[0] for row in rows]
        return self._with_conn(_do)

    def summary(self) -> dict:
        """Count by status — useful for monitoring."""
        def _do(conn):
            rows = conn.execute(
                "SELECT status, COUNT(*), COALESCE(SUM(documents_found), 0) "
                "FROM batch_rdf_progress GROUP BY status"
            ).fetchall()
            return {
                row[0]: {"count": row[1], "documents": row[2]}
                for row in rows
            }
        return self._with_conn(_do)
