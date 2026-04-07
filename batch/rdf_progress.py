"""PostgreSQL-backed progress store for the batch RDF document discovery.

Tracks which KRS numbers have had their RDF documents fetched.
Uses a separate table (batch_rdf_progress) so it does not interfere
with the KRS entity scanner's batch_progress table.
"""

import psycopg2

from app.db.connection import make_connection


class RdfProgressStore:
    """Track which KRS numbers have had their RDF documents discovered."""

    def __init__(self, dsn: str):
        self._dsn = dsn
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
                "SELECT 1 FROM batch_rdf_progress WHERE krs = %s", [krs]
            ).fetchone()
            return row is not None
        return self._with_conn(_do)

    def mark(self, krs: str, status: str, documents_found: int, worker_id: int):
        def _do(conn):
            conn.execute("""
                INSERT INTO batch_rdf_progress (krs, status, documents_found, worker_id)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (krs) DO UPDATE SET
                    status = excluded.status,
                    documents_found = excluded.documents_found,
                    worker_id = excluded.worker_id,
                    processed_at = now()
            """, [krs, status, documents_found, worker_id])
        self._with_conn(_do)

    def get_pending_krs(
        self,
        worker_id: int,
        total_workers: int,
        legal_forms: list[str] | None = None,
    ) -> list[str]:
        """Fetch KRS numbers from batch_progress (status='found') that haven't
        been processed yet, partitioned by worker_id modulo total_workers.

        If *legal_forms* is provided, only KRS numbers whose entity has a
        matching legal_form in krs_entity_versions are returned.

        Returns zero-padded 10-char KRS strings.
        """
        def _do(conn):
            if legal_forms:
                rows = conn.execute("""
                    SELECT LPAD(CAST(bp.krs AS VARCHAR), 10, '0') AS krs_str
                    FROM batch_progress bp
                    JOIN krs_entity_versions ev
                        ON LPAD(CAST(bp.krs AS VARCHAR), 10, '0') = ev.krs
                        AND ev.is_current = true
                    LEFT JOIN batch_rdf_progress rp
                        ON LPAD(CAST(bp.krs AS VARCHAR), 10, '0') = rp.krs
                    WHERE bp.status = 'found'
                      AND rp.krs IS NULL
                      AND bp.krs %% %s = %s
                      AND ev.legal_form = ANY(%s)
                    ORDER BY bp.krs
                """, [total_workers, worker_id, legal_forms]).fetchall()
            else:
                rows = conn.execute("""
                    SELECT LPAD(CAST(bp.krs AS VARCHAR), 10, '0') AS krs_str
                    FROM batch_progress bp
                    LEFT JOIN batch_rdf_progress rp
                        ON LPAD(CAST(bp.krs AS VARCHAR), 10, '0') = rp.krs
                    WHERE bp.status = 'found'
                      AND rp.krs IS NULL
                      AND bp.krs %% %s = %s
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
                  AND CAST(rp.krs AS BIGINT) %% %s = %s
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
