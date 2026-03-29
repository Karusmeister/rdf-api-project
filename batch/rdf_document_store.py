"""Store discovered RDF documents into the krs_documents table.

Uses the same short-lived connection + retry pattern as other batch stores,
so multiple worker processes can write to the same DuckDB file.

Documents are inserted with ON CONFLICT DO NOTHING so re-runs are safe.
"""

import random
import time
from datetime import datetime, timezone

import duckdb

_MAX_LOCK_RETRIES = 10
_BASE_LOCK_DELAY = 0.05


class RdfDocumentStore:
    """Batch-insert discovered RDF documents into krs_documents."""

    def __init__(self, db_path: str):
        self._db_path = db_path
        self._ensure_table()

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

    def _ensure_table(self):
        def _do(conn):
            conn.execute("""
                CREATE TABLE IF NOT EXISTS krs_documents (
                    document_id         VARCHAR PRIMARY KEY,
                    krs                 VARCHAR(10) NOT NULL,
                    rodzaj              VARCHAR NOT NULL,
                    status              VARCHAR NOT NULL,
                    nazwa               VARCHAR,
                    okres_start         VARCHAR,
                    okres_end           VARCHAR,
                    filename            VARCHAR,
                    is_ifrs             BOOLEAN,
                    is_correction       BOOLEAN,
                    date_filed          VARCHAR,
                    date_prepared       VARCHAR,
                    is_downloaded       BOOLEAN DEFAULT false,
                    downloaded_at       TIMESTAMP,
                    storage_path        VARCHAR,
                    storage_backend     VARCHAR,
                    file_size_bytes     BIGINT,
                    zip_size_bytes      BIGINT,
                    file_count          INTEGER,
                    file_types          VARCHAR,
                    discovered_at       TIMESTAMP NOT NULL,
                    metadata_fetched_at TIMESTAMP,
                    download_error      VARCHAR
                )
            """)
        self._with_conn(_do)

    def insert_documents(self, krs: str, documents: list[dict]) -> int:
        """Insert discovered documents. Returns count of rows processed."""
        if not documents:
            return 0
        now = datetime.now(timezone.utc)

        def _do(conn):
            inserted = 0
            for doc in documents:
                conn.execute("""
                    INSERT INTO krs_documents
                        (document_id, krs, rodzaj, status, nazwa,
                         okres_start, okres_end, discovered_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT (document_id) DO NOTHING
                """, [
                    doc["id"],
                    krs,
                    doc["rodzaj"],
                    doc["status"],
                    doc.get("nazwa"),
                    doc.get("okresSprawozdawczyPoczatek"),
                    doc.get("okresSprawozdawczyKoniec"),
                    now,
                ])
                inserted += 1
            return inserted
        return self._with_conn(_do)

    def get_undownloaded(self, krs: str) -> list[str]:
        """Return document_ids for this KRS where is_downloaded = false."""
        def _do(conn):
            rows = conn.execute(
                "SELECT document_id FROM krs_documents "
                "WHERE krs = ? AND is_downloaded = false AND download_error IS NULL",
                [krs],
            ).fetchall()
            return [row[0] for row in rows]
        return self._with_conn(_do)

    def update_metadata(self, document_id: str, meta: dict) -> None:
        """Update extended metadata fields after fetching doc metadata."""
        now = datetime.now(timezone.utc)

        def _do(conn):
            conn.execute("""
                UPDATE krs_documents SET
                    filename            = ?,
                    is_ifrs             = ?,
                    is_correction       = ?,
                    date_filed          = ?,
                    date_prepared       = ?,
                    metadata_fetched_at = ?
                WHERE document_id = ?
            """, [
                meta.get("nazwaPliku"),
                meta.get("czyMSR"),
                meta.get("czyKorekta"),
                meta.get("dataDodania"),
                meta.get("dataSporządzenia"),
                now,
                document_id,
            ])
        self._with_conn(_do)

    def mark_downloaded(
        self,
        document_id: str,
        storage_path: str,
        storage_backend: str,
        file_size: int,
        zip_size: int,
        file_count: int,
        file_types: str,
    ) -> None:
        """Mark a document as successfully downloaded and extracted."""
        now = datetime.now(timezone.utc)

        def _do(conn):
            conn.execute("""
                UPDATE krs_documents SET
                    is_downloaded    = true,
                    downloaded_at    = ?,
                    storage_path     = ?,
                    storage_backend  = ?,
                    file_size_bytes  = ?,
                    zip_size_bytes   = ?,
                    file_count       = ?,
                    file_types       = ?,
                    download_error   = NULL
                WHERE document_id = ?
            """, [now, storage_path, storage_backend, file_size, zip_size,
                  file_count, file_types, document_id])
        self._with_conn(_do)

    def update_error(self, document_id: str, error: str) -> None:
        """Record a download error for a document."""
        def _do(conn):
            conn.execute(
                "UPDATE krs_documents SET download_error = ? WHERE document_id = ?",
                [error, document_id],
            )
        self._with_conn(_do)
