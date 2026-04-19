"""Store discovered RDF documents in krs_documents + krs_document_downloads.

Post SCHEMA_DEDUPE_PLAN #1 the document layout is two sibling tables with
no version history. Discovery writes an immutable row to ``krs_documents``
and a paired empty row to ``krs_document_downloads``; metadata, download,
and error updates target the downloads row.

Uses short-lived persistent connections via ``make_connection`` so multiple
worker processes can write concurrently.
"""

import logging
import time
from datetime import datetime, timezone

import psycopg2

from app.db.connection import make_connection

logger = logging.getLogger(__name__)


def _rodzaj_to_smallint(value) -> int | None:
    if value is None or value == "":
        return None
    return int(value)


def _file_type_from_types(file_types: str) -> str:
    parts = file_types.split(",") if file_types else []
    if "xml" in parts:
        return "xml"
    if "pdf" in parts:
        return "pdf"
    if file_types:
        return "other"
    return "unknown"


class RdfDocumentStore:
    """Batch-insert discovered RDF documents against the split schema."""

    def __init__(self, dsn: str, *, init_schema: bool = True):
        self._dsn = dsn
        self._conn = None
        # The authoritative tables (krs_documents, krs_document_downloads)
        # live in the dedupe/006 migration. Batch workers do not bootstrap
        # schema on their own — they must run after the API has applied
        # migrations at least once.
        _ = init_schema

    def _get_conn(self):
        if self._conn is not None and not self._conn.closed:
            return self._conn
        self._conn = make_connection(self._dsn)
        return self._conn

    def _with_conn(self, fn):
        """Execute fn(conn) with one retry on OperationalError."""
        last_err = None
        for attempt in range(4):
            if attempt > 0:
                self._close_stale()
                time.sleep(1.0 * (2 ** (attempt - 1)))
            try:
                return fn(self._get_conn())
            except psycopg2.OperationalError as exc:
                last_err = exc
                self._close_stale()
                logger.warning("db_retry attempt=%d/%d error=%s", attempt + 1, 4, exc)
        raise last_err

    def _close_stale(self):
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception:
                pass
            self._conn = None

    def close(self):
        if self._conn is not None and not self._conn.closed:
            try:
                self._conn.close()
            except Exception:
                pass
            self._conn = None

    # ------------------------------------------------------------------
    # Public API (signatures unchanged for caller compatibility)
    # ------------------------------------------------------------------

    def insert_documents(self, krs: str, documents: list[dict]) -> int:
        """Insert discovered documents. Returns count of rows processed."""
        if not documents:
            return 0
        now = datetime.now(timezone.utc)

        def _do(conn):
            inserted = 0
            for doc in documents:
                doc_id = doc["id"]
                rodzaj_int = _rodzaj_to_smallint(doc["rodzaj"])
                if rodzaj_int is None:
                    raise ValueError(
                        f"insert_documents: rodzaj required, got {doc['rodzaj']!r}"
                    )
                is_deleted = (doc.get("status") == "USUNIETY")

                conn.execute(
                    """
                    INSERT INTO krs_documents (
                        document_id, krs, rodzaj, nazwa, okres_start, okres_end,
                        is_deleted, discovered_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (document_id) DO NOTHING
                    """,
                    [
                        doc_id, krs, rodzaj_int, doc.get("nazwa"),
                        doc.get("okresSprawozdawczyPoczatek"),
                        doc.get("okresSprawozdawczyKoniec"),
                        is_deleted, now,
                    ],
                )
                conn.execute(
                    """
                    INSERT INTO krs_document_downloads (document_id, is_downloaded)
                    VALUES (%s, false)
                    ON CONFLICT (document_id) DO NOTHING
                    """,
                    [doc_id],
                )
                inserted += 1
            return inserted
        return self._with_conn(_do)

    def get_undownloaded(self, krs: str) -> list[str]:
        """Return document_ids for this KRS where is_downloaded = false."""
        def _do(conn):
            rows = conn.execute(
                """
                SELECT d.document_id
                FROM krs_documents d
                LEFT JOIN krs_document_downloads dl USING (document_id)
                WHERE d.krs = %s
                  AND (dl.is_downloaded IS FALSE OR dl.is_downloaded IS NULL)
                  AND dl.download_error IS NULL
                """,
                [krs],
            ).fetchall()
            return [row[0] for row in rows]
        return self._with_conn(_do)

    def _document_exists(self, conn, document_id: str) -> bool:
        return conn.execute(
            "SELECT 1 FROM krs_documents WHERE document_id = %s", [document_id]
        ).fetchone() is not None

    def update_metadata(self, document_id: str, meta: dict) -> None:
        """Update metadata fields after fetching doc metadata."""
        now = datetime.now(timezone.utc)

        def _do(conn):
            if not self._document_exists(conn, document_id):
                raise ValueError(f"Document {document_id} not found in krs_documents")
            conn.execute(
                """
                UPDATE krs_documents SET
                    filename      = COALESCE(%s, filename),
                    is_ifrs       = COALESCE(%s, is_ifrs),
                    is_correction = COALESCE(%s, is_correction),
                    date_filed    = COALESCE(NULLIF(%s, '')::date, date_filed)
                WHERE document_id = %s
                """,
                [
                    meta.get("nazwaPliku"),
                    meta.get("czyMSR"),
                    meta.get("czyKorekta"),
                    meta.get("dataDodania"),
                    document_id,
                ],
            )
            conn.execute(
                """
                UPDATE krs_document_downloads SET
                    metadata_fetched_at = %s,
                    updated_at          = %s
                WHERE document_id = %s
                """,
                [now, now, document_id],
            )
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
        del storage_backend, zip_size  # dropped by the dedupe plan
        now = datetime.now(timezone.utc)

        def _do(conn):
            if not self._document_exists(conn, document_id):
                raise ValueError(f"Document {document_id} not found in krs_documents")
            conn.execute(
                """
                UPDATE krs_document_downloads SET
                    is_downloaded   = true,
                    downloaded_at   = %s,
                    storage_path    = %s,
                    file_size_bytes = %s,
                    file_count      = %s,
                    file_type       = %s,
                    download_error  = NULL,
                    updated_at      = %s
                WHERE document_id = %s
                """,
                [
                    now, storage_path, file_size, file_count,
                    _file_type_from_types(file_types), now, document_id,
                ],
            )
        self._with_conn(_do)

    def update_error(self, document_id: str, error: str) -> None:
        """Record a download error for a document."""
        now = datetime.now(timezone.utc)

        def _do(conn):
            if not self._document_exists(conn, document_id):
                raise ValueError(f"Document {document_id} not found in krs_documents")
            conn.execute(
                """
                UPDATE krs_document_downloads SET
                    download_error = %s,
                    updated_at     = %s
                WHERE document_id = %s
                """,
                [error, now, document_id],
            )
        self._with_conn(_do)
