"""Store discovered RDF documents using append-only versioning.

Uses short-lived PostgreSQL connections via make_connection,
so multiple worker processes can write concurrently.

Each document change (discovery, metadata, download, error) creates a new
version row in ``krs_document_versions``. The legacy ``krs_documents`` table
is still populated for backward compatibility.
"""

import hashlib
import json
import logging
import time
from datetime import datetime, timezone

import psycopg2

from app.db.connection import make_connection

logger = logging.getLogger(__name__)

_DOC_SNAPSHOT_FIELDS = (
    "rodzaj", "status", "nazwa", "okres_start", "okres_end",
    "filename", "is_ifrs", "is_correction", "date_filed", "date_prepared",
    "is_downloaded", "storage_path", "storage_backend",
    "file_size_bytes", "zip_size_bytes", "file_count", "file_types",
    "download_error",
)


def _doc_snapshot_hash(snapshot: dict) -> str:
    canonical = json.dumps(snapshot, sort_keys=True, ensure_ascii=False, default=str)
    return hashlib.md5(canonical.encode()).hexdigest()


class RdfDocumentStore:
    """Batch-insert discovered RDF documents with append-only versioning."""

    def __init__(self, dsn: str, *, init_schema: bool = True):
        self._dsn = dsn
        self._conn = None
        if init_schema:
            self._ensure_table()

    def _get_conn(self):
        """Return a cached connection, reconnecting if needed."""
        if self._conn is not None and not self._conn.closed:
            return self._conn
        self._conn = make_connection(self._dsn)
        return self._conn

    def _with_conn(self, fn):
        """Execute fn(conn) using a persistent connection with retry on failure."""
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

    def _ensure_table(self):
        def _do(conn):
            # DB-003: Legacy krs_documents table removed.
            conn.execute("CREATE SEQUENCE IF NOT EXISTS seq_krs_document_versions START 1")
            conn.execute("""
                CREATE TABLE IF NOT EXISTS krs_document_versions (
                    version_id           BIGINT PRIMARY KEY DEFAULT nextval('seq_krs_document_versions'),
                    document_id          VARCHAR NOT NULL,
                    version_no           INTEGER NOT NULL,
                    krs                  VARCHAR(10) NOT NULL,
                    rodzaj               VARCHAR NOT NULL,
                    status               VARCHAR NOT NULL,
                    nazwa                VARCHAR,
                    okres_start          VARCHAR,
                    okres_end            VARCHAR,
                    filename             VARCHAR,
                    is_ifrs              BOOLEAN,
                    is_correction        BOOLEAN,
                    date_filed           VARCHAR,
                    date_prepared        VARCHAR,
                    is_downloaded        BOOLEAN,
                    downloaded_at        TIMESTAMP,
                    storage_path         VARCHAR,
                    storage_backend      VARCHAR,
                    file_size_bytes      BIGINT,
                    zip_size_bytes       BIGINT,
                    file_count           INTEGER,
                    file_types           VARCHAR,
                    discovered_at        TIMESTAMP,
                    metadata_fetched_at  TIMESTAMP,
                    download_error       VARCHAR,
                    valid_from           TIMESTAMP NOT NULL,
                    valid_to             TIMESTAMP,
                    is_current           BOOLEAN NOT NULL DEFAULT true,
                    snapshot_hash        VARCHAR NOT NULL,
                    change_reason        VARCHAR,
                    run_id               VARCHAR,
                    observed_at          TIMESTAMP NOT NULL DEFAULT current_timestamp,
                    UNIQUE(document_id, version_no)
                )
            """)
        self._with_conn(_do)

    # ------------------------------------------------------------------
    # Private: append-only version helpers
    # ------------------------------------------------------------------

    def _get_current(self, conn, document_id: str) -> dict | None:
        row = conn.execute(
            "SELECT * FROM krs_document_versions WHERE document_id = %s AND is_current = true "
            "ORDER BY version_no DESC, version_id DESC LIMIT 1",
            [document_id],
        ).fetchone()
        if row is None:
            return None
        cols = [d[0] for d in conn.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = 'krs_document_versions' AND table_schema = 'public' "
            "ORDER BY ordinal_position"
        ).fetchall()]
        return dict(zip(cols, row))

    def _merge(self, current: dict | None, patch: dict) -> dict:
        if current is None:
            return {f: patch.get(f) for f in _DOC_SNAPSHOT_FIELDS}
        merged = {}
        for f in _DOC_SNAPSHOT_FIELDS:
            if f in patch:
                merged[f] = patch[f]  # None is a valid explicit value
            else:
                merged[f] = current.get(f)
        return merged

    def _append_if_changed(
        self, conn, document_id: str, krs: str, patch: dict,
        *, change_reason: str | None = None,
    ) -> bool:
        now = datetime.now(timezone.utc).isoformat()
        conn.execute("BEGIN")
        try:
            current = self._get_current(conn, document_id)
            merged = self._merge(current, patch)
            new_hash = _doc_snapshot_hash(merged)

            if current is not None and current.get("snapshot_hash") == new_hash:
                conn.execute(
                    "UPDATE krs_document_versions SET observed_at = %s WHERE version_id = %s",
                    [now, current["version_id"]],
                )
                conn.execute("COMMIT")
                return False

            next_vno = 1
            if current is not None:
                conn.execute(
                    "UPDATE krs_document_versions SET valid_to = %s, is_current = false WHERE version_id = %s AND is_current = true",
                    [now, current["version_id"]],
                )
                next_vno = current["version_no"] + 1

            discovered_at = patch["discovered_at"] if "discovered_at" in patch else (current["discovered_at"] if current else now)
            metadata_fetched_at = patch["metadata_fetched_at"] if "metadata_fetched_at" in patch else (current["metadata_fetched_at"] if current else None)
            downloaded_at = patch["downloaded_at"] if "downloaded_at" in patch else (current["downloaded_at"] if current else None)

            conn.execute("""
                INSERT INTO krs_document_versions (
                    document_id, version_no, krs,
                    rodzaj, status, nazwa, okres_start, okres_end,
                    filename, is_ifrs, is_correction, date_filed, date_prepared,
                    is_downloaded, downloaded_at, storage_path, storage_backend,
                    file_size_bytes, zip_size_bytes, file_count, file_types,
                    discovered_at, metadata_fetched_at, download_error,
                    valid_from, is_current, snapshot_hash, change_reason, observed_at
                ) VALUES (
                    %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s, %s,
                    %s, true, %s, %s, %s
                )
            """, [
                document_id, next_vno, krs,
                merged["rodzaj"], merged["status"], merged["nazwa"],
                merged.get("okres_start"), merged.get("okres_end"),
                merged["filename"], merged["is_ifrs"], merged["is_correction"],
                merged["date_filed"], merged["date_prepared"],
                merged["is_downloaded"], downloaded_at,
                merged["storage_path"], merged["storage_backend"],
                merged["file_size_bytes"], merged["zip_size_bytes"],
                merged["file_count"], merged["file_types"],
                discovered_at, metadata_fetched_at, merged["download_error"],
                now, new_hash, change_reason, now,
            ])
            conn.execute("COMMIT")
            return True
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except Exception as rollback_err:
                logger.error("ROLLBACK failed: %s", rollback_err)
            raise

    # ------------------------------------------------------------------
    # Public API (unchanged signatures)
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

                self._append_if_changed(
                    conn, doc_id, krs,
                    patch={
                        "rodzaj": doc["rodzaj"],
                        "status": doc["status"],
                        "nazwa": doc.get("nazwa"),
                        "okres_start": doc.get("okresSprawozdawczyPoczatek"),
                        "okres_end": doc.get("okresSprawozdawczyKoniec"),
                        "is_downloaded": False,
                        "discovered_at": now.isoformat(),
                    },
                    change_reason="discovery",
                )

                # DB-003: Legacy krs_documents write removed.
                inserted += 1
            return inserted
        return self._with_conn(_do)

    def get_undownloaded(self, krs: str) -> list[str]:
        """Return document_ids for this KRS where is_downloaded = false."""
        def _do(conn):
            # Read from version table (current) since view may not exist in batch context
            rows = conn.execute(
                """SELECT document_id FROM krs_document_versions
                   WHERE krs = %s AND is_current = true
                     AND (is_downloaded = false OR is_downloaded IS NULL)
                     AND download_error IS NULL""",
                [krs],
            ).fetchall()
            return [row[0] for row in rows]
        return self._with_conn(_do)

    def _resolve_krs(self, conn, document_id: str) -> str:
        """Get KRS from current version. Raises if not found."""
        current = self._get_current(conn, document_id)
        if current is not None:
            return current["krs"]
        # DB-003: Legacy krs_documents fallback removed.
        raise ValueError(f"Document {document_id} has no version history")

    def update_metadata(self, document_id: str, meta: dict) -> None:
        """Update extended metadata fields after fetching doc metadata."""
        now = datetime.now(timezone.utc)

        def _do(conn):
            krs = self._resolve_krs(conn, document_id)

            self._append_if_changed(
                conn, document_id, krs,
                patch={
                    "filename": meta.get("nazwaPliku"),
                    "is_ifrs": meta.get("czyMSR"),
                    "is_correction": meta.get("czyKorekta"),
                    "date_filed": meta.get("dataDodania"),
                    "date_prepared": meta.get("dataSporządzenia"),
                    "metadata_fetched_at": now.isoformat(),
                },
                change_reason="metadata_update",
            )

            # DB-003: Legacy krs_documents write removed.
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
            krs = self._resolve_krs(conn, document_id)

            self._append_if_changed(
                conn, document_id, krs,
                patch={
                    "is_downloaded": True,
                    "downloaded_at": now.isoformat(),
                    "storage_path": storage_path,
                    "storage_backend": storage_backend,
                    "file_size_bytes": file_size,
                    "zip_size_bytes": zip_size,
                    "file_count": file_count,
                    "file_types": file_types,
                    "download_error": None,
                },
                change_reason="downloaded",
            )

            # DB-003: Legacy krs_documents write removed.
        self._with_conn(_do)

    def update_error(self, document_id: str, error: str) -> None:
        """Record a download error for a document."""
        def _do(conn):
            krs = self._resolve_krs(conn, document_id)

            self._append_if_changed(
                conn, document_id, krs,
                patch={"download_error": error},
                change_reason="download_error",
            )

            # DB-003: Legacy krs_documents write removed.
        self._with_conn(_do)
