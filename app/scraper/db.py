"""Scraper control-plane database.

Post SCHEMA_DEDUPE_PLAN #1+#3 the document layout is two sibling tables:

- ``krs_documents``         — immutable discovery record (1 row per doc ever)
- ``krs_document_downloads`` — mutable download state (1 row per doc ever)

and the historical ``krs_document_versions`` is gone. Readers continue to
use the ``krs_documents_current`` view, which is now a plain LEFT JOIN over
the two tables and is created by dedupe/007.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

import logging

from app.config import settings
from app.db import connection as shared_conn

logger = logging.getLogger(__name__)

_schema_initialized = False


def connect() -> None:
    """Ensure shared connection is open and scraper schema exists."""
    shared_conn.connect()
    _ensure_schema()


def close() -> None:
    """No-op. Connection lifecycle is managed by app.db.connection."""
    pass


def get_conn():
    """Return the shared database connection."""
    return shared_conn.get_conn()


def _ensure_schema() -> None:
    global _schema_initialized
    if _schema_initialized:
        return
    _init_schema()
    _schema_initialized = True


def _init_schema() -> None:
    """Create bootstrap tables if they don't exist. Idempotent.

    The authoritative document tables (``krs_documents``,
    ``krs_document_downloads``) and the ``krs_documents_current`` view come
    from the dedupe/006 and dedupe/007 migrations, not this bootstrap —
    same pattern as krs_companies.
    """
    conn = get_conn()

    conn.execute("""
        CREATE TABLE IF NOT EXISTS scraper_runs (
            run_id              VARCHAR PRIMARY KEY,
            started_at          TIMESTAMP NOT NULL,
            finished_at         TIMESTAMP,
            status              VARCHAR DEFAULT 'running',

            mode                VARCHAR NOT NULL,
            krs_checked         INTEGER DEFAULT 0,
            krs_new_found       INTEGER DEFAULT 0,
            documents_discovered INTEGER DEFAULT 0,
            documents_downloaded INTEGER DEFAULT 0,
            documents_failed    INTEGER DEFAULT 0,
            bytes_downloaded    BIGINT DEFAULT 0,

            config_snapshot     VARCHAR,
            error_message       VARCHAR
        )
    """)

    existing = {
        row[0]
        for row in conn.execute(
            "SELECT indexname FROM pg_indexes WHERE schemaname = 'public'"
        ).fetchall()
    }
    if "idx_runs_started" not in existing:
        conn.execute("CREATE INDEX idx_runs_started ON scraper_runs(started_at DESC)")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _rodzaj_to_smallint(value) -> Optional[int]:
    """Coerce upstream rodzaj (str or int) to SMALLINT; None passes through."""
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"rodzaj must be integer-valued, got {value!r}") from exc


def _file_type_from_types(file_types: str) -> str:
    """Classify the file_types csv the same way the old code did."""
    parts = file_types.split(",") if file_types else []
    if "xml" in parts:
        return "xml"
    if "pdf" in parts:
        return "pdf"
    if file_types:
        return "other"
    return "unknown"


# ---------------------------------------------------------------------------
# CRUD — companies (registry scheduling on krs_companies)
# ---------------------------------------------------------------------------


def upsert_krs(krs: str, company_name: Optional[str], legal_form: Optional[str], is_active: bool) -> None:
    """Insert or update a KRS company. Scheduling fields live on krs_companies.

    ``name`` is NOT NULL, so we coerce None → '' for the INSERT path. On
    update we treat '' as "no information" via NULLIF so successive calls
    that pass None preserve the current name instead of blanking it.
    """
    conn = get_conn()
    now = datetime.now(timezone.utc)
    conn.execute("""
        INSERT INTO krs_companies (krs, name, legal_form, is_active, first_seen_at, synced_at)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (krs) DO UPDATE SET
            name       = COALESCE(NULLIF(excluded.name, ''), krs_companies.name),
            legal_form = COALESCE(excluded.legal_form, krs_companies.legal_form),
            is_active  = excluded.is_active
    """, [krs, company_name or "", legal_form, is_active, now, now])


def get_krs_to_check(strategy: str, limit: int, error_backoff_hours: int) -> list[dict]:
    """Return KRS companies to check, ordered by strategy. Skip recently-errored ones."""
    conn = get_conn()

    order_clause = {
        "priority_then_oldest": "ORDER BY check_priority DESC, last_checked_at ASC NULLS FIRST",
        "oldest_first":         "ORDER BY last_checked_at ASC NULLS FIRST",
        "newest_first":         "ORDER BY first_seen_at DESC",
        "random":               "ORDER BY random()",
        "sequential":           "ORDER BY krs ASC",
    }.get(strategy, "ORDER BY check_priority DESC, last_checked_at ASC NULLS FIRST")

    rows = conn.execute(f"""
        SELECT krs, name, legal_form, is_active,
               check_priority, check_error_count, last_checked_at, last_error_message
        FROM krs_companies
        WHERE is_active = true
          AND NOT (
              check_error_count >= %s
              AND last_checked_at > (NOW() - %s * INTERVAL '1 hour')
          )
        {order_clause}
        LIMIT %s
    """, [settings.scraper_max_errors_before_skip, error_backoff_hours, limit]).fetchall()

    cols = ["krs", "company_name", "legal_form", "is_active",
            "check_priority", "check_error_count", "last_checked_at", "last_error_message"]
    return [dict(zip(cols, row)) for row in rows]


# ---------------------------------------------------------------------------
# CRUD — documents (split into krs_documents + krs_document_downloads)
# ---------------------------------------------------------------------------


def get_known_document_ids(krs: str) -> set[str]:
    """Return the set of document_ids already discovered for this KRS."""
    conn = get_conn()
    rows = conn.execute(
        "SELECT document_id FROM krs_documents WHERE krs = %s", [krs]
    ).fetchall()
    return {row[0] for row in rows}


def get_undownloaded_documents(krs: str) -> list[str]:
    """Return document_ids belonging to ``krs`` whose download is still pending."""
    conn = get_conn()
    rows = conn.execute(
        """
        SELECT d.document_id
        FROM krs_documents d
        LEFT JOIN krs_document_downloads dl USING (document_id)
        WHERE d.krs = %s
          AND (dl.is_downloaded IS FALSE OR dl.is_downloaded IS NULL)
        """,
        [krs],
    ).fetchall()
    return [row[0] for row in rows]


def insert_documents(docs: list[dict]) -> None:
    """Insert newly-discovered documents.

    Each discovery yields one row in ``krs_documents`` and a paired
    ``krs_document_downloads`` row with ``is_downloaded=false``. Re-runs
    are idempotent via ``ON CONFLICT DO NOTHING`` — existing download
    state is preserved unchanged.
    """
    if not docs:
        return
    conn = get_conn()
    for doc in docs:
        rodzaj_int = _rodzaj_to_smallint(doc.get("rodzaj"))
        if rodzaj_int is None:
            raise ValueError(f"insert_documents: rodzaj required, got {doc.get('rodzaj')!r}")
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
                doc["document_id"], doc["krs"], rodzaj_int, doc.get("nazwa"),
                doc.get("okres_start"), doc.get("okres_end"),
                is_deleted, doc["discovered_at"],
            ],
        )

        conn.execute(
            """
            INSERT INTO krs_document_downloads (document_id, is_downloaded)
            VALUES (%s, false)
            ON CONFLICT (document_id) DO NOTHING
            """,
            [doc["document_id"]],
        )


def update_document_metadata(document_id: str, meta: dict) -> None:
    """Fill in metadata fetched from the RDF API's dokumenty/{id} endpoint."""
    conn = get_conn()
    now = datetime.now(timezone.utc)

    if not _document_exists(conn, document_id):
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
            meta.get("filename"),
            meta.get("is_ifrs"),
            meta.get("is_correction"),
            meta.get("date_filed"),
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


def update_document_error(document_id: str, error: str) -> None:
    """Record a download error against a document."""
    conn = get_conn()
    if not _document_exists(conn, document_id):
        raise ValueError(f"Document {document_id} not found in krs_documents")
    now = datetime.now(timezone.utc)
    conn.execute(
        """
        UPDATE krs_document_downloads SET
            download_error = %s,
            updated_at     = %s
        WHERE document_id = %s
        """,
        [error, now, document_id],
    )


def mark_downloaded(
    document_id: str,
    storage_path: str,
    storage_backend: str,  # retained in signature; value is no longer persisted
    file_size: int,
    zip_size: int,         # retained; redundant with file_size_bytes, dropped
    file_count: int,
    file_types: str,
) -> None:
    """Flip ``is_downloaded`` to true and record storage + size metadata."""
    del storage_backend, zip_size  # dropped by the dedupe plan
    conn = get_conn()
    if not _document_exists(conn, document_id):
        raise ValueError(f"Document {document_id} not found in krs_documents")
    now = datetime.now(timezone.utc)
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


def _document_exists(conn, document_id: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM krs_documents WHERE document_id = %s", [document_id]
    ).fetchone()
    return row is not None


def update_krs_checked(krs: str, total_docs: int, total_downloaded: int, error: Optional[str] = None) -> None:
    """Update krs_companies scheduling columns after checking a KRS."""
    conn = get_conn()
    now = datetime.now(timezone.utc)
    if error:
        conn.execute("""
            UPDATE krs_companies SET
                last_checked_at     = %s,
                check_error_count   = check_error_count + 1,
                last_error_message  = %s
            WHERE krs = %s
        """, [now, error, krs])
    else:
        conn.execute("""
            UPDATE krs_companies SET
                last_checked_at     = %s,
                last_download_at    = %s,
                check_error_count   = 0,
                last_error_message  = NULL,
                total_documents     = %s,
                total_downloaded    = %s
            WHERE krs = %s
        """, [now, now, total_docs, total_downloaded, krs])


def create_run(run_id: str, mode: str, config_snapshot: str) -> None:
    """Insert a new scraper_runs record with status='running'."""
    conn = get_conn()
    now = datetime.now(timezone.utc)
    conn.execute("""
        INSERT INTO scraper_runs (run_id, started_at, status, mode, config_snapshot)
        VALUES (%s, %s, 'running', %s, %s)
    """, [run_id, now, mode, config_snapshot])


def finish_run(run_id: str, status: str, stats: dict) -> None:
    """Update a scraper_runs record with final stats."""
    conn = get_conn()
    now = datetime.now(timezone.utc)
    conn.execute("""
        UPDATE scraper_runs SET
            finished_at          = %s,
            status               = %s,
            krs_checked          = %s,
            krs_new_found        = %s,
            documents_discovered = %s,
            documents_downloaded = %s,
            documents_failed     = %s,
            bytes_downloaded     = %s,
            error_message        = %s
        WHERE run_id = %s
    """, [
        now, status,
        stats.get("krs_checked", 0),
        stats.get("krs_new_found", 0),
        stats.get("documents_discovered", 0),
        stats.get("documents_downloaded", 0),
        stats.get("documents_failed", 0),
        stats.get("bytes_downloaded", 0),
        stats.get("error_message"),
        run_id,
    ])


def get_stats() -> dict:
    """Return dashboard stats."""
    conn = get_conn()
    row = conn.execute("""
        SELECT
            count(*)                                                    AS total_krs,
            count(*) FILTER (WHERE last_checked_at IS NOT NULL)        AS checked,
            count(*) FILTER (WHERE last_checked_at IS NULL)            AS unchecked,
            count(*) FILTER (WHERE check_error_count > 0)              AS with_errors,
            coalesce(sum(total_documents), 0)                          AS total_documents,
            coalesce(sum(total_downloaded), 0)                         AS total_downloaded
        FROM krs_companies
    """).fetchone()
    cols = ["total_krs", "krs_checked", "krs_unchecked", "krs_with_errors",
            "total_documents", "total_downloaded"]
    return dict(zip(cols, row))


def get_last_run() -> Optional[dict]:
    """Return the most recent scraper_runs record."""
    conn = get_conn()
    row = conn.execute("""
        SELECT run_id, started_at, finished_at, status,
               mode, krs_checked, documents_downloaded, documents_failed,
               bytes_downloaded, error_message
        FROM scraper_runs
        ORDER BY started_at DESC
        LIMIT 1
    """).fetchone()
    if row is None:
        return None
    cols = ["run_id", "started_at", "finished_at", "status",
            "mode", "krs_checked", "documents_downloaded", "documents_failed",
            "bytes_downloaded", "error_message"]
    result = dict(zip(cols, row))
    # Convert timestamps to ISO strings for JSON serialization
    for key in ("started_at", "finished_at"):
        if result[key] is not None:
            result[key] = str(result[key])
    return result
