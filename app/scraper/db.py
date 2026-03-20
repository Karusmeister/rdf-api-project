from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

import duckdb

from app.config import settings
from app.db import connection as shared_conn

_schema_initialized = False

# Backward-compat: tests set ``_conn = None`` to force reconnect.
# This property-like attribute is kept so existing test code that writes
# ``scraper_db._conn = None`` continues to work by resetting the shared
# connection instead.  (Module-level __setattr__ is not possible, so we
# provide a reset helper that tests should migrate toward.)


def connect() -> None:
    """Ensure shared connection is open and scraper schema exists."""
    shared_conn.connect()
    _ensure_schema()


def close() -> None:
    """No-op. Connection lifecycle is managed by app.db.connection."""
    pass


def get_conn() -> duckdb.DuckDBPyConnection:
    """Return the shared DuckDB connection."""
    return shared_conn.get_conn()


def _ensure_schema() -> None:
    global _schema_initialized
    if _schema_initialized:
        return
    _init_schema()
    _schema_initialized = True


def _init_schema() -> None:
    """Create tables if they don't exist. Idempotent."""
    conn = get_conn()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS krs_registry (
            krs                 VARCHAR(10) PRIMARY KEY,
            company_name        VARCHAR,
            legal_form          VARCHAR,
            is_active           BOOLEAN DEFAULT true,

            first_seen_at       TIMESTAMP NOT NULL,
            last_checked_at     TIMESTAMP,
            last_download_at    TIMESTAMP,

            check_priority      INTEGER DEFAULT 0,
            check_error_count   INTEGER DEFAULT 0,
            last_error_message  VARCHAR,

            total_documents     INTEGER DEFAULT 0,
            total_downloaded    INTEGER DEFAULT 0
        )
    """)

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

            -- NOTE: No FK to krs_registry. DuckDB FK enforcement blocks UPDATE
            -- on the parent row when child rows exist (a known DuckDB limitation).
            -- Referential integrity is enforced by application logic instead:
            -- scraper job always upserts krs_registry before inserting documents.
        )
    """)

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

    # Indexes (CREATE INDEX IF NOT EXISTS not supported in all DuckDB versions,
    # so we check manually via information_schema)
    existing = {
        row[0]
        for row in conn.execute(
            "SELECT index_name FROM duckdb_indexes()"
        ).fetchall()
    }

    index_defs = [
        ("idx_registry_last_checked", "CREATE INDEX idx_registry_last_checked ON krs_registry(last_checked_at)"),
        ("idx_registry_priority", "CREATE INDEX idx_registry_priority ON krs_registry(check_priority DESC, last_checked_at ASC)"),
        ("idx_documents_krs", "CREATE INDEX idx_documents_krs ON krs_documents(krs)"),
        ("idx_documents_not_downloaded", "CREATE INDEX idx_documents_not_downloaded ON krs_documents(is_downloaded)"),
        ("idx_runs_started", "CREATE INDEX idx_runs_started ON scraper_runs(started_at DESC)"),
    ]

    for name, sql in index_defs:
        if name not in existing:
            conn.execute(sql)


# ---------------------------------------------------------------------------
# CRUD helpers
# ---------------------------------------------------------------------------

def upsert_krs(krs: str, company_name: Optional[str], legal_form: Optional[str], is_active: bool) -> None:
    """Insert or update a KRS in the registry."""
    conn = get_conn()
    now = datetime.now(timezone.utc)
    conn.execute("""
        INSERT INTO krs_registry (krs, company_name, legal_form, is_active, first_seen_at)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT (krs) DO UPDATE SET
            company_name = COALESCE(excluded.company_name, krs_registry.company_name),
            legal_form   = COALESCE(excluded.legal_form,   krs_registry.legal_form),
            is_active    = excluded.is_active
    """, [krs, company_name, legal_form, is_active, now])


def get_krs_to_check(strategy: str, limit: int, error_backoff_hours: int) -> list[dict]:
    """Return KRS entries to check, ordered by strategy. Skip recently-errored ones."""
    conn = get_conn()

    order_clause = {
        "priority_then_oldest": "ORDER BY check_priority DESC, last_checked_at ASC NULLS FIRST",
        "oldest_first":         "ORDER BY last_checked_at ASC NULLS FIRST",
        "newest_first":         "ORDER BY first_seen_at DESC",
        "random":               "ORDER BY random()",
        "sequential":           "ORDER BY krs ASC",
    }.get(strategy, "ORDER BY check_priority DESC, last_checked_at ASC NULLS FIRST")

    rows = conn.execute(f"""
        SELECT krs, company_name, legal_form, is_active,
               check_priority, check_error_count, last_checked_at, last_error_message
        FROM krs_registry
        WHERE is_active = true
          AND NOT (
              check_error_count >= ?
              AND last_checked_at > (NOW() - INTERVAL (? || ' hours'))
          )
        {order_clause}
        LIMIT ?
    """, [settings.scraper_max_errors_before_skip, str(error_backoff_hours), limit]).fetchall()

    cols = ["krs", "company_name", "legal_form", "is_active",
            "check_priority", "check_error_count", "last_checked_at", "last_error_message"]
    return [dict(zip(cols, row)) for row in rows]


def get_known_document_ids(krs: str) -> set[str]:
    """Return set of document_ids we already know about for this KRS."""
    conn = get_conn()
    rows = conn.execute(
        "SELECT document_id FROM krs_documents WHERE krs = ?", [krs]
    ).fetchall()
    return {row[0] for row in rows}


def get_undownloaded_documents(krs: str) -> list[str]:
    """Return list of document_ids for this KRS where is_downloaded = false."""
    conn = get_conn()
    rows = conn.execute(
        "SELECT document_id FROM krs_documents WHERE krs = ? AND is_downloaded = false", [krs]
    ).fetchall()
    return [row[0] for row in rows]


def insert_documents(docs: list[dict]) -> None:
    """Batch insert new documents. Each dict has keys matching krs_documents columns."""
    conn = get_conn()
    for doc in docs:
        conn.execute("""
            INSERT INTO krs_documents
                (document_id, krs, rodzaj, status, nazwa, okres_start, okres_end, discovered_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (document_id) DO NOTHING
        """, [
            doc["document_id"], doc["krs"], doc["rodzaj"], doc["status"],
            doc.get("nazwa"), doc.get("okres_start"), doc.get("okres_end"),
            doc["discovered_at"],
        ])


def update_document_metadata(document_id: str, meta: dict) -> None:
    """Update extended metadata fields on krs_documents."""
    conn = get_conn()
    now = datetime.now(timezone.utc)
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
        meta.get("filename"),
        meta.get("is_ifrs"),
        meta.get("is_correction"),
        meta.get("date_filed"),
        meta.get("date_prepared"),
        now,
        document_id,
    ])


def update_document_error(document_id: str, error: str) -> None:
    """Set download_error on a document."""
    conn = get_conn()
    conn.execute(
        "UPDATE krs_documents SET download_error = ? WHERE document_id = ?",
        [error, document_id],
    )


def mark_downloaded(
    document_id: str,
    storage_path: str,
    storage_backend: str,
    file_size: int,
    zip_size: int,
    file_count: int,
    file_types: str,
) -> None:
    """Mark a document as downloaded."""
    conn = get_conn()
    now = datetime.now(timezone.utc)
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
    """, [now, storage_path, storage_backend, file_size, zip_size, file_count, file_types, document_id])


def update_krs_checked(krs: str, total_docs: int, total_downloaded: int, error: Optional[str] = None) -> None:
    """Update krs_registry after checking a KRS. Resets or increments error count."""
    conn = get_conn()
    now = datetime.now(timezone.utc)
    if error:
        conn.execute("""
            UPDATE krs_registry SET
                last_checked_at     = ?,
                check_error_count   = check_error_count + 1,
                last_error_message  = ?
            WHERE krs = ?
        """, [now, error, krs])
    else:
        conn.execute("""
            UPDATE krs_registry SET
                last_checked_at     = ?,
                last_download_at    = ?,
                check_error_count   = 0,
                last_error_message  = NULL,
                total_documents     = ?,
                total_downloaded    = ?
            WHERE krs = ?
        """, [now, now, total_docs, total_downloaded, krs])


def create_run(run_id: str, mode: str, config_snapshot: str) -> None:
    """Insert a new scraper_runs record with status='running'."""
    conn = get_conn()
    now = datetime.now(timezone.utc)
    conn.execute("""
        INSERT INTO scraper_runs (run_id, started_at, status, mode, config_snapshot)
        VALUES (?, ?, 'running', ?, ?)
    """, [run_id, now, mode, config_snapshot])


def finish_run(run_id: str, status: str, stats: dict) -> None:
    """Update a scraper_runs record with final stats."""
    conn = get_conn()
    now = datetime.now(timezone.utc)
    conn.execute("""
        UPDATE scraper_runs SET
            finished_at          = ?,
            status               = ?,
            krs_checked          = ?,
            krs_new_found        = ?,
            documents_discovered = ?,
            documents_downloaded = ?,
            documents_failed     = ?,
            bytes_downloaded     = ?,
            error_message        = ?
        WHERE run_id = ?
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
        FROM krs_registry
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
