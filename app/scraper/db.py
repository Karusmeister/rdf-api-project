"""Scraper control-plane database: krs_registry, krs_documents, scraper_runs.

Document writes use an append-only pattern via ``krs_document_versions``.
The legacy ``krs_documents`` table is kept as a cache for backward compat;
reads migrate to the ``krs_documents_current`` view.
"""
from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Optional

import duckdb

import logging

from app.config import settings
from app.db import connection as shared_conn

logger = logging.getLogger(__name__)

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
    _check_backfill_needed()


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

    # --- Append-only version history for RDF documents ---
    conn.execute("""
        CREATE SEQUENCE IF NOT EXISTS seq_krs_document_versions START 1
    """)

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

    conn.execute("""
        CREATE OR REPLACE VIEW krs_documents_current AS
        SELECT
            document_id, krs, rodzaj, status, nazwa, okres_start, okres_end,
            filename, is_ifrs, is_correction, date_filed, date_prepared,
            is_downloaded, downloaded_at, storage_path, storage_backend,
            file_size_bytes, zip_size_bytes, file_count, file_types,
            discovered_at, metadata_fetched_at, download_error
        FROM (
            SELECT
                kdv.*,
                row_number() OVER (
                    PARTITION BY kdv.document_id
                    ORDER BY kdv.version_no DESC, kdv.version_id DESC
                ) AS rn
            FROM krs_document_versions kdv
            WHERE kdv.is_current = true
        ) ranked
        WHERE rn = 1
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
        ("idx_krs_doc_versions_doc_current", "CREATE INDEX idx_krs_doc_versions_doc_current ON krs_document_versions(document_id, is_current)"),
        ("idx_krs_doc_versions_krs_current", "CREATE INDEX idx_krs_doc_versions_krs_current ON krs_document_versions(krs, is_current)"),
        ("idx_krs_doc_versions_valid_from", "CREATE INDEX idx_krs_doc_versions_valid_from ON krs_document_versions(valid_from)"),
    ]

    for name, sql in index_defs:
        if name not in existing:
            conn.execute(sql)


def _check_backfill_needed() -> None:
    """Fail fast when legacy cache has rows but version table is empty."""
    conn = get_conn()
    legacy = conn.execute("SELECT count(*) FROM krs_documents").fetchone()[0]
    versions = conn.execute("SELECT count(*) FROM krs_document_versions").fetchone()[0]
    if legacy > 0 and versions == 0:
        migration_cmd = (
            "python -m scripts.run_db_migration "
            "scripts/db_migrations/001_append_only_backfill.py "
            f"--db {settings.scraper_db_path}"
        )
        msg = (
            "Cutover blocked: krs_document_versions is empty while legacy "
            f"krs_documents has {legacy} rows. Run backfill before startup: {migration_cmd}"
        )
        logger.error(
            "krs_document_backfill_required",
            extra={
                "event": "backfill_required",
                "legacy_count": legacy,
                "versions_count": versions,
                "hint": migration_cmd,
            },
        )
        raise RuntimeError(msg)


# ---------------------------------------------------------------------------
# Append-only document versioning helpers
# ---------------------------------------------------------------------------

# Snapshot fields used for hash comparison — these are the fields whose
# change constitutes a meaningful new version.
_DOC_SNAPSHOT_FIELDS = (
    "rodzaj", "status", "nazwa", "okres_start", "okres_end",
    "filename", "is_ifrs", "is_correction", "date_filed", "date_prepared",
    "is_downloaded", "storage_path", "storage_backend",
    "file_size_bytes", "zip_size_bytes", "file_count", "file_types",
    "download_error",
)


def _document_snapshot_hash(snapshot: dict) -> str:
    """Deterministic hash of a document snapshot dict."""
    canonical = json.dumps(snapshot, sort_keys=True, ensure_ascii=False, default=str)
    return hashlib.md5(canonical.encode()).hexdigest()


def _get_current_document_snapshot(conn, document_id: str) -> Optional[dict]:
    """Return the current version row as a dict, or None."""
    row = conn.execute(
        """
        SELECT * FROM krs_document_versions
        WHERE document_id = ? AND is_current = true
        ORDER BY version_no DESC, version_id DESC
        LIMIT 1
        """,
        [document_id],
    ).fetchone()
    if row is None:
        return None
    cols = [d[0] for d in conn.execute("SELECT * FROM krs_document_versions LIMIT 0").description]
    return dict(zip(cols, row))


def _merge_document_patch(current: Optional[dict], patch: dict) -> dict:
    """Merge a partial update onto the current snapshot.

    If a key is present in ``patch``, its value is used — even if ``None``
    (explicit NULL overwrite).  Keys absent from ``patch`` carry forward
    from ``current``.
    """
    if current is None:
        return {f: patch.get(f) for f in _DOC_SNAPSHOT_FIELDS}
    merged = {}
    for f in _DOC_SNAPSHOT_FIELDS:
        if f in patch:
            merged[f] = patch[f]
        else:
            merged[f] = current.get(f)
    return merged


def _append_document_version_if_changed(
    conn,
    document_id: str,
    krs: str,
    patch: dict,
    *,
    change_reason: str | None = None,
    run_id: str | None = None,
) -> bool:
    """Append a new document version if the snapshot hash changed.

    ``patch`` is a dict with any subset of _DOC_SNAPSHOT_FIELDS plus
    optionally ``discovered_at``, ``metadata_fetched_at``, ``downloaded_at``.

    Returns True if a new version was inserted.
    """
    now = datetime.now(timezone.utc).isoformat()

    conn.execute("BEGIN")
    try:
        current = _get_current_document_snapshot(conn, document_id)
        merged = _merge_document_patch(current, patch)
        new_hash = _document_snapshot_hash(merged)

        if current is not None and current.get("snapshot_hash") == new_hash:
            conn.execute(
                "UPDATE krs_document_versions SET observed_at = ? WHERE version_id = ?",
                [now, current["version_id"]],
            )
            conn.execute("COMMIT")
            return False

        next_version_no = 1
        if current is not None:
            conn.execute(
                "UPDATE krs_document_versions SET valid_to = ?, is_current = false WHERE version_id = ? AND is_current = true",
                [now, current["version_id"]],
            )
            next_version_no = current["version_no"] + 1

        discovered_at = patch.get("discovered_at") or (current["discovered_at"] if current else now)
        metadata_fetched_at = patch.get("metadata_fetched_at") or (current["metadata_fetched_at"] if current else None)
        downloaded_at = patch.get("downloaded_at") or (current["downloaded_at"] if current else None)

        conn.execute(
        """
        INSERT INTO krs_document_versions (
            document_id, version_no, krs,
            rodzaj, status, nazwa, okres_start, okres_end,
            filename, is_ifrs, is_correction, date_filed, date_prepared,
            is_downloaded, downloaded_at, storage_path, storage_backend,
            file_size_bytes, zip_size_bytes, file_count, file_types,
            discovered_at, metadata_fetched_at, download_error,
            valid_from, is_current, snapshot_hash, change_reason, run_id, observed_at
        ) VALUES (
            ?, ?, ?,
            ?, ?, ?, ?, ?,
            ?, ?, ?, ?, ?,
            ?, ?, ?, ?,
            ?, ?, ?, ?,
            ?, ?, ?,
            ?, true, ?, ?, ?, ?
        )
        """,
        [
            document_id, next_version_no, krs,
            merged["rodzaj"], merged["status"], merged["nazwa"],
            merged.get("okres_start"), merged.get("okres_end"),
            merged["filename"], merged["is_ifrs"], merged["is_correction"],
            merged["date_filed"], merged["date_prepared"],
            merged["is_downloaded"], downloaded_at,
            merged["storage_path"], merged["storage_backend"],
            merged["file_size_bytes"], merged["zip_size_bytes"],
            merged["file_count"], merged["file_types"],
            discovered_at, metadata_fetched_at, merged["download_error"],
            now, new_hash, change_reason, run_id, now,
        ],
        )
        conn.execute("COMMIT")
        return True
    except Exception:
        conn.execute("ROLLBACK")
        raise


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
        "SELECT document_id FROM krs_documents_current WHERE krs = ?", [krs]
    ).fetchall()
    return {row[0] for row in rows}


def get_undownloaded_documents(krs: str) -> list[str]:
    """Return list of document_ids for this KRS where is_downloaded = false."""
    conn = get_conn()
    rows = conn.execute(
        """SELECT document_id FROM krs_documents_current
           WHERE krs = ? AND (is_downloaded = false OR is_downloaded IS NULL)""",
        [krs],
    ).fetchall()
    return [row[0] for row in rows]


def insert_documents(docs: list[dict]) -> None:
    """Batch insert new documents — creates initial version in krs_document_versions."""
    conn = get_conn()
    for doc in docs:
        doc_id = doc["document_id"]
        krs = doc["krs"]

        # Append-only: create version 1 if document not yet known
        _append_document_version_if_changed(
            conn, doc_id, krs,
            patch={
                "rodzaj": doc["rodzaj"],
                "status": doc["status"],
                "nazwa": doc.get("nazwa"),
                "okres_start": doc.get("okres_start"),
                "okres_end": doc.get("okres_end"),
                "is_downloaded": False,
                "discovered_at": doc["discovered_at"],
            },
            change_reason="discovery",
        )

        # Legacy cache
        conn.execute("""
            INSERT INTO krs_documents
                (document_id, krs, rodzaj, status, nazwa, okres_start, okres_end, discovered_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (document_id) DO NOTHING
        """, [
            doc_id, krs, doc["rodzaj"], doc["status"],
            doc.get("nazwa"), doc.get("okres_start"), doc.get("okres_end"),
            doc["discovered_at"],
        ])


def update_document_metadata(document_id: str, meta: dict) -> None:
    """Update extended metadata fields — appends a new version."""
    conn = get_conn()
    now = datetime.now(timezone.utc)

    # Need krs from current version
    current = _get_current_document_snapshot(conn, document_id)
    krs = current["krs"] if current else ""

    _append_document_version_if_changed(
        conn, document_id, krs,
        patch={
            "filename": meta.get("filename"),
            "is_ifrs": meta.get("is_ifrs"),
            "is_correction": meta.get("is_correction"),
            "date_filed": meta.get("date_filed"),
            "date_prepared": meta.get("date_prepared"),
            "metadata_fetched_at": now.isoformat(),
        },
        change_reason="metadata_update",
    )

    # Legacy cache
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
    """Set download_error on a document — appends a new version."""
    conn = get_conn()

    current = _get_current_document_snapshot(conn, document_id)
    krs = current["krs"] if current else ""

    _append_document_version_if_changed(
        conn, document_id, krs,
        patch={"download_error": error},
        change_reason="download_error",
    )

    # Legacy cache
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
    """Mark a document as downloaded — appends a new version."""
    conn = get_conn()
    now = datetime.now(timezone.utc)

    current = _get_current_document_snapshot(conn, document_id)
    krs = current["krs"] if current else ""

    _append_document_version_if_changed(
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

    # Legacy cache
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
