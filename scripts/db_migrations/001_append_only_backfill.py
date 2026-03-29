"""Python backfill for append-only version tables.

Uses the same hashing logic as runtime to avoid hash mismatch on first
post-migration upsert.  Idempotent: skips rows already present.

Usage:
    python -m scripts.run_db_migration scripts/db_migrations/001_append_only_backfill.py [--db path]
    python scripts/db_migrations/001_append_only_backfill.py [--db path]
"""

import hashlib
import json
import os
import sys
from datetime import datetime, timezone as tz
from pathlib import Path

import duckdb


# ---------------------------------------------------------------------------
# Hash helpers — MUST match runtime (krs_repo._entity_snapshot_hash,
# scraper/db._document_snapshot_hash)
# ---------------------------------------------------------------------------

def _entity_snapshot_hash(row: dict) -> str:
    snapshot = {
        "name": row.get("name"),
        "legal_form": row.get("legal_form"),
        "status": row.get("status"),
        "registered_at": str(row["registered_at"]) if row.get("registered_at") is not None else None,
        "last_changed_at": str(row["last_changed_at"]) if row.get("last_changed_at") is not None else None,
        "nip": row.get("nip"),
        "regon": row.get("regon"),
        "address_city": row.get("address_city"),
        "address_street": row.get("address_street"),
        "address_postal_code": row.get("address_postal_code"),
        "raw": json.loads(row["raw"]) if isinstance(row.get("raw"), str) else row.get("raw"),
    }
    canonical = json.dumps(snapshot, sort_keys=True, ensure_ascii=False, default=str)
    return hashlib.md5(canonical.encode()).hexdigest()


_DOC_SNAPSHOT_FIELDS = (
    "rodzaj", "status", "nazwa", "okres_start", "okres_end",
    "filename", "is_ifrs", "is_correction", "date_filed", "date_prepared",
    "is_downloaded", "storage_path", "storage_backend",
    "file_size_bytes", "zip_size_bytes", "file_count", "file_types",
    "download_error",
)


def _document_snapshot_hash(row: dict) -> str:
    snapshot = {f: row.get(f) for f in _DOC_SNAPSHOT_FIELDS}
    canonical = json.dumps(snapshot, sort_keys=True, ensure_ascii=False, default=str)
    return hashlib.md5(canonical.encode()).hexdigest()


# ---------------------------------------------------------------------------
# Backfill
# ---------------------------------------------------------------------------

def _ensure_repo_root_on_path() -> None:
    """Allow running this migration directly via file path."""
    repo_root = Path(__file__).resolve().parents[2]
    repo_root_str = str(repo_root)
    if repo_root_str not in sys.path:
        sys.path.insert(0, repo_root_str)


def _init_schema(conn: duckdb.DuckDBPyConnection) -> None:
    """Bootstrap app schema so backfill queries can find the tables."""
    _ensure_repo_root_on_path()
    from app.db import connection as shared_conn
    from app.repositories import krs_repo
    from app.scraper import db as scraper_db
    from app.db import prediction_db

    original = getattr(shared_conn, "_conn", None)
    shared_conn._conn = conn
    try:
        krs_repo._schema_initialized = False
        scraper_db._schema_initialized = False
        prediction_db._schema_initialized = False
        krs_repo._init_schema()
        scraper_db._init_schema()
        prediction_db._init_schema()
    finally:
        shared_conn._conn = original
        krs_repo._schema_initialized = False
        scraper_db._schema_initialized = False
        prediction_db._schema_initialized = False


def run_backfill(db_path: str) -> None:
    conn = duckdb.connect(db_path)
    try:
        _init_schema(conn)
        _backfill_entities(conn)
        _backfill_documents(conn)
        print("Backfill completed successfully.")
    finally:
        conn.close()


def _backfill_entities(conn) -> None:
    rows = conn.execute("""
        SELECT e.*
        FROM krs_entities e
        LEFT JOIN krs_entity_versions kev ON kev.krs = e.krs AND kev.is_current = true
        WHERE kev.krs IS NULL
    """).fetchall()
    if not rows:
        print("  entity backfill: 0 rows (already done)")
        return

    cols = [d[0] for d in conn.execute("SELECT * FROM krs_entities LIMIT 0").description]
    for row_tuple in rows:
        row = dict(zip(cols, row_tuple))
        snap_hash = _entity_snapshot_hash(row)
        valid_from = row.get("synced_at") or datetime.now(tz.utc).isoformat()
        conn.execute("""
            INSERT INTO krs_entity_versions (
                krs, name, legal_form, status, registered_at, last_changed_at,
                nip, regon, address_city, address_street, address_postal_code,
                raw, source, valid_from, valid_to, is_current, snapshot_hash, change_reason
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, true, ?, 'bootstrap_from_krs_entities')
        """, [
            row["krs"], row["name"], row.get("legal_form"), row.get("status"),
            row.get("registered_at"), row.get("last_changed_at"),
            row.get("nip"), row.get("regon"),
            row.get("address_city"), row.get("address_street"), row.get("address_postal_code"),
            row.get("raw"), row.get("source", "ms_gov"),
            valid_from, snap_hash,
        ])
    print(f"  entity backfill: {len(rows)} rows")


def _backfill_documents(conn) -> None:
    rows = conn.execute("""
        SELECT d.*
        FROM krs_documents d
        LEFT JOIN krs_document_versions v ON v.document_id = d.document_id AND v.is_current = true
        WHERE v.document_id IS NULL
    """).fetchall()
    if not rows:
        print("  document backfill: 0 rows (already done)")
        return

    cols = [d[0] for d in conn.execute("SELECT * FROM krs_documents LIMIT 0").description]
    for row_tuple in rows:
        row = dict(zip(cols, row_tuple))
        snap_hash = _document_snapshot_hash(row)
        valid_from = row.get("discovered_at") or datetime.now(tz.utc).isoformat()
        conn.execute("""
            INSERT INTO krs_document_versions (
                document_id, version_no, krs,
                rodzaj, status, nazwa, okres_start, okres_end,
                filename, is_ifrs, is_correction, date_filed, date_prepared,
                is_downloaded, downloaded_at, storage_path, storage_backend,
                file_size_bytes, zip_size_bytes, file_count, file_types,
                discovered_at, metadata_fetched_at, download_error,
                valid_from, valid_to, is_current, snapshot_hash, change_reason
            ) VALUES (
                ?, 1, ?,
                ?, ?, ?, ?, ?,
                ?, ?, ?, ?, ?,
                ?, ?, ?, ?,
                ?, ?, ?, ?,
                ?, ?, ?,
                ?, NULL, true, ?, 'bootstrap_from_krs_documents'
            )
        """, [
            row["document_id"], row["krs"],
            row["rodzaj"], row["status"], row.get("nazwa"),
            row.get("okres_start"), row.get("okres_end"),
            row.get("filename"), row.get("is_ifrs"), row.get("is_correction"),
            row.get("date_filed"), row.get("date_prepared"),
            row.get("is_downloaded"), row.get("downloaded_at"),
            row.get("storage_path"), row.get("storage_backend"),
            row.get("file_size_bytes"), row.get("zip_size_bytes"),
            row.get("file_count"), row.get("file_types"),
            row.get("discovered_at"), row.get("metadata_fetched_at"),
            row.get("download_error"),
            valid_from, snap_hash,
        ])
    print(f"  document backfill: {len(rows)} rows")


if __name__ == "__main__":
    db = os.environ.get("SCRAPER_DB_PATH", "data/scraper.duckdb")
    if "--db" in sys.argv:
        idx = sys.argv.index("--db")
        db = sys.argv[idx + 1]
    run_backfill(db)
