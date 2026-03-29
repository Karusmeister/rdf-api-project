"""DuckDB repository for KRS entity data and sync logging.

Tables are created via _init_schema() at app startup. Company-level address
fields used by the adapter contract are stored in columns; sensitive personal
data stays only in the ``raw`` JSON payload.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any, Optional

from app.db import connection as shared_conn

logger = logging.getLogger(__name__)

_schema_initialized = False


# ---------------------------------------------------------------------------
# Lifecycle
# ---------------------------------------------------------------------------


def connect() -> None:
    """Ensure shared connection is open and KRS entity schema exists."""
    shared_conn.connect()
    _ensure_schema()
    _close_orphaned_runs()
    logger.info("krs_repo_ready", extra={"event": "krs_repo_ready"})


def get_conn():
    return shared_conn.get_conn()


def _ensure_schema() -> None:
    global _schema_initialized
    if _schema_initialized:
        return
    _init_schema()
    _schema_initialized = True


def _init_schema() -> None:
    conn = get_conn()

    conn.execute("""
        CREATE TABLE IF NOT EXISTS krs_entities (
            krs             VARCHAR(10) PRIMARY KEY,
            name            VARCHAR NOT NULL,
            legal_form      VARCHAR,
            status          VARCHAR,
            registered_at   DATE,
            last_changed_at DATE,
            nip             VARCHAR(13),
            regon           VARCHAR(14),
            address_city    VARCHAR,
            address_street  VARCHAR,
            address_postal_code VARCHAR,
            raw             JSON,
            source          VARCHAR NOT NULL DEFAULT 'ms_gov',
            synced_at       TIMESTAMP NOT NULL DEFAULT current_timestamp
        )
    """)
    _ensure_krs_entities_columns(conn)

    conn.execute("""
        CREATE SEQUENCE IF NOT EXISTS seq_krs_sync_log START 1
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS krs_sync_log (
            id              INTEGER PRIMARY KEY DEFAULT nextval('seq_krs_sync_log'),
            started_at      TIMESTAMP NOT NULL,
            finished_at     TIMESTAMP,
            krs_count       INTEGER NOT NULL DEFAULT 0,
            new_count       INTEGER NOT NULL DEFAULT 0,
            updated_count   INTEGER NOT NULL DEFAULT 0,
            error_count     INTEGER NOT NULL DEFAULT 0,
            source          VARCHAR NOT NULL DEFAULT 'ms_gov',
            status          VARCHAR NOT NULL DEFAULT 'running'
        )
    """)

    # --- Sequential scanner tables (PKR-39) ---
    conn.execute("""
        CREATE TABLE IF NOT EXISTS krs_scan_cursor (
            id          BOOLEAN PRIMARY KEY DEFAULT TRUE,
            next_krs_int INTEGER NOT NULL DEFAULT 1,
            updated_at  TIMESTAMP NOT NULL DEFAULT current_timestamp
        )
    """)
    # Seed the single-row cursor if it doesn't exist
    conn.execute("""
        INSERT OR IGNORE INTO krs_scan_cursor (id, next_krs_int) VALUES (TRUE, 1)
    """)

    conn.execute("""
        CREATE SEQUENCE IF NOT EXISTS seq_krs_scan_runs START 1
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS krs_scan_runs (
            id              INTEGER PRIMARY KEY DEFAULT nextval('seq_krs_scan_runs'),
            started_at      TIMESTAMP NOT NULL DEFAULT current_timestamp,
            finished_at     TIMESTAMP,
            status          VARCHAR NOT NULL DEFAULT 'running',
            krs_from        INTEGER NOT NULL,
            krs_to          INTEGER,
            probed_count    INTEGER NOT NULL DEFAULT 0,
            valid_count     INTEGER NOT NULL DEFAULT 0,
            error_count     INTEGER NOT NULL DEFAULT 0,
            stopped_reason  VARCHAR
        )
    """)


def _close_orphaned_runs() -> None:
    """Close any scan/sync runs left in 'running' state from a prior crash."""
    conn = get_conn()
    now = datetime.now(timezone.utc).isoformat()

    orphaned_scans = conn.execute(
        "SELECT id FROM krs_scan_runs WHERE status = 'running'"
    ).fetchall()
    if orphaned_scans:
        conn.execute(
            """
            UPDATE krs_scan_runs
            SET status = 'interrupted', finished_at = ?, stopped_reason = 'process_killed'
            WHERE status = 'running'
            """,
            [now],
        )
        ids = [r[0] for r in orphaned_scans]
        logger.warning("krs_scan_orphaned_runs_closed", extra={
            "event": "krs_scan_orphaned_runs_closed", "run_ids": ids,
        })

    orphaned_syncs = conn.execute(
        "SELECT id FROM krs_sync_log WHERE status = 'running'"
    ).fetchall()
    if orphaned_syncs:
        conn.execute(
            """
            UPDATE krs_sync_log
            SET status = 'interrupted', finished_at = ?
            WHERE status = 'running'
            """,
            [now],
        )
        ids = [r[0] for r in orphaned_syncs]
        logger.warning("krs_sync_orphaned_runs_closed", extra={
            "event": "krs_sync_orphaned_runs_closed", "run_ids": ids,
        })


def _ensure_krs_entities_columns(conn) -> None:
    existing_columns = {
        row[0] for row in conn.execute("DESCRIBE krs_entities").fetchall()
    }
    for name, definition in (
        ("address_street", "VARCHAR"),
        ("address_postal_code", "VARCHAR"),
    ):
        if name not in existing_columns:
            conn.execute(f"ALTER TABLE krs_entities ADD COLUMN {name} {definition}")


# ---------------------------------------------------------------------------
# CRUD — krs_entities
# ---------------------------------------------------------------------------


def upsert_entity(
    krs: str,
    name: str,
    *,
    legal_form: str | None = None,
    status: str | None = None,
    registered_at: Any = None,
    last_changed_at: Any = None,
    nip: str | None = None,
    regon: str | None = None,
    address_city: str | None = None,
    address_street: str | None = None,
    address_postal_code: str | None = None,
    raw: dict | None = None,
    source: str = "ms_gov",
) -> None:
    """Insert or update a KRS entity. Idempotent on krs."""
    conn = get_conn()
    raw_json = json.dumps(raw, ensure_ascii=False) if raw is not None else None
    now = datetime.now(timezone.utc).isoformat()

    conn.execute(
        """
        INSERT INTO krs_entities
            (krs, name, legal_form, status, registered_at, last_changed_at,
             nip, regon, address_city, address_street, address_postal_code,
             raw, source, synced_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (krs) DO UPDATE SET
            name            = excluded.name,
            legal_form      = excluded.legal_form,
            status          = excluded.status,
            registered_at   = excluded.registered_at,
            last_changed_at = excluded.last_changed_at,
            nip             = excluded.nip,
            regon           = excluded.regon,
            address_city    = excluded.address_city,
            address_street  = excluded.address_street,
            address_postal_code = excluded.address_postal_code,
            raw             = excluded.raw,
            source          = excluded.source,
            synced_at       = excluded.synced_at
        """,
        [
            krs, name, legal_form, status, registered_at, last_changed_at,
            nip, regon, address_city, address_street, address_postal_code,
            raw_json, source, now,
        ],
    )


def upsert_from_krs_entity(entity, *, source: str = "ms_gov") -> None:
    """Convenience: upsert from a KrsEntity model instance."""
    upsert_entity(
        krs=entity.krs,
        name=entity.name,
        legal_form=entity.legal_form,
        status=entity.status,
        registered_at=entity.registered_at,
        last_changed_at=entity.last_changed_at,
        nip=entity.nip,
        regon=entity.regon,
        address_city=entity.address_city,
        address_street=entity.address_street,
        address_postal_code=entity.address_postal_code,
        raw=entity.raw,
        source=source,
    )


def get_entity(krs: str) -> Optional[dict]:
    """Fetch a single entity row as a dict. Returns None if not found."""
    conn = get_conn()
    result = conn.execute(
        "SELECT * FROM krs_entities WHERE krs = ?", [krs]
    ).fetchone()
    if result is None:
        return None
    columns = [desc[0] for desc in conn.execute(
        "SELECT * FROM krs_entities LIMIT 0"
    ).description]
    row = dict(zip(columns, result))
    raw = row.get("raw")
    if isinstance(raw, str):
        row["raw"] = json.loads(raw)
    return row


def list_stale(older_than: datetime) -> list[dict]:
    """Return entities not synced since ``older_than``."""
    conn = get_conn()
    rows = conn.execute(
        "SELECT krs, name, synced_at FROM krs_entities WHERE synced_at < ? ORDER BY synced_at",
        [older_than.isoformat()],
    ).fetchall()
    return [{"krs": r[0], "name": r[1], "synced_at": r[2]} for r in rows]


def count_entities() -> int:
    """Return total entity count."""
    conn = get_conn()
    return conn.execute("SELECT count(*) FROM krs_entities").fetchone()[0]


# ---------------------------------------------------------------------------
# CRUD — krs_sync_log
# ---------------------------------------------------------------------------


def log_sync_start(
    *,
    source: str = "ms_gov",
    started_at: datetime | None = None,
) -> int:
    """Create a new sync log entry and return its id."""
    conn = get_conn()
    ts = (started_at or datetime.now(timezone.utc)).isoformat()
    result = conn.execute(
        """
        INSERT INTO krs_sync_log (started_at, source, status)
        VALUES (?, ?, 'running')
        RETURNING id
        """,
        [ts, source],
    ).fetchone()
    return result[0]


def log_sync_finish(
    sync_id: int,
    *,
    krs_count: int = 0,
    new_count: int = 0,
    updated_count: int = 0,
    error_count: int = 0,
    status: str = "completed",
) -> None:
    """Finalise a sync log entry."""
    conn = get_conn()
    now = datetime.now(timezone.utc).isoformat()
    conn.execute(
        """
        UPDATE krs_sync_log
        SET finished_at = ?, krs_count = ?, new_count = ?,
            updated_count = ?, error_count = ?, status = ?
        WHERE id = ?
        """,
        [now, krs_count, new_count, updated_count, error_count, status, sync_id],
    )


# ---------------------------------------------------------------------------
# CRUD — krs_scan_cursor
# ---------------------------------------------------------------------------


def get_cursor() -> int:
    """Return the next KRS integer to probe."""
    conn = get_conn()
    row = conn.execute(
        "SELECT next_krs_int FROM krs_scan_cursor WHERE id = TRUE"
    ).fetchone()
    return row[0] if row else 1


def advance_cursor(next_krs_int: int) -> None:
    """Update the single-row cursor. Idempotent for the same value."""
    conn = get_conn()
    now = datetime.now(timezone.utc).isoformat()
    conn.execute(
        """
        UPDATE krs_scan_cursor
        SET next_krs_int = ?, updated_at = ?
        WHERE id = TRUE
        """,
        [next_krs_int, now],
    )


# ---------------------------------------------------------------------------
# CRUD — krs_scan_runs
# ---------------------------------------------------------------------------


def open_scan_run(krs_from: int) -> int:
    """Insert a new scan run row and return its id."""
    conn = get_conn()
    now = datetime.now(timezone.utc).isoformat()
    result = conn.execute(
        """
        INSERT INTO krs_scan_runs (started_at, krs_from)
        VALUES (?, ?)
        RETURNING id
        """,
        [now, krs_from],
    ).fetchone()
    return result[0]


def update_scan_run(
    run_id: int,
    *,
    probed_count: int | None = None,
    valid_count: int | None = None,
    error_count: int | None = None,
) -> None:
    """Update stats on a running scan."""
    conn = get_conn()
    sets: list[str] = []
    params: list[Any] = []
    if probed_count is not None:
        sets.append("probed_count = ?")
        params.append(probed_count)
    if valid_count is not None:
        sets.append("valid_count = ?")
        params.append(valid_count)
    if error_count is not None:
        sets.append("error_count = ?")
        params.append(error_count)
    if not sets:
        return
    params.append(run_id)
    conn.execute(
        f"UPDATE krs_scan_runs SET {', '.join(sets)} WHERE id = ?",
        params,
    )


def close_scan_run(
    run_id: int,
    *,
    status: str,
    krs_to: int,
    stopped_reason: str | None = None,
    probed_count: int | None = None,
    valid_count: int | None = None,
    error_count: int | None = None,
) -> None:
    """Finalise a scan run."""
    conn = get_conn()
    now = datetime.now(timezone.utc).isoformat()
    conn.execute(
        """
        UPDATE krs_scan_runs
        SET finished_at = ?, status = ?, krs_to = ?, stopped_reason = ?,
            probed_count = COALESCE(?, probed_count),
            valid_count  = COALESCE(?, valid_count),
            error_count  = COALESCE(?, error_count)
        WHERE id = ?
        """,
        [now, status, krs_to, stopped_reason,
         probed_count, valid_count, error_count, run_id],
    )


def get_last_scan_run() -> Optional[dict]:
    """Return the most recent scan run."""
    conn = get_conn()
    row = conn.execute(
        """
        SELECT * FROM krs_scan_runs
        ORDER BY started_at DESC
        LIMIT 1
        """
    ).fetchone()
    if row is None:
        return None
    columns = [desc[0] for desc in conn.execute(
        "SELECT * FROM krs_scan_runs LIMIT 0"
    ).description]
    return dict(zip(columns, row))


# ---------------------------------------------------------------------------
# CRUD — krs_sync_log
# ---------------------------------------------------------------------------


def get_last_sync(source: str = "ms_gov") -> Optional[dict]:
    """Return the most recent sync log entry for a source."""
    conn = get_conn()
    row = conn.execute(
        """
        SELECT * FROM krs_sync_log
        WHERE source = ?
        ORDER BY started_at DESC
        LIMIT 1
        """,
        [source],
    ).fetchone()
    if row is None:
        return None
    columns = [desc[0] for desc in conn.execute(
        "SELECT * FROM krs_sync_log LIMIT 0"
    ).description]
    return dict(zip(columns, row))
