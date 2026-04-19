"""PostgreSQL repository for KRS entity data and sync logging.

Post-dedupe (SCHEMA_DEDUPE_PLAN #2) there is a single ``krs_companies``
table — no version history, no separate registry. Prior code carried an
append-only ``krs_entity_versions`` table, but production had zero historical
rows on it, so the versioning machinery was pure overhead.

Tables are created via ``_init_schema()`` at app startup for the bookkeeping
state (``krs_sync_log``, ``krs_scan_cursor``, ``krs_scan_runs``).
The ``krs_companies`` table itself comes from the dedupe/003 migration.
"""

from __future__ import annotations

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
    """Ensure shared connection is open and KRS bookkeeping schema exists."""
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

    # The authoritative krs_companies table is created by dedupe/003. We do
    # NOT redeclare it here; that way a rollback of the migration surfaces
    # cleanly (no silent table re-creation).

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
    conn.execute("""
        INSERT INTO krs_scan_cursor (id, next_krs_int) VALUES (TRUE, 1)
        ON CONFLICT DO NOTHING
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
            SET status = 'interrupted', finished_at = %s, stopped_reason = 'process_killed'
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
            SET status = 'interrupted', finished_at = %s
            WHERE status = 'running'
            """,
            [now],
        )
        ids = [r[0] for r in orphaned_syncs]
        logger.warning("krs_sync_orphaned_runs_closed", extra={
            "event": "krs_sync_orphaned_runs_closed", "run_ids": ids,
        })


# ---------------------------------------------------------------------------
# CRUD — krs_companies (plain upsert; no versioning)
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
    raw: dict | None = None,  # retained in the signature, no longer stored
    source: str = "ms_gov",
) -> None:
    """Insert-or-update a company in krs_companies.

    The ``raw`` kwarg is accepted for signature compatibility with callers
    that still pass the upstream payload. It is intentionally not persisted
    — the plan dropped the raw JSON column because ``raw.podmiot.*`` fully
    duplicates the flat columns.
    """
    del raw  # unused — see docstring
    conn = get_conn()
    now = datetime.now(timezone.utc).isoformat()

    conn.execute(
        """
        INSERT INTO krs_companies (
            krs, name, legal_form, status,
            registered_at, last_changed_at, nip, regon,
            address_city, address_street, address_postal_code,
            source, synced_at, first_seen_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (krs) DO UPDATE SET
            name                = EXCLUDED.name,
            legal_form          = COALESCE(EXCLUDED.legal_form, krs_companies.legal_form),
            status              = COALESCE(EXCLUDED.status, krs_companies.status),
            registered_at       = COALESCE(EXCLUDED.registered_at, krs_companies.registered_at),
            last_changed_at     = COALESCE(EXCLUDED.last_changed_at, krs_companies.last_changed_at),
            nip                 = COALESCE(EXCLUDED.nip, krs_companies.nip),
            regon               = COALESCE(EXCLUDED.regon, krs_companies.regon),
            address_city        = COALESCE(EXCLUDED.address_city, krs_companies.address_city),
            address_street      = COALESCE(EXCLUDED.address_street, krs_companies.address_street),
            address_postal_code = COALESCE(EXCLUDED.address_postal_code, krs_companies.address_postal_code),
            source              = EXCLUDED.source,
            synced_at           = EXCLUDED.synced_at
        """,
        [
            krs, name, legal_form, status,
            registered_at, last_changed_at, nip, regon,
            address_city, address_street, address_postal_code,
            source, now, now,
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


_ENTITY_COLUMNS = (
    "krs", "name", "legal_form", "status",
    "registered_at", "last_changed_at",
    "nip", "regon", "address_city", "address_street", "address_postal_code",
    "source", "synced_at",
)


def get_entity(krs: str) -> Optional[dict]:
    """Fetch a company by KRS. Returns None if not found."""
    conn = get_conn()
    cols = ", ".join(_ENTITY_COLUMNS)
    row = conn.execute(
        f"SELECT {cols} FROM krs_companies WHERE krs = %s", [krs]
    ).fetchone()
    if row is None:
        return None
    return dict(zip(_ENTITY_COLUMNS, row))


def list_stale(older_than: datetime) -> list[dict]:
    """Return companies not synced since ``older_than``."""
    conn = get_conn()
    rows = conn.execute(
        "SELECT krs, name, synced_at FROM krs_companies "
        "WHERE synced_at < %s ORDER BY synced_at",
        [older_than.isoformat()],
    ).fetchall()
    return [{"krs": r[0], "name": r[1], "synced_at": r[2]} for r in rows]


def count_entities() -> int:
    """Return total company count."""
    conn = get_conn()
    return conn.execute("SELECT count(*) FROM krs_companies").fetchone()[0]


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
        VALUES (%s, %s, 'running')
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
        SET finished_at = %s, krs_count = %s, new_count = %s,
            updated_count = %s, error_count = %s, status = %s
        WHERE id = %s
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
        SET next_krs_int = %s, updated_at = %s
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
        VALUES (%s, %s)
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
        sets.append("probed_count = %s")
        params.append(probed_count)
    if valid_count is not None:
        sets.append("valid_count = %s")
        params.append(valid_count)
    if error_count is not None:
        sets.append("error_count = %s")
        params.append(error_count)
    if not sets:
        return
    params.append(run_id)
    conn.execute(
        f"UPDATE krs_scan_runs SET {', '.join(sets)} WHERE id = %s",
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
        SET finished_at = %s, status = %s, krs_to = %s, stopped_reason = %s,
            probed_count = COALESCE(%s, probed_count),
            valid_count  = COALESCE(%s, valid_count),
            error_count  = COALESCE(%s, error_count)
        WHERE id = %s
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
        "SELECT column_name FROM information_schema.columns WHERE table_name = 'krs_scan_runs' AND table_schema = 'public' ORDER BY ordinal_position"
    ).fetchall()]
    return dict(zip(columns, row))


# ---------------------------------------------------------------------------
# Lookup — most recent sync
# ---------------------------------------------------------------------------


def get_last_sync(source: str = "ms_gov") -> Optional[dict]:
    """Return the most recent sync log entry for a source."""
    conn = get_conn()
    row = conn.execute(
        """
        SELECT * FROM krs_sync_log
        WHERE source = %s
        ORDER BY started_at DESC
        LIMIT 1
        """,
        [source],
    ).fetchone()
    if row is None:
        return None
    columns = [desc[0] for desc in conn.execute(
        "SELECT column_name FROM information_schema.columns WHERE table_name = 'krs_sync_log' AND table_schema = 'public' ORDER BY ordinal_position"
    ).fetchall()]
    return dict(zip(columns, row))
