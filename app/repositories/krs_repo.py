"""PostgreSQL repository for KRS entity data and sync logging.

Tables are created via _init_schema() at app startup. Company-level address
fields used by the adapter contract are stored in columns; sensitive personal
data stays only in the ``raw`` JSON payload.

Entity writes use an append-only pattern: each change creates a new version
in ``krs_entity_versions``. The ``krs_entities`` table is kept as a legacy
cache and the ``krs_entities_current`` view provides the read path.
"""

from __future__ import annotations

import hashlib
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
    _check_backfill_needed()
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

    # --- Append-only version history for KRS entities ---
    conn.execute("""
        CREATE SEQUENCE IF NOT EXISTS seq_krs_entity_versions START 1
    """)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS krs_entity_versions (
            version_id           BIGINT PRIMARY KEY DEFAULT nextval('seq_krs_entity_versions'),
            krs                  VARCHAR(10) NOT NULL,
            name                 VARCHAR NOT NULL,
            legal_form           VARCHAR,
            status               VARCHAR,
            registered_at        DATE,
            last_changed_at      DATE,
            nip                  VARCHAR(13),
            regon                VARCHAR(14),
            address_city         VARCHAR,
            address_street       VARCHAR,
            address_postal_code  VARCHAR,
            raw                  JSON,
            source               VARCHAR NOT NULL,

            valid_from           TIMESTAMP NOT NULL,
            valid_to             TIMESTAMP,
            is_current           BOOLEAN NOT NULL DEFAULT true,
            snapshot_hash        VARCHAR NOT NULL,
            change_reason        VARCHAR,
            observed_at          TIMESTAMP NOT NULL DEFAULT current_timestamp
        )
    """)

    # Indexes for krs_entity_versions
    existing_idx = {
        row[0] for row in conn.execute("SELECT indexname FROM pg_indexes WHERE schemaname = 'public'").fetchall()
    }
    for idx_name, idx_sql in [
        ("idx_krs_entity_versions_krs", "CREATE INDEX idx_krs_entity_versions_krs ON krs_entity_versions(krs)"),
        ("idx_krs_entity_versions_current", "CREATE INDEX idx_krs_entity_versions_current ON krs_entity_versions(krs, is_current)"),
        ("idx_krs_entity_versions_valid_from", "CREATE INDEX idx_krs_entity_versions_valid_from ON krs_entity_versions(valid_from)"),
    ]:
        if idx_name not in existing_idx:
            conn.execute(idx_sql)

    conn.execute("""
        CREATE OR REPLACE VIEW krs_entities_current AS
        SELECT
            krs, name, legal_form, status, registered_at, last_changed_at,
            nip, regon, address_city, address_street, address_postal_code,
            raw, source, valid_from AS synced_at
        FROM (
            SELECT
                kev.*,
                row_number() OVER (
                    PARTITION BY kev.krs
                    ORDER BY kev.valid_from DESC, kev.version_id DESC
                ) AS rn
            FROM krs_entity_versions kev
            WHERE kev.is_current = true
        ) ranked
        WHERE rn = 1
    """)

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


_ALLOWED_ALTER_COLUMNS = {
    "address_street": "VARCHAR",
    "address_postal_code": "VARCHAR",
}


def _ensure_krs_entities_columns(conn) -> None:
    existing_columns = {
        row[0] for row in conn.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'krs_entities' AND table_schema = 'public' ORDER BY ordinal_position").fetchall()
    }
    for name, definition in _ALLOWED_ALTER_COLUMNS.items():
        if name not in existing_columns:
            if name not in _ALLOWED_ALTER_COLUMNS or _ALLOWED_ALTER_COLUMNS[name] != definition:
                raise ValueError(f"Refusing to add unapproved column: {name} {definition}")
            conn.execute(f"ALTER TABLE krs_entities ADD COLUMN {name} {definition}")


def _check_backfill_needed() -> None:
    """Fail fast when legacy cache has rows but version table is empty."""
    conn = get_conn()
    legacy = conn.execute("SELECT count(*) FROM krs_entities").fetchone()[0]
    versions = conn.execute("SELECT count(*) FROM krs_entity_versions").fetchone()[0]
    if legacy > 0 and versions == 0:
        msg = (
            "Cutover blocked: krs_entity_versions is empty while legacy "
            f"krs_entities has {legacy} rows. "
            "Run the append-only backfill migration against PostgreSQL before startup."
        )
        logger.error(
            "krs_entity_backfill_required",
            extra={
                "event": "backfill_required",
                "legacy_count": legacy,
                "versions_count": versions,
                "hint": msg,
            },
        )
        raise RuntimeError(msg)


# ---------------------------------------------------------------------------
# Append-only versioning helpers
# ---------------------------------------------------------------------------

# Columns included in the canonical snapshot for hash comparison.
_ENTITY_SNAPSHOT_FIELDS = (
    "name", "legal_form", "status", "registered_at", "last_changed_at",
    "nip", "regon", "address_city", "address_street", "address_postal_code",
    "raw",
)


def _normalize_entity_snapshot(
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
) -> dict:
    """Build a canonical dict from entity fields (deterministic key order)."""
    return {
        "name": name,
        "legal_form": legal_form,
        "status": status,
        "registered_at": str(registered_at) if registered_at is not None else None,
        "last_changed_at": str(last_changed_at) if last_changed_at is not None else None,
        "nip": nip,
        "regon": regon,
        "address_city": address_city,
        "address_street": address_street,
        "address_postal_code": address_postal_code,
        "raw": raw,
    }


def _entity_snapshot_hash(snapshot: dict) -> str:
    """Compute a deterministic hash of an entity snapshot."""
    canonical = json.dumps(snapshot, sort_keys=True, ensure_ascii=False, default=str)
    return hashlib.md5(canonical.encode()).hexdigest()


def _append_entity_version_if_changed(
    conn,
    krs: str,
    snapshot: dict,
    snapshot_hash: str,
    source: str,
    now: str,
    raw_json: str | None,
) -> bool:
    """Compare hash with current version; append new version if changed.

    The read-close-insert cycle is wrapped in a transaction to prevent
    multi-current races.  Returns True if a new version was created.
    """
    conn.execute("BEGIN")
    try:
        current = conn.execute(
            """
            SELECT version_id, snapshot_hash
            FROM krs_entity_versions
            WHERE krs = %s AND is_current = true
            ORDER BY valid_from DESC, version_id DESC
            LIMIT 1
            """,
            [krs],
        ).fetchone()

        if current is not None and current[1] == snapshot_hash:
            conn.execute(
                "UPDATE krs_entity_versions SET observed_at = %s WHERE version_id = %s",
                [now, current[0]],
            )
            conn.execute("COMMIT")
            return False

        if current is not None:
            conn.execute(
                """
                UPDATE krs_entity_versions
                SET valid_to = %s, is_current = false
                WHERE version_id = %s AND is_current = true
                """,
                [now, current[0]],
            )

        conn.execute(
            """
            INSERT INTO krs_entity_versions
                (krs, name, legal_form, status, registered_at, last_changed_at,
                 nip, regon, address_city, address_street, address_postal_code,
                 raw, source, valid_from, is_current, snapshot_hash, observed_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, true, %s, %s)
            """,
            [
                krs,
                snapshot["name"],
                snapshot["legal_form"],
                snapshot["status"],
                snapshot["registered_at"],
                snapshot["last_changed_at"],
                snapshot["nip"],
                snapshot["regon"],
                snapshot["address_city"],
                snapshot["address_street"],
                snapshot["address_postal_code"],
                raw_json,
                source,
                now,
                snapshot_hash,
                now,
            ],
        )
        conn.execute("COMMIT")
        return True
    except Exception:
        conn.execute("ROLLBACK")
        raise


# ---------------------------------------------------------------------------
# CRUD — krs_entities (append-only via krs_entity_versions)
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
    """Append-only upsert: creates a new version only if data changed."""
    conn = get_conn()
    raw_json = json.dumps(raw, ensure_ascii=False) if raw is not None else None
    now = datetime.now(timezone.utc).isoformat()

    snapshot = _normalize_entity_snapshot(
        name,
        legal_form=legal_form,
        status=status,
        registered_at=registered_at,
        last_changed_at=last_changed_at,
        nip=nip,
        regon=regon,
        address_city=address_city,
        address_street=address_street,
        address_postal_code=address_postal_code,
        raw=raw,
    )
    snap_hash = _entity_snapshot_hash(snapshot)

    _append_entity_version_if_changed(conn, krs, snapshot, snap_hash, source, now, raw_json)

    # Keep legacy cache table in sync for backward compatibility
    conn.execute(
        """
        INSERT INTO krs_entities
            (krs, name, legal_form, status, registered_at, last_changed_at,
             nip, regon, address_city, address_street, address_postal_code,
             raw, source, synced_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
    """Fetch current entity from krs_entities_current view. Returns None if not found."""
    conn = get_conn()
    result = conn.execute(
        "SELECT * FROM krs_entities_current WHERE krs = %s", [krs]
    ).fetchone()
    if result is None:
        return None
    columns = [desc[0] for desc in conn.execute(
        "SELECT column_name FROM information_schema.columns WHERE table_name = 'krs_entities_current' AND table_schema = 'public' ORDER BY ordinal_position"
    ).fetchall()]
    row = dict(zip(columns, result))
    raw = row.get("raw")
    if isinstance(raw, str):
        row["raw"] = json.loads(raw)
    return row


def list_stale(older_than: datetime) -> list[dict]:
    """Return entities not synced since ``older_than``."""
    conn = get_conn()
    rows = conn.execute(
        "SELECT krs, name, synced_at FROM krs_entities_current WHERE synced_at < %s ORDER BY synced_at",
        [older_than.isoformat()],
    ).fetchall()
    return [{"krs": r[0], "name": r[1], "synced_at": r[2]} for r in rows]


def count_entities() -> int:
    """Return total unique entity count."""
    conn = get_conn()
    return conn.execute("SELECT count(*) FROM krs_entities_current").fetchone()[0]


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
# CRUD — krs_sync_log
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
