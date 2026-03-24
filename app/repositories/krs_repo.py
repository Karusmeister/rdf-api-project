"""DuckDB repository for KRS entity data and sync logging.

Tables are created via _init_schema() at app startup. GDPR note: PESEL and
personal addresses stay in the ``raw`` JSON column only — never normalised.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Optional

from app.db import connection as shared_conn

_schema_initialized = False


# ---------------------------------------------------------------------------
# Lifecycle
# ---------------------------------------------------------------------------


def connect() -> None:
    """Ensure shared connection is open and KRS entity schema exists."""
    shared_conn.connect()
    _ensure_schema()


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
            raw             JSON,
            source          VARCHAR NOT NULL DEFAULT 'ms_gov',
            synced_at       TIMESTAMP NOT NULL DEFAULT current_timestamp
        )
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
    raw: dict | None = None,
    source: str = "ms_gov",
) -> None:
    """Insert or update a KRS entity. Idempotent on krs."""
    conn = get_conn()
    raw_json = json.dumps(raw) if raw else None
    now = datetime.now(timezone.utc).isoformat()

    conn.execute(
        """
        INSERT INTO krs_entities
            (krs, name, legal_form, status, registered_at, last_changed_at,
             nip, regon, address_city, raw, source, synced_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (krs) DO UPDATE SET
            name            = excluded.name,
            legal_form      = excluded.legal_form,
            status          = excluded.status,
            registered_at   = excluded.registered_at,
            last_changed_at = excluded.last_changed_at,
            nip             = excluded.nip,
            regon           = excluded.regon,
            address_city    = excluded.address_city,
            raw             = excluded.raw,
            source          = excluded.source,
            synced_at       = excluded.synced_at
        """,
        [
            krs, name, legal_form, status, registered_at, last_changed_at,
            nip, regon, address_city, raw_json, source, now,
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
    return dict(zip(columns, result))


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
