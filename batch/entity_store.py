"""Store discovered KRS entities into krs_entity_versions + krs_registry tables.

Uses short-lived PostgreSQL connections via make_connection,
so multiple worker processes can write concurrently.

Entity writes use append-only versioning: a new version row is created only
when the snapshot hash changes.
"""

import hashlib
import json
import logging
from datetime import datetime, timezone

import psycopg2

from app.db.connection import make_connection

logger = logging.getLogger(__name__)


def _entity_snapshot_hash(name: str, legal_form: str | None, raw: dict | None) -> str:
    """Deterministic hash — MUST match krs_repo._entity_snapshot_hash.

    Uses the same _normalize_entity_snapshot field set as krs_repo so that
    both the batch scanner and the app-level sync produce identical hashes
    for identical data.
    """
    snapshot = {
        "name": name,
        "legal_form": legal_form,
        "status": None,
        "registered_at": None,
        "last_changed_at": None,
        "nip": None,
        "regon": None,
        "address_city": None,
        "address_street": None,
        "address_postal_code": None,
        "raw": raw,
    }
    canonical = json.dumps(snapshot, sort_keys=True, ensure_ascii=False, default=str)
    return hashlib.md5(canonical.encode()).hexdigest()


class EntityStore:
    """Upsert KRS entities discovered by the batch scanner."""

    def __init__(self, dsn: str, *, init_schema: bool = True):
        self._dsn = dsn
        if init_schema:
            self._ensure_tables()

    def _with_conn(self, fn):
        """Open a short-lived connection, call fn(conn), close, return result."""
        conn = make_connection(self._dsn)
        try:
            result = fn(conn)
        finally:
            conn.close()
        return result

    def _ensure_tables(self):
        """Create tables if they don't exist. Idempotent."""
        def _do(conn):
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
                    source          VARCHAR NOT NULL DEFAULT 'rdf_batch',
                    synced_at       TIMESTAMP NOT NULL DEFAULT current_timestamp
                )
            """)
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
        self._with_conn(_do)

    def upsert_entity(
        self,
        krs: str,
        name: str,
        legal_form: str | None = None,
        raw: dict | None = None,
    ) -> None:
        """Append-only store of a discovered entity + krs_registry update."""
        now = datetime.now(timezone.utc)
        now_iso = now.isoformat()
        raw_json = json.dumps(raw, ensure_ascii=False) if raw else None
        snap_hash = _entity_snapshot_hash(name, legal_form, raw)

        def _do(conn):
            # All writes (version + legacy + registry) in one transaction
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

                if current is not None and current[1] == snap_hash:
                    conn.execute(
                        "UPDATE krs_entity_versions SET observed_at = %s WHERE version_id = %s",
                        [now_iso, current[0]],
                    )
                else:
                    if current is not None:
                        conn.execute(
                            "UPDATE krs_entity_versions SET valid_to = %s, is_current = false WHERE version_id = %s AND is_current = true",
                            [now_iso, current[0]],
                        )
                    conn.execute(
                        """
                        INSERT INTO krs_entity_versions
                            (krs, name, legal_form, raw, source,
                             valid_from, is_current, snapshot_hash, observed_at)
                        VALUES (%s, %s, %s, %s, 'rdf_batch', %s, true, %s, %s)
                        """,
                        [krs, name, legal_form, raw_json, now_iso, snap_hash, now_iso],
                    )

                # Legacy cache (inside same transaction)
                conn.execute("""
                    INSERT INTO krs_entities (krs, name, legal_form, raw, source, synced_at)
                    VALUES (%s, %s, %s, %s, 'rdf_batch', %s)
                    ON CONFLICT (krs) DO UPDATE SET
                        name = COALESCE(excluded.name, krs_entities.name),
                        legal_form = COALESCE(excluded.legal_form, krs_entities.legal_form),
                        raw = COALESCE(excluded.raw, krs_entities.raw),
                        synced_at = excluded.synced_at
                """, [krs, name, legal_form, raw_json, now_iso])

                # krs_registry (inside same transaction)
                conn.execute("""
                    INSERT INTO krs_registry (krs, company_name, legal_form, is_active, first_seen_at)
                    VALUES (%s, %s, %s, true, %s)
                    ON CONFLICT (krs) DO UPDATE SET
                        company_name = COALESCE(excluded.company_name, krs_registry.company_name),
                        legal_form = COALESCE(excluded.legal_form, krs_registry.legal_form),
                        is_active = excluded.is_active
                """, [krs, name, legal_form, now_iso])

                conn.execute("COMMIT")
            except Exception:
                try:
                    conn.execute("ROLLBACK")
                except Exception as rollback_err:
                    logger.error("ROLLBACK failed: %s", rollback_err)
                raise

        self._with_conn(_do)
