"""Store discovered KRS entities into krs_entities + krs_registry tables.

Uses the same short-lived connection + retry pattern as ProgressStore,
so multiple worker processes can write to the same DuckDB file.
"""

import json
import random
import time
from datetime import datetime, timezone

import duckdb

_MAX_LOCK_RETRIES = 20
_BASE_LOCK_DELAY = 0.05


class EntityStore:
    """Upsert KRS entities discovered by the batch scanner."""

    def __init__(self, db_path: str, *, init_schema: bool = True):
        self._db_path = db_path
        if init_schema:
            self._ensure_tables()

    def _with_conn(self, fn):
        for attempt in range(_MAX_LOCK_RETRIES):
            try:
                conn = duckdb.connect(self._db_path)
                try:
                    result = fn(conn)
                finally:
                    conn.close()
                return result
            except duckdb.IOException:
                if attempt == _MAX_LOCK_RETRIES - 1:
                    raise
                delay = _BASE_LOCK_DELAY * (2 ** attempt) + random.uniform(0, 0.05)
                time.sleep(delay)

    def _ensure_tables(self):
        """Create krs_entities and krs_registry if they don't exist.

        These tables may already exist from app startup — CREATE IF NOT EXISTS
        is idempotent.
        """
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
        """Store a discovered entity in krs_entities + krs_registry."""
        now = datetime.now(timezone.utc)
        raw_json = json.dumps(raw, ensure_ascii=False) if raw else None

        def _do(conn):
            # krs_entities — full entity record
            conn.execute("""
                INSERT INTO krs_entities (krs, name, legal_form, raw, source, synced_at)
                VALUES (?, ?, ?, ?, 'rdf_batch', ?)
                ON CONFLICT (krs) DO UPDATE SET
                    name = COALESCE(excluded.name, krs_entities.name),
                    legal_form = COALESCE(excluded.legal_form, krs_entities.legal_form),
                    raw = COALESCE(excluded.raw, krs_entities.raw),
                    synced_at = excluded.synced_at
            """, [krs, name, legal_form, raw_json, now])

            # krs_registry — for scraper discovery
            conn.execute("""
                INSERT INTO krs_registry (krs, company_name, legal_form, is_active, first_seen_at)
                VALUES (?, ?, ?, true, ?)
                ON CONFLICT (krs) DO UPDATE SET
                    company_name = COALESCE(excluded.company_name, krs_registry.company_name),
                    legal_form = COALESCE(excluded.legal_form, krs_registry.legal_form),
                    is_active = excluded.is_active
            """, [krs, name, legal_form, now])

        self._with_conn(_do)
