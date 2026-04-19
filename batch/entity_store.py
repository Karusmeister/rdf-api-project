"""Store discovered KRS companies from the batch scanner.

Uses short-lived PostgreSQL connections via make_connection, so multiple
worker processes can write concurrently.

Post-dedupe (SCHEMA_DEDUPE_PLAN #2) there is a single krs_companies table
holding both the entity fields and the scraper-scheduling columns — no
append-only version history, no separate registry.
"""

import logging
import time
from datetime import datetime, timezone

import psycopg2

from app.db.connection import make_connection

logger = logging.getLogger(__name__)


class EntityStore:
    """Upsert KRS companies discovered by the batch scanner."""

    def __init__(self, dsn: str, *, init_schema: bool = True):
        self._dsn = dsn
        self._conn = None
        if init_schema:
            # The authoritative table (krs_companies) is created via the
            # dedupe/003 migration on API startup. Batch workers do not
            # bootstrap schema on their own — they must run after the API
            # has applied migrations at least once.
            pass

    def _get_conn(self):
        if self._conn is not None and not self._conn.closed:
            return self._conn
        self._conn = make_connection(self._dsn)
        return self._conn

    def _with_conn(self, fn):
        """Execute fn(conn) using a persistent connection with retry on failure."""
        for attempt in range(3):
            try:
                return fn(self._get_conn())
            except psycopg2.OperationalError:
                self._close_stale()
                if attempt == 2:
                    raise
                time.sleep(1.0 * (2 ** attempt))

    def _close_stale(self):
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception:
                pass
            self._conn = None

    def close(self):
        if self._conn is not None and not self._conn.closed:
            try:
                self._conn.close()
            except Exception:
                pass
            self._conn = None

    def upsert_entity(
        self,
        krs: str,
        name: str,
        legal_form: str | None = None,
        raw: dict | None = None,
    ) -> None:
        """Insert-or-update a company row. ``raw`` is no longer persisted."""
        del raw  # raw payload is intentionally dropped by the dedupe plan
        now = datetime.now(timezone.utc).isoformat()

        def _do(conn):
            conn.execute(
                """
                INSERT INTO krs_companies (krs, name, legal_form, source,
                                           synced_at, first_seen_at)
                VALUES (%s, %s, %s, 'rdf_batch', %s, %s)
                ON CONFLICT (krs) DO UPDATE SET
                    name       = EXCLUDED.name,
                    legal_form = COALESCE(EXCLUDED.legal_form, krs_companies.legal_form),
                    synced_at  = EXCLUDED.synced_at
                """,
                [krs, name, legal_form, now, now],
            )

        self._with_conn(_do)
