"""Root conftest for PostgreSQL-backed tests.

Provides fixtures for database isolation. Each test gets a fresh schema
via TRUNCATE of all tables and sequence resets, so tests are independent.
"""

import os

import psycopg2
import pytest

from app.db.connection import make_connection

# Default test DSN — override via TEST_DATABASE_URL env var
_DEFAULT_TEST_DSN = "postgresql://localhost:5432/rdf_test"


def _get_test_dsn() -> str:
    return os.environ.get("TEST_DATABASE_URL", _DEFAULT_TEST_DSN)


def _pg_available(dsn: str) -> bool:
    try:
        conn = psycopg2.connect(dsn)
        conn.close()
        return True
    except Exception:
        return False


# All tables in dependency order (children first for TRUNCATE CASCADE)
_ALL_TABLES = [
    "predictions",
    "prediction_runs",
    "model_registry",
    "computed_features",
    "feature_set_members",
    "feature_sets",
    "feature_definitions",
    "financial_line_items",
    "raw_financial_data",
    "financial_reports",
    "companies",
    "etl_attempts",
    "assessment_jobs",
    "bankruptcy_events",
    "krs_document_versions",
    "krs_documents",
    "krs_entity_versions",
    "krs_entities",
    "krs_registry",
    "krs_sync_log",
    "krs_scan_cursor",
    "krs_scan_runs",
    "scraper_runs",
    "password_reset_tokens",
    "activity_log",
    "batch_progress",
    "batch_rdf_progress",
]

_ALL_SEQUENCES = [
    "seq_krs_document_versions",
    "seq_krs_entity_versions",
    "seq_krs_sync_log",
    "seq_krs_scan_runs",
    "seq_etl_attempts",
]


@pytest.fixture(scope="session")
def pg_dsn():
    """Return the test PostgreSQL DSN. Skip all tests if PG is unavailable."""
    dsn = _get_test_dsn()
    if not _pg_available(dsn):
        pytest.skip("PostgreSQL not available at " + dsn)
    return dsn


import pathlib

_MIGRATIONS_ROOT = pathlib.Path(__file__).resolve().parent.parent / "migrations"

# These use psql client-side `:'var'` substitution (e.g. CREATE ROLE rdf_batch
# LOGIN PASSWORD :'batch_password'). They fail psycopg2 parsing, so operators
# apply them manually. In tests we don't need the scoped roles — mark as
# applied so apply_pending() skips them and the retroactive-insertion check
# doesn't flag pending prediction/dedupe migrations as back-fills.
_ROLE_MIGRATIONS_HANDLED_MANUALLY = {
    "008_scoped_batch_role",
    "009_scoped_api_role",
    "010_reassign_object_ownership_to_rdf_api",
}


def _apply_test_migrations(conn) -> None:
    """Apply SQL migrations directly, skipping psql-only role migrations.

    Mirrors app.db.migrations.apply_pending() but:
      * inserts a stub schema_migrations row for every skipped role migration
        so its version number doesn't trip the retroactive-insertion guard
        when new prediction/NNN files land below it;
      * discovers namespaces in a fixed order (prediction then dedupe) so
        dedupe/* can reference tables created by prediction/*.
    """
    conn.execute("""
        CREATE TABLE IF NOT EXISTS schema_migrations (
            version VARCHAR PRIMARY KEY,
            applied_at TIMESTAMP NOT NULL DEFAULT current_timestamp
        )
    """)
    for namespace in ("prediction", "dedupe"):
        ns_dir = _MIGRATIONS_ROOT / namespace
        if not ns_dir.is_dir():
            continue
        for path in sorted(ns_dir.iterdir()):
            if path.suffix != ".sql":
                continue
            key = f"{namespace}/{path.stem}"
            already = conn.execute(
                "SELECT 1 FROM schema_migrations WHERE version = %s", [key]
            ).fetchone()
            if already:
                continue
            if namespace == "prediction" and path.stem in _ROLE_MIGRATIONS_HANDLED_MANUALLY:
                conn.execute(
                    "INSERT INTO schema_migrations (version) VALUES (%s) "
                    "ON CONFLICT DO NOTHING",
                    [key],
                )
                continue
            sql = path.read_text(encoding="utf-8")
            raw = conn.raw
            prev = raw.autocommit
            raw.autocommit = False
            try:
                with raw.cursor() as cur:
                    cur.execute(sql)
                    cur.execute(
                        "INSERT INTO schema_migrations (version) VALUES (%s)", [key]
                    )
                raw.commit()
            except Exception:
                raw.rollback()
                raise
            finally:
                raw.autocommit = prev


@pytest.fixture(scope="session")
def pg_schema_initialized(pg_dsn):
    """Initialize all schemas once per test session."""
    from unittest.mock import patch
    from app.config import settings
    from app.db import connection as db_conn
    from app.scraper import db as scraper_db
    from app.db import prediction_db
    from app.repositories import krs_repo

    with patch.object(settings, "database_url", pg_dsn):
        db_conn.reset()
        scraper_db._schema_initialized = False
        prediction_db._schema_initialized = False
        krs_repo._schema_initialized = False

        db_conn.connect()
        scraper_db._ensure_schema()
        prediction_db._ensure_schema()
        krs_repo._ensure_schema()
        _apply_test_migrations(db_conn.get_conn())
        db_conn.close()
        db_conn.reset()

        scraper_db._schema_initialized = False
        prediction_db._schema_initialized = False
        krs_repo._schema_initialized = False

    return True


@pytest.fixture(autouse=False)
def clean_pg(pg_dsn, pg_schema_initialized):
    """Truncate all tables before each test. Use this fixture for DB tests."""
    wrapper = make_connection(pg_dsn)
    for table in _ALL_TABLES:
        try:
            wrapper.execute(f"TRUNCATE TABLE {table} CASCADE")
        except Exception:
            wrapper.raw.rollback()
    for seq in _ALL_SEQUENCES:
        try:
            wrapper.execute(f"ALTER SEQUENCE {seq} RESTART WITH 1")
        except Exception:
            wrapper.raw.rollback()
    wrapper.close()
    yield
