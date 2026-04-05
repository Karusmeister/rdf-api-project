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


@pytest.fixture(scope="session")
def pg_schema_initialized(pg_dsn):
    """Initialize all schemas once per test session."""
    from unittest.mock import patch
    from app.config import settings
    from app.db import connection as db_conn
    from app.db import migrations as db_migrations
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
        # CR2-OPS-004: apply versioned migrations after bootstrap tables exist
        # so tests see the same schema shape (columns, FKs) as production.
        db_migrations.apply_pending(db_conn.get_conn())
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
