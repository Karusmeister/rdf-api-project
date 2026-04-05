"""Fixtures for pipeline tests.

Creates a second database alongside the existing test database so we can
test the dual-DB (scraper + pipeline) topology.
"""
from __future__ import annotations

import os
from unittest.mock import patch

import psycopg2
import pytest


def _pipeline_dsn_from(pg_dsn: str) -> str:
    """Derive rdf_test_pipeline DSN from the main pg_dsn."""
    # Replace the db name with rdf_test_pipeline
    # pg_dsn is of the form postgresql://[user[:pwd]@]host:port/dbname
    if "/" not in pg_dsn.rsplit("@", 1)[-1]:
        return pg_dsn.rstrip("/") + "/rdf_test_pipeline"
    base, _ = pg_dsn.rsplit("/", 1)
    return f"{base}/rdf_test_pipeline"


@pytest.fixture(scope="session")
def pipeline_dsn(pg_dsn):
    """Ensure an rdf_test_pipeline database exists; return its DSN."""
    target = _pipeline_dsn_from(pg_dsn)
    # Connect to the 'postgres' maintenance db
    admin_base = pg_dsn.rsplit("/", 1)[0]
    admin_dsn = f"{admin_base}/postgres"
    try:
        admin = psycopg2.connect(admin_dsn)
    except Exception:
        # Try the default pg_dsn as admin (some setups)
        admin = psycopg2.connect(pg_dsn)
    admin.autocommit = True
    cur = admin.cursor()
    cur.execute("SELECT 1 FROM pg_database WHERE datname = 'rdf_test_pipeline'")
    if cur.fetchone() is None:
        try:
            cur.execute("CREATE DATABASE rdf_test_pipeline")
        except Exception:
            pass
    admin.close()

    # Verify we can connect
    try:
        probe = psycopg2.connect(target)
        probe.close()
    except Exception as e:
        pytest.skip(f"Cannot connect to rdf_test_pipeline: {e}")
    return target


@pytest.fixture
def dual_db(pg_dsn, pipeline_dsn, clean_pg):
    """Reset + initialize schema on BOTH databases for a test.

    `clean_pg` truncates the scraper test DB. We additionally create and
    truncate the pipeline test DB schema.
    """
    from app.config import settings
    from app.db import pipeline_db

    with patch.object(settings, "database_url", pg_dsn), \
         patch.object(settings, "pipeline_database_url", pipeline_dsn):
        pipeline_db.reset()
        pipeline_db.connect()

        # Truncate pipeline tables (best effort).
        conn = pipeline_db.get_conn()
        for table in (
            "population_stats", "pipeline_queue", "pipeline_runs",
            "predictions", "prediction_runs", "model_registry",
            "computed_features", "feature_set_members", "feature_sets",
            "feature_definitions", "financial_line_items",
            "raw_financial_data", "financial_reports", "companies",
            "etl_attempts", "bankruptcy_events",
        ):
            try:
                conn.execute(f"TRUNCATE TABLE {table} CASCADE")
            except Exception:
                conn.rollback() if hasattr(conn, "rollback") else None

        yield {"pg_dsn": pg_dsn, "pipeline_dsn": pipeline_dsn}

        pipeline_db.close()
        pipeline_db.reset()
