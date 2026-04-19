"""Tests for append-only schema DDL.

Verifies that:
- New tables and views are created at startup.
- Views return correct current-state rows.
"""

import pytest
from unittest.mock import patch

from app.config import settings
from app.db import connection as db_conn
from app.db import prediction_db
from app.repositories import krs_repo
from app.scraper import db as scraper_db


@pytest.fixture(autouse=True)
def isolated_db(pg_dsn, clean_pg):
    """Isolated PostgreSQL schema for each test.

    clean_pg depends on pg_schema_initialized which has already run the full
    bootstrap + dedupe migrations once per session, so krs_companies exists.
    """
    db_conn.reset()
    krs_repo._schema_initialized = False
    scraper_db._schema_initialized = False
    prediction_db._schema_initialized = False
    with patch.object(settings, "database_url", pg_dsn):
        db_conn.connect()
        krs_repo._ensure_schema()
        scraper_db._ensure_schema()
        prediction_db._ensure_schema()
        yield
        db_conn.close()
    db_conn.reset()
    krs_repo._schema_initialized = False
    scraper_db._schema_initialized = False
    prediction_db._schema_initialized = False


# ---------------------------------------------------------------------------
# DDL presence (post-dedupe SCHEMA_DEDUPE_PLAN #2: entity history collapsed
# into krs_companies; doc history still lives on krs_document_versions until
# SCHEMA_DEDUPE_PLAN #1 lands).
# ---------------------------------------------------------------------------

class TestDDLPresence:
    def test_krs_companies_table_exists(self):
        conn = db_conn.get_conn()
        tables = {
            row[0] for row in conn.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"
            ).fetchall()
        }
        assert "krs_companies" in tables

    def test_krs_document_versions_table_exists(self):
        conn = db_conn.get_conn()
        tables = {
            row[0] for row in conn.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"
            ).fetchall()
        }
        assert "krs_document_versions" in tables

    def test_etl_attempts_table_exists(self):
        conn = db_conn.get_conn()
        tables = {
            row[0] for row in conn.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"
            ).fetchall()
        }
        assert "etl_attempts" in tables

    def test_krs_documents_current_view_exists(self):
        conn = db_conn.get_conn()
        rows = conn.execute("SELECT * FROM krs_documents_current").fetchall()
        assert rows == []

    def test_latest_successful_financial_reports_view_exists(self):
        conn = db_conn.get_conn()
        rows = conn.execute("SELECT * FROM latest_successful_financial_reports").fetchall()
        assert rows == []

    def test_schema_init_is_idempotent(self):
        """Second schema init should not raise."""
        krs_repo._schema_initialized = False
        scraper_db._schema_initialized = False
        prediction_db._schema_initialized = False
        krs_repo._ensure_schema()
        scraper_db._ensure_schema()
        prediction_db._ensure_schema()



# ---------------------------------------------------------------------------
# latest_successful_financial_reports view
# ---------------------------------------------------------------------------

class TestLatestSuccessfulReportsView:
    def test_only_completed_reports_appear(self):
        conn = db_conn.get_conn()
        conn.execute("""
            INSERT INTO financial_reports
                (id, logical_key, report_version, krs, data_source_id, report_type,
                 fiscal_year, period_start, period_end, ingestion_status)
            VALUES
                ('rpt-ok', 'k1', 1, '0000000001', 'KRS', 'annual', 2023, '2023-01-01', '2023-12-31', 'completed'),
                ('rpt-fail', 'k2', 1, '0000000002', 'KRS', 'annual', 2023, '2023-01-01', '2023-12-31', 'failed')
        """)
        rows = conn.execute("SELECT id FROM latest_successful_financial_reports").fetchall()
        ids = [r[0] for r in rows]
        assert "rpt-ok" in ids
        assert "rpt-fail" not in ids

    def test_latest_version_wins(self):
        conn = db_conn.get_conn()
        conn.execute("""
            INSERT INTO financial_reports
                (id, logical_key, report_version, krs, data_source_id, report_type,
                 fiscal_year, period_start, period_end, ingestion_status)
            VALUES
                ('rpt-v1', 'k3', 1, '0000000003', 'KRS', 'annual', 2022, '2022-01-01', '2022-12-31', 'completed'),
                ('rpt-v2', 'k3', 2, '0000000003', 'KRS', 'annual', 2022, '2022-01-01', '2022-12-31', 'completed')
        """)
        rows = conn.execute("SELECT id FROM latest_successful_financial_reports WHERE logical_key = 'k3'").fetchall()
        assert len(rows) == 1
        assert rows[0][0] == "rpt-v2"
