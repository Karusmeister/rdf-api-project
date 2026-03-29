"""Tests for PR1: append-only schema DDL + backfill.

Verifies that:
- New tables and views are created at startup.
- Backfill populates version tables from legacy tables.
- Backfill is idempotent (second run inserts zero rows).
- Views return correct current-state rows.
"""
from pathlib import Path

import pytest
from unittest.mock import patch

from app.config import settings
from app.db import connection as db_conn
from app.db import prediction_db
from app.repositories import krs_repo
from app.scraper import db as scraper_db


@pytest.fixture(autouse=True)
def isolated_db(tmp_path):
    """Isolated DuckDB for each test."""
    db_path = str(tmp_path / "test.duckdb")
    db_conn.reset()
    krs_repo._schema_initialized = False
    scraper_db._schema_initialized = False
    prediction_db._schema_initialized = False
    with patch.object(settings, "scraper_db_path", db_path):
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
# DDL presence
# ---------------------------------------------------------------------------

class TestDDLPresence:
    def test_krs_entity_versions_table_exists(self):
        conn = db_conn.get_conn()
        tables = {
            row[0] for row in conn.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
            ).fetchall()
        }
        assert "krs_entity_versions" in tables

    def test_krs_document_versions_table_exists(self):
        conn = db_conn.get_conn()
        tables = {
            row[0] for row in conn.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
            ).fetchall()
        }
        assert "krs_document_versions" in tables

    def test_etl_attempts_table_exists(self):
        conn = db_conn.get_conn()
        tables = {
            row[0] for row in conn.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
            ).fetchall()
        }
        assert "etl_attempts" in tables

    def test_krs_entities_current_view_exists(self):
        conn = db_conn.get_conn()
        # View should be queryable even when empty
        rows = conn.execute("SELECT * FROM krs_entities_current").fetchall()
        assert rows == []

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
# Backfill
# ---------------------------------------------------------------------------

class TestBackfill:
    def _run_backfill(self):
        """Run the Python backfill using the same hash logic as runtime."""
        # Import the backfill module (filename starts with digit so use importlib)
        import importlib.util
        spec = importlib.util.spec_from_file_location(
            "backfill", Path("scripts/db_migrations/001_append_only_backfill.py").resolve(),
        )
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        conn = db_conn.get_conn()
        mod._backfill_entities(conn)
        mod._backfill_documents(conn)

    def test_entity_backfill(self):
        conn = db_conn.get_conn()
        # Insert legacy entity
        conn.execute("""
            INSERT INTO krs_entities (krs, name, source, synced_at)
            VALUES ('0000000001', 'Test Sp. z o.o.', 'ms_gov', '2026-01-01 00:00:00')
        """)
        self._run_backfill()
        rows = conn.execute(
            "SELECT krs, name, is_current, change_reason FROM krs_entity_versions"
        ).fetchall()
        assert len(rows) == 1
        assert rows[0][0] == "0000000001"
        assert rows[0][2] is True  # is_current
        assert rows[0][3] == "bootstrap_from_krs_entities"

    def test_document_backfill(self):
        conn = db_conn.get_conn()
        # Insert legacy document
        conn.execute("""
            INSERT INTO krs_documents
                (document_id, krs, rodzaj, status, discovered_at)
            VALUES ('doc-001', '0000000001', 'sprawozdanie', 'aktywny', '2026-01-01 00:00:00')
        """)
        self._run_backfill()
        rows = conn.execute(
            "SELECT document_id, version_no, is_current, change_reason FROM krs_document_versions"
        ).fetchall()
        assert len(rows) == 1
        assert rows[0][0] == "doc-001"
        assert rows[0][1] == 1  # version_no
        assert rows[0][2] is True  # is_current
        assert rows[0][3] == "bootstrap_from_krs_documents"

    def test_backfill_is_idempotent(self):
        conn = db_conn.get_conn()
        conn.execute("""
            INSERT INTO krs_entities (krs, name, source, synced_at)
            VALUES ('0000000002', 'Idempotent SA', 'ms_gov', '2026-01-01 00:00:00')
        """)
        self._run_backfill()
        self._run_backfill()  # second run
        count = conn.execute(
            "SELECT count(*) FROM krs_entity_versions WHERE krs = '0000000002'"
        ).fetchone()[0]
        assert count == 1

    def test_backfill_entities_appear_in_current_view(self):
        conn = db_conn.get_conn()
        conn.execute("""
            INSERT INTO krs_entities (krs, name, source, synced_at)
            VALUES ('0000000003', 'View SA', 'ms_gov', '2026-01-01 00:00:00')
        """)
        self._run_backfill()
        rows = conn.execute(
            "SELECT krs, name FROM krs_entities_current WHERE krs = '0000000003'"
        ).fetchall()
        assert len(rows) == 1
        assert rows[0][1] == "View SA"

    def test_backfill_documents_appear_in_current_view(self):
        conn = db_conn.get_conn()
        conn.execute("""
            INSERT INTO krs_documents
                (document_id, krs, rodzaj, status, discovered_at)
            VALUES ('doc-002', '0000000003', 'sprawozdanie', 'aktywny', '2026-01-01 00:00:00')
        """)
        self._run_backfill()
        rows = conn.execute(
            "SELECT document_id, krs FROM krs_documents_current WHERE document_id = 'doc-002'"
        ).fetchall()
        assert len(rows) == 1

    def test_multiple_entities_backfill(self):
        conn = db_conn.get_conn()
        for i in range(1, 6):
            conn.execute(
                "INSERT INTO krs_entities (krs, name, source) VALUES (?, ?, 'ms_gov')",
                [f"000000000{i}", f"Company {i}"],
            )
        self._run_backfill()
        count = conn.execute("SELECT count(*) FROM krs_entity_versions WHERE is_current = true").fetchone()[0]
        assert count == 5
        # No multi-current per krs
        multi = conn.execute("""
            SELECT krs, count(*) FROM krs_entity_versions
            WHERE is_current = true GROUP BY krs HAVING count(*) > 1
        """).fetchall()
        assert len(multi) == 0


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


# ---------------------------------------------------------------------------
# Regression: backfill hash matches runtime hash (Fix 1)
# ---------------------------------------------------------------------------

class TestBackfillHashConsistency:
    """After Python backfill, the SAME upsert must NOT create a new version."""

    def _run_backfill(self):
        import importlib.util
        spec = importlib.util.spec_from_file_location(
            "backfill", Path("scripts/db_migrations/001_append_only_backfill.py").resolve(),
        )
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        conn = db_conn.get_conn()
        mod._backfill_entities(conn)
        mod._backfill_documents(conn)

    def test_entity_backfill_then_same_upsert_no_new_version(self):
        conn = db_conn.get_conn()
        conn.execute("""
            INSERT INTO krs_entities (krs, name, legal_form, source, synced_at)
            VALUES ('0000099901', 'Hash Test Corp', 'SA', 'ms_gov', '2026-01-01 00:00:00')
        """)
        self._run_backfill()
        before = conn.execute(
            "SELECT count(*) FROM krs_entity_versions WHERE krs = '0000099901'"
        ).fetchone()[0]
        assert before == 1

        # Same data via runtime upsert
        krs_repo.upsert_entity(krs="0000099901", name="Hash Test Corp", legal_form="SA")

        after = conn.execute(
            "SELECT count(*) FROM krs_entity_versions WHERE krs = '0000099901'"
        ).fetchone()[0]
        assert after == 1, f"Expected 1 version (no-op), got {after}"

    def test_document_backfill_then_same_insert_no_new_version(self):
        from datetime import datetime, timezone
        conn = db_conn.get_conn()
        conn.execute("""
            INSERT INTO krs_documents
                (document_id, krs, rodzaj, status, discovered_at)
            VALUES ('hash-doc-001', '0000099901', 'sprawozdanie', 'aktywny', '2026-01-01 00:00:00')
        """)
        self._run_backfill()
        before = conn.execute(
            "SELECT count(*) FROM krs_document_versions WHERE document_id = 'hash-doc-001'"
        ).fetchone()[0]
        assert before == 1

        # Same data via runtime insert
        scraper_db.insert_documents([{
            "document_id": "hash-doc-001", "krs": "0000099901",
            "rodzaj": "sprawozdanie", "status": "aktywny",
            "discovered_at": datetime(2026, 1, 1, tzinfo=timezone.utc),
        }])

        after = conn.execute(
            "SELECT count(*) FROM krs_document_versions WHERE document_id = 'hash-doc-001'"
        ).fetchone()[0]
        assert after == 1, f"Expected 1 version (no-op), got {after}"
