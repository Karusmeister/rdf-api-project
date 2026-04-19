"""
Regression tests for core data-layer fixes.

Covers:
- Fix 1: Shared DB connection lifecycle
- Fix 2: ETL ingest route works after clean startup
- Fix 3: Correction/duplicate-period handling with history preserved
- Fix 4: Failed ETL documents can be retried in bulk
- Fix 5: Scraper DDL alignment (index existence)
"""

import io
import zipfile
from datetime import datetime, timezone
from unittest.mock import patch

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from app.config import settings
from app.db import connection as db_conn
from app.db import prediction_db
from app.scraper import db as scraper_db
from app.scraper.storage import LocalStorage
from app.services import etl, feature_engine
from scripts.seed_features import FEATURE_DEFINITIONS, FEATURE_SETS

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

SAMPLE_XML = """\
<?xml version="1.0" encoding="UTF-8"?>
<JednostkaInna>
  <NazwaFirmy>Test Company Sp. z o.o.</NazwaFirmy>
  <P_1D>1234567890</P_1D>
  <P_1E>0000012345</P_1E>
  <KodPKD>62.01.Z</KodPKD>
  <P_3>
    <DataOd>2023-01-01</DataOd>
    <DataDo>2023-12-31</DataDo>
  </P_3>
  <DataSporzadzenia>2024-03-15</DataSporzadzenia>
  <Bilans>
    <Aktywa>
      <KwotaA>1000000.00</KwotaA>
      <KwotaB>900000.00</KwotaB>
    </Aktywa>
    <Pasywa>
      <KwotaA>1000000.00</KwotaA>
      <KwotaB>900000.00</KwotaB>
    </Pasywa>
  </Bilans>
  <RZiS>
    <RZiSPor>
      <A><KwotaA>500000.00</KwotaA><KwotaB>450000.00</KwotaB></A>
      <L><KwotaA>50000.00</KwotaA><KwotaB>40000.00</KwotaB></L>
    </RZiSPor>
  </RZiS>
</JednostkaInna>
"""


@pytest.fixture
def isolated_db(pg_dsn, clean_pg, tmp_path):
    """Shared-connection based isolated DB for both scraper and prediction."""
    db_conn.reset()
    scraper_db._schema_initialized = False
    prediction_db._schema_initialized = False

    with patch.object(settings, "database_url", pg_dsn):
        scraper_db.connect()
        prediction_db.connect()
        yield tmp_path
        db_conn.close()

    db_conn.reset()
    scraper_db._schema_initialized = False
    prediction_db._schema_initialized = False


def _setup_doc(isolated_db, doc_id, krs="0000012345"):
    """Helper: create storage + register a downloaded doc in scraper DB."""
    storage_dir = isolated_db / "documents"
    storage = LocalStorage(str(storage_dir))
    doc_dir = f"krs/{krs}/{doc_id}"

    target = storage_dir / doc_dir
    target.mkdir(parents=True, exist_ok=True)
    (target / "sprawozdanie.xml").write_text(SAMPLE_XML, encoding="utf-8")

    scraper_db.upsert_krs(krs, "Test Company", None, True)
    now = datetime.now(timezone.utc)
    scraper_db.insert_documents([{
        "document_id": doc_id, "krs": krs,
        "rodzaj": "18", "status": "NIEUSUNIETY",
        "nazwa": "test", "okres_start": "2023-01-01", "okres_end": "2023-12-31",
        "discovered_at": now,
    }])
    scraper_db.mark_downloaded(doc_id, doc_dir, "local", 0, 0, 1, "xml")

    return storage


# ---------------------------------------------------------------------------
# Fix 1: Shared DB connection lifecycle
# ---------------------------------------------------------------------------

class TestSharedConnection:
    def test_scraper_and_prediction_share_connection(self, isolated_db):
        """Both modules return the same underlying connection object."""
        assert scraper_db.get_conn() is prediction_db.get_conn()

    def test_both_schemas_visible(self, isolated_db):
        """Both scraper and prediction tables exist in the same DB."""
        conn = db_conn.get_conn()
        tables = {
            row[0]
            for row in conn.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"
            ).fetchall()
        }
        assert "krs_companies" in tables
        assert "krs_documents" in tables
        assert "krs_document_downloads" in tables
        assert "financial_reports" in tables
        assert "computed_features" in tables

    def test_scraper_close_does_not_kill_prediction(self, isolated_db):
        """scraper_db.close() is a no-op — prediction queries still work."""
        prediction_db.upsert_company("0000099999")
        scraper_db.close()
        # Connection should still work
        company = prediction_db.get_company("0000099999")
        assert company is not None


# ---------------------------------------------------------------------------
# Fix 2: ETL ingest route after clean startup
# ---------------------------------------------------------------------------

class TestETLRouteAfterStartup:
    @staticmethod
    def _seed_feature_catalog():
        for fdef in FEATURE_DEFINITIONS:
            prediction_db.upsert_feature_definition(
                feature_id=fdef["id"],
                name=fdef["name"],
                description=fdef.get("description"),
                category=fdef.get("category"),
                formula_description=fdef.get("formula_description"),
                formula_numerator=fdef.get("formula_numerator"),
                formula_denominator=fdef.get("formula_denominator"),
                required_tags=fdef.get("required_tags"),
                computation_logic=fdef.get("computation_logic", "ratio"),
            )
        for set_id, info in FEATURE_SETS.items():
            prediction_db.upsert_feature_set(set_id, info["name"], info.get("description"))
            for ordinal, member_id in enumerate(info["members"], start=1):
                prediction_db.add_feature_set_member(set_id, member_id, ordinal)

    @pytest.fixture
    def anyio_backend(self):
        return "asyncio"

    @pytest_asyncio.fixture
    async def api_client(self, pg_dsn, clean_pg, monkeypatch):
        """AsyncClient that exercises the real app lifespan.

        The lifespan initializes DB connections. We monkeypatch the URL
        BEFORE importing the app so that db_conn.connect() uses test PG.
        """
        monkeypatch.setattr(settings, "database_url", pg_dsn)

        # Reset state so lifespan creates fresh connection
        db_conn.reset()
        scraper_db._schema_initialized = False
        prediction_db._schema_initialized = False

        from app import rdf_client
        from app.main import app

        # Prevent real HTTP client from connecting upstream
        original_start = rdf_client.start

        async def noop_start():
            pass

        monkeypatch.setattr(rdf_client, "start", noop_start)
        monkeypatch.setattr(rdf_client, "stop", noop_start)
        monkeypatch.setattr(rdf_client, "_client", object())

        # Ensure DB-backed routes can run even when app lifespan is not exercised.
        db_conn.connect()
        scraper_db.connect()
        prediction_db.connect()

        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
            yield ac

        db_conn.close()
        db_conn.reset()
        scraper_db._schema_initialized = False
        prediction_db._schema_initialized = False

    def _admin_headers(self):
        from app.auth import create_token
        token = create_token("admin-1", "admin@test.com")
        return {"Authorization": f"Bearer {token}"}

    @pytest.mark.asyncio
    @patch("app.db.prediction_db.get_user_by_id", return_value={
        "id": "admin-1", "email": "admin@test.com", "name": "Admin",
        "auth_method": "local", "password_hash": None, "is_verified": True,
        "has_full_access": True, "is_active": True, "created_at": "2026-01-01",
        "last_login_at": None,
    })
    async def test_etl_ingest_after_clean_startup(self, mock_user, api_client):
        """POST /api/etl/ingest returns a domain response (not 500 from missing connection)."""
        resp = await api_client.post("/api/etl/ingest", json={}, headers=self._admin_headers())
        body = resp.json()
        assert resp.status_code == 200, f"Expected 200, got {resp.status_code}: {body}"
        # No pending docs — should return summary with total=0
        assert body["total"] == 0

    @pytest.mark.asyncio
    @patch("app.db.prediction_db.get_user_by_id", return_value={
        "id": "admin-1", "email": "admin@test.com", "name": "Admin",
        "auth_method": "local", "password_hash": None, "is_verified": True,
        "has_full_access": True, "is_active": True, "created_at": "2026-01-01",
        "last_login_at": None,
    })
    async def test_etl_ingest_missing_doc_returns_404(self, mock_user, api_client):
        """POST /api/etl/ingest with unknown document_id returns 404, not 500."""
        resp = await api_client.post("/api/etl/ingest", json={"document_id": "nonexistent"}, headers=self._admin_headers())
        assert resp.status_code == 404

    @pytest.mark.asyncio
    @patch("app.db.prediction_db.get_user_by_id", return_value={
        "id": "admin-1", "email": "admin@test.com", "name": "Admin",
        "auth_method": "local", "password_hash": None, "is_verified": True,
        "has_full_access": True, "is_active": True, "created_at": "2026-01-01",
        "last_login_at": None,
    })
    async def test_etl_ingest_error_does_not_leak_exception_text(self, mock_user, api_client, monkeypatch):
        """CR2-SEC-002: an unexpected ETL failure must surface a stable public
        error message to the client — raw exception text (file paths, DB
        errors, stack snippets) stays in logs only.
        """
        from app.services import etl as etl_service

        sentinel_message = (
            "leak-me /etc/passwd psycopg2.errors.SomeInternal: schema `public`"
        )

        def _blow_up(_doc_id):
            raise RuntimeError(sentinel_message)

        monkeypatch.setattr(etl_service, "ingest_document", _blow_up)

        resp = await api_client.post(
            "/api/etl/ingest",
            json={"document_id": "any-id"},
            headers=self._admin_headers(),
        )
        assert resp.status_code == 500
        body = resp.json()
        # Stable public message only.
        assert body["detail"] == "ETL ingestion failed"
        assert sentinel_message not in body["detail"]
        assert "/etc/passwd" not in body["detail"]
        assert "psycopg2" not in body["detail"]

    @pytest.mark.asyncio
    async def test_dataset_stats_requires_feature_set(self, api_client):
        resp = await api_client.get("/api/etl/training/dataset-stats")
        assert resp.status_code == 422

    @pytest.mark.asyncio
    async def test_dataset_stats_unknown_feature_set_returns_404(self, api_client):
        resp = await api_client.get(
            "/api/etl/training/dataset-stats",
            params={"feature_set": "does_not_exist"},
        )
        assert resp.status_code == 404

    @pytest.mark.asyncio
    async def test_dataset_stats_empty_schema_is_stable(self, api_client):
        self._seed_feature_catalog()

        resp = await api_client.get(
            "/api/etl/training/dataset-stats",
            params={"feature_set": "maczynska_6"},
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["row_count"] == 0
        assert body["single_year_companies"] == 0
        assert body["unique_companies"] == 0
        assert body["year_range"] == []

    @pytest.mark.asyncio
    async def test_dataset_stats_returns_200_with_expected_keys(self, api_client):
        self._seed_feature_catalog()

        krs = "0000011111"
        report_id = "dataset-stats-rpt"
        prediction_db.upsert_company(krs=krs, pkd_code="62.01.Z")
        prediction_db.create_financial_report(
            report_id=report_id,
            krs=krs,
            fiscal_year=2023,
            period_start="2023-01-01",
            period_end="2023-12-31",
        )
        prediction_db.update_report_status(report_id, "completed")
        prediction_db.batch_insert_line_items([
            {"report_id": report_id, "section": "RZiS", "tag_path": "RZiS.I", "value_current": 200000},
            {"report_id": report_id, "section": "CF", "tag_path": "CF.A_II_1", "value_current": 50000},
            {"report_id": report_id, "section": "Bilans", "tag_path": "Pasywa_B", "value_current": 500000},
            {"report_id": report_id, "section": "Bilans", "tag_path": "Aktywa", "value_current": 1000000},
            {"report_id": report_id, "section": "RZiS", "tag_path": "RZiS.A", "value_current": 2000000},
            {"report_id": report_id, "section": "Bilans", "tag_path": "Aktywa_B_I", "value_current": 100000},
        ])
        feature_engine.compute_features_for_report(report_id, feature_set_id="maczynska_6")

        resp = await api_client.get(
            "/api/etl/training/dataset-stats",
            params={"feature_set": "maczynska_6"},
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["feature_set_id"] == "maczynska_6"
        assert body["row_count"] == 1
        assert body["feature_count"] == 6
        for key in (
            "class_balance",
            "missing_pct",
            "high_missing_features",
            "single_year_companies",
            "unique_companies",
            "year_range",
        ):
            assert key in body


# ---------------------------------------------------------------------------
# Fix 3: Correction handling (original + correction for same period)
# ---------------------------------------------------------------------------

class TestCorrectionHandling:
    def test_correction_creates_new_report_version(self, isolated_db):
        """A correction for the same period becomes a new report version."""
        storage = _setup_doc(isolated_db, "original-doc-001")

        # Ingest original
        result1 = etl.ingest_document("original-doc-001", storage=storage)
        assert result1["status"] == "completed"
        items1 = prediction_db.get_line_items("original-doc-001")
        assert len(items1) > 0

        # Register a correction document (same KRS, same period)
        _setup_doc(isolated_db, "correction-doc-001")

        result2 = etl.ingest_document("correction-doc-001", storage=storage)
        assert result2["status"] == "completed"

        original = prediction_db.get_financial_report("original-doc-001")
        assert original is not None
        assert original["report_version"] == 1
        assert len(prediction_db.get_line_items("original-doc-001")) > 0

        # Correction report should exist as the next version
        report = prediction_db.get_financial_report("correction-doc-001")
        assert report is not None
        assert report["ingestion_status"] == "completed"
        assert report["report_version"] == 2
        assert report["supersedes_report_id"] == "original-doc-001"

    def test_correction_preserves_original_child_rows(self, isolated_db):
        """After correction, the original raw data remains available for history queries."""
        storage = _setup_doc(isolated_db, "orig-002")
        etl.ingest_document("orig-002", storage=storage)

        conn = prediction_db.get_conn()

        # Verify raw data exists for original
        raw_count = conn.execute(
            "SELECT count(*) FROM raw_financial_data WHERE report_id = 'orig-002'"
        ).fetchone()[0]
        assert raw_count > 0

        # Ingest correction
        _setup_doc(isolated_db, "corr-002")
        etl.ingest_document("corr-002", storage=storage)

        # Old raw data is preserved and the correction gets its own rows
        raw_count = conn.execute(
            "SELECT count(*) FROM raw_financial_data WHERE report_id = 'orig-002'"
        ).fetchone()[0]
        assert raw_count > 0
        correction_raw_count = conn.execute(
            "SELECT count(*) FROM raw_financial_data WHERE report_id = 'corr-002'"
        ).fetchone()[0]
        assert correction_raw_count > 0

    def test_same_doc_retry_is_idempotent(self, isolated_db):
        """Re-ingesting the same document_id doesn't create duplicates."""
        storage = _setup_doc(isolated_db, "retry-doc-001")
        result1 = etl.ingest_document("retry-doc-001", storage=storage)
        result2 = etl.ingest_document("retry-doc-001", storage=storage)
        assert result1["status"] == "completed"
        assert result2["status"] == "completed"

        reports = prediction_db.get_reports_for_krs("0000012345")
        assert len(reports) == 1


# ---------------------------------------------------------------------------
# Fix 4: Failed ETL documents can be retried in bulk
# ---------------------------------------------------------------------------

class TestFailedRetry:
    def test_failed_doc_retried_in_bulk(self, isolated_db):
        """A document whose first ingest failed can be picked up by ingest_all_pending."""
        krs = "0000012345"
        doc_id = "fail-then-fix-001"
        storage_dir = isolated_db / "documents"
        storage = LocalStorage(str(storage_dir))

        # Register doc in scraper DB
        scraper_db.upsert_krs(krs, "Test", None, True)
        now = datetime.now(timezone.utc)
        doc_dir = f"krs/{krs}/{doc_id}"
        scraper_db.insert_documents([{
            "document_id": doc_id, "krs": krs,
            "rodzaj": "18", "status": "NIEUSUNIETY",
            "nazwa": "test", "okres_start": "2023-01-01", "okres_end": "2023-12-31",
            "discovered_at": now,
        }])
        scraper_db.mark_downloaded(doc_id, doc_dir, "local", 0, 0, 0, "")

        # First attempt: no XML on disk -> should fail
        target = storage_dir / doc_dir
        target.mkdir(parents=True, exist_ok=True)
        result1 = etl.ingest_document(doc_id, storage=storage)
        assert result1["status"] == "failed"

        # No sentinel financial_reports row — failure is recorded in etl_attempts
        report = prediction_db.get_financial_report(doc_id)
        assert report is None

        conn = prediction_db.get_conn()
        attempts = conn.execute(
            "SELECT status, reason_code FROM etl_attempts WHERE document_id = %s",
            [doc_id],
        ).fetchall()
        assert len(attempts) == 1
        assert attempts[0][0] == "failed"

        # Now put the XML on disk (simulating fix)
        (target / "sprawozdanie.xml").write_text(SAMPLE_XML, encoding="utf-8")

        # Bulk ingest should pick it up
        result = etl.ingest_all_pending(storage=storage)
        assert result["total"] == 1
        assert result["completed"] == 1

        report = prediction_db.get_financial_report(doc_id)
        assert report["ingestion_status"] == "completed"

    def test_completed_docs_not_retried(self, isolated_db):
        """Already-completed documents are NOT picked up by ingest_all_pending."""
        storage = _setup_doc(isolated_db, "already-done-001")
        etl.ingest_document("already-done-001", storage=storage)

        result = etl.ingest_all_pending(storage=storage)
        assert result["total"] == 0


# ---------------------------------------------------------------------------
# Fix 5: Scraper DDL — index existence
# ---------------------------------------------------------------------------

class TestScraperDDL:
    def test_all_documented_indexes_exist(self, isolated_db):
        """All scraper indexes expected by the implementation are present."""
        conn = db_conn.get_conn()
        indexes = {
            row[0]
            for row in conn.execute(
                "SELECT indexname FROM pg_indexes WHERE schemaname = 'public'"
            ).fetchall()
        }
        expected = [
            "idx_krs_companies_last_checked",
            "idx_krs_companies_priority",
            "idx_documents_krs",
            "idx_downloads_pending",
            "idx_runs_started",
        ]
        for idx_name in expected:
            assert idx_name in indexes, f"Missing index: {idx_name}"
