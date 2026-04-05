"""Tests for the XML-to-PostgreSQL ETL pipeline."""

import io
import json
import os
import zipfile
from datetime import datetime, timezone
from unittest.mock import patch

import pytest

from app.config import settings
from app.db import connection as db_conn
from app.db import prediction_db
from app.scraper import db as scraper_db
from app.scraper.storage import LocalStorage
from app.services import etl

# ---------------------------------------------------------------------------
# Sample XML fixture (minimal JednostkaInna)
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
      <Aktywa_A>
        <KwotaA>600000.00</KwotaA>
        <KwotaB>550000.00</KwotaB>
      </Aktywa_A>
      <Aktywa_B>
        <KwotaA>400000.00</KwotaA>
        <KwotaB>350000.00</KwotaB>
        <Aktywa_B_I>
          <KwotaA>100000.00</KwotaA>
          <KwotaB>80000.00</KwotaB>
        </Aktywa_B_I>
        <Aktywa_B_III>
          <KwotaA>200000.00</KwotaA>
          <KwotaB>180000.00</KwotaB>
          <Aktywa_B_III_1>
            <KwotaA>200000.00</KwotaA>
            <KwotaB>180000.00</KwotaB>
            <Aktywa_B_III_1_C>
              <KwotaA>150000.00</KwotaA>
              <KwotaB>120000.00</KwotaB>
            </Aktywa_B_III_1_C>
          </Aktywa_B_III_1>
        </Aktywa_B_III>
      </Aktywa_B>
    </Aktywa>
    <Pasywa>
      <KwotaA>1000000.00</KwotaA>
      <KwotaB>900000.00</KwotaB>
      <Pasywa_A>
        <KwotaA>500000.00</KwotaA>
        <KwotaB>450000.00</KwotaB>
        <Pasywa_A_VI>
          <KwotaA>50000.00</KwotaA>
          <KwotaB>40000.00</KwotaB>
        </Pasywa_A_VI>
      </Pasywa_A>
      <Pasywa_B>
        <KwotaA>500000.00</KwotaA>
        <KwotaB>450000.00</KwotaB>
        <Pasywa_B_III>
          <KwotaA>300000.00</KwotaA>
          <KwotaB>280000.00</KwotaB>
        </Pasywa_B_III>
      </Pasywa_B>
    </Pasywa>
  </Bilans>
  <RZiS>
    <RZiSPor>
      <A>
        <KwotaA>2000000.00</KwotaA>
        <KwotaB>1800000.00</KwotaB>
      </A>
      <B>
        <KwotaA>1700000.00</KwotaA>
        <KwotaB>1550000.00</KwotaB>
      </B>
      <C>
        <KwotaA>300000.00</KwotaA>
        <KwotaB>250000.00</KwotaB>
      </C>
      <F>
        <KwotaA>200000.00</KwotaA>
        <KwotaB>180000.00</KwotaB>
      </F>
      <I>
        <KwotaA>80000.00</KwotaA>
        <KwotaB>70000.00</KwotaB>
      </I>
      <L>
        <KwotaA>50000.00</KwotaA>
        <KwotaB>40000.00</KwotaB>
      </L>
    </RZiSPor>
  </RZiS>
  <RachPrzeplywow>
    <PrzeplywyPosr>
      <PrzeplywyA>
        <KwotaA>50000.00</KwotaA>
        <KwotaB>40000.00</KwotaB>
      </PrzeplywyA>
    </PrzeplywyPosr>
  </RachPrzeplywow>
</JednostkaInna>
"""


def _make_zip(xml_content: str) -> bytes:
    """Create a ZIP file containing the XML as sprawozdanie.xml."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("sprawozdanie.xml", xml_content)
    return buf.getvalue()


@pytest.fixture
def isolated_db(tmp_path, pg_dsn, clean_pg):
    """Set up isolated PostgreSQL DB for both scraper and prediction tables."""
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


@pytest.fixture
def storage_with_xml(isolated_db):
    """Set up local storage with a sample extracted document."""
    storage_dir = isolated_db / "documents"
    storage = LocalStorage(str(storage_dir))

    krs = "0000012345"
    doc_id = "test-doc-001"
    doc_dir = f"krs/{krs}/{doc_id}"

    # Write XML directly (simulates what save_extracted does)
    target = storage_dir / doc_dir
    target.mkdir(parents=True, exist_ok=True)
    (target / "sprawozdanie.xml").write_text(SAMPLE_XML, encoding="utf-8")

    # Register in scraper DB (KRS first due to FK constraint)
    scraper_db.upsert_krs(krs, "Test Company Sp. z o.o.", "SP_Z_OO", True)

    now = datetime.now(timezone.utc)
    scraper_db.insert_documents([{
        "document_id": doc_id, "krs": krs,
        "rodzaj": "18", "status": "NIEUSUNIETY",
        "nazwa": "test", "okres_start": "2023-01-01", "okres_end": "2023-12-31",
        "discovered_at": now,
    }])
    scraper_db.mark_downloaded(doc_id, doc_dir, "local", 0, 0, 1, "xml")

    return {"storage": storage, "krs": krs, "document_id": doc_id, "doc_dir": doc_dir}


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestIngestDocument:
    def test_ingest_creates_financial_report(self, storage_with_xml):
        ctx = storage_with_xml
        result = etl.ingest_document(ctx["document_id"], storage=ctx["storage"])

        assert result["status"] == "completed"
        assert result["krs"] == ctx["krs"]
        assert result["fiscal_year"] == 2023
        assert result["line_items_count"] > 0

        report = prediction_db.get_financial_report(ctx["document_id"])
        assert report is not None
        assert report["krs"] == ctx["krs"]
        assert report["ingestion_status"] == "completed"

    def test_ingest_creates_company(self, storage_with_xml):
        ctx = storage_with_xml
        etl.ingest_document(ctx["document_id"], storage=ctx["storage"])

        company = prediction_db.get_company(ctx["krs"])
        assert company is not None
        assert company["nip"] == "1234567890"
        assert company["pkd_code"] == "62.01.Z"

    def test_ingest_creates_line_items(self, storage_with_xml):
        ctx = storage_with_xml
        etl.ingest_document(ctx["document_id"], storage=ctx["storage"])

        items = prediction_db.get_line_items(ctx["document_id"])
        assert len(items) > 0

        # Check we have all sections
        sections = {i["section"] for i in items}
        assert "Bilans" in sections
        assert "RZiS" in sections

        # Check specific values
        aktywa = [i for i in items if i["tag_path"] == "Aktywa" and i["section"] == "Bilans"]
        assert len(aktywa) == 1
        assert aktywa[0]["value_current"] == 1000000.0
        assert aktywa[0]["value_previous"] == 900000.0

    def test_ingest_creates_raw_financial_data(self, storage_with_xml):
        ctx = storage_with_xml
        etl.ingest_document(ctx["document_id"], storage=ctx["storage"])

        conn = prediction_db.get_conn()
        rows = conn.execute(
            "SELECT section FROM raw_financial_data WHERE report_id = %s",
            [ctx["document_id"]],
        ).fetchall()
        sections = {r[0] for r in rows}
        assert "balance_sheet" in sections
        assert "income_statement" in sections

    def test_ingest_bilans_line_items_detail(self, storage_with_xml):
        ctx = storage_with_xml
        etl.ingest_document(ctx["document_id"], storage=ctx["storage"])

        items = prediction_db.get_line_items(ctx["document_id"], section="Bilans")
        tag_map = {i["tag_path"]: i for i in items}

        assert "Aktywa" in tag_map
        assert "Aktywa_A" in tag_map
        assert "Aktywa_B" in tag_map
        assert "Pasywa" in tag_map
        assert "Pasywa_A" in tag_map
        assert "Pasywa_B" in tag_map
        assert "Pasywa_B_III" in tag_map

        assert tag_map["Pasywa_B_III"]["value_current"] == 300000.0

    def test_ingest_rzis_line_items(self, storage_with_xml):
        ctx = storage_with_xml
        etl.ingest_document(ctx["document_id"], storage=ctx["storage"])

        items = prediction_db.get_line_items(ctx["document_id"], section="RZiS")
        tag_map = {i["tag_path"]: i for i in items}

        assert "RZiS.A" in tag_map
        assert "RZiS.L" in tag_map
        assert tag_map["RZiS.A"]["value_current"] == 2000000.0
        assert tag_map["RZiS.L"]["value_current"] == 50000.0

    def test_ingest_idempotent(self, storage_with_xml):
        """Re-running on same document doesn't create duplicates."""
        ctx = storage_with_xml
        result1 = etl.ingest_document(ctx["document_id"], storage=ctx["storage"])
        result2 = etl.ingest_document(ctx["document_id"], storage=ctx["storage"])

        # Both should succeed (ON CONFLICT DO NOTHING / DO UPDATE)
        assert result1["status"] == "completed"
        assert result2["status"] == "completed"

        # Only one report should exist
        reports = prediction_db.get_reports_for_krs(ctx["krs"])
        assert len(reports) == 1

    def test_ingest_missing_document(self, isolated_db):
        with pytest.raises(ValueError, match="not found"):
            etl.ingest_document("nonexistent-doc")

    def test_ingest_stores_schema_code_on_report(self, storage_with_xml):
        """Ingesting a JednostkaInna XML must persist schema_code='SFJINZ' on the report."""
        ctx = storage_with_xml
        etl.ingest_document(ctx["document_id"], storage=ctx["storage"])

        report = prediction_db.get_financial_report(ctx["document_id"])
        assert report is not None
        assert report["schema_code"] == "SFJINZ"

    def test_ingest_stores_schema_code_on_line_items(self, storage_with_xml):
        """All line items must carry the detected schema_code."""
        ctx = storage_with_xml
        etl.ingest_document(ctx["document_id"], storage=ctx["storage"])

        items = prediction_db.get_line_items(ctx["document_id"])
        assert len(items) > 0
        for item in items:
            assert item["schema_code"] == "SFJINZ", f"Missing schema_code on tag_path={item['tag_path']}"

    def test_ingest_not_downloaded(self, isolated_db):
        scraper_db.upsert_krs("0000099999", None, None, True)
        now = datetime.now(timezone.utc)
        scraper_db.insert_documents([{
            "document_id": "doc-not-dl", "krs": "0000099999",
            "rodzaj": "18", "status": "NIEUSUNIETY",
            "discovered_at": now,
        }])

        with pytest.raises(ValueError, match="not yet downloaded"):
            etl.ingest_document("doc-not-dl")


class TestReIngest:
    def test_re_ingest_appends_new_extraction_version(self, storage_with_xml):
        ctx = storage_with_xml
        result1 = etl.ingest_document(ctx["document_id"], storage=ctx["storage"])
        count1 = result1["line_items_count"]
        items1 = prediction_db.get_line_items(ctx["document_id"])
        version1 = max(i["extraction_version"] for i in items1)

        result2 = etl.re_ingest(ctx["document_id"], storage=ctx["storage"])
        assert result2["status"] == "completed"
        assert result2["line_items_count"] == count1
        items2 = prediction_db.get_line_items(ctx["document_id"])
        version2 = max(i["extraction_version"] for i in items2)
        assert version2 == version1 + 1

        # Still only one report
        reports = prediction_db.get_reports_for_krs(ctx["krs"])
        assert len(reports) == 1
        conn = prediction_db.get_conn()
        history_versions = conn.execute("""
            SELECT count(DISTINCT extraction_version)
            FROM financial_line_items
            WHERE report_id = %s
        """, [ctx["document_id"]]).fetchone()[0]
        assert history_versions == 2


class TestIngestAllPending:
    def test_ingest_all_pending(self, storage_with_xml):
        ctx = storage_with_xml
        result = etl.ingest_all_pending(storage=ctx["storage"])

        assert result["total"] == 1
        assert result["completed"] == 1
        assert result["failed"] == 0

    def test_ingest_all_pending_skips_already_ingested(self, storage_with_xml):
        ctx = storage_with_xml
        etl.ingest_document(ctx["document_id"], storage=ctx["storage"])

        result = etl.ingest_all_pending(storage=ctx["storage"])
        assert result["total"] == 0


class TestEtlAttempts:
    """PR4: ETL failure tracking via etl_attempts, no sentinel reports."""

    def test_no_xml_records_etl_attempt(self, isolated_db):
        """no_xml_found should create etl_attempts row, NOT a sentinel report."""
        # Set up a document with empty storage dir (no XML)
        storage_dir = isolated_db / "documents"
        storage = LocalStorage(str(storage_dir))
        krs = "0000099001"
        doc_id = "doc-no-xml"
        doc_dir = f"krs/{krs}/{doc_id}"
        (storage_dir / doc_dir).mkdir(parents=True, exist_ok=True)

        scraper_db.upsert_krs(krs, "NoXml Corp", None, True)
        now = datetime.now(timezone.utc)
        scraper_db.insert_documents([{
            "document_id": doc_id, "krs": krs,
            "rodzaj": "18", "status": "NIEUSUNIETY",
            "discovered_at": now,
        }])
        scraper_db.mark_downloaded(doc_id, doc_dir, "local", 0, 0, 0, "")

        result = etl.ingest_document(doc_id, storage=storage)
        assert result["status"] == "failed"
        assert result["error"] == "no_xml_found"

        # etl_attempts should have a row
        conn = prediction_db.get_conn()
        attempts = conn.execute(
            "SELECT status, reason_code FROM etl_attempts WHERE document_id = %s",
            [doc_id],
        ).fetchall()
        assert len(attempts) == 1
        assert attempts[0][0] == "failed"
        assert attempts[0][1] == "no_xml_found"

        # NO sentinel financial_reports should exist
        sentinel_count = conn.execute("""
            SELECT count(*) FROM financial_reports
            WHERE source_document_id = %s AND fiscal_year = 0
        """, [doc_id]).fetchone()[0]
        assert sentinel_count == 0

    def test_parse_error_records_etl_attempt(self, isolated_db):
        """parse_error should create etl_attempts row, NOT a sentinel report.

        Also enforces CR3-SEC-002: the returned payload must use the stable
        `parse_error` code and MUST NOT carry the raw exception text. Full
        detail stays in logs and the etl_attempts audit row.
        """
        storage_dir = isolated_db / "documents"
        storage = LocalStorage(str(storage_dir))
        krs = "0000099002"
        doc_id = "doc-bad-xml"
        doc_dir = f"krs/{krs}/{doc_id}"
        target = storage_dir / doc_dir
        target.mkdir(parents=True, exist_ok=True)
        (target / "bad.xml").write_text("<<<TOTALLY BROKEN XML>>>", encoding="utf-8")

        scraper_db.upsert_krs(krs, "BadXml Corp", None, True)
        now = datetime.now(timezone.utc)
        scraper_db.insert_documents([{
            "document_id": doc_id, "krs": krs,
            "rodzaj": "18", "status": "NIEUSUNIETY",
            "discovered_at": now,
        }])
        scraper_db.mark_downloaded(doc_id, doc_dir, "local", 0, 0, 1, "xml")

        result = etl.ingest_document(doc_id, storage=storage)
        assert result["status"] == "failed"
        # CR3-SEC-002: stable error code only, no ElementTree/parser internals.
        assert result["error"] == "parse_error"
        assert "line" not in str(result)  # parser errors often embed "line N, col M"
        assert "<<<" not in str(result)
        assert "ParseError" not in str(result)

        conn = prediction_db.get_conn()
        attempts = conn.execute(
            "SELECT status, reason_code, error_message FROM etl_attempts WHERE document_id = %s",
            [doc_id],
        ).fetchall()
        assert len(attempts) == 1
        assert attempts[0][0] == "failed"
        assert attempts[0][1] == "parse_error"
        # The raw exception text IS allowed in the audit table (operator-only);
        # only the API return contract is sanitized.
        assert attempts[0][2] is not None

        sentinel_count = conn.execute("""
            SELECT count(*) FROM financial_reports
            WHERE source_document_id = %s AND fiscal_year = 0
        """, [doc_id]).fetchone()[0]
        assert sentinel_count == 0

    def test_ingest_all_pending_sanitizes_unexpected_errors(self, isolated_db, monkeypatch):
        """CR3-SEC-002: bulk ingestion must surface stable error codes in the
        aggregated `errors[]` list — raw exception text is for logs, not for
        clients. The old implementation stuffed `str(e)` directly into the
        returned dict.
        """
        import sys

        storage_dir = isolated_db / "documents"
        storage = LocalStorage(str(storage_dir))
        krs = "0000099099"
        doc_id = "doc-boom"

        scraper_db.upsert_krs(krs, "Boom Corp", None, True)
        now = datetime.now(timezone.utc)
        scraper_db.insert_documents([{
            "document_id": doc_id, "krs": krs,
            "rodzaj": "18", "status": "NIEUSUNIETY",
            "discovered_at": now,
        }])
        scraper_db.mark_downloaded(doc_id, f"krs/{krs}/{doc_id}", "local", 0, 0, 1, "xml")

        # Force an unexpected RuntimeError from the per-document path so we
        # land in the bulk except-branch, which is the one that used to
        # stringify the exception directly.
        sentinel = "LEAK: /etc/shadow postgres://user:pass@host/db"

        def _boom(*args, **kwargs):
            raise RuntimeError(sentinel)

        monkeypatch.setattr(etl, "ingest_document", _boom)

        result = etl.ingest_all_pending(storage=storage)

        assert result["failed"] == 1
        assert result["errors"], "bulk result should record the failure"
        err = result["errors"][0]
        assert err["document_id"] == doc_id
        # Stable code only — no raw exception text allowed in the payload.
        assert err["error"] == "unexpected_error"
        assert err.get("error_type") == "RuntimeError"
        # Negative assertions — the leaky substrings must not appear anywhere
        # in the returned object.
        flat = str(result)
        assert sentinel not in flat
        assert "/etc/shadow" not in flat
        assert "postgres://" not in flat

    def test_success_records_etl_attempt_with_timestamps(self, storage_with_xml):
        """Successful ingest should have started_at < finished_at."""
        ctx = storage_with_xml
        result = etl.ingest_document(ctx["document_id"], storage=ctx["storage"])
        assert result["status"] == "completed"

        conn = prediction_db.get_conn()
        row = conn.execute(
            "SELECT status, report_id, started_at, finished_at FROM etl_attempts WHERE document_id = %s",
            [ctx["document_id"]],
        ).fetchone()
        assert row[0] == "completed"
        assert row[1] == ctx["document_id"]
        assert row[2] is not None  # started_at
        assert row[3] is not None  # finished_at

    def test_not_downloaded_records_skipped_attempt(self, isolated_db):
        """Early exit for not-downloaded doc should record skipped attempt."""
        scraper_db.upsert_krs("0000099003", None, None, True)
        now = datetime.now(timezone.utc)
        scraper_db.insert_documents([{
            "document_id": "doc-skip-dl", "krs": "0000099003",
            "rodzaj": "18", "status": "NIEUSUNIETY",
            "discovered_at": now,
        }])

        with pytest.raises(ValueError, match="not yet downloaded"):
            etl.ingest_document("doc-skip-dl")

        conn = prediction_db.get_conn()
        row = conn.execute(
            "SELECT status, reason_code FROM etl_attempts WHERE document_id = 'doc-skip-dl'"
        ).fetchone()
        assert row is not None
        assert row[0] == "skipped"
        assert row[1] == "not_downloaded"

    def test_unexpected_error_finalises_attempt(self, storage_with_xml):
        """If an unexpected exception occurs mid-ingest, attempt must be finalised."""
        ctx = storage_with_xml
        with patch.object(prediction_db, "create_financial_report", side_effect=RuntimeError("boom")):
            with pytest.raises(RuntimeError, match="boom"):
                etl.ingest_document(ctx["document_id"], storage=ctx["storage"])

        conn = prediction_db.get_conn()
        row = conn.execute(
            "SELECT status, reason_code, error_message FROM etl_attempts WHERE document_id = %s",
            [ctx["document_id"]],
        ).fetchone()
        assert row is not None
        assert row[0] == "failed"
        assert row[1] == "unexpected_error"
        assert "boom" in row[2]


class TestFlattenTree:
    def test_flatten_simple_node(self):
        node = {
            "tag": "Aktywa",
            "label": "AKTYWA",
            "kwota_a": 1000.0,
            "kwota_b": 900.0,
            "children": [
                {
                    "tag": "Aktywa_A",
                    "label": "A. Aktywa trwale",
                    "kwota_a": 600.0,
                    "kwota_b": 500.0,
                    "children": [],
                },
            ],
        }

        items = etl._flatten_tree(node, "Bilans", "rpt-1")
        assert len(items) == 2
        assert items[0]["tag_path"] == "Aktywa"
        assert items[0]["value_current"] == 1000.0
        assert items[1]["tag_path"] == "Aktywa_A"

    def test_flatten_skips_no_values(self):
        node = {
            "tag": "Empty",
            "label": "Empty",
            "kwota_a": None,
            "kwota_b": None,
            "children": [],
        }
        items = etl._flatten_tree(node, "Bilans", "rpt-1")
        assert len(items) == 0
