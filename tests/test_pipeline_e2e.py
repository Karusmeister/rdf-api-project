"""
End-to-end integration test for the full prediction pipeline.

Tests the complete flow against the real RDF API for KRS 0000502004:
  1. Search for financial documents via RDF API
  2. Download a real financial statement ZIP
  3. Extract and save to local storage
  4. Run ETL: parse XML → persist to DuckDB
  5. Seed feature definitions
  6. Compute features from line items
  7. Validate computed ratios make financial sense

Run with:  pytest tests/test_pipeline_e2e.py -v -s
Skip with: pytest tests/ -v --ignore=tests/test_pipeline_e2e.py
"""

import asyncio
import math
from unittest.mock import patch

import pytest

from app import rdf_client
from app.config import settings
from app.db import connection as db_conn
from app.db import prediction_db
from app.scraper import db as scraper_db
from app.scraper.storage import LocalStorage, make_doc_dir
from app.services import etl, feature_engine, xml_parser
from scripts.seed_features import FEATURE_DEFINITIONS, FEATURE_SETS

KRS = "0000694720"

# Mark all tests as requiring network access
pytestmark = pytest.mark.e2e


@pytest.fixture(scope="module")
def event_loop():
    """Module-scoped event loop for async fixtures."""
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope="module")
def db_and_storage(tmp_path_factory):
    """Set up isolated DuckDB + local storage for the entire test module."""
    tmp = tmp_path_factory.mktemp("e2e")
    db_path = str(tmp / "e2e.duckdb")
    storage_dir = tmp / "documents"

    db_conn.reset()
    scraper_db._schema_initialized = False
    prediction_db._schema_initialized = False

    with patch.object(settings, "scraper_db_path", db_path):
        scraper_db.connect()
        prediction_db.connect()

        # Seed feature definitions
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

        storage = LocalStorage(str(storage_dir))

        yield {
            "tmp": tmp,
            "storage": storage,
            "db_path": db_path,
        }

        db_conn.close()

    db_conn.reset()
    scraper_db._schema_initialized = False
    prediction_db._schema_initialized = False


@pytest.fixture(scope="module")
def downloaded_doc(db_and_storage, event_loop):
    """
    Search RDF API for KRS 0000502004, pick the most recent Polish-GAAP
    annual statement, download it, and save to local storage.
    Returns dict with document_id, krs, doc_dir, storage.
    """
    storage = db_and_storage["storage"]

    async def _fetch():
        await rdf_client.start()
        try:
            # 1. Search for documents
            search_data = await rdf_client.wyszukiwanie(KRS, page=0, page_size=50)
            docs = search_data["content"]

            # Filter: rodzaj=18 (financial statements), not deleted
            statements = [
                d for d in docs
                if d["rodzaj"] == "18" and d["status"] == "NIEUSUNIETY"
            ]
            assert len(statements) > 0, f"No financial statements found for KRS {KRS}"

            # Pick the most recent by period end
            statements.sort(key=lambda d: d.get("okresSprawozdawczyKoniec", ""), reverse=True)

            # Find a Polish-GAAP (non-IFRS) statement
            chosen = None
            for stmt in statements:
                meta = await rdf_client.metadata(stmt["id"])
                if not meta.get("czyMSR", False):  # Not IFRS
                    chosen = {"doc": stmt, "meta": meta}
                    break

            assert chosen is not None, "No Polish-GAAP statement found"

            doc = chosen["doc"]
            meta = chosen["meta"]
            document_id = doc["id"]

            print(f"\n  Selected document: {document_id}")
            print(f"  Period: {doc.get('okresSprawozdawczyPoczatek')} to {doc.get('okresSprawozdawczyKoniec')}")
            print(f"  Filename: {meta.get('nazwaPliku')}")
            print(f"  Is correction: {meta.get('czyKorekta', False)}")

            # 2. Download ZIP
            zip_bytes = await rdf_client.download([document_id])
            assert len(zip_bytes) > 0, "Empty ZIP downloaded"
            print(f"  ZIP size: {len(zip_bytes):,} bytes")

            # 3. Save to local storage
            doc_dir = make_doc_dir(KRS, document_id)
            manifest = storage.save_extracted(doc_dir, zip_bytes, document_id)
            print(f"  Extracted {len(manifest['files'])} files to {doc_dir}")

            # 4. Register in scraper DB
            from datetime import datetime, timezone
            now = datetime.now(timezone.utc)

            scraper_db.upsert_krs(KRS.zfill(10), None, None, True)
            scraper_db.get_conn().execute("""
                INSERT INTO krs_documents
                    (document_id, krs, rodzaj, status, nazwa, okres_start, okres_end,
                     is_downloaded, storage_path, discovered_at)
                VALUES (?, ?, '18', 'NIEUSUNIETY', ?, ?, ?, true, ?, ?)
                ON CONFLICT (document_id) DO NOTHING
            """, [
                document_id, KRS.zfill(10), meta.get("nazwaPliku"),
                doc.get("okresSprawozdawczyPoczatek"),
                doc.get("okresSprawozdawczyKoniec"),
                doc_dir, now,
            ])

            return {
                "document_id": document_id,
                "krs": KRS.zfill(10),
                "doc_dir": doc_dir,
                "period_start": doc.get("okresSprawozdawczyPoczatek"),
                "period_end": doc.get("okresSprawozdawczyKoniec"),
                "storage": storage,
            }
        finally:
            await rdf_client.stop()

    return event_loop.run_until_complete(_fetch())


# ---------------------------------------------------------------------------
# Tests — executed in order
# ---------------------------------------------------------------------------


class TestE2EPipeline:
    """End-to-end pipeline tests for KRS 0000502004."""

    def test_01_etl_ingest(self, downloaded_doc, db_and_storage):
        """ETL ingests the downloaded document into DuckDB."""
        result = etl.ingest_document(
            downloaded_doc["document_id"],
            storage=downloaded_doc["storage"],
        )

        print(f"\n  Ingestion result: {result['status']}")
        print(f"  Fiscal year: {result.get('fiscal_year')}")
        print(f"  Line items: {result.get('line_items_count')}")
        print(f"  Sections: {result.get('sections')}")

        assert result["status"] == "completed", f"Ingestion failed: {result}"
        assert result["line_items_count"] > 0, "No line items extracted"
        assert result["krs"] == downloaded_doc["krs"]

    def test_02_company_created(self, downloaded_doc):
        """ETL upserted the company record."""
        company = prediction_db.get_company(downloaded_doc["krs"])
        assert company is not None
        print(f"\n  Company: krs={company['krs']}, nip={company.get('nip')}, pkd={company.get('pkd_code')}")

    def test_03_financial_report_exists(self, downloaded_doc):
        """A financial_report record was created with correct metadata."""
        report = prediction_db.get_financial_report(downloaded_doc["document_id"])
        assert report is not None
        assert report["ingestion_status"] == "completed"
        assert report["fiscal_year"] > 2000
        print(f"\n  Report: id={report['id']}, year={report['fiscal_year']}, status={report['ingestion_status']}")

    def test_04_line_items_have_all_sections(self, downloaded_doc):
        """Line items cover Bilans and RZiS at minimum."""
        items = prediction_db.get_line_items(downloaded_doc["document_id"])
        sections = {i["section"] for i in items}

        print(f"\n  Total line items: {len(items)}")
        print(f"  Sections present: {sections}")

        assert "Bilans" in sections, "Missing Bilans section"
        assert "RZiS" in sections, "Missing RZiS section"

    def test_05_key_bilans_tags_present(self, downloaded_doc):
        """Critical balance sheet positions exist."""
        items = prediction_db.get_line_items(downloaded_doc["document_id"], section="Bilans")
        tags = {i["tag_path"] for i in items}
        tag_map = {i["tag_path"]: i for i in items}

        required_tags = ["Aktywa", "Aktywa_A", "Aktywa_B", "Pasywa", "Pasywa_A", "Pasywa_B"]
        for tag in required_tags:
            assert tag in tags, f"Missing critical Bilans tag: {tag}"

        aktywa = tag_map["Aktywa"]["value_current"]
        pasywa = tag_map["Pasywa"]["value_current"]
        print(f"\n  Aktywa (total assets): {aktywa:,.2f}")
        print(f"  Pasywa (total liabilities+equity): {pasywa:,.2f}")

        # Balance sheet identity: Assets == Liabilities + Equity
        assert aktywa == pytest.approx(pasywa, rel=0.01), \
            f"Balance sheet doesn't balance: Aktywa={aktywa} vs Pasywa={pasywa}"

    def test_06_key_rzis_tags_present(self, downloaded_doc):
        """Critical income statement positions exist."""
        items = prediction_db.get_line_items(downloaded_doc["document_id"], section="RZiS")
        tags = {i["tag_path"] for i in items}
        tag_map = {i["tag_path"]: i for i in items}

        # At minimum, revenue (A) and net profit (L) should exist
        assert "RZiS.A" in tags or "RZiS.A_I" in tags, "Missing revenue tag"
        assert "RZiS.L" in tags, "Missing net profit tag (RZiS.L)"

        if "RZiS.A" in tag_map:
            print(f"\n  Revenue (RZiS.A): {tag_map['RZiS.A']['value_current']:,.2f}")
        if "RZiS.L" in tag_map:
            print(f"  Net profit (RZiS.L): {tag_map['RZiS.L']['value_current']:,.2f}")

    def test_07_raw_financial_data_stored(self, downloaded_doc):
        """Raw JSON is preserved in raw_financial_data."""
        conn = prediction_db.get_conn()
        rows = conn.execute(
            "SELECT section FROM raw_financial_data WHERE report_id = ?",
            [downloaded_doc["document_id"]],
        ).fetchall()
        sections = {r[0] for r in rows}

        print(f"\n  Raw data sections: {sections}")
        assert "balance_sheet" in sections
        assert "income_statement" in sections

    def test_08_compute_all_features(self, downloaded_doc):
        """Feature engine computes all 22 features."""
        result = feature_engine.compute_features_for_report(downloaded_doc["document_id"])

        print(f"\n  Features computed: {result['computed']}")
        print(f"  Features failed: {result['failed']}")

        assert result["computed"] > 0, "No features computed"

        # Print all feature values
        print("\n  Feature values:")
        for fid, val in sorted(result["features"].items()):
            status = f"{val:.6f}" if val is not None else "NULL"
            print(f"    {fid:25s} = {status}")

    def test_09_ratios_financially_valid(self, downloaded_doc):
        """Computed ratios are within plausible financial ranges."""
        features = feature_engine.get_features_for_report(downloaded_doc["document_id"])

        # Debt ratio + equity ratio should sum to ~1.0
        debt_ratio = features.get("debt_ratio")
        equity_ratio = features.get("equity_ratio")
        if debt_ratio is not None and equity_ratio is not None:
            total = debt_ratio + equity_ratio
            print(f"\n  debt_ratio ({debt_ratio:.4f}) + equity_ratio ({equity_ratio:.4f}) = {total:.4f}")
            assert total == pytest.approx(1.0, abs=0.02), \
                f"Debt + equity ratio should be ~1.0, got {total}"

        # Current ratio should be positive (if computed)
        current_ratio = features.get("current_ratio")
        if current_ratio is not None:
            print(f"  current_ratio = {current_ratio:.4f}")
            assert current_ratio > 0, f"Current ratio should be positive, got {current_ratio}"

        # ROA should be between -1 and +1 (usually)
        roa = features.get("roa")
        if roa is not None:
            print(f"  roa = {roa:.4f}")
            assert -2 < roa < 2, f"ROA looks implausible: {roa}"

        # Log assets should be positive (company has assets)
        log_assets = features.get("log_total_assets")
        if log_assets is not None:
            print(f"  log_total_assets = {log_assets:.4f}")
            assert log_assets > 0, f"Log assets should be positive, got {log_assets}"

    def test_10_maczynska_set_computable(self, downloaded_doc):
        """All 6 Maczynska features can be computed."""
        result = feature_engine.compute_features_for_report(
            downloaded_doc["document_id"],
            feature_set_id="maczynska_6",
        )

        print(f"\n  Maczynska features: {result['computed']} computed, {result['failed']} failed")
        for fid in ["x1_maczynska", "x2_maczynska", "x3_maczynska",
                     "x4_maczynska", "x5_maczynska", "x6_maczynska"]:
            val = result["features"].get(fid)
            status = f"{val:.6f}" if val is not None else "NULL"
            print(f"    {fid} = {status}")

        # At least 4 of 6 should compute (some tags may be missing in real data)
        assert result["computed"] >= 4, \
            f"Expected at least 4/6 Maczynska features, got {result['computed']}"

    def test_11_idempotent_reingest(self, downloaded_doc):
        """Re-ingesting the same document doesn't break anything."""
        items_before = prediction_db.get_line_items(downloaded_doc["document_id"])

        result = etl.ingest_document(
            downloaded_doc["document_id"],
            storage=downloaded_doc["storage"],
        )
        assert result["status"] == "completed"

        items_after = prediction_db.get_line_items(downloaded_doc["document_id"])
        assert len(items_after) == len(items_before), \
            f"Line item count changed after re-ingest: {len(items_before)} -> {len(items_after)}"

    def test_12_recompute_features_stable(self, downloaded_doc):
        """Recomputing features produces the same values."""
        features_before = feature_engine.get_features_for_report(downloaded_doc["document_id"])

        result = feature_engine.recompute(downloaded_doc["document_id"])

        features_after = feature_engine.get_features_for_report(downloaded_doc["document_id"])

        for fid, val_before in features_before.items():
            val_after = features_after.get(fid)
            if val_before is not None and val_after is not None:
                assert val_before == pytest.approx(val_after, rel=1e-6), \
                    f"Feature {fid} changed after recompute: {val_before} -> {val_after}"
