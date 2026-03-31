"""Tests for the feature computation engine."""

import math
from unittest.mock import patch

import pytest

from app.config import settings
from app.db import connection as db_conn
from app.db import prediction_db
from app.scraper import db as scraper_db
from app.services import feature_engine
from scripts.seed_features import FEATURE_DEFINITIONS, FEATURE_SETS


@pytest.fixture
def isolated_db(pg_dsn, clean_pg):
    """Set up isolated PostgreSQL DB for both scraper and prediction tables."""
    db_conn.reset()
    scraper_db._schema_initialized = False
    prediction_db._schema_initialized = False

    with patch.object(settings, "database_url", pg_dsn):
        scraper_db.connect()
        prediction_db.connect()
        yield
        db_conn.close()

    db_conn.reset()
    scraper_db._schema_initialized = False
    prediction_db._schema_initialized = False


@pytest.fixture
def seeded_db(isolated_db):
    """DB with feature definitions seeded."""
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
    return isolated_db


@pytest.fixture
def report_with_data(seeded_db):
    """A report with line items that cover all standard ratios."""
    krs = "0000012345"
    report_id = "test-report-001"

    prediction_db.upsert_company(krs=krs, nip="1234567890", pkd_code="62.01.Z")
    prediction_db.create_financial_report(
        report_id=report_id, krs=krs, fiscal_year=2023,
        period_start="2023-01-01", period_end="2023-12-31",
    )
    prediction_db.update_report_status(report_id, "completed")

    # Insert line items covering all major tags
    line_items = [
        # Bilans
        {"report_id": report_id, "section": "Bilans", "tag_path": "Aktywa", "value_current": 1000000, "value_previous": 900000},
        {"report_id": report_id, "section": "Bilans", "tag_path": "Aktywa_A", "value_current": 600000, "value_previous": 550000},
        {"report_id": report_id, "section": "Bilans", "tag_path": "Aktywa_B", "value_current": 400000, "value_previous": 350000},
        {"report_id": report_id, "section": "Bilans", "tag_path": "Aktywa_B_I", "value_current": 100000, "value_previous": 80000},
        {"report_id": report_id, "section": "Bilans", "tag_path": "Aktywa_B_II", "value_current": 100000, "value_previous": 90000},
        {"report_id": report_id, "section": "Bilans", "tag_path": "Aktywa_B_III", "value_current": 200000, "value_previous": 180000},
        {"report_id": report_id, "section": "Bilans", "tag_path": "Pasywa_A", "value_current": 500000, "value_previous": 450000},
        {"report_id": report_id, "section": "Bilans", "tag_path": "Pasywa_B", "value_current": 500000, "value_previous": 450000},
        {"report_id": report_id, "section": "Bilans", "tag_path": "Pasywa_B_III", "value_current": 300000, "value_previous": 280000},
        # RZiS
        {"report_id": report_id, "section": "RZiS", "tag_path": "RZiS.A", "value_current": 2000000, "value_previous": 1800000},
        {"report_id": report_id, "section": "RZiS", "tag_path": "RZiS.B", "value_current": 1700000, "value_previous": 1550000},
        {"report_id": report_id, "section": "RZiS", "tag_path": "RZiS.C", "value_current": 300000, "value_previous": 250000},
        {"report_id": report_id, "section": "RZiS", "tag_path": "RZiS.F", "value_current": 200000, "value_previous": 180000},
        {"report_id": report_id, "section": "RZiS", "tag_path": "RZiS.I", "value_current": 80000, "value_previous": 70000},
        {"report_id": report_id, "section": "RZiS", "tag_path": "RZiS.L", "value_current": 50000, "value_previous": 40000},
    ]
    prediction_db.batch_insert_line_items(line_items)

    return {"krs": krs, "report_id": report_id}


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestSeedFeatures:
    def test_definitions_count(self, seeded_db):
        defs = prediction_db.get_feature_definitions(active_only=True)
        assert len(defs) == len(FEATURE_DEFINITIONS)

    def test_maczynska_set_has_6_members(self, seeded_db):
        members = prediction_db.get_feature_set_members("maczynska_6")
        assert len(members) == 6

    def test_basic_set_has_20_members(self, seeded_db):
        members = prediction_db.get_feature_set_members("basic_20")
        assert len(members) == 20

    def test_definitions_idempotent(self, seeded_db):
        """Re-seeding doesn't change count."""
        for fdef in FEATURE_DEFINITIONS:
            prediction_db.upsert_feature_definition(
                feature_id=fdef["id"], name=fdef["name"],
                computation_logic=fdef.get("computation_logic", "ratio"),
            )
        defs = prediction_db.get_feature_definitions(active_only=True)
        assert len(defs) == len(FEATURE_DEFINITIONS)


class TestComputeFeatures:
    def test_compute_all_features(self, report_with_data):
        ctx = report_with_data
        result = feature_engine.compute_features_for_report(ctx["report_id"])

        assert result["computed"] > 0
        assert result["krs"] == ctx["krs"]
        assert result["fiscal_year"] == 2023

    def test_roa_value(self, report_with_data):
        ctx = report_with_data
        result = feature_engine.compute_features_for_report(ctx["report_id"])

        # ROA = Net Profit / Total Assets = 50000 / 1000000 = 0.05
        assert result["features"]["roa"] == pytest.approx(0.05, abs=1e-4)

    def test_roe_value(self, report_with_data):
        ctx = report_with_data
        result = feature_engine.compute_features_for_report(ctx["report_id"])

        # ROE = Net Profit / Equity = 50000 / 500000 = 0.1
        assert result["features"]["roe"] == pytest.approx(0.1, abs=1e-4)

    def test_current_ratio(self, report_with_data):
        ctx = report_with_data
        result = feature_engine.compute_features_for_report(ctx["report_id"])

        # Current Ratio = Current Assets / ST Liabilities = 400000 / 300000 = 1.333
        assert result["features"]["current_ratio"] == pytest.approx(1.3333, abs=1e-3)

    def test_quick_ratio_custom(self, report_with_data):
        ctx = report_with_data
        result = feature_engine.compute_features_for_report(ctx["report_id"])

        # Quick Ratio = (400000 - 100000) / 300000 = 1.0
        assert result["features"]["quick_ratio"] == pytest.approx(1.0, abs=1e-4)

    def test_debt_ratio(self, report_with_data):
        ctx = report_with_data
        result = feature_engine.compute_features_for_report(ctx["report_id"])

        # Debt Ratio = Liabilities / Assets = 500000 / 1000000 = 0.5
        assert result["features"]["debt_ratio"] == pytest.approx(0.5, abs=1e-4)

    def test_log_total_assets_custom(self, report_with_data):
        ctx = report_with_data
        result = feature_engine.compute_features_for_report(ctx["report_id"])

        # ln(1000000) ~= 13.8155
        expected = math.log(1000000)
        assert result["features"]["log_total_assets"] == pytest.approx(expected, abs=1e-3)

    def test_log_revenue_custom(self, report_with_data):
        ctx = report_with_data
        result = feature_engine.compute_features_for_report(ctx["report_id"])

        expected = math.log(2000000)
        assert result["features"]["log_revenue"] == pytest.approx(expected, abs=1e-3)

    def test_maczynska_features(self, report_with_data):
        ctx = report_with_data
        result = feature_engine.compute_features_for_report(
            ctx["report_id"], feature_set_id="maczynska_6"
        )

        # X1 is custom: (RZiS.I + CF.A_II_1) / Pasywa_B — CF.A_II_1 missing → dep=0
        # (80000 + 0) / 500000 = 0.16
        assert result["features"]["x1_maczynska"] == pytest.approx(0.16, abs=1e-4)
        # X2 = Aktywa / Pasywa_B = 1000000 / 500000 = 2.0
        assert result["features"]["x2_maczynska"] == pytest.approx(2.0, abs=1e-4)
        # X3 = RZiS.I / Aktywa = 80000 / 1000000 = 0.08
        assert result["features"]["x3_maczynska"] == pytest.approx(0.08, abs=1e-4)
        # X4 = RZiS.I / RZiS.A = 80000 / 2000000 = 0.04
        assert result["features"]["x4_maczynska"] == pytest.approx(0.04, abs=1e-4)
        # X5 = Aktywa_B_I / RZiS.A = 100000 / 2000000 = 0.05
        assert result["features"]["x5_maczynska"] == pytest.approx(0.05, abs=1e-4)
        # X6 = RZiS.A / Aktywa = 2000000 / 1000000 = 2.0
        assert result["features"]["x6_maczynska"] == pytest.approx(2.0, abs=1e-4)

    def test_compute_with_feature_set_filter(self, report_with_data):
        ctx = report_with_data
        result = feature_engine.compute_features_for_report(
            ctx["report_id"], feature_set_id="maczynska_6"
        )
        # Should only have 6 features (the Maczynska set)
        total = result["computed"] + result["failed"]
        assert total == 6

    def test_features_persisted(self, report_with_data):
        ctx = report_with_data
        feature_engine.compute_features_for_report(ctx["report_id"])

        stored = prediction_db.get_computed_features(ctx["krs"], 2023)
        assert len(stored) > 0

        roa = [f for f in stored if f["feature_definition_id"] == "roa"]
        assert len(roa) == 1
        assert roa[0]["value"] == pytest.approx(0.05, abs=1e-4)


class TestGetFeaturesForReport:
    def test_returns_dict(self, report_with_data):
        ctx = report_with_data
        feature_engine.compute_features_for_report(ctx["report_id"])

        features = feature_engine.get_features_for_report(ctx["report_id"])
        assert isinstance(features, dict)
        assert "roa" in features
        assert features["roa"] == pytest.approx(0.05, abs=1e-4)

    def test_not_found(self, seeded_db):
        with pytest.raises(ValueError, match="not found"):
            feature_engine.get_features_for_report("nonexistent")


class TestDivisionByZero:
    def test_zero_denominator(self, seeded_db):
        krs = "0000099999"
        report_id = "zero-denom-report"

        prediction_db.upsert_company(krs=krs)
        prediction_db.create_financial_report(
            report_id=report_id, krs=krs, fiscal_year=2023,
            period_start="2023-01-01", period_end="2023-12-31",
        )
        prediction_db.update_report_status(report_id, "completed")

        # Assets = 0 (zero denominator for ROA)
        prediction_db.batch_insert_line_items([
            {"report_id": report_id, "section": "Bilans", "tag_path": "Aktywa", "value_current": 0},
            {"report_id": report_id, "section": "RZiS", "tag_path": "RZiS.L", "value_current": 50000},
        ])

        result = feature_engine.compute_features_for_report(report_id)
        assert result["features"]["roa"] is None

        # Verify error stored correctly
        conn = prediction_db.get_conn()
        row = conn.execute("""
            SELECT is_valid, error_message FROM computed_features
            WHERE report_id = %s AND feature_definition_id = 'roa'
        """, [report_id]).fetchone()
        assert row[0] is False
        assert row[1] == "division_by_zero"


class TestMissingTags:
    def test_missing_tag_handled(self, seeded_db):
        krs = "0000088888"
        report_id = "missing-tags-report"

        prediction_db.upsert_company(krs=krs)
        prediction_db.create_financial_report(
            report_id=report_id, krs=krs, fiscal_year=2023,
            period_start="2023-01-01", period_end="2023-12-31",
        )
        prediction_db.update_report_status(report_id, "completed")

        # Only insert Aktywa — no RZiS tags
        prediction_db.batch_insert_line_items([
            {"report_id": report_id, "section": "Bilans", "tag_path": "Aktywa", "value_current": 1000000},
        ])

        result = feature_engine.compute_features_for_report(report_id)

        # ROA needs RZiS.L which is missing
        assert result["features"]["roa"] is None
        assert result["failed"] > 0

        conn = prediction_db.get_conn()
        row = conn.execute("""
            SELECT error_message FROM computed_features
            WHERE report_id = %s AND feature_definition_id = 'roa'
        """, [report_id]).fetchone()
        assert "missing_tag" in row[0]


class TestRecompute:
    def test_recompute_appends_new_feature_version(self, report_with_data):
        ctx = report_with_data

        result1 = feature_engine.compute_features_for_report(ctx["report_id"])
        assert result1["features"]["roa"] == pytest.approx(0.05, abs=1e-4)

        # Change the net profit
        prediction_db.batch_insert_line_items([
            {"report_id": ctx["report_id"], "section": "RZiS", "tag_path": "RZiS.L",
             "value_current": 100000, "value_previous": 40000},
        ])

        result2 = feature_engine.recompute(ctx["report_id"])
        # New ROA = 100000 / 1000000 = 0.1
        assert result2["features"]["roa"] == pytest.approx(0.1, abs=1e-4)
        conn = prediction_db.get_conn()
        history_count = conn.execute("""
            SELECT count(*) FROM computed_features
            WHERE report_id = %s AND feature_definition_id = 'roa'
        """, [ctx["report_id"]]).fetchone()[0]
        assert history_count == 2


class TestComputeAllPending:
    def test_computes_pending(self, seeded_db):
        krs = "0000077777"
        report_id = "pending-report"

        prediction_db.upsert_company(krs=krs)
        prediction_db.create_financial_report(
            report_id=report_id, krs=krs, fiscal_year=2023,
            period_start="2023-01-01", period_end="2023-12-31",
        )
        prediction_db.update_report_status(report_id, "completed")

        prediction_db.batch_insert_line_items([
            {"report_id": report_id, "section": "Bilans", "tag_path": "Aktywa", "value_current": 1000000},
            {"report_id": report_id, "section": "RZiS", "tag_path": "RZiS.L", "value_current": 50000},
        ])

        result = feature_engine.compute_all_pending()
        assert result["total"] == 1
        assert result["computed"] == 1
