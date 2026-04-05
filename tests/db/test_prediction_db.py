"""Tests for app/db/prediction_db.py — uses PostgreSQL via pg_dsn fixture."""
import pytest
from unittest.mock import patch

from app.config import settings
from app.db import connection as db_conn
from app.db import prediction_db as db


@pytest.fixture(autouse=True)
def isolated_db(pg_dsn, clean_pg):
    """Override DB URL to test PostgreSQL and reset the shared connection."""
    db_conn.reset()
    db._schema_initialized = False
    with patch.object(settings, "database_url", pg_dsn):
        db.connect()
        yield
        db_conn.close()
    db_conn.reset()
    db._schema_initialized = False


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

def test_schema_creation():
    conn = db.get_conn()
    tables = {
        row[0]
        for row in conn.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"
        ).fetchall()
    }
    expected = {
        "companies",
        "financial_reports", "raw_financial_data", "financial_line_items",
        "feature_definitions", "feature_sets", "feature_set_members", "computed_features",
        "model_registry", "prediction_runs", "predictions",
        "bankruptcy_events", "assessment_jobs", "etl_attempts",
    }
    assert expected.issubset(tables)


# ---------------------------------------------------------------------------
# Layer 1: companies & company_identifiers
# ---------------------------------------------------------------------------

def test_upsert_company_insert():
    db.upsert_company("0000000001", nip="1234567890", pkd_code="62.01.Z",
                      voivodeship="mazowieckie")
    company = db.get_company("0000000001")
    assert company is not None
    assert company["nip"] == "1234567890"
    assert company["pkd_code"] == "62.01.Z"
    assert company["voivodeship"] == "mazowieckie"


def test_upsert_company_update_merges():
    db.upsert_company("0000000002", nip="1111111111")
    db.upsert_company("0000000002", regon="123456789", pkd_code="64.99.Z")
    company = db.get_company("0000000002")
    # nip preserved from first insert
    assert company["nip"] == "1111111111"
    # regon and pkd from update
    assert company["regon"] == "123456789"
    assert company["pkd_code"] == "64.99.Z"


def test_upsert_company_none_does_not_overwrite():
    db.upsert_company("0000000003", nip="9999999999")
    db.upsert_company("0000000003", nip=None)
    company = db.get_company("0000000003")
    assert company["nip"] == "9999999999"


def test_get_company_not_found():
    assert db.get_company("9999999999") is None


# ---------------------------------------------------------------------------
# Layer 2: financial_reports, raw_financial_data, financial_line_items
# ---------------------------------------------------------------------------

def test_create_financial_report():
    db.upsert_company("0000000010")
    db.create_financial_report(
        "rpt-001", "0000000010", 2023,
        "2023-01-01", "2023-12-31",
    )
    report = db.get_financial_report("rpt-001")
    assert report is not None
    assert report["krs"] == "0000000010"
    assert report["fiscal_year"] == 2023
    assert report["ingestion_status"] == "pending"


def test_create_financial_report_correction_preserves_history():
    """A correction becomes a new report version and preserves the original."""
    db.upsert_company("0000000011")
    db.create_financial_report("rpt-002", "0000000011", 2022, "2022-01-01", "2022-12-31")

    # Insert child data for the original
    db.batch_insert_line_items([
        {"report_id": "rpt-002", "section": "Bilans", "tag_path": "Aktywa",
         "value_current": 100.0},
    ])

    # Correction with different ID but same business key
    superseded = db.create_financial_report("rpt-002b", "0000000011", 2022, "2022-01-01", "2022-12-31")
    assert superseded == "rpt-002"

    conn = db.get_conn()
    count = conn.execute(
        "SELECT count(*) FROM financial_reports WHERE krs = '0000000011'"
    ).fetchone()[0]
    assert count == 2

    original = db.get_financial_report("rpt-002")
    assert original is not None
    assert original["report_version"] == 1

    # Correction becomes the latest version for the same logical report
    report = db.get_financial_report("rpt-002b")
    assert report is not None
    assert report["report_version"] == 2
    assert report["supersedes_report_id"] == "rpt-002"

    # Original child data is preserved for audit/history
    items = db.get_line_items("rpt-002")
    assert len(items) == 1


def test_update_report_status():
    db.upsert_company("0000000012")
    db.create_financial_report("rpt-003", "0000000012", 2021, "2021-01-01", "2021-12-31")
    db.update_report_status("rpt-003", "completed")
    report = db.get_financial_report("rpt-003")
    assert report["ingestion_status"] == "completed"


def test_update_report_status_with_error():
    db.upsert_company("0000000013")
    db.create_financial_report("rpt-004", "0000000013", 2020, "2020-01-01", "2020-12-31")
    db.update_report_status("rpt-004", "failed", error="parse error")
    report = db.get_financial_report("rpt-004")
    assert report["ingestion_status"] == "failed"


def test_get_reports_for_krs():
    db.upsert_company("0000000014")
    db.create_financial_report("rpt-005", "0000000014", 2023, "2023-01-01", "2023-12-31")
    db.create_financial_report("rpt-006", "0000000014", 2022, "2022-01-01", "2022-12-31")
    reports = db.get_reports_for_krs("0000000014")
    assert len(reports) == 2
    assert reports[0]["fiscal_year"] == 2023  # DESC order


def test_upsert_raw_financial_data():
    db.upsert_company("0000000015")
    db.create_financial_report("rpt-007", "0000000015", 2023, "2023-01-01", "2023-12-31")
    data = {"Bilans": {"Aktywa": {"A": {"I": 1000000}}}}
    db.upsert_raw_financial_data("rpt-007", "balance_sheet", data)
    conn = db.get_conn()
    row = conn.execute(
        "SELECT section FROM raw_financial_data WHERE report_id = 'rpt-007'"
    ).fetchone()
    assert row is not None
    assert row[0] == "balance_sheet"


def test_batch_insert_line_items():
    db.upsert_company("0000000016")
    db.create_financial_report("rpt-008", "0000000016", 2023, "2023-01-01", "2023-12-31")
    items = [
        {"report_id": "rpt-008", "section": "Bilans", "tag_path": "Bilans.Aktywa",
         "label_pl": "Aktywa", "value_current": 5000000.0, "value_previous": 4500000.0},
        {"report_id": "rpt-008", "section": "Bilans", "tag_path": "Bilans.Aktywa.A",
         "label_pl": "Aktywa trwale", "value_current": 2000000.0, "value_previous": None},
        {"report_id": "rpt-008", "section": "RZiS", "tag_path": "RZiS.L",
         "label_pl": "Zysk netto", "value_current": 300000.0, "value_previous": 250000.0},
    ]
    db.batch_insert_line_items(items)
    result = db.get_line_items("rpt-008")
    assert len(result) == 3


def test_get_line_items_by_section():
    db.upsert_company("0000000017")
    db.create_financial_report("rpt-009", "0000000017", 2023, "2023-01-01", "2023-12-31")
    items = [
        {"report_id": "rpt-009", "section": "Bilans", "tag_path": "Bilans.Aktywa",
         "value_current": 100.0},
        {"report_id": "rpt-009", "section": "RZiS", "tag_path": "RZiS.A",
         "value_current": 50.0},
    ]
    db.batch_insert_line_items(items)
    bilans = db.get_line_items("rpt-009", section="Bilans")
    assert len(bilans) == 1
    assert bilans[0]["section"] == "Bilans"


def test_get_source_line_items_uses_prior_year_report_values():
    db.upsert_company("0000000017")
    db.create_financial_report("rpt-009-prev", "0000000017", 2022, "2022-01-01", "2022-12-31")
    db.create_financial_report("rpt-009-curr", "0000000017", 2023, "2023-01-01", "2023-12-31")
    db.update_report_status("rpt-009-prev", "completed")
    db.update_report_status("rpt-009-curr", "completed")

    db.batch_insert_line_items([
        {
            "report_id": "rpt-009-prev",
            "section": "RZiS",
            "tag_path": "RZiS.I",
            "label_pl": "Zysk (strata) brutto",
            "value_current": 90.0,
            "value_previous": 80.0,
        },
    ])
    db.batch_insert_line_items([
        {
            "report_id": "rpt-009-curr",
            "section": "RZiS",
            "tag_path": "RZiS.I",
            "label_pl": "Zysk (strata) brutto",
            "value_current": 100.0,
            # Should be ignored by lookup logic in favor of prior-year report.
            "value_previous": 10.0,
        },
    ])

    rows = db.get_source_line_items_for_report("rpt-009-curr", ["RZiS.I", "CF.A_II_1"])
    by_tag = {r["tag_path"]: r for r in rows}

    assert by_tag["RZiS.I"]["value_current"] == pytest.approx(100.0)
    assert by_tag["RZiS.I"]["value_previous"] == pytest.approx(90.0)
    assert by_tag["CF.A_II_1"]["value_current"] is None
    assert by_tag["CF.A_II_1"]["value_previous"] is None


def test_line_items_pk_constraint():
    db.upsert_company("0000000018")
    db.create_financial_report("rpt-010", "0000000018", 2023, "2023-01-01", "2023-12-31")
    item = {"report_id": "rpt-010", "section": "Bilans", "tag_path": "Bilans.Aktywa",
            "value_current": 100.0}
    db.batch_insert_line_items([item])
    # Upsert with updated value
    item["value_current"] = 999.0
    db.batch_insert_line_items([item])
    result = db.get_line_items("rpt-010")
    assert len(result) == 1
    assert result[0]["value_current"] == 999.0
    conn = db.get_conn()
    history_count = conn.execute(
        "SELECT count(*) FROM financial_line_items WHERE report_id = 'rpt-010' AND tag_path = 'Bilans.Aktywa'"
    ).fetchone()[0]
    assert history_count == 2


# ---------------------------------------------------------------------------
# Layer 3: feature_definitions, feature_sets, computed_features
# ---------------------------------------------------------------------------

def test_upsert_feature_definition():
    db.upsert_feature_definition(
        "roa", "Return on Assets", category="profitability",
        formula_numerator="RZiS.L", formula_denominator="Bilans.Aktywa",
        required_tags=["RZiS.L", "Bilans.Aktywa"], computation_logic="ratio"
    )
    defs = db.get_feature_definitions()
    ids = [d["id"] for d in defs]
    assert "roa" in ids


def test_get_feature_definitions_active_only():
    db.upsert_feature_definition("active_feat", "Active Feature")
    db.upsert_feature_definition("inactive_feat", "Inactive Feature")
    conn = db.get_conn()
    conn.execute("UPDATE feature_definitions SET is_active = false WHERE id = 'inactive_feat'")
    defs = db.get_feature_definitions(active_only=True)
    ids = [d["id"] for d in defs]
    assert "active_feat" in ids
    assert "inactive_feat" not in ids


def test_feature_set_and_members():
    db.upsert_feature_definition("x1", "Feature X1", computation_logic="ratio")
    db.upsert_feature_definition("x2", "Feature X2", computation_logic="ratio")
    db.upsert_feature_set("maczynska_6", "Maczynska 6-factor model")
    db.add_feature_set_member("maczynska_6", "x1", ordinal=1)
    db.add_feature_set_member("maczynska_6", "x2", ordinal=2)
    members = db.get_feature_set_members("maczynska_6")
    assert len(members) == 2
    assert members[0]["feature_definition_id"] == "x1"
    assert members[1]["ordinal"] == 2


def test_feature_set_member_pk_constraint():
    db.upsert_feature_definition("feat_dup", "Feat Dup")
    db.upsert_feature_set("set_dup", "Set Dup")
    db.add_feature_set_member("set_dup", "feat_dup", ordinal=1)
    db.add_feature_set_member("set_dup", "feat_dup", ordinal=99)  # update ordinal
    members = db.get_feature_set_members("set_dup")
    assert len(members) == 1
    assert members[0]["ordinal"] == 99


def test_upsert_computed_feature():
    db.upsert_company("0000000019")
    db.create_financial_report("rpt-011", "0000000019", 2023, "2023-01-01", "2023-12-31")
    db.upsert_feature_definition("current_ratio", "Current Ratio")
    db.upsert_computed_feature("rpt-011", "current_ratio", "0000000019", 2023, value=1.75)
    feats = db.get_computed_features("0000000019", fiscal_year=2023)
    assert len(feats) == 1
    assert feats[0]["value"] == pytest.approx(1.75)
    assert feats[0]["is_valid"] is True


def test_computed_feature_invalid():
    db.upsert_company("0000000020")
    db.create_financial_report("rpt-012", "0000000020", 2022, "2022-01-01", "2022-12-31")
    db.upsert_feature_definition("bad_ratio", "Bad Ratio")
    db.upsert_computed_feature("rpt-012", "bad_ratio", "0000000020", 2022,
                                value=None, is_valid=False, error_message="division_by_zero")
    # is_valid=False rows excluded from get_computed_features
    feats = db.get_computed_features("0000000020", fiscal_year=2022)
    assert len(feats) == 0


def test_computed_feature_pk_upsert():
    db.upsert_company("0000000021")
    db.create_financial_report("rpt-013", "0000000021", 2023, "2023-01-01", "2023-12-31")
    db.upsert_feature_definition("roe", "Return on Equity")
    db.upsert_computed_feature("rpt-013", "roe", "0000000021", 2023, value=0.15)
    db.upsert_computed_feature("rpt-013", "roe", "0000000021", 2023, value=0.18)
    feats = db.get_computed_features("0000000021", 2023)
    assert len(feats) == 1
    assert feats[0]["value"] == pytest.approx(0.18)
    conn = db.get_conn()
    history_count = conn.execute(
        "SELECT count(*) FROM computed_features WHERE report_id = 'rpt-013' AND feature_definition_id = 'roe'"
    ).fetchone()[0]
    assert history_count == 2


# ---------------------------------------------------------------------------
# Layer 4: model_registry, prediction_runs, predictions
# ---------------------------------------------------------------------------

def test_register_model():
    db.register_model(
        "maczynska_v1", "Maczynska MDA", "discriminant", "1.0",
        description="Baseline MDA model",
        training_metrics={"auc": 0.85},
        is_baseline=True,
    )
    models = db.get_active_models()
    ids = [m["id"] for m in models]
    assert "maczynska_v1" in ids
    m = next(m for m in models if m["id"] == "maczynska_v1")
    assert m["is_baseline"] is True


def test_register_model_unique_name_version():
    db.register_model("rf_v1", "RandomForest", "random_forest", "1.0")
    # Same name+version -> upsert updates
    db.register_model("rf_v1b", "RandomForest", "random_forest", "1.0",
                      description="updated")
    conn = db.get_conn()
    count = conn.execute(
        "SELECT count(*) FROM model_registry WHERE name = 'RandomForest' AND version = '1.0'"
    ).fetchone()[0]
    assert count == 1


def test_prediction_run_lifecycle():
    db.register_model("xgb_v1", "XGBoost", "xgboost", "1.0")
    db.create_prediction_run("run-001", "xgb_v1", parameters={"threshold": 0.5})

    conn = db.get_conn()
    row = conn.execute("SELECT status FROM prediction_runs WHERE id = 'run-001'").fetchone()
    assert row[0] == "running"

    db.finish_prediction_run("run-001", "completed", companies_scored=42, duration_seconds=5.3)
    row = conn.execute(
        "SELECT status, companies_scored, duration_seconds FROM prediction_runs WHERE id = 'run-001'"
    ).fetchone()
    assert row[0] == "completed"
    assert row[1] == 42
    assert row[2] == pytest.approx(5.3)


def test_insert_and_get_prediction():
    db.upsert_company("0000000030")
    db.create_financial_report("rpt-100", "0000000030", 2023, "2023-01-01", "2023-12-31")
    db.register_model("mda_v1", "MDA", "discriminant", "1.0")
    db.create_prediction_run("run-002", "mda_v1")

    db.insert_prediction(
        "pred-001", "run-002", "0000000030", "rpt-100",
        raw_score=-1.2, probability=0.78, classification=1, risk_category="high",
        feature_contributions={"x1": 0.3, "x2": -0.1},
    )

    pred = db.get_latest_prediction("0000000030")
    assert pred is not None
    assert pred["krs"] == "0000000030"
    assert pred["risk_category"] == "high"
    assert pred["probability"] == pytest.approx(0.78)
    assert pred["model_name"] == "MDA"


def test_get_prediction_history():
    db.upsert_company("0000000031")
    db.create_financial_report("rpt-101", "0000000031", 2022, "2022-01-01", "2022-12-31")
    db.create_financial_report("rpt-102", "0000000031", 2023, "2023-01-01", "2023-12-31")
    db.register_model("mda_v2", "MDA", "discriminant", "2.0")
    db.create_prediction_run("run-003", "mda_v2")
    db.insert_prediction("pred-010", "run-003", "0000000031", "rpt-101",
                          raw_score=0.5, probability=0.4, classification=0, risk_category="low")
    db.insert_prediction("pred-011", "run-003", "0000000031", "rpt-102",
                          raw_score=-0.5, probability=0.6, classification=1, risk_category="medium")
    history = db.get_prediction_history("0000000031")
    assert len(history) == 2


def test_get_predictions_fat_orders_by_fiscal_year():
    """Latest fiscal year should win even when an older year was rescored later."""
    db.upsert_company("0000000033")
    db.create_financial_report("rpt-200", "0000000033", 2022, "2022-01-01", "2022-12-31")
    db.create_financial_report("rpt-201", "0000000033", 2023, "2023-01-01", "2023-12-31")
    db.register_model("mcz_v1", "maczynska", "discriminant", "1.0")
    db.create_prediction_run("run-fat-1", "mcz_v1")
    # Insert 2023 first, then 2022 — so 2022 has a LATER created_at timestamp.
    db.insert_prediction("pred-fat-1", "run-fat-1", "0000000033", "rpt-201",
                         raw_score=1.5, probability=None, classification=0, risk_category="medium")
    db.insert_prediction("pred-fat-2", "run-fat-1", "0000000033", "rpt-200",
                         raw_score=-0.5, probability=None, classification=1, risk_category="critical")

    rows = db.get_predictions_fat("0000000033")
    assert len(rows) == 2
    # Most recent fiscal year must come first, regardless of scoring time.
    assert rows[0]["fiscal_year"] == 2023
    assert rows[0]["report_id"] == "rpt-201"
    assert rows[1]["fiscal_year"] == 2022


def test_get_prediction_history_fat_dedupes_rescored_years():
    """Rescoring a year should not create duplicate history points for charting."""
    db.upsert_company("0000000034")
    db.create_financial_report("rpt-300", "0000000034", 2021, "2021-01-01", "2021-12-31")
    db.create_financial_report("rpt-301", "0000000034", 2022, "2022-01-01", "2022-12-31")
    db.register_model("mcz_v2", "maczynska", "discriminant", "2.0")
    # Two runs scoring the same reports — simulates a rescoring pass.
    db.create_prediction_run("run-hist-1", "mcz_v2")
    db.insert_prediction("pred-hist-1", "run-hist-1", "0000000034", "rpt-300",
                         raw_score=0.1, probability=None, classification=0, risk_category="high")
    db.insert_prediction("pred-hist-2", "run-hist-1", "0000000034", "rpt-301",
                         raw_score=0.2, probability=None, classification=0, risk_category="high")
    db.create_prediction_run("run-hist-2", "mcz_v2")
    db.insert_prediction("pred-hist-3", "run-hist-2", "0000000034", "rpt-300",
                         raw_score=0.9, probability=None, classification=0, risk_category="low")
    db.insert_prediction("pred-hist-4", "run-hist-2", "0000000034", "rpt-301",
                         raw_score=1.1, probability=None, classification=0, risk_category="medium")

    history = db.get_prediction_history_fat("0000000034")
    # One point per (model, fiscal_year), not four.
    assert len(history) == 2
    years = [h["fiscal_year"] for h in history]
    assert years == [2021, 2022]  # chronological order for charting
    # Most recently scored value wins for each year.
    by_year = {h["fiscal_year"]: h for h in history}
    assert by_year[2021]["raw_score"] == pytest.approx(0.9)
    assert by_year[2022]["raw_score"] == pytest.approx(1.1)


def test_get_predictions_fat_deterministic_on_timestamp_tie():
    """Two predictions with identical created_at must resolve to a stable winner."""
    db.upsert_company("0000000035")
    db.create_financial_report("rpt-400", "0000000035", 2024, "2024-01-01", "2024-12-31")
    db.register_model("mcz_v3", "maczynska", "discriminant", "3.0")
    db.create_prediction_run("run-tie-1", "mcz_v3")
    db.insert_prediction("pred-tie-a", "run-tie-1", "0000000035", "rpt-400",
                         raw_score=0.1, probability=None, classification=0, risk_category="high")
    db.insert_prediction("pred-tie-b", "run-tie-1", "0000000035", "rpt-400",
                         raw_score=0.9, probability=None, classification=0, risk_category="low")
    # Force identical timestamps so only the secondary tie-breakers decide the winner.
    conn = db.get_conn()
    conn.execute(
        "UPDATE predictions SET created_at = '2026-04-01 12:00:00+00' WHERE id IN (%s, %s)",
        ["pred-tie-a", "pred-tie-b"],
    )

    rows = db.get_predictions_fat("0000000035")
    # Stable tie-break: p.id DESC => 'pred-tie-b' wins over 'pred-tie-a'.
    assert rows[0]["raw_score"] == pytest.approx(0.9)
    # Repeated calls return the same winner (deterministic).
    assert db.get_predictions_fat("0000000035")[0]["raw_score"] == pytest.approx(0.9)


def test_get_prediction_history_fat_deterministic_on_timestamp_tie():
    """History dedupe must pick the same row deterministically when timestamps tie."""
    db.upsert_company("0000000036")
    db.create_financial_report("rpt-500", "0000000036", 2023, "2023-01-01", "2023-12-31")
    db.register_model("mcz_v4", "maczynska", "discriminant", "4.0")
    db.create_prediction_run("run-tie-2", "mcz_v4")
    db.insert_prediction("pred-hist-a", "run-tie-2", "0000000036", "rpt-500",
                         raw_score=0.2, probability=None, classification=0, risk_category="high")
    db.insert_prediction("pred-hist-b", "run-tie-2", "0000000036", "rpt-500",
                         raw_score=0.8, probability=None, classification=0, risk_category="low")
    conn = db.get_conn()
    conn.execute(
        "UPDATE predictions SET created_at = '2026-04-01 09:00:00+00' WHERE id IN (%s, %s)",
        ["pred-hist-a", "pred-hist-b"],
    )

    history = db.get_prediction_history_fat("0000000036")
    assert len(history) == 1  # deduped
    # p.id DESC => 'pred-hist-b' wins.
    assert history[0]["raw_score"] == pytest.approx(0.8)
    # Stable across repeated calls.
    assert db.get_prediction_history_fat("0000000036")[0]["raw_score"] == pytest.approx(0.8)


def test_prediction_no_duplicate_id():
    db.upsert_company("0000000032")
    db.create_financial_report("rpt-103", "0000000032", 2023, "2023-01-01", "2023-12-31")
    db.register_model("rf_v2", "RF", "random_forest", "2.0")
    db.create_prediction_run("run-004", "rf_v2")
    db.insert_prediction("pred-020", "run-004", "0000000032", "rpt-103",
                          raw_score=1.0, probability=0.3, classification=0, risk_category="low")
    # Same id -> ON CONFLICT DO NOTHING
    db.insert_prediction("pred-020", "run-004", "0000000032", "rpt-103",
                          raw_score=99.0, probability=0.9, classification=1, risk_category="critical")
    conn = db.get_conn()
    count = conn.execute("SELECT count(*) FROM predictions WHERE id = 'pred-020'").fetchone()[0]
    assert count == 1


# ---------------------------------------------------------------------------
# Layer 5: bankruptcy_events
# ---------------------------------------------------------------------------

def test_insert_bankruptcy_event():
    db.insert_bankruptcy_event(
        "evt-001", "0000000040", "bankruptcy", "2022-06-15",
        court_case_ref="KRS/001/2022", is_confirmed=True,
    )
    events = db.get_bankruptcy_events("0000000040")
    assert len(events) == 1
    assert events[0]["event_type"] == "bankruptcy"
    assert events[0]["is_confirmed"] is True


def test_bankruptcy_event_unique_constraint():
    db.insert_bankruptcy_event("evt-002", "0000000041", "liquidation", "2021-03-01")
    # Duplicate (krs, event_type, event_date) -> do nothing
    db.insert_bankruptcy_event("evt-003", "0000000041", "liquidation", "2021-03-01")
    conn = db.get_conn()
    count = conn.execute(
        "SELECT count(*) FROM bankruptcy_events WHERE krs = '0000000041'"
    ).fetchone()[0]
    assert count == 1


def test_get_bankruptcy_events_empty():
    events = db.get_bankruptcy_events("9999999998")
    assert events == []


# ---------------------------------------------------------------------------
# Job tracking: assessment_jobs
# ---------------------------------------------------------------------------

def test_create_and_get_assessment_job():
    db.create_assessment_job("job-001", "0000000050")
    job = db.get_assessment_job("job-001")
    assert job is not None
    assert job["krs"] == "0000000050"
    assert job["status"] == "pending"
    assert job["stage"] is None


def test_update_assessment_job_stages():
    db.create_assessment_job("job-002", "0000000051")
    db.update_assessment_job("job-002", "running", stage="downloading")
    job = db.get_assessment_job("job-002")
    assert job["status"] == "running"
    assert job["stage"] == "downloading"

    db.update_assessment_job("job-002", "running", stage="parsing")
    job = db.get_assessment_job("job-002")
    assert job["stage"] == "parsing"

    db.update_assessment_job("job-002", "completed", stage="completed",
                              result={"risk_category": "low"})
    job = db.get_assessment_job("job-002")
    assert job["status"] == "completed"
    assert job["result_json"] is not None


def test_update_assessment_job_failed():
    db.create_assessment_job("job-003", "0000000052")
    db.update_assessment_job("job-003", "failed", error_message="XML parse error")
    job = db.get_assessment_job("job-003")
    assert job["status"] == "failed"
    assert job["error_message"] == "XML parse error"


def test_assessment_job_not_found():
    assert db.get_assessment_job("nonexistent-job") is None


def test_assessment_job_no_duplicate():
    db.create_assessment_job("job-004", "0000000053")
    db.create_assessment_job("job-004", "0000000053")  # ON CONFLICT DO NOTHING
    conn = db.get_conn()
    count = conn.execute("SELECT count(*) FROM assessment_jobs WHERE id = 'job-004'").fetchone()[0]
    assert count == 1


# ---------------------------------------------------------------------------
# R2-005: batched feature loader + snapshot + source-tag edge cases
# ---------------------------------------------------------------------------

def _seed_feature_set(set_id: str, feature_ids: list[str]) -> None:
    """Create feature definitions + a feature_set + members ordinal-by-ordinal."""
    for ordinal, fid in enumerate(feature_ids, start=1):
        db.upsert_feature_definition(
            feature_id=fid,
            name=fid.upper(),
            description=f"{fid} desc",
            category="test",
            formula_description=None,
            required_tags=["Aktywa"],
            computation_logic="ratio",
        )
    db.upsert_feature_set(set_id, f"Set {set_id}")
    for ordinal, fid in enumerate(feature_ids, start=1):
        db.add_feature_set_member(set_id, fid, ordinal)


def test_get_features_for_predictions_batch_exact_snapshot_happy_path():
    """Snapshot triples return the immutable rows even when newer versions exist."""
    db.upsert_company("0000000200")
    db.create_financial_report("rpt-r2-1", "0000000200", 2023, "2023-01-01", "2023-12-31")
    _seed_feature_set("set_r2_1", ["r2_f1", "r2_f2"])

    # Insert two computation versions for each feature. Snapshot pins v1.
    db.upsert_computed_feature("rpt-r2-1", "r2_f1", "0000000200", 2023, 1.0,
                               computation_version=1)
    db.upsert_computed_feature("rpt-r2-1", "r2_f2", "0000000200", 2023, 2.0,
                               computation_version=1)
    db.upsert_computed_feature("rpt-r2-1", "r2_f1", "0000000200", 2023, 99.0,
                               computation_version=2)
    db.upsert_computed_feature("rpt-r2-1", "r2_f2", "0000000200", 2023, 99.0,
                               computation_version=2)

    result = db.get_features_for_predictions_batch([{
        "request_id": "req-r2-1",
        "report_id": "rpt-r2-1",
        "feature_set_id": "set_r2_1",
        "feature_snapshot": {"r2_f1": 1, "r2_f2": 1},
        "scored_at": None,
    }])

    rows = result["req-r2-1"]
    assert len(rows) == 2
    by_id = {r["feature_definition_id"]: r for r in rows}
    assert by_id["r2_f1"]["value"] == pytest.approx(1.0)
    assert by_id["r2_f2"]["value"] == pytest.approx(2.0)
    # Snapshot rows must carry the pinned computation_version, not the latest.
    assert by_id["r2_f1"]["computation_version"] == 1
    # Ordering honours feature_set_members.ordinal.
    assert [r["feature_definition_id"] for r in rows] == ["r2_f1", "r2_f2"]


def test_get_features_for_predictions_batch_partial_snapshot_fallbacks(caplog):
    """When a snapshot version is missing, the key falls back to latest valid rows."""
    db.upsert_company("0000000201")
    db.create_financial_report("rpt-r2-2", "0000000201", 2022, "2022-01-01", "2022-12-31")
    _seed_feature_set("set_r2_2", ["r2_g1", "r2_g2"])

    # Only r2_g1 has version=1; r2_g2 only has version=3 (snapshot mentions v=2 which
    # was never materialised, simulating a corrupted / partial snapshot payload).
    db.upsert_computed_feature("rpt-r2-2", "r2_g1", "0000000201", 2022, 5.0,
                               computation_version=1)
    db.upsert_computed_feature("rpt-r2-2", "r2_g2", "0000000201", 2022, 7.0,
                               computation_version=3)

    import logging
    with caplog.at_level(logging.WARNING, logger="app.db.prediction_db"):
        result = db.get_features_for_predictions_batch([{
            "request_id": "req-r2-2",
            "report_id": "rpt-r2-2",
            "feature_set_id": "set_r2_2",
            "feature_snapshot": {"r2_g1": 1, "r2_g2": 2},  # v=2 does not exist
            "scored_at": None,
            "model_id": "test_model",
            "fiscal_year": 2022,
        }])

    rows = result["req-r2-2"]
    # Fallback returns the complete feature set — both features present.
    assert len(rows) == 2
    by_id = {r["feature_definition_id"]: r for r in rows}
    assert by_id["r2_g1"]["value"] == pytest.approx(5.0)
    assert by_id["r2_g2"]["value"] == pytest.approx(7.0)  # latest valid

    # Structured warning is emitted with the expected metadata.
    warnings = [r for r in caplog.records if r.message == "feature_snapshot_incomplete_fallback"]
    assert warnings, "expected a feature_snapshot_incomplete_fallback warning"
    w = warnings[0]
    assert getattr(w, "report_id", None) == "rpt-r2-2"
    assert getattr(w, "model_id", None) == "test_model"
    assert getattr(w, "fiscal_year", None) == 2022
    assert "r2_g2" in getattr(w, "missing", [])


def test_get_features_for_predictions_batch_exact_excludes_invalid_rows():
    """Invalid computed_features rows must never surface via the exact snapshot path."""
    db.upsert_company("0000000202")
    db.create_financial_report("rpt-r2-3", "0000000202", 2024, "2024-01-01", "2024-12-31")
    _seed_feature_set("set_r2_3", ["r2_h1"])

    db.upsert_computed_feature("rpt-r2-3", "r2_h1", "0000000202", 2024, None,
                               is_valid=False, error_message="div_by_zero",
                               computation_version=1)
    db.upsert_computed_feature("rpt-r2-3", "r2_h1", "0000000202", 2024, 3.0,
                               computation_version=2)

    # Snapshot mentions the invalid version -> exact path returns nothing
    # for that feature, completeness check fails, request falls back to
    # "latest valid" which picks version=2.
    result = db.get_features_for_predictions_batch([{
        "request_id": "req-r2-3",
        "report_id": "rpt-r2-3",
        "feature_set_id": "set_r2_3",
        "feature_snapshot": {"r2_h1": 1},
        "scored_at": None,
    }])
    rows = result["req-r2-3"]
    assert len(rows) == 1
    assert rows[0]["value"] == pytest.approx(3.0)
    assert rows[0]["computation_version"] == 2


def test_get_features_for_predictions_batch_fallback_is_single_query(monkeypatch):
    """Legacy requests (no snapshot) resolve in O(1) SQL calls, not O(N)."""
    db.upsert_company("0000000203")
    _seed_feature_set("set_r2_4", ["r2_k1"])

    report_ids = []
    for year in range(2015, 2025):  # 10 fiscal years
        rid = f"rpt-r2-4-{year}"
        db.create_financial_report(rid, "0000000203", year, f"{year}-01-01", f"{year}-12-31")
        db.upsert_computed_feature(rid, "r2_k1", "0000000203", year, float(year),
                                   computation_version=1)
        report_ids.append(rid)

    requests = [
        {
            "request_id": f"req-{rid}",
            "report_id": rid,
            "feature_set_id": "set_r2_4",
            "feature_snapshot": None,   # legacy path
            "scored_at": None,
        }
        for rid in report_ids
    ]

    # Count all cursor.execute calls made against the fallback path.
    call_count = {"n": 0}
    original_execute = db.get_conn().execute

    def counting_execute(sql, *args, **kwargs):
        if "computed_features" in sql and "row_number()" in sql:
            call_count["n"] += 1
        return original_execute(sql, *args, **kwargs)

    monkeypatch.setattr(db.get_conn(), "execute", counting_execute)

    result = db.get_features_for_predictions_batch(requests)

    # One batched query handles every legacy request.
    assert call_count["n"] == 1
    for rid in report_ids:
        rows = result[f"req-{rid}"]
        assert len(rows) == 1
        assert rows[0]["feature_definition_id"] == "r2_k1"


def test_get_source_line_items_for_reports_batch_respects_report_type_and_source():
    """Prior-year lookup must be constrained by data_source_id + report_type."""
    db.upsert_company("0000000204")

    # Current-year annual KRS report.
    db.create_financial_report("rpt-r2-5-cur", "0000000204", 2023, "2023-01-01", "2023-12-31",
                               report_type="annual", data_source_id="KRS")
    db.update_report_status("rpt-r2-5-cur", "completed")
    db.batch_insert_line_items([
        {"report_id": "rpt-r2-5-cur", "section": "Bilans", "tag_path": "Aktywa",
         "label_pl": "Aktywa", "value_current": 1000.0, "value_previous": None,
         "currency": "PLN", "schema_code": "SFJINZ"},
    ])

    # Prior-year annual KRS report (the *correct* match).
    db.create_financial_report("rpt-r2-5-prev-annual", "0000000204", 2022,
                               "2022-01-01", "2022-12-31",
                               report_type="annual", data_source_id="KRS")
    db.update_report_status("rpt-r2-5-prev-annual", "completed")
    db.batch_insert_line_items([
        {"report_id": "rpt-r2-5-prev-annual", "section": "Bilans", "tag_path": "Aktywa",
         "label_pl": "Aktywa", "value_current": 800.0, "value_previous": None,
         "currency": "PLN", "schema_code": "SFJINZ"},
    ])

    # Prior-year but different report_type — must be IGNORED.
    db.create_financial_report("rpt-r2-5-prev-interim", "0000000204", 2022,
                               "2022-01-01", "2022-06-30",
                               report_type="interim", data_source_id="KRS")
    db.update_report_status("rpt-r2-5-prev-interim", "completed")
    db.batch_insert_line_items([
        {"report_id": "rpt-r2-5-prev-interim", "section": "Bilans", "tag_path": "Aktywa",
         "label_pl": "Aktywa", "value_current": 5555.0, "value_previous": None,
         "currency": "PLN", "schema_code": "SFJINZ"},
    ])

    # Prior-year but different data_source_id — must also be IGNORED.
    db.create_financial_report("rpt-r2-5-prev-other", "0000000204", 2022,
                               "2022-01-01", "2022-12-31",
                               report_type="annual", data_source_id="OTHER")
    db.update_report_status("rpt-r2-5-prev-other", "completed")
    db.batch_insert_line_items([
        {"report_id": "rpt-r2-5-prev-other", "section": "Bilans", "tag_path": "Aktywa",
         "label_pl": "Aktywa", "value_current": 9999.0, "value_previous": None,
         "currency": "PLN", "schema_code": "SFJINZ"},
    ])

    result = db.get_source_line_items_for_reports_batch([("rpt-r2-5-cur", ["Aktywa"])])
    items = result["rpt-r2-5-cur"]
    assert len(items) == 1
    item = items[0]
    assert item["value_current"] == pytest.approx(1000.0)
    # value_previous must come from the matching (annual, KRS) report, not interim/OTHER.
    assert item["value_previous"] == pytest.approx(800.0)


def test_get_features_for_predictions_batch_same_report_distinct_snapshots():
    """R3-001/002: two requests sharing report+feature_set but with different
    snapshots must each get their own pinned feature versions — no merging."""
    db.upsert_company("0000000210")
    db.create_financial_report("rpt-r3-1", "0000000210", 2023, "2023-01-01", "2023-12-31")
    _seed_feature_set("set_r3_1", ["r3_f1"])

    # Two distinct valid computation versions exist.
    db.upsert_computed_feature("rpt-r3-1", "r3_f1", "0000000210", 2023, 10.0,
                               computation_version=1)
    db.upsert_computed_feature("rpt-r3-1", "r3_f1", "0000000210", 2023, 20.0,
                               computation_version=2)

    # Two requests for the same (report_id, feature_set_id) but different snapshot pins.
    result = db.get_features_for_predictions_batch([
        {
            "request_id": "req-r3-a",
            "report_id": "rpt-r3-1",
            "feature_set_id": "set_r3_1",
            "feature_snapshot": {"r3_f1": 1},
            "scored_at": None,
        },
        {
            "request_id": "req-r3-b",
            "report_id": "rpt-r3-1",
            "feature_set_id": "set_r3_1",
            "feature_snapshot": {"r3_f1": 2},
            "scored_at": None,
        },
    ])

    # Each request gets exactly its pinned version — no duplication, no mixing.
    rows_a = result["req-r3-a"]
    rows_b = result["req-r3-b"]
    assert len(rows_a) == 1
    assert len(rows_b) == 1
    assert rows_a[0]["value"] == pytest.approx(10.0)
    assert rows_a[0]["computation_version"] == 1
    assert rows_b[0]["value"] == pytest.approx(20.0)
    assert rows_b[0]["computation_version"] == 2


def test_get_features_for_predictions_batch_fallback_respects_scored_at_per_request():
    """R3-003: fallback window partition must honour each request's scored_at
    so two requests with the same report+feature_set but different scored_at
    pick different feature versions."""
    db.upsert_company("0000000211")
    db.create_financial_report("rpt-r3-2", "0000000211", 2024, "2024-01-01", "2024-12-31")
    _seed_feature_set("set_r3_2", ["r3_g1"])

    # Two versions of the same feature with distinct computed_at timestamps.
    db.upsert_computed_feature("rpt-r3-2", "r3_g1", "0000000211", 2024, 1.0,
                               computation_version=1)
    db.upsert_computed_feature("rpt-r3-2", "r3_g1", "0000000211", 2024, 9.0,
                               computation_version=2)
    conn = db.get_conn()
    conn.execute("""
        UPDATE computed_features SET computed_at = '2024-06-01 12:00:00+00'
        WHERE report_id = 'rpt-r3-2' AND computation_version = 1
    """)
    conn.execute("""
        UPDATE computed_features SET computed_at = '2025-06-01 12:00:00+00'
        WHERE report_id = 'rpt-r3-2' AND computation_version = 2
    """)

    # Request A was scored before v2 existed; B was scored after.
    result = db.get_features_for_predictions_batch([
        {
            "request_id": "req-r3-early",
            "report_id": "rpt-r3-2",
            "feature_set_id": "set_r3_2",
            "feature_snapshot": None,  # legacy → fallback path
            "scored_at": "2024-12-31 00:00:00+00",
        },
        {
            "request_id": "req-r3-late",
            "report_id": "rpt-r3-2",
            "feature_set_id": "set_r3_2",
            "feature_snapshot": None,
            "scored_at": "2025-12-31 00:00:00+00",
        },
    ])

    rows_early = result["req-r3-early"]
    rows_late = result["req-r3-late"]
    assert len(rows_early) == 1
    assert len(rows_late) == 1
    # Early request sees only v1 (v2 didn't exist yet); late request sees v2.
    assert rows_early[0]["value"] == pytest.approx(1.0)
    assert rows_early[0]["computation_version"] == 1
    assert rows_late[0]["value"] == pytest.approx(9.0)
    assert rows_late[0]["computation_version"] == 2


def test_get_features_for_predictions_batch_rejects_duplicate_request_ids():
    """Defensive guardrail — the loader refuses duplicate request_ids."""
    with pytest.raises(ValueError, match="duplicate request_id"):
        db.get_features_for_predictions_batch([
            {"request_id": "dup", "report_id": "r", "feature_set_id": "s",
             "feature_snapshot": None, "scored_at": None},
            {"request_id": "dup", "report_id": "r", "feature_set_id": "s",
             "feature_snapshot": None, "scored_at": None},
        ])


def test_x1_maczynska_migration_hardens_null_required_tags():
    """R2-003: backfill updates both stale and NULL required_tags rows."""
    conn = db.get_conn()
    # Seed a stale row missing CF.A_II_1 entirely.
    db.upsert_feature_definition(
        feature_id="x1_maczynska",
        name="Maczynska X1",
        formula_description="(RZiS.I + CF.A_II_1) / Pasywa_B",
        required_tags=["RZiS.I", "Pasywa_B"],
        computation_logic="custom",
    )
    # Force a NULL required_tags directly in the DB.
    conn.execute("UPDATE feature_definitions SET required_tags = NULL WHERE id = 'x1_maczynska'")

    # Re-run schema init: migration block must fix both cases idempotently.
    db._schema_initialized = False
    db._init_schema()

    row = conn.execute(
        "SELECT required_tags::text FROM feature_definitions WHERE id = 'x1_maczynska'"
    ).fetchone()
    assert row is not None
    assert "CF.A_II_1" in row[0]
    assert "RZiS.I" in row[0]
    assert "Pasywa_B" in row[0]

    # Re-running the migration is a no-op (idempotent).
    db._schema_initialized = False
    db._init_schema()
    row2 = conn.execute(
        "SELECT required_tags::text FROM feature_definitions WHERE id = 'x1_maczynska'"
    ).fetchone()
    assert row2[0] == row[0]
