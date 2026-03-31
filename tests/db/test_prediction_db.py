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
