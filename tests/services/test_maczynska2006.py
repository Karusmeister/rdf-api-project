"""Tests for the Maczynska-Zawadzki (2006) SHV discriminant model."""

from unittest.mock import patch

import pytest

from app.config import settings
from app.db import connection as db_conn
from app.db import prediction_db
from app.scraper import db as scraper_db
from app.services import feature_engine, maczynska2006, predictions
from scripts.seed_features import FEATURE_DEFINITIONS, FEATURE_SETS


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def isolated_db(pg_dsn, clean_pg):
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
    predictions.register_builtin_models()
    return isolated_db


def _create_report_with_tags(krs, report_id, tag_values):
    """Helper: create a company + report + line items from a {tag_path: value} dict."""
    prediction_db.upsert_company(krs=krs)
    prediction_db.create_financial_report(
        report_id=report_id, krs=krs, fiscal_year=2023,
        period_start="2023-01-01", period_end="2023-12-31",
    )
    prediction_db.update_report_status(report_id, "completed")

    section_map = {
        "Aktywa": "Bilans", "Aktywa_B": "Bilans", "Aktywa_B_I": "Bilans",
        "Pasywa_A": "Bilans", "Pasywa_B": "Bilans", "Pasywa_B_III": "Bilans",
    }
    items = []
    for tag, value in tag_values.items():
        section = section_map.get(tag, "CF" if tag.startswith("CF.") else "RZiS")
        items.append({
            "report_id": report_id, "section": section,
            "tag_path": tag, "value_current": value,
        })
    prediction_db.batch_insert_line_items(items)


def _full_tagset(overrides=None):
    """Baseline healthy tagset for Maczynska 2006 producing ZMZ > 1."""
    base = {
        "RZiS.F": 200000,      # Operating profit
        "RZiS.L": 150000,      # Net profit
        "RZiS.A": 2000000,     # Revenue
        "CF.A_II_1": 50000,    # Depreciation
        "Aktywa": 1000000,     # Total assets
        "Aktywa_B": 600000,    # Current assets
        "Pasywa_A": 500000,    # Equity
        "Pasywa_B": 500000,    # Total liabilities
        "Pasywa_B_III": 250000,  # Short-term liabilities
    }
    if overrides:
        base.update(overrides)
    return base


# ---------------------------------------------------------------------------
# W3 custom computation tests
# ---------------------------------------------------------------------------


class TestW3Maczynska2006:
    def test_happy_path(self, seeded_db):
        _create_report_with_tags("0000100001", "w3-happy", _full_tagset())
        result = feature_engine.compute_features_for_report("w3-happy", feature_set_id="maczynska2006_4")
        # (150000 + 50000) / 500000 = 0.4
        assert result["features"]["w3_maczynska2006"] == pytest.approx(0.4, abs=1e-4)

    def test_missing_net_profit_returns_none(self, seeded_db):
        tags = _full_tagset()
        del tags["RZiS.L"]
        _create_report_with_tags("0000100002", "w3-no-net", tags)
        result = feature_engine.compute_features_for_report("w3-no-net", feature_set_id="maczynska2006_4")
        assert result["features"]["w3_maczynska2006"] is None

    def test_missing_depreciation_uses_zero(self, seeded_db):
        tags = _full_tagset()
        del tags["CF.A_II_1"]
        _create_report_with_tags("0000100003", "w3-no-dep", tags)
        result = feature_engine.compute_features_for_report("w3-no-dep", feature_set_id="maczynska2006_4")
        # (150000 + 0) / 500000 = 0.3
        assert result["features"]["w3_maczynska2006"] == pytest.approx(0.3, abs=1e-4)

    def test_zero_liabilities_returns_none(self, seeded_db):
        _create_report_with_tags("0000100004", "w3-zero-liab", _full_tagset({"Pasywa_B": 0}))
        result = feature_engine.compute_features_for_report("w3-zero-liab", feature_set_id="maczynska2006_4")
        assert result["features"]["w3_maczynska2006"] is None


# ---------------------------------------------------------------------------
# Classification tests
# ---------------------------------------------------------------------------


class TestClassify:
    def test_critical(self):
        assert maczynska2006.classify(-0.5) == (1, "critical")

    def test_medium(self):
        assert maczynska2006.classify(0.5) == (0, "medium")

    def test_low(self):
        assert maczynska2006.classify(2.5) == (0, "low")

    def test_boundary_zero(self):
        assert maczynska2006.classify(0.0) == (0, "medium")

    def test_boundary_one(self):
        assert maczynska2006.classify(1.0) == (0, "low")


# ---------------------------------------------------------------------------
# score_report tests
# ---------------------------------------------------------------------------


class TestScoreReport:
    def test_healthy_company(self, seeded_db):
        _create_report_with_tags("0000200001", "healthy-mz06", _full_tagset())
        feature_engine.compute_features_for_report("healthy-mz06", feature_set_id="maczynska2006_4")

        result = maczynska2006.score_report("healthy-mz06")
        assert result is not None
        # Expected Z:
        #   W1 = 200000/1000000 = 0.2           -> 9.498 * 0.2  = 1.8996
        #   W2 = 500000/1000000 = 0.5           -> 3.566 * 0.5  = 1.783
        #   W3 = (150000+50000)/500000 = 0.4    -> 2.903 * 0.4  = 1.1612
        #   W4 = 600000/250000 = 2.4            -> 0.452 * 2.4  = 1.0848
        #   intercept = -1.498
        #   Z = 1.8996 + 1.783 + 1.1612 + 1.0848 - 1.498 = 4.4306
        assert result["raw_score"] == pytest.approx(4.4306, abs=1e-3)
        assert result["classification"] == 0
        assert result["risk_category"] == "low"

    def test_bankrupt_company(self, seeded_db):
        tags = _full_tagset({
            "RZiS.F": -300000,
            "RZiS.L": -400000,
            "Pasywa_A": 50000,
            "Pasywa_B": 950000,
            "Pasywa_B_III": 800000,
            "Aktywa_B": 300000,
        })
        _create_report_with_tags("0000200002", "bankrupt-mz06", tags)
        feature_engine.compute_features_for_report("bankrupt-mz06", feature_set_id="maczynska2006_4")

        result = maczynska2006.score_report("bankrupt-mz06")
        assert result is not None
        assert result["classification"] == 1
        assert result["risk_category"] == "critical"
        assert result["raw_score"] < 0

    def test_missing_features_returns_none(self, seeded_db):
        _create_report_with_tags("0000200003", "incomplete-mz06", {
            "RZiS.F": 100000, "Aktywa": 500000,
        })
        feature_engine.compute_features_for_report("incomplete-mz06", feature_set_id="maczynska2006_4")
        assert maczynska2006.score_report("incomplete-mz06") is None

    def test_intercept_included_in_contributions(self, seeded_db):
        _create_report_with_tags("0000200004", "intercept-mz06", _full_tagset())
        feature_engine.compute_features_for_report("intercept-mz06", feature_set_id="maczynska2006_4")

        result = maczynska2006.score_report("intercept-mz06")
        assert "_intercept" in result["feature_contributions"]
        assert result["feature_contributions"]["_intercept"] == pytest.approx(-1.498, abs=1e-6)

        numeric = {
            k: v for k, v in result["feature_contributions"].items()
            if not k.startswith("_") or k == "_intercept"
        }
        total = sum(numeric.values())
        assert total == pytest.approx(result["raw_score"], abs=1e-3)


# ---------------------------------------------------------------------------
# Model registration tests
# ---------------------------------------------------------------------------


class TestEnsureModelRegistered:
    def test_registers_model(self, isolated_db):
        maczynska2006.ensure_model_registered()
        models = prediction_db.get_active_models()
        m = [m for m in models if m["id"] == maczynska2006.MODEL_ID]
        assert len(m) == 1
        assert m[0]["is_baseline"] is True
        assert m[0]["model_type"] == "discriminant"

    def test_idempotent(self, isolated_db):
        maczynska2006.ensure_model_registered()
        maczynska2006.ensure_model_registered()
        models = prediction_db.get_active_models()
        m = [m for m in models if m["id"] == maczynska2006.MODEL_ID]
        assert len(m) == 1


# ---------------------------------------------------------------------------
# Batch scoring tests
# ---------------------------------------------------------------------------


class TestScoreBatch:
    def test_scores_multiple_reports(self, seeded_db):
        _create_report_with_tags("0000300001", "mz06-batch-1", _full_tagset())
        feature_engine.compute_features_for_report("mz06-batch-1", feature_set_id="maczynska2006_4")

        _create_report_with_tags("0000300002", "mz06-batch-2", _full_tagset({"RZiS.F": 80000}))
        feature_engine.compute_features_for_report("mz06-batch-2", feature_set_id="maczynska2006_4")

        _create_report_with_tags("0000300003", "mz06-batch-3", {"Aktywa": 500000})
        feature_engine.compute_features_for_report("mz06-batch-3", feature_set_id="maczynska2006_4")

        result = maczynska2006.score_batch(["mz06-batch-1", "mz06-batch-2", "mz06-batch-3"])
        assert result["scored"] == 2
        assert result["skipped"] == 1
        assert result["run_id"] is not None

    def test_creates_prediction_run(self, seeded_db):
        _create_report_with_tags("0000300004", "mz06-run-rpt", _full_tagset())
        feature_engine.compute_features_for_report("mz06-run-rpt", feature_set_id="maczynska2006_4")

        result = maczynska2006.score_batch(["mz06-run-rpt"])
        assert result["scored"] == 1

        conn = prediction_db.get_conn()
        run = conn.execute(
            "SELECT status, companies_scored FROM prediction_runs WHERE id = %s",
            [result["run_id"]],
        ).fetchone()
        assert run[0] == "completed"
        assert run[1] == 1

    def test_predictions_persisted(self, seeded_db):
        _create_report_with_tags("0000300005", "mz06-persist", _full_tagset())
        feature_engine.compute_features_for_report("mz06-persist", feature_set_id="maczynska2006_4")
        maczynska2006.score_batch(["mz06-persist"])

        pred = prediction_db.get_latest_prediction("0000300005")
        assert pred is not None
        assert pred["krs"] == "0000300005"
        assert pred["raw_score"] is not None
        assert pred["risk_category"] in ("low", "medium", "high", "critical")

    def test_auto_discover_unscored(self, seeded_db):
        _create_report_with_tags("0000300006", "mz06-auto", _full_tagset())
        feature_engine.compute_features_for_report("mz06-auto", feature_set_id="maczynska2006_4")

        result = maczynska2006.score_batch(report_ids=None)
        assert result["scored"] == 1

        result2 = maczynska2006.score_batch(report_ids=None)
        assert result2["scored"] == 0


# ---------------------------------------------------------------------------
# Batch finalization on failure
# ---------------------------------------------------------------------------


class TestBatchFinalizationOnFailure:
    def test_bulk_read_failure_finalizes_run_as_failed(self, seeded_db, monkeypatch):
        _create_report_with_tags("0000300900", "mz06-rel-rpt", _full_tagset())
        feature_engine.compute_features_for_report(
            "mz06-rel-rpt", feature_set_id="maczynska2006_4"
        )

        def _boom(*args, **kwargs):
            raise RuntimeError("simulated DB outage during bulk-load")

        monkeypatch.setattr(
            prediction_db, "get_computed_features_for_reports_batch", _boom
        )

        with pytest.raises(RuntimeError, match="simulated DB outage"):
            maczynska2006.score_batch(["mz06-rel-rpt"])

        conn = prediction_db.get_conn()
        row = conn.execute(
            """
            SELECT status, error_message
            FROM prediction_runs
            WHERE model_id = %s
            ORDER BY created_at DESC
            LIMIT 1
            """,
            [maczynska2006.MODEL_ID],
        ).fetchone()
        assert row is not None
        status, error_message = row
        assert status == "failed"
        assert error_message and error_message.startswith("batch_error:")
        assert "simulated DB outage" not in (error_message or "")

    def test_bulk_insert_failure_finalizes_run_as_failed(self, seeded_db, monkeypatch):
        _create_report_with_tags("0000300901", "mz06-rel-rpt-2", _full_tagset())
        feature_engine.compute_features_for_report(
            "mz06-rel-rpt-2", feature_set_id="maczynska2006_4"
        )

        def _boom(*args, **kwargs):
            raise RuntimeError("simulated DB outage during bulk-insert")

        monkeypatch.setattr(prediction_db, "insert_predictions_batch", _boom)

        with pytest.raises(RuntimeError, match="simulated DB outage"):
            maczynska2006.score_batch(["mz06-rel-rpt-2"])

        conn = prediction_db.get_conn()
        row = conn.execute(
            """
            SELECT status
            FROM prediction_runs
            WHERE model_id = %s
            ORDER BY created_at DESC
            LIMIT 1
            """,
            [maczynska2006.MODEL_ID],
        ).fetchone()
        assert row is not None
        assert row[0] == "failed"
