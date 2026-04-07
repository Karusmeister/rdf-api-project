"""Tests for the Prusak P1 (2005) discriminant model."""

from unittest.mock import patch

import pytest

from app.config import settings
from app.db import connection as db_conn
from app.db import prediction_db
from app.scraper import db as scraper_db
from app.services import feature_engine, prusak, predictions
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
    """Baseline healthy tagset for Prusak P1 producing P1 > 0.65."""
    base = {
        "RZiS.L": 150000,      # Net profit
        "RZiS.F": 200000,      # Operating profit
        "RZiS.A": 2000000,     # Revenue
        "RZiS.B": 1800000,     # Operating costs
        "Aktywa": 1000000,     # Total assets
        "Aktywa_B": 600000,    # Current assets
        "Pasywa_B": 500000,    # Total liabilities
        "Pasywa_B_III": 250000,  # Short-term liabilities
    }
    if overrides:
        base.update(overrides)
    return base


# ---------------------------------------------------------------------------
# Classification tests
# ---------------------------------------------------------------------------


class TestClassify:
    def test_critical(self):
        assert prusak.classify(-0.5) == (1, "critical")

    def test_medium(self):
        assert prusak.classify(0.0) == (0, "medium")

    def test_low(self):
        assert prusak.classify(1.0) == (0, "low")

    def test_boundary_critical(self):
        # -0.13 is the boundary — exactly at it means grey zone (medium)
        assert prusak.classify(-0.13) == (0, "medium")

    def test_boundary_low(self):
        # 0.65 is the boundary — exactly at it means safe (low)
        assert prusak.classify(0.65) == (0, "low")

    def test_deep_negative(self):
        assert prusak.classify(-5.0) == (1, "critical")


# ---------------------------------------------------------------------------
# score_report tests
# ---------------------------------------------------------------------------


class TestScoreReport:
    def test_healthy_company(self, seeded_db):
        _create_report_with_tags("0000200001", "healthy-prk", _full_tagset())
        feature_engine.compute_features_for_report("healthy-prk", feature_set_id="prusak_p1_4")

        result = prusak.score_report("healthy-prk")
        assert result is not None
        # Expected P1:
        #   X1 = 150000/1000000 = 0.15           -> 6.5245 * 0.15 = 0.978675
        #   X2 = 1800000/250000 = 7.2            -> 0.1480 * 7.2  = 1.0656
        #   X3 = 600000/500000 = 1.2             -> 0.4061 * 1.2  = 0.48732
        #   X4 = 200000/2000000 = 0.1            -> 2.1754 * 0.1  = 0.21754
        #   intercept = -1.5685
        #   P1 = 0.978675 + 1.0656 + 0.48732 + 0.21754 - 1.5685 = 1.180635
        assert result["raw_score"] == pytest.approx(1.180635, abs=1e-3)
        assert result["classification"] == 0
        assert result["risk_category"] == "low"

    def test_bankrupt_company(self, seeded_db):
        tags = _full_tagset({
            "RZiS.L": -400000,
            "RZiS.F": -300000,
            "RZiS.B": 2300000,
            "Pasywa_B": 900000,
            "Pasywa_B_III": 800000,
            "Aktywa_B": 300000,
        })
        _create_report_with_tags("0000200002", "bankrupt-prk", tags)
        feature_engine.compute_features_for_report("bankrupt-prk", feature_set_id="prusak_p1_4")

        result = prusak.score_report("bankrupt-prk")
        assert result is not None
        assert result["classification"] == 1
        assert result["risk_category"] == "critical"
        assert result["raw_score"] < -0.13

    def test_missing_features_returns_none(self, seeded_db):
        _create_report_with_tags("0000200003", "incomplete-prk", {
            "RZiS.L": 100000, "Aktywa": 500000,
        })
        feature_engine.compute_features_for_report("incomplete-prk", feature_set_id="prusak_p1_4")
        assert prusak.score_report("incomplete-prk") is None

    def test_intercept_included_in_contributions(self, seeded_db):
        _create_report_with_tags("0000200004", "intercept-prk", _full_tagset())
        feature_engine.compute_features_for_report("intercept-prk", feature_set_id="prusak_p1_4")

        result = prusak.score_report("intercept-prk")
        assert "_intercept" in result["feature_contributions"]
        assert result["feature_contributions"]["_intercept"] == pytest.approx(-1.5685, abs=1e-6)

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
        prusak.ensure_model_registered()
        models = prediction_db.get_active_models()
        m = [m for m in models if m["id"] == prusak.MODEL_ID]
        assert len(m) == 1
        assert m[0]["model_type"] == "discriminant"

    def test_idempotent(self, isolated_db):
        prusak.ensure_model_registered()
        prusak.ensure_model_registered()
        models = prediction_db.get_active_models()
        m = [m for m in models if m["id"] == prusak.MODEL_ID]
        assert len(m) == 1


# ---------------------------------------------------------------------------
# Batch scoring tests
# ---------------------------------------------------------------------------


class TestScoreBatch:
    def test_scores_multiple_reports(self, seeded_db):
        _create_report_with_tags("0000300001", "prk-batch-1", _full_tagset())
        feature_engine.compute_features_for_report("prk-batch-1", feature_set_id="prusak_p1_4")

        _create_report_with_tags("0000300002", "prk-batch-2", _full_tagset({"RZiS.L": 80000}))
        feature_engine.compute_features_for_report("prk-batch-2", feature_set_id="prusak_p1_4")

        _create_report_with_tags("0000300003", "prk-batch-3", {"Aktywa": 500000})
        feature_engine.compute_features_for_report("prk-batch-3", feature_set_id="prusak_p1_4")

        result = prusak.score_batch(["prk-batch-1", "prk-batch-2", "prk-batch-3"])
        assert result["scored"] == 2
        assert result["skipped"] == 1
        assert result["run_id"] is not None

    def test_creates_prediction_run(self, seeded_db):
        _create_report_with_tags("0000300004", "prk-run-rpt", _full_tagset())
        feature_engine.compute_features_for_report("prk-run-rpt", feature_set_id="prusak_p1_4")

        result = prusak.score_batch(["prk-run-rpt"])
        assert result["scored"] == 1

        conn = prediction_db.get_conn()
        run = conn.execute(
            "SELECT status, companies_scored FROM prediction_runs WHERE id = %s",
            [result["run_id"]],
        ).fetchone()
        assert run[0] == "completed"
        assert run[1] == 1

    def test_predictions_persisted(self, seeded_db):
        _create_report_with_tags("0000300005", "prk-persist", _full_tagset())
        feature_engine.compute_features_for_report("prk-persist", feature_set_id="prusak_p1_4")
        prusak.score_batch(["prk-persist"])

        pred = prediction_db.get_latest_prediction("0000300005")
        assert pred is not None
        assert pred["krs"] == "0000300005"
        assert pred["raw_score"] is not None
        assert pred["risk_category"] in ("low", "medium", "high", "critical")

    def test_auto_discover_unscored(self, seeded_db):
        _create_report_with_tags("0000300006", "prk-auto", _full_tagset())
        feature_engine.compute_features_for_report("prk-auto", feature_set_id="prusak_p1_4")

        result = prusak.score_batch(report_ids=None)
        assert result["scored"] == 1

        result2 = prusak.score_batch(report_ids=None)
        assert result2["scored"] == 0


# ---------------------------------------------------------------------------
# Batch finalization on failure
# ---------------------------------------------------------------------------


class TestBatchFinalizationOnFailure:
    def test_bulk_read_failure_finalizes_run_as_failed(self, seeded_db, monkeypatch):
        _create_report_with_tags("0000300900", "prk-rel-rpt", _full_tagset())
        feature_engine.compute_features_for_report(
            "prk-rel-rpt", feature_set_id="prusak_p1_4"
        )

        def _boom(*args, **kwargs):
            raise RuntimeError("simulated DB outage during bulk-load")

        monkeypatch.setattr(
            prediction_db, "get_computed_features_for_reports_batch", _boom
        )

        with pytest.raises(RuntimeError, match="simulated DB outage"):
            prusak.score_batch(["prk-rel-rpt"])

        conn = prediction_db.get_conn()
        row = conn.execute(
            """
            SELECT status, error_message
            FROM prediction_runs
            WHERE model_id = %s
            ORDER BY created_at DESC
            LIMIT 1
            """,
            [prusak.MODEL_ID],
        ).fetchone()
        assert row is not None
        status, error_message = row
        assert status == "failed"
        assert error_message and error_message.startswith("batch_error:")
        assert "simulated DB outage" not in (error_message or "")

    def test_bulk_insert_failure_finalizes_run_as_failed(self, seeded_db, monkeypatch):
        _create_report_with_tags("0000300901", "prk-rel-rpt-2", _full_tagset())
        feature_engine.compute_features_for_report(
            "prk-rel-rpt-2", feature_set_id="prusak_p1_4"
        )

        def _boom(*args, **kwargs):
            raise RuntimeError("simulated DB outage during bulk-insert")

        monkeypatch.setattr(prediction_db, "insert_predictions_batch", _boom)

        with pytest.raises(RuntimeError, match="simulated DB outage"):
            prusak.score_batch(["prk-rel-rpt-2"])

        conn = prediction_db.get_conn()
        row = conn.execute(
            """
            SELECT status
            FROM prediction_runs
            WHERE model_id = %s
            ORDER BY created_at DESC
            LIMIT 1
            """,
            [prusak.MODEL_ID],
        ).fetchone()
        assert row is not None
        assert row[0] == "failed"
