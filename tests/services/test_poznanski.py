"""Tests for the Poznanski (Hamrol/Czajka/Piechocki 2004) discriminant model."""

from unittest.mock import patch

import pytest

from app.config import settings
from app.db import connection as db_conn
from app.db import prediction_db
from app.scraper import db as scraper_db
from app.services import feature_engine, poznanski, predictions
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
    # CR-PZN-001: mirror production startup — register built-in models up front
    # instead of relying on `score_batch` side effects.
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
        "Pasywa_A": "Bilans", "Pasywa_B_II": "Bilans", "Pasywa_B_III": "Bilans",
    }
    items = []
    for tag, value in tag_values.items():
        section = section_map.get(tag, "RZiS")
        items.append({
            "report_id": report_id, "section": section,
            "tag_path": tag, "value_current": value,
        })
    prediction_db.batch_insert_line_items(items)


def _full_tagset(overrides=None):
    """Baseline healthy tagset producing Z > 1."""
    base = {
        "RZiS.L": 150000,    # Net profit
        "RZiS.C": 300000,    # Profit on sales (Zysk ze sprzedazy)
        "RZiS.A": 2000000,   # Net revenue
        "Aktywa": 1000000,   # Total assets
        "Aktywa_B": 600000,  # Current assets
        "Aktywa_B_I": 100000,  # Inventory
        "Pasywa_A": 500000,  # Equity
        "Pasywa_B_II": 200000,  # Long-term liabilities
        "Pasywa_B_III": 250000,  # Short-term liabilities
    }
    if overrides:
        base.update(overrides)
    return base


# ---------------------------------------------------------------------------
# Feature computation tests
# ---------------------------------------------------------------------------


class TestX2Poznanski:
    def test_happy_path(self, seeded_db):
        _create_report_with_tags("0000100001", "x2p-happy", _full_tagset())
        result = feature_engine.compute_features_for_report("x2p-happy", feature_set_id="poznanski_4")
        # (600000 - 100000) / 250000 = 2.0
        assert result["features"]["x2_poznanski"] == pytest.approx(2.0, abs=1e-4)

    def test_zero_st_liabilities_returns_none(self, seeded_db):
        _create_report_with_tags("0000100002", "x2p-zero", _full_tagset({"Pasywa_B_III": 0}))
        result = feature_engine.compute_features_for_report("x2p-zero", feature_set_id="poznanski_4")
        assert result["features"]["x2_poznanski"] is None

    def test_missing_inventory_treated_as_zero(self, seeded_db):
        tags = _full_tagset()
        del tags["Aktywa_B_I"]
        _create_report_with_tags("0000100003", "x2p-noinv", tags)
        result = feature_engine.compute_features_for_report("x2p-noinv", feature_set_id="poznanski_4")
        # (600000 - 0) / 250000 = 2.4
        assert result["features"]["x2_poznanski"] == pytest.approx(2.4, abs=1e-4)


class TestX3Poznanski:
    def test_happy_path(self, seeded_db):
        _create_report_with_tags("0000100010", "x3p-happy", _full_tagset())
        result = feature_engine.compute_features_for_report("x3p-happy", feature_set_id="poznanski_4")
        # (500000 + 200000) / 1000000 = 0.7
        assert result["features"]["x3_poznanski"] == pytest.approx(0.7, abs=1e-4)

    def test_missing_lt_liabilities_treated_as_zero(self, seeded_db):
        tags = _full_tagset()
        del tags["Pasywa_B_II"]
        _create_report_with_tags("0000100011", "x3p-nolt", tags)
        result = feature_engine.compute_features_for_report("x3p-nolt", feature_set_id="poznanski_4")
        # (500000 + 0) / 1000000 = 0.5
        assert result["features"]["x3_poznanski"] == pytest.approx(0.5, abs=1e-4)

    def test_zero_total_assets_returns_none(self, seeded_db):
        _create_report_with_tags("0000100012", "x3p-zero", _full_tagset({"Aktywa": 0}))
        result = feature_engine.compute_features_for_report("x3p-zero", feature_set_id="poznanski_4")
        assert result["features"]["x3_poznanski"] is None


# ---------------------------------------------------------------------------
# Classification tests
# ---------------------------------------------------------------------------


class TestClassify:
    def test_critical(self):
        assert poznanski.classify(-0.5) == (1, "critical")

    def test_medium(self):
        assert poznanski.classify(0.5) == (0, "medium")

    def test_low(self):
        assert poznanski.classify(2.5) == (0, "low")

    def test_boundary_zero(self):
        # Z == 0 is "safe" per published model (>= 0)
        assert poznanski.classify(0.0) == (0, "medium")

    def test_boundary_one(self):
        assert poznanski.classify(1.0) == (0, "low")


# ---------------------------------------------------------------------------
# score_report tests
# ---------------------------------------------------------------------------


class TestScoreReport:
    def test_healthy_company(self, seeded_db):
        _create_report_with_tags("0000200001", "healthy-pzn", _full_tagset())
        feature_engine.compute_features_for_report("healthy-pzn", feature_set_id="poznanski_4")

        result = poznanski.score_report("healthy-pzn")
        assert result is not None
        # Expected Z:
        #   X1 = 150000/1000000 = 0.15           -> 3.562 * 0.15 = 0.5343
        #   X2 = (600000-100000)/250000 = 2.0    -> 1.588 * 2.0  = 3.176
        #   X3 = (500000+200000)/1000000 = 0.7   -> 4.288 * 0.7  = 3.0016
        #   X4 = 300000/2000000 = 0.15           -> 6.719 * 0.15 = 1.00785
        #   intercept = -2.368
        #   Z = 0.5343 + 3.176 + 3.0016 + 1.00785 - 2.368 = 5.35175
        assert result["raw_score"] == pytest.approx(5.35175, abs=1e-3)
        assert result["classification"] == 0
        assert result["risk_category"] == "low"
        assert result["warnings"] == []

    def test_bankrupt_company(self, seeded_db):
        # Net losses + weak capital structure
        tags = _full_tagset({
            "RZiS.L": -400000,
            "RZiS.C": -200000,
            "Pasywa_A": 50000,
            "Pasywa_B_II": 0,
            "Pasywa_B_III": 800000,
            "Aktywa_B": 300000,
            "Aktywa_B_I": 150000,
        })
        _create_report_with_tags("0000200002", "bankrupt-pzn", tags)
        feature_engine.compute_features_for_report("bankrupt-pzn", feature_set_id="poznanski_4")

        result = poznanski.score_report("bankrupt-pzn")
        assert result is not None
        assert result["classification"] == 1
        assert result["risk_category"] == "critical"
        assert result["raw_score"] < 0

    def test_missing_features_returns_none(self, seeded_db):
        _create_report_with_tags("0000200003", "incomplete-pzn", {
            "RZiS.L": 100000, "Aktywa": 500000,
        })
        feature_engine.compute_features_for_report("incomplete-pzn", feature_set_id="poznanski_4")
        assert poznanski.score_report("incomplete-pzn") is None

    def test_intercept_included_in_contributions(self, seeded_db):
        _create_report_with_tags("0000200004", "intercept-pzn", _full_tagset())
        feature_engine.compute_features_for_report("intercept-pzn", feature_set_id="poznanski_4")

        result = poznanski.score_report("intercept-pzn")
        assert "_intercept" in result["feature_contributions"]
        assert result["feature_contributions"]["_intercept"] == pytest.approx(-2.368, abs=1e-6)

        # Sum of all numeric contributions equals raw_score
        numeric = {
            k: v for k, v in result["feature_contributions"].items()
            if not k.startswith("_") or k == "_intercept"
        }
        total = sum(numeric.values())
        assert total == pytest.approx(result["raw_score"], abs=1e-3)


# ---------------------------------------------------------------------------
# U-shape liquidity warning tests
# ---------------------------------------------------------------------------


class TestNonLinearLiquidityWarning:
    def test_high_x2_flags_warning(self, seeded_db):
        # Extreme quick ratio: (1_000_000 - 0) / 100_000 = 10
        tags = _full_tagset({
            "Aktywa_B": 1000000,
            "Aktywa_B_I": 0,
            "Pasywa_B_III": 100000,
        })
        _create_report_with_tags("0000200005", "highx2-pzn", tags)
        feature_engine.compute_features_for_report("highx2-pzn", feature_set_id="poznanski_4")

        result = poznanski.score_report("highx2-pzn")
        assert result is not None
        assert poznanski.WARNING_NON_LINEAR_LIQUIDITY in result["warnings"]
        assert result["feature_contributions"]["_warnings"] == [
            poznanski.WARNING_NON_LINEAR_LIQUIDITY
        ]
        # A "low" risk verdict should have been downgraded to "medium"
        assert result["risk_category"] == "medium"

    def test_normal_x2_no_warning(self, seeded_db):
        _create_report_with_tags("0000200006", "normx2-pzn", _full_tagset())
        feature_engine.compute_features_for_report("normx2-pzn", feature_set_id="poznanski_4")

        result = poznanski.score_report("normx2-pzn")
        assert result["warnings"] == []
        assert "_warnings" not in result["feature_contributions"]

    def test_warning_does_not_override_critical(self, seeded_db):
        # Bankrupt company whose quick ratio sits just above the threshold.
        # Heavy losses push Z deep into the critical zone despite the warning.
        tags = _full_tagset({
            "RZiS.L": -2000000,
            "RZiS.C": -1000000,
            "RZiS.A": 1000000,
            "Aktywa": 1000000,
            "Pasywa_A": 10000,
            "Pasywa_B_II": 0,
            "Aktywa_B": 900000,
            "Aktywa_B_I": 0,
            "Pasywa_B_III": 200000,
        })
        _create_report_with_tags("0000200007", "warncrit-pzn", tags)
        feature_engine.compute_features_for_report("warncrit-pzn", feature_set_id="poznanski_4")

        result = poznanski.score_report("warncrit-pzn")
        assert poznanski.WARNING_NON_LINEAR_LIQUIDITY in result["warnings"]
        # Critical stays critical — warning only downgrades "low" verdicts.
        assert result["risk_category"] == "critical"
        assert result["classification"] == 1


# ---------------------------------------------------------------------------
# Model registration tests
# ---------------------------------------------------------------------------


class TestEnsureModelRegistered:
    def test_registers_model(self, isolated_db):
        poznanski.ensure_model_registered()
        models = prediction_db.get_active_models()
        m = [m for m in models if m["id"] == poznanski.MODEL_ID]
        assert len(m) == 1
        assert m[0]["is_baseline"] is True
        assert m[0]["model_type"] == "discriminant"

    def test_idempotent(self, isolated_db):
        poznanski.ensure_model_registered()
        poznanski.ensure_model_registered()
        models = prediction_db.get_active_models()
        m = [m for m in models if m["id"] == poznanski.MODEL_ID]
        assert len(m) == 1


# ---------------------------------------------------------------------------
# Batch scoring tests
# ---------------------------------------------------------------------------


class TestScoreBatch:
    def test_scores_multiple_reports(self, seeded_db):
        _create_report_with_tags("0000300001", "pzn-batch-1", _full_tagset())
        feature_engine.compute_features_for_report("pzn-batch-1", feature_set_id="poznanski_4")

        _create_report_with_tags("0000300002", "pzn-batch-2", _full_tagset({"RZiS.L": 80000}))
        feature_engine.compute_features_for_report("pzn-batch-2", feature_set_id="poznanski_4")

        # Incomplete report — no features computable
        _create_report_with_tags("0000300003", "pzn-batch-3", {"Aktywa": 500000})
        feature_engine.compute_features_for_report("pzn-batch-3", feature_set_id="poznanski_4")

        result = poznanski.score_batch(["pzn-batch-1", "pzn-batch-2", "pzn-batch-3"])
        assert result["scored"] == 2
        assert result["skipped"] == 1
        assert result["run_id"] is not None

    def test_creates_prediction_run(self, seeded_db):
        _create_report_with_tags("0000300004", "pzn-run-rpt", _full_tagset())
        feature_engine.compute_features_for_report("pzn-run-rpt", feature_set_id="poznanski_4")

        result = poznanski.score_batch(["pzn-run-rpt"])
        assert result["scored"] == 1

        conn = prediction_db.get_conn()
        run = conn.execute(
            "SELECT status, companies_scored FROM prediction_runs WHERE id = %s",
            [result["run_id"]],
        ).fetchone()
        assert run[0] == "completed"
        assert run[1] == 1

    def test_predictions_persisted(self, seeded_db):
        _create_report_with_tags("0000300005", "pzn-persist", _full_tagset())
        feature_engine.compute_features_for_report("pzn-persist", feature_set_id="poznanski_4")
        poznanski.score_batch(["pzn-persist"])

        pred = prediction_db.get_latest_prediction("0000300005")
        assert pred is not None
        assert pred["krs"] == "0000300005"
        assert pred["raw_score"] is not None
        assert pred["risk_category"] in ("low", "medium", "high", "critical")

    def test_auto_discover_unscored(self, seeded_db):
        _create_report_with_tags("0000300006", "pzn-auto", _full_tagset())
        feature_engine.compute_features_for_report("pzn-auto", feature_set_id="poznanski_4")

        result = poznanski.score_batch(report_ids=None)
        assert result["scored"] == 1

        result2 = poznanski.score_batch(report_ids=None)
        assert result2["scored"] == 0


# ---------------------------------------------------------------------------
# CR3-REL-004: prediction run finalization on batch failure
# ---------------------------------------------------------------------------


class TestBatchFinalizationOnFailure:
    """Same guarantees as Maczynska — a batch-level exception must not leave
    the `prediction_runs` row stuck in `running`.
    """

    def test_bulk_read_failure_finalizes_run_as_failed(self, seeded_db, monkeypatch):
        _create_report_with_tags("0000300900", "pzn-rel-rpt", _full_tagset())
        feature_engine.compute_features_for_report(
            "pzn-rel-rpt", feature_set_id="poznanski_4"
        )

        def _boom(*args, **kwargs):
            raise RuntimeError("simulated DB outage during bulk-load")

        monkeypatch.setattr(
            prediction_db, "get_computed_features_for_reports_batch", _boom
        )

        with pytest.raises(RuntimeError, match="simulated DB outage"):
            poznanski.score_batch(["pzn-rel-rpt"])

        conn = prediction_db.get_conn()
        row = conn.execute(
            """
            SELECT status, error_message
            FROM prediction_runs
            WHERE model_id = %s
            ORDER BY created_at DESC
            LIMIT 1
            """,
            [poznanski.MODEL_ID],
        ).fetchone()
        assert row is not None
        status, error_message = row
        assert status == "failed", (
            f"Run must be finalized as 'failed' after batch-level error, got {status!r}"
        )
        assert error_message and error_message.startswith("batch_error:")
        assert "simulated DB outage" not in (error_message or "")

    def test_bulk_insert_failure_finalizes_run_as_failed(self, seeded_db, monkeypatch):
        _create_report_with_tags("0000300901", "pzn-rel-rpt-2", _full_tagset())
        feature_engine.compute_features_for_report(
            "pzn-rel-rpt-2", feature_set_id="poznanski_4"
        )

        def _boom(*args, **kwargs):
            raise RuntimeError("simulated DB outage during bulk-insert")

        monkeypatch.setattr(prediction_db, "insert_predictions_batch", _boom)

        with pytest.raises(RuntimeError, match="simulated DB outage"):
            poznanski.score_batch(["pzn-rel-rpt-2"])

        conn = prediction_db.get_conn()
        row = conn.execute(
            """
            SELECT status
            FROM prediction_runs
            WHERE model_id = %s
            ORDER BY created_at DESC
            LIMIT 1
            """,
            [poznanski.MODEL_ID],
        ).fetchone()
        assert row is not None
        assert row[0] == "failed"
