"""Tests for the Maczynska (1994) discriminant model."""

from unittest.mock import patch

import pytest

from app.config import settings
from app.db import connection as db_conn
from app.db import prediction_db
from app.scraper import db as scraper_db
from app.services import feature_engine, maczynska, predictions
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
        "Aktywa": "Bilans", "Aktywa_A": "Bilans", "Aktywa_B": "Bilans",
        "Aktywa_B_I": "Bilans", "Aktywa_B_II": "Bilans", "Aktywa_B_III": "Bilans",
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


# ---------------------------------------------------------------------------
# X1 custom computation tests
# ---------------------------------------------------------------------------


class TestX1Maczynska:
    def test_happy_path(self, seeded_db):
        _create_report_with_tags("0000100001", "x1-happy", {
            "RZiS.I": 100, "CF.A_II_1": 20, "Pasywa_B": 400,
            "Aktywa": 600, "RZiS.A": 500, "Aktywa_B_I": 50,
        })
        result = feature_engine.compute_features_for_report("x1-happy", feature_set_id="maczynska_6")
        # (100 + 20) / 400 = 0.3
        assert result["features"]["x1_maczynska"] == pytest.approx(0.3, abs=1e-4)

    def test_missing_gross_profit_returns_none(self, seeded_db):
        _create_report_with_tags("0000100002", "x1-no-gross", {
            "CF.A_II_1": 20, "Pasywa_B": 400,
            "Aktywa": 600, "RZiS.A": 500, "Aktywa_B_I": 50,
        })
        result = feature_engine.compute_features_for_report("x1-no-gross", feature_set_id="maczynska_6")
        assert result["features"]["x1_maczynska"] is None

    def test_missing_depreciation_uses_zero(self, seeded_db):
        _create_report_with_tags("0000100003", "x1-no-dep", {
            "RZiS.I": 100, "Pasywa_B": 400,
            "Aktywa": 600, "RZiS.A": 500, "Aktywa_B_I": 50,
        })
        result = feature_engine.compute_features_for_report("x1-no-dep", feature_set_id="maczynska_6")
        # (100 + 0) / 400 = 0.25
        assert result["features"]["x1_maczynska"] == pytest.approx(0.25, abs=1e-4)

    def test_zero_liabilities_returns_none(self, seeded_db):
        _create_report_with_tags("0000100004", "x1-zero-liab", {
            "RZiS.I": 100, "CF.A_II_1": 20, "Pasywa_B": 0,
            "Aktywa": 600, "RZiS.A": 500, "Aktywa_B_I": 50,
        })
        result = feature_engine.compute_features_for_report("x1-zero-liab", feature_set_id="maczynska_6")
        assert result["features"]["x1_maczynska"] is None

    def test_negative_gross_profit(self, seeded_db):
        _create_report_with_tags("0000100005", "x1-neg", {
            "RZiS.I": -50, "CF.A_II_1": 20, "Pasywa_B": 400,
            "Aktywa": 600, "RZiS.A": 500, "Aktywa_B_I": 50,
        })
        result = feature_engine.compute_features_for_report("x1-neg", feature_set_id="maczynska_6")
        # (-50 + 20) / 400 = -0.075
        assert result["features"]["x1_maczynska"] == pytest.approx(-0.075, abs=1e-4)


# ---------------------------------------------------------------------------
# Classification tests
# ---------------------------------------------------------------------------


class TestClassify:
    def test_critical(self):
        assert maczynska.classify(-1.5) == (1, "critical")

    def test_high(self):
        assert maczynska.classify(0.5) == (1, "high")

    def test_medium(self):
        assert maczynska.classify(1.5) == (0, "medium")

    def test_low(self):
        assert maczynska.classify(3.0) == (0, "low")

    def test_boundary_zero(self):
        assert maczynska.classify(0.0) == (1, "high")

    def test_boundary_one(self):
        assert maczynska.classify(1.0) == (0, "medium")

    def test_boundary_two(self):
        assert maczynska.classify(2.0) == (0, "low")


# ---------------------------------------------------------------------------
# score_report tests
# ---------------------------------------------------------------------------


class TestScoreReport:
    def _make_healthy_report(self, seeded_db):
        """Create a report with features that produce Zm > 2 (healthy)."""
        _create_report_with_tags("0000200001", "healthy-rpt", {
            "RZiS.I": 200000, "CF.A_II_1": 50000, "Pasywa_B": 500000,
            "Aktywa": 1000000, "RZiS.A": 2000000, "Aktywa_B_I": 100000,
        })
        feature_engine.compute_features_for_report("healthy-rpt", feature_set_id="maczynska_6")
        return "healthy-rpt"

    def _make_bankrupt_report(self, seeded_db):
        """Create a report with features that produce Zm < 0 (bankrupt)."""
        _create_report_with_tags("0000200002", "bankrupt-rpt", {
            "RZiS.I": -100000, "CF.A_II_1": 5000, "Pasywa_B": 900000,
            "Aktywa": 500000, "RZiS.A": 200000, "Aktywa_B_I": 10000,
        })
        feature_engine.compute_features_for_report("bankrupt-rpt", feature_set_id="maczynska_6")
        return "bankrupt-rpt"

    def test_healthy_company(self, seeded_db):
        report_id = self._make_healthy_report(seeded_db)
        result = maczynska.score_report(report_id)

        assert result is not None
        assert result["risk_category"] == "low"
        assert result["classification"] == 0
        assert result["raw_score"] > 2.0

    def test_bankrupt_company(self, seeded_db):
        report_id = self._make_bankrupt_report(seeded_db)
        result = maczynska.score_report(report_id)

        assert result is not None
        assert result["classification"] == 1
        assert result["raw_score"] < 0

    def test_missing_features_returns_none(self, seeded_db):
        _create_report_with_tags("0000200003", "incomplete-rpt", {
            "RZiS.I": 100000, "Aktywa": 500000,
            # Missing Pasywa_B, RZiS.A, Aktywa_B_I — most features won't compute
        })
        feature_engine.compute_features_for_report("incomplete-rpt", feature_set_id="maczynska_6")
        result = maczynska.score_report("incomplete-rpt")
        assert result is None

    def test_feature_contributions_sum(self, seeded_db):
        report_id = self._make_healthy_report(seeded_db)
        result = maczynska.score_report(report_id)

        assert result is not None
        total = sum(result["feature_contributions"].values())
        assert total == pytest.approx(result["raw_score"], abs=1e-4)

    def test_feature_contributions_breakdown(self, seeded_db):
        report_id = self._make_healthy_report(seeded_db)
        result = maczynska.score_report(report_id)

        contribs = result["feature_contributions"]
        assert set(contribs.keys()) == set(maczynska.COEFFICIENTS.keys())

        features = prediction_db.get_computed_features_for_report(report_id)
        fmap = {f["feature_definition_id"]: f["value"] for f in features}
        for fid, coeff in maczynska.COEFFICIENTS.items():
            expected = round(coeff * fmap[fid], 6)
            assert contribs[fid] == pytest.approx(expected, abs=1e-4)


# ---------------------------------------------------------------------------
# Model registration tests
# ---------------------------------------------------------------------------


class TestEnsureModelRegistered:
    def test_registers_model(self, isolated_db):
        maczynska.ensure_model_registered()
        models = prediction_db.get_active_models()
        m = [m for m in models if m["id"] == maczynska.MODEL_ID]
        assert len(m) == 1
        assert m[0]["is_baseline"] is True
        assert m[0]["model_type"] == "discriminant"

    def test_idempotent(self, isolated_db):
        maczynska.ensure_model_registered()
        maczynska.ensure_model_registered()
        models = prediction_db.get_active_models()
        m = [m for m in models if m["id"] == maczynska.MODEL_ID]
        assert len(m) == 1


# ---------------------------------------------------------------------------
# Batch scoring tests
# ---------------------------------------------------------------------------


class TestScoreBatch:
    def test_scores_multiple_reports(self, seeded_db):
        # Create two scoreable reports
        _create_report_with_tags("0000300001", "batch-rpt-1", {
            "RZiS.I": 200000, "CF.A_II_1": 50000, "Pasywa_B": 500000,
            "Aktywa": 1000000, "RZiS.A": 2000000, "Aktywa_B_I": 100000,
        })
        feature_engine.compute_features_for_report("batch-rpt-1", feature_set_id="maczynska_6")

        _create_report_with_tags("0000300002", "batch-rpt-2", {
            "RZiS.I": 150000, "CF.A_II_1": 30000, "Pasywa_B": 600000,
            "Aktywa": 800000, "RZiS.A": 1500000, "Aktywa_B_I": 80000,
        })
        feature_engine.compute_features_for_report("batch-rpt-2", feature_set_id="maczynska_6")

        # One incomplete report (missing features)
        _create_report_with_tags("0000300003", "batch-rpt-3", {
            "Aktywa": 500000,
        })
        feature_engine.compute_features_for_report("batch-rpt-3", feature_set_id="maczynska_6")

        result = maczynska.score_batch(["batch-rpt-1", "batch-rpt-2", "batch-rpt-3"])

        assert result["scored"] == 2
        assert result["skipped"] == 1
        assert result["run_id"] is not None

    def test_creates_prediction_run(self, seeded_db):
        _create_report_with_tags("0000300004", "batch-run-rpt", {
            "RZiS.I": 200000, "CF.A_II_1": 50000, "Pasywa_B": 500000,
            "Aktywa": 1000000, "RZiS.A": 2000000, "Aktywa_B_I": 100000,
        })
        feature_engine.compute_features_for_report("batch-run-rpt", feature_set_id="maczynska_6")

        result = maczynska.score_batch(["batch-run-rpt"])
        assert result["scored"] == 1

        conn = prediction_db.get_conn()
        run = conn.execute(
            "SELECT status, companies_scored FROM prediction_runs WHERE id = %s",
            [result["run_id"]],
        ).fetchone()
        assert run[0] == "completed"
        assert run[1] == 1

    def test_predictions_persisted(self, seeded_db):
        _create_report_with_tags("0000300005", "persist-rpt", {
            "RZiS.I": 200000, "CF.A_II_1": 50000, "Pasywa_B": 500000,
            "Aktywa": 1000000, "RZiS.A": 2000000, "Aktywa_B_I": 100000,
        })
        feature_engine.compute_features_for_report("persist-rpt", feature_set_id="maczynska_6")
        maczynska.score_batch(["persist-rpt"])

        pred = prediction_db.get_latest_prediction("0000300005")
        assert pred is not None
        assert pred["krs"] == "0000300005"
        assert pred["raw_score"] is not None
        assert pred["risk_category"] in ("low", "medium", "high", "critical")

    def test_auto_discover_unscored(self, seeded_db):
        _create_report_with_tags("0000300006", "auto-rpt", {
            "RZiS.I": 200000, "CF.A_II_1": 50000, "Pasywa_B": 500000,
            "Aktywa": 1000000, "RZiS.A": 2000000, "Aktywa_B_I": 100000,
        })
        feature_engine.compute_features_for_report("auto-rpt", feature_set_id="maczynska_6")

        # score_batch with None should auto-discover
        result = maczynska.score_batch(report_ids=None)
        assert result["scored"] == 1

        # Running again should find nothing new
        result2 = maczynska.score_batch(report_ids=None)
        assert result2["scored"] == 0

    def test_auto_discover_uses_latest_report_versions_only(self, seeded_db):
        # Original report (v1)
        _create_report_with_tags("0000300007", "auto-old", {
            "RZiS.I": 100000, "CF.A_II_1": 10000, "Pasywa_B": 500000,
            "Aktywa": 1000000, "RZiS.A": 2000000, "Aktywa_B_I": 100000,
        })
        feature_engine.compute_features_for_report("auto-old", feature_set_id="maczynska_6")

        # Correction report (v2) for same company + same period
        _create_report_with_tags("0000300007", "auto-new", {
            "RZiS.I": 300000, "CF.A_II_1": 10000, "Pasywa_B": 500000,
            "Aktywa": 1000000, "RZiS.A": 2000000, "Aktywa_B_I": 100000,
        })
        feature_engine.compute_features_for_report("auto-new", feature_set_id="maczynska_6")

        result = maczynska.score_batch(report_ids=None)

        assert result["scored"] == 1

        conn = prediction_db.get_conn()
        rows = conn.execute(
            "SELECT report_id FROM predictions WHERE prediction_run_id = %s",
            [result["run_id"]],
        ).fetchall()
        assert rows == [("auto-new",)]

    def test_auto_discover_skips_failed_correction(self, seeded_db):
        """If the latest correction failed ETL, fall back to last successful report."""
        # Successful v1
        _create_report_with_tags("0000300008", "auto-ok", {
            "RZiS.I": 200000, "CF.A_II_1": 50000, "Pasywa_B": 500000,
            "Aktywa": 1000000, "RZiS.A": 2000000, "Aktywa_B_I": 100000,
        })
        feature_engine.compute_features_for_report("auto-ok", feature_set_id="maczynska_6")

        # Failed correction v2 — same company, same period
        prediction_db.create_financial_report(
            report_id="auto-fail", krs="0000300008", fiscal_year=2023,
            period_start="2023-01-01", period_end="2023-12-31",
        )
        prediction_db.update_report_status("auto-fail", "failed")

        result = maczynska.score_batch(report_ids=None)
        assert result["scored"] == 1

        conn = prediction_db.get_conn()
        rows = conn.execute(
            "SELECT report_id FROM predictions WHERE prediction_run_id = %s",
            [result["run_id"]],
        ).fetchall()
        # Should score the v1 completed report, not discover the failed v2
        assert rows == [("auto-ok",)]


# ---------------------------------------------------------------------------
# CR3-REL-004: prediction run finalization on batch failure
# ---------------------------------------------------------------------------


class TestBatchFinalizationOnFailure:
    """Guards that `score_batch` never leaves a `prediction_runs` row stuck
    in `running` when a batch-level (pre-loop or bulk-insert) failure occurs.
    """

    def test_bulk_read_failure_finalizes_run_as_failed(self, seeded_db, monkeypatch):
        """An exception raised from `get_computed_features_for_reports_batch`
        happens BEFORE the scoring loop even starts — the old code path
        would skip `finish_prediction_run` entirely and strand the row."""
        _create_report_with_tags("0000300900", "cr3-rel-rpt", {
            "RZiS.I": 200000, "CF.A_II_1": 50000, "Pasywa_B": 500000,
            "Aktywa": 1000000, "RZiS.A": 2000000, "Aktywa_B_I": 100000,
        })
        feature_engine.compute_features_for_report(
            "cr3-rel-rpt", feature_set_id="maczynska_6"
        )

        def _boom(*args, **kwargs):
            raise RuntimeError("simulated DB outage during bulk-load")

        monkeypatch.setattr(
            prediction_db, "get_computed_features_for_reports_batch", _boom
        )

        with pytest.raises(RuntimeError, match="simulated DB outage"):
            maczynska.score_batch(["cr3-rel-rpt"])

        # The run row must exist and be finalized as `failed`. Fetch the
        # most recent Maczynska run to identify what `score_batch` created.
        conn = prediction_db.get_conn()
        row = conn.execute(
            """
            SELECT id, status, error_message
            FROM prediction_runs
            WHERE model_id = %s
            ORDER BY created_at DESC
            LIMIT 1
            """,
            [maczynska.MODEL_ID],
        ).fetchone()
        assert row is not None, "Run row was not created"
        run_id, status, error_message = row
        assert status == "failed", (
            f"Run must be finalized as 'failed' after batch-level error, got {status!r}"
        )
        assert error_message and error_message.startswith("batch_error:"), (
            "error_message must carry sanitized batch error code, "
            f"got {error_message!r}"
        )
        # No raw exception text in the persisted error.
        assert "simulated DB outage" not in (error_message or "")

    def test_bulk_insert_failure_finalizes_run_as_failed(self, seeded_db, monkeypatch):
        """An exception from `insert_predictions_batch` happens AFTER the
        scoring loop produced rows. Finalization in the `finally` block
        must still transition the run to `failed`."""
        _create_report_with_tags("0000300901", "cr3-rel-rpt-2", {
            "RZiS.I": 200000, "CF.A_II_1": 50000, "Pasywa_B": 500000,
            "Aktywa": 1000000, "RZiS.A": 2000000, "Aktywa_B_I": 100000,
        })
        feature_engine.compute_features_for_report(
            "cr3-rel-rpt-2", feature_set_id="maczynska_6"
        )

        def _boom(*args, **kwargs):
            raise RuntimeError("simulated DB outage during bulk-insert")

        monkeypatch.setattr(prediction_db, "insert_predictions_batch", _boom)

        with pytest.raises(RuntimeError, match="simulated DB outage"):
            maczynska.score_batch(["cr3-rel-rpt-2"])

        conn = prediction_db.get_conn()
        row = conn.execute(
            """
            SELECT status
            FROM prediction_runs
            WHERE model_id = %s
            ORDER BY created_at DESC
            LIMIT 1
            """,
            [maczynska.MODEL_ID],
        ).fetchone()
        assert row is not None
        assert row[0] == "failed"
