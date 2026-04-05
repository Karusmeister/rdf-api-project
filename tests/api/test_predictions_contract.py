"""Integration contract test for the predictions API (BE-PRED-008).

Exercises the full loader path end-to-end against PostgreSQL instead of stubbing
`get_features_for_predictions_batch` / `get_source_line_items_for_reports_batch`
with empty dicts. Guards against regressions of:

- Missing `CF.A_II_1` in Maczynska X1 `source_tags[]`.
- Empty `source_tags[]` for any feature.
- Missing `value_previous` when a prior-year report exists.
- Tags referenced in `formula_description` that drop out of `source_tags[]`.
"""

from __future__ import annotations

import re
from unittest.mock import patch

import pytest

from app.config import settings
from app.db import connection as db_conn
from app.db import prediction_db
from app.scraper import db as scraper_db
from app.services import feature_engine, maczynska, poznanski, predictions
from scripts.seed_features import FEATURE_DEFINITIONS, FEATURE_SETS


# Matches the regex used in the frontend to detect tag tokens in formula_description.
_FORMULA_TAG_RE = re.compile(r"[A-Z][A-Za-z0-9]*(?:[._][A-Za-z0-9]+)+")


@pytest.fixture
def seeded_predictions_db(pg_dsn, clean_pg):
    """Isolated PostgreSQL with feature_definitions + feature_sets seeded."""
    db_conn.reset()
    scraper_db._schema_initialized = False
    prediction_db._schema_initialized = False

    with patch.object(settings, "database_url", pg_dsn):
        scraper_db.connect()
        prediction_db.connect()

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

        # CR-PZN-001: mirror production startup — register every built-in model
        # up front instead of relying on `score_batch` side effects.
        predictions.register_builtin_models()

        yield
        db_conn.close()

    db_conn.reset()
    scraper_db._schema_initialized = False
    prediction_db._schema_initialized = False


def _seed_year(krs: str, report_id: str, fiscal_year: int, tag_values: dict[str, float]) -> None:
    """Create a completed financial report with the given line items for one year."""
    prediction_db.upsert_company(krs=krs)
    prediction_db.create_financial_report(
        report_id=report_id, krs=krs, fiscal_year=fiscal_year,
        period_start=f"{fiscal_year}-01-01", period_end=f"{fiscal_year}-12-31",
    )
    prediction_db.update_report_status(report_id, "completed")

    items = []
    for tag, value in tag_values.items():
        if tag.startswith("CF."):
            section = "CF"
        elif tag.startswith("RZiS."):
            section = "RZiS"
        else:
            section = "Bilans"
        items.append({
            "report_id": report_id, "section": section,
            "tag_path": tag, "value_current": value,
        })
    prediction_db.batch_insert_line_items(items)


def _tag_values(scale: float = 1.0) -> dict[str, float]:
    """Produce a Maczynska-complete tag set scaled by `scale` so YoY deltas exist."""
    return {
        "RZiS.I": 200000 * scale,
        "RZiS.A": 2000000 * scale,
        "RZiS.C": 800000 * scale,
        "RZiS.F": 300000 * scale,
        "RZiS.L": 150000 * scale,
        "RZiS.B": 1200000 * scale,
        "CF.A_II_1": 50000 * scale,
        "Pasywa_A": 600000 * scale,
        "Pasywa_B": 500000 * scale,
        "Pasywa_B_III": 200000 * scale,
        "Aktywa": 1100000 * scale,
        "Aktywa_A": 400000 * scale,
        "Aktywa_B": 700000 * scale,
        "Aktywa_B_I": 100000 * scale,
        "Aktywa_B_II": 150000 * scale,
        "Aktywa_B_III": 50000 * scale,
    }


class TestPredictionsContract:
    """End-to-end: seed two years, score both, call the service, assert the contract."""

    def test_multi_year_source_tags_and_value_previous(self, seeded_predictions_db):
        krs = "0000500001"
        # Two years of realistic line items. 2024 values are scaled up so
        # value_previous (the 2023 number) is distinct and non-null.
        _seed_year(krs, "rpt-2023", 2023, _tag_values(scale=1.0))
        _seed_year(krs, "rpt-2024", 2024, _tag_values(scale=1.25))

        feature_engine.compute_features_for_report("rpt-2023", feature_set_id="maczynska_6")
        feature_engine.compute_features_for_report("rpt-2024", feature_set_id="maczynska_6")

        maczynska.score_batch(report_ids=["rpt-2023", "rpt-2024"])
        # Fresh service call — invalidate caches so the newly registered model
        # and feature defs are picked up.
        predictions.invalidate_caches()

        resp = predictions.get_predictions(krs)

        # 1. Two PredictionDetail rows, one per fiscal year, same model.
        preds = resp["predictions"]
        years = sorted(p["data_source"]["fiscal_year"] for p in preds)
        assert years == [2023, 2024]
        assert all(p["model"]["model_id"] == "maczynska_1994_v1" for p in preds)

        # 2. Every feature in every year has non-empty source_tags.
        for pred in preds:
            assert pred["features"], f"empty features[] for year {pred['data_source']['fiscal_year']}"
            for feat in pred["features"]:
                assert feat["source_tags"], (
                    f"{feat['feature_id']} @ {pred['data_source']['fiscal_year']} "
                    f"has empty source_tags"
                )

        # 3. Every tag referenced in formula_description is present in source_tags.
        for pred in preds:
            for feat in pred["features"]:
                expected = set(_FORMULA_TAG_RE.findall(feat["formula_description"] or ""))
                returned = {st["tag_path"] for st in feat["source_tags"]}
                missing = expected - returned
                assert not missing, (
                    f"{feat['feature_id']} @ {pred['data_source']['fiscal_year']}: "
                    f"formula references {missing} but source_tags only has {returned}"
                )

        # 4. Regression guard — x1_maczynska carries CF.A_II_1 across every year.
        for pred in preds:
            x1 = next(
                (f for f in pred["features"] if f["feature_id"] == "x1_maczynska"),
                None,
            )
            assert x1 is not None, f"x1_maczynska missing from year {pred['data_source']['fiscal_year']}"
            x1_tags = {st["tag_path"] for st in x1["source_tags"]}
            assert "CF.A_II_1" in x1_tags, (
                f"CF.A_II_1 missing from x1_maczynska source_tags "
                f"@ {pred['data_source']['fiscal_year']}: {x1_tags}"
            )
            # The CF.A_II_1 source tag must carry the prior-year value for 2024.
            if pred["data_source"]["fiscal_year"] == 2024:
                cf = next(st for st in x1["source_tags"] if st["tag_path"] == "CF.A_II_1")
                assert cf["value_previous"] is not None, (
                    "2024 CF.A_II_1 must have non-null value_previous (2023 exists)"
                )
                # Value flows through raw, unscaled — 2023 had 50000.
                assert cf["value_previous"] == pytest.approx(50000.0)

        # 5. At least one source tag in the later year carries a non-null
        # value_previous (broader guard across all features).
        pred_2024 = next(p for p in preds if p["data_source"]["fiscal_year"] == 2024)
        any_prev = any(
            st["value_previous"] is not None
            for f in pred_2024["features"]
            for st in f["source_tags"]
        )
        assert any_prev, "No value_previous populated for 2024 sources"

        # 6. BE-PRED-010: higher_is_better is resolved against the semantic
        # lookup. Known-direction tags must not be null.
        semantic_expectations = {
            "RZiS.I": True,     # profit → higher is better
            "RZiS.A": True,     # revenue → higher is better
            "Pasywa_B": False,  # liabilities → higher is worse
            "CF.A_II_1": True,  # operating cash flow → higher is better
            "Aktywa_B_I": False,  # inventory → higher is worse
        }
        for pred in preds:
            tag_flags: dict[str, bool | None] = {}
            for feat in pred["features"]:
                for st in feat["source_tags"]:
                    tag_flags[st["tag_path"]] = st["higher_is_better"]
            for tag, expected in semantic_expectations.items():
                if tag in tag_flags:
                    assert tag_flags[tag] is expected, (
                        f"{tag}.higher_is_better = {tag_flags[tag]!r}, expected {expected!r}"
                    )

    def test_first_year_value_previous_is_null(self, seeded_predictions_db):
        """For the earliest fiscal year of a company, value_previous must be None."""
        krs = "0000500002"
        _seed_year(krs, "rpt-only-2022", 2022, _tag_values(scale=1.0))

        feature_engine.compute_features_for_report("rpt-only-2022", feature_set_id="maczynska_6")
        maczynska.score_batch(report_ids=["rpt-only-2022"])
        predictions.invalidate_caches()

        resp = predictions.get_predictions(krs)
        assert len(resp["predictions"]) == 1
        pred = resp["predictions"][0]
        assert pred["data_source"]["fiscal_year"] == 2022

        # No prior-year report exists → every source tag's value_previous is None.
        for feat in pred["features"]:
            for st in feat["source_tags"]:
                assert st["value_previous"] is None, (
                    f"{st['tag_path']}: expected value_previous=None for first-year company"
                )

    def test_poznanski_warning_and_interpretation_contract(self, seeded_predictions_db):
        """CR-PZN-002/005: a Poznanski prediction must surface `result.warnings`
        and interpretation thresholds end-to-end through the read path.

        We drive the warning deterministically by injecting a pathologically
        high X2 (quick ratio) via large Aktywa_B / Aktywa_B_I values relative to
        Pasywa_B_III. The scorer raises `WARNING_NON_LINEAR_LIQUIDITY` and the
        service must propagate it into the API response shape.
        """
        krs = "0000500003"

        # Build a tag set where Aktywa_B ~= 10M and Pasywa_B_III = 100K so
        # quick ratio X2 >> NON_LINEAR_LIQUIDITY_THRESHOLD (4.0).
        tags = _tag_values(scale=1.0)
        tags["Aktywa_B"] = 10_000_000.0
        tags["Aktywa_B_I"] = 100_000.0
        tags["Pasywa_B_III"] = 100_000.0
        # Poznanski also needs Pasywa_B_II for X3 (fixed capital ratio). Keep
        # it small so the model doesn't numerically explode while we exercise
        # the warning path.
        tags["Pasywa_B_II"] = 50_000.0
        _seed_year(krs, "rpt-pzn-2024", 2024, tags)

        # Compute both sets so the synthesized report carries every required
        # feature, then score with Poznanski.
        feature_engine.compute_features_for_report(
            "rpt-pzn-2024", feature_set_id="poznanski_4"
        )
        result = poznanski.score_batch(report_ids=["rpt-pzn-2024"])
        assert result["scored"] == 1
        predictions.invalidate_caches()

        resp = predictions.get_predictions(krs)
        pzn = [p for p in resp["predictions"] if p["model"]["model_id"] == "poznanski_2004_v1"]
        assert len(pzn) == 1, "Poznanski prediction missing from response"
        pzn_pred = pzn[0]

        # CR-PZN-002: warnings list carries the stable code.
        assert "WARNING_NON_LINEAR_LIQUIDITY" in pzn_pred["result"]["warnings"]

        # CR-PZN-002: the `_warnings` / `_intercept` metadata keys must NOT
        # leak as feature-level contributions — they are implementation-detail
        # keys stashed in the persisted contributions JSON.
        feature_ids = {f["feature_id"] for f in pzn_pred["features"]}
        assert "_warnings" not in feature_ids
        assert "_intercept" not in feature_ids

        # Interpretation guide must be present with the Poznanski bands.
        interp = pzn_pred["interpretation"]
        assert interp is not None
        labels = {t["label"] for t in interp["thresholds"]}
        assert {"critical", "medium", "low"}.issubset(labels)
        assert interp["higher_is_better"] is True
        # Exactly one threshold should be marked current (the one containing
        # the raw score the API returned).
        current_bands = [t for t in interp["thresholds"] if t["is_current"]]
        assert len(current_bands) == 1, (
            f"Expected exactly one current band, got {current_bands}"
        )
