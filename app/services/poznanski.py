"""
Poznanski (Hamrol, Czajka, Piechocki 2004) discriminant model
— baseline bankruptcy predictor.

Z = 3.562*X1 + 1.588*X2 + 4.288*X3 + 6.719*X4 - 2.368

Where:
  X1 = Net profit / Total assets
  X2 = (Current assets - Inventory) / Short-term liabilities (quick ratio)
  X3 = (Equity + Long-term liabilities) / Total assets (fixed capital ratio)
  X4 = Profit on sales / Net revenue from sales

This is a deterministic model with fixed coefficients — no training needed.
Reads pre-computed features from computed_features, applies the discriminant
function, and writes results to prediction_runs + predictions.

Non-linearity mitigation: when X2 (quick ratio) is abnormally high the model's
linear extrapolation becomes unreliable (U-shaped risk curve — too much idle
cash also signals mismanagement). Such scores are flagged with
`WARNING_NON_LINEAR_LIQUIDITY` in the returned object and persisted inside
`feature_contributions._warnings` so downstream consumers can surface it.
"""

from __future__ import annotations

import logging
import time
import uuid
from typing import Optional

from app.db import prediction_db

logger = logging.getLogger(__name__)

MODEL_ID = "poznanski_2004_v1"
MODEL_NAME = "poznanski"
MODEL_VERSION = "2004_v1"
FEATURE_SET_ID = "poznanski_4"

COEFFICIENTS = {
    "x1_poznanski": 3.562,
    "x2_poznanski": 1.588,
    "x3_poznanski": 4.288,
    "x4_poznanski": 6.719,
}
INTERCEPT = -2.368

REQUIRED_FEATURES = list(COEFFICIENTS.keys())

# Threshold above which X2 (quick ratio) is considered abnormally high and the
# linear assumption breaks down. Calibrate against the empirical distribution
# once enough data has been ingested.
NON_LINEAR_LIQUIDITY_THRESHOLD = 4.0
WARNING_NON_LINEAR_LIQUIDITY = "WARNING_NON_LINEAR_LIQUIDITY"


def ensure_model_registered() -> None:
    """Insert poznanski_2004_v1 into model_registry if not already present."""
    prediction_db.register_model(
        model_id=MODEL_ID,
        name=MODEL_NAME,
        model_type="discriminant",
        version=MODEL_VERSION,
        feature_set_id=FEATURE_SET_ID,
        description=(
            "Poznanski (Hamrol, Czajka, Piechocki 2004) 4-variable "
            "discriminant model for Polish companies"
        ),
        hyperparameters={
            "coefficients": COEFFICIENTS,
            "intercept": INTERCEPT,
            "cutoffs": {"critical": 0, "medium": 1},
            "non_linear_liquidity_threshold": NON_LINEAR_LIQUIDITY_THRESHOLD,
        },
        is_baseline=True,
    )


def classify(z_score: float) -> tuple[int, str]:
    """Map Z-score to (classification, risk_category).

    Per the published model: Z < 0 → at-risk of bankruptcy, Z >= 0 → safe.
    We subdivide the safe zone for UI interpretability.
    """
    if z_score < 0:
        return (1, "critical")
    elif z_score < 1:
        return (0, "medium")
    else:
        return (0, "low")


def _score_from_feature_rows(report_id: str, features: list[dict]) -> Optional[dict]:
    """Pure scoring path that takes pre-loaded feature rows.

    Factored out so `score_batch` can drive it from a single bulk query instead
    of one `get_computed_features_for_report` call per report (CR-PZN-003).
    """
    feature_map = {f["feature_definition_id"]: f["value"] for f in features}
    version_map = {f["feature_definition_id"]: f["computation_version"] for f in features}

    missing = [
        fid for fid in REQUIRED_FEATURES
        if fid not in feature_map or feature_map[fid] is None
    ]
    if missing:
        logger.warning(
            "poznanski_missing_features",
            extra={
                "event": "poznanski_missing_features",
                "report_id": report_id,
                "missing": missing,
            },
        )
        return None

    z_score = float(INTERCEPT)
    contributions = {"_intercept": round(float(INTERCEPT), 6)}
    for fid, coeff in COEFFICIENTS.items():
        value = feature_map[fid]
        contribution = coeff * value
        contributions[fid] = round(contribution, 6)
        z_score += contribution

    z_score = round(z_score, 6)
    classification, risk_category = classify(z_score)

    # U-shape non-linearity check on the quick ratio (X2).
    warnings: list[str] = []
    x2_value = feature_map["x2_poznanski"]
    if x2_value is not None and x2_value > NON_LINEAR_LIQUIDITY_THRESHOLD:
        warnings.append(WARNING_NON_LINEAR_LIQUIDITY)
        # Don't blindly trust a "low risk" verdict when liquidity is
        # pathologically high — downgrade one notch so the UI surfaces it.
        if risk_category == "low":
            risk_category = "medium"

    if warnings:
        contributions["_warnings"] = warnings

    feature_snapshot = {fid: version_map[fid] for fid in REQUIRED_FEATURES}

    return {
        "raw_score": z_score,
        "classification": classification,
        "risk_category": risk_category,
        "feature_contributions": contributions,
        "feature_snapshot": feature_snapshot,
        "warnings": warnings,
    }


def score_report(report_id: str) -> Optional[dict]:
    """Score a single report using the Poznanski discriminant function.

    Returns dict with raw_score, classification, risk_category,
    feature_contributions, warnings, or None if required features are missing.
    """
    features = prediction_db.get_computed_features_for_report(report_id, valid_only=True)
    return _score_from_feature_rows(report_id, features)


def score_batch(report_ids: Optional[list[str]] = None) -> dict:
    """Score multiple reports and write results to predictions.

    If report_ids is None, finds reports with computed Poznanski features
    but no existing prediction for this model.

    Note: the model is registered at application startup
    (`predictions_service.register_builtin_models`), so this path does not
    register as a side effect. Registration is idempotent so callers running
    outside an app context (scripts, tests) can still call
    `ensure_model_registered()` explicitly.
    """
    if report_ids is None:
        report_ids = _find_unscored_reports()

    # CR-PZN-003: short-circuit on empty input so we don't create a "ran, did
    # nothing" row in prediction_runs.
    if not report_ids:
        logger.info(
            "poznanski_batch_noop",
            extra={"event": "poznanski_batch_noop", "model_id": MODEL_ID},
        )
        return {
            "run_id": None,
            "scored": 0,
            "skipped": 0,
            "errors": 0,
            "duration_seconds": 0.0,
        }

    run_id = str(uuid.uuid4())
    prediction_db.create_prediction_run(run_id, MODEL_ID)

    start = time.monotonic()
    scored = 0
    skipped = 0
    error_counts: dict[str, int] = {}

    # CR3-REL-004: wrap bulk-load / scoring / bulk-insert in an outer
    # try/except/finally so batch-level failures still finalize the
    # prediction_runs row. Prior code path could leave the row in
    # `running` forever if any bulk DB call raised before
    # `finish_prediction_run`.
    batch_level_error: str | None = None
    try:
        # CR-PZN-003: bulk-load the inputs once instead of hitting the DB per report.
        features_by_report = prediction_db.get_computed_features_for_reports_batch(
            report_ids, valid_only=True
        )
        reports_by_id = prediction_db.get_financial_reports_batch(report_ids)

        prediction_rows: list[dict] = []
        for report_id in report_ids:
            try:
                report = reports_by_id.get(report_id)
                if report is None:
                    skipped += 1
                    continue

                result = _score_from_feature_rows(
                    report_id, features_by_report.get(report_id, [])
                )
                if result is None:
                    skipped += 1
                    continue

                prediction_rows.append({
                    "prediction_id": str(uuid.uuid4()),
                    "prediction_run_id": run_id,
                    "krs": report["krs"],
                    "report_id": report_id,
                    "raw_score": result["raw_score"],
                    "probability": None,
                    "classification": result["classification"],
                    "risk_category": result["risk_category"],
                    "feature_contributions": result["feature_contributions"],
                    "feature_snapshot": result.get("feature_snapshot"),
                })
                scored += 1

            except Exception as e:
                skipped += 1
                # CR-PZN-004: aggregate by stable error code (exception class name),
                # never persist raw exception text to the DB. Full details stay in
                # logs with access controls.
                code = type(e).__name__
                error_counts[code] = error_counts.get(code, 0) + 1
                logger.error(
                    "poznanski_score_error",
                    extra={
                        "event": "poznanski_score_error",
                        "model_id": MODEL_ID,
                        "run_id": run_id,
                        "report_id": report_id,
                        "error_code": code,
                    },
                    exc_info=True,
                )

        # CR-PZN-003: single bulk insert for the whole run instead of per-row.
        if prediction_rows:
            prediction_db.insert_predictions_batch(prediction_rows)

    except Exception as e:
        batch_level_error = f"batch_error:{type(e).__name__}"
        logger.error(
            "poznanski_batch_error",
            extra={
                "event": "poznanski_batch_error",
                "model_id": MODEL_ID,
                "run_id": run_id,
                "error_code": type(e).__name__,
            },
            exc_info=True,
        )
        raise
    finally:
        duration = round(time.monotonic() - start, 3)
        if batch_level_error is not None:
            status = "failed"
            error_summary = batch_level_error[:500]
        elif error_counts:
            status = "completed_with_errors"
            # CR-PZN-004: sanitized error summary; no driver/stack text.
            error_summary = ",".join(
                f"{code}:{count}" for code, count in sorted(error_counts.items())
            )[:500]
        else:
            status = "completed"
            error_summary = None

        try:
            prediction_db.finish_prediction_run(
                run_id=run_id,
                status=status,
                companies_scored=scored,
                duration_seconds=duration,
                error_message=error_summary,
            )
        except Exception:
            logger.error(
                "poznanski_finalize_failed",
                extra={
                    "event": "poznanski_finalize_failed",
                    "model_id": MODEL_ID,
                    "run_id": run_id,
                },
                exc_info=True,
            )
            # Don't mask the original exception (if any) by raising here.

    logger.info(
        "poznanski_batch_completed",
        extra={
            "event": "poznanski_batch_completed",
            "run_id": run_id,
            "scored": scored,
            "skipped": skipped,
            "errors": sum(error_counts.values()),
            "duration": duration,
        },
    )

    return {
        "run_id": run_id,
        "scored": scored,
        "skipped": skipped,
        "errors": sum(error_counts.values()),
        "duration_seconds": duration,
    }


def _find_unscored_reports() -> list[str]:
    """Find reports that have all 4 Poznanski features computed but no prediction yet."""
    conn = prediction_db.get_conn()
    rows = conn.execute("""
        SELECT cf.report_id
        FROM latest_computed_features cf
        JOIN latest_successful_financial_reports fr ON fr.id = cf.report_id
        WHERE cf.feature_definition_id IN ('x1_poznanski', 'x2_poznanski',
                                           'x3_poznanski', 'x4_poznanski')
          AND cf.is_valid = true
        GROUP BY cf.report_id
        HAVING count(DISTINCT cf.feature_definition_id) = 4
        EXCEPT
        SELECT p.report_id
        FROM predictions p
        JOIN prediction_runs pr ON pr.id = p.prediction_run_id
        WHERE pr.model_id = %s
    """, [MODEL_ID]).fetchall()
    return [r[0] for r in rows]
