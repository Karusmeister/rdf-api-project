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


def score_report(report_id: str) -> Optional[dict]:
    """Score a single report using the Poznanski discriminant function.

    Returns dict with raw_score, classification, risk_category,
    feature_contributions, warnings, or None if required features are missing.
    """
    features = prediction_db.get_computed_features_for_report(report_id, valid_only=True)
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


def score_batch(report_ids: Optional[list[str]] = None) -> dict:
    """Score multiple reports and write results to predictions.

    If report_ids is None, finds reports with computed Poznanski features
    but no existing prediction for this model.
    """
    ensure_model_registered()

    if report_ids is None:
        report_ids = _find_unscored_reports()

    run_id = str(uuid.uuid4())
    prediction_db.create_prediction_run(run_id, MODEL_ID)

    start = time.monotonic()
    scored = 0
    skipped = 0
    errors = []

    for report_id in report_ids:
        try:
            result = score_report(report_id)
            if result is None:
                skipped += 1
                continue

            report = prediction_db.get_financial_report(report_id)
            if report is None:
                skipped += 1
                continue

            prediction_db.insert_prediction(
                prediction_id=str(uuid.uuid4()),
                prediction_run_id=run_id,
                krs=report["krs"],
                report_id=report_id,
                raw_score=result["raw_score"],
                probability=None,
                classification=result["classification"],
                risk_category=result["risk_category"],
                feature_contributions=result["feature_contributions"],
                feature_snapshot=result.get("feature_snapshot"),
            )
            scored += 1

        except Exception as e:
            skipped += 1
            errors.append({"report_id": report_id, "error": str(e)})
            logger.error(
                "poznanski_score_error",
                extra={"event": "poznanski_score_error", "report_id": report_id, "error": str(e)},
                exc_info=True,
            )

    duration = round(time.monotonic() - start, 3)
    status = "completed" if not errors else "completed_with_errors"

    prediction_db.finish_prediction_run(
        run_id=run_id,
        status=status,
        companies_scored=scored,
        duration_seconds=duration,
        error_message="; ".join(e["error"] for e in errors)[:500] if errors else None,
    )

    logger.info(
        "poznanski_batch_completed",
        extra={
            "event": "poznanski_batch_completed",
            "run_id": run_id,
            "scored": scored,
            "skipped": skipped,
            "errors": len(errors),
            "duration": duration,
        },
    )

    return {
        "run_id": run_id,
        "scored": scored,
        "skipped": skipped,
        "errors": len(errors),
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
