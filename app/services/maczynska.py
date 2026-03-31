"""
Maczynska (1994) discriminant model — baseline bankruptcy predictor.

Zm = 1.5*X1 + 0.08*X2 + 10*X3 + 5*X4 + 0.3*X5 + 0.1*X6

This is a deterministic model with fixed coefficients — no training needed.
Reads pre-computed features from computed_features, applies the discriminant
function, and writes results to prediction_runs + predictions.
"""

from __future__ import annotations

import logging
import time
import uuid
from typing import Optional

from app.db import prediction_db

logger = logging.getLogger(__name__)

MODEL_ID = "maczynska_1994_v1"
MODEL_NAME = "maczynska"
MODEL_VERSION = "1994_v1"
FEATURE_SET_ID = "maczynska_6"

COEFFICIENTS = {
    "x1_maczynska": 1.5,
    "x2_maczynska": 0.08,
    "x3_maczynska": 10.0,
    "x4_maczynska": 5.0,
    "x5_maczynska": 0.3,
    "x6_maczynska": 0.1,
}

REQUIRED_FEATURES = list(COEFFICIENTS.keys())


def ensure_model_registered() -> None:
    """Insert maczynska_1994_v1 into model_registry if not already present."""
    prediction_db.register_model(
        model_id=MODEL_ID,
        name=MODEL_NAME,
        model_type="discriminant",
        version=MODEL_VERSION,
        feature_set_id=FEATURE_SET_ID,
        description="Maczynska (1994) 6-variable discriminant model for Polish companies",
        hyperparameters={
            "coefficients": COEFFICIENTS,
            "cutoffs": {"critical": 0, "high": 1, "medium": 2},
        },
        is_baseline=True,
    )


def classify(z_score: float) -> tuple[int, str]:
    """Map Z-score to (classification, risk_category)."""
    if z_score < 0:
        return (1, "critical")
    elif z_score < 1:
        return (1, "high")
    elif z_score < 2:
        return (0, "medium")
    else:
        return (0, "low")


def score_report(report_id: str) -> Optional[dict]:
    """Score a single report using the Maczynska discriminant function.

    Returns dict with raw_score, classification, risk_category, feature_contributions,
    or None if required features are missing.
    """
    features = prediction_db.get_computed_features_for_report(report_id, valid_only=True)
    feature_map = {f["feature_definition_id"]: f["value"] for f in features}

    missing = [fid for fid in REQUIRED_FEATURES if fid not in feature_map or feature_map[fid] is None]
    if missing:
        logger.warning(
            "maczynska_missing_features",
            extra={
                "event": "maczynska_missing_features",
                "report_id": report_id,
                "missing": missing,
            },
        )
        return None

    z_score = 0.0
    contributions = {}
    for fid, coeff in COEFFICIENTS.items():
        value = feature_map[fid]
        contribution = coeff * value
        contributions[fid] = round(contribution, 6)
        z_score += contribution

    z_score = round(z_score, 6)
    classification, risk_category = classify(z_score)

    return {
        "raw_score": z_score,
        "classification": classification,
        "risk_category": risk_category,
        "feature_contributions": contributions,
    }


def score_batch(report_ids: Optional[list[str]] = None) -> dict:
    """Score multiple reports and write results to predictions.

    If report_ids is None, finds reports with computed Maczynska features
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
            )
            scored += 1

        except Exception as e:
            skipped += 1
            errors.append({"report_id": report_id, "error": str(e)})
            logger.error(
                "maczynska_score_error",
                extra={"event": "maczynska_score_error", "report_id": report_id, "error": str(e)},
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
        "maczynska_batch_completed",
        extra={
            "event": "maczynska_batch_completed",
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
    """Find reports that have all 6 Maczynska features computed but no prediction yet."""
    conn = prediction_db.get_conn()
    rows = conn.execute("""
        SELECT cf.report_id
        FROM latest_computed_features cf
        JOIN latest_successful_financial_reports fr ON fr.id = cf.report_id
        WHERE cf.feature_definition_id IN ('x1_maczynska', 'x2_maczynska', 'x3_maczynska',
                                           'x4_maczynska', 'x5_maczynska', 'x6_maczynska')
          AND cf.is_valid = true
        GROUP BY cf.report_id
        HAVING count(DISTINCT cf.feature_definition_id) = 6
        EXCEPT
        SELECT p.report_id
        FROM predictions p
        JOIN prediction_runs pr ON pr.id = p.prediction_run_id
        WHERE pr.model_id = %s
    """, [MODEL_ID]).fetchall()
    return [r[0] for r in rows]
