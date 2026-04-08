"""
Maczynska-Zawadzki (2006) SHV discriminant model — bankruptcy predictor.

ZMZ = 9.498*W1 + 3.566*W2 + 2.903*W3 + 0.452*W4 - 1.498

Where:
  W1 = Operating profit / Total assets
  W2 = Equity / Total assets
  W3 = (Net profit + Depreciation) / Total liabilities
  W4 = Current assets / Short-term liabilities

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

MODEL_ID = "maczynska_2006_v1"
MODEL_NAME = "maczynska2006"
MODEL_VERSION = "2006_v1"
FEATURE_SET_ID = "maczynska2006_4"

COEFFICIENTS = {
    "w1_maczynska2006": 9.498,
    "w2_maczynska2006": 3.566,
    "w3_maczynska2006": 2.903,
    "w4_maczynska2006": 0.452,
}
INTERCEPT = -1.498

REQUIRED_FEATURES = list(COEFFICIENTS.keys())


def ensure_model_registered() -> None:
    """Insert maczynska_2006_v1 into model_registry if not already present."""
    prediction_db.register_model(
        model_id=MODEL_ID,
        name=MODEL_NAME,
        model_type="discriminant",
        version=MODEL_VERSION,
        feature_set_id=FEATURE_SET_ID,
        description=(
            "Maczynska-Zawadzki (2006) SHV 4-variable "
            "discriminant model for Polish companies"
        ),
        hyperparameters={
            "coefficients": COEFFICIENTS,
            "intercept": INTERCEPT,
            "cutoffs": {"critical": 0, "medium": 1},
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
    """Pure scoring path that takes pre-loaded feature rows."""
    feature_map = {f["feature_definition_id"]: f["value"] for f in features}
    version_map = {f["feature_definition_id"]: f["computation_version"] for f in features}

    missing = [
        fid for fid in REQUIRED_FEATURES
        if fid not in feature_map or feature_map[fid] is None
    ]
    if missing:
        logger.warning(
            "maczynska2006_missing_features",
            extra={
                "event": "maczynska2006_missing_features",
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

    feature_snapshot = {fid: version_map[fid] for fid in REQUIRED_FEATURES}

    return {
        "raw_score": z_score,
        "classification": classification,
        "risk_category": risk_category,
        "feature_contributions": contributions,
        "feature_snapshot": feature_snapshot,
    }


def score_report(report_id: str) -> Optional[dict]:
    """Score a single report using the Maczynska-Zawadzki 2006 discriminant function.

    Returns dict with raw_score, classification, risk_category, feature_contributions,
    feature_snapshot, or None if required features are missing.
    """
    features = prediction_db.get_computed_features_for_report(report_id, valid_only=True)
    return _score_from_feature_rows(report_id, features)


def score_batch(report_ids: Optional[list[str]] = None) -> dict:
    """Score multiple reports and write results to predictions.

    If report_ids is None, finds reports with computed Maczynska 2006 features
    but no existing prediction for this model.
    """
    if report_ids is None:
        report_ids = _find_unscored_reports()

    if not report_ids:
        logger.info(
            "maczynska2006_batch_noop",
            extra={"event": "maczynska2006_batch_noop", "model_id": MODEL_ID},
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

    batch_level_error: str | None = None
    try:
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
                code = type(e).__name__
                error_counts[code] = error_counts.get(code, 0) + 1
                logger.error(
                    "maczynska2006_score_error",
                    extra={
                        "event": "maczynska2006_score_error",
                        "model_id": MODEL_ID,
                        "run_id": run_id,
                        "report_id": report_id,
                        "error_code": code,
                    },
                    exc_info=True,
                )

        if prediction_rows:
            prediction_db.insert_predictions_batch(prediction_rows)

    except Exception as e:
        batch_level_error = f"batch_error:{type(e).__name__}"
        logger.error(
            "maczynska2006_batch_error",
            extra={
                "event": "maczynska2006_batch_error",
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
                "maczynska2006_finalize_failed",
                extra={
                    "event": "maczynska2006_finalize_failed",
                    "model_id": MODEL_ID,
                    "run_id": run_id,
                },
                exc_info=True,
            )

    logger.info(
        "maczynska2006_batch_completed",
        extra={
            "event": "maczynska2006_batch_completed",
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
    """Find reports that have all 4 Maczynska 2006 features computed but no prediction yet."""
    conn = prediction_db.get_conn()
    rows = conn.execute("""
        SELECT cf.report_id
        FROM latest_computed_features cf
        JOIN latest_successful_financial_reports fr ON fr.id = cf.report_id
        WHERE cf.feature_definition_id IN ('w1_maczynska2006', 'w2_maczynska2006',
                                           'w3_maczynska2006', 'w4_maczynska2006')
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
