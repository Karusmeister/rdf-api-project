"""
Predictions service — function-based scoring with caching.

SCORERS dict maps model_id -> pure scoring function.
Adding a new model = write a function + register it.
"""

from __future__ import annotations

import json
import logging
from typing import Callable

from app.db import prediction_db
from app.services.maczynska import COEFFICIENTS as MACZYNSKA_COEFFICIENTS
from app.services.maczynska import classify as maczynska_classify

logger = logging.getLogger(__name__)

SCORERS: dict[str, Callable] = {}

# ---------------------------------------------------------------------------
# Caches (populated at startup, invalidated on seed/deploy)
# ---------------------------------------------------------------------------

_feature_defs_cache: dict[str, dict] | None = None
_models_cache: list[dict] | None = None

INTERPRETATION: dict[str, dict] = {
    "maczynska_1994_v1": {
        "score_name": "Z-score (Zm)",
        "higher_is_better": True,
        "thresholds": [
            {"label": "critical", "max": 0, "summary": "Bankruptcy zone."},
            {"label": "high", "min": 0, "max": 1, "summary": "Weak condition."},
            {"label": "medium", "min": 1, "max": 2, "summary": "Acceptable."},
            {"label": "low", "min": 2, "summary": "Good condition."},
        ],
    }
}


def _get_feature_defs() -> dict[str, dict]:
    global _feature_defs_cache
    if _feature_defs_cache is None:
        defs = prediction_db.get_feature_definitions()
        _feature_defs_cache = {d["id"]: d for d in defs}
    return _feature_defs_cache


def _get_models() -> list[dict]:
    global _models_cache
    if _models_cache is None:
        _models_cache = prediction_db.get_models_with_details()
    return _models_cache


def invalidate_caches() -> None:
    global _feature_defs_cache, _models_cache
    _feature_defs_cache = None
    _models_cache = None


def warm_caches() -> None:
    _get_feature_defs()
    _get_models()
    logger.info("predictions_caches_warmed", extra={"event": "predictions_caches_warmed"})


# ---------------------------------------------------------------------------
# Scoring functions (pure, no DB)
# ---------------------------------------------------------------------------

def score_maczynska(features: dict[str, float | None]) -> dict | None:
    missing = [k for k in MACZYNSKA_COEFFICIENTS if features.get(k) is None]
    if missing:
        return None
    z = sum(MACZYNSKA_COEFFICIENTS[k] * features[k] for k in MACZYNSKA_COEFFICIENTS)
    z = round(z, 6)
    classification, risk_category = maczynska_classify(z)
    contributions = {k: round(MACZYNSKA_COEFFICIENTS[k] * features[k], 6) for k in MACZYNSKA_COEFFICIENTS}
    return {
        "raw_score": z,
        "classification": classification,
        "risk_category": risk_category,
        "contributions": contributions,
    }


def register_scorer(model_id: str, fn: Callable) -> None:
    SCORERS[model_id] = fn


register_scorer("maczynska_1994_v1", score_maczynska)


# ---------------------------------------------------------------------------
# Response assembly
# ---------------------------------------------------------------------------

def _build_interpretation(model_id: str, risk_category: str | None) -> dict | None:
    interp = INTERPRETATION.get(model_id)
    if interp is None:
        return None
    thresholds = []
    for t in interp["thresholds"]:
        thresholds.append({
            **t,
            "is_current": t["label"] == risk_category,
        })
    return {
        "score_name": interp["score_name"],
        "higher_is_better": interp["higher_is_better"],
        "thresholds": thresholds,
    }


def _build_features(report_id: str, feature_set_id: str, contributions: dict | None) -> list[dict]:
    features_data = prediction_db.get_features_for_report(report_id, feature_set_id)
    if not features_data:
        return []

    # Collect all required tags across features
    all_tags = set()
    for f in features_data:
        tags = f.get("required_tags")
        if tags:
            if isinstance(tags, str):
                tags = json.loads(tags)
            if isinstance(tags, list):
                all_tags.update(tags)

    # Fetch source line items in one query
    source_items = {}
    if all_tags:
        items = prediction_db.get_source_line_items_for_report(report_id, list(all_tags))
        for item in items:
            source_items[item["tag_path"]] = item

    result = []
    for f in features_data:
        tags = f.get("required_tags")
        if isinstance(tags, str):
            tags = json.loads(tags)

        source_tags = []
        if tags and isinstance(tags, list):
            for tp in tags:
                si = source_items.get(tp)
                if si:
                    source_tags.append({
                        "tag_path": tp,
                        "label_pl": si.get("label_pl"),
                        "value_current": si.get("value_current"),
                        "value_previous": si.get("value_previous"),
                        "section": si.get("section"),
                    })

        contribution = None
        if contributions and f["feature_definition_id"] in contributions:
            contribution = contributions[f["feature_definition_id"]]

        result.append({
            "feature_id": f["feature_definition_id"],
            "name": f["name"],
            "category": f.get("category"),
            "value": f["value"],
            "contribution": contribution,
            "formula_description": f.get("formula_description"),
            "source_tags": source_tags,
        })
    return result


def get_predictions(krs: str) -> dict:
    company = prediction_db.get_company(krs)
    company_info = {
        "krs": krs,
        "name": None,
        "nip": None,
        "pkd_code": None,
    }
    if company:
        company_info.update({
            "nip": company.get("nip"),
            "pkd_code": company.get("pkd_code"),
        })

    raw_predictions = prediction_db.get_predictions_fat(krs)
    if not raw_predictions:
        history = prediction_db.get_prediction_history_fat(krs)
        history_entries = [
            {
                "model_id": h["model_id"],
                "model_name": h["model_name"],
                "model_version": h["model_version"],
                "fiscal_year": h["fiscal_year"],
                "raw_score": h["raw_score"],
                "probability": h["probability"],
                "classification": h["classification"],
                "risk_category": h["risk_category"],
                "scored_at": h["scored_at"],
            }
            for h in history
        ]
        return {
            "company": company_info,
            "predictions": [],
            "history": history_entries,
        }

    # Group by model, take latest prediction per model
    seen_models: dict[str, dict] = {}
    for p in raw_predictions:
        mid = p["model_id"]
        if mid not in seen_models:
            seen_models[mid] = p

    predictions = []
    for p in seen_models.values():
        contributions = p.get("feature_contributions")
        features = _build_features(
            p["report_id"],
            p["feature_set_id"],
            contributions,
        ) if p["feature_set_id"] else []

        predictions.append({
            "model": {
                "model_id": p["model_id"],
                "model_name": p["model_name"],
                "model_type": p["model_type"],
                "model_version": p["model_version"],
                "is_baseline": p["is_baseline"],
                "description": p["model_description"],
            },
            "result": {
                "raw_score": p["raw_score"],
                "probability": p["probability"],
                "classification": p["classification"],
                "risk_category": p["risk_category"],
            },
            "interpretation": _build_interpretation(p["model_id"], p["risk_category"]),
            "features": features,
            "data_source": {
                "report_id": p["report_id"],
                "fiscal_year": p["fiscal_year"],
                "period_start": p["period_start"],
                "period_end": p["period_end"],
                "report_version": p["report_version"],
                "data_source_id": p["data_source_id"],
                "ingested_at": p["ingested_at"],
            },
            "scored_at": p["scored_at"],
        })

    # Build history
    history_data = prediction_db.get_prediction_history_fat(krs)
    history_entries = [
        {
            "model_id": h["model_id"],
            "model_name": h["model_name"],
            "model_version": h["model_version"],
            "fiscal_year": h["fiscal_year"],
            "raw_score": h["raw_score"],
            "probability": h["probability"],
            "classification": h["classification"],
            "risk_category": h["risk_category"],
            "scored_at": h["scored_at"],
        }
        for h in history_data
    ]

    return {
        "company": company_info,
        "predictions": predictions,
        "history": history_entries,
    }


def get_history(krs: str, model_id: str | None = None) -> dict:
    history_data = prediction_db.get_prediction_history_fat(krs, model_id=model_id)
    return {
        "krs": krs,
        "history": [
            {
                "model_id": h["model_id"],
                "model_name": h["model_name"],
                "model_version": h["model_version"],
                "fiscal_year": h["fiscal_year"],
                "raw_score": h["raw_score"],
                "probability": h["probability"],
                "classification": h["classification"],
                "risk_category": h["risk_category"],
                "scored_at": h["scored_at"],
            }
            for h in history_data
        ],
    }


def get_models() -> dict:
    models_data = _get_models()
    models = []
    for m in models_data:
        interp = INTERPRETATION.get(m["id"])
        interpretation = None
        if interp:
            interpretation = {
                "score_name": interp["score_name"],
                "higher_is_better": interp["higher_is_better"],
                "thresholds": [
                    {"label": t["label"], "min": t.get("min"), "max": t.get("max"), "summary": t["summary"], "is_current": False}
                    for t in interp["thresholds"]
                ],
            }
        models.append({
            "model_id": m["id"],
            "model_name": m["name"],
            "model_type": m["model_type"],
            "model_version": m["version"],
            "is_baseline": m["is_baseline"],
            "description": m.get("description"),
            "feature_set_id": m.get("feature_set_id"),
            "interpretation": interpretation,
        })
    return {"models": models}
