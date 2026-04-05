"""
Predictions service — function-based scoring with caching.

SCORERS dict maps model_id -> pure scoring function.
Adding a new model = write a function + register it.
"""

from __future__ import annotations

import json
import logging
import re
from typing import Callable

from app.db import prediction_db
from app.services.maczynska import COEFFICIENTS as MACZYNSKA_COEFFICIENTS
from app.services.maczynska import classify as maczynska_classify
from app.services.poznanski import (
    COEFFICIENTS as POZNANSKI_COEFFICIENTS,
    INTERCEPT as POZNANSKI_INTERCEPT,
    NON_LINEAR_LIQUIDITY_THRESHOLD as POZNANSKI_X2_THRESHOLD,
    WARNING_NON_LINEAR_LIQUIDITY,
    classify as poznanski_classify,
)
from app.services.schema_labels import SCHEMA_REGISTRY

logger = logging.getLogger(__name__)

SCORERS: dict[str, Callable] = {}
FORMULA_TAG_PATTERN = re.compile(r"[A-Z][A-Za-z0-9]*(?:[._][A-Za-z0-9]+)+")

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
    },
    "poznanski_2004_v1": {
        "score_name": "Z-score (Poznanski)",
        "higher_is_better": True,
        "thresholds": [
            {"label": "critical", "max": 0, "summary": "Bankruptcy risk zone."},
            {"label": "medium", "min": 0, "max": 1, "summary": "Stable but monitor."},
            {"label": "low", "min": 1, "summary": "Good condition."},
        ],
    },
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


def score_poznanski(features: dict[str, float | None]) -> dict | None:
    missing = [k for k in POZNANSKI_COEFFICIENTS if features.get(k) is None]
    if missing:
        return None
    z = float(POZNANSKI_INTERCEPT) + sum(
        POZNANSKI_COEFFICIENTS[k] * features[k] for k in POZNANSKI_COEFFICIENTS
    )
    z = round(z, 6)
    classification, risk_category = poznanski_classify(z)
    contributions: dict = {"_intercept": round(float(POZNANSKI_INTERCEPT), 6)}
    for k in POZNANSKI_COEFFICIENTS:
        contributions[k] = round(POZNANSKI_COEFFICIENTS[k] * features[k], 6)

    warnings: list[str] = []
    x2 = features.get("x2_poznanski")
    if x2 is not None and x2 > POZNANSKI_X2_THRESHOLD:
        warnings.append(WARNING_NON_LINEAR_LIQUIDITY)
        if risk_category == "low":
            risk_category = "medium"
    if warnings:
        contributions["_warnings"] = warnings

    return {
        "raw_score": z,
        "classification": classification,
        "risk_category": risk_category,
        "contributions": contributions,
    }


register_scorer("poznanski_2004_v1", score_poznanski)


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


def _extract_formula_tags(formula_description: str | None) -> list[str]:
    if not formula_description:
        return []
    return [match.group(0) for match in FORMULA_TAG_PATTERN.finditer(formula_description)]


def _collect_feature_tags(feature_row: dict) -> list[str]:
    ordered_tags: list[str] = []
    seen: set[str] = set()

    for tag in _extract_formula_tags(feature_row.get("formula_description")):
        if tag not in seen:
            ordered_tags.append(tag)
            seen.add(tag)

    required_tags = feature_row.get("required_tags")
    if isinstance(required_tags, str):
        required_tags = json.loads(required_tags)
    if isinstance(required_tags, list):
        for tag in required_tags:
            if isinstance(tag, str) and tag not in seen:
                ordered_tags.append(tag)
                seen.add(tag)

    return ordered_tags


def _infer_section_from_tag(tag_path: str) -> str:
    if tag_path.startswith("CF."):
        return "CF"
    if tag_path.startswith("RZiS."):
        return "RZiS"
    return "Bilans"


# BE-PRED-010: semantic direction per source tag.
#
# Explicit, per-tag registry at full-tag-path granularity (no prefix
# inheritance). Adding a new entry is a deliberate domain decision — every row
# has been reviewed against the canonical Polish financial-statement taxonomy
# and tracks "does a higher value indicate better financial health?". Tags
# outside this registry resolve to `None` and the frontend renders them with
# neutral coloring; we do NOT guess for unknowns because prefix-based rules
# can silently miscolor siblings that happen to carry opposite business
# meaning (e.g. a `Pasywa_B_*` bucket where one child is equity-like).
#
# Scope: covers every tag used by the Mączyńska model's features
# (x1..x6 → RZiS.I, RZiS.A, CF.A_II_1, Pasywa_B, Aktywa, Aktywa_B_I) plus the
# nearest-neighbor tags the frontend exposes in the expanded detail panel.
# Extend deliberately and paired with a test in
# tests/services/test_predictions_service.py.
_TAG_SEMANTIC_REGISTRY: dict[str, bool] = {
    # --- Profit & loss statement (RZiS) — higher is generally better. ---
    "RZiS.A": True,    # Przychody netto ze sprzedaży (net revenue)
    "RZiS.C": True,    # Zysk brutto ze sprzedaży (gross profit on sales)
    "RZiS.F": True,    # Zysk z działalności operacyjnej (operating profit)
    "RZiS.I": True,    # Zysk brutto (pre-tax profit)
    "RZiS.L": True,    # Zysk netto (net profit)
    # --- Cash flow statement (CF) — operating cash + depreciation. ---
    "CF.A_II_1": True,  # Amortyzacja (depreciation add-back in CF)
    # --- Balance sheet: equity (Pasywa_A) — higher is better. ---
    "Pasywa_A": True,   # Kapitał (fundusz) własny (equity)
    # --- Balance sheet: liabilities (Pasywa_B) — higher is worse. ---
    "Pasywa_B": False,  # Zobowiązania i rezerwy na zobowiązania (total liabilities)
    # --- Balance sheet: current inventory (Aktywa_B_I) — bloating is negative. ---
    "Aktywa_B_I": False,  # Zapasy (inventories)
    # --- Neutral tags: intentionally omitted (resolve to None). ---
    # "Aktywa"       — size proxy, neither good nor bad alone.
    # "Aktywa_B_II"  — short-term receivables: direction is context-dependent.
    # "Aktywa_B_III" — cash & short-term investments: context-dependent.
    # "Aktywa_A"     — fixed assets: neutral.
    # "Pasywa_B_III" — short-term liabilities: already covered by Pasywa_B when exposed.
}


def _resolve_higher_is_better(tag_path: str) -> bool | None:
    """Look up the semantic direction of a tag.

    Returns True if higher values indicate better financial health, False if
    higher values are a negative signal, None if the tag is not in the
    registry (neutral — frontend colors it muted). Matching is exact on the
    full `tag_path` by design — see `_TAG_SEMANTIC_REGISTRY` docstring for
    rationale.
    """
    return _TAG_SEMANTIC_REGISTRY.get(tag_path)


def _resolve_tag_label(tag_path: str, schema_code: str | None) -> str | None:
    schema = SCHEMA_REGISTRY.get(schema_code or "SFJINZ")
    labels = schema["tag_labels"] if schema else {}
    raw = tag_path.split(".")[-1] if "." in tag_path else tag_path
    return labels.get(tag_path) or labels.get(raw)


def _build_features(
    report_id: str,
    feature_set_id: str,
    contributions: dict | None,
    scored_at: str | None = None,
    schema_code: str | None = None,
) -> list[dict]:
    """Single-prediction feature builder (kept for legacy callers and tests).

    Hot path uses `get_features_for_predictions_batch` via `get_predictions`;
    this helper issues one query per report and must not be used in loops.
    """
    features_data = prediction_db.get_features_for_prediction(
        report_id,
        feature_set_id,
        scored_at=scored_at,
    )
    if not features_data and scored_at is not None:
        features_data = prediction_db.get_features_for_prediction(
            report_id,
            feature_set_id,
            scored_at=None,
        )
    if not features_data:
        return []

    all_tags: set[str] = set()
    for f in features_data:
        all_tags.update(_collect_feature_tags(f))

    source_items_by_tag: dict[str, dict] = {}
    if all_tags:
        items = prediction_db.get_source_line_items_for_report(report_id, sorted(all_tags))
        source_items_by_tag = {it["tag_path"]: it for it in items}

    return _assemble_features(
        features_data=features_data,
        source_items_by_tag=source_items_by_tag,
        contributions=contributions,
        schema_code=schema_code,
    )


def _assemble_features(
    features_data: list[dict],
    source_items_by_tag: dict[str, dict],
    contributions: dict | None,
    schema_code: str | None,
) -> list[dict]:
    """Assemble the per-feature response shape from pre-loaded data.

    `source_items_by_tag` is the map of already-fetched source items for the
    report. `schema_code` is the report-level schema used only as a last-ditch
    fallback when an item has no schema_code of its own.
    """
    result = []
    for f in features_data:
        tags = _collect_feature_tags(f)
        source_tags = []
        for tp in tags:
            si = source_items_by_tag.get(tp, {})
            # CR-005: resolve label against the item's own schema first, then
            # the report-level schema, then global fallback.
            item_schema = si.get("schema_code") or schema_code
            source_tags.append({
                "tag_path": tp,
                "label_pl": si.get("label_pl") or _resolve_tag_label(tp, item_schema),
                "value_current": si.get("value_current"),
                "value_previous": si.get("value_previous"),
                "section": si.get("section") or _infer_section_from_tag(tp),
                "higher_is_better": _resolve_higher_is_better(tp),
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

    # Return one entry per (model_id, fiscal_year). When a fiscal year has
    # been rescored, keep the first row — `get_predictions_fat` already orders
    # by scored_at DESC, report_version DESC within the same (model, year).
    # Assign a stable per-prediction request_id so feature-loader keys never
    # collapse across distinct predictions that happen to share a report +
    # feature set (R3-001/002/003).
    seen_rows: dict[tuple[str, int], dict] = {}
    for p in raw_predictions:
        key = (p["model_id"], p["fiscal_year"])
        if key not in seen_rows:
            p = dict(p)
            p["_request_id"] = f"{p['model_id']}::{p['fiscal_year']}::{p['report_id']}"
            seen_rows[key] = p

    # Batch-load features per prediction (keyed by request_id) and source items
    # per report in two DB round-trips regardless of the prediction count.
    feature_requests = [
        {
            "request_id": p["_request_id"],
            "report_id": p["report_id"],
            "feature_set_id": p["feature_set_id"],
            "feature_snapshot": p.get("feature_snapshot"),
            "scored_at": p.get("scored_at"),
            "model_id": p.get("model_id"),
            "fiscal_year": p.get("fiscal_year"),
        }
        for p in seen_rows.values()
        if p.get("feature_set_id")
    ]
    features_by_request = prediction_db.get_features_for_predictions_batch(feature_requests)

    # Gather all tags per report for the source-items batch.
    tags_by_report: dict[str, set[str]] = {}
    for p in seen_rows.values():
        if not p.get("feature_set_id"):
            continue
        for f in features_by_request.get(p["_request_id"], []):
            tags_by_report.setdefault(p["report_id"], set()).update(_collect_feature_tags(f))

    source_requests = [
        (report_id, sorted(tags)) for report_id, tags in tags_by_report.items() if tags
    ]
    source_items_by_report = prediction_db.get_source_line_items_for_reports_batch(source_requests)

    predictions = []
    for p in seen_rows.values():
        contributions = p.get("feature_contributions")
        fs_id = p.get("feature_set_id")
        if fs_id:
            features_data = features_by_request.get(p["_request_id"], [])
            items_list = source_items_by_report.get(p["report_id"], [])
            items_by_tag = {it["tag_path"]: it for it in items_list}
            features = _assemble_features(
                features_data=features_data,
                source_items_by_tag=items_by_tag,
                contributions=contributions,
                schema_code=p.get("schema_code"),
            )
        else:
            features = []

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
