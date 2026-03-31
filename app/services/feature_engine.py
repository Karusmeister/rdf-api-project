"""
Feature computation engine.

Reads financial_line_items + feature_definitions, computes ratios and features,
writes results to computed_features.
"""

from __future__ import annotations

import logging
import math
from typing import Callable, Optional

from app.db import prediction_db

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Custom function registry
# ---------------------------------------------------------------------------

_CUSTOM_FUNCTIONS: dict[str, Callable[[dict[str, Optional[float]], dict], Optional[float]]] = {}


def register_custom(feature_id: str):
    """Decorator to register a custom computation function."""
    def decorator(fn: Callable):
        _CUSTOM_FUNCTIONS[feature_id] = fn
        return fn
    return decorator


@register_custom("quick_ratio")
def _quick_ratio(values: dict[str, Optional[float]], fdef: dict) -> Optional[float]:
    """(Current Assets - Inventory) / Short-term Liabilities."""
    current_assets = values.get("Aktywa_B")
    inventory = values.get("Aktywa_B_I")
    st_liabilities = values.get("Pasywa_B_III")
    if current_assets is None or st_liabilities is None or st_liabilities == 0:
        return None
    inv = inventory or 0.0
    return (current_assets - inv) / st_liabilities


@register_custom("log_total_assets")
def _log_total_assets(values: dict[str, Optional[float]], fdef: dict) -> Optional[float]:
    """ln(Total Assets)."""
    v = values.get("Aktywa")
    if v is None or v <= 0:
        return None
    return math.log(v)


@register_custom("x1_maczynska")
def _x1_maczynska(
    values: dict[str, Optional[float]], fdef: dict
) -> Optional[float]:
    """(Gross profit + Depreciation) / Total liabilities.

    Gross profit       = RZiS.I  (Zysk brutto)
    Depreciation       = CF.A_II_1  (Amortyzacja from cash flow)
    Total liabilities  = Pasywa_B  (Zobowiazania i rezerwy)
    """
    gross_profit = values.get("RZiS.I")
    depreciation = values.get("CF.A_II_1")
    liabilities = values.get("Pasywa_B")

    if gross_profit is None or liabilities is None or liabilities == 0:
        return None
    # Depreciation may be absent for micro entities (no cash flow statement)
    dep = depreciation or 0.0
    return (gross_profit + dep) / liabilities


@register_custom("log_revenue")
def _log_revenue(values: dict[str, Optional[float]], fdef: dict) -> Optional[float]:
    """ln(Revenue)."""
    v = values.get("RZiS.A")
    if v is None or v <= 0:
        return None
    return math.log(v)


# ---------------------------------------------------------------------------
# Core computation
# ---------------------------------------------------------------------------

def _get_tag_value(tag_path: str, values: dict[str, Optional[float]]) -> Optional[float]:
    """Look up a tag value from the flat values dict."""
    return values.get(tag_path)


def _compute_single_feature(
    fdef: dict,
    values: dict[str, Optional[float]],
) -> tuple[Optional[float], bool, Optional[str]]:
    """
    Compute a single feature value.
    Returns (value, is_valid, error_message).
    """
    logic = fdef.get("computation_logic", "ratio")
    feature_id = fdef["id"]

    if logic == "custom":
        fn = _CUSTOM_FUNCTIONS.get(feature_id)
        if fn is None:
            return None, False, f"no_custom_function:{feature_id}"
        try:
            result = fn(values, fdef)
            if result is None:
                # Determine which tag was missing
                for tag in (fdef.get("required_tags") or []):
                    if isinstance(tag, str) and values.get(tag) is None:
                        return None, False, f"missing_tag:{tag}"
                return None, False, "computation_returned_null"
            return round(result, 6), True, None
        except Exception as e:
            return None, False, f"custom_error:{e}"

    elif logic == "ratio":
        num_tag = fdef.get("formula_numerator")
        den_tag = fdef.get("formula_denominator")

        if not num_tag or not den_tag:
            return None, False, "missing_formula"

        num = _get_tag_value(num_tag, values)
        den = _get_tag_value(den_tag, values)

        if num is None:
            return None, False, f"missing_tag:{num_tag}"
        if den is None:
            return None, False, f"missing_tag:{den_tag}"
        if den == 0:
            return None, False, "division_by_zero"

        return round(num / den, 6), True, None

    elif logic == "difference":
        num_tag = fdef.get("formula_numerator")
        den_tag = fdef.get("formula_denominator")

        if not num_tag or not den_tag:
            return None, False, "missing_formula"

        num = _get_tag_value(num_tag, values)
        den = _get_tag_value(den_tag, values)

        if num is None:
            return None, False, f"missing_tag:{num_tag}"
        if den is None:
            return None, False, f"missing_tag:{den_tag}"

        return round(num - den, 6), True, None

    elif logic == "raw_value":
        tag = fdef.get("formula_numerator")
        if not tag:
            return None, False, "missing_formula"
        val = _get_tag_value(tag, values)
        if val is None:
            return None, False, f"missing_tag:{tag}"
        return round(val, 6), True, None

    else:
        return None, False, f"unknown_logic:{logic}"


def compute_features_for_report(
    report_id: str,
    feature_set_id: Optional[str] = None,
) -> dict:
    """
    Compute features for a given report.

    Args:
        report_id: The financial report to compute features for.
        feature_set_id: If provided, only compute features in this set.

    Returns:
        Summary dict with computed/failed counts.
    """
    # Load report metadata
    report = prediction_db.get_financial_report(report_id)
    if report is None:
        raise ValueError(f"Report {report_id} not found")

    krs = report["krs"]
    fiscal_year = report["fiscal_year"]

    # Load all line items into flat dict: {tag_path: value_current}
    items = prediction_db.get_line_items(report_id)
    values: dict[str, Optional[float]] = {}
    for item in items:
        values[item["tag_path"]] = item["value_current"]
    source_extraction_version = max((int(item["extraction_version"]) for item in items), default=0)

    # Determine which features to compute
    if feature_set_id:
        members = prediction_db.get_feature_set_members(feature_set_id)
        feature_ids = [m["feature_definition_id"] for m in members]
        all_defs = prediction_db.get_feature_definitions(active_only=True)
        feature_defs = [d for d in all_defs if d["id"] in feature_ids]
    else:
        feature_defs = prediction_db.get_feature_definitions(active_only=True)

    computed = 0
    failed = 0
    results = {}

    for fdef in feature_defs:
        value, is_valid, error_msg = _compute_single_feature(fdef, values)

        prediction_db.upsert_computed_feature(
            report_id=report_id,
            feature_definition_id=fdef["id"],
            krs=krs,
            fiscal_year=fiscal_year,
            value=value,
            is_valid=is_valid,
            error_message=error_msg,
            source_extraction_version=source_extraction_version,
        )

        results[fdef["id"]] = value
        if is_valid:
            computed += 1
        else:
            failed += 1

    logger.info(
        "features_computed",
        extra={
            "event": "features_computed",
            "report_id": report_id,
            "computed": computed,
            "failed": failed,
        },
    )

    return {
        "report_id": report_id,
        "krs": krs,
        "fiscal_year": fiscal_year,
        "computed": computed,
        "failed": failed,
        "features": results,
    }


def get_features_for_report(report_id: str) -> dict[str, Optional[float]]:
    """Return a clean dict of {feature_code: value} for a report."""
    report = prediction_db.get_financial_report(report_id)
    if report is None:
        raise ValueError(f"Report {report_id} not found")

    features = prediction_db.get_computed_features_for_report(report["id"])
    return {f["feature_definition_id"]: f["value"] for f in features}


def compute_all_pending() -> dict:
    """Find reports whose latest extraction version has no computed features yet."""
    conn = prediction_db.get_conn()
    rows = conn.execute("""
        WITH latest_line_items AS (
            SELECT report_id, max(extraction_version) AS latest_extraction_version
            FROM financial_line_items
            GROUP BY report_id
        ),
        latest_feature_inputs AS (
            SELECT report_id, max(source_extraction_version) AS latest_feature_extraction_version
            FROM computed_features
            GROUP BY report_id
        )
        SELECT fr.id
        FROM latest_successful_financial_reports fr
        JOIN latest_line_items lli ON lli.report_id = fr.id
        LEFT JOIN latest_feature_inputs lfi ON lfi.report_id = fr.id
        WHERE coalesce(lfi.latest_feature_extraction_version, 0) < lli.latest_extraction_version
    """).fetchall()

    results = {"total": len(rows), "computed": 0, "failed": 0, "errors": []}

    for (report_id,) in rows:
        try:
            result = compute_features_for_report(report_id)
            results["computed"] += 1
        except Exception as e:
            results["failed"] += 1
            results["errors"].append({"report_id": report_id, "error": str(e)})
            logger.error(
                "feature_compute_failed",
                extra={"event": "feature_compute_failed", "report_id": report_id, "error": str(e)},
                exc_info=True,
            )

    return results


def recompute(report_id: str) -> dict:
    """Recompute features and append a new computation version."""
    return compute_features_for_report(report_id)
