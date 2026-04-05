"""Batched feature computation against the pipeline database.

Uses the existing feature formulas from `app.services.feature_engine` (custom
functions + ratio/difference/raw_value logic) as the reference implementation
— that guarantees parity with the existing scoring service.

The batching optimization is on the I/O side: we pull all line_items for a
batch of reports in ONE query, then compute all features in Python, then
bulk-insert the results with a single multi-row INSERT.
"""
from __future__ import annotations

import json
import logging
from typing import Optional

from app.db.connection import ConnectionWrapper
from app.services.feature_engine import _compute_single_feature

logger = logging.getLogger(__name__)


def _fetch_feature_definitions(conn: ConnectionWrapper) -> list[dict]:
    rows = conn.execute(
        """
        SELECT id, name, description, category, formula_description,
               formula_numerator, formula_denominator, required_tags,
               computation_logic, version, is_active
        FROM feature_definitions WHERE is_active = true
        ORDER BY id
        """
    ).fetchall()
    cols = ["id", "name", "description", "category", "formula_description",
            "formula_numerator", "formula_denominator", "required_tags",
            "computation_logic", "version", "is_active"]
    defs = []
    for row in rows:
        d = dict(zip(cols, row))
        if isinstance(d["required_tags"], str):
            try:
                d["required_tags"] = json.loads(d["required_tags"])
            except Exception:
                d["required_tags"] = None
        defs.append(d)
    return defs


def _fetch_report_values(
    conn: ConnectionWrapper,
    report_ids: list[str],
) -> dict[str, dict]:
    """Return {report_id: {'krs':..., 'fiscal_year':..., 'values': {tag: val}, 'ext_version': n}}."""
    if not report_ids:
        return {}
    rows = conn.execute(
        """
        SELECT fr.id, fr.krs, fr.fiscal_year
        FROM financial_reports fr
        WHERE fr.id = ANY(%s)
        """,
        [report_ids],
    ).fetchall()
    out: dict[str, dict] = {
        r[0]: {"krs": r[1], "fiscal_year": int(r[2] or 0),
               "values": {}, "ext_version": 0}
        for r in rows
    }

    items = conn.execute(
        """
        SELECT report_id, tag_path, value_current, extraction_version
        FROM latest_financial_line_items
        WHERE report_id = ANY(%s)
        """,
        [report_ids],
    ).fetchall()
    for rid, tag, val, ext in items:
        if rid in out:
            out[rid]["values"][tag] = val
            if (ext or 0) > out[rid]["ext_version"]:
                out[rid]["ext_version"] = int(ext or 0)
    return out


def _insert_computed_features(
    conn: ConnectionWrapper,
    rows: list[tuple],
) -> None:
    """Bulk insert computed_features. Rows are:
    (report_id, feature_definition_id, krs, fiscal_year, value, is_valid,
     error_message, source_extraction_version, computation_version)."""
    if not rows:
        return
    cur = conn.raw.cursor()
    from psycopg2.extras import execute_values
    execute_values(
        cur,
        """
        INSERT INTO computed_features
            (report_id, feature_definition_id, krs, fiscal_year, value,
             is_valid, error_message, source_extraction_version, computation_version)
        VALUES %s
        ON CONFLICT (report_id, feature_definition_id, computation_version) DO NOTHING
        """,
        rows,
    )


def compute_features_for_reports(
    conn: ConnectionWrapper,
    report_ids: list[str],
) -> dict:
    """Compute all active features for the given reports and write to computed_features.

    Returns {'computed': int, 'failed': int}.
    """
    if not report_ids:
        return {"computed": 0, "failed": 0}

    feature_defs = _fetch_feature_definitions(conn)
    report_data = _fetch_report_values(conn, report_ids)

    computed = 0
    failed = 0
    rows: list[tuple] = []

    for report_id, meta in report_data.items():
        values = meta["values"]
        krs = meta["krs"]
        fiscal_year = meta["fiscal_year"]
        ext_version = meta["ext_version"] or 1

        for fdef in feature_defs:
            value, is_valid, err = _compute_single_feature(fdef, values)
            rows.append((
                report_id, fdef["id"], krs, fiscal_year, value,
                is_valid, err, ext_version, 1,
            ))
            if is_valid:
                computed += 1
            else:
                failed += 1

    # Clear any prior computation at computation_version=1 for these reports
    # so the insert is idempotent.
    conn.execute(
        """
        DELETE FROM computed_features
        WHERE report_id = ANY(%s) AND computation_version = 1
        """,
        [report_ids],
    )
    _insert_computed_features(conn, rows)

    logger.info(
        "pipeline_features_computed",
        extra={"event": "pipeline_features_computed",
               "reports": len(report_ids),
               "computed": computed, "failed": failed},
    )
    return {"computed": computed, "failed": failed}


def compute_features_for_krs_list(
    conn: ConnectionWrapper,
    krs_list: list[str],
) -> dict:
    if not krs_list:
        return {"computed": 0, "failed": 0}
    rows = conn.execute(
        """
        SELECT id FROM financial_reports
        WHERE krs = ANY(%s) AND ingestion_status = 'completed'
        """,
        [krs_list],
    ).fetchall()
    report_ids = [r[0] for r in rows]
    return compute_features_for_reports(conn, report_ids)
