"""Verify SQL feature computation produces identical values to the Python engine."""
from __future__ import annotations

import json

import pytest

from app.db import pipeline_db
from pipeline.feature_compute import (
    _fetch_feature_definitions,
    compute_features_for_reports,
)
from app.services.feature_engine import _compute_single_feature


def _seed_simple_feature_defs(conn):
    defs = [
        {
            "id": "roa", "name": "ROA", "category": "profitability",
            "formula_numerator": "RZiS.L", "formula_denominator": "Aktywa",
            "required_tags": ["RZiS.L", "Aktywa"],
            "computation_logic": "ratio",
        },
        {
            "id": "log_total_assets", "name": "ln(Aktywa)",
            "category": "size",
            "formula_numerator": None, "formula_denominator": None,
            "required_tags": ["Aktywa"],
            "computation_logic": "custom",
        },
    ]
    for d in defs:
        conn.execute(
            """
            INSERT INTO feature_definitions
                (id, name, category, formula_numerator, formula_denominator,
                 required_tags, computation_logic, is_active)
            VALUES (%s, %s, %s, %s, %s, %s, %s, TRUE)
            ON CONFLICT (id) DO UPDATE SET name = excluded.name
            """,
            [d["id"], d["name"], d["category"], d["formula_numerator"],
             d["formula_denominator"],
             json.dumps(d["required_tags"]),
             d["computation_logic"]],
        )


def _insert_test_report_with_line_items(conn, report_id: str, krs: str,
                                         fiscal_year: int,
                                         values: dict[str, float]):
    conn.execute(
        """
        INSERT INTO financial_reports
            (id, logical_key, report_version, krs, fiscal_year,
             period_start, period_end, ingestion_status)
        VALUES (%s, %s, 1, %s, %s, '2022-01-01', '2022-12-31', 'completed')
        ON CONFLICT (id) DO NOTHING
        """,
        [report_id, f"{krs}|KRS|annual|{fiscal_year}|2022-12-31", krs, fiscal_year],
    )
    for tag, val in values.items():
        conn.execute(
            """
            INSERT INTO financial_line_items
                (report_id, section, tag_path, extraction_version,
                 value_current, currency)
            VALUES (%s, 'Bilans', %s, 1, %s, 'PLN')
            ON CONFLICT DO NOTHING
            """,
            [report_id, tag, val],
        )


def test_pipeline_features_match_python_feature_engine(dual_db):
    conn = pipeline_db.get_conn()
    _seed_simple_feature_defs(conn)
    _insert_test_report_with_line_items(
        conn, "rpt-match", "0000012345", 2022,
        {"RZiS.L": 50_000.0, "Aktywa": 1_000_000.0},
    )

    result = compute_features_for_reports(conn, ["rpt-match"])
    assert result["computed"] >= 1

    sql_rows = conn.execute(
        "SELECT feature_definition_id, value FROM computed_features "
        "WHERE report_id = %s AND is_valid = true",
        ["rpt-match"],
    ).fetchall()
    sql_values = {r[0]: r[1] for r in sql_rows}

    # Compare against Python reference
    defs = _fetch_feature_definitions(conn)
    values = {"RZiS.L": 50_000.0, "Aktywa": 1_000_000.0}
    for d in defs:
        py_val, py_valid, _ = _compute_single_feature(d, values)
        if py_valid:
            assert d["id"] in sql_values, f"{d['id']} missing from SQL results"
            assert sql_values[d["id"]] == pytest.approx(py_val, abs=1e-6)


def test_features_idempotent(dual_db):
    conn = pipeline_db.get_conn()
    _seed_simple_feature_defs(conn)
    _insert_test_report_with_line_items(
        conn, "rpt-idem", "0000099999", 2023,
        {"RZiS.L": 10.0, "Aktywa": 100.0},
    )
    compute_features_for_reports(conn, ["rpt-idem"])
    compute_features_for_reports(conn, ["rpt-idem"])
    cnt = conn.execute(
        "SELECT count(*) FROM computed_features WHERE report_id = 'rpt-idem'"
    ).fetchone()[0]
    # Only computation_version=1 rows, one per feature
    defs = _fetch_feature_definitions(conn)
    assert cnt == len(defs)
