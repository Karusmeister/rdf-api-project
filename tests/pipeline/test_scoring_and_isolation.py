"""Scoring and scraper-db isolation tests."""
from __future__ import annotations

import json

import pytest

from app.db import pipeline_db
from app.db.connection import make_connection
from pipeline.feature_compute import compute_features_for_reports
from pipeline.scoring import score_reports


def _register_maczynska(conn):
    from app.services.maczynska import COEFFICIENTS
    conn.execute(
        """
        INSERT INTO model_registry
            (id, name, model_type, version, feature_set_id, description,
             hyperparameters, is_active, is_baseline)
        VALUES ('maczynska_1994_v1', 'maczynska', 'discriminant', '1994_v1',
                'maczynska_6', 'test', %s, TRUE, TRUE)
        ON CONFLICT (name, version) DO NOTHING
        """,
        [json.dumps({"coefficients": COEFFICIENTS})],
    )


def _seed_maczynska_feature_defs(conn):
    for fid in ("x1_maczynska", "x2_maczynska", "x3_maczynska",
                "x4_maczynska", "x5_maczynska", "x6_maczynska"):
        conn.execute(
            """
            INSERT INTO feature_definitions
                (id, name, category, computation_logic, is_active)
            VALUES (%s, %s, 'maczynska', 'raw_value', TRUE)
            ON CONFLICT (id) DO NOTHING
            """,
            [fid, fid],
        )


def _seed_report_with_features(conn, report_id: str, krs: str,
                                feature_values: dict[str, float]):
    conn.execute(
        """
        INSERT INTO financial_reports
            (id, logical_key, report_version, krs, fiscal_year,
             period_start, period_end, ingestion_status)
        VALUES (%s, %s, 1, %s, 2023, '2023-01-01', '2023-12-31', 'completed')
        ON CONFLICT DO NOTHING
        """,
        [report_id, f"{krs}|KRS|annual|2023|2023-12-31", krs],
    )
    for fid, val in feature_values.items():
        conn.execute(
            """
            INSERT INTO computed_features
                (report_id, feature_definition_id, krs, fiscal_year,
                 value, is_valid, computation_version)
            VALUES (%s, %s, %s, 2023, %s, TRUE, 1)
            ON CONFLICT DO NOTHING
            """,
            [report_id, fid, krs, val],
        )


def test_scoring_matches_python_reference(dual_db):
    conn = pipeline_db.get_conn()
    _register_maczynska(conn)
    _seed_maczynska_feature_defs(conn)

    features = {
        "x1_maczynska": 0.5,
        "x2_maczynska": 2.0,
        "x3_maczynska": 0.1,
        "x4_maczynska": 0.3,
        "x5_maczynska": 1.5,
        "x6_maczynska": 0.8,
    }
    _seed_report_with_features(conn, "rpt-score-1", "0000055555", features)

    score_reports(conn, ["rpt-score-1"])

    # Reference score via the Python scorer
    from app.services.predictions import score_maczynska
    expected = score_maczynska(features)
    assert expected is not None

    row = conn.execute(
        """
        SELECT raw_score, risk_category
        FROM predictions
        WHERE report_id = 'rpt-score-1'
        """
    ).fetchone()
    assert row is not None
    assert row[0] == pytest.approx(expected["raw_score"], abs=1e-4)
    assert row[1] == expected["risk_category"]


def test_pipeline_never_writes_to_scraper_db(dual_db):
    """Running pipeline-side operations must not introduce any row in the
    scraper DB or create any pipeline_* tables there."""
    scraper = make_connection(dual_db["pg_dsn"])
    before_rows = scraper.execute(
        "SELECT count(*) FROM krs_registry"
    ).fetchone()[0]

    conn = pipeline_db.get_conn()
    _register_maczynska(conn)
    _seed_maczynska_feature_defs(conn)
    _seed_report_with_features(conn, "rpt-iso", "0000077777", {
        "x1_maczynska": 0.1, "x2_maczynska": 0.2, "x3_maczynska": 0.3,
        "x4_maczynska": 0.4, "x5_maczynska": 0.5, "x6_maczynska": 0.6,
    })
    score_reports(conn, ["rpt-iso"])

    # No new tables
    tbl_names = {
        r[0] for r in scraper.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = 'public'"
        ).fetchall()
    }
    assert "pipeline_queue" not in tbl_names
    assert "pipeline_runs" not in tbl_names
    assert "population_stats" not in tbl_names

    after_rows = scraper.execute(
        "SELECT count(*) FROM krs_registry"
    ).fetchone()[0]
    assert after_rows == before_rows
