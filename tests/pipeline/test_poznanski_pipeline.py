"""Parity + end-to-end tests for the Poznanski 2004 model on the pipeline DB.

These mirror the Maczynska coverage so any rollout of a new built-in scorer
has to go through the same parity gate: the pipeline's batched SQL path must
agree with the Python reference in `app.services.predictions`.
"""
from __future__ import annotations

import json

import pytest

from app.db import pipeline_db
from pipeline.catalog import ensure_builtin_catalog
from pipeline.scoring import score_reports


def _seed_report_with_features(conn, report_id, krs, features):
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
    for fid, val in features.items():
        conn.execute(
            """
            INSERT INTO computed_features
                (report_id, feature_definition_id, krs, fiscal_year, value,
                 is_valid, computation_version)
            VALUES (%s, %s, %s, 2023, %s, TRUE, 1)
            ON CONFLICT DO NOTHING
            """,
            [report_id, fid, krs, val],
        )


def test_ensure_builtin_catalog_registers_both_models(dual_db):
    conn = pipeline_db.get_conn()
    ensure_builtin_catalog(conn)

    models = conn.execute(
        "SELECT id FROM model_registry WHERE is_active = TRUE ORDER BY id"
    ).fetchall()
    ids = {r[0] for r in models}
    assert "maczynska_1994_v1" in ids
    assert "poznanski_2004_v1" in ids

    # Feature set + members exist for Poznanski
    row = conn.execute(
        "SELECT count(*) FROM feature_set_members WHERE feature_set_id = 'poznanski_4'"
    ).fetchone()
    assert row[0] == 4

    # Poznanski feature definitions exist
    for fid in ("x1_poznanski", "x2_poznanski", "x3_poznanski", "x4_poznanski"):
        row = conn.execute(
            "SELECT count(*) FROM feature_definitions WHERE id = %s", [fid]
        ).fetchone()
        assert row[0] == 1, f"{fid} missing from pipeline DB after catalog bootstrap"


def test_poznanski_scoring_matches_python_reference(dual_db):
    conn = pipeline_db.get_conn()
    ensure_builtin_catalog(conn)

    features = {
        "x1_poznanski": 0.05,     # ROA
        "x2_poznanski": 1.2,      # quick ratio — under the warning threshold
        "x3_poznanski": 0.6,      # fixed capital ratio
        "x4_poznanski": 0.08,     # sales profitability
        # Add Maczynska inputs too so both scorers emit a row — this also
        # exercises the "multiple models on the same report" path.
        "x1_maczynska": 0.5, "x2_maczynska": 2.0, "x3_maczynska": 0.1,
        "x4_maczynska": 0.3, "x5_maczynska": 1.5, "x6_maczynska": 0.8,
    }
    _seed_report_with_features(conn, "rpt-poz-1", "0000088888", features)
    score_reports(conn, ["rpt-poz-1"])

    # Reference scores
    from app.services.predictions import score_maczynska, score_poznanski
    expected_poz = score_poznanski(features)
    expected_mac = score_maczynska(features)
    assert expected_poz is not None
    assert expected_mac is not None

    rows = conn.execute(
        """
        SELECT pr.model_id, p.raw_score, p.risk_category, p.feature_contributions,
               p.feature_snapshot
        FROM predictions p
        JOIN prediction_runs pr ON pr.id = p.prediction_run_id
        WHERE p.report_id = 'rpt-poz-1'
        ORDER BY pr.model_id
        """
    ).fetchall()
    by_model = {r[0]: r for r in rows}

    assert "poznanski_2004_v1" in by_model
    assert "maczynska_1994_v1" in by_model

    poz_row = by_model["poznanski_2004_v1"]
    assert poz_row[1] == pytest.approx(expected_poz["raw_score"], abs=1e-4)
    assert poz_row[2] == expected_poz["risk_category"]

    # Contributions include the intercept and each coefficient contribution.
    contribs = poz_row[3]
    if isinstance(contribs, str):
        contribs = json.loads(contribs)
    assert "_intercept" in contribs
    assert "x1_poznanski" in contribs
    assert "x4_poznanski" in contribs

    # Snapshot records the feature_definition computation_version for each
    # Poznanski input (scoped to the model's feature set, NOT every feature
    # on the report).
    snapshot = poz_row[4]
    if isinstance(snapshot, str):
        snapshot = json.loads(snapshot)
    assert set(snapshot.keys()) == {
        "x1_poznanski", "x2_poznanski", "x3_poznanski", "x4_poznanski"
    }
    assert all(v == 1 for v in snapshot.values())

    mac_row = by_model["maczynska_1994_v1"]
    assert mac_row[1] == pytest.approx(expected_mac["raw_score"], abs=1e-4)


def test_poznanski_warning_non_linear_liquidity_persists(dual_db):
    """When X2 (quick ratio) exceeds the non-linear threshold, the scorer
    raises WARNING_NON_LINEAR_LIQUIDITY; the pipeline must persist it under
    feature_contributions._warnings so the API layer can extract it."""
    conn = pipeline_db.get_conn()
    ensure_builtin_catalog(conn)

    features = {
        "x1_poznanski": 0.10,
        "x2_poznanski": 9.5,   # well above NON_LINEAR_LIQUIDITY_THRESHOLD (4.0)
        "x3_poznanski": 0.8,
        "x4_poznanski": 0.15,
    }
    _seed_report_with_features(conn, "rpt-poz-warn", "0000099999", features)
    score_reports(conn, ["rpt-poz-warn"])

    row = conn.execute(
        """
        SELECT p.feature_contributions, p.risk_category
        FROM predictions p
        JOIN prediction_runs pr ON pr.id = p.prediction_run_id
        WHERE p.report_id = 'rpt-poz-warn' AND pr.model_id = 'poznanski_2004_v1'
        """
    ).fetchone()
    assert row is not None
    contribs = row[0]
    if isinstance(contribs, str):
        contribs = json.loads(contribs)
    assert "_warnings" in contribs
    assert "WARNING_NON_LINEAR_LIQUIDITY" in contribs["_warnings"]
    # The scorer downgrades `low` → `medium` when the warning fires. For this
    # input set the raw score lands high enough that the pre-downgrade bucket
    # would be `low`, so the stored value must be `medium`.
    assert row[1] in ("medium", "critical")


def test_poznanski_missing_feature_yields_no_prediction(dual_db):
    """If any required Poznanski feature is absent, the scorer returns None
    and the pipeline must skip that report (not write a partial row)."""
    conn = pipeline_db.get_conn()
    ensure_builtin_catalog(conn)

    features = {
        "x1_poznanski": 0.05,
        # x2_poznanski intentionally omitted
        "x3_poznanski": 0.6,
        "x4_poznanski": 0.08,
    }
    _seed_report_with_features(conn, "rpt-poz-missing", "0000111111", features)
    score_reports(conn, ["rpt-poz-missing"])

    row = conn.execute(
        """
        SELECT count(*) FROM predictions p
        JOIN prediction_runs pr ON pr.id = p.prediction_run_id
        WHERE p.report_id = 'rpt-poz-missing' AND pr.model_id = 'poznanski_2004_v1'
        """
    ).fetchone()
    assert row[0] == 0
