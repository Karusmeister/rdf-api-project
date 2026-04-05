"""Batched model scoring against the pipeline database.

For discriminant / linear models we reuse the Python scorers registered in
`app.services.predictions` (guaranteeing parity with the existing service).
For richer ML models we would load the joblib artifact from GCS/local and
apply it — that's stubbed out for future work and is a no-op if the artifact
path can't be loaded.
"""
from __future__ import annotations

import json
import logging
import uuid
from typing import Optional

from app.db.connection import ConnectionWrapper
from app.services.predictions import SCORERS

logger = logging.getLogger(__name__)


def _feature_ids_for_set(
    conn: ConnectionWrapper, feature_set_id: Optional[str]
) -> list[str]:
    if not feature_set_id:
        return []
    rows = conn.execute(
        """
        SELECT feature_definition_id
        FROM feature_set_members
        WHERE feature_set_id = %s
        ORDER BY ordinal
        """,
        [feature_set_id],
    ).fetchall()
    return [r[0] for r in rows]


def _get_active_models(conn: ConnectionWrapper) -> list[dict]:
    rows = conn.execute(
        """
        SELECT id, name, model_type, version, feature_set_id, artifact_path, is_baseline
        FROM model_registry WHERE is_active = true
        """
    ).fetchall()
    cols = ["id", "name", "model_type", "version", "feature_set_id", "artifact_path", "is_baseline"]
    return [dict(zip(cols, r)) for r in rows]


def _fetch_feature_matrix(
    conn: ConnectionWrapper,
    report_ids: list[str],
) -> dict[str, dict]:
    """{report_id: {'krs':..., 'features': {fid: val}, 'snapshot': {fid: version}}}.

    The `snapshot` sub-dict maps every feature we used when scoring to the
    computation_version that produced it. It gets persisted verbatim into
    predictions.feature_snapshot so downstream consumers can tell exactly
    which feature vector was used for each prediction (matching the CR-PZN
    behaviour added in prediction_db on main).
    """
    if not report_ids:
        return {}
    meta = {
        r[0]: {"krs": r[1], "features": {}, "snapshot": {}}
        for r in conn.execute(
            "SELECT id, krs FROM financial_reports WHERE id = ANY(%s)",
            [report_ids],
        ).fetchall()
    }
    rows = conn.execute(
        """
        SELECT report_id, feature_definition_id, value, computation_version
        FROM latest_computed_features
        WHERE report_id = ANY(%s) AND is_valid = true
        """,
        [report_ids],
    ).fetchall()
    for rid, fid, val, ver in rows:
        if rid in meta:
            meta[rid]["features"][fid] = val
            meta[rid]["snapshot"][fid] = int(ver or 1)
    return meta


def _create_prediction_run(conn: ConnectionWrapper, model_id: str) -> str:
    run_id = str(uuid.uuid4())
    conn.execute(
        """
        INSERT INTO prediction_runs (id, model_id, status, run_date, created_at)
        VALUES (%s, %s, 'running', now(), now())
        """,
        [run_id, model_id],
    )
    return run_id


def _finalize_prediction_run(
    conn: ConnectionWrapper,
    run_id: str,
    scored: int,
    status: str = "completed",
) -> None:
    conn.execute(
        """
        UPDATE prediction_runs
        SET status = %s, companies_scored = %s
        WHERE id = %s
        """,
        [status, scored, run_id],
    )


def score_reports(
    conn: ConnectionWrapper,
    report_ids: list[str],
) -> dict:
    """Score a batch of reports with every active model. Returns counts."""
    if not report_ids:
        return {"predictions_written": 0}

    models = _get_active_models(conn)
    if not models:
        logger.warning("no_active_models", extra={"event": "no_active_models"})
        return {"predictions_written": 0}

    feature_matrix = _fetch_feature_matrix(conn, report_ids)
    total_written = 0

    from psycopg2.extras import execute_values

    for model in models:
        model_id = model["id"]
        scorer = SCORERS.get(model_id)
        if scorer is None:
            logger.info(
                "no_scorer_for_model",
                extra={"event": "no_scorer_for_model", "model_id": model_id},
            )
            continue

        # Which feature defs does this model need? We scope the snapshot we
        # persist to JUST those, mirroring `poznanski.score_batch` /
        # `maczynska.score_batch` on main so the recorded snapshot is
        # meaningful instead of being "every feature in the matrix".
        model_feature_ids = _feature_ids_for_set(conn, model.get("feature_set_id"))

        run_id = _create_prediction_run(conn, model_id)
        rows: list[tuple] = []

        for report_id, data in feature_matrix.items():
            result = scorer(data["features"])
            if result is None:
                continue

            # Merge `contributions` (SCORERS convention) or
            # `feature_contributions` (batch scorer convention). Poznanski
            # additionally nests `_warnings` inside contributions so they
            # persist without a schema change.
            contributions = (
                result.get("contributions")
                or result.get("feature_contributions")
                or {}
            )
            # If the scorer returned standalone warnings, fold them into the
            # persisted contributions under the reserved `_warnings` key so
            # the API layer can extract them later (parity with main's
            # `_extract_warnings` contract).
            warnings = result.get("warnings") or []
            if warnings and "_warnings" not in contributions:
                contributions = {**contributions, "_warnings": list(warnings)}

            if model_feature_ids:
                snapshot = {
                    fid: data["snapshot"].get(fid, 1)
                    for fid in model_feature_ids
                    if fid in data["snapshot"]
                }
            else:
                snapshot = dict(data["snapshot"])

            rows.append((
                str(uuid.uuid4()),
                run_id,
                data["krs"],
                report_id,
                result.get("raw_score"),
                result.get("probability"),
                result.get("classification"),
                result.get("risk_category"),
                json.dumps(contributions),
                json.dumps(snapshot),
            ))

        if rows:
            cur = conn.raw.cursor()
            execute_values(
                cur,
                """
                INSERT INTO predictions
                    (id, prediction_run_id, krs, report_id, raw_score, probability,
                     classification, risk_category, feature_contributions,
                     feature_snapshot)
                VALUES %s
                ON CONFLICT (id) DO NOTHING
                """,
                rows,
            )

        _finalize_prediction_run(conn, run_id, scored=len(rows))
        total_written += len(rows)

    logger.info(
        "pipeline_scoring_done",
        extra={"event": "pipeline_scoring_done",
               "models": len(models),
               "predictions_written": total_written},
    )
    return {"predictions_written": total_written}


def score_krs_list(conn: ConnectionWrapper, krs_list: list[str]) -> dict:
    if not krs_list:
        return {"predictions_written": 0}
    rows = conn.execute(
        """
        SELECT id FROM financial_reports
        WHERE krs = ANY(%s) AND ingestion_status = 'completed'
        """,
        [krs_list],
    ).fetchall()
    return score_reports(conn, [r[0] for r in rows])
