"""Pipeline status and control endpoints.

All reads come from the pipeline database (`pipeline_db`). The scraper DB is
untouched. Admin-only endpoints require a JWT with admin role.
"""
from __future__ import annotations

import asyncio
import logging
from typing import Annotated, Optional

from fastapi import APIRouter, BackgroundTasks, HTTPException, Path, Query
from pydantic import BaseModel, Field

from app.auth import CurrentUser, require_admin
from app.db import pipeline_db
from pipeline.queue import enqueue_krs, get_queue_stats

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/pipeline", tags=["pipeline"])


class PipelineRunSummary(BaseModel):
    run_id: int
    started_at: str | None = None
    finished_at: str | None = None
    status: str
    trigger: str | None = None
    krs_queued: int = 0
    krs_processed: int = 0
    krs_failed: int = 0
    etl_docs_parsed: int = 0
    features_computed: int = 0
    predictions_written: int = 0
    total_duration_seconds: float | None = None
    error_message: str | None = None


class QueueStats(BaseModel):
    pending: int
    processing: int
    completed: int
    failed: int
    oldest_pending: str | None


class PipelineStatusResponse(BaseModel):
    recent_runs: list[PipelineRunSummary]
    queue: QueueStats


class EnqueueRequest(BaseModel):
    krs: list[str] = Field(description="KRS numbers to enqueue")
    reason: str = Field(default="manual", description="Trigger reason label")


def _row_to_run_summary(row) -> PipelineRunSummary:
    return PipelineRunSummary(
        run_id=int(row[0]),
        started_at=str(row[1]) if row[1] else None,
        finished_at=str(row[2]) if row[2] else None,
        status=row[3],
        trigger=row[4],
        krs_queued=int(row[5] or 0),
        krs_processed=int(row[6] or 0),
        krs_failed=int(row[7] or 0),
        etl_docs_parsed=int(row[8] or 0),
        features_computed=int(row[9] or 0),
        predictions_written=int(row[10] or 0),
        total_duration_seconds=float(row[11]) if row[11] is not None else None,
        error_message=row[12],
    )


@router.get("/status", summary="Pipeline status overview")
def status() -> PipelineStatusResponse:
    conn = pipeline_db.get_conn()
    rows = conn.execute(
        """
        SELECT run_id, started_at, finished_at, status, trigger,
               krs_queued, krs_processed, krs_failed,
               etl_docs_parsed, features_computed, predictions_written,
               total_duration_seconds, error_message
        FROM pipeline_runs
        ORDER BY run_id DESC
        LIMIT 20
        """
    ).fetchall()
    recent = [_row_to_run_summary(r) for r in rows]
    queue = QueueStats(**get_queue_stats(conn))
    return PipelineStatusResponse(recent_runs=recent, queue=queue)


@router.get("/runs/{run_id}", summary="Pipeline run detail")
def run_detail(run_id: Annotated[int, Path(ge=1)]) -> PipelineRunSummary:
    conn = pipeline_db.get_conn()
    row = conn.execute(
        """
        SELECT run_id, started_at, finished_at, status, trigger,
               krs_queued, krs_processed, krs_failed,
               etl_docs_parsed, features_computed, predictions_written,
               total_duration_seconds, error_message
        FROM pipeline_runs
        WHERE run_id = %s
        """,
        [run_id],
    ).fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail=f"Pipeline run {run_id} not found")
    return _row_to_run_summary(row)


@router.post("/queue", summary="Manually enqueue KRS numbers (admin)")
def enqueue(req: EnqueueRequest, user: CurrentUser) -> dict:
    require_admin(user)
    conn = pipeline_db.get_conn()
    for krs in req.krs:
        enqueue_krs(conn, krs, reason=req.reason)
    return {"enqueued": len(req.krs)}


@router.get("/peer-stats/{krs}", summary="Peer-group stats for a KRS")
def peer_stats(
    krs: Annotated[str, Path(pattern=r"^\d{1,10}$")],
    model_id: Annotated[str, Query()] = "maczynska_1994_v1",
) -> dict:
    """Return peer-group score distribution for this company (PKD × tenure bucket).

    Looks up the company's PKD + tenure bucket, then joins to `population_stats`
    to return mean/stddev/percentiles. The frontend uses this to compute a
    z-score and percentile rank for the company's current raw score.
    """
    conn = pipeline_db.get_conn()
    row = conn.execute(
        """
        SELECT pkd_code, incorporation_date
        FROM companies
        WHERE krs = %s
        """,
        [krs],
    ).fetchone()
    if row is None:
        return {"krs": krs, "peer_stats": None}
    pkd_code = row[0] or "UNKNOWN"

    # Best-effort tenure bucket (unknown if no incorporation_date)
    tenure_bucket = "unknown"
    if row[1] is not None:
        from datetime import date
        years = (date.today() - row[1]).days // 365
        if years <= 2:
            tenure_bucket = "early"
        elif years <= 7:
            tenure_bucket = "growth"
        elif years <= 15:
            tenure_bucket = "mature"
        else:
            tenure_bucket = "established"

    stats_row = conn.execute(
        """
        SELECT mean_score, stddev_score, p25, p50, p75, p90, p95, sample_size
        FROM population_stats
        WHERE pkd_code = %s AND tenure_bucket = %s AND model_id = %s
        """,
        [pkd_code, tenure_bucket, model_id],
    ).fetchone()
    if stats_row is None:
        return {"krs": krs, "peer_stats": None}

    return {
        "krs": krs,
        "peer_stats": {
            "pkd_code": pkd_code,
            "tenure_bucket": tenure_bucket,
            "peer_group_mean": float(stats_row[0]) if stats_row[0] is not None else None,
            "peer_group_stddev": float(stats_row[1]) if stats_row[1] is not None else None,
            "p25": float(stats_row[2]) if stats_row[2] is not None else None,
            "p50": float(stats_row[3]) if stats_row[3] is not None else None,
            "p75": float(stats_row[4]) if stats_row[4] is not None else None,
            "p90": float(stats_row[5]) if stats_row[5] is not None else None,
            "p95": float(stats_row[6]) if stats_row[6] is not None else None,
            "peer_group_size": int(stats_row[7] or 0),
        },
    }


@router.post("/trigger", summary="Trigger a pipeline run in background (admin)")
def trigger(
    user: CurrentUser,
    background: BackgroundTasks,
    limit: Annotated[Optional[int], Query(ge=1)] = None,
    skip_bq: Annotated[bool, Query()] = True,
) -> dict:
    """Queue a pipeline run on the API server. In production, the Cloud Run
    Job is triggered by Cloud Scheduler — this endpoint is for manual/dev
    runs and small batches."""
    require_admin(user)

    def _run():
        from pipeline.runner import run_pipeline
        try:
            run_pipeline(trigger="manual", limit=limit,
                         skip_bq=skip_bq, engine="postgres")
        except Exception:
            logger.error("pipeline_trigger_failed",
                         extra={"event": "pipeline_trigger_failed"},
                         exc_info=True)

    background.add_task(_run)
    return {"status": "triggered"}
