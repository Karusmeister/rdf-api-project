import asyncio
import functools
import logging
from typing import Annotated

from fastapi import APIRouter, BackgroundTasks, HTTPException, Path, Query, Request

from app.auth import CurrentUser, require_krs_access, require_admin
from app.db import prediction_db
from app.services import assessment as assessment_service
from app.services import predictions as predictions_service
from app.services.activity import activity_logger

from .schemas import (
    HistoryResponse,
    ModelsResponse,
    PipelineStatusResponse,
    PipelineTriggerResponse,
    PredictionResponse,
)

logger = logging.getLogger(__name__)

_MAX_CONCURRENT_PIPELINES = 5

router = APIRouter(prefix="/api/predictions", tags=["predictions"])


@router.get("/models", summary="List active prediction models")
def list_models() -> ModelsResponse:
    """Return all active models with their interpretation thresholds. No authentication required."""
    return predictions_service.get_models()


@router.get("/{krs}", summary="Get predictions for a company")
def get_predictions(
    krs: Annotated[str, Path(pattern=r"^\d{1,10}$")],
    user: CurrentUser,
    request: Request,
    background: BackgroundTasks,
) -> PredictionResponse:
    """Full prediction detail for a KRS number: scores, features with source financial data,
    interpretation thresholds, and score history. Requires JWT auth and KRS access."""
    require_krs_access(krs, user)
    result = predictions_service.get_predictions(krs)
    company = result["company"]
    has_company_data = any(company.get(k) is not None for k in ("nip", "pkd_code"))
    if not has_company_data and not result["predictions"] and not result["history"]:
        raise HTTPException(status_code=404, detail=f"No data found for KRS {krs}")
    background.add_task(
        activity_logger.log,
        user["id"],
        "prediction_view",
        krs,
        None,
        request.client.host if request.client else None,
    )
    return result


@router.get("/{krs}/history", summary="Get prediction score history")
def get_history(
    krs: Annotated[str, Path(pattern=r"^\d{1,10}$")],
    user: CurrentUser,
    model_id: Annotated[str | None, Query()] = None,
) -> HistoryResponse:
    """Score timeline for a company, ordered by fiscal year. Optionally filter by model_id.
    Useful for charting score trends over time. Requires JWT auth and KRS access."""
    require_krs_access(krs, user)
    return predictions_service.get_history(krs, model_id=model_id)


@router.post("/{krs}/generate", summary="Trigger on-demand pipeline")
async def generate_predictions(
    krs: Annotated[str, Path(pattern=r"^\d{1,10}$")],
    user: CurrentUser,
    background: BackgroundTasks,
    request: Request,
) -> PipelineTriggerResponse:
    """Trigger the full download + ETL + scoring pipeline for a KRS number.

    If the pipeline is already running for this KRS, returns the existing job.
    Enforces a max of 5 concurrent pipelines across all users.

    Dedup + concurrency check + job creation happen inside a single
    advisory-locked DB call to prevent TOCTOU races under concurrent requests.
    """
    require_krs_access(krs, user)
    padded_krs = krs.zfill(10)

    loop = asyncio.get_running_loop()

    # Atomic: dedup + concurrency guard + job creation in one DB call.
    # Uses a dedicated pooled connection — safe to call from executor.
    result = await loop.run_in_executor(
        None,
        functools.partial(
            prediction_db.atomic_start_pipeline, padded_krs, _MAX_CONCURRENT_PIPELINES,
        ),
    )

    if result["outcome"] == "existing":
        return PipelineTriggerResponse(
            job_id=result["job_id"],
            krs=padded_krs,
            status="running",
            message="Pipeline already in progress for this KRS",
        )

    if result["outcome"] == "rejected":
        raise HTTPException(
            status_code=429,
            detail=f"Maximum concurrent pipelines ({_MAX_CONCURRENT_PIPELINES}) reached. Try again later.",
        )

    # outcome == "created"
    job_id = result["job_id"]
    is_new = True
    background.add_task(assessment_service.run_pipeline, job_id, padded_krs)
    logger.info(
        "pipeline_triggered",
        extra={"event": "pipeline_triggered", "job_id": job_id, "krs": padded_krs},
    )

    background.add_task(
        activity_logger.log,
        user["id"], "pipeline_trigger", padded_krs,
        {"job_id": job_id, "is_new": is_new},
        request.client.host if request.client else None,
    )

    return PipelineTriggerResponse(
        job_id=job_id,
        krs=padded_krs,
        status="pending",
        message="Pipeline started",
    )


@router.get("/{krs}/status", summary="Get pipeline status for a KRS")
async def get_pipeline_status(
    krs: Annotated[str, Path(pattern=r"^\d{1,10}$")],
    user: CurrentUser,
) -> PipelineStatusResponse:
    """Poll the current pipeline stage for a KRS. Returns the latest job status."""
    require_krs_access(krs, user)
    padded_krs = krs.zfill(10)

    loop = asyncio.get_running_loop()
    job = await loop.run_in_executor(
        None, functools.partial(prediction_db.get_running_assessment_for_krs, padded_krs),
    )

    if job is None:
        # Check for the most recent completed/failed job
        job = await loop.run_in_executor(
            None, functools.partial(prediction_db.get_latest_assessment_for_krs, padded_krs),
        )

    if job is None:
        raise HTTPException(status_code=404, detail=f"No pipeline found for KRS {padded_krs}")

    progress_data = None
    diagnosis = None
    result_json = job.get("result_json")
    if result_json:
        import json as _json
        parsed = _json.loads(result_json) if isinstance(result_json, str) else result_json
        if isinstance(parsed, dict):
            if "progress" in parsed:
                progress_data = parsed["progress"]
            diagnosis = parsed.get("diagnosis")

    return PipelineStatusResponse(
        job_id=job["id"],
        krs=padded_krs,
        status=job["status"],
        current_stage=job.get("stage"),
        error_message=job.get("error_message"),
        created_at=str(job["created_at"]),
        progress=progress_data,
        diagnosis=diagnosis,
    )


@router.post("/cache/invalidate", tags=["admin"], summary="Flush prediction caches")
def invalidate_cache(user: CurrentUser):
    """Admin-only. Flush the in-memory model and feature definition caches.
    Use after seeding new models or feature definitions."""
    require_admin(user)
    predictions_service.invalidate_caches()
    return {"status": "caches_invalidated"}
