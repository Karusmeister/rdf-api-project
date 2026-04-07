"""On-demand assessment pipeline endpoints.

POST /api/assessment/{krs}       — check readiness or start pipeline (200 / 202)
GET  /api/assessment/jobs/{job_id} — poll job progress
"""

from __future__ import annotations

import asyncio
import functools
import logging
from typing import Annotated

from fastapi import APIRouter, BackgroundTasks, HTTPException, Path, Request

from app.auth import CurrentUser, require_krs_access
from app.services import assessment as assessment_service
from app.services.activity import activity_logger

from .schemas import (
    AssessmentProgress,
    DataSummary,
    JobStatusResponse,
    StartAssessmentResponse,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/assessment", tags=["assessment"])


@router.post("/{krs}", summary="Start or check KRS assessment")
async def start_assessment(
    krs: Annotated[str, Path(pattern=r"^\d{1,10}$")],
    user: CurrentUser,
    background: BackgroundTasks,
    request: Request,
) -> StartAssessmentResponse:
    """Check if prediction data is ready for this KRS. If not, start the pipeline.

    Returns 200 if data is already complete (navigate to predictions).
    Returns 202 if the pipeline was started or is already running (poll for progress).
    """
    require_krs_access(krs, user)
    padded_krs = krs.zfill(10)

    loop = asyncio.get_running_loop()

    # Check if data is already ready
    summary = await loop.run_in_executor(
        None, functools.partial(assessment_service.check_data_readiness, padded_krs),
    )

    if assessment_service.is_data_ready(summary):
        background.add_task(
            activity_logger.log,
            user["id"], "assessment_check", padded_krs,
            {"ready": True},
            request.client.host if request.client else None,
        )
        return StartAssessmentResponse(
            job_id=None,
            krs=padded_krs,
            status="ready",
            message="All data available",
            data_summary=DataSummary(**summary),
        )

    # Start or attach to existing pipeline
    job_id, is_new = await loop.run_in_executor(
        None, functools.partial(assessment_service.start_assessment, padded_krs),
    )

    if is_new:
        background.add_task(assessment_service.run_pipeline, job_id, padded_krs)
        logger.info(
            "assessment_started",
            extra={"event": "assessment_started", "job_id": job_id, "krs": padded_krs},
        )
    else:
        logger.info(
            "assessment_attached",
            extra={"event": "assessment_attached", "job_id": job_id, "krs": padded_krs},
        )

    background.add_task(
        activity_logger.log,
        user["id"], "assessment_trigger", padded_krs,
        {"job_id": job_id, "is_new": is_new},
        request.client.host if request.client else None,
    )

    return StartAssessmentResponse(
        job_id=job_id,
        krs=padded_krs,
        status="pending" if is_new else "running",
        stage=None,
        message="Assessment pipeline started" if is_new else "Analysis already in progress",
        data_summary=DataSummary(**summary),
    )


@router.get("/jobs/{job_id}", summary="Poll assessment job progress")
async def get_job_status(
    job_id: Annotated[str, Path()],
    user: CurrentUser,
) -> JobStatusResponse:
    """Poll the status of an assessment pipeline job."""
    loop = asyncio.get_running_loop()
    job = await loop.run_in_executor(
        None, functools.partial(assessment_service.get_job_status, job_id),
    )
    if job is None:
        raise HTTPException(status_code=404, detail="Assessment job not found")

    # Parse progress from result_json
    progress = None
    result_json = job.get("result_json")
    if isinstance(result_json, str):
        import json
        result_json = json.loads(result_json)
    if isinstance(result_json, dict) and "progress" in result_json:
        progress = AssessmentProgress(**result_json["progress"])

    return JobStatusResponse(
        job_id=job["id"],
        krs=job["krs"],
        status=job["status"],
        stage=job["stage"],
        progress=progress,
        error_message=job.get("error_message"),
        created_at=job["created_at"],
        updated_at=job["updated_at"],
    )
