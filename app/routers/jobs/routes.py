"""Endpoints for the KRS sync job: status check and manual trigger."""

from __future__ import annotations

import logging

from fastapi import APIRouter, status
from fastapi.responses import JSONResponse

from app.jobs import krs_sync
from app.repositories import krs_repo

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/jobs/krs-sync", tags=["jobs"])


@router.get("/status")
async def sync_status():
    """Return the last sync log entry."""
    last = krs_repo.get_last_sync(source="ms_gov")
    if last is None:
        return {"message": "No sync runs recorded yet"}
    return last


@router.post("/trigger")
async def trigger_sync():
    """Manually trigger a KRS sync run without holding the request open."""
    logger.info("krs_sync_manual_trigger", extra={"event": "krs_sync_manual_trigger"})
    scheduled = await krs_sync.start_sync_task()
    if not scheduled:
        return JSONResponse(
            status_code=status.HTTP_409_CONFLICT,
            content={"status": "skipped", "reason": "already_running"},
        )
    return JSONResponse(
        status_code=status.HTTP_202_ACCEPTED,
        content={"status": "scheduled"},
    )
