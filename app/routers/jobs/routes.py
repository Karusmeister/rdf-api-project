"""Endpoints for KRS sync job and KRS sequential scanner."""

from __future__ import annotations

import logging

from fastapi import APIRouter, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from app.jobs import krs_scanner, krs_sync
from app.repositories import krs_repo

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# KRS Sync endpoints
# ---------------------------------------------------------------------------

sync_router = APIRouter(prefix="/jobs/krs-sync", tags=["jobs"])


@sync_router.get("/status", summary="Last KRS sync run")
async def sync_status():
    """Return the last sync log entry."""
    last = krs_repo.get_last_sync(source="ms_gov")
    if last is None:
        return {"message": "No sync runs recorded yet"}
    return last


@sync_router.post("/trigger", summary="Trigger KRS sync")
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


# ---------------------------------------------------------------------------
# KRS Scanner endpoints (PKR-42)
# ---------------------------------------------------------------------------

scan_router = APIRouter(prefix="/jobs/krs-scan", tags=["jobs"])


class ResetCursorBody(BaseModel):
    """Reset the sequential scanner cursor."""

    next_krs_int: int = Field(description="KRS integer to resume scanning from (0-99999999)")


@scan_router.get("/status", summary="Scanner status")
async def scan_status():
    """Return current scanner state: cursor, running status, last run."""
    cursor = krs_repo.get_cursor()
    last_run = krs_repo.get_last_scan_run()
    total_entities = krs_repo.count_entities()
    return {
        "cursor": cursor,
        "is_running": krs_scanner.is_scan_running(),
        "last_run": last_run,
        "total_entities": total_entities,
    }


@scan_router.post("/trigger", summary="Trigger KRS scan")
async def trigger_scan():
    """Fire run_scan() in a background task. 202 if accepted, 409 if running."""
    logger.info("krs_scan_manual_trigger", extra={"event": "krs_scan_manual_trigger"})
    scheduled = await krs_scanner.start_scan_task()
    if not scheduled:
        return JSONResponse(
            status_code=status.HTTP_409_CONFLICT,
            content={"status": "skipped", "reason": "already_running"},
        )
    return JSONResponse(
        status_code=status.HTTP_202_ACCEPTED,
        content={"status": "scheduled"},
    )


@scan_router.post("/stop", summary="Stop running scan")
async def stop_scan():
    """Signal the running scan to stop after its current probe."""
    krs_scanner.request_stop()
    return {"status": "stop_requested"}


@scan_router.post("/reset-cursor", summary="Reset scan cursor")
async def reset_cursor(body: ResetCursorBody):
    """Reset the scan cursor to an arbitrary position. Rejected if running."""
    if not 0 <= body.next_krs_int <= 99_999_999:
        return JSONResponse(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            content={"status": "rejected", "reason": "next_krs_int must be between 0 and 99999999"},
        )
    if krs_scanner.is_scan_running():
        return JSONResponse(
            status_code=status.HTTP_409_CONFLICT,
            content={"status": "rejected", "reason": "scan_running"},
        )
    krs_repo.advance_cursor(body.next_krs_int)
    return {"status": "ok", "cursor": body.next_krs_int}


# ---------------------------------------------------------------------------
# Combined router
# ---------------------------------------------------------------------------

router = APIRouter()
router.include_router(sync_router)
router.include_router(scan_router)
