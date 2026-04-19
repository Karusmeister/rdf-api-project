"""Scheduled KRS entity sync job.

Discovers new KRS numbers from krs_registry, re-enriches stale entities,
and writes results to krs_entities + krs_sync_log via krs_repo.

Note: The KRS Open API has no search endpoint, so discovery pulls KRS
numbers from the scraper's krs_registry table instead of adapter.search().
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone

from app.adapters.exceptions import AdapterError
from app.adapters.registry import get as get_adapter
from app.config import settings
from app.repositories import krs_repo
from app.scraper import db as scraper_db

logger = logging.getLogger(__name__)

# Module-level lock prevents overlapping runs (manual trigger during cron)
_running = asyncio.Lock()
_active_task: asyncio.Task[dict] | None = None


def _current_active_task() -> asyncio.Task[dict] | None:
    global _active_task
    if _active_task is not None and _active_task.done():
        _active_task = None
    return _active_task


def is_sync_running() -> bool:
    """Return True when a run is active or has been accepted but not finished."""
    return _running.locked() or _current_active_task() is not None


def _on_background_task_done(task: asyncio.Task[dict]) -> None:
    global _active_task
    if _active_task is task:
        _active_task = None
    try:
        task.result()
    except Exception:
        logger.exception("krs_sync_background_task_failed", extra={
            "event": "krs_sync_background_task_failed",
        })


async def _run_sync_with_lock() -> dict:
    try:
        return await _run_sync_inner()
    finally:
        if _running.locked():
            _running.release()


async def start_sync_task() -> bool:
    """Reserve the run slot and schedule a background sync task."""
    global _active_task

    if is_sync_running():
        return False

    await _running.acquire()
    try:
        task = asyncio.get_running_loop().create_task(
            _run_sync_with_lock(),
            name="krs_sync_manual",
        )
    except Exception:
        _running.release()
        raise

    _active_task = task
    task.add_done_callback(_on_background_task_done)
    return True


def _discover_new_krs_numbers(limit: int) -> list[str]:
    """Return KRS numbers that the scanner found but the sync job hasn't enriched yet.

    Post-dedupe (SCHEMA_DEDUPE_PLAN #2) there's a single krs_companies table.
    Scanner rows land with source = 'rdf_batch'; the sync job upgrades them
    by rewriting source = 'ms_gov' via krs_repo.upsert_from_krs_entity.
    """
    conn = scraper_db.get_conn()
    rows = conn.execute(
        """
        SELECT krs
        FROM krs_companies
        WHERE is_active = true
          AND source = 'rdf_batch'
        ORDER BY check_priority DESC, first_seen_at ASC
        LIMIT %s
        """,
        [limit],
    ).fetchall()
    return [r[0] for r in rows]


async def run_sync() -> dict:
    """Execute one sync cycle: discover + re-enrich.

    Returns a summary dict with counts. Idempotent — running twice
    with no upstream changes produces no new DB rows.
    """
    if _running.locked():
        logger.warning("krs_sync_skipped", extra={
            "event": "krs_sync_skipped", "reason": "already_running",
        })
        return {"status": "skipped", "reason": "already_running"}

    await _running.acquire()
    return await _run_sync_with_lock()


async def _run_sync_inner() -> dict:
    batch_size = settings.krs_sync_batch_size
    stale_hours = settings.krs_sync_stale_hours
    adapter = get_adapter("ms_gov")

    sync_id = krs_repo.log_sync_start()
    logger.info("krs_sync_started", extra={
        "event": "krs_sync_started", "sync_id": sync_id, "batch_size": batch_size,
    })

    new_count = 0
    updated_count = 0
    error_count = 0
    processed: list[str] = []

    try:
        # --- Phase 1: Discover new KRS numbers ---
        discovery_budget = 0
        if batch_size > 0:
            discovery_budget = min(batch_size, max(1, batch_size // 2))
        new_krs_numbers = _discover_new_krs_numbers(discovery_budget)

        for krs in new_krs_numbers:
            ok = await _sync_one(adapter, krs, processed)
            if ok is True:
                new_count += 1
            elif ok is False:
                error_count += 1

        # --- Phase 2: Re-enrich stale entities ---
        enrich_budget = batch_size - len(processed)
        if enrich_budget > 0:
            cutoff = datetime.now(timezone.utc) - timedelta(hours=stale_hours)
            stale = krs_repo.list_stale(cutoff)
            for entry in stale[:enrich_budget]:
                krs = entry["krs"]
                if krs in processed:
                    continue
                ok = await _sync_one(adapter, krs, processed)
                if ok is True:
                    updated_count += 1
                elif ok is False:
                    error_count += 1

        status = "completed"

    except Exception:
        status = "failed"
        logger.exception("krs_sync_failed", extra={
            "event": "krs_sync_failed", "sync_id": sync_id,
        })

    krs_count = len(processed)
    krs_repo.log_sync_finish(
        sync_id,
        krs_count=krs_count,
        new_count=new_count,
        updated_count=updated_count,
        error_count=error_count,
        status=status,
    )

    summary = {
        "sync_id": sync_id,
        "status": status,
        "krs_count": krs_count,
        "new_count": new_count,
        "updated_count": updated_count,
        "error_count": error_count,
    }
    logger.info("krs_sync_finished", extra={"event": "krs_sync_finished", **summary})
    return summary


async def _sync_one(
    adapter,
    krs: str,
    processed: list[str],
) -> bool | None:
    """Fetch and upsert a single entity. Returns True on success, False on
    error, None if entity not found upstream (404)."""
    processed.append(krs)
    try:
        entity = await adapter.get_entity(krs)
        if entity is None:
            logger.info("krs_sync_not_found", extra={
                "event": "krs_sync_not_found", "krs": krs,
            })
            return None
        krs_repo.upsert_from_krs_entity(entity, source="ms_gov")
        return True
    except AdapterError as exc:
        logger.warning("krs_sync_entity_error", extra={
            "event": "krs_sync_entity_error",
            "krs": krs,
            "error": str(exc),
        })
        return False
