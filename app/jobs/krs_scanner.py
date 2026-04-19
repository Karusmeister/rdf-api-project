"""Resumable sequential KRS integer scanner (PKR-41).

Probes KRS numbers sequentially (1, 2, 3…) via the MS Gov adapter,
writes discovered entities to krs_entities and krs_registry, and
tracks progress in krs_scan_cursor + krs_scan_runs.

The scanner can be killed and restarted at any time — it resumes from
the cursor. ``advance_cursor`` is called after EVERY probe so a crash
loses at most one probe.
"""

from __future__ import annotations

import asyncio
import logging

from app.adapters.exceptions import AdapterError, EntityNotFoundError, RateLimitedError
from app.adapters.registry import get as get_adapter
from app.config import settings
from app.repositories import krs_repo

logger = logging.getLogger(__name__)

# Module-level concurrency controls
_scan_lock: asyncio.Lock = asyncio.Lock()
_stop_event: asyncio.Event = asyncio.Event()
_active_task: asyncio.Task[dict] | None = None

CHECKPOINT_INTERVAL = settings.krs_scan_checkpoint_interval
RATE_LIMIT_BACKOFF_S = settings.krs_scan_rate_limit_backoff_s
MAX_CONSECUTIVE_ERRORS = settings.krs_scan_max_consecutive_errors


def is_scan_running() -> bool:
    """Return True when a scan is active or has been accepted but not finished."""
    return _scan_lock.locked() or _current_active_task() is not None


def request_stop() -> None:
    """Signal the running scan to stop after its current probe."""
    _stop_event.set()


def _current_active_task() -> asyncio.Task[dict] | None:
    global _active_task
    if _active_task is not None and _active_task.done():
        _active_task = None
    return _active_task


def _on_background_task_done(task: asyncio.Task[dict]) -> None:
    global _active_task
    if _active_task is task:
        _active_task = None
    try:
        task.result()
    except Exception:
        logger.exception("krs_scan_background_task_failed", extra={
            "event": "krs_scan_background_task_failed",
        })


async def start_scan_task(batch_size: int | None = None) -> bool:
    """Reserve the run slot and schedule a background scan task.

    Returns True if accepted, False if already running.
    """
    global _active_task

    if is_scan_running():
        return False

    await _scan_lock.acquire()
    try:
        task = asyncio.get_running_loop().create_task(
            _run_scan_with_lock(batch_size),
            name="krs_scan_manual",
        )
    except Exception:
        _scan_lock.release()
        raise

    _active_task = task
    task.add_done_callback(_on_background_task_done)
    return True


async def _run_scan_with_lock(batch_size: int | None = None) -> dict:
    try:
        return await run_scan(batch_size=batch_size, _already_locked=True)
    finally:
        if _scan_lock.locked():
            _scan_lock.release()


async def run_scan(
    batch_size: int | None = None,
    *,
    _already_locked: bool = False,
) -> dict:
    """Probe KRS integers sequentially from the cursor position.

    Returns a summary dict. Safe to call while a cron-fired scan is
    already running — returns immediately with status='skipped'.
    """
    if not _already_locked:
        if _scan_lock.locked():
            logger.warning("krs_scan_skipped", extra={
                "event": "krs_scan_skipped", "reason": "already_running",
            })
            return {"status": "skipped", "reason": "already_running"}
        await _scan_lock.acquire()

    try:
        return await _run_scan_inner(batch_size)
    finally:
        if not _already_locked and _scan_lock.locked():
            _scan_lock.release()


async def _run_scan_inner(batch_size: int | None = None) -> dict:
    size = batch_size or settings.krs_scan_batch_size
    adapter = get_adapter("ms_gov")

    cursor_start = krs_repo.get_cursor()
    run_id = krs_repo.open_scan_run(krs_from=cursor_start)

    logger.info("krs_scan_started", extra={
        "event": "krs_scan_started",
        "run_id": run_id,
        "krs_from": cursor_start,
        "batch_size": size,
    })

    probed_count = 0
    valid_count = 0
    error_count = 0
    consecutive_errors = 0
    last_probed = cursor_start
    stopped_by_signal = False
    status = "completed"

    for krs_int in range(cursor_start, cursor_start + size):
        # Check for graceful stop
        if _stop_event.is_set():
            stopped_by_signal = True
            break

        krs_str = str(krs_int).zfill(10)
        try:
            entity = await adapter.get_entity(krs_str)
            if entity is not None:
                # Post-dedupe: krs_repo + scraper scheduling share one table,
                # so a single upsert covers what used to be two writes.
                krs_repo.upsert_from_krs_entity(entity, source="ms_gov_scan")
                valid_count += 1
            consecutive_errors = 0
        except EntityNotFoundError:
            consecutive_errors = 0  # 404 is expected, not an error
        except RateLimitedError as exc:
            logger.warning("krs_scan_rate_limited", extra={
                "event": "krs_scan_rate_limited",
                "krs_int": krs_int,
                "backoff_s": RATE_LIMIT_BACKOFF_S,
            })
            error_count += 1
            consecutive_errors += 1
            await asyncio.sleep(RATE_LIMIT_BACKOFF_S)
        except AdapterError as exc:
            logger.warning("krs_scan_probe_error", extra={
                "event": "krs_scan_probe_error",
                "krs_int": krs_int,
                "error": str(exc),
            })
            error_count += 1
            consecutive_errors += 1
        except Exception as exc:
            logger.exception("krs_scan_probe_unexpected", extra={
                "event": "krs_scan_probe_unexpected",
                "krs_int": krs_int,
            })
            error_count += 1
            consecutive_errors += 1

        probed_count += 1
        last_probed = krs_int

        # Advance cursor after EVERY probe — crash loses at most 1
        krs_repo.advance_cursor(krs_int + 1)

        # Periodic checkpoint
        if probed_count % CHECKPOINT_INTERVAL == 0:
            krs_repo.update_scan_run(
                run_id,
                probed_count=probed_count,
                valid_count=valid_count,
                error_count=error_count,
            )

        # Bail if too many consecutive errors (upstream probably down)
        if consecutive_errors >= MAX_CONSECUTIVE_ERRORS:
            logger.error("krs_scan_too_many_errors", extra={
                "event": "krs_scan_too_many_errors",
                "run_id": run_id,
                "consecutive_errors": consecutive_errors,
            })
            status = "failed"
            break

    if stopped_by_signal:
        stopped_reason = "signal"
    elif status == "failed":
        stopped_reason = "error"
    else:
        stopped_reason = "batch_limit"

    krs_repo.close_scan_run(
        run_id,
        status=status,
        krs_to=last_probed,
        stopped_reason=stopped_reason,
        probed_count=probed_count,
        valid_count=valid_count,
        error_count=error_count,
    )

    _stop_event.clear()

    summary = {
        "status": status,
        "run_id": run_id,
        "krs_from": cursor_start,
        "krs_to": last_probed,
        "probed_count": probed_count,
        "valid_count": valid_count,
        "error_count": error_count,
        "stopped_reason": stopped_reason,
    }
    logger.info("krs_scan_finished", extra={"event": "krs_scan_finished", **summary})
    return summary
