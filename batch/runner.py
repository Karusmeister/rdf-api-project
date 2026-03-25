"""Multiprocessing orchestrator for the batch KRS scanner.

Spawns N worker processes with stride-offset starting KRS numbers.
VPN is handled at the OS level (e.g. `nordvpn connect pl157`) before
running the batch — all workers share the same tunnel.

Usage:
    python -m batch.runner [options]

All flags are optional and fall back to settings from .env / app/config.py.
"""

import argparse
import logging
import multiprocessing
import signal

from app.config import settings
from batch.worker import run_worker

logger = logging.getLogger(__name__)


def run_batch(
    *,
    start_krs: int | None = None,
    workers: int | None = None,
    concurrency: int | None = None,
    delay: float | None = None,
    db_path: str | None = None,
) -> None:
    """Spawn N worker processes and block until all exit.

    All arguments are optional — defaults come from settings.
    """
    _start = start_krs if start_krs is not None else settings.batch_start_krs
    _workers = workers if workers is not None else settings.batch_workers
    _concurrency = concurrency if concurrency is not None else settings.batch_concurrency_per_worker
    _delay = delay if delay is not None else settings.batch_delay_seconds
    _db = db_path if db_path is not None else settings.batch_db_path

    logger.info(
        "batch_start workers=%d start_krs=%d concurrency=%d delay=%.1f db=%s",
        _workers, _start, _concurrency, _delay, _db,
    )

    processes: list[multiprocessing.Process] = []
    for worker_id in range(_workers):
        p = multiprocessing.Process(
            target=run_worker,
            name=f"krs-worker-{worker_id}",
            kwargs=dict(
                worker_id=worker_id,
                start_krs=_start + worker_id,  # stride offset
                stride=_workers,
                concurrency=_concurrency,
                delay=_delay,
                db_path=_db,
            ),
        )
        processes.append(p)

    # Graceful shutdown on SIGINT/SIGTERM
    def _shutdown(signum, frame):
        logger.info("batch_shutdown signal=%s — terminating workers", signum)
        for p in processes:
            if p.is_alive():
                p.terminate()

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    for p in processes:
        p.start()
        logger.info("spawned %s pid=%d", p.name, p.pid)

    for p in processes:
        p.join()
        logger.info("joined %s exitcode=%s", p.name, p.exitcode)

    failed = [p for p in processes if p.exitcode != 0]
    if failed:
        names = ", ".join(f"{p.name} (exit={p.exitcode})" for p in failed)
        logger.error("batch_failed workers_crashed: %s", names)
        raise SystemExit(1)

    logger.info("batch_complete all workers exited successfully")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m batch.runner",
        description="Batch KRS scanner — multiprocess runner. Connect VPN at OS level before running.",
    )
    parser.add_argument(
        "--start", type=int, default=None,
        help=f"KRS number to start from (default: {settings.batch_start_krs})",
    )
    parser.add_argument(
        "--workers", type=int, default=None,
        help=f"Number of parallel worker processes (default: {settings.batch_workers})",
    )
    parser.add_argument(
        "--concurrency", type=int, default=None,
        help=f"Async concurrency per worker (default: {settings.batch_concurrency_per_worker})",
    )
    parser.add_argument(
        "--delay", type=float, default=None,
        help=f"Delay in seconds between KRS requests (default: {settings.batch_delay_seconds})",
    )
    parser.add_argument(
        "--db", type=str, default=None,
        help=f"Progress DB path (default: {settings.batch_db_path})",
    )
    return parser


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [runner] %(levelname)s %(message)s",
    )
    parser = _build_parser()
    args = parser.parse_args()

    run_batch(
        start_krs=args.start,
        workers=args.workers,
        concurrency=args.concurrency,
        delay=args.delay,
        db_path=args.db,
    )


if __name__ == "__main__":
    main()
