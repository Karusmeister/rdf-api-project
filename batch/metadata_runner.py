"""Multiprocessing orchestrator for metadata backfill.

Spawns N workers to backfill metadata for downloaded documents
that were fetched with --skip-metadata.

Usage:
    python -m batch.metadata_runner [options]
"""

import argparse
import logging
import multiprocessing
import signal

from app.config import settings
from batch.connections import Connection, build_pool, validate_vpn_config
from batch.metadata_backfill import run_metadata_backfill

logger = logging.getLogger(__name__)


def _pick_connection(worker_id: int, use_vpn: bool) -> Connection:
    pool = build_pool()
    if not use_vpn:
        return pool[0]
    if len(pool) < 2:
        raise RuntimeError(
            "VPN enabled but connection pool has no VPN entries. "
            "Set NORDVPN_SERVERS in .env."
        )
    return pool[worker_id % len(pool)]


def run_metadata_batch(
    *, workers: int = 3,
    use_vpn: bool = False,
    concurrency: int = 10,
    delay: float = 0.2,
    dsn: str | None = None,
) -> None:
    _db = dsn or settings.database_url

    if use_vpn:
        validate_vpn_config()

    pool = build_pool()
    vpn_count = len(pool) - 1  # pool[0] is always direct

    logger.info(
        "metadata_batch_start workers=%d vpn=%s vpn_connections=%d "
        "concurrency=%d delay=%.1f",
        workers, use_vpn, vpn_count if use_vpn else 0, concurrency, delay,
    )

    processes = []
    for wid in range(workers):
        conn = _pick_connection(wid, use_vpn)
        p = multiprocessing.Process(
            target=run_metadata_backfill,
            name=f"meta-backfill-{wid}",
            kwargs=dict(
                worker_id=wid,
                total_workers=workers,
                connection=conn,
                concurrency=concurrency,
                delay=delay,
                dsn=_db,
            ),
        )
        processes.append(p)

    def _shutdown(signum, frame):
        for p in processes:
            if p.is_alive():
                p.terminate()

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    for p in processes:
        p.start()

    for p in processes:
        p.join()
        logger.info("joined %s exitcode=%s", p.name, p.exitcode)

    failed = [p for p in processes if p.exitcode != 0]
    if failed:
        names = ", ".join(f"{p.name} (exit={p.exitcode})" for p in failed)
        logger.error("metadata_batch_failed workers_crashed: %s", names)
        raise SystemExit(1)

    logger.info("metadata_batch_complete all workers exited successfully")


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [meta-runner] %(levelname)s %(message)s",
    )
    parser = argparse.ArgumentParser(
        prog="python -m batch.metadata_runner",
        description="Backfill metadata for downloaded documents missing it.",
    )
    parser.add_argument("--workers", type=int, default=3)
    parser.add_argument("--concurrency", type=int, default=10)
    parser.add_argument("--delay", type=float, default=0.2)
    parser.add_argument("--no-vpn", action="store_true")
    parser.add_argument("--db", type=str, default=None)
    args = parser.parse_args()

    run_metadata_batch(
        workers=args.workers,
        use_vpn=not args.no_vpn,
        concurrency=args.concurrency,
        delay=args.delay,
        dsn=args.db,
    )


if __name__ == "__main__":
    main()
