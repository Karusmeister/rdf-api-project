"""Multiprocessing orchestrator for batch RDF document discovery.

Reads confirmed KRS numbers from batch_progress (status='found'),
spawns N worker processes that each handle a modulo-partition of
the KRS numbers, and fetches all available documents from the RDF API.

Usage:
    python -m batch.rdf_runner [options]

All flags are optional and fall back to settings from .env / app/config.py.
"""

import argparse
import logging
import multiprocessing
import signal
import sys

from app.config import settings
from batch.connections import Connection, build_pool
from batch.rdf_worker import run_rdf_worker

logger = logging.getLogger(__name__)


def _pick_connection(worker_id: int, use_vpn: bool) -> Connection:
    pool = build_pool()
    if not use_vpn:
        return pool[0]
    # Pool = [direct, vpn0, vpn1, ...]. Round-robin across all entries
    # so one worker uses the VM's direct IP and the rest use VPN proxies.
    if len(pool) < 2:
        raise RuntimeError(
            "VPN enabled but NORDVPN_SERVERS is empty. Set NORDVPN_SERVERS in .env."
        )
    return pool[worker_id % len(pool)]


def _validate_vpn_config() -> None:
    if not settings.nordvpn_username:
        raise RuntimeError("VPN enabled but NORDVPN_USERNAME is empty.")
    if not settings.nordvpn_password:
        raise RuntimeError("VPN enabled but NORDVPN_PASSWORD is empty.")
    if not settings.nordvpn_servers:
        raise RuntimeError("VPN enabled but NORDVPN_SERVERS is empty.")


def run_rdf_batch(
    *,
    workers: int | None = None,
    use_vpn: bool | None = None,
    concurrency: int | None = None,
    delay: float | None = None,
    page_size: int | None = None,
    dsn: str | None = None,
) -> None:
    """Spawn N worker processes for RDF document discovery + download."""
    _workers = workers if workers is not None else settings.batch_workers
    _vpn = use_vpn if use_vpn is not None else settings.batch_use_vpn
    _concurrency = concurrency if concurrency is not None else settings.rdf_batch_concurrency
    _delay = delay if delay is not None else settings.rdf_batch_delay_seconds
    _page_size = page_size if page_size is not None else settings.rdf_batch_page_size
    _db = dsn if dsn is not None else settings.database_url

    if _vpn:
        _validate_vpn_config()

    logger.info(
        "rdf_batch_start workers=%d vpn=%s concurrency=%d delay=%.1f "
        "page_size=%d db=%s storage_backend=%s",
        _workers, _vpn, _concurrency, _delay, _page_size, _db, settings.storage_backend,
    )

    processes: list[multiprocessing.Process] = []
    for worker_id in range(_workers):
        conn = _pick_connection(worker_id, _vpn)
        p = multiprocessing.Process(
            target=run_rdf_worker,
            name=f"rdf-worker-{worker_id}",
            kwargs=dict(
                worker_id=worker_id,
                total_workers=_workers,
                connection=conn,
                concurrency=_concurrency,
                delay=_delay,
                page_size=_page_size,
                dsn=_db,
            ),
        )
        processes.append(p)

    def _shutdown(signum, frame):
        logger.info("rdf_batch_shutdown signal=%s — terminating workers", signum)
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
        logger.error("rdf_batch_failed workers_crashed: %s", names)
        raise SystemExit(1)

    logger.info("rdf_batch_complete all workers exited successfully")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m batch.rdf_runner",
        description=(
            "Batch RDF document discovery — fetch documents for all known KRS numbers."
        ),
    )
    parser.add_argument(
        "--workers", type=int, default=None,
        help=f"Number of parallel worker processes (default: {settings.batch_workers})",
    )

    vpn_group = parser.add_mutually_exclusive_group()
    vpn_group.add_argument(
        "--vpn", action="store_true", default=None,
        help="Enable VPN connections",
    )
    vpn_group.add_argument(
        "--no-vpn", action="store_true", default=None,
        help="Disable VPN connections",
    )

    parser.add_argument(
        "--concurrency", type=int, default=None,
        help=f"Async concurrency per worker (default: {settings.rdf_batch_concurrency})",
    )
    parser.add_argument(
        "--delay", type=float, default=None,
        help=f"Delay between requests in seconds (default: {settings.rdf_batch_delay_seconds})",
    )
    parser.add_argument(
        "--page-size", type=int, default=None,
        help=f"Documents per page (default: {settings.rdf_batch_page_size})",
    )
    parser.add_argument(
        "--db", type=str, default=None,
        help=f"PostgreSQL DSN (default: DATABASE_URL from .env)",
    )
    return parser


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [rdf-runner] %(levelname)s %(message)s",
    )
    parser = _build_parser()
    args = parser.parse_args()

    use_vpn: bool | None = None
    if args.vpn:
        use_vpn = True
    elif args.no_vpn:
        use_vpn = False

    run_rdf_batch(
        workers=args.workers,
        use_vpn=use_vpn,
        concurrency=args.concurrency,
        delay=args.delay,
        page_size=args.page_size,
        dsn=args.db,
    )


if __name__ == "__main__":
    main()
