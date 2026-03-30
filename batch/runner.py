"""Multiprocessing orchestrator for the batch KRS scanner.

Spawns N worker processes, assigns stride-offset starting KRS numbers,
and routes each worker through either a direct or VPN connection.

Usage:
    python -m batch.runner [options]

All flags are optional and fall back to settings from .env / app/config.py.
"""

import argparse
import logging
import multiprocessing
import os
import signal
import sys

from app.config import settings
from batch.connections import Connection, build_pool
from batch.entity_store import EntityStore
from batch.progress import ProgressStore
from batch.worker import run_worker

logger = logging.getLogger(__name__)


def _pick_connection(worker_id: int, use_vpn: bool) -> Connection:
    """Assign a connection to a worker. Direct if VPN disabled; round-robin VPN otherwise."""
    pool = build_pool()
    if not use_vpn:
        return pool[0]  # direct
    vpn_conns = [c for c in pool if c.proxy_url is not None]
    if not vpn_conns:
        raise RuntimeError(
            "BATCH_USE_VPN=true but NORDVPN_SERVERS is empty. "
            "Set NORDVPN_SERVERS in .env."
        )
    return vpn_conns[worker_id % len(vpn_conns)]


def _validate_vpn_config() -> None:
    """Raise RuntimeError if VPN is enabled but credentials/servers are missing."""
    if not settings.nordvpn_username:
        raise RuntimeError(
            "BATCH_USE_VPN=true but NORDVPN_USERNAME is empty. "
            "Set NordVPN service credentials in .env."
        )
    if not settings.nordvpn_password:
        raise RuntimeError(
            "BATCH_USE_VPN=true but NORDVPN_PASSWORD is empty. "
            "Set NordVPN service credentials in .env."
        )
    if not settings.nordvpn_servers:
        raise RuntimeError(
            "BATCH_USE_VPN=true but NORDVPN_SERVERS is empty. "
            "Set NORDVPN_SERVERS in .env (JSON array of server hostnames)."
        )


def run_batch(
    *,
    start_krs: int | None = None,
    workers: int | None = None,
    use_vpn: bool | None = None,
    concurrency: int | None = None,
    delay: float | None = None,
    db_path: str | None = None,
) -> None:
    """Spawn N worker processes and block until all exit.

    All arguments are optional — defaults come from settings.
    """
    _start = start_krs if start_krs is not None else settings.batch_start_krs
    _workers = workers if workers is not None else settings.batch_workers
    _vpn = use_vpn if use_vpn is not None else settings.batch_use_vpn
    _concurrency = concurrency if concurrency is not None else settings.batch_concurrency_per_worker
    _delay = delay if delay is not None else settings.batch_delay_seconds
    _db = db_path if db_path is not None else settings.batch_db_path

    if _workers <= 0:
        raise ValueError("workers must be > 0")
    if _concurrency <= 0:
        raise ValueError("concurrency must be > 0")
    if _delay < 0:
        raise ValueError("delay must be >= 0")

    if _vpn:
        _validate_vpn_config()

    # Init DB schemas once in the parent process before spawning workers.
    # This avoids all workers racing to CREATE TABLE simultaneously.
    ProgressStore(_db)
    EntityStore(_db)

    logger.info(
        "batch_start workers=%d start_krs=%d vpn=%s concurrency=%d delay=%.1f db=%s",
        _workers, _start, _vpn, _concurrency, _delay, _db,
    )

    processes: list[multiprocessing.Process] = []
    for worker_id in range(_workers):
        conn = _pick_connection(worker_id, _vpn)
        p = multiprocessing.Process(
            target=run_worker,
            name=f"krs-worker-{worker_id}",
            kwargs=dict(
                worker_id=worker_id,
                start_krs=_start + worker_id,  # stride offset
                stride=_workers,
                connection=conn,
                concurrency=_concurrency,
                delay=_delay,
                db_path=_db,
            ),
        )
        processes.append(p)

    # Graceful shutdown on SIGINT/SIGTERM
    _shutdown_in_progress = False

    def _shutdown(signum, frame):
        nonlocal _shutdown_in_progress
        if _shutdown_in_progress:
            return
        _shutdown_in_progress = True
        logger.info("batch_shutdown signal=%s — terminating workers", signum)
        for p in processes:
            try:
                if p.is_alive():
                    p.terminate()
            except ProcessLookupError:
                pass

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    for p in processes:
        p.start()
        logger.info("spawned %s pid=%d", p.name, p.pid)

    for p in processes:
        p.join()  # block until worker exits naturally or via signal
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
        description="Batch KRS scanner — multiprocess runner with optional VPN rotation.",
    )
    parser.add_argument(
        "--start", type=int, default=None,
        help=f"KRS number to start from (default: {settings.batch_start_krs})",
    )
    parser.add_argument(
        "--workers", type=int, default=None,
        help=f"Number of parallel worker processes (default: {settings.batch_workers})",
    )

    vpn_group = parser.add_mutually_exclusive_group()
    vpn_group.add_argument(
        "--vpn", action="store_true", default=None,
        help="Enable VPN connections (overrides BATCH_USE_VPN=false)",
    )
    vpn_group.add_argument(
        "--no-vpn", action="store_true", default=None,
        help="Disable VPN connections (overrides BATCH_USE_VPN=true)",
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
    # macOS defaults to 'spawn' which doesn't survive nohup well.
    # 'fork' keeps children alive when parent is backgrounded.
    if os.name != "nt":
        multiprocessing.set_start_method("fork", force=True)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [runner] %(levelname)s %(message)s",
    )
    parser = _build_parser()
    args = parser.parse_args()

    use_vpn: bool | None = None
    if args.vpn:
        use_vpn = True
    elif args.no_vpn:
        use_vpn = False

    run_batch(
        start_krs=args.start,
        workers=args.workers,
        use_vpn=use_vpn,
        concurrency=args.concurrency,
        delay=args.delay,
        db_path=args.db,
    )


if __name__ == "__main__":
    main()
