"""Standalone metadata backfill for downloaded documents missing metadata.

Finds documents where is_downloaded=true AND metadata_fetched_at IS NULL,
fetches metadata from the RDF API, and updates the document store.

Uses keyset pagination to fetch work in bounded batches, keeping memory
at O(concurrency + batch_size) regardless of total backlog depth.

Can run concurrently with the download workers without interference --
it only touches documents that are already fully downloaded.
"""

import asyncio
import logging
import time
from dataclasses import dataclass, field

import httpx

from app.config import settings
from app.db.connection import make_connection
from batch.connections import Connection, DeadProxyRegistry, ProxyRotator
from batch.rdf_document_store import RdfDocumentStore
from batch.rdf_worker import (
    _fetch_metadata_with_backoff,
    _make_client,
    _RDF_BASE,
)

logger = logging.getLogger(__name__)


@dataclass
class BackfillStats:
    total: int = 0
    success: int = 0
    failed: int = 0
    skipped: int = 0
    start_time: float = field(default_factory=time.monotonic)

    def log(self, worker_id: int) -> None:
        elapsed = time.monotonic() - self.start_time
        rate = self.total / elapsed if elapsed > 0 else 0
        logger.info(
            "metadata_backfill=%d total=%d success=%d failed=%d "
            "skipped=%d elapsed=%.0fs rate=%.2f/s",
            worker_id, self.total, self.success, self.failed,
            self.skipped, elapsed, rate,
        )


def _get_needs_metadata_batch(
    dsn: str,
    worker_id: int,
    total_workers: int,
    batch_size: int,
    after_krs: str | None = None,
    after_doc_id: str | None = None,
) -> list[tuple[str, str]]:
    """Return up to batch_size (document_id, krs) pairs using keyset pagination.

    Uses hashtext() for even distribution across workers regardless of KRS
    number density (avoids hot-spots from sequential KRS ranges).

    Keyset pagination: each call returns the next page strictly after
    (after_krs, after_doc_id), ordered by (krs, document_id).
    """
    conn = make_connection(dsn)
    try:
        if after_krs is not None and after_doc_id is not None:
            rows = conn.execute("""
                SELECT document_id, krs
                FROM krs_document_versions
                WHERE is_current = true
                  AND is_downloaded = true
                  AND metadata_fetched_at IS NULL
                  AND abs(hashtext(krs)) %% %s = %s
                  AND (krs, document_id) > (%s, %s)
                ORDER BY krs, document_id
                LIMIT %s
            """, [total_workers, worker_id, after_krs, after_doc_id, batch_size]).fetchall()
        else:
            rows = conn.execute("""
                SELECT document_id, krs
                FROM krs_document_versions
                WHERE is_current = true
                  AND is_downloaded = true
                  AND metadata_fetched_at IS NULL
                  AND abs(hashtext(krs)) %% %s = %s
                ORDER BY krs, document_id
                LIMIT %s
            """, [total_workers, worker_id, batch_size]).fetchall()
        return [(row[0], row[1]) for row in rows]
    finally:
        conn.close()


async def _backfill_loop(
    worker_id: int,
    total_workers: int,
    connection: Connection,
    concurrency: int,
    delay: float,
    dsn: str,
    batch_size: int | None = None,
    proxy_pool: list[Connection] | None = None,
) -> None:
    """Fetch metadata for all downloaded docs missing it.

    Processes work in keyset-paginated batches of ``batch_size``.
    Memory stays bounded at O(concurrency + batch_size).
    """
    _batch_size = batch_size if batch_size is not None else settings.metadata_backfill_fetch_batch_size
    doc_store = RdfDocumentStore(dsn, init_schema=False)
    try:
        await _backfill_loop_inner(
            worker_id, total_workers, connection, concurrency, delay,
            dsn, _batch_size, doc_store, proxy_pool,
        )
    finally:
        doc_store.close()


async def _backfill_loop_inner(
    worker_id: int,
    total_workers: int,
    connection: Connection,
    concurrency: int,
    delay: float,
    dsn: str,
    _batch_size: int,
    doc_store: "RdfDocumentStore",
    proxy_pool: list[Connection] | None = None,
) -> None:
    """Inner loop, separated so caller can wrap with try/finally for cleanup."""
    stats = BackfillStats()
    sem = asyncio.Semaphore(concurrency)
    unexpected_errors = 0
    consecutive_failures = 0
    _MAX_CONSECUTIVE_FAILURES = 3

    # Set up proxy rotation if a pool was provided
    rotator: ProxyRotator | None = None
    if proxy_pool and len(proxy_pool) > 1:
        registry = DeadProxyRegistry(dsn)
        rotator = ProxyRotator(
            proxy_pool, start_index=worker_id,
            registry=registry, worker_id=worker_id,
        )
        connection = rotator.current

    # Keyset cursor — start from the beginning
    after_krs: str | None = None
    after_doc_id: str | None = None
    batch_num = 0

    while True:
        if rotator and rotator.exhausted:
            logger.error("metadata_backfill=%d all proxies exhausted", worker_id)
            break

        needs_reconnect = False

        async with _make_client(connection) as client:

            while True:
                # Fetch next batch via keyset pagination
                batch = _get_needs_metadata_batch(
                    dsn, worker_id, total_workers, _batch_size,
                    after_krs=after_krs, after_doc_id=after_doc_id,
                )

                if not batch:
                    break

                batch_num += 1
                logger.info(
                    "metadata_backfill=%d batch=%d size=%d cursor=(%s, %s)",
                    worker_id, batch_num, len(batch),
                    after_krs or "START", (after_doc_id or "START")[:20],
                )

                async def _do_one(doc_id: str, krs: str) -> None:
                    nonlocal consecutive_failures, needs_reconnect
                    async with sem:
                        meta = await _fetch_metadata_with_backoff(
                            client, doc_id, worker_id,
                        )
                        await asyncio.sleep(delay)

                    stats.total += 1

                    if meta is None:
                        stats.failed += 1
                        consecutive_failures += 1
                        if rotator and consecutive_failures >= _MAX_CONSECUTIVE_FAILURES:
                            new_conn = rotator.record_failure()
                            if new_conn is not None:
                                needs_reconnect = True
                        return

                    consecutive_failures = 0
                    if rotator:
                        rotator.record_success()

                    try:
                        doc_store.update_metadata(doc_id, meta)
                        stats.success += 1
                    except Exception as exc:
                        logger.warning(
                            "metadata_backfill=%d doc=%s error=%s",
                            worker_id, doc_id[:20], exc,
                        )
                        stats.failed += 1

                    if stats.total % 100 == 0:
                        stats.log(worker_id)

                results = await asyncio.gather(
                    *(_do_one(doc_id, krs) for doc_id, krs in batch),
                    return_exceptions=True,
                )

                # Surface any unexpected exceptions
                for i, result in enumerate(results):
                    if isinstance(result, BaseException):
                        doc_id, krs = batch[i]
                        logger.error(
                            "metadata_backfill=%d doc=%s unexpected_error %s: %s",
                            worker_id, doc_id[:20], type(result).__name__, result,
                        )
                        unexpected_errors += 1
                        stats.failed += 1

                # Advance keyset cursor to last row in this batch
                last_doc_id, last_krs = batch[-1]
                after_krs = last_krs
                after_doc_id = last_doc_id

                # If batch was smaller than limit, there are no more rows
                if len(batch) < _batch_size:
                    break

                if needs_reconnect:
                    break

        # Switch to new proxy if rotated
        if needs_reconnect and rotator and not rotator.exhausted:
            connection = rotator.current
            consecutive_failures = 0
            logger.info(
                "metadata_backfill=%d rotated to proxy=%s",
                worker_id, connection.name,
            )
            continue

        # Normal exit — no more batches or all work done
        break

    stats.log(worker_id)

    if unexpected_errors:
        raise RuntimeError(
            f"metadata_backfill worker {worker_id}: {unexpected_errors} unexpected errors"
        )

    logger.info("metadata_backfill=%d finished batches=%d", worker_id, batch_num)


def run_metadata_backfill(
    worker_id: int,
    total_workers: int,
    connection: Connection,
    concurrency: int,
    delay: float,
    dsn: str,
    proxy_pool: list[Connection] | None = None,
) -> None:
    """Entrypoint for multiprocessing.Process."""
    logging.basicConfig(
        level=logging.INFO,
        format=f"%(asctime)s [meta-backfill-{worker_id}] %(levelname)s %(message)s",
    )
    asyncio.run(
        _backfill_loop(
            worker_id=worker_id,
            total_workers=total_workers,
            connection=connection,
            concurrency=concurrency,
            delay=delay,
            dsn=dsn,
            proxy_pool=proxy_pool,
        )
    )
