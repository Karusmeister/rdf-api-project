"""Async worker loop for the batch RDF document discovery and download.

Each worker process fetches its partition of KRS numbers (modulo worker_id)
from batch_progress where status='found', then for each KRS:
  1. Discovers all available documents via the encrypted search API
  2. Downloads each document (metadata + ZIP), extracts to disk

Shares the same connection health / backoff patterns as the KRS scanner
but operates on a completely separate progress table (batch_rdf_progress).
"""

import asyncio
import logging
import time
import urllib.parse
from dataclasses import dataclass, field

import httpx

from app.config import settings
from app.crypto import encrypt_nrkrs
from app.scraper.storage import StorageBackend, create_storage, make_doc_dir
from batch.connections import Connection, DeadProxyRegistry, ProxyRotator
from batch.rdf_document_store import RdfDocumentStore
from batch.rdf_progress import RdfProgressStore

logger = logging.getLogger(__name__)

_RDF_BASE = settings.rdf_base_url
_RDF_HEADERS = {
    "Accept": "application/json",
    "Content-Type": "application/json",
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    ),
    "Referer": settings.rdf_referer,
    "Origin": settings.rdf_origin,
}

# Backoff constants
_MAX_RETRIES_RATE_LIMIT = 4
_MAX_RETRIES_NETWORK = 2
_BACKOFF_CAP_SECONDS = 60.0
_NETWORK_RETRY_DELAY = 5.0

# Connection health constants
_CONSECUTIVE_FAILURES_THRESHOLD = 3
_EXTRA_DELAY_INCREMENT = 1.0
_EXTRA_DELAY_CAP = 5.0
_COOLDOWN_SECONDS = 60.0


@dataclass
class RdfWorkerStats:
    krs_processed: int = 0
    krs_with_docs: int = 0
    krs_empty: int = 0
    krs_errors: int = 0
    documents_found: int = 0
    documents_downloaded: int = 0
    documents_failed: int = 0
    bytes_downloaded: int = 0
    start_time: float = field(default_factory=time.monotonic)

    def log(self, worker_id: int) -> None:
        elapsed = time.monotonic() - self.start_time
        rate = self.krs_processed / elapsed if elapsed > 0 else 0
        logger.info(
            "rdf_worker=%d krs_processed=%d with_docs=%d empty=%d errors=%d "
            "total_docs=%d downloaded=%d failed=%d bytes=%d elapsed=%.0fs rate=%.2f/s",
            worker_id, self.krs_processed, self.krs_with_docs, self.krs_empty,
            self.krs_errors, self.documents_found, self.documents_downloaded,
            self.documents_failed, self.bytes_downloaded, elapsed, rate,
        )


@dataclass
class ConnectionHealth:
    consecutive_failures: int = 0
    extra_delay: float = 0.0

    def record_success(self):
        self.consecutive_failures = 0
        self.extra_delay = 0.0

    def record_failure(self) -> float | None:
        self.consecutive_failures += 1
        if self.consecutive_failures % _CONSECUTIVE_FAILURES_THRESHOLD == 0:
            self.extra_delay = min(
                self.extra_delay + _EXTRA_DELAY_INCREMENT,
                _EXTRA_DELAY_CAP,
            )
        if (self.extra_delay >= _EXTRA_DELAY_CAP
                and self.consecutive_failures >= _CONSECUTIVE_FAILURES_THRESHOLD * 2):
            self.consecutive_failures = 0
            self.extra_delay = 0.0
            return _COOLDOWN_SECONDS
        return None


class AdaptiveSemaphore:
    """Semaphore whose effective capacity can be reduced at runtime.

    When ``reduce()`` is called, the next release is silently absorbed
    instead of making the slot available again, permanently shrinking
    the pool by one.
    """

    def __init__(self, value: int, min_value: int = 2):
        self._sem = asyncio.Semaphore(value)
        self._effective = value
        self._min = min_value
        self._pending_reductions = 0

    @property
    def capacity(self) -> int:
        return self._effective

    def reduce(self) -> int:
        """Reduce effective capacity by 1. Returns new capacity."""
        if self._effective <= self._min:
            return self._effective
        self._effective -= 1
        self._pending_reductions += 1
        return self._effective

    async def __aenter__(self):
        await self._sem.acquire()
        return self

    async def __aexit__(self, *exc):
        if self._pending_reductions > 0:
            self._pending_reductions -= 1
            # Slot permanently consumed — don't release.
        else:
            self._sem.release()


@dataclass
class RateLimitTracker:
    """Counts 429 responses per rolling window and signals when to back off."""

    threshold: int = 5
    window_secs: float = 60.0
    _count: int = field(default=0, init=False, repr=False)
    _window_start: float = field(default_factory=time.monotonic, init=False, repr=False)

    def record(self) -> bool:
        """Record a 429. Returns True exactly once per burst of *threshold* hits."""
        now = time.monotonic()
        if now - self._window_start > self.window_secs:
            self._count = 0
            self._window_start = now
        self._count += 1
        if self._count >= self.threshold:
            self._count = 0
            self._window_start = now
            return True
        return False


def _make_client(connection: Connection) -> httpx.AsyncClient:
    kwargs: dict = {
        "base_url": _RDF_BASE,
        "headers": _RDF_HEADERS,
        "timeout": settings.request_timeout,
        "follow_redirects": True,
    }
    if connection.proxy_url is not None:
        kwargs["proxy"] = connection.proxy_url
    return httpx.AsyncClient(**kwargs)


# ---------------------------------------------------------------------------
# Discovery: fetch document listings
# ---------------------------------------------------------------------------

async def _fetch_documents_page(
    client: httpx.AsyncClient,
    krs: str,
    page: int,
    page_size: int,
    worker_id: int,
    on_429=None,
) -> tuple[str, list[dict], int]:
    """Fetch one page of documents for a KRS number.

    Returns (status, documents, total_pages) where status is
    "ok" / "empty" / "error".
    """
    for attempt in range(_MAX_RETRIES_RATE_LIMIT + 1):
        try:
            encrypted = encrypt_nrkrs(krs.lstrip("0") or "0")
            payload = {
                "metadaneStronicowania": {
                    "numerStrony": page,
                    "rozmiarStrony": page_size,
                    "metadaneSortowania": [
                        {"atrybut": "id", "kierunek": "MALEJACO"}
                    ],
                },
                "nrKRS": encrypted,
            }
            resp = await client.post("/dokumenty/wyszukiwanie", json=payload)

            if resp.status_code in (429, 503):
                if resp.status_code == 429 and on_429:
                    on_429()
                if attempt < _MAX_RETRIES_RATE_LIMIT:
                    delay = min(1.0 * (2 ** attempt), _BACKOFF_CAP_SECONDS)
                    logger.warning(
                        "rdf_worker=%d krs=%s page=%d retry=%d after=%.0fs reason=%d",
                        worker_id, krs, page, attempt + 1, delay, resp.status_code,
                    )
                    await asyncio.sleep(delay)
                    continue
                return "error", [], 0

            resp.raise_for_status()
            data = resp.json()

            content = data.get("content", [])
            meta = data.get("metadaneWynikow", {})
            total_pages = meta.get("liczbaStron", 1)

            if not content:
                return "empty", [], total_pages

            return "ok", content, total_pages

        except httpx.HTTPStatusError as exc:
            status_code = exc.response.status_code
            if status_code in (429, 503) and attempt < _MAX_RETRIES_RATE_LIMIT:
                if status_code == 429 and on_429:
                    on_429()
                delay = min(1.0 * (2 ** attempt), _BACKOFF_CAP_SECONDS)
                await asyncio.sleep(delay)
                continue
            logger.error(
                "rdf_worker=%d krs=%s page=%d http_error status=%d",
                worker_id, krs, page, status_code,
            )
            return "error", [], 0

        except (httpx.TimeoutException, httpx.ConnectError, OSError) as exc:
            if attempt < _MAX_RETRIES_NETWORK:
                logger.warning(
                    "rdf_worker=%d krs=%s page=%d retry=%d after=%.0fs reason=%s",
                    worker_id, krs, page, attempt + 1, _NETWORK_RETRY_DELAY,
                    type(exc).__name__,
                )
                await asyncio.sleep(_NETWORK_RETRY_DELAY)
                continue
            logger.error(
                "rdf_worker=%d krs=%s page=%d network_error type=%s",
                worker_id, krs, page, type(exc).__name__,
            )
            return "error", [], 0

    return "error", [], 0


async def _fetch_all_documents(
    client: httpx.AsyncClient,
    krs: str,
    page_size: int,
    delay: float,
    worker_id: int,
    on_429=None,
) -> tuple[str, list[dict]]:
    """Fetch ALL pages of documents for a KRS number.

    Returns (status, all_documents) where status is "ok" / "empty" / "error".
    """
    all_docs: list[dict] = []

    status, docs, total_pages = await _fetch_documents_page(
        client, krs, page=0, page_size=page_size, worker_id=worker_id,
        on_429=on_429,
    )

    if status == "error":
        return "error", []
    if status == "empty" or not docs:
        return "empty", []

    all_docs.extend(docs)

    for page in range(1, total_pages):
        await asyncio.sleep(delay)
        status, docs, _ = await _fetch_documents_page(
            client, krs, page=page, page_size=page_size, worker_id=worker_id,
            on_429=on_429,
        )
        if status == "error":
            logger.warning(
                "rdf_worker=%d krs=%s partial_fetch page=%d/%d docs_so_far=%d",
                worker_id, krs, page, total_pages, len(all_docs),
            )
            return "error", all_docs
        if docs:
            all_docs.extend(docs)

    return "ok", all_docs


# ---------------------------------------------------------------------------
# Download: metadata + ZIP + extract
# ---------------------------------------------------------------------------

async def _fetch_metadata_with_backoff(
    client: httpx.AsyncClient,
    doc_id: str,
    worker_id: int,
    on_429=None,
) -> dict | None:
    """Fetch document metadata. Returns metadata dict or None on failure."""
    encoded_id = urllib.parse.quote(doc_id, safe="")
    for attempt in range(_MAX_RETRIES_RATE_LIMIT + 1):
        try:
            resp = await client.get(f"/dokumenty/{encoded_id}")

            if resp.status_code in (429, 503):
                if resp.status_code == 429 and on_429:
                    on_429()
                if attempt < _MAX_RETRIES_RATE_LIMIT:
                    delay = min(1.0 * (2 ** attempt), _BACKOFF_CAP_SECONDS)
                    await asyncio.sleep(delay)
                    continue
                return None

            resp.raise_for_status()
            return resp.json()

        except httpx.HTTPStatusError:
            return None
        except (httpx.TimeoutException, httpx.ConnectError, OSError):
            if attempt < _MAX_RETRIES_NETWORK:
                await asyncio.sleep(_NETWORK_RETRY_DELAY)
                continue
            return None

    return None


async def _download_zip_with_backoff(
    client: httpx.AsyncClient,
    doc_id: str,
    worker_id: int,
    on_429=None,
) -> bytes | None:
    """Download document ZIP. Returns raw bytes or None on failure."""
    for attempt in range(_MAX_RETRIES_RATE_LIMIT + 1):
        try:
            resp = await client.post(
                "/dokumenty/tresc",
                json=[doc_id],
                headers={"Accept": "application/octet-stream"},
                timeout=settings.scraper_download_timeout,
            )

            if resp.status_code in (429, 503):
                if resp.status_code == 429 and on_429:
                    on_429()
                if attempt < _MAX_RETRIES_RATE_LIMIT:
                    delay = min(1.0 * (2 ** attempt), _BACKOFF_CAP_SECONDS)
                    await asyncio.sleep(delay)
                    continue
                return None

            resp.raise_for_status()
            return resp.content

        except httpx.HTTPStatusError:
            return None
        except (httpx.TimeoutException, httpx.ConnectError, OSError):
            if attempt < _MAX_RETRIES_NETWORK:
                await asyncio.sleep(_NETWORK_RETRY_DELAY)
                continue
            return None

    return None


async def _download_one_document(
    client: httpx.AsyncClient,
    krs: str,
    doc_id: str,
    doc_store: RdfDocumentStore,
    storage: StorageBackend,
    delay: float,
    worker_id: int,
    stats: RdfWorkerStats,
    skip_metadata: bool = False,
    on_429=None,
) -> bool:
    """Download a single document: metadata → ZIP → extract → mark in DB.

    Returns True on success, False on failure.
    When skip_metadata=True, the metadata fetch is skipped entirely
    (can be backfilled later with batch.metadata_backfill).
    """
    # 1. Fetch metadata (skip when flag is set)
    if not skip_metadata:
        meta = await _fetch_metadata_with_backoff(client, doc_id, worker_id, on_429=on_429)
        await asyncio.sleep(delay)

        if meta is not None:
            try:
                doc_store.update_metadata(doc_id, meta)
            except Exception as exc:
                logger.warning(
                    "rdf_worker=%d krs=%s doc=%s metadata_store_error %s",
                    worker_id, krs, doc_id[:20], type(exc).__name__,
                )

    # 2. Download ZIP
    zip_bytes = await _download_zip_with_backoff(client, doc_id, worker_id, on_429=on_429)
    await asyncio.sleep(delay)

    if zip_bytes is None:
        doc_store.update_error(doc_id, "download_failed")
        stats.documents_failed += 1
        return False

    # 3. Extract to disk (prefer async to avoid blocking the event loop)
    try:
        doc_dir = make_doc_dir(krs, doc_id)
        if hasattr(storage, 'async_save_extracted'):
            manifest = await storage.async_save_extracted(doc_dir, zip_bytes, doc_id)
        else:
            manifest = storage.save_extracted(doc_dir, zip_bytes, doc_id)

        total_extracted = sum(f["size"] for f in manifest["files"])
        file_types = ",".join(sorted({f["type"] for f in manifest["files"]}))

        doc_store.mark_downloaded(
            document_id=doc_id,
            storage_path=doc_dir,
            storage_backend=settings.storage_backend,
            file_size=total_extracted,
            zip_size=len(zip_bytes),
            file_count=len(manifest["files"]),
            file_types=file_types,
        )

        stats.documents_downloaded += 1
        stats.bytes_downloaded += total_extracted
        return True

    except Exception as exc:
        logger.warning(
            "rdf_worker=%d krs=%s doc=%s extract_error %s: %s",
            worker_id, krs, doc_id[:20], type(exc).__name__, exc,
        )
        doc_store.update_error(doc_id, str(exc))
        stats.documents_failed += 1
        return False


# ---------------------------------------------------------------------------
# Main worker loop
# ---------------------------------------------------------------------------

async def _worker_loop(
    worker_id: int,
    total_workers: int,
    connection: Connection,
    concurrency: int,
    delay: float,
    download_delay: float,
    page_size: int,
    dsn: str,
    skip_metadata: bool = False,
    proxy_pool: list[Connection] | None = None,
    legal_forms: list[str] | None = None,
) -> None:
    """Main async loop for a single RDF document discovery + download worker."""
    progress = RdfProgressStore(dsn)
    doc_store = RdfDocumentStore(dsn)
    try:
        await _rdf_worker_loop_inner(
            worker_id, total_workers, connection, concurrency, delay,
            download_delay, page_size, dsn, progress, doc_store,
            skip_metadata, proxy_pool, legal_forms,
        )
    finally:
        progress.close()
        doc_store.close()


async def _rdf_worker_loop_inner(
    worker_id: int,
    total_workers: int,
    connection: Connection,
    concurrency: int,
    delay: float,
    download_delay: float,
    page_size: int,
    dsn: str,
    progress: "RdfProgressStore",
    doc_store: "RdfDocumentStore",
    skip_metadata: bool = False,
    proxy_pool: list[Connection] | None = None,
    legal_forms: list[str] | None = None,
) -> None:
    """Inner loop, separated so caller can wrap with try/finally for cleanup."""
    storage = create_storage()
    stats = RdfWorkerStats()
    health = ConnectionHealth()
    adaptive_sem = AdaptiveSemaphore(concurrency)
    rate_tracker = RateLimitTracker()

    # Set up proxy rotation if a pool was provided
    rotator: ProxyRotator | None = None
    if proxy_pool and len(proxy_pool) > 1:
        registry = DeadProxyRegistry(dsn)
        rotator = ProxyRotator(
            proxy_pool, start_index=worker_id,
            registry=registry, worker_id=worker_id,
        )
        connection = rotator.current

    def _on_429():
        if rate_tracker.record():
            old_cap = adaptive_sem.capacity
            new_cap = adaptive_sem.reduce()
            if new_cap < old_cap:
                logger.warning(
                    "rdf_worker=%d adaptive_concurrency_reduced to=%d",
                    worker_id, new_cap,
                )

    # Pre-fetch our partition: new KRS numbers + already-discovered but undownloaded
    new_krs = progress.get_pending_krs(worker_id, total_workers, legal_forms=legal_forms)
    needs_download = progress.get_needs_download_krs(worker_id, total_workers)
    # Deduplicate, new first (they need discovery + download), then download-only
    seen = set(new_krs)
    pending_krs = list(new_krs)
    for krs in needs_download:
        if krs not in seen:
            pending_krs.append(krs)
            seen.add(krs)

    logger.info(
        "rdf_worker=%d pending_krs=%d (new=%d, needs_download=%d) total_workers=%d",
        worker_id, len(pending_krs), len(new_krs), len(needs_download), total_workers,
    )

    if not pending_krs:
        logger.info("rdf_worker=%d no_pending_krs — exiting", worker_id)
        return

    # Outer loop: reconnects with a new proxy when the rotator signals rotation
    idx = 0
    retry_after_rotation: list[str] = []
    while idx < len(pending_krs) or retry_after_rotation:
        if rotator and rotator.exhausted:
            logger.error("rdf_worker=%d all proxies exhausted — aborting", worker_id)
            break

        logger.info(
            "rdf_worker=%d connecting via %s (remaining_proxies=%d)",
            worker_id, connection.name,
            rotator.remaining if rotator else 1,
        )

        needs_reconnect = False

        async with _make_client(connection) as client:

            async def _handle_one(krs: str) -> None:
                nonlocal needs_reconnect
                already_discovered = progress.is_done(krs)

                # Phase 1: Discovery (skip if already done)
                doc_count = 0
                if not already_discovered:
                    all_docs: list[dict] = []
                    result = "error"
                    try:
                        async with adaptive_sem:
                            result, all_docs = await _fetch_all_documents(
                                client, krs, page_size, delay, worker_id,
                                on_429=_on_429,
                            )
                            effective_delay = delay + health.extra_delay
                            await asyncio.sleep(effective_delay)
                    except Exception as exc:
                        logger.error(
                            "rdf_worker=%d krs=%s unexpected_error %s: %s",
                            worker_id, krs, type(exc).__name__, exc,
                        )
                        result = "error"

                    # Track connection health + proxy rotation
                    if result == "error" and not all_docs:
                        cooldown = health.record_failure()
                        if rotator:
                            new_conn = rotator.record_failure()
                            if new_conn is not None:
                                # Don't persist — retry this item on the new proxy
                                retry_after_rotation.append(krs)
                                needs_reconnect = True
                                return
                        if cooldown:
                            logger.warning(
                                "rdf_worker=%d connection=%s cooldown=%.0fs",
                                worker_id, connection.name, cooldown,
                            )
                            await asyncio.sleep(cooldown)
                    else:
                        health.record_success()
                        if rotator:
                            rotator.record_success()

                    # Store discovered documents
                    if all_docs:
                        try:
                            doc_count = doc_store.insert_documents(krs, all_docs)
                        except Exception as exc:
                            logger.warning(
                                "rdf_worker=%d krs=%s doc_store_error %s: %s",
                                worker_id, krs, type(exc).__name__, exc,
                            )

                    stats.documents_found += doc_count

                    # Update discovery progress
                    if result == "ok":
                        final_status = "done" if doc_count > 0 else "empty"
                    elif all_docs:
                        final_status = "partial"
                    else:
                        final_status = "error"

                    progress.mark(krs, final_status, doc_count, worker_id)

                    if final_status == "done":
                        stats.krs_with_docs += 1
                    elif final_status == "empty":
                        stats.krs_empty += 1
                    else:
                        stats.krs_errors += 1

                # Phase 2: Download undownloaded documents (parallel within KRS)
                try:
                    undownloaded = doc_store.get_undownloaded(krs)
                    if undownloaded:
                        async def _dl_one(doc_id: str) -> None:
                            async with adaptive_sem:
                                await _download_one_document(
                                    client, krs, doc_id, doc_store, storage,
                                    download_delay, worker_id, stats,
                                    skip_metadata=skip_metadata,
                                    on_429=_on_429,
                                )

                        await asyncio.gather(
                            *(_dl_one(did) for did in undownloaded),
                            return_exceptions=True,
                        )
                except Exception as exc:
                    logger.warning(
                        "rdf_worker=%d krs=%s download_phase_error %s: %s",
                        worker_id, krs, type(exc).__name__, exc,
                    )

                stats.krs_processed += 1

                if stats.krs_processed % 50 == 0:
                    stats.log(worker_id)

            # Process KRS numbers with bounded concurrency
            pending: set[asyncio.Task] = set()

            # Retry items from previous rotation first
            for retry_krs in retry_after_rotation:
                task = asyncio.create_task(_handle_one(retry_krs))
                pending.add(task)
                task.add_done_callback(pending.discard)
            retry_after_rotation.clear()

            while idx < len(pending_krs) or pending:
                while (len(pending) < adaptive_sem.capacity
                       and idx < len(pending_krs)
                       and not needs_reconnect):
                    krs = pending_krs[idx]
                    idx += 1
                    task = asyncio.create_task(_handle_one(krs))
                    pending.add(task)
                    task.add_done_callback(pending.discard)

                if pending:
                    done, _ = await asyncio.wait(
                        pending, return_when=asyncio.FIRST_COMPLETED,
                    )
                    for t in done:
                        if t.exception() is not None:
                            logger.error(
                                "rdf_worker=%d task_error %s", worker_id, t.exception(),
                            )

                if needs_reconnect:
                    # Drain remaining tasks before switching proxy
                    if pending:
                        done, _ = await asyncio.wait(pending)
                    break

        # After client context exits — switch to new proxy if rotated
        if needs_reconnect and rotator and not rotator.exhausted:
            connection = rotator.current
            health = ConnectionHealth()
            logger.info(
                "rdf_worker=%d rotated to proxy=%s",
                worker_id, connection.name,
            )

    stats.log(worker_id)
    logger.info("rdf_worker=%d finished", worker_id)


def run_rdf_worker(
    worker_id: int,
    total_workers: int,
    connection: Connection,
    concurrency: int,
    delay: float,
    download_delay: float,
    page_size: int,
    dsn: str,
    skip_metadata: bool = False,
    proxy_pool: list[Connection] | None = None,
    legal_forms: list[str] | None = None,
) -> None:
    """Entrypoint for multiprocessing.Process."""
    logging.basicConfig(
        level=logging.INFO,
        format=f"%(asctime)s [rdf-worker-{worker_id}] %(levelname)s %(message)s",
    )
    logger.info(
        "rdf_worker=%d starting total_workers=%d connection=%s concurrency=%d "
        "delay=%.1f download_delay=%.1f page_size=%d storage_backend=%s "
        "skip_metadata=%s proxy_pool_size=%d legal_forms=%s",
        worker_id, total_workers, connection.name, concurrency, delay,
        download_delay, page_size, settings.storage_backend, skip_metadata,
        len(proxy_pool) if proxy_pool else 0,
        legal_forms,
    )
    asyncio.run(
        _worker_loop(
            worker_id=worker_id,
            total_workers=total_workers,
            connection=connection,
            concurrency=concurrency,
            delay=delay,
            download_delay=download_delay,
            page_size=page_size,
            dsn=dsn,
            skip_metadata=skip_metadata,
            proxy_pool=proxy_pool,
            legal_forms=legal_forms,
        )
    )
