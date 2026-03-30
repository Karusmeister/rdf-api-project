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
from batch.connections import Connection
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
) -> tuple[str, list[dict]]:
    """Fetch ALL pages of documents for a KRS number.

    Returns (status, all_documents) where status is "ok" / "empty" / "error".
    """
    all_docs: list[dict] = []

    status, docs, total_pages = await _fetch_documents_page(
        client, krs, page=0, page_size=page_size, worker_id=worker_id,
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
) -> dict | None:
    """Fetch document metadata. Returns metadata dict or None on failure."""
    encoded_id = urllib.parse.quote(doc_id, safe="")
    for attempt in range(_MAX_RETRIES_RATE_LIMIT + 1):
        try:
            resp = await client.get(f"/dokumenty/{encoded_id}")

            if resp.status_code in (429, 503):
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
) -> bool:
    """Download a single document: metadata → ZIP → extract → mark in DB.

    Returns True on success, False on failure.
    """
    # 1. Fetch metadata
    meta = await _fetch_metadata_with_backoff(client, doc_id, worker_id)
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
    zip_bytes = await _download_zip_with_backoff(client, doc_id, worker_id)
    await asyncio.sleep(delay)

    if zip_bytes is None:
        doc_store.update_error(doc_id, "download_failed")
        stats.documents_failed += 1
        return False

    # 3. Extract to disk
    try:
        doc_dir = make_doc_dir(krs, doc_id)
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
    page_size: int,
    db_path: str,
) -> None:
    """Main async loop for a single RDF document discovery + download worker."""
    progress = RdfProgressStore(db_path)
    doc_store = RdfDocumentStore(db_path)
    storage = create_storage()
    stats = RdfWorkerStats()
    health = ConnectionHealth()
    sem = asyncio.Semaphore(concurrency)

    # Pre-fetch our partition: new KRS numbers + already-discovered but undownloaded
    new_krs = progress.get_pending_krs(worker_id, total_workers)
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

    async with _make_client(connection) as client:

        async def _handle_one(krs: str) -> None:
            already_discovered = progress.is_done(krs)

            # Phase 1: Discovery (skip if already done)
            doc_count = 0
            if not already_discovered:
                all_docs: list[dict] = []
                result = "error"
                try:
                    async with sem:
                        result, all_docs = await _fetch_all_documents(
                            client, krs, page_size, delay, worker_id,
                        )
                        effective_delay = delay + health.extra_delay
                        await asyncio.sleep(effective_delay)
                except Exception as exc:
                    logger.error(
                        "rdf_worker=%d krs=%s unexpected_error %s: %s",
                        worker_id, krs, type(exc).__name__, exc,
                    )
                    result = "error"

                # Track connection health
                if result == "error" and not all_docs:
                    cooldown = health.record_failure()
                    if cooldown:
                        logger.warning(
                            "rdf_worker=%d connection=%s cooldown=%.0fs",
                            worker_id, connection.name, cooldown,
                        )
                        await asyncio.sleep(cooldown)
                else:
                    health.record_success()

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

            # Phase 2: Download undownloaded documents
            try:
                undownloaded = doc_store.get_undownloaded(krs)
                for doc_id in undownloaded:
                    async with sem:
                        await _download_one_document(
                            client, krs, doc_id, doc_store, storage,
                            delay, worker_id, stats,
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
        idx = 0

        while idx < len(pending_krs) or pending:
            while len(pending) < concurrency and idx < len(pending_krs):
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

    stats.log(worker_id)
    logger.info("rdf_worker=%d finished", worker_id)


def run_rdf_worker(
    worker_id: int,
    total_workers: int,
    connection: Connection,
    concurrency: int,
    delay: float,
    page_size: int,
    db_path: str,
) -> None:
    """Entrypoint for multiprocessing.Process."""
    logging.basicConfig(
        level=logging.INFO,
        format=f"%(asctime)s [rdf-worker-{worker_id}] %(levelname)s %(message)s",
    )
    logger.info(
        "rdf_worker=%d starting total_workers=%d connection=%s concurrency=%d "
        "delay=%.1f page_size=%d storage_backend=%s",
        worker_id, total_workers, connection.name, concurrency, delay,
        page_size, settings.storage_backend,
    )
    asyncio.run(
        _worker_loop(
            worker_id=worker_id,
            total_workers=total_workers,
            connection=connection,
            concurrency=concurrency,
            delay=delay,
            page_size=page_size,
            db_path=db_path,
        )
    )
