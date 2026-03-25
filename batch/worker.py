"""Async worker loop for the batch KRS scanner.

Each worker process runs one instance of this loop. Workers use stride
partitioning — with N workers, worker i processes KRS numbers:
  start + i, start + i + N, start + i + 2N, ...

No coordination between workers is required.
"""

import asyncio
import logging
import time
from dataclasses import dataclass, field

import httpx

from app.config import settings
from app.crypto import encrypt_nrkrs
from batch.connections import Connection
from batch.progress import ProgressStore

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
_MAX_RETRIES_RATE_LIMIT = 4  # for 429/503
_MAX_RETRIES_NETWORK = 2     # for connection/timeout errors
_BACKOFF_CAP_SECONDS = 60.0
_NETWORK_RETRY_DELAY = 5.0


@dataclass
class WorkerStats:
    processed: int = 0
    found: int = 0
    not_found: int = 0
    errors: int = 0
    start_time: float = field(default_factory=time.monotonic)

    def log(self, worker_id: int) -> None:
        elapsed = time.monotonic() - self.start_time
        rate = self.processed / elapsed if elapsed > 0 else 0
        logger.info(
            "worker=%d processed=%d found=%d not_found=%d errors=%d elapsed=%.0fs rate=%.1f/s",
            worker_id, self.processed, self.found, self.not_found,
            self.errors, elapsed, rate,
        )


def _make_client(connection: Connection) -> httpx.AsyncClient:
    """Create an httpx.AsyncClient, optionally with a SOCKS5 proxy."""
    kwargs: dict = {
        "base_url": _RDF_BASE,
        "headers": _RDF_HEADERS,
        "timeout": settings.request_timeout,
        "follow_redirects": True,
    }
    if connection.proxy_url is not None:
        kwargs["proxy"] = connection.proxy_url
    return httpx.AsyncClient(**kwargs)


async def _process_krs_with_backoff(
    client: httpx.AsyncClient,
    krs_str: str,
    worker_id: int,
) -> str:
    """Call RDF API for a single KRS number, with exponential backoff on 429/503.

    Returns one of: "found", "not_found", "error".
    """
    for attempt in range(_MAX_RETRIES_RATE_LIMIT + 1):
        try:
            resp = await client.post(
                "/podmioty/wyszukiwanie/dane-podstawowe",
                json={"numerKRS": krs_str},
            )
            if resp.status_code in (429, 503):
                if attempt < _MAX_RETRIES_RATE_LIMIT:
                    delay = min(1.0 * (2 ** attempt), _BACKOFF_CAP_SECONDS)
                    logger.warning(
                        "worker=%d krs=%s retry=%d after=%.0fs reason=%d",
                        worker_id, krs_str, attempt + 1, delay, resp.status_code,
                    )
                    await asyncio.sleep(delay)
                    continue
                logger.error(
                    "worker=%d krs=%s max_retries_exceeded status=%d",
                    worker_id, krs_str, resp.status_code,
                )
                return "error"

            resp.raise_for_status()

            data = resp.json()
            if not data or (isinstance(data, dict) and not data.get("numerKRS")):
                return "not_found"

            # Entity exists — fetch documents
            encrypted = encrypt_nrkrs(krs_str.lstrip("0") or "0")
            doc_payload = {
                "metadaneStronicowania": {
                    "numerStrony": 0,
                    "rozmiarStrony": 10,
                    "metadaneSortowania": [{"atrybut": "id", "kierunek": "MALEJACO"}],
                },
                "nrKRS": encrypted,
            }
            doc_resp = await client.post("/dokumenty/wyszukiwanie", json=doc_payload)
            doc_resp.raise_for_status()
            # TODO: insert into DB / trigger analysis pipeline here
            return "found"

        except httpx.HTTPStatusError as exc:
            # Non-retryable HTTP errors (4xx other than 429, 5xx other than 503)
            status = exc.response.status_code
            if status in (429, 503):
                # Shouldn't reach here but handle defensively
                if attempt < _MAX_RETRIES_RATE_LIMIT:
                    delay = min(1.0 * (2 ** attempt), _BACKOFF_CAP_SECONDS)
                    logger.warning(
                        "worker=%d krs=%s retry=%d after=%.0fs reason=%d",
                        worker_id, krs_str, attempt + 1, delay, status,
                    )
                    await asyncio.sleep(delay)
                    continue
            logger.error(
                "worker=%d krs=%s http_error status=%d",
                worker_id, krs_str, status,
            )
            return "error"

        except (httpx.TimeoutException, httpx.ConnectError, OSError) as exc:
            if attempt < _MAX_RETRIES_NETWORK:
                logger.warning(
                    "worker=%d krs=%s retry=%d after=%.0fs reason=%s",
                    worker_id, krs_str, attempt + 1, _NETWORK_RETRY_DELAY,
                    type(exc).__name__,
                )
                await asyncio.sleep(_NETWORK_RETRY_DELAY)
                continue
            logger.error(
                "worker=%d krs=%s network_error type=%s",
                worker_id, krs_str, type(exc).__name__,
            )
            return "error"

    return "error"


async def _worker_loop(
    worker_id: int,
    start_krs: int,
    stride: int,
    connection: Connection,
    concurrency: int,
    delay: float,
    db_path: str,
) -> None:
    """Main async loop for a single worker process.

    Launches up to `concurrency` tasks in parallel using a semaphore to
    bound in-flight requests.  A feeder coroutine continuously schedules
    new KRS numbers while existing tasks are still running.
    """
    store = ProgressStore(db_path)
    stats = WorkerStats()
    sem = asyncio.Semaphore(concurrency)

    async with _make_client(connection) as client:

        async def _handle_one(krs_num: int) -> None:
            krs_str = str(krs_num).zfill(10)

            if store.is_done(krs_num):
                return

            async with sem:
                result = await _process_krs_with_backoff(client, krs_str, worker_id)
                await asyncio.sleep(delay)

            store.mark(krs_num, result, worker_id)
            stats.processed += 1

            if result == "found":
                stats.found += 1
            elif result == "not_found":
                stats.not_found += 1
            else:
                stats.errors += 1

            if stats.found > 0 and stats.found % 100 == 0:
                stats.log(worker_id)

        pending: set[asyncio.Task] = set()
        krs = start_krs

        while True:
            # Fill up to `concurrency` in-flight tasks
            while len(pending) < concurrency:
                task = asyncio.create_task(_handle_one(krs))
                pending.add(task)
                task.add_done_callback(pending.discard)
                krs += stride

            # Wait for at least one task to finish before scheduling more
            if pending:
                done, _ = await asyncio.wait(
                    pending, return_when=asyncio.FIRST_COMPLETED,
                )
                # Re-raise any unexpected exceptions from completed tasks
                for t in done:
                    if t.exception() is not None:
                        logger.error(
                            "worker=%d task_error %s", worker_id, t.exception(),
                        )


def run_worker(
    worker_id: int,
    start_krs: int,
    stride: int,
    connection: Connection,
    concurrency: int,
    delay: float,
    db_path: str,
) -> None:
    """Entrypoint for multiprocessing.Process — sets up logging and runs the async loop."""
    logging.basicConfig(
        level=logging.INFO,
        format=f"%(asctime)s [worker-{worker_id}] %(levelname)s %(message)s",
    )
    logger.info(
        "worker=%d starting start_krs=%d stride=%d connection=%s concurrency=%d delay=%.1f",
        worker_id, start_krs, stride, connection.name, concurrency, delay,
    )
    asyncio.run(
        _worker_loop(
            worker_id=worker_id,
            start_krs=start_krs,
            stride=stride,
            connection=connection,
            concurrency=concurrency,
            delay=delay,
            db_path=db_path,
        )
    )
