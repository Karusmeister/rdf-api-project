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
from batch.entity_store import EntityStore
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

# Connection health constants
_CONSECUTIVE_FAILURES_THRESHOLD = 3   # failures before adding extra delay
_EXTRA_DELAY_INCREMENT = 1.0          # seconds added per threshold hit
_EXTRA_DELAY_CAP = 5.0               # max extra delay before cooldown
_COOLDOWN_SECONDS = 60.0             # pause connection for this long


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


@dataclass
class ConnectionHealth:
    """Track consecutive failures and manage adaptive delays / cooldowns."""
    consecutive_failures: int = 0
    extra_delay: float = 0.0

    def record_success(self):
        self.consecutive_failures = 0
        self.extra_delay = 0.0

    def record_failure(self) -> float | None:
        """Record a failure. Returns cooldown seconds if connection should pause, else None."""
        self.consecutive_failures += 1

        if self.consecutive_failures % _CONSECUTIVE_FAILURES_THRESHOLD == 0:
            self.extra_delay = min(
                self.extra_delay + _EXTRA_DELAY_INCREMENT,
                _EXTRA_DELAY_CAP,
            )

        # If we've hit the cap and still failing, trigger cooldown
        if (self.extra_delay >= _EXTRA_DELAY_CAP
                and self.consecutive_failures >= _CONSECUTIVE_FAILURES_THRESHOLD * 2):
            self.consecutive_failures = 0
            self.extra_delay = 0.0
            return _COOLDOWN_SECONDS

        return None


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
) -> tuple[str, dict | None]:
    """Call RDF API for a single KRS number, with exponential backoff on 429/503.

    Returns (status, entity_data) where status is "found"/"not_found"/"error"
    and entity_data is the parsed podmiot dict (or None).
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
                return "error", None

            resp.raise_for_status()

            data = resp.json()
            # The live API nests the entity under "podmiot" and sets
            # "czyPodmiotZnaleziony": true when the KRS exists.
            if not data:
                return "not_found", None
            if isinstance(data, dict):
                podmiot = data.get("podmiot") or {}
                found = (
                    data.get("czyPodmiotZnaleziony")
                    or data.get("numerKRS")
                    or podmiot.get("numerKRS")
                )
                if not found:
                    return "not_found", None

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
            return "found", data

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
            return "error", None

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
            return "error", None

    return "error", None


_MAX_KRS = 99_999_999  # upper bound for KRS numbers
_MAX_CONSECUTIVE_TASK_ERRORS = 50  # abort if too many consecutive task errors


async def _worker_loop(
    worker_id: int,
    start_krs: int,
    stride: int,
    connection: Connection,
    concurrency: int,
    delay: float,
    dsn: str,
) -> None:
    """Main async loop for a single worker process.

    Launches up to `concurrency` tasks in parallel using a semaphore to
    bound in-flight requests.  A feeder coroutine continuously schedules
    new KRS numbers while existing tasks are still running.

    Tracks connection health: after 3 consecutive failures the inter-request
    delay increases by 1s (up to +5s).  If failures persist at max delay,
    the connection pauses for 60s then resets.
    """
    # Ensure required tables exist even when _worker_loop is used standalone
    # (e.g. tests or direct invocation outside batch.runner pre-bootstrap).
    store = ProgressStore(dsn, init_schema=True)
    entities = EntityStore(dsn, init_schema=True)
    stats = WorkerStats()
    health = ConnectionHealth()
    sem = asyncio.Semaphore(concurrency)
    consecutive_task_errors = 0

    async with _make_client(connection) as client:

        async def _handle_one(krs_num: int) -> None:
            nonlocal consecutive_task_errors
            krs_str = str(krs_num).zfill(10)

            if store.is_done(krs_num):
                return

            entity_data = None
            try:
                async with sem:
                    result, entity_data = await _process_krs_with_backoff(client, krs_str, worker_id)
                    effective_delay = delay + health.extra_delay
                    await asyncio.sleep(effective_delay)
            except Exception as exc:
                logger.error(
                    "worker=%d krs=%s unexpected_error %s: %s",
                    worker_id, krs_str, type(exc).__name__, exc,
                )
                result = "error"

            # Track connection health
            if result == "error":
                cooldown = health.record_failure()
                if cooldown:
                    logger.warning(
                        "worker=%d connection=%s cooldown=%.0fs after %d consecutive failures",
                        worker_id, connection.name, cooldown,
                        _CONSECUTIVE_FAILURES_THRESHOLD * 2,
                    )
                    await asyncio.sleep(cooldown)
                elif health.extra_delay > 0:
                    logger.info(
                        "worker=%d connection=%s extra_delay=+%.1fs consecutive_failures=%d",
                        worker_id, connection.name, health.extra_delay,
                        health.consecutive_failures,
                    )
            else:
                health.record_success()

            # Store entity in krs_entities + krs_registry
            if result == "found" and entity_data:
                try:
                    podmiot = entity_data.get("podmiot") or {}
                    name = podmiot.get("nazwaPodmiotu", "")
                    legal_form = podmiot.get("formaPrawna")
                    entities.upsert_entity(krs_str, name, legal_form=legal_form, raw=entity_data)
                except Exception as exc:
                    logger.warning(
                        "worker=%d krs=%s entity_store_error %s: %s",
                        worker_id, krs_str, type(exc).__name__, exc,
                    )

            store.mark(krs_num, result, worker_id)
            stats.processed += 1

            if result == "found":
                stats.found += 1
                consecutive_task_errors = 0
            elif result == "not_found":
                stats.not_found += 1
                consecutive_task_errors = 0
            else:
                stats.errors += 1
                consecutive_task_errors += 1

            if stats.found > 0 and stats.found % 100 == 0:
                stats.log(worker_id)

        pending: set[asyncio.Task] = set()
        krs = start_krs

        while krs <= _MAX_KRS:
            if consecutive_task_errors >= _MAX_CONSECUTIVE_TASK_ERRORS:
                logger.error(
                    "worker=%d aborting after %d consecutive errors",
                    worker_id, consecutive_task_errors,
                )
                break

            # Fill up to `concurrency` in-flight tasks
            while len(pending) < concurrency and krs <= _MAX_KRS:
                task = asyncio.create_task(_handle_one(krs))
                pending.add(task)
                task.add_done_callback(pending.discard)
                krs += stride

            # Wait for at least one task to finish before scheduling more
            if pending:
                done, _ = await asyncio.wait(
                    pending, return_when=asyncio.FIRST_COMPLETED,
                )
                for t in done:
                    exc = t.exception()
                    if exc is not None:
                        logger.error(
                            "worker=%d task_error %s: %s",
                            worker_id, type(exc).__name__, exc,
                        )

        # Drain remaining tasks
        if pending:
            done, _ = await asyncio.wait(pending)
            for t in done:
                exc = t.exception()
                if exc is not None:
                    logger.error("worker=%d drain_task_error %s", worker_id, exc)

        stats.log(worker_id)
        logger.info("worker=%d finished krs_reached=%d", worker_id, krs)


def run_worker(
    worker_id: int,
    start_krs: int,
    stride: int,
    connection: Connection,
    concurrency: int,
    delay: float,
    dsn: str,
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
            dsn=dsn,
        )
    )
