"""
Resilient async HTTP client for the official KRS Open API.

Pure transport layer — no business logic. Handles retries, backoff,
session reuse, and polite pacing for the government API at
api-krs.ms.gov.pl.

Singleton AsyncClient, created/destroyed via start()/stop() in FastAPI lifespan.
"""

import asyncio
import logging
import random
import time
from typing import Any, Optional

import httpx

from app.config import settings

logger = logging.getLogger(__name__)

_client: Optional[httpx.AsyncClient] = None
_last_request_time: float = 0.0

_BASE_URL = settings.krs_api_base_url
_TIMEOUT = settings.krs_request_timeout
_MAX_RETRIES = settings.krs_max_retries
_DELAY_S = settings.krs_request_delay_ms / 1000.0

_RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}

_HEADERS = {
    "Accept": "application/json",
    "User-Agent": "rdf-api-project/1.0 (bankruptcy-prediction-engine)",
}


async def start() -> None:
    """Create the shared httpx.AsyncClient. Call once at app startup."""
    global _client
    _client = httpx.AsyncClient(
        base_url=_BASE_URL,
        headers=_HEADERS,
        timeout=_TIMEOUT,
        limits=httpx.Limits(max_connections=10),
        follow_redirects=True,
    )


async def stop() -> None:
    """Close the shared client. Call once at app shutdown."""
    global _client
    if _client is not None:
        await _client.aclose()
        _client = None


def _get_client() -> httpx.AsyncClient:
    if _client is None:
        raise RuntimeError("KRS client not initialised — call start() first")
    return _client


async def _polite_wait() -> None:
    """Wait until at least _DELAY_S has passed since the last request."""
    global _last_request_time
    if _last_request_time > 0:
        elapsed = time.monotonic() - _last_request_time
        remaining = _DELAY_S - elapsed
        if remaining > 0:
            await asyncio.sleep(remaining)


async def request(
    method: str,
    path: str,
    *,
    params: Optional[dict[str, Any]] = None,
) -> httpx.Response:
    """Make an HTTP request with retry, backoff, and structured logging.

    Retries on 429, 5xx, and connection errors with exponential backoff + jitter.
    Emits a structured log line for every attempt.

    Returns the httpx.Response on success.
    Raises httpx.HTTPStatusError for non-retryable 4xx errors.
    Raises httpx.RequestError if all retries are exhausted on connection errors.
    """
    global _last_request_time
    client = _get_client()
    last_exc: BaseException | None = None

    for attempt in range(1, _MAX_RETRIES + 1):
        await _polite_wait()

        t0 = time.monotonic()
        try:
            resp = await client.request(method, path, params=params)
            latency_ms = int((time.monotonic() - t0) * 1000)
            _last_request_time = time.monotonic()

            logger.info(
                "krs_api_call",
                extra={
                    "method": method,
                    "url": str(resp.url),
                    "status": resp.status_code,
                    "latency_ms": latency_ms,
                    "attempt": attempt,
                },
            )

            if resp.status_code in _RETRYABLE_STATUS_CODES:
                last_exc = httpx.HTTPStatusError(
                    f"Retryable {resp.status_code}",
                    request=resp.request,
                    response=resp,
                )
                if attempt < _MAX_RETRIES:
                    backoff = _backoff_delay(attempt)
                    logger.warning(
                        "krs_api_retry",
                        extra={
                            "status": resp.status_code,
                            "attempt": attempt,
                            "backoff_s": round(backoff, 2),
                        },
                    )
                    await asyncio.sleep(backoff)
                    continue

                resp.raise_for_status()

            return resp

        except httpx.RequestError as exc:
            latency_ms = int((time.monotonic() - t0) * 1000)
            _last_request_time = time.monotonic()
            last_exc = exc

            logger.warning(
                "krs_api_error",
                extra={
                    "method": method,
                    "path": path,
                    "error_type": type(exc).__name__,
                    "latency_ms": latency_ms,
                    "attempt": attempt,
                },
            )

            if attempt < _MAX_RETRIES:
                backoff = _backoff_delay(attempt)
                await asyncio.sleep(backoff)
                continue

            raise

    raise last_exc  # type: ignore[misc]


def _backoff_delay(attempt: int) -> float:
    """Exponential backoff with jitter: base * 2^attempt + random jitter."""
    base = 1.0
    delay = base * (2 ** (attempt - 1))
    jitter = random.uniform(0, delay * 0.5)
    return delay + jitter


async def get(
    path: str, *, params: Optional[dict[str, Any]] = None
) -> httpx.Response:
    """Shorthand for GET requests."""
    return await request("GET", path, params=params)


async def health_check() -> dict[str, Any]:
    """Ping the KRS API with a lightweight request and return health status.

    Uses a known KRS number (0000000001) to test connectivity.
    Returns {"ok": bool, "latency_ms": int, "source": "krs_open_api"}.
    """
    t0 = time.monotonic()
    try:
        resp = await get(
            "/OdpisAktualny/0000000001",
            params={"rejestr": "P", "format": "json"},
        )
        latency_ms = int((time.monotonic() - t0) * 1000)
        # 200 or 404 both mean the API is reachable
        ok = resp.status_code in {200, 404}
        return {"ok": ok, "latency_ms": latency_ms, "source": "krs_open_api"}
    except Exception:
        latency_ms = int((time.monotonic() - t0) * 1000)
        return {"ok": False, "latency_ms": latency_ms, "source": "krs_open_api"}
