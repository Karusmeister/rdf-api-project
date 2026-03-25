"""
Async httpx wrapper for the RDF upstream API.

A single AsyncClient instance is shared per worker process (created in lifespan).
"""

import logging
import time
import urllib.parse
from typing import Optional

import httpx

from app.config import settings
from app.crypto import encrypt_nrkrs

logger = logging.getLogger(__name__)

_client: Optional[httpx.AsyncClient] = None

_HEADERS = {
    "Accept": "application/json",
    "Content-Type": "application/json",
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    ),
    "Referer": settings.rdf_referer,
    "Origin": settings.rdf_origin,
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
}


async def start() -> None:
    global _client
    _client = httpx.AsyncClient(
        base_url=settings.rdf_base_url,
        headers=_HEADERS,
        timeout=settings.request_timeout,
        limits=httpx.Limits(max_connections=settings.max_connections),
        follow_redirects=True,
    )
    logger.info("rdf_client_started", extra={"event": "rdf_client_started", "base_url": settings.rdf_base_url})


async def stop() -> None:
    global _client
    if _client is not None:
        await _client.aclose()
        _client = None
        logger.info("rdf_client_stopped", extra={"event": "rdf_client_stopped"})


def _get_client() -> httpx.AsyncClient:
    if _client is None:
        raise RuntimeError("RDF client not initialised - call start() first")
    return _client


async def _log_call(operation: str, t0: float, resp: httpx.Response, **extra) -> None:
    latency_ms = int((time.monotonic() - t0) * 1000)
    logger.info(
        "rdf_api_call",
        extra={
            "event": "rdf_api_call",
            "operation": operation,
            "status": resp.status_code,
            "latency_ms": latency_ms,
            **extra,
        },
    )


async def dane_podstawowe(krs: str) -> dict:
    client = _get_client()
    t0 = time.monotonic()
    resp = await client.post(
        "/podmioty/wyszukiwanie/dane-podstawowe",
        json={"numerKRS": krs.zfill(10)},
    )
    await _log_call("dane_podstawowe", t0, resp, krs=krs.zfill(10))
    resp.raise_for_status()
    return resp.json()


async def rodzaje_dokumentow(krs: str) -> list:
    client = _get_client()
    t0 = time.monotonic()
    resp = await client.post(
        "/dokumenty/rodzajeDokWyszukiwanie",
        json={"nrKRS": krs.zfill(10)},
    )
    await _log_call("rodzaje_dokumentow", t0, resp, krs=krs.zfill(10))
    resp.raise_for_status()
    return resp.json()


async def wyszukiwanie(
    krs: str,
    page: int = 0,
    page_size: int = 10,
    sort_field: str = "id",
    sort_dir: str = "MALEJACO",
) -> dict:
    encrypted_krs = encrypt_nrkrs(krs)
    client = _get_client()
    payload = {
        "metadaneStronicowania": {
            "numerStrony": page,
            "rozmiarStrony": page_size,
            "metadaneSortowania": [{"atrybut": sort_field, "kierunek": sort_dir}],
        },
        "nrKRS": encrypted_krs,
    }
    t0 = time.monotonic()
    resp = await client.post("/dokumenty/wyszukiwanie", json=payload)
    await _log_call("wyszukiwanie", t0, resp, krs=krs.zfill(10), page=page)
    resp.raise_for_status()
    return resp.json()


async def metadata(doc_id: str) -> dict:
    encoded_id = urllib.parse.quote(doc_id, safe="")
    client = _get_client()
    t0 = time.monotonic()
    resp = await client.get(f"/dokumenty/{encoded_id}")
    await _log_call("metadata", t0, resp, doc_id=doc_id)
    resp.raise_for_status()
    return resp.json()


async def download(doc_ids: list[str]) -> bytes:
    client = _get_client()
    t0 = time.monotonic()
    resp = await client.post(
        "/dokumenty/tresc",
        json=doc_ids,
        headers={"Accept": "application/octet-stream"},
    )
    await _log_call("download", t0, resp, doc_count=len(doc_ids))
    resp.raise_for_status()
    return resp.content
