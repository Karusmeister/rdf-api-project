"""Tests for batch/rdf_worker.py — RDF document discovery + download worker."""

import io
import logging
import zipfile

import httpx
import pytest
import respx

from app.config import settings
from app.db.connection import make_connection
from batch.connections import Connection
from batch.rdf_document_store import RdfDocumentStore
from batch.rdf_worker import (
    AdaptiveSemaphore,
    ConnectionHealth,
    RateLimitTracker,
    RdfWorkerStats,
    _download_one_document,
    _download_zip_with_backoff,
    _fetch_all_documents,
    _fetch_documents_page,
    _fetch_metadata_with_backoff,
    _make_client,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def rdf_base():
    return settings.rdf_base_url


@pytest.fixture
def _no_sleep(monkeypatch):
    async def _fake_sleep(_):
        pass
    monkeypatch.setattr("batch.rdf_worker.asyncio.sleep", _fake_sleep)


_SAMPLE_DOC = {
    "id": "abc123==",
    "rodzaj": "SF",
    "status": "AKTUALNY",
    "nazwa": "Sprawozdanie finansowe 2023",
    "okresSprawozdawczyPoczatek": "2023-01-01",
    "okresSprawozdawczyKoniec": "2023-12-31",
}


def _make_zip(filename: str = "test.xml", content: bytes = b"<xml/>") -> bytes:
    """Create a minimal in-memory ZIP archive."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr(filename, content)
    return buf.getvalue()


# ---------------------------------------------------------------------------
# RdfWorkerStats
# ---------------------------------------------------------------------------

def test_rdf_worker_stats_defaults():
    s = RdfWorkerStats()
    assert s.krs_processed == 0
    assert s.documents_found == 0
    assert s.documents_downloaded == 0
    assert s.documents_failed == 0
    assert s.bytes_downloaded == 0


def test_rdf_worker_stats_log_does_not_raise(caplog):
    with caplog.at_level(logging.INFO, logger="batch.rdf_worker"):
        s = RdfWorkerStats()
        s.krs_processed = 10
        s.documents_found = 50
        s.log(worker_id=0)
    assert "rdf_worker=0" in caplog.text


# ---------------------------------------------------------------------------
# ConnectionHealth
# ---------------------------------------------------------------------------

def test_connection_health_success_resets():
    h = ConnectionHealth()
    h.consecutive_failures = 5
    h.extra_delay = 3.0
    h.record_success()
    assert h.consecutive_failures == 0
    assert h.extra_delay == 0.0


def test_connection_health_failure_increments():
    h = ConnectionHealth()
    result = h.record_failure()
    assert result is None
    assert h.consecutive_failures == 1


def test_connection_health_cooldown_after_sustained_failures():
    h = ConnectionHealth()
    cooldown = None
    for _ in range(15):
        cooldown = h.record_failure()
    assert cooldown is not None
    assert cooldown == 60.0


# ---------------------------------------------------------------------------
# AdaptiveSemaphore
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_adaptive_semaphore_basic_acquire_release():
    sem = AdaptiveSemaphore(3)
    assert sem.capacity == 3
    async with sem:
        pass  # acquire + release
    assert sem.capacity == 3


@pytest.mark.asyncio
async def test_adaptive_semaphore_reduce_absorbs_release():
    sem = AdaptiveSemaphore(3, min_value=1)
    assert sem.capacity == 3

    # Reduce by 1 — next release will be absorbed
    assert sem.reduce() == 2

    async with sem:
        pass  # this release is absorbed

    assert sem.capacity == 2
    # Subsequent releases are normal
    async with sem:
        pass
    assert sem.capacity == 2


@pytest.mark.asyncio
async def test_adaptive_semaphore_respects_min():
    sem = AdaptiveSemaphore(3, min_value=2)
    assert sem.reduce() == 2
    assert sem.reduce() == 2  # already at min
    assert sem.capacity == 2


# ---------------------------------------------------------------------------
# RateLimitTracker
# ---------------------------------------------------------------------------

def test_rate_limit_tracker_below_threshold():
    t = RateLimitTracker(threshold=5, window_secs=60.0)
    for _ in range(4):
        assert t.record() is False


def test_rate_limit_tracker_at_threshold():
    t = RateLimitTracker(threshold=5, window_secs=60.0)
    for _ in range(4):
        t.record()
    assert t.record() is True  # 5th hit


def test_rate_limit_tracker_fires_once_per_burst():
    """Reaching threshold fires True once, then resets — next burst needs another 5."""
    t = RateLimitTracker(threshold=5, window_secs=60.0)
    fires = []
    for i in range(15):
        fires.append(t.record())
    # Should fire exactly at hit 5, 10, 15
    assert fires == [
        False, False, False, False, True,   # burst 1
        False, False, False, False, True,   # burst 2
        False, False, False, False, True,   # burst 3
    ]


def test_rate_limit_tracker_burst_reductions_with_semaphore():
    """Integration: 10 429s with threshold=5 should reduce capacity exactly twice."""
    sem = AdaptiveSemaphore(8, min_value=2)
    tracker = RateLimitTracker(threshold=5, window_secs=60.0)
    reductions = 0
    for _ in range(10):
        if tracker.record():
            old = sem.capacity
            new = sem.reduce()
            if new < old:
                reductions += 1
    assert reductions == 2
    assert sem.capacity == 6  # 8 -> 7 -> 6


def test_rate_limit_tracker_resets_after_window(monkeypatch):
    import time as _time
    current = [_time.monotonic()]
    monkeypatch.setattr("batch.rdf_worker.time.monotonic", lambda: current[0])

    t = RateLimitTracker(threshold=3, window_secs=10.0)
    t.record()
    t.record()

    # Advance past window
    current[0] += 15.0
    assert t.record() is False  # reset, only 1 in new window


# ---------------------------------------------------------------------------
# on_429 callback integration
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
@pytest.mark.usefixtures("_no_sleep")
async def test_fetch_page_429_fires_on_429_callback(rdf_base):
    calls = []

    def side_effect(request):
        if len(calls) == 0:
            calls.append(1)
            return httpx.Response(429)
        return httpx.Response(200, json={
            "content": [_SAMPLE_DOC],
            "metadaneWynikow": {
                "numerStrony": 0, "rozmiarStrony": 10,
                "liczbaStron": 1, "calkowitaLiczbaObiektow": 1,
            },
        })

    callback_count = [0]

    def on_429():
        callback_count[0] += 1

    with respx.mock:
        respx.post(f"{rdf_base}/dokumenty/wyszukiwanie").mock(
            side_effect=side_effect
        )
        async with httpx.AsyncClient(base_url=rdf_base) as client:
            status, docs, _ = await _fetch_documents_page(
                client, "0000000001", page=0, page_size=10, worker_id=0,
                on_429=on_429,
            )
    assert status == "ok"
    assert callback_count[0] == 1


@pytest.mark.asyncio
@pytest.mark.usefixtures("_no_sleep")
async def test_download_zip_429_fires_on_429_callback(rdf_base):
    calls = []

    def side_effect(request):
        if len(calls) == 0:
            calls.append(1)
            return httpx.Response(429)
        return httpx.Response(200, content=b"fake-zip")

    callback_count = [0]

    def on_429():
        callback_count[0] += 1

    with respx.mock:
        respx.post(f"{rdf_base}/dokumenty/tresc").mock(
            side_effect=side_effect
        )
        async with httpx.AsyncClient(base_url=rdf_base) as client:
            result = await _download_zip_with_backoff(
                client, "doc1==", worker_id=0, on_429=on_429,
            )
    assert result == b"fake-zip"
    assert callback_count[0] == 1


# ---------------------------------------------------------------------------
# _make_client
# ---------------------------------------------------------------------------

def test_make_client_direct():
    conn = Connection(name="direct")
    client = _make_client(conn)
    assert isinstance(client, httpx.AsyncClient)


# ---------------------------------------------------------------------------
# _fetch_documents_page — respx mocks
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_fetch_page_returns_documents(rdf_base):
    response_data = {
        "content": [_SAMPLE_DOC],
        "metadaneWynikow": {
            "numerStrony": 0,
            "rozmiarStrony": 10,
            "liczbaStron": 1,
            "calkowitaLiczbaObiektow": 1,
        },
    }
    with respx.mock:
        respx.post(f"{rdf_base}/dokumenty/wyszukiwanie").mock(
            return_value=httpx.Response(200, json=response_data)
        )
        async with httpx.AsyncClient(base_url=rdf_base) as client:
            status, docs, total_pages = await _fetch_documents_page(
                client, "0000000001", page=0, page_size=10, worker_id=0,
            )
    assert status == "ok"
    assert len(docs) == 1
    assert docs[0]["id"] == "abc123=="
    assert total_pages == 1


@pytest.mark.asyncio
async def test_fetch_page_empty_response(rdf_base):
    response_data = {
        "content": [],
        "metadaneWynikow": {
            "numerStrony": 0,
            "rozmiarStrony": 10,
            "liczbaStron": 0,
            "calkowitaLiczbaObiektow": 0,
        },
    }
    with respx.mock:
        respx.post(f"{rdf_base}/dokumenty/wyszukiwanie").mock(
            return_value=httpx.Response(200, json=response_data)
        )
        async with httpx.AsyncClient(base_url=rdf_base) as client:
            status, docs, _ = await _fetch_documents_page(
                client, "0000000001", page=0, page_size=10, worker_id=0,
            )
    assert status == "empty"
    assert docs == []


@pytest.mark.asyncio
async def test_fetch_page_http_500_error(rdf_base):
    with respx.mock:
        respx.post(f"{rdf_base}/dokumenty/wyszukiwanie").mock(
            return_value=httpx.Response(500)
        )
        async with httpx.AsyncClient(base_url=rdf_base) as client:
            status, docs, _ = await _fetch_documents_page(
                client, "0000000001", page=0, page_size=10, worker_id=0,
            )
    assert status == "error"


@pytest.mark.asyncio
@pytest.mark.usefixtures("_no_sleep")
async def test_fetch_page_429_retries_then_succeeds(rdf_base):
    call_count = 0

    def side_effect(request):
        nonlocal call_count
        call_count += 1
        if call_count <= 2:
            return httpx.Response(429)
        return httpx.Response(200, json={
            "content": [_SAMPLE_DOC],
            "metadaneWynikow": {
                "numerStrony": 0, "rozmiarStrony": 10,
                "liczbaStron": 1, "calkowitaLiczbaObiektow": 1,
            },
        })

    with respx.mock:
        respx.post(f"{rdf_base}/dokumenty/wyszukiwanie").mock(
            side_effect=side_effect
        )
        async with httpx.AsyncClient(base_url=rdf_base) as client:
            status, docs, _ = await _fetch_documents_page(
                client, "0000000001", page=0, page_size=10, worker_id=0,
            )
    assert status == "ok"
    assert call_count == 3


@pytest.mark.asyncio
@pytest.mark.usefixtures("_no_sleep")
async def test_fetch_page_network_error_retries(rdf_base):
    call_count = 0

    def side_effect(request):
        nonlocal call_count
        call_count += 1
        raise httpx.ConnectError("connection refused")

    with respx.mock:
        respx.post(f"{rdf_base}/dokumenty/wyszukiwanie").mock(
            side_effect=side_effect
        )
        async with httpx.AsyncClient(base_url=rdf_base) as client:
            status, docs, _ = await _fetch_documents_page(
                client, "0000000001", page=0, page_size=10, worker_id=0,
            )
    assert status == "error"
    assert call_count == 3  # initial + 2 retries


# ---------------------------------------------------------------------------
# _fetch_all_documents — pagination
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
@pytest.mark.usefixtures("_no_sleep")
async def test_fetch_all_documents_single_page(rdf_base):
    response_data = {
        "content": [_SAMPLE_DOC],
        "metadaneWynikow": {
            "numerStrony": 0, "rozmiarStrony": 10,
            "liczbaStron": 1, "calkowitaLiczbaObiektow": 1,
        },
    }
    with respx.mock:
        respx.post(f"{rdf_base}/dokumenty/wyszukiwanie").mock(
            return_value=httpx.Response(200, json=response_data)
        )
        async with httpx.AsyncClient(base_url=rdf_base) as client:
            status, docs = await _fetch_all_documents(
                client, "0000000001", page_size=10, delay=0, worker_id=0,
            )
    assert status == "ok"
    assert len(docs) == 1


@pytest.mark.asyncio
@pytest.mark.usefixtures("_no_sleep")
async def test_fetch_all_documents_multi_page(rdf_base):
    doc_a = {**_SAMPLE_DOC, "id": "page0_doc"}
    doc_b = {**_SAMPLE_DOC, "id": "page1_doc"}
    call_count = 0

    def side_effect(request):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return httpx.Response(200, json={
                "content": [doc_a],
                "metadaneWynikow": {
                    "numerStrony": 0, "rozmiarStrony": 1,
                    "liczbaStron": 2, "calkowitaLiczbaObiektow": 2,
                },
            })
        return httpx.Response(200, json={
            "content": [doc_b],
            "metadaneWynikow": {
                "numerStrony": 1, "rozmiarStrony": 1,
                "liczbaStron": 2, "calkowitaLiczbaObiektow": 2,
            },
        })

    with respx.mock:
        respx.post(f"{rdf_base}/dokumenty/wyszukiwanie").mock(
            side_effect=side_effect
        )
        async with httpx.AsyncClient(base_url=rdf_base) as client:
            status, docs = await _fetch_all_documents(
                client, "0000000001", page_size=1, delay=0, worker_id=0,
            )
    assert status == "ok"
    assert len(docs) == 2
    assert {d["id"] for d in docs} == {"page0_doc", "page1_doc"}


@pytest.mark.asyncio
@pytest.mark.usefixtures("_no_sleep")
async def test_fetch_all_documents_partial_on_error(rdf_base):
    call_count = 0

    def side_effect(request):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return httpx.Response(200, json={
                "content": [_SAMPLE_DOC],
                "metadaneWynikow": {
                    "numerStrony": 0, "rozmiarStrony": 1,
                    "liczbaStron": 2, "calkowitaLiczbaObiektow": 2,
                },
            })
        return httpx.Response(500)

    with respx.mock:
        respx.post(f"{rdf_base}/dokumenty/wyszukiwanie").mock(
            side_effect=side_effect
        )
        async with httpx.AsyncClient(base_url=rdf_base) as client:
            status, docs = await _fetch_all_documents(
                client, "0000000001", page_size=1, delay=0, worker_id=0,
            )
    assert status == "error"
    assert len(docs) == 1


@pytest.mark.asyncio
@pytest.mark.usefixtures("_no_sleep")
async def test_fetch_all_documents_empty(rdf_base):
    with respx.mock:
        respx.post(f"{rdf_base}/dokumenty/wyszukiwanie").mock(
            return_value=httpx.Response(200, json={
                "content": [],
                "metadaneWynikow": {
                    "numerStrony": 0, "rozmiarStrony": 10,
                    "liczbaStron": 0, "calkowitaLiczbaObiektow": 0,
                },
            })
        )
        async with httpx.AsyncClient(base_url=rdf_base) as client:
            status, docs = await _fetch_all_documents(
                client, "0000000001", page_size=10, delay=0, worker_id=0,
            )
    assert status == "empty"
    assert docs == []


# ---------------------------------------------------------------------------
# _fetch_metadata_with_backoff
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
@pytest.mark.usefixtures("_no_sleep")
async def test_fetch_metadata_success(rdf_base):
    meta = {"nazwaPliku": "sf.xml", "czyMSR": False, "czyKorekta": False}
    with respx.mock:
        respx.get(f"{rdf_base}/dokumenty/abc123%3D%3D").mock(
            return_value=httpx.Response(200, json=meta)
        )
        async with httpx.AsyncClient(base_url=rdf_base) as client:
            result = await _fetch_metadata_with_backoff(client, "abc123==", worker_id=0)
    assert result is not None
    assert result["nazwaPliku"] == "sf.xml"


@pytest.mark.asyncio
@pytest.mark.usefixtures("_no_sleep")
async def test_fetch_metadata_500_returns_none(rdf_base):
    with respx.mock:
        respx.get(f"{rdf_base}/dokumenty/abc123%3D%3D").mock(
            return_value=httpx.Response(500)
        )
        async with httpx.AsyncClient(base_url=rdf_base) as client:
            result = await _fetch_metadata_with_backoff(client, "abc123==", worker_id=0)
    assert result is None


@pytest.mark.asyncio
@pytest.mark.usefixtures("_no_sleep")
async def test_fetch_metadata_network_error_returns_none(rdf_base):
    with respx.mock:
        respx.get(f"{rdf_base}/dokumenty/abc123%3D%3D").mock(
            side_effect=httpx.ConnectError("refused")
        )
        async with httpx.AsyncClient(base_url=rdf_base) as client:
            result = await _fetch_metadata_with_backoff(client, "abc123==", worker_id=0)
    assert result is None


# ---------------------------------------------------------------------------
# _download_zip_with_backoff
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
@pytest.mark.usefixtures("_no_sleep")
async def test_download_zip_success(rdf_base):
    zip_bytes = _make_zip()
    with respx.mock:
        respx.post(f"{rdf_base}/dokumenty/tresc").mock(
            return_value=httpx.Response(200, content=zip_bytes)
        )
        async with httpx.AsyncClient(base_url=rdf_base) as client:
            result = await _download_zip_with_backoff(client, "abc123==", worker_id=0)
    assert result is not None
    assert len(result) > 0


@pytest.mark.asyncio
@pytest.mark.usefixtures("_no_sleep")
async def test_download_zip_500_returns_none(rdf_base):
    with respx.mock:
        respx.post(f"{rdf_base}/dokumenty/tresc").mock(
            return_value=httpx.Response(500)
        )
        async with httpx.AsyncClient(base_url=rdf_base) as client:
            result = await _download_zip_with_backoff(client, "abc123==", worker_id=0)
    assert result is None


# ---------------------------------------------------------------------------
# _download_one_document — integration
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
@pytest.mark.usefixtures("_no_sleep")
async def test_download_one_document_full_flow(rdf_base, pg_dsn, clean_pg, tmp_path):
    """Full download flow: metadata + ZIP download + extract + mark in DB."""
    storage_dir = str(tmp_path / "documents")
    doc_store = RdfDocumentStore(pg_dsn)
    doc_store.insert_documents("0000000001", [_SAMPLE_DOC])

    from app.scraper.storage import LocalStorage
    storage = LocalStorage(storage_dir)
    stats = RdfWorkerStats()

    zip_bytes = _make_zip("report.xml", b"<sprawozdanie/>")
    meta = {"nazwaPliku": "report.xml", "czyMSR": False, "czyKorekta": False}

    with respx.mock:
        respx.get(f"{rdf_base}/dokumenty/abc123%3D%3D").mock(
            return_value=httpx.Response(200, json=meta)
        )
        respx.post(f"{rdf_base}/dokumenty/tresc").mock(
            return_value=httpx.Response(200, content=zip_bytes)
        )

        async with httpx.AsyncClient(base_url=rdf_base) as client:
            ok = await _download_one_document(
                client, "0000000001", "abc123==",
                doc_store, storage, delay=0, worker_id=0, stats=stats,
            )

    assert ok is True
    assert stats.documents_downloaded == 1
    assert stats.bytes_downloaded > 0

    # Verify DB updated
    wrapper = make_connection(pg_dsn)
    row = wrapper.execute(
        "SELECT is_downloaded, storage_path, file_count FROM krs_documents WHERE document_id = %s",
        ["abc123=="],
    ).fetchone()
    wrapper.close()
    assert row[0] is True
    assert row[1] is not None
    assert row[2] >= 1

    # Verify file on disk
    import os
    assert os.path.isdir(os.path.join(storage_dir, "krs", "0000000001"))


@pytest.mark.asyncio
@pytest.mark.usefixtures("_no_sleep")
async def test_download_one_document_zip_failure(rdf_base, pg_dsn, clean_pg, tmp_path):
    """When ZIP download fails, document gets error status."""
    storage_dir = str(tmp_path / "documents")
    doc_store = RdfDocumentStore(pg_dsn)
    doc_store.insert_documents("0000000001", [_SAMPLE_DOC])

    from app.scraper.storage import LocalStorage
    storage = LocalStorage(storage_dir)
    stats = RdfWorkerStats()

    meta = {"nazwaPliku": "report.xml"}

    with respx.mock:
        respx.get(f"{rdf_base}/dokumenty/abc123%3D%3D").mock(
            return_value=httpx.Response(200, json=meta)
        )
        respx.post(f"{rdf_base}/dokumenty/tresc").mock(
            return_value=httpx.Response(500)
        )

        async with httpx.AsyncClient(base_url=rdf_base) as client:
            ok = await _download_one_document(
                client, "0000000001", "abc123==",
                doc_store, storage, delay=0, worker_id=0, stats=stats,
            )

    assert ok is False
    assert stats.documents_failed == 1

    # Verify error recorded in DB
    wrapper = make_connection(pg_dsn)
    row = wrapper.execute(
        "SELECT is_downloaded, download_error FROM krs_documents WHERE document_id = %s",
        ["abc123=="],
    ).fetchone()
    wrapper.close()
    assert row[0] is False
    assert row[1] is not None


# ---------------------------------------------------------------------------
# RdfDocumentStore — CRUD
# ---------------------------------------------------------------------------

def test_document_store_insert(pg_dsn, clean_pg):
    store = RdfDocumentStore(pg_dsn)
    count = store.insert_documents("0000000001", [_SAMPLE_DOC])
    assert count == 1

    wrapper = make_connection(pg_dsn)
    row = wrapper.execute(
        "SELECT document_id, krs, rodzaj, status, nazwa FROM krs_documents WHERE document_id = %s",
        ["abc123=="],
    ).fetchone()
    wrapper.close()
    assert row[0] == "abc123=="
    assert row[1] == "0000000001"
    assert row[2] == "SF"
    assert row[3] == "AKTUALNY"


def test_document_store_idempotent(pg_dsn, clean_pg):
    store = RdfDocumentStore(pg_dsn)
    store.insert_documents("0000000001", [_SAMPLE_DOC])
    store.insert_documents("0000000001", [_SAMPLE_DOC])  # no error


def test_document_store_get_undownloaded(pg_dsn, clean_pg):
    store = RdfDocumentStore(pg_dsn)
    store.insert_documents("0000000001", [_SAMPLE_DOC])

    undownloaded = store.get_undownloaded("0000000001")
    assert "abc123==" in undownloaded


def test_document_store_get_undownloaded_excludes_errored(pg_dsn, clean_pg):
    store = RdfDocumentStore(pg_dsn)
    store.insert_documents("0000000001", [_SAMPLE_DOC])
    store.update_error("abc123==", "some error")

    undownloaded = store.get_undownloaded("0000000001")
    assert "abc123==" not in undownloaded


def test_document_store_mark_downloaded(pg_dsn, clean_pg):
    store = RdfDocumentStore(pg_dsn)
    store.insert_documents("0000000001", [_SAMPLE_DOC])
    store.mark_downloaded(
        "abc123==",
        storage_path="krs/0000000001/abc123",
        storage_backend="local",
        file_size=1024,
        zip_size=512,
        file_count=1,
        file_types="xml",
    )

    wrapper = make_connection(pg_dsn)
    row = wrapper.execute(
        "SELECT is_downloaded, storage_path, file_size_bytes FROM krs_documents WHERE document_id = %s",
        ["abc123=="],
    ).fetchone()
    wrapper.close()
    assert row[0] is True
    assert row[1] == "krs/0000000001/abc123"
    assert row[2] == 1024

    # Should no longer appear in undownloaded
    assert "abc123==" not in store.get_undownloaded("0000000001")


@pytest.mark.asyncio
@pytest.mark.usefixtures("_no_sleep")
async def test_download_one_document_skip_metadata(rdf_base, pg_dsn, clean_pg, tmp_path):
    """When skip_metadata=True, no metadata HTTP call is made but ZIP still downloads."""
    storage_dir = str(tmp_path / "documents")
    doc_store = RdfDocumentStore(pg_dsn)
    doc_store.insert_documents("0000000001", [_SAMPLE_DOC])

    from app.scraper.storage import LocalStorage
    storage = LocalStorage(storage_dir)
    stats = RdfWorkerStats()

    zip_bytes = _make_zip("report.xml", b"<sprawozdanie/>")

    with respx.mock:
        # NO metadata mock — if it's called, respx will raise
        respx.post(f"{rdf_base}/dokumenty/tresc").mock(
            return_value=httpx.Response(200, content=zip_bytes)
        )

        async with httpx.AsyncClient(base_url=rdf_base) as client:
            ok = await _download_one_document(
                client, "0000000001", "abc123==",
                doc_store, storage, delay=0, worker_id=0, stats=stats,
                skip_metadata=True,
            )

    assert ok is True
    assert stats.documents_downloaded == 1

    # Verify DB: downloaded but no metadata
    wrapper = make_connection(pg_dsn)
    row = wrapper.execute(
        "SELECT is_downloaded, metadata_fetched_at, file_count FROM krs_documents WHERE document_id = %s",
        ["abc123=="],
    ).fetchone()
    wrapper.close()
    assert row[0] is True
    assert row[1] is None  # metadata was skipped
    assert row[2] >= 1


@pytest.mark.asyncio
async def test_async_save_extracted_local(tmp_path):
    """async_save_extracted delegates to save_extracted in a thread and returns same manifest."""
    from app.scraper.storage import LocalStorage

    storage = LocalStorage(str(tmp_path / "docs"))
    zip_bytes = _make_zip("test.xml", b"<data/>")

    # Sync call
    manifest_sync = storage.save_extracted("krs/0000000001/sync_doc", zip_bytes, "sync_doc")

    # Async call
    manifest_async = await storage.async_save_extracted("krs/0000000001/async_doc", zip_bytes, "async_doc")

    # Same structure, different doc IDs
    assert len(manifest_sync["files"]) == len(manifest_async["files"])
    assert manifest_sync["files"][0]["name"] == manifest_async["files"][0]["name"]
    assert manifest_async["document_id"] == "async_doc"


def test_document_store_update_metadata(pg_dsn, clean_pg):
    store = RdfDocumentStore(pg_dsn)
    store.insert_documents("0000000001", [_SAMPLE_DOC])
    store.update_metadata("abc123==", {
        "nazwaPliku": "sprawozdanie.xml",
        "czyMSR": True,
        "czyKorekta": False,
        "dataDodania": "2024-01-15",
    })

    wrapper = make_connection(pg_dsn)
    row = wrapper.execute(
        "SELECT filename, is_ifrs, metadata_fetched_at FROM krs_documents WHERE document_id = %s",
        ["abc123=="],
    ).fetchone()
    wrapper.close()
    assert row[0] == "sprawozdanie.xml"
    assert row[1] is True
    assert row[2] is not None
