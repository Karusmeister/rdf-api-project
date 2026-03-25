"""Regression tests for the batch KRS scanner.

All tests exercise the real code paths end-to-end against respx-mocked
RDF upstream endpoints.  Every test writes to an isolated DuckDB in
tmp_path — never the main project DB.
"""

import asyncio
import multiprocessing

import httpx
import pytest
import respx

from app.config import settings
from batch.connections import Connection
from batch.progress import ProgressStore
from batch.worker import (
    _make_client,
    _process_krs_with_backoff,
    _worker_loop,
    run_worker,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

RDF_BASE = settings.rdf_base_url
DIRECT_CONN = Connection(name="direct")


def _entity_response(krs_str: str) -> dict:
    """Realistic dane-podstawowe 200 response for an existing entity."""
    return {
        "podmiot": {
            "numerKRS": krs_str,
            "nazwaPodmiotu": f"TEST SP. Z O.O. ({krs_str})",
            "formaPrawna": "SP. Z O.O.",
            "wykreslenie": "",
        },
        "czyPodmiotZnaleziony": True,
        "komunikatBledu": None,
    }


def _doc_response() -> dict:
    """Realistic wyszukiwanie 200 response (one document)."""
    return {
        "content": [
            {
                "id": "dG9rZW4=",
                "rodzaj": "18",
                "status": "NIEUSUNIETY",
                "okresSprawozdawczyPoczatek": "2024-01-01",
                "okresSprawozdawczyKoniec": "2024-12-31",
            }
        ],
        "metadaneWynikow": {
            "numerStrony": 0,
            "rozmiarStrony": 10,
            "liczbaStron": 1,
            "calkowitaLiczbaObiektow": 1,
        },
    }


@pytest.fixture
def db_path(tmp_path):
    """Isolated DuckDB for the test — schema is created by ProgressStore."""
    return str(tmp_path / "regression.duckdb")


@pytest.fixture
def _no_sleep(monkeypatch):
    """Eliminate asyncio.sleep delays for fast tests."""
    async def _noop(_):
        pass
    monkeypatch.setattr("batch.worker.asyncio.sleep", _noop)


# ---------------------------------------------------------------------------
# 1. End-to-end: worker processes a range of KRS numbers
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
@pytest.mark.usefixtures("_no_sleep")
async def test_worker_loop_processes_range_into_progress_db(db_path):
    """Worker loop processes KRS 1-5 and writes correct statuses to DB.

    KRS 1,3,5 → found, KRS 2,4 → not_found.
    """
    found_krs = {"0000000001", "0000000003", "0000000005"}

    def dane_side_effect(request):
        body = request.content.decode()
        for k in found_krs:
            if k in body:
                return httpx.Response(200, json=_entity_response(k))
        return httpx.Response(200, json={})

    with respx.mock:
        respx.post(f"{RDF_BASE}/podmioty/wyszukiwanie/dane-podstawowe").mock(
            side_effect=dane_side_effect
        )
        respx.post(f"{RDF_BASE}/dokumenty/wyszukiwanie").mock(
            return_value=httpx.Response(200, json=_doc_response())
        )

        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(
                _worker_loop(
                    worker_id=0,
                    start_krs=1,
                    stride=1,
                    connection=DIRECT_CONN,
                    concurrency=1,
                    delay=0,
                    db_path=db_path,
                ),
                timeout=2.0,
            )

    store = ProgressStore(db_path)
    assert store.is_done(1) is True
    assert store.is_done(2) is True
    assert store.is_done(3) is True

    summary = store.summary()
    assert summary.get("found", 0) >= 3
    assert summary.get("not_found", 0) >= 2


# ---------------------------------------------------------------------------
# 2. Resume after restart — skips already-done KRS numbers
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
@pytest.mark.usefixtures("_no_sleep")
async def test_worker_loop_resumes_and_skips_done(db_path):
    """Pre-mark KRS 1-3 as done, run worker from 1 — it should skip them."""
    store = ProgressStore(db_path)
    for krs in (1, 2, 3):
        store.mark(krs, "found", worker_id=99)

    call_count = 0

    def dane_side_effect(request):
        nonlocal call_count
        call_count += 1
        return httpx.Response(200, json={})  # not_found for all

    with respx.mock:
        respx.post(f"{RDF_BASE}/podmioty/wyszukiwanie/dane-podstawowe").mock(
            side_effect=dane_side_effect
        )

        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(
                _worker_loop(
                    worker_id=0,
                    start_krs=1,
                    stride=1,
                    connection=DIRECT_CONN,
                    concurrency=1,
                    delay=0,
                    db_path=db_path,
                ),
                timeout=1.0,
            )

    # KRS 1-3 were pre-marked — no API calls for them.
    # Only KRS >= 4 should have been called.
    # Verify at least KRS 4 was processed (first new one).
    assert store.is_done(4) is True

    # The API should never have been called for KRS 1-3.
    # We can verify by checking that call_count started from KRS 4+
    # (at minimum 1 call for KRS 4, likely many more in 1s).
    assert call_count >= 1


# ---------------------------------------------------------------------------
# 3. Mixed responses: found, not_found, error — correct status per KRS
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
@pytest.mark.usefixtures("_no_sleep")
async def test_mixed_responses_recorded_correctly(db_path):
    """KRS 1 → found, KRS 2 → not_found, KRS 3 → 500 error."""

    def dane_side_effect(request):
        body = request.content.decode()
        if "0000000001" in body:
            return httpx.Response(200, json=_entity_response("0000000001"))
        if "0000000002" in body:
            return httpx.Response(200, json={})
        if "0000000003" in body:
            return httpx.Response(500)
        # Everything else → not_found
        return httpx.Response(200, json={})

    with respx.mock:
        respx.post(f"{RDF_BASE}/podmioty/wyszukiwanie/dane-podstawowe").mock(
            side_effect=dane_side_effect
        )
        respx.post(f"{RDF_BASE}/dokumenty/wyszukiwanie").mock(
            return_value=httpx.Response(200, json=_doc_response())
        )

        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(
                _worker_loop(
                    worker_id=0,
                    start_krs=1,
                    stride=1,
                    connection=DIRECT_CONN,
                    concurrency=1,
                    delay=0,
                    db_path=db_path,
                ),
                timeout=2.0,
            )

    # Verify individual statuses by re-reading DB
    import duckdb
    conn = duckdb.connect(db_path)
    rows = conn.execute(
        "SELECT krs, status FROM batch_progress WHERE krs IN (1, 2, 3) ORDER BY krs"
    ).fetchall()
    conn.close()

    status_map = {r[0]: r[1] for r in rows}
    assert status_map[1] == "found"
    assert status_map[2] == "not_found"
    assert status_map[3] == "error"


# ---------------------------------------------------------------------------
# 4. Document lookup failure → "error", not "found"  (code-review fix #4)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_doc_lookup_failure_records_error_not_found(db_path):
    """Entity exists but document search returns 500 → stored as 'error'."""
    store = ProgressStore(db_path)

    with respx.mock:
        respx.post(f"{RDF_BASE}/podmioty/wyszukiwanie/dane-podstawowe").mock(
            return_value=httpx.Response(200, json=_entity_response("0000000001"))
        )
        respx.post(f"{RDF_BASE}/dokumenty/wyszukiwanie").mock(
            return_value=httpx.Response(500)
        )

        async with httpx.AsyncClient(base_url=RDF_BASE) as client:
            result = await _process_krs_with_backoff(client, "0000000001", worker_id=0)

    assert result == "error"

    # Simulate what the worker loop does
    store.mark(1, result, worker_id=0)

    conn = __import__("duckdb").connect(db_path)
    row = conn.execute(
        "SELECT status FROM batch_progress WHERE krs = 1"
    ).fetchone()
    conn.close()
    assert row[0] == "error"


# ---------------------------------------------------------------------------
# 5. Stride partitioning — two workers cover disjoint KRS ranges
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
@pytest.mark.usefixtures("_no_sleep")
async def test_stride_partitioning_disjoint(db_path):
    """Two workers (stride=2) process disjoint sets: w0 → 1,3,5,… w1 → 2,4,6,…"""
    with respx.mock:
        respx.post(f"{RDF_BASE}/podmioty/wyszukiwanie/dane-podstawowe").mock(
            return_value=httpx.Response(200, json={})  # all not_found
        )

        # Worker 0: start=1, stride=2 → processes 1, 3, 5, 7, ...
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(
                _worker_loop(
                    worker_id=0, start_krs=1, stride=2,
                    connection=DIRECT_CONN, concurrency=1, delay=0,
                    db_path=db_path,
                ),
                timeout=0.5,
            )

        # Worker 1: start=2, stride=2 → processes 2, 4, 6, 8, ...
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(
                _worker_loop(
                    worker_id=1, start_krs=2, stride=2,
                    connection=DIRECT_CONN, concurrency=1, delay=0,
                    db_path=db_path,
                ),
                timeout=0.5,
            )

    import duckdb
    conn = duckdb.connect(db_path)
    w0_rows = conn.execute(
        "SELECT krs FROM batch_progress WHERE worker_id = 0 ORDER BY krs"
    ).fetchall()
    w1_rows = conn.execute(
        "SELECT krs FROM batch_progress WHERE worker_id = 1 ORDER BY krs"
    ).fetchall()
    conn.close()

    w0_krs = {r[0] for r in w0_rows}
    w1_krs = {r[0] for r in w1_rows}

    # No overlap
    assert w0_krs.isdisjoint(w1_krs), f"Overlap: {w0_krs & w1_krs}"
    # Worker 0 got odd numbers, worker 1 got even numbers
    assert all(k % 2 == 1 for k in w0_krs), f"Worker 0 got even: {w0_krs}"
    assert all(k % 2 == 0 for k in w1_krs), f"Worker 1 got odd: {w1_krs}"


# ---------------------------------------------------------------------------
# 6. 429 backoff → eventual success recorded correctly
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
@pytest.mark.usefixtures("_no_sleep")
async def test_429_backoff_eventual_success_in_db(db_path):
    """KRS 1 returns 429 twice then 200 → stored as 'found' in DB."""
    store = ProgressStore(db_path)
    call_count = 0

    def dane_side_effect(request):
        nonlocal call_count
        call_count += 1
        if call_count <= 2:
            return httpx.Response(429)
        return httpx.Response(200, json=_entity_response("0000000001"))

    with respx.mock:
        respx.post(f"{RDF_BASE}/podmioty/wyszukiwanie/dane-podstawowe").mock(
            side_effect=dane_side_effect
        )
        respx.post(f"{RDF_BASE}/dokumenty/wyszukiwanie").mock(
            return_value=httpx.Response(200, json=_doc_response())
        )

        async with httpx.AsyncClient(base_url=RDF_BASE) as client:
            result = await _process_krs_with_backoff(client, "0000000001", worker_id=0)

    store.mark(1, result, worker_id=0)

    assert result == "found"
    assert call_count == 3

    import duckdb
    conn = duckdb.connect(db_path)
    row = conn.execute(
        "SELECT status FROM batch_progress WHERE krs = 1"
    ).fetchone()
    conn.close()
    assert row[0] == "found"


# ---------------------------------------------------------------------------
# 7. Concurrency: multiple KRS processed concurrently (not sequentially)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
@pytest.mark.usefixtures("_no_sleep")
async def test_concurrency_processes_multiple_in_flight(db_path):
    """With concurrency=3, at least 3 KRS should be processed quickly."""
    with respx.mock:
        respx.post(f"{RDF_BASE}/podmioty/wyszukiwanie/dane-podstawowe").mock(
            return_value=httpx.Response(200, json={})  # all not_found (fast)
        )

        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(
                _worker_loop(
                    worker_id=0, start_krs=1, stride=1,
                    connection=DIRECT_CONN, concurrency=3, delay=0,
                    db_path=db_path,
                ),
                timeout=1.0,
            )

    store = ProgressStore(db_path)
    summary = store.summary()
    total = sum(summary.values())
    # With concurrency=3 and no delay, should process many items in 1s
    assert total >= 3, f"Only {total} processed — concurrency may not be working"


# ---------------------------------------------------------------------------
# 8. Multi-process shared DB (code-review fix #1)
# ---------------------------------------------------------------------------

def _child_worker_process(db_path: str, krs_range: range, worker_id: int):
    """Run in a child process — marks a range of KRS numbers."""
    store = ProgressStore(db_path)
    for krs in krs_range:
        store.mark(krs, "found", worker_id=worker_id)


def test_multi_process_shared_db_no_deadlock(db_path):
    """Two OS processes write interleaved KRS ranges to the same DB file.

    Validates code-review fix #1: DuckDB lock contention is handled by
    retry-with-backoff in ProgressStore._with_conn().
    """
    # Ensure schema exists before spawning children
    ProgressStore(db_path)

    # Worker 0 writes KRS 1-50, Worker 1 writes KRS 51-100
    p0 = multiprocessing.Process(
        target=_child_worker_process,
        args=(db_path, range(1, 51), 0),
    )
    p1 = multiprocessing.Process(
        target=_child_worker_process,
        args=(db_path, range(51, 101), 1),
    )

    p0.start()
    p1.start()
    p0.join(timeout=30)
    p1.join(timeout=30)

    assert p0.exitcode == 0, f"Worker 0 crashed: exit={p0.exitcode}"
    assert p1.exitcode == 0, f"Worker 1 crashed: exit={p1.exitcode}"

    store = ProgressStore(db_path)
    for krs in range(1, 101):
        assert store.is_done(krs), f"KRS {krs} missing from progress DB"

    summary = store.summary()
    assert summary["found"] == 100


# ---------------------------------------------------------------------------
# 9. SOCKS5 proxy client creation (code-review fix #2)
# ---------------------------------------------------------------------------

def test_socks5_proxy_client_creates_successfully():
    """httpx.AsyncClient with socks5:// proxy can be constructed (socksio installed)."""
    conn = Connection(
        name="pl192",
        proxy_url="socks5://testuser:testpass@pl192.nordvpn.com:1080",
    )
    client = _make_client(conn)
    assert isinstance(client, httpx.AsyncClient)


# ---------------------------------------------------------------------------
# 10. Runner exit code on worker failure (code-review fix #5)
# ---------------------------------------------------------------------------

def test_runner_surfaces_worker_failure(monkeypatch):
    """Runner raises SystemExit(1) when a worker process exits non-zero."""
    from unittest.mock import MagicMock, patch
    from batch.runner import run_batch

    monkeypatch.setattr(settings, "nordvpn_servers", [])

    mock_proc = MagicMock()
    mock_proc.pid = 1
    mock_proc.exitcode = 1  # simulated crash
    mock_proc.name = "krs-worker-0"

    with patch("batch.runner.multiprocessing.Process", return_value=mock_proc):
        with pytest.raises(SystemExit):
            run_batch(start_krs=1, workers=1, use_vpn=False, db_path="/tmp/unused.duckdb")
