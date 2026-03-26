"""End-to-end batch scanner tests against the live RDF API.

These tests hit rdf-przegladarka.ms.gov.pl with real KRS numbers and
write results to an isolated DuckDB in tmp_path — never the main DB.

Run with:  pytest tests/test_batch_e2e.py -v -s --e2e
Skip with: pytest tests/ -v  (skipped by default)

Known test KRS numbers:
  0000694720 — exists (used in other e2e tests)
  0000006865 — exists (PKN Orlen, large company)
  0000019193 — exists (PZU SA)
  0000009999 — likely does not exist
  9999999999 — does not exist
"""

import asyncio
import duckdb

import httpx
import pytest

from app.config import settings
from app.crypto import encrypt_nrkrs
from batch.connections import Connection
from batch.progress import ProgressStore
from batch.worker import (
    _make_client,
    _process_krs_with_backoff,
    _worker_loop,
)

pytestmark = pytest.mark.e2e

DIRECT_CONN = Connection(name="direct")
RDF_BASE = settings.rdf_base_url

# Well-known Polish companies that must exist in KRS
KNOWN_EXISTING = [
    ("0000694720", "Expected existing entity"),
    ("0000006865", "PKN Orlen"),
    ("0000019193", "PZU SA"),
]
KNOWN_MISSING = "9999999999"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

@pytest.fixture
def db_path(tmp_path):
    return str(tmp_path / "e2e_batch.duckdb")


# ---------------------------------------------------------------------------
# 1. Single KRS lookup — known existing entity
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
@pytest.mark.parametrize("krs,label", KNOWN_EXISTING)
async def test_live_lookup_existing_entity(krs, label, db_path):
    """Hit the real RDF API for a known KRS and verify 'found'."""
    async with httpx.AsyncClient(
        base_url=RDF_BASE,
        headers={
            "Accept": "application/json",
            "Content-Type": "application/json",
            "User-Agent": "Mozilla/5.0",
            "Referer": settings.rdf_referer,
            "Origin": settings.rdf_origin,
        },
        timeout=30,
        follow_redirects=True,
    ) as client:
        result = await _process_krs_with_backoff(client, krs, worker_id=0)

    assert result == "found", f"Expected 'found' for KRS {krs} ({label}), got '{result}'"

    # Write to isolated DB and verify
    store = ProgressStore(db_path)
    store.mark(int(krs), result, worker_id=0)

    conn = duckdb.connect(db_path)
    row = conn.execute(
        "SELECT krs, status FROM batch_progress WHERE krs = ?", [int(krs)]
    ).fetchone()
    conn.close()

    print(f"\n  KRS {krs} ({label}): status={row[1]}")
    assert row[1] == "found"


# ---------------------------------------------------------------------------
# 2. Single KRS lookup — known non-existing entity
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_live_lookup_nonexistent_entity(db_path):
    """Hit the real RDF API for a KRS that doesn't exist → 'not_found'."""
    krs = KNOWN_MISSING

    async with httpx.AsyncClient(
        base_url=RDF_BASE,
        headers={
            "Accept": "application/json",
            "Content-Type": "application/json",
            "User-Agent": "Mozilla/5.0",
            "Referer": settings.rdf_referer,
            "Origin": settings.rdf_origin,
        },
        timeout=30,
        follow_redirects=True,
    ) as client:
        result = await _process_krs_with_backoff(client, krs, worker_id=0)

    assert result == "not_found", f"Expected 'not_found' for KRS {krs}, got '{result}'"

    store = ProgressStore(db_path)
    store.mark(int(krs), result, worker_id=0)

    conn = duckdb.connect(db_path)
    row = conn.execute(
        "SELECT status FROM batch_progress WHERE krs = ?", [int(krs)]
    ).fetchone()
    conn.close()

    print(f"\n  KRS {krs}: status={row[0]}")
    assert row[0] == "not_found"


# ---------------------------------------------------------------------------
# 3. Worker loop processes a small live range
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_live_worker_loop_small_range(db_path):
    """Run the worker loop against live API for KRS 1-5 and inspect results.

    We don't know which of 1-5 exist, but the loop must not crash and
    every probed KRS must end up in the progress DB with a valid status.
    """
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(
            _worker_loop(
                worker_id=0,
                start_krs=1,
                stride=1,
                connection=DIRECT_CONN,
                concurrency=1,
                delay=1.0,  # polite — 1s between requests to live API
                db_path=db_path,
            ),
            timeout=15.0,
        )

    store = ProgressStore(db_path)
    summary = store.summary()
    total = sum(summary.values())

    print(f"\n  Processed {total} KRS numbers in 15s")
    print(f"  Summary: {summary}")

    assert total >= 2, f"Expected at least 2 processed, got {total}"

    # Every entry must have a valid status
    conn = duckdb.connect(db_path)
    rows = conn.execute(
        "SELECT krs, status, worker_id, processed_at FROM batch_progress ORDER BY krs"
    ).fetchall()
    conn.close()

    print(f"\n  {'KRS':>10}  {'status':>10}  {'worker':>6}  processed_at")
    print("  " + "-" * 60)
    for r in rows:
        print(f"  {r[0]:>10}  {r[1]:>10}  {r[2]:>6}  {r[3]}")
        assert r[1] in ("found", "not_found", "error"), f"Invalid status: {r[1]}"
        assert r[2] == 0, f"Wrong worker_id: {r[2]}"


# ---------------------------------------------------------------------------
# 4. Worker loop with stride — two sequential runs cover disjoint ranges
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_live_stride_partitioning(db_path):
    """Simulate 2 workers (stride=2) against live API, verify disjoint coverage."""
    for worker_id, start in [(0, 1), (1, 2)]:
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(
                _worker_loop(
                    worker_id=worker_id,
                    start_krs=start,
                    stride=2,
                    connection=DIRECT_CONN,
                    concurrency=1,
                    delay=1.0,
                    db_path=db_path,
                ),
                timeout=8.0,
            )

    conn = duckdb.connect(db_path)
    w0 = {r[0] for r in conn.execute(
        "SELECT krs FROM batch_progress WHERE worker_id = 0"
    ).fetchall()}
    w1 = {r[0] for r in conn.execute(
        "SELECT krs FROM batch_progress WHERE worker_id = 1"
    ).fetchall()}
    total = conn.execute("SELECT COUNT(*) FROM batch_progress").fetchone()[0]
    conn.close()

    print(f"\n  Worker 0 processed: {sorted(w0)}")
    print(f"  Worker 1 processed: {sorted(w1)}")
    print(f"  Total: {total}")

    assert w0.isdisjoint(w1), f"Overlap detected: {w0 & w1}"
    assert all(k % 2 == 1 for k in w0), f"Worker 0 should have odds: {w0}"
    assert all(k % 2 == 0 for k in w1), f"Worker 1 should have evens: {w1}"


# ---------------------------------------------------------------------------
# 5. Resume — pre-mark entries, verify worker skips them
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_live_resume_skips_done(db_path):
    """Pre-mark KRS 1-3 as done, run worker from 1 — should skip to 4+."""
    store = ProgressStore(db_path)
    for krs in (1, 2, 3):
        store.mark(krs, "found", worker_id=99)

    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(
            _worker_loop(
                worker_id=0,
                start_krs=1,
                stride=1,
                connection=DIRECT_CONN,
                concurrency=1,
                delay=1.0,
                db_path=db_path,
            ),
            timeout=10.0,
        )

    conn = duckdb.connect(db_path)
    # KRS 1-3 should still have worker_id=99 (pre-marked, not overwritten)
    premarked = conn.execute(
        "SELECT krs, worker_id FROM batch_progress WHERE krs <= 3 ORDER BY krs"
    ).fetchall()
    # KRS 4+ should have worker_id=0 (processed by the test worker)
    new_rows = conn.execute(
        "SELECT krs, status, worker_id FROM batch_progress WHERE krs > 3 ORDER BY krs"
    ).fetchall()
    conn.close()

    print("\n  Pre-marked (should be worker_id=99):")
    for r in premarked:
        print(f"    KRS {r[0]}: worker_id={r[1]}")
        assert r[1] == 99, f"KRS {r[0]} was re-processed (worker_id={r[1]})"

    print("  New entries (should be worker_id=0):")
    for r in new_rows:
        print(f"    KRS {r[0]}: status={r[1]}, worker_id={r[2]}")

    assert len(new_rows) >= 1, "Worker didn't process any new KRS numbers"
    assert all(r[2] == 0 for r in new_rows), "New entries should have worker_id=0"


# ---------------------------------------------------------------------------
# 6. Known entity — verify dane-podstawowe + wyszukiwanie both succeed
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_live_full_pipeline_known_entity():
    """Full pipeline for KRS 0000694720: dane-podstawowe + document search."""
    krs = "0000694720"

    async with httpx.AsyncClient(
        base_url=RDF_BASE,
        headers={
            "Accept": "application/json",
            "Content-Type": "application/json",
            "User-Agent": "Mozilla/5.0",
            "Referer": settings.rdf_referer,
            "Origin": settings.rdf_origin,
        },
        timeout=30,
        follow_redirects=True,
    ) as client:
        # Step 1: dane-podstawowe
        resp1 = await client.post(
            "/podmioty/wyszukiwanie/dane-podstawowe",
            json={"numerKRS": krs},
        )
        resp1.raise_for_status()
        data = resp1.json()
        print(f"\n  dane-podstawowe response: {data}")
        assert data, "Empty response from dane-podstawowe"

        # Step 2: wyszukiwanie (encrypted KRS)
        encrypted = encrypt_nrkrs(krs.lstrip("0"))
        resp2 = await client.post(
            "/dokumenty/wyszukiwanie",
            json={
                "metadaneStronicowania": {
                    "numerStrony": 0,
                    "rozmiarStrony": 5,
                    "metadaneSortowania": [{"atrybut": "id", "kierunek": "MALEJACO"}],
                },
                "nrKRS": encrypted,
            },
        )
        resp2.raise_for_status()
        docs = resp2.json()
        print(f"  wyszukiwanie response keys: {list(docs.keys()) if isinstance(docs, dict) else type(docs)}")

        if isinstance(docs, dict) and "content" in docs:
            print(f"  Documents found: {len(docs['content'])}")
            for d in docs["content"][:3]:
                print(f"    id={d.get('id', '?')[:30]}  rodzaj={d.get('rodzaj')}  status={d.get('status')}")
