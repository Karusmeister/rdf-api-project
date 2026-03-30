"""Tests for batch/rdf_progress.py — RDF document discovery progress store."""

import multiprocessing

import pytest

from app.db.connection import make_connection
from batch.rdf_progress import RdfProgressStore


@pytest.fixture
def store(pg_dsn, clean_pg):
    return RdfProgressStore(pg_dsn)


def test_is_done_returns_false_for_new_krs(store):
    assert store.is_done("0000000001") is False


def test_mark_and_is_done(store):
    store.mark("0000000001", "done", documents_found=5, worker_id=0)
    assert store.is_done("0000000001") is True


def test_mark_idempotent_updates_status(store):
    store.mark("0000000001", "error", documents_found=0, worker_id=0)
    store.mark("0000000001", "done", documents_found=3, worker_id=1)
    assert store.is_done("0000000001") is True


def test_summary_counts_by_status(store):
    store.mark("0000000001", "done", documents_found=5, worker_id=0)
    store.mark("0000000002", "done", documents_found=3, worker_id=0)
    store.mark("0000000003", "empty", documents_found=0, worker_id=1)
    store.mark("0000000004", "error", documents_found=0, worker_id=1)
    summary = store.summary()
    assert summary["done"]["count"] == 2
    assert summary["done"]["documents"] == 8
    assert summary["empty"]["count"] == 1
    assert summary["error"]["count"] == 1


@pytest.mark.usefixtures("clean_pg")
def test_get_pending_krs_returns_unprocessed(pg_dsn):
    """Only KRS numbers in batch_progress(status='found') that are NOT in
    batch_rdf_progress should be returned."""

    # Set up batch_progress with some found KRS numbers
    conn = make_connection(pg_dsn)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS batch_progress (
            krs BIGINT PRIMARY KEY,
            status VARCHAR NOT NULL,
            worker_id INTEGER,
            processed_at TIMESTAMP DEFAULT now()
        )
    """)
    conn.execute("INSERT INTO batch_progress VALUES (1, 'found', 0, now())")
    conn.execute("INSERT INTO batch_progress VALUES (2, 'found', 0, now())")
    conn.execute("INSERT INTO batch_progress VALUES (3, 'not_found', 0, now())")
    conn.execute("INSERT INTO batch_progress VALUES (4, 'found', 1, now())")
    conn.close()

    store = RdfProgressStore(pg_dsn)

    # Mark KRS 1 as already processed
    store.mark("0000000001", "done", documents_found=5, worker_id=0)

    # Worker 0 with 1 total worker — should get KRS 2 and 4 (not 1 or 3)
    pending = store.get_pending_krs(worker_id=0, total_workers=1)
    assert "0000000002" in pending
    assert "0000000004" in pending
    assert "0000000001" not in pending  # already done
    assert "0000000003" not in pending  # status='not_found'


@pytest.mark.usefixtures("clean_pg")
def test_get_pending_krs_modulo_partition(pg_dsn):
    """Workers get disjoint partitions via modulo."""

    conn = make_connection(pg_dsn)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS batch_progress (
            krs BIGINT PRIMARY KEY,
            status VARCHAR NOT NULL,
            worker_id INTEGER,
            processed_at TIMESTAMP DEFAULT now()
        )
    """)
    for krs in range(1, 11):
        conn.execute(
            "INSERT INTO batch_progress VALUES (%s, 'found', 0, now())", [krs]
        )
    conn.close()

    store = RdfProgressStore(pg_dsn)
    w0 = store.get_pending_krs(worker_id=0, total_workers=2)
    w1 = store.get_pending_krs(worker_id=1, total_workers=2)

    # Partitions should be disjoint and together cover all 10
    w0_ints = {int(k) for k in w0}
    w1_ints = {int(k) for k in w1}
    assert w0_ints & w1_ints == set()  # disjoint
    assert w0_ints | w1_ints == set(range(1, 11))


@pytest.mark.usefixtures("clean_pg")
def test_resume_persists_across_connections(pg_dsn):
    s1 = RdfProgressStore(pg_dsn)
    s1.mark("0000000100", "done", documents_found=3, worker_id=0)
    s2 = RdfProgressStore(pg_dsn)
    assert s2.is_done("0000000100") is True
    assert s2.is_done("0000000101") is False


@pytest.mark.usefixtures("clean_pg")
def test_coexists_with_batch_progress(pg_dsn):
    """batch_rdf_progress and batch_progress live in the same database."""
    conn = make_connection(pg_dsn)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS batch_progress (
            krs BIGINT PRIMARY KEY,
            status VARCHAR NOT NULL,
            worker_id INTEGER,
            processed_at TIMESTAMP DEFAULT now()
        )
    """)
    conn.execute("INSERT INTO batch_progress VALUES (1, 'found', 0, now())")
    conn.close()

    store = RdfProgressStore(pg_dsn)
    store.mark("0000000001", "done", documents_found=5, worker_id=0)
    assert store.is_done("0000000001") is True

    # Verify batch_progress is untouched
    conn = make_connection(pg_dsn)
    row = conn.execute("SELECT status FROM batch_progress WHERE krs = 1").fetchone()
    conn.close()
    assert row[0] == "found"


def _worker_mark(dsn: str, krs: str, worker_id: int):
    store = RdfProgressStore(dsn)
    store.mark(krs, "done", documents_found=2, worker_id=worker_id)


@pytest.mark.usefixtures("clean_pg")
def test_multi_process_writes(pg_dsn):
    RdfProgressStore(pg_dsn)

    p1 = multiprocessing.Process(target=_worker_mark, args=(pg_dsn, "0000000001", 0))
    p2 = multiprocessing.Process(target=_worker_mark, args=(pg_dsn, "0000000002", 1))

    p1.start()
    p2.start()
    p1.join(timeout=10)
    p2.join(timeout=10)

    assert p1.exitcode == 0
    assert p2.exitcode == 0

    store = RdfProgressStore(pg_dsn)
    assert store.is_done("0000000001") is True
    assert store.is_done("0000000002") is True
