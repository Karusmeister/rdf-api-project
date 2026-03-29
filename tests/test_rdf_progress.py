"""Tests for batch/rdf_progress.py — RDF document discovery progress store."""

import multiprocessing

import duckdb
import pytest

from batch.rdf_progress import RdfProgressStore


@pytest.fixture
def store(tmp_path):
    return RdfProgressStore(str(tmp_path / "test.duckdb"))


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


def test_get_pending_krs_returns_unprocessed(tmp_path):
    """Only KRS numbers in batch_progress(status='found') that are NOT in
    batch_rdf_progress should be returned."""
    db = str(tmp_path / "test.duckdb")

    # Set up batch_progress with some found KRS numbers
    conn = duckdb.connect(db)
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

    store = RdfProgressStore(db)

    # Mark KRS 1 as already processed
    store.mark("0000000001", "done", documents_found=5, worker_id=0)

    # Worker 0 with 1 total worker — should get KRS 2 and 4 (not 1 or 3)
    pending = store.get_pending_krs(worker_id=0, total_workers=1)
    assert "0000000002" in pending
    assert "0000000004" in pending
    assert "0000000001" not in pending  # already done
    assert "0000000003" not in pending  # status='not_found'


def test_get_pending_krs_modulo_partition(tmp_path):
    """Workers get disjoint partitions via modulo."""
    db = str(tmp_path / "test.duckdb")

    conn = duckdb.connect(db)
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
            "INSERT INTO batch_progress VALUES (?, 'found', 0, now())", [krs]
        )
    conn.close()

    store = RdfProgressStore(db)
    w0 = store.get_pending_krs(worker_id=0, total_workers=2)
    w1 = store.get_pending_krs(worker_id=1, total_workers=2)

    # Partitions should be disjoint and together cover all 10
    w0_ints = {int(k) for k in w0}
    w1_ints = {int(k) for k in w1}
    assert w0_ints & w1_ints == set()  # disjoint
    assert w0_ints | w1_ints == set(range(1, 11))


def test_resume_persists_across_connections(tmp_path):
    db = str(tmp_path / "test.duckdb")
    s1 = RdfProgressStore(db)
    s1.mark("0000000100", "done", documents_found=3, worker_id=0)
    s2 = RdfProgressStore(db)
    assert s2.is_done("0000000100") is True
    assert s2.is_done("0000000101") is False


def test_coexists_with_batch_progress(tmp_path):
    """batch_rdf_progress and batch_progress live in the same DB file."""
    db = str(tmp_path / "test.duckdb")
    conn = duckdb.connect(db)
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

    store = RdfProgressStore(db)
    store.mark("0000000001", "done", documents_found=5, worker_id=0)
    assert store.is_done("0000000001") is True

    # Verify batch_progress is untouched
    conn = duckdb.connect(db)
    row = conn.execute("SELECT status FROM batch_progress WHERE krs = 1").fetchone()
    conn.close()
    assert row[0] == "found"


def _worker_mark(db_path: str, krs: str, worker_id: int):
    store = RdfProgressStore(db_path)
    store.mark(krs, "done", documents_found=2, worker_id=worker_id)


def test_multi_process_writes(tmp_path):
    db = str(tmp_path / "test.duckdb")
    RdfProgressStore(db)

    p1 = multiprocessing.Process(target=_worker_mark, args=(db, "0000000001", 0))
    p2 = multiprocessing.Process(target=_worker_mark, args=(db, "0000000002", 1))

    p1.start()
    p2.start()
    p1.join(timeout=10)
    p2.join(timeout=10)

    assert p1.exitcode == 0
    assert p2.exitcode == 0

    store = RdfProgressStore(db)
    assert store.is_done("0000000001") is True
    assert store.is_done("0000000002") is True
