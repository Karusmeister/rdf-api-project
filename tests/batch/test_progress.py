"""Tests for batch/progress.py — PostgreSQL progress store."""

import multiprocessing
import pytest

from app.db.connection import make_connection
from batch.progress import ProgressStore


@pytest.fixture
def store(pg_dsn, clean_pg):
    return ProgressStore(pg_dsn)


def test_is_done_returns_false_for_new_krs(store):
    assert store.is_done(12345) is False


def test_mark_and_is_done(store):
    store.mark(12345, "found", worker_id=0)
    assert store.is_done(12345) is True


def test_mark_idempotent(store):
    store.mark(1, "found", worker_id=0)
    store.mark(1, "error", worker_id=1)  # ON CONFLICT replace — should not raise
    assert store.is_done(1) is True


def test_summary_counts_by_status(store):
    store.mark(1, "found", worker_id=0)
    store.mark(2, "found", worker_id=0)
    store.mark(3, "not_found", worker_id=1)
    summary = store.summary()
    assert summary["found"] == 2
    assert summary["not_found"] == 1


@pytest.mark.usefixtures("clean_pg")
def test_resume_persists_across_connections(pg_dsn):
    s1 = ProgressStore(pg_dsn)
    s1.mark(100, "found", worker_id=0)
    # Re-open — simulates process restart
    s2 = ProgressStore(pg_dsn)
    assert s2.is_done(100) is True
    assert s2.is_done(101) is False


@pytest.mark.usefixtures("clean_pg")
def test_batch_progress_table_coexists_with_other_tables(pg_dsn):
    """Verify batch_progress schema doesn't conflict with financial data tables."""
    # Pre-create a table simulating existing financial schema
    conn = make_connection(pg_dsn)
    conn.execute("CREATE TABLE IF NOT EXISTS companies (krs BIGINT PRIMARY KEY, name VARCHAR)")
    conn.close()
    # ProgressStore should initialise cleanly alongside it
    store = ProgressStore(pg_dsn)
    store.mark(1, "found", worker_id=0)
    assert store.is_done(1) is True


def _worker_mark(dsn: str, krs: int, worker_id: int):
    """Helper for multiprocessing test — runs in a child process."""
    store = ProgressStore(dsn)
    store.mark(krs, "found", worker_id)


@pytest.mark.usefixtures("clean_pg")
def test_multi_process_writes_do_not_deadlock(pg_dsn):
    """Two separate OS processes can write to the same DB without lock errors.

    This was the core bug in finding #1 — persistent database connections
    held exclusive locks, preventing multi-worker operation.
    """
    # Ensure schema exists before spawning
    ProgressStore(pg_dsn)

    p1 = multiprocessing.Process(target=_worker_mark, args=(pg_dsn, 1, 0))
    p2 = multiprocessing.Process(target=_worker_mark, args=(pg_dsn, 2, 1))

    p1.start()
    p2.start()
    p1.join(timeout=10)
    p2.join(timeout=10)

    assert p1.exitcode == 0, f"Worker 0 crashed with exit code {p1.exitcode}"
    assert p2.exitcode == 0, f"Worker 1 crashed with exit code {p2.exitcode}"

    # Both writes visible
    store = ProgressStore(pg_dsn)
    assert store.is_done(1) is True
    assert store.is_done(2) is True
