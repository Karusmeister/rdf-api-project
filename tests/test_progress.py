"""Tests for batch/progress.py — DuckDB progress store."""

import pytest
import duckdb

from batch.progress import ProgressStore


@pytest.fixture
def store(tmp_path):
    return ProgressStore(str(tmp_path / "test.duckdb"))


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


def test_resume_persists_across_connections(tmp_path):
    db = str(tmp_path / "test.duckdb")
    s1 = ProgressStore(db)
    s1.mark(100, "found", worker_id=0)
    # Re-open — simulates process restart
    s2 = ProgressStore(db)
    assert s2.is_done(100) is True
    assert s2.is_done(101) is False


def test_batch_progress_table_coexists_with_other_tables(tmp_path):
    """Verify batch_progress schema doesn't conflict with financial data tables."""
    db = str(tmp_path / "test.duckdb")
    # Pre-create a table simulating existing financial schema
    conn = duckdb.connect(db)
    conn.execute("CREATE TABLE companies (krs BIGINT PRIMARY KEY, name VARCHAR)")
    conn.close()
    # ProgressStore should initialise cleanly alongside it
    store = ProgressStore(db)
    store.mark(1, "found", worker_id=0)
    assert store.is_done(1) is True
