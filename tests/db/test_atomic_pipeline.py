"""Integration tests for atomic_start_pipeline (PKR-85 concurrency safety).

Requires a real PostgreSQL database. Skipped when PG is unavailable.
"""

from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch

import pytest

from app.config import settings
from app.db import connection as db_conn
from app.db import prediction_db


@pytest.fixture(autouse=True)
def isolated_db(pg_dsn, clean_pg):
    """Override DB URL to test PostgreSQL and reset the shared connection."""
    db_conn.reset()
    prediction_db._schema_initialized = False
    with patch.object(settings, "database_url", pg_dsn):
        db_conn.connect()
        prediction_db._ensure_schema()
        yield
        db_conn.close()
        db_conn.reset()
        prediction_db._schema_initialized = False


class TestAtomicStartPipelineIntegration:
    """Real DB tests for advisory-lock-based atomic pipeline start."""

    def test_creates_new_job(self):
        result = prediction_db.atomic_start_pipeline("0000694720", 5)
        assert result["outcome"] == "created"
        assert "job_id" in result

        # Verify job exists in DB
        conn = db_conn.get_conn()
        row = conn.execute(
            "SELECT status FROM assessment_jobs WHERE id = %s",
            [result["job_id"]],
        ).fetchone()
        assert row is not None
        assert row[0] == "pending"

    def test_dedup_returns_existing_for_same_krs(self):
        first = prediction_db.atomic_start_pipeline("0000694720", 5)
        assert first["outcome"] == "created"

        second = prediction_db.atomic_start_pipeline("0000694720", 5)
        assert second["outcome"] == "existing"
        assert second["job_id"] == first["job_id"]

    def test_different_krs_creates_separate_jobs(self):
        r1 = prediction_db.atomic_start_pipeline("0000000001", 5)
        r2 = prediction_db.atomic_start_pipeline("0000000002", 5)
        assert r1["outcome"] == "created"
        assert r2["outcome"] == "created"
        assert r1["job_id"] != r2["job_id"]

    def test_max_concurrent_enforced(self):
        # Create 3 jobs (max_concurrent=3)
        for i in range(1, 4):
            result = prediction_db.atomic_start_pipeline(f"000000000{i}", 3)
            assert result["outcome"] == "created"

        # 4th should be rejected
        result = prediction_db.atomic_start_pipeline("0000000004", 3)
        assert result["outcome"] == "rejected"

    def test_concurrent_starts_for_same_krs_produce_one_job(self):
        """Parallel trigger calls for the same KRS must produce exactly one
        job and the rest must attach to it ('existing' outcome)."""
        results = []

        def trigger():
            return prediction_db.atomic_start_pipeline("0000099999", 10)

        with ThreadPoolExecutor(max_workers=5) as pool:
            futures = [pool.submit(trigger) for _ in range(5)]
            results = [f.result() for f in futures]

        created = [r for r in results if r["outcome"] == "created"]
        existing = [r for r in results if r["outcome"] == "existing"]

        assert len(created) == 1, f"Expected exactly 1 created, got {len(created)}"
        assert len(existing) == 4, f"Expected 4 existing, got {len(existing)}"
        # All should reference the same job
        job_ids = {r["job_id"] for r in results}
        assert len(job_ids) == 1

    def test_concurrent_starts_respect_max_limit(self):
        """Parallel triggers for distinct KRS values must not exceed
        max_concurrent."""
        results = []

        def trigger(krs):
            return prediction_db.atomic_start_pipeline(krs, 3)

        krs_list = [f"000000{i:04d}" for i in range(10)]

        with ThreadPoolExecutor(max_workers=10) as pool:
            futures = [pool.submit(trigger, krs) for krs in krs_list]
            results = [f.result() for f in futures]

        created = [r for r in results if r["outcome"] == "created"]
        rejected = [r for r in results if r["outcome"] == "rejected"]

        assert len(created) == 3, f"Expected 3 created (max_concurrent=3), got {len(created)}"
        assert len(rejected) == 7, f"Expected 7 rejected, got {len(rejected)}"
