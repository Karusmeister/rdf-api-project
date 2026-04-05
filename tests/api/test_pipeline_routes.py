"""Tests for /api/pipeline/* endpoints.

These run against the sibling rdf_test_pipeline database using the `dual_db`
fixture from tests/pipeline/conftest.py. The FastAPI TestClient does NOT go
through the real lifespan hook, so we bind `pipeline_db` to the test DB
manually via the fixture.
"""
from __future__ import annotations

from datetime import datetime
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from app.auth import create_token
from app.db import pipeline_db
from app.main import app


client = TestClient(app, raise_server_exceptions=False)


_FAKE_ADMIN = {
    "id": "admin-1",
    "email": "admin@example.com",
    "name": "Admin",
    "auth_method": "local",
    "password_hash": None,
    "is_verified": True,
    "has_full_access": True,
    "is_active": True,
    "created_at": "2026-01-01",
    "last_login_at": None,
}

_FAKE_USER = {
    **_FAKE_ADMIN,
    "id": "user-1",
    "email": "user@example.com",
    "has_full_access": False,
}


def _auth_header(user):
    token = create_token(user["id"], user["email"])
    return {"Authorization": f"Bearer {token}"}


# ---------------------------------------------------------------------------
# GET /api/pipeline/status
# ---------------------------------------------------------------------------

def test_status_empty(dual_db):
    r = client.get("/api/pipeline/status")
    assert r.status_code == 200
    body = r.json()
    assert body["recent_runs"] == []
    assert body["queue"] == {
        "pending": 0, "processing": 0, "completed": 0,
        "failed": 0, "oldest_pending": None,
    }


def test_status_reports_recent_runs_and_queue(dual_db):
    conn = pipeline_db.get_conn()
    # Insert a completed run
    conn.execute(
        """
        INSERT INTO pipeline_runs
            (trigger, status, finished_at, krs_queued, krs_processed,
             etl_docs_parsed, features_computed, predictions_written,
             total_duration_seconds)
        VALUES ('scheduled', 'completed', now(), 5, 4, 4, 24, 4, 12.5)
        """
    )
    # Seed queue items
    conn.execute(
        """
        INSERT INTO pipeline_queue (krs, document_key, trigger_reason, status)
        VALUES ('0000000001', 'doc-1', 'test', 'pending'),
               ('0000000002', 'doc-2', 'test', 'completed')
        """
    )

    r = client.get("/api/pipeline/status")
    assert r.status_code == 200
    body = r.json()
    assert len(body["recent_runs"]) == 1
    run = body["recent_runs"][0]
    assert run["status"] == "completed"
    assert run["trigger"] == "scheduled"
    assert run["krs_processed"] == 4
    assert run["etl_docs_parsed"] == 4
    assert body["queue"]["pending"] == 1
    assert body["queue"]["completed"] == 1


# ---------------------------------------------------------------------------
# GET /api/pipeline/runs/{run_id}
# ---------------------------------------------------------------------------

def test_run_detail_not_found(dual_db):
    r = client.get("/api/pipeline/runs/9999")
    assert r.status_code == 404


def test_run_detail_returns_summary(dual_db):
    conn = pipeline_db.get_conn()
    row = conn.execute(
        """
        INSERT INTO pipeline_runs (trigger, status, krs_processed)
        VALUES ('manual', 'running', 0)
        RETURNING run_id
        """
    ).fetchone()
    run_id = int(row[0])
    r = client.get(f"/api/pipeline/runs/{run_id}")
    assert r.status_code == 200
    body = r.json()
    assert body["run_id"] == run_id
    assert body["trigger"] == "manual"
    assert body["status"] == "running"


# ---------------------------------------------------------------------------
# POST /api/pipeline/queue — admin only
# ---------------------------------------------------------------------------

def test_queue_requires_auth(dual_db):
    r = client.post("/api/pipeline/queue", json={"krs": ["0000000001"]})
    assert r.status_code == 401


def test_queue_rejects_non_admin(dual_db):
    with patch("app.auth.prediction_db.get_user_by_id", return_value=_FAKE_USER):
        r = client.post(
            "/api/pipeline/queue",
            json={"krs": ["0000000001"], "reason": "manual_test"},
            headers=_auth_header(_FAKE_USER),
        )
    assert r.status_code == 403


def test_queue_admin_enqueues(dual_db):
    with patch("app.auth.prediction_db.get_user_by_id", return_value=_FAKE_ADMIN):
        r = client.post(
            "/api/pipeline/queue",
            json={"krs": ["0000000001", "0000000002"], "reason": "admin_test"},
            headers=_auth_header(_FAKE_ADMIN),
        )
    assert r.status_code == 200
    assert r.json() == {"enqueued": 2}

    conn = pipeline_db.get_conn()
    n = conn.execute(
        "SELECT count(*) FROM pipeline_queue WHERE trigger_reason = 'admin_test'"
    ).fetchone()[0]
    assert n == 2


# ---------------------------------------------------------------------------
# GET /api/pipeline/peer-stats/{krs}
# ---------------------------------------------------------------------------

def test_peer_stats_unknown_company(dual_db):
    r = client.get("/api/pipeline/peer-stats/0000099999")
    assert r.status_code == 200
    body = r.json()
    assert body["krs"] == "0000099999"
    assert body["peer_stats"] is None


def test_peer_stats_with_population_stats(dual_db):
    from datetime import date
    conn = pipeline_db.get_conn()
    # Seed a company with incorporation_date in the "mature" bucket (8-15y)
    old_year = date.today().year - 10
    conn.execute(
        """
        INSERT INTO companies (krs, pkd_code, incorporation_date)
        VALUES ('0000050505', '62.01.Z', %s)
        """,
        [f"{old_year}-01-01"],
    )
    # Seed matching population_stats row
    conn.execute(
        """
        INSERT INTO population_stats
            (pkd_code, tenure_bucket, model_id, mean_score, stddev_score,
             p25, p50, p75, p90, p95, sample_size)
        VALUES ('62.01.Z', 'mature', 'maczynska_1994_v1',
                1.5, 0.5, 1.0, 1.5, 2.0, 2.5, 3.0, 42)
        """
    )

    r = client.get("/api/pipeline/peer-stats/0000050505")
    assert r.status_code == 200
    body = r.json()
    assert body["krs"] == "0000050505"
    assert body["peer_stats"] is not None
    assert body["peer_stats"]["pkd_code"] == "62.01.Z"
    assert body["peer_stats"]["tenure_bucket"] == "mature"
    assert body["peer_stats"]["peer_group_mean"] == pytest.approx(1.5)
    assert body["peer_stats"]["peer_group_size"] == 42


# ---------------------------------------------------------------------------
# POST /api/pipeline/trigger — admin only
# ---------------------------------------------------------------------------

def test_trigger_requires_admin(dual_db):
    with patch("app.auth.prediction_db.get_user_by_id", return_value=_FAKE_USER):
        r = client.post("/api/pipeline/trigger", headers=_auth_header(_FAKE_USER))
    assert r.status_code == 403


def test_trigger_schedules_background_task(dual_db):
    """Happy path — endpoint returns 200 and schedules the task. We patch
    run_pipeline so the background worker doesn't execute a real pipeline run
    (and so we can confirm it was invoked)."""
    called = {"n": 0}

    def _fake_run_pipeline(**kwargs):
        called["n"] += 1
        return None

    import pipeline.runner as runner_mod

    with patch("app.auth.prediction_db.get_user_by_id", return_value=_FAKE_ADMIN), \
         patch.object(runner_mod, "run_pipeline", _fake_run_pipeline):
        r = client.post(
            "/api/pipeline/trigger?limit=1&skip_bq=true",
            headers=_auth_header(_FAKE_ADMIN),
        )

    assert r.status_code == 200
    assert r.json() == {"status": "triggered"}
    # TestClient runs background tasks after the response is sent
    assert called["n"] == 1
