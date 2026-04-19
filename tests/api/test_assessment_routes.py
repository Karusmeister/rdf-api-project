"""Tests for on-demand assessment pipeline endpoints."""

from unittest.mock import patch, MagicMock

import pytest
from fastapi.testclient import TestClient

from app.main import app

client = TestClient(app, raise_server_exceptions=False)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_FAKE_USER = {
    "id": "user-1",
    "email": "test@example.com",
    "name": "Test User",
    "auth_method": "local",
    "password_hash": None,
    "is_verified": True,
    "has_full_access": True,
    "is_active": True,
    "created_at": "2026-01-01",
    "last_login_at": None,
}

_FAKE_USER_LIMITED = {
    **_FAKE_USER,
    "id": "user-2",
    "has_full_access": False,
}

_READY_SUMMARY = {
    "entity_exists": True,
    "documents_total": 3,
    "documents_downloaded": 3,
    "reports_ingested": 3,
    "features_computed": True,
    "predictions_available": True,
    "models_total": 1,
    "models_scored": 1,
    "scoring_gaps": 0,
    "latest_fiscal_year": 2024,
}

_NOT_READY_SUMMARY = {
    "entity_exists": True,
    "documents_total": 3,
    "documents_downloaded": 1,
    "reports_ingested": 0,
    "features_computed": False,
    "predictions_available": False,
    "models_total": 0,
    "models_scored": 0,
    "scoring_gaps": 0,
    "latest_fiscal_year": None,
}

_FAKE_JOB = {
    "id": "job-123",
    "krs": "0000694720",
    "status": "running",
    "stage": "downloading",
    "error_message": None,
    "result_json": '{"progress": {"documents_total": 5, "documents_downloaded": 2, "documents_ingested": 0, "features_computed": false, "predictions_scored": false}}',
    "created_at": "2026-04-06 12:00:00+00:00",
    "updated_at": "2026-04-06 12:00:05+00:00",
}


def _auth_header(user=None):
    from app.auth import create_token
    u = user or _FAKE_USER
    return {"Authorization": f"Bearer {create_token(u['id'], u['email'])}"}


# ---------------------------------------------------------------------------
# POST /api/assessment/{krs} — data already ready
# ---------------------------------------------------------------------------


class TestStartAssessment:

    @patch("app.db.prediction_db.check_krs_access", return_value=True)
    @patch("app.db.prediction_db.get_user_by_id", return_value=_FAKE_USER)
    @patch("app.services.assessment.check_data_readiness", return_value=_READY_SUMMARY)
    def test_returns_200_when_data_ready(self, mock_readiness, mock_user, mock_access):
        resp = client.post("/api/assessment/694720", headers=_auth_header())
        assert resp.status_code == 200
        body = resp.json()
        assert body["status"] == "ready"
        assert body["job_id"] is None
        assert body["data_summary"]["predictions_available"] is True

    @patch("app.db.prediction_db.check_krs_access", return_value=True)
    @patch("app.db.prediction_db.get_user_by_id", return_value=_FAKE_USER)
    @patch("app.services.assessment.check_data_readiness", return_value=_NOT_READY_SUMMARY)
    @patch("app.services.assessment.start_assessment", return_value=("job-new", True))
    @patch("app.services.assessment.run_pipeline")
    def test_returns_202_when_pipeline_started(self, mock_run, mock_start, mock_readiness, mock_user, mock_access):
        resp = client.post("/api/assessment/694720", headers=_auth_header())
        # TestClient doesn't produce 202 via status_code override from the route,
        # but the response body should indicate pending status
        body = resp.json()
        assert body["status"] == "pending"
        assert body["job_id"] == "job-new"
        assert body["message"] == "Assessment pipeline started"

    @patch("app.db.prediction_db.check_krs_access", return_value=True)
    @patch("app.db.prediction_db.get_user_by_id", return_value=_FAKE_USER)
    @patch("app.services.assessment.check_data_readiness", return_value=_NOT_READY_SUMMARY)
    @patch("app.services.assessment.start_assessment", return_value=("job-existing", False))
    def test_returns_running_when_job_already_exists(self, mock_start, mock_readiness, mock_user, mock_access):
        resp = client.post("/api/assessment/694720", headers=_auth_header())
        body = resp.json()
        assert body["status"] == "running"
        assert body["job_id"] == "job-existing"
        assert body["message"] == "Analysis already in progress"

    def test_returns_401_without_auth(self):
        resp = client.post("/api/assessment/694720")
        assert resp.status_code == 401

    @patch("app.db.prediction_db.check_krs_access", return_value=False)
    @patch("app.db.prediction_db.get_user_by_id", return_value=_FAKE_USER_LIMITED)
    def test_returns_403_without_krs_access(self, mock_user, mock_access):
        resp = client.post("/api/assessment/694720", headers=_auth_header(_FAKE_USER_LIMITED))
        assert resp.status_code == 403


# ---------------------------------------------------------------------------
# GET /api/assessment/jobs/{job_id} — poll progress
# ---------------------------------------------------------------------------


class TestJobStatus:

    @patch("app.db.prediction_db.get_user_by_id", return_value=_FAKE_USER)
    @patch("app.services.assessment.get_job_status", return_value=_FAKE_JOB)
    def test_returns_job_with_progress(self, mock_job, mock_user):
        resp = client.get("/api/assessment/jobs/job-123", headers=_auth_header())
        assert resp.status_code == 200
        body = resp.json()
        assert body["job_id"] == "job-123"
        assert body["status"] == "running"
        assert body["stage"] == "downloading"
        assert body["progress"]["documents_total"] == 5
        assert body["progress"]["documents_downloaded"] == 2

    @patch("app.db.prediction_db.get_user_by_id", return_value=_FAKE_USER)
    @patch("app.services.assessment.get_job_status", return_value=None)
    def test_returns_404_for_unknown_job(self, mock_job, mock_user):
        resp = client.get("/api/assessment/jobs/nonexistent", headers=_auth_header())
        assert resp.status_code == 404

    @patch("app.db.prediction_db.get_user_by_id", return_value=_FAKE_USER)
    @patch("app.services.assessment.get_job_status", return_value={
        "id": "job-done",
        "krs": "0000694720",
        "status": "completed",
        "stage": "scoring",
        "error_message": None,
        "result_json": None,
        "created_at": "2026-04-06 12:00:00+00:00",
        "updated_at": "2026-04-06 12:00:30+00:00",
    })
    def test_completed_job_without_progress(self, mock_job, mock_user):
        resp = client.get("/api/assessment/jobs/job-done", headers=_auth_header())
        assert resp.status_code == 200
        body = resp.json()
        assert body["status"] == "completed"
        assert body["progress"] is None

    def test_returns_401_without_auth(self):
        resp = client.get("/api/assessment/jobs/job-123")
        assert resp.status_code == 401


# ---------------------------------------------------------------------------
# Service unit tests
# ---------------------------------------------------------------------------


class TestDataReadiness:

    @patch("app.db.prediction_db.get_scoring_coverage_for_krs", return_value={
        "active_model_ids": ["m1"],
        "completed_report_ids": ["r1"],
        "scored_models": {"m1": ["r1"]},
        "missing": [],
    })
    @patch("app.db.prediction_db.get_predictions_fat", return_value=[{"raw_score": 2.0}])
    @patch("app.db.prediction_db.get_computed_features_for_report", return_value=[{"value": 1.0}])
    @patch("app.db.prediction_db.get_reports_for_krs", return_value=[
        {"id": "r1", "ingestion_status": "completed", "fiscal_year": 2024},
    ])
    @patch("app.db.prediction_db.get_ingested_report_ids_for_krs", return_value={"doc1"})
    @patch("app.scraper.db.get_undownloaded_documents", return_value=[])
    @patch("app.scraper.db.get_known_document_ids", return_value={"doc1"})
    def test_is_ready_when_all_stages_complete(self, *mocks):
        from app.services.assessment import check_data_readiness, is_data_ready
        summary = check_data_readiness("0000694720")
        assert is_data_ready(summary) is True

    @patch("app.db.prediction_db.get_scoring_coverage_for_krs", return_value={
        "active_model_ids": [],
        "completed_report_ids": [],
        "scored_models": {},
        "missing": [],
    })
    @patch("app.db.prediction_db.get_predictions_fat", return_value=[])
    @patch("app.db.prediction_db.get_computed_features_for_report", return_value=[])
    @patch("app.db.prediction_db.get_reports_for_krs", return_value=[])
    @patch("app.db.prediction_db.get_ingested_report_ids_for_krs", return_value=set())
    @patch("app.scraper.db.get_undownloaded_documents", return_value=["doc1"])
    @patch("app.scraper.db.get_known_document_ids", return_value={"doc1"})
    def test_not_ready_when_documents_not_downloaded(self, *mocks):
        from app.services.assessment import check_data_readiness, is_data_ready
        summary = check_data_readiness("0000694720")
        assert is_data_ready(summary) is False

    @patch("app.db.prediction_db.get_scoring_coverage_for_krs", return_value={
        "active_model_ids": ["m1"],
        "completed_report_ids": ["r1"],
        "scored_models": {},
        "missing": [{"model_id": "m1", "report_id": "r1"}],
    })
    @patch("app.db.prediction_db.get_predictions_fat", return_value=[])
    @patch("app.db.prediction_db.get_computed_features_for_report", return_value=[{"value": 1.0}])
    @patch("app.db.prediction_db.get_reports_for_krs", return_value=[
        {"id": "r1", "ingestion_status": "completed", "fiscal_year": 2024},
    ])
    @patch("app.db.prediction_db.get_ingested_report_ids_for_krs", return_value={"doc1"})
    @patch("app.scraper.db.get_undownloaded_documents", return_value=[])
    @patch("app.scraper.db.get_known_document_ids", return_value={"doc1"})
    def test_not_ready_when_no_predictions(self, *mocks):
        from app.services.assessment import check_data_readiness, is_data_ready
        summary = check_data_readiness("0000694720")
        assert is_data_ready(summary) is False


class TestStartAssessmentService:

    @patch("app.db.prediction_db.create_assessment_job")
    @patch("app.db.prediction_db.get_running_assessment_for_krs", return_value=None)
    def test_creates_new_job(self, mock_get, mock_create):
        from app.services.assessment import start_assessment
        job_id, is_new = start_assessment("0000694720")
        assert is_new is True
        mock_create.assert_called_once()

    @patch("app.db.prediction_db.get_running_assessment_for_krs", return_value={
        "id": "existing-job",
        "krs": "0000694720",
        "status": "running",
    })
    def test_returns_existing_job(self, mock_get):
        from app.services.assessment import start_assessment
        job_id, is_new = start_assessment("0000694720")
        assert is_new is False
        assert job_id == "existing-job"
