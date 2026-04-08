"""Tests for pipeline trigger and status endpoints (PKR-85)."""

from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from app.main import app

client = TestClient(app, raise_server_exceptions=False)

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


def _auth_header(user=None):
    from app.auth import create_token
    u = user or _FAKE_USER
    token = create_token(u["id"], u["email"])
    return {"Authorization": f"Bearer {token}"}


# ---------------------------------------------------------------------------
# POST /api/predictions/{krs}/generate
# ---------------------------------------------------------------------------

class TestPipelineTrigger:
    @patch("app.db.prediction_db.check_krs_access", return_value=True)
    @patch("app.db.prediction_db.get_user_by_id", return_value=_FAKE_USER)
    @patch("app.db.prediction_db.atomic_start_pipeline")
    def test_trigger_creates_new_job(self, mock_atomic, mock_user, mock_access):
        mock_atomic.return_value = {"outcome": "created", "job_id": "job-123"}

        resp = client.post("/api/predictions/0000694720/generate", headers=_auth_header())
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "pending"
        assert data["job_id"] == "job-123"
        assert data["krs"] == "0000694720"

    @patch("app.db.prediction_db.check_krs_access", return_value=True)
    @patch("app.db.prediction_db.get_user_by_id", return_value=_FAKE_USER)
    @patch("app.db.prediction_db.atomic_start_pipeline")
    def test_trigger_returns_existing_job(self, mock_atomic, mock_user, mock_access):
        mock_atomic.return_value = {"outcome": "existing", "job_id": "job-456"}

        resp = client.post("/api/predictions/0000694720/generate", headers=_auth_header())
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "running"
        assert data["job_id"] == "job-456"

    @patch("app.db.prediction_db.check_krs_access", return_value=True)
    @patch("app.db.prediction_db.get_user_by_id", return_value=_FAKE_USER)
    @patch("app.db.prediction_db.atomic_start_pipeline")
    def test_trigger_rejects_at_max_concurrency(self, mock_atomic, mock_user, mock_access):
        mock_atomic.return_value = {"outcome": "rejected"}

        resp = client.post("/api/predictions/0000694720/generate", headers=_auth_header())
        assert resp.status_code == 429

    def test_trigger_requires_auth(self):
        resp = client.post("/api/predictions/0000694720/generate")
        assert resp.status_code == 401

    @patch("app.db.prediction_db.check_krs_access", return_value=False)
    @patch("app.db.prediction_db.get_user_by_id", return_value=_FAKE_USER_LIMITED)
    def test_trigger_requires_krs_access(self, mock_user, mock_access):
        resp = client.post(
            "/api/predictions/0000694720/generate",
            headers=_auth_header(_FAKE_USER_LIMITED),
        )
        assert resp.status_code == 403


# ---------------------------------------------------------------------------
# GET /api/predictions/{krs}/status
# ---------------------------------------------------------------------------

class TestPipelineStatus:
    @patch("app.db.prediction_db.check_krs_access", return_value=True)
    @patch("app.db.prediction_db.get_user_by_id", return_value=_FAKE_USER)
    @patch("app.db.prediction_db.get_running_assessment_for_krs")
    def test_status_returns_running_job(self, mock_running, mock_user, mock_access):
        mock_running.return_value = {
            "id": "job-123",
            "krs": "0000694720",
            "status": "running",
            "stage": "downloading",
            "error_message": None,
            "result_json": None,
            "created_at": "2026-04-08 12:00:00",
            "updated_at": "2026-04-08 12:01:00",
        }

        resp = client.get("/api/predictions/0000694720/status", headers=_auth_header())
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "running"
        assert data["current_stage"] == "downloading"

    @patch("app.db.prediction_db.check_krs_access", return_value=True)
    @patch("app.db.prediction_db.get_user_by_id", return_value=_FAKE_USER)
    @patch("app.db.prediction_db.get_latest_assessment_for_krs", return_value=None)
    @patch("app.db.prediction_db.get_running_assessment_for_krs", return_value=None)
    def test_status_404_when_no_job(self, mock_running, mock_latest, mock_user, mock_access):
        resp = client.get("/api/predictions/9999999999/status", headers=_auth_header())
        assert resp.status_code == 404

    def test_status_requires_auth(self):
        resp = client.get("/api/predictions/0000694720/status")
        assert resp.status_code == 401


# ---------------------------------------------------------------------------
# data_coverage in GET /api/predictions/{krs}
# ---------------------------------------------------------------------------

class TestDataCoverage:
    @patch("app.db.prediction_db.check_krs_access", return_value=True)
    @patch("app.db.prediction_db.get_user_by_id", return_value=_FAKE_USER)
    @patch("app.db.prediction_db.get_document_coverage")
    @patch("app.db.prediction_db.get_prediction_history_fat", return_value=[])
    @patch("app.db.prediction_db.get_source_line_items_for_reports_batch", return_value={})
    @patch("app.db.prediction_db.get_features_for_predictions_batch", return_value={})
    @patch("app.db.prediction_db.get_predictions_fat", return_value=[])
    @patch("app.db.prediction_db.get_company")
    def test_data_coverage_included_in_response(
        self, mock_company, mock_preds, mock_feat, mock_sources,
        mock_hist, mock_coverage, mock_user, mock_access,
    ):
        mock_company.return_value = {"krs": "0000694720", "nip": "123", "pkd_code": "62.01.Z"}
        mock_coverage.return_value = [
            {"fiscal_year": 2019, "file_type": "xml", "doc_count": 1, "is_parsed": True},
            {"fiscal_year": 2020, "file_type": "xml", "doc_count": 1, "is_parsed": True},
            {"fiscal_year": 2016, "file_type": "pdf", "doc_count": 1, "is_parsed": False},
            {"fiscal_year": 2017, "file_type": "pdf", "doc_count": 1, "is_parsed": False},
        ]

        resp = client.get("/api/predictions/0000694720", headers=_auth_header())
        assert resp.status_code == 200
        data = resp.json()
        dc = data["data_coverage"]
        assert dc is not None
        assert dc["xml_years"] == [2019, 2020]
        assert dc["pdf_only_years"] == [2016, 2017]
        assert dc["earliest_xml_year"] == 2019
        assert dc["earliest_document_year"] == 2016
        assert "XML" in dc["analysis_note_en"]
        assert "PDF" in dc["analysis_note_en"]

    @patch("app.db.prediction_db.check_krs_access", return_value=True)
    @patch("app.db.prediction_db.get_user_by_id", return_value=_FAKE_USER)
    @patch("app.db.prediction_db.get_document_coverage", return_value=[])
    @patch("app.db.prediction_db.get_prediction_history_fat", return_value=[])
    @patch("app.db.prediction_db.get_source_line_items_for_reports_batch", return_value={})
    @patch("app.db.prediction_db.get_features_for_predictions_batch", return_value={})
    @patch("app.db.prediction_db.get_predictions_fat", return_value=[])
    @patch("app.db.prediction_db.get_company")
    def test_data_coverage_null_when_no_documents(
        self, mock_company, mock_preds, mock_feat, mock_sources,
        mock_hist, mock_coverage, mock_user, mock_access,
    ):
        mock_company.return_value = {"krs": "0000694720", "nip": "123", "pkd_code": "62.01.Z"}

        resp = client.get("/api/predictions/0000694720", headers=_auth_header())
        assert resp.status_code == 200
        data = resp.json()
        assert data["data_coverage"] is None

    @patch("app.db.prediction_db.check_krs_access", return_value=True)
    @patch("app.db.prediction_db.get_user_by_id", return_value=_FAKE_USER)
    @patch("app.db.prediction_db.get_document_coverage", side_effect=RuntimeError("unexpected DB error"))
    @patch("app.db.prediction_db.get_prediction_history_fat", return_value=[])
    @patch("app.db.prediction_db.get_predictions_fat", return_value=[])
    @patch("app.db.prediction_db.get_company")
    def test_data_coverage_propagates_unexpected_errors(
        self, mock_company, mock_preds, mock_hist,
        mock_coverage, mock_user, mock_access,
    ):
        """Unexpected DB errors in data_coverage must propagate, not be silently swallowed."""
        mock_company.return_value = {"krs": "0000694720", "nip": "123", "pkd_code": "62.01.Z"}

        resp = client.get("/api/predictions/0000694720", headers=_auth_header())
        # The error propagates and the endpoint returns 500, not a silent null
        assert resp.status_code == 500


# ---------------------------------------------------------------------------
# Unit test for atomic_start_pipeline contract (no real DB needed)
# ---------------------------------------------------------------------------

class TestAtomicStartPipelineContract:
    """Verify atomic_start_pipeline uses a standalone connection, never shared."""

    @patch("psycopg2.connect")
    def test_uses_standalone_connection_not_shared(self, mock_connect):
        """atomic_start_pipeline must call psycopg2.connect() for a standalone
        connection and close it afterwards — never get_conn()/get_db()."""
        from app.db.prediction_db import atomic_start_pipeline

        mock_raw = MagicMock()
        mock_connect.return_value = mock_raw

        # Simulate: no existing job, count=0
        mock_cursor = MagicMock()
        mock_cursor.fetchone.side_effect = [
            None,   # no existing job for this KRS
            (0,),   # global running count
        ]
        mock_raw.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_raw.cursor.return_value.__exit__ = MagicMock(return_value=False)

        result = atomic_start_pipeline("0000694720", 5)

        # Verify psycopg2.connect() was called (standalone connection)
        mock_connect.assert_called_once()
        # Verify connection is closed in finally block
        mock_raw.close.assert_called_once()
        assert result["outcome"] == "created"
        assert "job_id" in result

    @patch("psycopg2.connect")
    def test_connection_closed_on_error(self, mock_connect):
        """Connection must be closed even when the transaction fails."""
        from app.db.prediction_db import atomic_start_pipeline

        mock_raw = MagicMock()
        mock_connect.return_value = mock_raw
        mock_raw.cursor.return_value.__enter__ = MagicMock(
            side_effect=RuntimeError("DB explosion"),
        )
        mock_raw.cursor.return_value.__exit__ = MagicMock(return_value=False)

        with pytest.raises(RuntimeError, match="DB explosion"):
            atomic_start_pipeline("0000694720", 5)

        mock_raw.close.assert_called_once()
