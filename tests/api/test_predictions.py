"""Tests for predictions API endpoints (M6/PKR-65) and auth-gated access (M8/PKR-70)."""

from unittest.mock import patch

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

_FAKE_COMPANY = {
    "krs": "0000694720",
    "nip": "1234567890",
    "regon": None,
    "pkd_code": "62.01.Z",
    "incorporation_date": None,
    "voivodeship": None,
}

_FAKE_PREDICTION_FAT = [{
    "raw_score": 2.5,
    "probability": None,
    "classification": 0,
    "risk_category": "low",
    "feature_contributions": {"x1_maczynska": 0.5},
    "scored_at": "2026-03-01 12:00:00",
    "model_id": "maczynska_1994_v1",
    "model_name": "maczynska",
    "model_type": "discriminant",
    "model_version": "1994_v1",
    "is_baseline": True,
    "model_description": "Maczynska 1994",
    "hyperparameters": {"coefficients": {}},
    "feature_set_id": "maczynska_6",
    "report_id": "rpt-1",
    "fiscal_year": 2024,
    "period_start": "2024-01-01",
    "period_end": "2024-12-31",
    "report_version": 1,
    "data_source_id": "KRS",
    "ingested_at": "2026-02-01 10:00:00",
}]


def _auth_header(user=None):
    """Create a valid JWT token for test user."""
    from app.auth import create_token
    u = user or _FAKE_USER
    token = create_token(u["id"], u["email"])
    return {"Authorization": f"Bearer {token}"}


# ---------------------------------------------------------------------------
# GET /api/predictions/models — no auth required
# ---------------------------------------------------------------------------

class TestListModels:
    @patch("app.services.predictions._get_models")
    def test_returns_models(self, mock_models):
        mock_models.return_value = [{
            "id": "maczynska_1994_v1",
            "name": "maczynska",
            "model_type": "discriminant",
            "version": "1994_v1",
            "feature_set_id": "maczynska_6",
            "description": "Maczynska 1994",
            "hyperparameters": None,
            "is_baseline": True,
            "is_active": True,
            "created_at": "2026-01-01",
        }]
        resp = client.get("/api/predictions/models")
        assert resp.status_code == 200
        data = resp.json()
        assert "models" in data
        assert len(data["models"]) == 1
        assert data["models"][0]["model_id"] == "maczynska_1994_v1"


# ---------------------------------------------------------------------------
# GET /api/predictions/{krs} — auth + KRS access required
# ---------------------------------------------------------------------------

class TestGetPredictions:
    @patch("app.db.prediction_db.check_krs_access", return_value=True)
    @patch("app.db.prediction_db.get_user_by_id", return_value=_FAKE_USER)
    @patch("app.db.prediction_db.get_predictions_fat", return_value=_FAKE_PREDICTION_FAT)
    @patch("app.db.prediction_db.get_company", return_value=_FAKE_COMPANY)
    @patch("app.db.prediction_db.get_features_for_report", return_value=[])
    @patch("app.db.prediction_db.get_prediction_history_fat", return_value=[])
    def test_returns_predictions(self, mock_hist, mock_feat, mock_company, mock_preds, mock_user, mock_access):
        resp = client.get("/api/predictions/0000694720", headers=_auth_header())
        assert resp.status_code == 200
        data = resp.json()
        assert data["company"]["krs"] == "0000694720"
        assert len(data["predictions"]) == 1
        assert data["predictions"][0]["result"]["risk_category"] == "low"

    def test_401_without_token(self):
        resp = client.get("/api/predictions/0000694720")
        assert resp.status_code == 401

    @patch("app.db.prediction_db.get_user_by_id", return_value=_FAKE_USER_LIMITED)
    @patch("app.db.prediction_db.check_krs_access", return_value=False)
    def test_403_without_krs_access(self, mock_access, mock_user):
        resp = client.get("/api/predictions/0000694720", headers=_auth_header(_FAKE_USER_LIMITED))
        assert resp.status_code == 403

    @patch("app.db.prediction_db.check_krs_access", return_value=True)
    @patch("app.db.prediction_db.get_user_by_id", return_value=_FAKE_USER)
    @patch("app.db.prediction_db.get_prediction_history_fat", return_value=[])
    @patch("app.db.prediction_db.get_predictions_fat", return_value=[])
    @patch("app.db.prediction_db.get_company", return_value=None)
    def test_404_unknown_krs(self, mock_company, mock_preds, mock_hist, mock_user, mock_access):
        resp = client.get("/api/predictions/9999999999", headers=_auth_header())
        assert resp.status_code == 404

    @patch("app.db.prediction_db.check_krs_access", return_value=True)
    @patch("app.db.prediction_db.get_user_by_id", return_value=_FAKE_USER)
    @patch("app.db.prediction_db.get_prediction_history_fat", return_value=[])
    @patch("app.db.prediction_db.get_predictions_fat", return_value=[])
    @patch("app.db.prediction_db.get_company", return_value=_FAKE_COMPANY)
    def test_200_company_exists_no_predictions(self, mock_company, mock_preds, mock_hist, mock_user, mock_access):
        """Company exists with metadata but no predictions/history -> 200, not 404."""
        resp = client.get("/api/predictions/0000694720", headers=_auth_header())
        assert resp.status_code == 200
        data = resp.json()
        assert data["company"]["krs"] == "0000694720"
        assert data["predictions"] == []


# ---------------------------------------------------------------------------
# GET /api/predictions/{krs}/history — auth + KRS access required
# ---------------------------------------------------------------------------

class TestGetHistory:
    @patch("app.db.prediction_db.check_krs_access", return_value=True)
    @patch("app.db.prediction_db.get_user_by_id", return_value=_FAKE_USER)
    @patch("app.db.prediction_db.get_prediction_history_fat")
    def test_returns_history(self, mock_hist, mock_user, mock_access):
        mock_hist.return_value = [{
            "raw_score": 2.5, "probability": None, "classification": 0,
            "risk_category": "low", "feature_contributions": {},
            "scored_at": "2026-03-01", "model_id": "maczynska_1994_v1",
            "model_name": "maczynska", "model_version": "1994_v1",
            "fiscal_year": 2024, "period_start": "2024-01-01", "period_end": "2024-12-31",
        }]
        resp = client.get("/api/predictions/0000694720/history", headers=_auth_header())
        assert resp.status_code == 200
        data = resp.json()
        assert data["krs"] == "0000694720"
        assert len(data["history"]) == 1

    def test_401_without_token(self):
        resp = client.get("/api/predictions/0000694720/history")
        assert resp.status_code == 401


# ---------------------------------------------------------------------------
# POST /api/predictions/cache/invalidate
# ---------------------------------------------------------------------------

class TestCacheInvalidate:
    @patch("app.services.predictions.invalidate_caches")
    @patch("app.db.prediction_db.get_user_by_id", return_value=_FAKE_USER)
    def test_invalidates_as_admin(self, mock_user, mock_inv):
        resp = client.post("/api/predictions/cache/invalidate", headers=_auth_header())
        assert resp.status_code == 200
        mock_inv.assert_called_once()

    def test_invalidate_requires_auth(self):
        resp = client.post("/api/predictions/cache/invalidate")
        assert resp.status_code == 401

    @patch("app.db.prediction_db.get_user_by_id", return_value=_FAKE_USER_LIMITED)
    def test_invalidate_requires_admin(self, mock_user):
        resp = client.post("/api/predictions/cache/invalidate", headers=_auth_header(_FAKE_USER_LIMITED))
        assert resp.status_code == 403
