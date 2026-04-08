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
    "schema_code": "SFJINZ",
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
    @patch("app.db.prediction_db.get_features_for_predictions_batch", return_value={})
    @patch("app.db.prediction_db.get_source_line_items_for_reports_batch", return_value={})
    @patch("app.db.prediction_db.get_prediction_history_fat", return_value=[])
    @patch("app.db.prediction_db.get_document_coverage", return_value=[])
    def test_returns_predictions(self, mock_cov, mock_hist, mock_sources, mock_feat, mock_company, mock_preds, mock_user, mock_access):
        resp = client.get("/api/predictions/0000694720", headers=_auth_header())
        assert resp.status_code == 200
        data = resp.json()
        assert data["company"]["krs"] == "0000694720"
        assert len(data["predictions"]) == 1
        assert data["predictions"][0]["result"]["risk_category"] == "low"

    @patch("app.db.prediction_db.check_krs_access", return_value=True)
    @patch("app.db.prediction_db.get_user_by_id", return_value=_FAKE_USER)
    @patch(
        "app.db.prediction_db.get_predictions_fat",
        return_value=[
            _FAKE_PREDICTION_FAT[0],
            {
                **_FAKE_PREDICTION_FAT[0],
                "report_id": "rpt-0",
                "fiscal_year": 2023,
                "period_start": "2023-01-01",
                "period_end": "2023-12-31",
                "raw_score": 1.1,
                "risk_category": "medium",
                "scored_at": "2025-03-01 12:00:00",
            },
        ],
    )
    @patch("app.db.prediction_db.get_company", return_value=_FAKE_COMPANY)
    @patch("app.db.prediction_db.get_features_for_predictions_batch", return_value={})
    @patch("app.db.prediction_db.get_source_line_items_for_reports_batch", return_value={})
    @patch("app.db.prediction_db.get_prediction_history_fat", return_value=[])
    @patch("app.db.prediction_db.get_document_coverage", return_value=[])
    def test_returns_multi_year_predictions_per_model(
        self,
        mock_cov,
        mock_hist,
        mock_sources,
        mock_feat,
        mock_company,
        mock_preds,
        mock_user,
        mock_access,
    ):
        resp = client.get("/api/predictions/0000694720", headers=_auth_header())
        assert resp.status_code == 200
        data = resp.json()
        assert len(data["predictions"]) == 2
        years = {p["data_source"]["fiscal_year"] for p in data["predictions"]}
        assert years == {2023, 2024}

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
    @patch("app.db.prediction_db.get_document_coverage", return_value=[])
    def test_404_unknown_krs(self, mock_cov, mock_company, mock_preds, mock_hist, mock_user, mock_access):
        resp = client.get("/api/predictions/9999999999", headers=_auth_header())
        assert resp.status_code == 404

    @patch("app.db.prediction_db.check_krs_access", return_value=True)
    @patch("app.db.prediction_db.get_user_by_id", return_value=_FAKE_USER)
    @patch("app.db.prediction_db.get_prediction_history_fat", return_value=[])
    @patch("app.db.prediction_db.get_predictions_fat", return_value=[])
    @patch("app.db.prediction_db.get_company", return_value=_FAKE_COMPANY)
    @patch("app.db.prediction_db.get_document_coverage", return_value=[])
    def test_200_company_exists_no_predictions(self, mock_cov, mock_company, mock_preds, mock_hist, mock_user, mock_access):
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


# ---------------------------------------------------------------------------
# CR-PZN-005: Poznanski-specific API coverage
# ---------------------------------------------------------------------------

_FAKE_POZNANSKI_FAT = {
    **_FAKE_PREDICTION_FAT[0],
    "raw_score": 0.8,
    "classification": 0,
    "risk_category": "medium",
    # `_warnings` + `_intercept` live under feature_contributions (how scorers
    # persist out-of-band metadata). The API must hoist `_warnings` to
    # `result.warnings` and leave the underscore keys out of feature-level
    # contributions (they do not match any feature_definition_id).
    "feature_contributions": {
        "_intercept": -2.368,
        "_warnings": ["WARNING_NON_LINEAR_LIQUIDITY"],
        "x1_poznanski": 0.3,
        "x2_poznanski": 0.6,
        "x3_poznanski": 0.9,
        "x4_poznanski": 0.4,
    },
    "model_id": "poznanski_2004_v1",
    "model_name": "poznanski",
    "model_version": "2004_v1",
    "model_description": "Poznanski 2004",
    "feature_set_id": "poznanski_4",
}


class TestPoznanskiModelsCatalog:
    """The `/api/predictions/models` endpoint must expose Poznanski without any
    prior scoring run (CR-PZN-001)."""

    @patch("app.services.predictions._get_models")
    def test_catalog_includes_poznanski(self, mock_models):
        mock_models.return_value = [
            {
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
            },
            {
                "id": "poznanski_2004_v1",
                "name": "poznanski",
                "model_type": "discriminant",
                "version": "2004_v1",
                "feature_set_id": "poznanski_4",
                "description": "Poznanski 2004",
                "hyperparameters": None,
                "is_baseline": True,
                "is_active": True,
                "created_at": "2026-01-01",
            },
        ]
        resp = client.get("/api/predictions/models")
        assert resp.status_code == 200
        data = resp.json()
        ids = {m["model_id"] for m in data["models"]}
        assert "poznanski_2004_v1" in ids

        poznanski_entry = next(m for m in data["models"] if m["model_id"] == "poznanski_2004_v1")
        interp = poznanski_entry["interpretation"]
        assert interp is not None
        assert interp["higher_is_better"] is True
        labels = [t["label"] for t in interp["thresholds"]]
        assert {"critical", "medium", "low"}.issubset(set(labels))


class TestPoznanskiWarningsPropagation:
    """CR-PZN-002 — `WARNING_NON_LINEAR_LIQUIDITY` must surface in
    `result.warnings` and NOT leak into per-feature contributions."""

    @patch("app.db.prediction_db.check_krs_access", return_value=True)
    @patch("app.db.prediction_db.get_user_by_id", return_value=_FAKE_USER)
    @patch("app.db.prediction_db.get_predictions_fat", return_value=[_FAKE_POZNANSKI_FAT])
    @patch("app.db.prediction_db.get_company", return_value=_FAKE_COMPANY)
    @patch("app.db.prediction_db.get_features_for_predictions_batch", return_value={})
    @patch("app.db.prediction_db.get_source_line_items_for_reports_batch", return_value={})
    @patch("app.db.prediction_db.get_prediction_history_fat", return_value=[])
    @patch("app.db.prediction_db.get_document_coverage", return_value=[])
    def test_warning_surfaces_in_result(self, *_):
        resp = client.get("/api/predictions/0000694720", headers=_auth_header())
        assert resp.status_code == 200
        data = resp.json()
        assert len(data["predictions"]) == 1
        result = data["predictions"][0]["result"]
        assert result["warnings"] == ["WARNING_NON_LINEAR_LIQUIDITY"]

    @patch("app.db.prediction_db.check_krs_access", return_value=True)
    @patch("app.db.prediction_db.get_user_by_id", return_value=_FAKE_USER)
    @patch(
        "app.db.prediction_db.get_predictions_fat",
        return_value=[{
            **_FAKE_POZNANSKI_FAT,
            "feature_contributions": {
                "_intercept": -2.368,
                "x1_poznanski": 0.3,
                "x2_poznanski": 0.6,
                "x3_poznanski": 0.9,
                "x4_poznanski": 0.4,
            },
        }],
    )
    @patch("app.db.prediction_db.get_company", return_value=_FAKE_COMPANY)
    @patch("app.db.prediction_db.get_features_for_predictions_batch", return_value={})
    @patch("app.db.prediction_db.get_source_line_items_for_reports_batch", return_value={})
    @patch("app.db.prediction_db.get_prediction_history_fat", return_value=[])
    @patch("app.db.prediction_db.get_document_coverage", return_value=[])
    def test_warnings_empty_when_none_raised(self, *_):
        resp = client.get("/api/predictions/0000694720", headers=_auth_header())
        assert resp.status_code == 200
        data = resp.json()
        assert data["predictions"][0]["result"]["warnings"] == []


class TestPoznanskiHistoryFilter:
    """CR-PZN-005 — `/history?model_id=poznanski_2004_v1` must scope results
    to the Poznanski timeline and not leak Maczynska rows."""

    @patch("app.db.prediction_db.check_krs_access", return_value=True)
    @patch("app.db.prediction_db.get_user_by_id", return_value=_FAKE_USER)
    @patch("app.db.prediction_db.get_prediction_history_fat")
    def test_model_filter_forwarded(self, mock_hist, mock_user, mock_access):
        mock_hist.return_value = [{
            "raw_score": 0.8, "probability": None, "classification": 0,
            "risk_category": "medium", "feature_contributions": {},
            "scored_at": "2026-03-01", "model_id": "poznanski_2004_v1",
            "model_name": "poznanski", "model_version": "2004_v1",
            "fiscal_year": 2024, "period_start": "2024-01-01", "period_end": "2024-12-31",
        }]
        resp = client.get(
            "/api/predictions/0000694720/history",
            params={"model_id": "poznanski_2004_v1"},
            headers=_auth_header(),
        )
        assert resp.status_code == 200
        # The service must forward the filter to the DB query so the stored
        # procedure can prune other models server-side.
        call_kwargs = mock_hist.call_args.kwargs
        call_args = mock_hist.call_args.args
        assert call_kwargs.get("model_id") == "poznanski_2004_v1" or "poznanski_2004_v1" in call_args

        data = resp.json()
        assert len(data["history"]) == 1
        assert data["history"][0]["model_id"] == "poznanski_2004_v1"


# ---------------------------------------------------------------------------
# CR2-REL-006: fail-fast + health signal on built-in model registration
# ---------------------------------------------------------------------------


class TestBuiltinModelRegistrationHealth:
    """Guards for the registration failure policy.

    Two behaviors under test:
      * In non-local environments a registrar exception aborts startup with
        RuntimeError — partial model catalogs must never ship.
      * In local dev the registrar falls open: it logs, marks the service
        degraded in `get_builtin_models_health()`, and `/health/predictions`
        reports 503 so operators see the signal.
    """

    def setup_method(self):
        # Reset the module-level state between tests so one failure doesn't
        # leak into the next case.
        from app.services import predictions as pred

        pred._registration_state["ok"] = True
        pred._registration_state["failed"] = []

    def teardown_method(self):
        from app.services import predictions as pred

        pred._registration_state["ok"] = True
        pred._registration_state["failed"] = []

    def test_fail_fast_in_non_local_environment(self):
        """Any registrar exception must raise in staging/production."""
        from app.config import settings
        from app.services import predictions as pred

        def _boom():
            raise RuntimeError("intentional failure for CR2-REL-006")

        with patch.object(settings, "environment", "staging"), patch.object(
            pred, "_BUILTIN_MODEL_REGISTRARS", [_boom]
        ):
            with pytest.raises(RuntimeError, match="CR2-REL-006"):
                pred.register_builtin_models()

        # The failure must also land in the health snapshot so observers can
        # confirm the abort reason without parsing the exception.
        health = pred.get_builtin_models_health()
        assert health["ok"] is False
        assert health["failed_registrars"], "failed_registrars must be populated"

    def test_degraded_health_in_local_environment(self):
        """Local dev falls open: the process continues but the health
        endpoint reports degraded so operators still see the problem."""
        from app.config import settings
        from app.services import predictions as pred

        def _boom():
            raise RuntimeError("intentional failure for CR2-REL-006")

        with patch.object(settings, "environment", "local"), patch.object(
            pred, "_BUILTIN_MODEL_REGISTRARS", [_boom]
        ):
            # No exception — the call should complete.
            pred.register_builtin_models()

        health = pred.get_builtin_models_health()
        assert health["ok"] is False
        assert any("boom" in f or "tests" in f for f in health["failed_registrars"]) or \
            health["failed_registrars"], "expected at least one failed registrar recorded"

    def test_health_endpoint_returns_200_when_ok(self):
        """`/health/predictions` is 200 with `status=ok` on a healthy
        catalog, no auth required."""
        resp = client.get("/health/predictions")
        assert resp.status_code == 200
        body = resp.json()
        assert body["status"] == "ok"
        assert body["builtin_models"]["ok"] is True
        assert body["builtin_models"]["failed_registrars"] == []

    def test_health_endpoint_returns_503_when_degraded(self):
        """`/health/predictions` returns 503 when any registrar failed."""
        from app.services import predictions as pred

        pred._registration_state["ok"] = False
        pred._registration_state["failed"] = ["app.services.poznanski"]

        resp = client.get("/health/predictions")
        assert resp.status_code == 503
        body = resp.json()
        assert body["status"] == "degraded"
        assert body["builtin_models"]["ok"] is False
        assert body["builtin_models"]["failed_registrars"] == ["app.services.poznanski"]

    def test_successful_registration_clears_prior_failure_state(self):
        """A successful registration must wipe any prior degraded flag so
        operators don't get stale 503s after a recovery."""
        from app.config import settings
        from app.services import predictions as pred

        # Prime the module with a stale failure (simulating a previous
        # startup where a registrar raised).
        pred._registration_state["ok"] = False
        pred._registration_state["failed"] = ["stale.registrar"]

        with patch.object(settings, "environment", "local"), patch.object(
            pred, "_BUILTIN_MODEL_REGISTRARS", []
        ):
            pred.register_builtin_models()

        health = pred.get_builtin_models_health()
        assert health["ok"] is True
        assert health["failed_registrars"] == []
