"""Tests for auth API endpoints (M8/PKR-68, PKR-69, PKR-70)."""

from unittest.mock import patch

import jwt
import pytest
from fastapi.testclient import TestClient

from app.auth import create_token
from app.config import settings
from app.main import app
from app.rate_limit import limiter

client = TestClient(app, raise_server_exceptions=False)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_FAKE_USER = {
    "id": "user-1",
    "email": "test@example.com",
    "name": "Test User",
    "auth_method": "local",
    "password_hash": "$2b$12$KIXQhZ5m5Q5Q5Q5Q5Q5Q5O5Q5Q5Q5Q5Q5Q5Q5Q5Q5Q5Q5Q5Q5Q",
    "is_verified": True,
    "has_full_access": False,
    "is_active": True,
    "created_at": "2026-01-01",
    "last_login_at": None,
}

_FAKE_ADMIN = {
    **_FAKE_USER,
    "id": "admin-1",
    "email": "admin@example.com",
    "has_full_access": True,
}


def _auth_header(user=None):
    u = user or _FAKE_USER
    token = create_token(u["id"], u["email"])
    return {"Authorization": f"Bearer {token}"}


# ---------------------------------------------------------------------------
# JWT token tests
# ---------------------------------------------------------------------------

class TestJWT:
    def test_create_and_decode_token(self):
        token = create_token("user-1", "test@example.com")
        payload = jwt.decode(token, settings.jwt_secret, algorithms=[settings.jwt_algorithm])
        assert payload["sub"] == "user-1"
        assert payload["email"] == "test@example.com"
        assert "exp" in payload

    def test_expired_token(self):
        from datetime import datetime, timedelta, timezone
        payload = {
            "sub": "user-1",
            "email": "test@example.com",
            "exp": datetime.now(timezone.utc) - timedelta(hours=1),
        }
        token = jwt.encode(payload, settings.jwt_secret, algorithm=settings.jwt_algorithm)
        with patch("app.db.prediction_db.get_user_by_id", return_value=_FAKE_USER):
            resp = client.get("/api/auth/me", headers={"Authorization": f"Bearer {token}"})
        assert resp.status_code == 401

    def test_invalid_token(self):
        resp = client.get("/api/auth/me", headers={"Authorization": "Bearer invalid.token.here"})
        assert resp.status_code == 401

    def test_missing_token(self):
        resp = client.get("/api/auth/me")
        assert resp.status_code == 401


# ---------------------------------------------------------------------------
# POST /api/auth/signup
# ---------------------------------------------------------------------------

class TestSignup:
    @patch("app.routers.auth.routes._send_verification_email")
    @patch("app.db.prediction_db.create_verification_code", return_value="code-1")
    @patch("app.db.prediction_db.create_user")
    @patch("app.db.prediction_db.get_user_by_email", return_value=None)
    @patch("app.routers.auth.routes._hash_password", return_value="hashed")
    def test_signup_success(self, mock_hash, mock_get, mock_create, mock_code, mock_send):
        resp = client.post("/api/auth/signup", json={
            "email": "new@example.com",
            "password": "securepass123",
            "name": "New User",
        })
        assert resp.status_code == 200
        data = resp.json()
        assert data["message"] == "Verification code sent"
        assert "user_id" in data
        mock_create.assert_called_once()
        mock_send.assert_called_once()

    @patch("app.db.prediction_db.get_user_by_email", return_value=_FAKE_USER)
    def test_signup_duplicate_verified_email(self, mock_get):
        resp = client.post("/api/auth/signup", json={
            "email": "test@example.com",
            "password": "securepass123",
        })
        assert resp.status_code == 409

    @patch("app.routers.auth.routes._send_verification_email")
    @patch("app.db.prediction_db.create_verification_code", return_value="code-1")
    @patch("app.db.prediction_db.get_user_by_email")
    def test_signup_unverified_resends_code(self, mock_get, mock_code, mock_send):
        mock_get.return_value = {**_FAKE_USER, "is_verified": False}
        resp = client.post("/api/auth/signup", json={
            "email": "test@example.com",
            "password": "securepass123",
        })
        assert resp.status_code == 200
        data = resp.json()
        assert data["message"] == "Verification code sent"
        mock_send.assert_called_once()

    @patch("app.db.prediction_db.delete_unverified_user")
    @patch("app.routers.auth.routes._send_verification_email", side_effect=Exception("SMTP down"))
    @patch("app.db.prediction_db.create_verification_code", return_value="code-1")
    @patch("app.db.prediction_db.create_user")
    @patch("app.db.prediction_db.get_user_by_email", return_value=None)
    @patch("app.routers.auth.routes._hash_password", return_value="hashed")
    def test_signup_email_failure_compensates(self, mock_hash, mock_get, mock_create, mock_code, mock_send, mock_delete):
        """First signup with SMTP failure deletes the unverified user so retry works."""
        limiter.reset()
        resp = client.post("/api/auth/signup", json={
            "email": "new@example.com",
            "password": "securepass123",
        })
        assert resp.status_code == 503
        mock_delete.assert_called_once()

    def test_signup_short_password(self):
        resp = client.post("/api/auth/signup", json={
            "email": "new@example.com",
            "password": "short",
        })
        assert resp.status_code == 422


# ---------------------------------------------------------------------------
# POST /api/auth/verify
# ---------------------------------------------------------------------------

class TestVerify:
    @patch("app.db.prediction_db.get_user_by_id", return_value=_FAKE_USER)
    @patch("app.db.prediction_db.verify_user")
    @patch("app.db.prediction_db.consume_verification_code", return_value=True)
    def test_verify_success(self, mock_consume, mock_verify, mock_get):
        resp = client.post("/api/auth/verify", json={
            "user_id": "user-1",
            "code": "123456",
        })
        assert resp.status_code == 200
        data = resp.json()
        assert "token" in data
        mock_verify.assert_called_once_with("user-1")

    @patch("app.db.prediction_db.consume_verification_code", return_value=False)
    def test_verify_wrong_code(self, mock_consume):
        resp = client.post("/api/auth/verify", json={
            "user_id": "user-1",
            "code": "000000",
        })
        assert resp.status_code == 400

    def test_verify_bad_code_format(self):
        resp = client.post("/api/auth/verify", json={
            "user_id": "user-1",
            "code": "abc",
        })
        assert resp.status_code == 422


# ---------------------------------------------------------------------------
# POST /api/auth/login
# ---------------------------------------------------------------------------

class TestLogin:
    @patch("app.db.prediction_db.update_last_login")
    @patch("app.routers.auth.routes._verify_password", return_value=True)
    @patch("app.db.prediction_db.get_user_by_email", return_value=_FAKE_USER)
    def test_login_success(self, mock_get, mock_verify, mock_login):
        resp = client.post("/api/auth/login", json={
            "email": "test@example.com",
            "password": "securepass123",
        })
        assert resp.status_code == 200
        data = resp.json()
        assert "token" in data
        assert data["user"]["email"] == "test@example.com"

    @patch("app.db.prediction_db.get_user_by_email", return_value=None)
    def test_login_wrong_email(self, mock_get):
        resp = client.post("/api/auth/login", json={
            "email": "wrong@example.com",
            "password": "securepass123",
        })
        assert resp.status_code == 401

    @patch("app.routers.auth.routes._verify_password", return_value=False)
    @patch("app.db.prediction_db.get_user_by_email", return_value=_FAKE_USER)
    def test_login_wrong_password(self, mock_get, mock_verify):
        resp = client.post("/api/auth/login", json={
            "email": "test@example.com",
            "password": "wrongpassword",
        })
        assert resp.status_code == 401

    @patch("app.db.prediction_db.get_user_by_email")
    def test_login_unverified(self, mock_get):
        mock_get.return_value = {**_FAKE_USER, "is_verified": False, "password_hash": "hash"}
        with patch("app.routers.auth.routes._verify_password", return_value=True):
            resp = client.post("/api/auth/login", json={
                "email": "test@example.com",
                "password": "securepass123",
            })
        assert resp.status_code == 403


# ---------------------------------------------------------------------------
# GET /api/auth/me
# ---------------------------------------------------------------------------

class TestMe:
    @patch("app.db.prediction_db.get_user_krs_access", return_value=["0000694720"])
    @patch("app.db.prediction_db.get_user_by_id", return_value=_FAKE_USER)
    def test_me_returns_profile(self, mock_get, mock_access):
        resp = client.get("/api/auth/me", headers=_auth_header())
        assert resp.status_code == 200
        data = resp.json()
        assert data["email"] == "test@example.com"
        assert data["krs_access"] == ["0000694720"]

    @patch("app.db.prediction_db.get_user_krs_access", return_value=[])
    @patch("app.db.prediction_db.get_user_by_id", return_value=_FAKE_ADMIN)
    def test_me_admin_shows_full_access(self, mock_get, mock_access):
        resp = client.get("/api/auth/me", headers=_auth_header(_FAKE_ADMIN))
        assert resp.status_code == 200
        data = resp.json()
        assert data["has_full_access"] is True
        assert data["krs_access"] == []


# ---------------------------------------------------------------------------
# POST /api/auth/admin/grant-access
# ---------------------------------------------------------------------------

class TestGrantAccess:
    @patch("app.db.prediction_db.grant_krs_access")
    @patch("app.db.prediction_db.get_user_by_id")
    def test_admin_grants_access(self, mock_get, mock_grant):
        mock_get.side_effect = lambda uid: _FAKE_ADMIN if uid == "admin-1" else _FAKE_USER
        resp = client.post(
            "/api/auth/admin/grant-access",
            json={"user_id": "user-1", "krs": "0000694720"},
            headers=_auth_header(_FAKE_ADMIN),
        )
        assert resp.status_code == 200
        assert resp.json()["granted"] is True
        mock_grant.assert_called_once_with("user-1", "0000694720", granted_by="admin-1")

    @patch("app.db.prediction_db.get_user_by_id", return_value=_FAKE_USER)
    def test_non_admin_rejected(self, mock_get):
        resp = client.post(
            "/api/auth/admin/grant-access",
            json={"user_id": "user-2", "krs": "0000694720"},
            headers=_auth_header(),
        )
        assert resp.status_code == 403

    @patch("app.db.prediction_db.get_user_by_id")
    def test_unknown_target_user_404(self, mock_get):
        mock_get.side_effect = lambda uid: _FAKE_ADMIN if uid == "admin-1" else None
        resp = client.post(
            "/api/auth/admin/grant-access",
            json={"user_id": "nonexistent", "krs": "0000694720"},
            headers=_auth_header(_FAKE_ADMIN),
        )
        assert resp.status_code == 404


# ---------------------------------------------------------------------------
# Rate limiting
# ---------------------------------------------------------------------------

class TestRateLimiting:
    @patch("app.routers.auth.routes._send_verification_email")
    @patch("app.db.prediction_db.create_verification_code", return_value="code-1")
    @patch("app.db.prediction_db.create_user")
    @patch("app.db.prediction_db.get_user_by_email", return_value=None)
    @patch("app.routers.auth.routes._hash_password", return_value="hashed")
    def test_signup_rate_limit(self, mock_hash, mock_get, mock_create, mock_code, mock_send):
        limiter.reset()
        for i in range(5):
            resp = client.post("/api/auth/signup", json={
                "email": f"user{i}@example.com",
                "password": "securepass123",
            })
            assert resp.status_code == 200, f"Request {i+1} should succeed"
        resp = client.post("/api/auth/signup", json={
            "email": "user99@example.com",
            "password": "securepass123",
        })
        assert resp.status_code == 429
        limiter.reset()

    @patch("app.db.prediction_db.get_user_by_id", return_value=_FAKE_USER)
    @patch("app.db.prediction_db.verify_user")
    @patch("app.db.prediction_db.consume_verification_code", return_value=True)
    def test_verify_rate_limit(self, mock_consume, mock_verify, mock_get):
        limiter.reset()
        for i in range(10):
            resp = client.post("/api/auth/verify", json={
                "user_id": "user-1",
                "code": "123456",
            })
            assert resp.status_code == 200, f"Request {i+1} should succeed"
        resp = client.post("/api/auth/verify", json={
            "user_id": "user-1",
            "code": "123456",
        })
        assert resp.status_code == 429
        limiter.reset()

    @patch("app.db.prediction_db.grant_krs_access")
    @patch("app.db.prediction_db.get_user_by_id")
    def test_grant_access_rate_limit(self, mock_get, mock_grant):
        mock_get.side_effect = lambda uid: _FAKE_ADMIN if uid == "admin-1" else _FAKE_USER
        limiter.reset()
        for i in range(20):
            resp = client.post(
                "/api/auth/admin/grant-access",
                json={"user_id": "user-1", "krs": "0000694720"},
                headers=_auth_header(_FAKE_ADMIN),
            )
            assert resp.status_code == 200, f"Request {i+1} should succeed"
        resp = client.post(
            "/api/auth/admin/grant-access",
            json={"user_id": "user-1", "krs": "0000694720"},
            headers=_auth_header(_FAKE_ADMIN),
        )
        assert resp.status_code == 429
        limiter.reset()
