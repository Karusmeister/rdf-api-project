"""Tests for auth API endpoints (M8/PKR-68-70, M10/PKR-80-83)."""

from unittest.mock import AsyncMock, patch

import httpx
import jwt
import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient

from app.auth import create_token
from app.config import Settings, settings
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
    @patch("app.routers.auth.routes.verify_captcha", new_callable=AsyncMock)
    @patch("app.routers.auth.routes._send_verification_email")
    @patch("app.db.prediction_db.create_verification_code", return_value="code-1")
    @patch("app.db.prediction_db.create_user")
    @patch("app.db.prediction_db.get_user_by_email", return_value=None)
    @patch("app.routers.auth.routes._hash_password", return_value="hashed")
    def test_signup_success(self, mock_hash, mock_get, mock_create, mock_code, mock_send, mock_captcha):
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

    @patch("app.routers.auth.routes.verify_captcha", new_callable=AsyncMock)
    @patch("app.db.prediction_db.get_user_by_email", return_value=_FAKE_USER)
    def test_signup_duplicate_verified_email(self, mock_get, mock_captcha):
        resp = client.post("/api/auth/signup", json={
            "email": "test@example.com",
            "password": "securepass123",
        })
        assert resp.status_code == 409

    @patch("app.routers.auth.routes.verify_captcha", new_callable=AsyncMock)
    @patch("app.routers.auth.routes._send_verification_email")
    @patch("app.db.prediction_db.create_verification_code", return_value="code-1")
    @patch("app.db.prediction_db.get_user_by_email")
    def test_signup_unverified_resends_code(self, mock_get, mock_code, mock_send, mock_captcha):
        mock_get.return_value = {**_FAKE_USER, "is_verified": False}
        resp = client.post("/api/auth/signup", json={
            "email": "test@example.com",
            "password": "securepass123",
        })
        assert resp.status_code == 200
        data = resp.json()
        assert data["message"] == "Verification code sent"
        mock_send.assert_called_once()

    @patch("app.routers.auth.routes.verify_captcha", new_callable=AsyncMock)
    @patch("app.db.prediction_db.delete_unverified_user")
    @patch("app.routers.auth.routes._send_verification_email", side_effect=Exception("SMTP down"))
    @patch("app.db.prediction_db.create_verification_code", return_value="code-1")
    @patch("app.db.prediction_db.create_user")
    @patch("app.db.prediction_db.get_user_by_email", return_value=None)
    @patch("app.routers.auth.routes._hash_password", return_value="hashed")
    def test_signup_email_failure_compensates(self, mock_hash, mock_get, mock_create, mock_code, mock_send, mock_delete, mock_captcha):
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
# POST /api/auth/register (alias for signup)
# ---------------------------------------------------------------------------

class TestRegister:
    @patch("app.routers.auth.routes.verify_captcha", new_callable=AsyncMock)
    @patch("app.routers.auth.routes._send_verification_email")
    @patch("app.db.prediction_db.create_verification_code", return_value="code-1")
    @patch("app.db.prediction_db.create_user")
    @patch("app.db.prediction_db.get_user_by_email", return_value=None)
    @patch("app.routers.auth.routes._hash_password", return_value="hashed")
    def test_register_success(self, mock_hash, mock_get, mock_create, mock_code, mock_send, mock_captcha):
        resp = client.post("/api/auth/register", json={
            "email": "new@example.com",
            "password": "securepass123",
            "name": "New User",
            "captcha_token": "test-token",
        })
        assert resp.status_code == 200
        data = resp.json()
        assert data["message"] == "Verification code sent"
        assert "user_id" in data

    @patch("app.routers.auth.routes.verify_captcha", new_callable=AsyncMock)
    @patch("app.db.prediction_db.get_user_by_email", return_value=_FAKE_USER)
    def test_register_duplicate_email(self, mock_get, mock_captcha):
        resp = client.post("/api/auth/register", json={
            "email": "test@example.com",
            "password": "securepass123",
        })
        assert resp.status_code == 409


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
    @patch("app.routers.auth.routes.verify_captcha", new_callable=AsyncMock)
    @patch("app.db.prediction_db.update_last_login")
    @patch("app.routers.auth.routes._verify_password", return_value=True)
    @patch("app.db.prediction_db.get_user_by_email", return_value=_FAKE_USER)
    def test_login_success(self, mock_get, mock_verify, mock_login, mock_captcha):
        resp = client.post("/api/auth/login", json={
            "email": "test@example.com",
            "password": "securepass123",
        })
        assert resp.status_code == 200
        data = resp.json()
        assert "token" in data
        assert data["user"]["email"] == "test@example.com"

    @patch("app.routers.auth.routes.verify_captcha", new_callable=AsyncMock)
    @patch("app.db.prediction_db.get_user_by_email", return_value=None)
    def test_login_wrong_email(self, mock_get, mock_captcha):
        resp = client.post("/api/auth/login", json={
            "email": "wrong@example.com",
            "password": "securepass123",
        })
        assert resp.status_code == 401

    @patch("app.routers.auth.routes.verify_captcha", new_callable=AsyncMock)
    @patch("app.routers.auth.routes._verify_password", return_value=False)
    @patch("app.db.prediction_db.get_user_by_email", return_value=_FAKE_USER)
    def test_login_wrong_password(self, mock_get, mock_verify, mock_captcha):
        resp = client.post("/api/auth/login", json={
            "email": "test@example.com",
            "password": "wrongpassword",
        })
        assert resp.status_code == 401

    @patch("app.routers.auth.routes.verify_captcha", new_callable=AsyncMock)
    @patch("app.db.prediction_db.get_user_by_email")
    def test_login_unverified(self, mock_get, mock_captcha):
        mock_get.return_value = {**_FAKE_USER, "is_verified": False, "password_hash": "hash"}
        with patch("app.routers.auth.routes._verify_password", return_value=True):
            resp = client.post("/api/auth/login", json={
                "email": "test@example.com",
                "password": "securepass123",
            })
        assert resp.status_code == 403


# ---------------------------------------------------------------------------
# reCAPTCHA verification
# ---------------------------------------------------------------------------

class TestCaptcha:
    @patch("app.routers.auth.routes.verify_captcha", new_callable=AsyncMock)
    @patch("app.routers.auth.routes._send_verification_email")
    @patch("app.db.prediction_db.create_verification_code", return_value="code-1")
    @patch("app.db.prediction_db.create_user")
    @patch("app.db.prediction_db.get_user_by_email", return_value=None)
    @patch("app.routers.auth.routes._hash_password", return_value="hashed")
    def test_captcha_token_passed_to_verify(self, mock_hash, mock_get, mock_create, mock_code, mock_send, mock_captcha):
        """captcha_token is forwarded to verify_captcha."""
        resp = client.post("/api/auth/signup", json={
            "email": "new@example.com",
            "password": "securepass123",
            "captcha_token": "test-captcha-token",
        })
        assert resp.status_code == 200
        mock_captcha.assert_called_once_with("test-captcha-token", "register")

    def test_captcha_skipped_when_no_secret_key(self):
        """When RECAPTCHA_SECRET_KEY is empty, captcha is skipped."""
        import asyncio
        from app.routers.auth.routes import verify_captcha
        asyncio.run(verify_captcha("any-token", "register"))

    def test_captcha_required_when_secret_set_but_no_token(self):
        """When RECAPTCHA_SECRET_KEY is set but no token provided, 400 is raised."""
        import asyncio
        from app.routers.auth.routes import verify_captcha
        with patch.object(settings, "recaptcha_secret_key", "test-secret"):
            with pytest.raises(Exception) as exc_info:
                asyncio.run(verify_captcha(None, "register"))
            assert "required" in str(exc_info.value).lower()

    def test_captcha_timeout_returns_503(self):
        """AUTH-002: Google timeout produces 503, not 500."""
        import asyncio
        from app.routers.auth.routes import verify_captcha
        with patch.object(settings, "recaptcha_secret_key", "test-secret"):
            with patch("app.routers.auth.routes.httpx.AsyncClient") as mock_cls:
                mock_client = AsyncMock()
                mock_client.post.side_effect = httpx.TimeoutException("read timed out")
                mock_client.__aenter__ = AsyncMock(return_value=mock_client)
                mock_client.__aexit__ = AsyncMock(return_value=False)
                mock_cls.return_value = mock_client
                with pytest.raises(HTTPException) as exc_info:
                    asyncio.run(verify_captcha("tok", "login"))
                assert exc_info.value.status_code == 503

    def test_captcha_request_error_returns_503(self):
        """AUTH-002: Network error produces 503."""
        import asyncio
        from app.routers.auth.routes import verify_captcha
        with patch.object(settings, "recaptcha_secret_key", "test-secret"):
            with patch("app.routers.auth.routes.httpx.AsyncClient") as mock_cls:
                mock_client = AsyncMock()
                mock_client.post.side_effect = httpx.ConnectError("refused")
                mock_client.__aenter__ = AsyncMock(return_value=mock_client)
                mock_client.__aexit__ = AsyncMock(return_value=False)
                mock_cls.return_value = mock_client
                with pytest.raises(HTTPException) as exc_info:
                    asyncio.run(verify_captcha("tok", "login"))
                assert exc_info.value.status_code == 503

    def test_captcha_low_score_returns_400(self):
        """AUTH-002: Low score produces 400."""
        import asyncio
        from unittest.mock import MagicMock
        from app.routers.auth.routes import verify_captcha
        mock_resp = MagicMock()
        mock_resp.raise_for_status = MagicMock()
        mock_resp.json.return_value = {"success": True, "score": 0.1, "action": "login"}
        with patch.object(settings, "recaptcha_secret_key", "test-secret"):
            with patch("app.routers.auth.routes.httpx.AsyncClient") as mock_cls:
                mock_client = AsyncMock()
                mock_client.post.return_value = mock_resp
                mock_client.__aenter__ = AsyncMock(return_value=mock_client)
                mock_client.__aexit__ = AsyncMock(return_value=False)
                mock_cls.return_value = mock_client
                with pytest.raises(HTTPException) as exc_info:
                    asyncio.run(verify_captcha("tok", "login"))
                assert exc_info.value.status_code == 400

    def test_captcha_action_mismatch_returns_400(self):
        """AUTH-002: Action mismatch produces 400."""
        import asyncio
        from unittest.mock import MagicMock
        from app.routers.auth.routes import verify_captcha
        mock_resp = MagicMock()
        mock_resp.raise_for_status = MagicMock()
        mock_resp.json.return_value = {"success": True, "score": 0.9, "action": "wrong_action"}
        with patch.object(settings, "recaptcha_secret_key", "test-secret"):
            with patch("app.routers.auth.routes.httpx.AsyncClient") as mock_cls:
                mock_client = AsyncMock()
                mock_client.post.return_value = mock_resp
                mock_client.__aenter__ = AsyncMock(return_value=mock_client)
                mock_client.__aexit__ = AsyncMock(return_value=False)
                mock_cls.return_value = mock_client
                with pytest.raises(HTTPException) as exc_info:
                    asyncio.run(verify_captcha("tok", "login"))
                assert exc_info.value.status_code == 400


# ---------------------------------------------------------------------------
# POST /api/auth/forgot-password
# ---------------------------------------------------------------------------

class TestForgotPassword:
    @patch("app.routers.auth.routes.verify_captcha", new_callable=AsyncMock)
    @patch("app.routers.auth.routes._send_password_reset_email")
    @patch("app.db.prediction_db.create_password_reset_token", return_value="tok-1")
    @patch("app.db.prediction_db.get_user_by_email", return_value=_FAKE_USER)
    def test_forgot_password_sends_email(self, mock_get, mock_token, mock_send, mock_captcha):
        limiter.reset()
        resp = client.post("/api/auth/forgot-password", json={
            "email": "test@example.com",
        })
        assert resp.status_code == 200
        assert "reset link" in resp.json()["message"].lower()
        mock_send.assert_called_once()
        # Token passed to email should be raw (not hashed)
        raw_token = mock_send.call_args[0][1]
        assert len(raw_token) > 30  # urlsafe token is long

    @patch("app.routers.auth.routes.verify_captcha", new_callable=AsyncMock)
    @patch("app.db.prediction_db.get_user_by_email", return_value=None)
    def test_forgot_password_unknown_email_still_200(self, mock_get, mock_captcha):
        """Always returns 200 to avoid leaking whether the email exists."""
        limiter.reset()
        resp = client.post("/api/auth/forgot-password", json={
            "email": "nonexistent@example.com",
        })
        assert resp.status_code == 200

    @patch("app.routers.auth.routes.verify_captcha", new_callable=AsyncMock)
    @patch("app.db.prediction_db.get_user_by_email")
    def test_forgot_password_google_user_ignored(self, mock_get, mock_captcha):
        """Google-auth users don't get reset emails."""
        mock_get.return_value = {**_FAKE_USER, "auth_method": "google"}
        limiter.reset()
        resp = client.post("/api/auth/forgot-password", json={
            "email": "test@example.com",
        })
        assert resp.status_code == 200


# ---------------------------------------------------------------------------
# POST /api/auth/reset-password
# ---------------------------------------------------------------------------

class TestResetPassword:
    @patch("app.db.prediction_db.reset_password_atomic", return_value="user-1")
    @patch("app.routers.auth.routes._hash_password", return_value="new-hashed")
    def test_reset_password_success(self, mock_hash, mock_atomic):
        limiter.reset()
        resp = client.post("/api/auth/reset-password", json={
            "token": "valid-reset-token",
            "new_password": "newSecurePass!",
        })
        assert resp.status_code == 200
        assert "updated" in resp.json()["message"].lower()
        mock_atomic.assert_called_once()

    @patch("app.db.prediction_db.reset_password_atomic", return_value=None)
    def test_reset_password_invalid_token(self, mock_atomic):
        limiter.reset()
        resp = client.post("/api/auth/reset-password", json={
            "token": "expired-or-used-token",
            "new_password": "newSecurePass!",
        })
        assert resp.status_code == 400

    @patch("app.db.prediction_db.reset_password_atomic")
    @patch("app.routers.auth.routes._hash_password", return_value="hashed")
    def test_second_token_fails_after_first_reset(self, mock_hash, mock_atomic):
        """AUTH-003: After one successful reset, all other tokens for the same user are revoked."""
        mock_atomic.side_effect = ["user-1", None]  # first succeeds, second fails
        limiter.reset()
        resp1 = client.post("/api/auth/reset-password", json={
            "token": "first-token",
            "new_password": "newPass123!",
        })
        assert resp1.status_code == 200

        resp2 = client.post("/api/auth/reset-password", json={
            "token": "second-token",
            "new_password": "anotherPass!",
        })
        assert resp2.status_code == 400

    def test_reset_password_short_password(self):
        resp = client.post("/api/auth/reset-password", json={
            "token": "any-token",
            "new_password": "short",
        })
        assert resp.status_code == 422


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
    @patch("app.routers.auth.routes.verify_captcha", new_callable=AsyncMock)
    @patch("app.routers.auth.routes._send_verification_email")
    @patch("app.db.prediction_db.create_verification_code", return_value="code-1")
    @patch("app.db.prediction_db.create_user")
    @patch("app.db.prediction_db.get_user_by_email", return_value=None)
    @patch("app.routers.auth.routes._hash_password", return_value="hashed")
    def test_signup_rate_limit(self, mock_hash, mock_get, mock_create, mock_code, mock_send, mock_captcha):
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


# ---------------------------------------------------------------------------
# AUTH-001: Fail-closed config validation
# ---------------------------------------------------------------------------

class TestAuthConfigValidation:
    def test_production_rejects_empty_captcha_secret(self):
        """AUTH-001: production + empty RECAPTCHA_SECRET_KEY fails at startup."""
        s = Settings(
            environment="production",
            jwt_secret="a" * 32,
            recaptcha_secret_key="",
            auth_require_captcha_in_nonlocal=True,
            verification_email_mode="smtp",
            frontend_url="https://app.example.com",
        )
        with pytest.raises(ValueError, match="RECAPTCHA_SECRET_KEY"):
            s.validate_auth_security()

    def test_production_rejects_log_email_mode(self):
        """AUTH-001: production + verification_email_mode=log fails."""
        s = Settings(
            environment="production",
            jwt_secret="a" * 32,
            recaptcha_secret_key="secret",
            verification_email_mode="log",
            frontend_url="https://app.example.com",
        )
        with pytest.raises(ValueError, match="VERIFICATION_EMAIL_MODE"):
            s.validate_auth_security()

    def test_production_rejects_http_frontend_url(self):
        """AUTH-001: production + http:// frontend_url fails."""
        s = Settings(
            environment="production",
            jwt_secret="a" * 32,
            recaptcha_secret_key="secret",
            verification_email_mode="smtp",
            frontend_url="http://app.example.com",
        )
        with pytest.raises(ValueError, match="https://"):
            s.validate_auth_security()

    def test_production_rejects_localhost_frontend_url(self):
        """AUTH-001: production + localhost frontend_url fails."""
        s = Settings(
            environment="production",
            jwt_secret="a" * 32,
            recaptcha_secret_key="secret",
            verification_email_mode="smtp",
            frontend_url="https://localhost:5173",
        )
        with pytest.raises(ValueError, match="localhost"):
            s.validate_auth_security()

    def test_local_allows_all_defaults(self):
        """AUTH-001: local env allows empty captcha, log mode, http URL."""
        s = Settings(
            environment="local",
            recaptcha_secret_key="",
            verification_email_mode="log",
            frontend_url="http://localhost:5173",
        )
        s.validate_auth_security()  # should not raise

    def test_production_passes_with_valid_config(self):
        """AUTH-001: properly configured production passes."""
        s = Settings(
            environment="production",
            jwt_secret="a" * 32,
            recaptcha_secret_key="6Le...",
            verification_email_mode="smtp",
            frontend_url="https://app.example.com",
        )
        s.validate_auth_security()  # should not raise


# ---------------------------------------------------------------------------
# AUTH-004: No raw reset token in logs
# ---------------------------------------------------------------------------

class TestResetTokenLogSafety:
    def test_log_mode_does_not_emit_raw_token(self, caplog):
        """AUTH-004: log-mode password reset must not contain raw token or ?token= in logs."""
        import logging
        from app.routers.auth.routes import _send_password_reset_email
        raw_token = "super-secret-reset-token-value-that-should-not-appear"
        with caplog.at_level(logging.INFO, logger="app.routers.auth.routes"):
            _send_password_reset_email("user@example.com", raw_token)

        # Raw token and full URL must not appear anywhere in logs
        full_log = caplog.text
        for record in caplog.records:
            full_log += str(getattr(record, "url", ""))
            full_log += str(getattr(record, "token_fingerprint", ""))
        assert raw_token not in full_log
        assert "?token=" not in full_log

        # But the event should still be observable
        assert "password_reset_requested" in caplog.text
        # Email is in the record extras, not in the formatted text
        reset_records = [r for r in caplog.records if r.getMessage() == "password_reset_requested"]
        assert len(reset_records) == 1
        assert getattr(reset_records[0], "email", None) == "user@example.com"
        # Fingerprint should be present and short (12 hex chars)
        fingerprint = getattr(reset_records[0], "token_fingerprint", "")
        assert len(fingerprint) == 12
