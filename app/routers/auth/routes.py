from __future__ import annotations

import hashlib
import logging
import secrets
import uuid
from datetime import datetime, timedelta, timezone

import bcrypt as _bcrypt
import httpx
from fastapi import APIRouter, HTTPException, Request

from app import auth_lockout
from app.auth import CurrentUser, create_token
from app.config import settings
from app.db import prediction_db

from .schemas import (
    AuthResponse,
    ForgotPasswordRequest,
    GoogleLoginRequest,
    GrantAccessRequest,
    LoginRequest,
    ResetPasswordRequest,
    SignupRequest,
    SignupResponse,
    UserProfile,
    VerifyRequest,
)

logger = logging.getLogger(__name__)

from app.rate_limit import limiter

router = APIRouter(prefix="/api/auth", tags=["auth"])


# ---------------------------------------------------------------------------
# CR2-AUTH-001 / CR3-AUTH-003: brute-force protection for /login
#
# Two layers run on top of slowapi's per-IP rate limit:
#   1. Per-account lockout: LOGIN_MAX_FAILURES failures inside
#      LOGIN_WINDOW_SECONDS lock the email for LOGIN_LOCKOUT_SECONDS regardless
#      of the source IP. Prevents distributed credential-stuffing from grinding
#      through a single account.
#   2. Per-IP lockout: LOGIN_MAX_FAILURES failures inside LOGIN_WINDOW_SECONDS
#      from one IP locks the IP regardless of which email it targets. Catches
#      credential-stuffing across many accounts from a single host that stays
#      below slowapi's per-IP request-rate ceiling.
#
# CR3-AUTH-003 hardening:
#   * Sliding window counts only failures inside LOGIN_WINDOW_SECONDS. Trickle
#     failures that fall outside the window drop out of the count, so a
#     low-rate attacker cannot slowly accrete a lockout on an innocent user.
#   * State is kept in a bounded LRU store (`LOGIN_STATE_MAX_KEYS` cap).
#     Random-identity floods cannot grow process memory past the cap.
#   * Backend lives behind a `LockoutStore` protocol so a multi-worker Redis
#     backend is a drop-in swap without touching the router.
# ---------------------------------------------------------------------------


def _login_state_key(email: str) -> str:
    return (email or "").strip().lower()


def _check_login_cooldown(email: str, ip: str | None) -> None:
    """Raise 429 if `email` or `ip` is currently locked out.

    Called at the very start of /login so a locked account or IP short-circuits
    without touching the database (avoids acting as a timing oracle too).
    """
    account_key = _login_state_key(email)
    account_wait = auth_lockout.account_lockout_store.is_locked(account_key)
    if account_wait > 0:
        logger.warning(
            "login_account_locked",
            extra={
                "event": "login_account_locked",
                "email": account_key,
                "retry_after_seconds": account_wait,
            },
        )
        raise HTTPException(
            status_code=429,
            detail="Too many failed login attempts. Try again later.",
            headers={"Retry-After": str(account_wait)},
        )

    if ip:
        ip_wait = auth_lockout.ip_lockout_store.is_locked(ip)
        if ip_wait > 0:
            logger.warning(
                "login_ip_locked",
                extra={
                    "event": "login_ip_locked",
                    "ip": ip,
                    "retry_after_seconds": ip_wait,
                },
            )
            raise HTTPException(
                status_code=429,
                detail="Too many failed login attempts. Try again later.",
                headers={"Retry-After": str(ip_wait)},
            )


def _record_login_failure(email: str, ip: str | None) -> None:
    account_key = _login_state_key(email)
    auth_lockout.account_lockout_store.record_failure(account_key)
    if ip:
        auth_lockout.ip_lockout_store.record_failure(ip)


def _record_login_success(email: str, ip: str | None) -> None:
    account_key = _login_state_key(email)
    auth_lockout.account_lockout_store.record_success(account_key)
    if ip:
        auth_lockout.ip_lockout_store.record_success(ip)


def _reset_login_cooldowns() -> None:
    """Test helper — clears the shared lockout stores."""
    auth_lockout.account_lockout_store.clear()
    auth_lockout.ip_lockout_store.clear()


# Re-export tuneables so tests that imported them from this module keep
# working without changes.
LOGIN_MAX_FAILURES = auth_lockout.LOGIN_MAX_FAILURES
LOGIN_LOCKOUT_SECONDS = auth_lockout.LOGIN_LOCKOUT_SECONDS
LOGIN_WINDOW_SECONDS = auth_lockout.LOGIN_WINDOW_SECONDS


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _hash_password(password: str) -> str:
    return _bcrypt.hashpw(password.encode(), _bcrypt.gensalt()).decode()


def _verify_password(password: str, password_hash: str) -> bool:
    return _bcrypt.checkpw(password.encode(), password_hash.encode())


def _send_verification_email(email: str, code: str) -> None:
    if settings.verification_email_mode == "log":
        logger.info(
            "verification_code",
            extra={"event": "verification_code", "email": email, "code": code},
        )
    else:
        import smtplib
        from email.message import EmailMessage

        msg = EmailMessage()
        msg["Subject"] = "Your verification code"
        msg["From"] = settings.smtp_from
        msg["To"] = email
        msg.set_content(f"Your verification code is: {code}\n\nThis code expires in {settings.verification_code_expire_minutes} minutes.")
        with smtplib.SMTP(settings.smtp_host, settings.smtp_port) as s:
            s.starttls()
            s.login(settings.smtp_user, settings.smtp_password)
            s.send_message(msg)


def _send_password_reset_email(email: str, token: str) -> None:
    reset_url = f"{settings.frontend_url}/reset-password?token={token}"
    if settings.verification_email_mode == "log":
        # Log a safe fingerprint — never the raw token or full URL
        token_fingerprint = _hash_token(token)[:12]
        logger.info(
            "password_reset_requested",
            extra={"event": "password_reset_requested", "email": email, "token_fingerprint": token_fingerprint},
        )
    else:
        import smtplib
        from email.message import EmailMessage

        msg = EmailMessage()
        msg["Subject"] = "Password reset request"
        msg["From"] = settings.smtp_from
        msg["To"] = email
        msg.set_content(
            f"Click the link below to reset your password:\n\n{reset_url}\n\n"
            f"This link expires in 1 hour. If you did not request this, ignore this email."
        )
        with smtplib.SMTP(settings.smtp_host, settings.smtp_port) as s:
            s.starttls()
            s.login(settings.smtp_user, settings.smtp_password)
            s.send_message(msg)


def _hash_token(token: str) -> str:
    """SHA-256 hash a reset token for safe DB storage."""
    return hashlib.sha256(token.encode()).hexdigest()


async def verify_captcha(token: str | None, action: str) -> None:
    """Verify a reCAPTCHA v3 token with Google's API.

    Skips verification when RECAPTCHA_SECRET_KEY is not set (dev mode).
    Raises HTTPException(400) for captcha failures, HTTPException(503) for
    provider errors (timeout, network, bad response). Fail-closed: if
    verification cannot complete, the request is denied.
    """
    if not settings.recaptcha_secret_key:
        return  # dev mode — skip

    if not token:
        raise HTTPException(400, "reCAPTCHA token is required")

    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(10.0)) as client:
            resp = await client.post(
                "https://www.google.com/recaptcha/api/siteverify",
                data={
                    "secret": settings.recaptcha_secret_key,
                    "response": token,
                },
            )
            resp.raise_for_status()
    except httpx.TimeoutException:
        logger.warning("captcha_timeout action=%s", action)
        raise HTTPException(503, "Temporary authentication provider failure. Please retry.")
    except (httpx.RequestError, httpx.HTTPStatusError) as exc:
        logger.warning("captcha_request_error action=%s error=%s", action, type(exc).__name__)
        raise HTTPException(503, "Temporary authentication provider failure. Please retry.")

    try:
        result = resp.json()
    except ValueError:
        logger.warning("captcha_invalid_json action=%s", action)
        raise HTTPException(503, "Temporary authentication provider failure. Please retry.")

    if not result.get("success"):
        logger.warning("captcha_failed action=%s errors=%s", action, result.get("error-codes"))
        raise HTTPException(400, "reCAPTCHA verification failed")

    if result.get("score", 0) < 0.5:
        logger.warning("captcha_low_score action=%s score=%s", action, result.get("score"))
        raise HTTPException(400, "reCAPTCHA verification failed")

    if result.get("action") != action:
        logger.warning("captcha_action_mismatch expected=%s got=%s", action, result.get("action"))
        raise HTTPException(400, "reCAPTCHA verification failed")


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.post("/google", summary="Google SSO login")
@limiter.limit("10/minute")
async def google_login(request: Request, body: GoogleLoginRequest) -> AuthResponse:
    """Exchange a Google OAuth2 ID token for a JWT. Auto-creates and verifies the user on first login.

    Rate limited to 10/minute per IP (CR2-AUTH-001) to prevent brute-forcing of
    forged Google ID tokens through this SSO endpoint.
    """
    from google.auth.transport import requests
    from google.oauth2 import id_token

    try:
        info = id_token.verify_oauth2_token(
            body.id_token, requests.Request(), settings.google_client_id
        )
    except ValueError:
        # CR2-SEC-002: never surface the raw ValueError text — the `google-auth`
        # library embeds implementation detail (key IDs, timing info, parser
        # state) in its messages that leaks to clients. Log the exception
        # server-side and return a stable public message.
        logger.warning(
            "google_id_token_invalid",
            extra={"event": "google_id_token_invalid"},
            exc_info=True,
        )
        raise HTTPException(401, "Invalid Google ID token")

    email = info["email"]
    name = info.get("name")

    user = prediction_db.get_user_by_email(email)
    if user is None:
        user_id = str(uuid.uuid4())
        prediction_db.create_user(user_id, email, name, auth_method="google")
        prediction_db.verify_user(user_id)
        user = prediction_db.get_user_by_id(user_id)

    prediction_db.update_last_login(user["id"])
    token = create_token(user["id"], user["email"])
    krs_list = prediction_db.get_user_krs_access(user["id"])
    return AuthResponse(
        token=token,
        user=UserProfile(
            id=user["id"], email=user["email"], name=user.get("name"),
            has_full_access=user.get("has_full_access", False),
            krs_access=krs_list,
        ),
    )


@router.post("/signup", response_model=SignupResponse, summary="Register new account")
@limiter.limit("5/minute")
async def signup(request: Request, body: SignupRequest):
    """Create an account with email and password. Sends a 6-digit verification code.
    Rate limited to 5 requests/minute per IP. Retrying for an unverified email resends the code."""
    await verify_captcha(body.captcha_token, "register")

    existing = prediction_db.get_user_by_email(body.email)
    if existing:
        if existing["is_verified"]:
            raise HTTPException(409, "Email already registered")
        # Unverified account — resend verification code
        user_id = existing["id"]
        code = f"{secrets.randbelow(1000000):06d}"
        expires = datetime.now(timezone.utc) + timedelta(minutes=settings.verification_code_expire_minutes)
        prediction_db.create_verification_code(user_id, code, "email_verify", expires)
        try:
            _send_verification_email(body.email, code)
        except Exception:
            logger.exception("verification_email_failed", extra={"event": "verification_email_failed", "email": body.email})
            raise HTTPException(503, "Failed to send verification email. Please try again.")
        return {"message": "Verification code sent", "user_id": user_id}

    user_id = str(uuid.uuid4())
    hashed = _hash_password(body.password)
    prediction_db.create_user(user_id, body.email, body.name, auth_method="local", password_hash=hashed)

    code = f"{secrets.randbelow(1000000):06d}"
    expires = datetime.now(timezone.utc) + timedelta(minutes=settings.verification_code_expire_minutes)
    prediction_db.create_verification_code(user_id, code, "email_verify", expires)

    try:
        _send_verification_email(body.email, code)
    except Exception:
        logger.exception("verification_email_failed", extra={"event": "verification_email_failed", "email": body.email})
        prediction_db.delete_unverified_user(user_id)
        raise HTTPException(503, "Failed to send verification email. Please try again.")

    return {"message": "Verification code sent", "user_id": user_id}


# /register is an alias for /signup — the frontend uses this path
@router.post("/register", response_model=SignupResponse, summary="Register new account (alias)")
@limiter.limit("5/minute")
async def register(request: Request, body: SignupRequest):
    """Alias for /signup. Creates an account, sends a verification code.
    The frontend must then call /verify with the code before login."""
    return await signup(request, body)


@router.post("/verify", response_model=AuthResponse, summary="Verify email address")
@limiter.limit("10/minute")
def verify_email(request: Request, body: VerifyRequest):
    """Submit the 6-digit code from the signup email. Returns a JWT on success.
    Rate limited to 10 requests/minute per IP."""
    success = prediction_db.consume_verification_code(body.user_id, body.code, "email_verify")
    if not success:
        raise HTTPException(400, "Invalid or expired verification code")

    prediction_db.verify_user(body.user_id)
    user = prediction_db.get_user_by_id(body.user_id)
    if user is None:
        raise HTTPException(404, "User not found")

    token = create_token(user["id"], user["email"])
    krs_list = prediction_db.get_user_krs_access(user["id"])
    return AuthResponse(
        token=token,
        user=UserProfile(
            id=user["id"], email=user["email"], name=user.get("name"),
            has_full_access=user.get("has_full_access", False),
            krs_access=krs_list,
        ),
    )


@router.post("/login", summary="Login with email and password")
@limiter.limit("10/minute")
async def login(request: Request, body: LoginRequest) -> AuthResponse:
    """Authenticate with email and password. Returns a JWT. Account must be verified and active.

    CR2-AUTH-001 protections:
      * slowapi per-IP rate limit (10/minute).
      * Per-account lockout after `LOGIN_MAX_FAILURES` consecutive bad
        passwords — blocks distributed attacks on a single account.
      * Per-IP lockout after `LOGIN_MAX_FAILURES` consecutive failures from
        one host — catches credential stuffing that stays below slowapi's
        request-rate ceiling by hammering different accounts.
    """
    client_ip = request.client.host if request.client else None
    # Check cooldowns BEFORE captcha / DB work so a locked account or IP
    # short-circuits without additional side effects or timing leaks.
    _check_login_cooldown(body.email, client_ip)

    await verify_captcha(body.captcha_token, "login")

    user = prediction_db.get_user_by_email(body.email)
    if user is None:
        _record_login_failure(body.email, client_ip)
        raise HTTPException(401, "Invalid email or password")

    if user["auth_method"] != "local":
        raise HTTPException(400, "This account uses Google sign-in")

    if not user.get("password_hash") or not _verify_password(body.password, user["password_hash"]):
        _record_login_failure(body.email, client_ip)
        raise HTTPException(401, "Invalid email or password")

    if not user["is_verified"]:
        raise HTTPException(403, "Email not verified. Check your inbox for the verification code.")

    if not user["is_active"]:
        raise HTTPException(403, "Account deactivated")

    # Successful login — clear any prior failure streak for this account/IP.
    _record_login_success(body.email, client_ip)

    prediction_db.update_last_login(user["id"])
    token = create_token(user["id"], user["email"])
    krs_list = prediction_db.get_user_krs_access(user["id"])
    return AuthResponse(
        token=token,
        user=UserProfile(
            id=user["id"], email=user["email"], name=user.get("name"),
            has_full_access=user.get("has_full_access", False),
            krs_access=krs_list,
        ),
    )


@router.post("/forgot-password", summary="Request password reset email")
@limiter.limit("5/minute")
async def forgot_password(request: Request, body: ForgotPasswordRequest):
    """Send a password reset link to the registered email.
    Always returns 200 to avoid leaking whether the email exists."""
    await verify_captcha(body.captcha_token, "forgot_password")

    user = prediction_db.get_user_by_email(body.email)
    if user is None or user["auth_method"] != "local":
        # Don't reveal whether the email exists
        return {"message": "If this email is registered, a reset link has been sent."}

    raw_token = secrets.token_urlsafe(48)
    token_hash = _hash_token(raw_token)
    expires = datetime.now(timezone.utc) + timedelta(hours=1)
    prediction_db.create_password_reset_token(user["id"], token_hash, expires)

    try:
        _send_password_reset_email(body.email, raw_token)
    except Exception:
        logger.exception("password_reset_email_failed", extra={"event": "password_reset_email_failed", "email": body.email})
        # Still return 200 to avoid leaking email existence
    return {"message": "If this email is registered, a reset link has been sent."}


@router.post("/reset-password", summary="Reset password with token")
@limiter.limit("10/minute")
def reset_password(request: Request, body: ResetPasswordRequest):
    """Validate a password reset token and set a new password.
    The token is single-use and expires after 1 hour.
    All other outstanding reset tokens for the same user are revoked atomically."""
    token_hash = _hash_token(body.token)
    hashed = _hash_password(body.new_password)
    user_id = prediction_db.reset_password_atomic(token_hash, hashed)
    if user_id is None:
        raise HTTPException(400, "Invalid or expired reset token")

    return {"message": "Password updated successfully"}


@router.get("/me", response_model=UserProfile, summary="Get current user profile")
def me(user: CurrentUser) -> UserProfile:
    """Return the authenticated user's profile including KRS access list."""
    krs_list = prediction_db.get_user_krs_access(user["id"])
    return UserProfile(
        id=user["id"],
        email=user["email"],
        name=user.get("name"),
        has_full_access=user.get("has_full_access", False),
        krs_access=krs_list,
    )


@router.post("/admin/grant-access", summary="Grant KRS access to a user", tags=["admin"])
@limiter.limit("20/minute")
def grant_access(request: Request, body: GrantAccessRequest, user: CurrentUser):
    """Admin-only. Grant a user permission to query predictions for a specific KRS number."""
    if not user.get("has_full_access"):
        raise HTTPException(403, "Admin access required")
    target_user = prediction_db.get_user_by_id(body.user_id)
    if target_user is None:
        raise HTTPException(404, "Target user not found")
    prediction_db.grant_krs_access(body.user_id, body.krs, granted_by=user["id"])
    return {"granted": True}
