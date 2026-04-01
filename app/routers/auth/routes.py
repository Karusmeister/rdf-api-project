from __future__ import annotations

import hashlib
import logging
import secrets
import uuid
from datetime import datetime, timedelta, timezone

import bcrypt as _bcrypt
import httpx
from fastapi import APIRouter, HTTPException, Request

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
        logger.info(
            "password_reset_link",
            extra={"event": "password_reset_link", "email": email, "url": reset_url},
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
    Raises HTTPException(400) on failure.
    """
    if not settings.recaptcha_secret_key:
        return  # dev mode — skip

    if not token:
        raise HTTPException(400, "reCAPTCHA token is required")

    async with httpx.AsyncClient() as client:
        resp = await client.post(
            "https://www.google.com/recaptcha/api/siteverify",
            data={
                "secret": settings.recaptcha_secret_key,
                "response": token,
            },
        )

    result = resp.json()

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
def google_login(body: GoogleLoginRequest) -> AuthResponse:
    """Exchange a Google OAuth2 ID token for a JWT. Auto-creates and verifies the user on first login."""
    from google.auth.transport import requests
    from google.oauth2 import id_token

    try:
        info = id_token.verify_oauth2_token(
            body.id_token, requests.Request(), settings.google_client_id
        )
    except ValueError as e:
        raise HTTPException(401, f"Invalid Google ID token: {e}")

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
    return AuthResponse(
        token=token,
        user=UserProfile(
            id=user["id"], email=user["email"], name=user.get("name"),
            has_full_access=user.get("has_full_access", False),
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


# /register is an alias for /signup — the Lovable frontend uses this path
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
    return AuthResponse(
        token=token,
        user=UserProfile(
            id=user["id"], email=user["email"], name=user.get("name"),
            has_full_access=user.get("has_full_access", False),
        ),
    )


@router.post("/login", summary="Login with email and password")
async def login(body: LoginRequest) -> AuthResponse:
    """Authenticate with email and password. Returns a JWT. Account must be verified and active."""
    await verify_captcha(body.captcha_token, "login")

    user = prediction_db.get_user_by_email(body.email)
    if user is None:
        raise HTTPException(401, "Invalid email or password")

    if user["auth_method"] != "local":
        raise HTTPException(400, "This account uses Google sign-in")

    if not user.get("password_hash") or not _verify_password(body.password, user["password_hash"]):
        raise HTTPException(401, "Invalid email or password")

    if not user["is_verified"]:
        raise HTTPException(403, "Email not verified. Check your inbox for the verification code.")

    if not user["is_active"]:
        raise HTTPException(403, "Account deactivated")

    prediction_db.update_last_login(user["id"])
    token = create_token(user["id"], user["email"])
    return AuthResponse(
        token=token,
        user=UserProfile(
            id=user["id"], email=user["email"], name=user.get("name"),
            has_full_access=user.get("has_full_access", False),
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
    The token is single-use and expires after 1 hour."""
    token_hash = _hash_token(body.token)
    user_id = prediction_db.consume_password_reset_token(token_hash)
    if user_id is None:
        raise HTTPException(400, "Invalid or expired reset token")

    hashed = _hash_password(body.new_password)
    prediction_db.update_password(user_id, hashed)
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
