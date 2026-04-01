from __future__ import annotations

import logging
import secrets
import uuid
from datetime import datetime, timedelta, timezone

import bcrypt as _bcrypt
from fastapi import APIRouter, HTTPException, Request

from app.auth import CurrentUser, create_token
from app.config import settings
from app.db import prediction_db

from .schemas import (
    AuthResponse,
    GoogleLoginRequest,
    GrantAccessRequest,
    LoginRequest,
    SignupRequest,
    SignupResponse,
    UserProfile,
    VerifyRequest,
)

logger = logging.getLogger(__name__)

from app.rate_limit import limiter

router = APIRouter(prefix="/api/auth", tags=["auth"])


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
def signup(request: Request, body: SignupRequest):
    """Create an account with email and password. Sends a 6-digit verification code.
    Rate limited to 5 requests/minute per IP. Retrying for an unverified email resends the code."""
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
def login(body: LoginRequest) -> AuthResponse:
    """Authenticate with email and password. Returns a JWT. Account must be verified and active."""
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
