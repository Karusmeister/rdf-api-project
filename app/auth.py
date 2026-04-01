"""
JWT token utilities and FastAPI auth dependencies.

create_token() produces JWTs. get_current_user is a FastAPI dependency
that extracts + validates the current user from Authorization: Bearer header.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Annotated

import jwt
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from app.config import settings
from app.db import prediction_db

security = HTTPBearer(auto_error=False)


def create_token(user_id: str, email: str) -> str:
    payload = {
        "sub": user_id,
        "email": email,
        "exp": datetime.now(timezone.utc) + timedelta(minutes=settings.jwt_expire_minutes),
    }
    return jwt.encode(payload, settings.jwt_secret, algorithm=settings.jwt_algorithm)


def get_current_user(
    credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(security)],
) -> dict:
    if credentials is None:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Not authenticated")
    try:
        payload = jwt.decode(
            credentials.credentials, settings.jwt_secret, algorithms=[settings.jwt_algorithm]
        )
    except jwt.ExpiredSignatureError:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Token expired")
    except jwt.InvalidTokenError:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Invalid token")

    user = prediction_db.get_user_by_id(payload["sub"])
    if user is None or not user["is_active"]:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "User not found or inactive")
    return user


CurrentUser = Annotated[dict, Depends(get_current_user)]


def get_optional_user(
    credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(security)],
) -> dict | None:
    if credentials is None:
        return None
    try:
        return get_current_user(credentials)
    except HTTPException:
        return None


OptionalUser = Annotated[dict | None, Depends(get_optional_user)]


def require_admin(user: dict) -> None:
    if not user.get("has_full_access"):
        raise HTTPException(status.HTTP_403_FORBIDDEN, "Admin access required")


def require_krs_access(krs: str, user: dict) -> None:
    if user.get("has_full_access"):
        return
    if not prediction_db.check_krs_access(user["id"], krs):
        raise HTTPException(
            status.HTTP_403_FORBIDDEN,
            "Insufficient permissions. Contact admin to request access.",
        )
