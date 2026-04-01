from __future__ import annotations

from pydantic import BaseModel, EmailStr, Field


class GoogleLoginRequest(BaseModel):
    """Exchange a Google OAuth2 ID token for a JWT."""

    id_token: str = Field(description="Google OAuth2 ID token from the client-side sign-in flow")

    model_config = {"json_schema_extra": {"examples": [{"id_token": "eyJhbGciOiJSUzI1NiIs..."}]}}


class SignupRequest(BaseModel):
    """Register a new account with email and password."""

    email: EmailStr = Field(description="User email address")
    password: str = Field(min_length=8, description="Password (minimum 8 characters)")
    name: str | None = Field(default=None, description="Display name (optional)")

    model_config = {"json_schema_extra": {"examples": [{"email": "user@example.com", "password": "securepass123", "name": "Jan Kowalski"}]}}


class VerifyRequest(BaseModel):
    """Submit a 6-digit verification code to confirm email ownership."""

    user_id: str = Field(description="User ID returned from the signup endpoint")
    code: str = Field(pattern=r"^\d{6}$", description="6-digit verification code from email")

    model_config = {"json_schema_extra": {"examples": [{"user_id": "550e8400-e29b-41d4-a716-446655440000", "code": "123456"}]}}


class LoginRequest(BaseModel):
    """Authenticate with email and password."""

    email: EmailStr = Field(description="Registered email address")
    password: str = Field(description="Account password")

    model_config = {"json_schema_extra": {"examples": [{"email": "user@example.com", "password": "securepass123"}]}}


class SignupResponse(BaseModel):
    """Response after successful signup. Use `user_id` with the verify endpoint."""

    message: str = Field(description="Status message", examples=["Verification code sent"])
    user_id: str = Field(description="New user's ID — pass to /api/auth/verify")


class UserProfile(BaseModel):
    """Current user profile with access information."""

    id: str = Field(description="User ID")
    email: str = Field(description="Email address")
    name: str | None = Field(default=None, description="Display name")
    has_full_access: bool = Field(default=False, description="True for admin users who can access all KRS numbers")
    krs_access: list[str] = Field(default_factory=list, description="List of KRS numbers this user can query predictions for")


class AuthResponse(BaseModel):
    """JWT token and user profile returned after login or verification."""

    token: str = Field(description="JWT bearer token — include as `Authorization: Bearer <token>`")
    user: UserProfile


class GrantAccessRequest(BaseModel):
    """Grant a user access to query predictions for a specific KRS number."""

    user_id: str = Field(description="Target user's ID")
    krs: str = Field(pattern=r"^\d{1,10}$", description="KRS number to grant access to (1-10 digits)")

    model_config = {"json_schema_extra": {"examples": [{"user_id": "550e8400-e29b-41d4-a716-446655440000", "krs": "0000694720"}]}}
