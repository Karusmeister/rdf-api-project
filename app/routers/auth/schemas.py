from __future__ import annotations

from pydantic import BaseModel, EmailStr, Field


class GoogleLoginRequest(BaseModel):
    id_token: str


class SignupRequest(BaseModel):
    email: EmailStr
    password: str = Field(min_length=8)
    name: str | None = None


class VerifyRequest(BaseModel):
    user_id: str
    code: str = Field(pattern=r"^\d{6}$")


class LoginRequest(BaseModel):
    email: EmailStr
    password: str


class SignupResponse(BaseModel):
    message: str
    user_id: str


class UserProfile(BaseModel):
    id: str
    email: str
    name: str | None = None
    has_full_access: bool = False
    krs_access: list[str] = []


class AuthResponse(BaseModel):
    token: str
    user: UserProfile


class GrantAccessRequest(BaseModel):
    user_id: str
    krs: str = Field(pattern=r"^\d{1,10}$")
