"""Pydantic schemas for admin dashboard endpoints."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field


# --- Stats Overview ---

class KrsEntityStats(BaseModel):
    total_entities: int
    entities_with_documents: int


class DocumentStats(BaseModel):
    total_documents: int
    by_rodzaj: dict[str, int]


class ScannerStats(BaseModel):
    cursor_position: int | None
    is_running: bool
    last_run: dict[str, Any] | None


class SyncStats(BaseModel):
    runs_24h: int
    runs_7d: int


class PredictionStats(BaseModel):
    companies_with_predictions: int


class UserStats(BaseModel):
    active_users: int
    admin_users: int


class StatsOverviewResponse(BaseModel):
    krs_entities: KrsEntityStats
    documents: DocumentStats
    scanner: ScannerStats
    sync: SyncStats
    predictions: PredictionStats
    users: UserStats


# --- KRS Coverage ---

class KrsCoverageItem(BaseModel):
    krs: str
    company_name: str | None = None
    legal_form: str | None = None
    first_seen_at: datetime | None = None
    last_checked_at: datetime | None = None
    freshness_days: int | None = None
    doc_types_count: int = 0
    doc_types_present: list[str] = []
    financial_periods: int = 0


class KrsCoverageResponse(BaseModel):
    items: list[KrsCoverageItem]
    total: int
    page: int
    size: int


# --- KRS Detail ---

class KrsDetailResponse(BaseModel):
    company: dict[str, Any] | None = None
    documents: list[dict[str, Any]] = Field(default_factory=list)
    doc_total: int = 0
    sync_history: list[dict[str, Any]] = Field(default_factory=list)
    user_activity: list[dict[str, Any]] = Field(default_factory=list)


class RefreshResponse(BaseModel):
    krs: str
    status: str


# --- User Management ---

class UserWithStats(BaseModel):
    id: str
    email: str
    name: str | None = None
    has_full_access: bool
    is_active: bool
    auth_method: str
    created_at: datetime | None = None
    last_login_at: datetime | None = None
    total_actions: int = 0
    unique_krs_viewed: int = 0
    last_active_at: datetime | None = None


class UsersResponse(BaseModel):
    items: list[UserWithStats]
    total: int
    page: int
    size: int


class ActivityEntry(BaseModel):
    action: str
    krs_number: str | None = None
    detail: dict[str, Any] | None = None
    ip_address: str | None = None
    created_at: datetime | None = None


class UserActivityResponse(BaseModel):
    user: UserWithStats
    items: list[ActivityEntry]
    total: int
    krs_breakdown: dict[str, int]
