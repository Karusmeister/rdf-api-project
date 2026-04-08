"""Pydantic models for company search and health metrics endpoints."""

from __future__ import annotations

from pydantic import BaseModel, Field


# --- PKR-124: Company search ---


class CompanySearchResult(BaseModel):
    krs: str = Field(description="KRS number (10 digits)")
    name: str | None = Field(default=None, description="Company name")
    nip: str | None = Field(default=None, description="Tax ID (NIP)")
    pkd_code: str | None = Field(default=None, description="Primary PKD activity code")
    legal_form: str | None = Field(default=None, description="Legal form")
    status: str | None = Field(default=None, description="active or inactive")
    has_predictions: bool = Field(default=False, description="Whether predictions exist for this company")


class CompanySearchResponse(BaseModel):
    results: list[CompanySearchResult] = Field(default_factory=list)
    total_count: int = 0
    query: str


class PopularCompany(BaseModel):
    krs: str
    name: str | None = None
    click_count: int = 0


class PopularCompaniesResponse(BaseModel):
    results: list[PopularCompany] = Field(default_factory=list)


# --- PKR-121: Health metrics ---


class HealthMetricPoint(BaseModel):
    fiscal_year: int
    value: float | None = None


class HealthMetric(BaseModel):
    current_value: float | None = None
    label: str | None = None
    history: list[HealthMetricPoint] = Field(default_factory=list)


class HealthMetricsResponse(BaseModel):
    krs: str
    metrics: dict[str, HealthMetric] = Field(default_factory=dict)
