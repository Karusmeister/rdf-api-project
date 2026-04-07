"""Pydantic models for the on-demand assessment pipeline."""

from __future__ import annotations

from typing import Optional

from pydantic import BaseModel


class DataSummary(BaseModel):
    entity_exists: bool
    documents_total: int
    documents_downloaded: int
    reports_ingested: int
    features_computed: bool
    predictions_available: bool
    latest_fiscal_year: Optional[int] = None


class AssessmentProgress(BaseModel):
    documents_total: int = 0
    documents_downloaded: int = 0
    documents_ingested: int = 0
    features_computed: bool = False
    predictions_scored: bool = False


class StartAssessmentResponse(BaseModel):
    job_id: Optional[str] = None
    krs: str
    status: str
    stage: Optional[str] = None
    message: str
    data_summary: Optional[DataSummary] = None


class JobStatusResponse(BaseModel):
    job_id: str
    krs: str
    status: str
    stage: Optional[str] = None
    progress: Optional[AssessmentProgress] = None
    error_message: Optional[str] = None
    created_at: str
    updated_at: str
