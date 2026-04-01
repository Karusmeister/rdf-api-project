from __future__ import annotations

from pydantic import BaseModel


class SourceTag(BaseModel):
    tag_path: str
    label_pl: str | None = None
    value_current: float | None = None
    value_previous: float | None = None
    section: str | None = None


class FeatureDetail(BaseModel):
    feature_id: str
    name: str
    category: str | None = None
    value: float | None = None
    contribution: float | None = None
    formula_description: str | None = None
    source_tags: list[SourceTag] = []


class ResultDetail(BaseModel):
    raw_score: float | None = None
    probability: float | None = None
    classification: int | None = None
    risk_category: str | None = None


class ThresholdDetail(BaseModel):
    label: str
    min: float | None = None
    max: float | None = None
    summary: str
    is_current: bool = False


class InterpretationDetail(BaseModel):
    score_name: str
    higher_is_better: bool
    thresholds: list[ThresholdDetail] = []


class DataSourceDetail(BaseModel):
    report_id: str
    fiscal_year: int
    period_start: str | None = None
    period_end: str | None = None
    report_version: int = 1
    data_source_id: str = "KRS"
    ingested_at: str | None = None


class ModelInfo(BaseModel):
    model_id: str
    model_name: str
    model_type: str
    model_version: str
    is_baseline: bool = False
    description: str | None = None


class PredictionDetail(BaseModel):
    model: ModelInfo
    result: ResultDetail
    interpretation: InterpretationDetail | None = None
    features: list[FeatureDetail] = []
    data_source: DataSourceDetail
    scored_at: str | None = None


class CompanyInfo(BaseModel):
    krs: str
    name: str | None = None
    nip: str | None = None
    pkd_code: str | None = None


class HistoryEntry(BaseModel):
    model_id: str
    model_name: str
    model_version: str
    fiscal_year: int
    raw_score: float | None = None
    probability: float | None = None
    classification: int | None = None
    risk_category: str | None = None
    scored_at: str | None = None


class PredictionResponse(BaseModel):
    company: CompanyInfo
    predictions: list[PredictionDetail] = []
    history: list[HistoryEntry] = []


class HistoryResponse(BaseModel):
    krs: str
    history: list[HistoryEntry] = []


class ModelCatalogEntry(BaseModel):
    model_id: str
    model_name: str
    model_type: str
    model_version: str
    is_baseline: bool = False
    description: str | None = None
    feature_set_id: str | None = None
    interpretation: InterpretationDetail | None = None


class ModelsResponse(BaseModel):
    models: list[ModelCatalogEntry] = []
