from __future__ import annotations

from pydantic import BaseModel, Field


class SourceTag(BaseModel):
    """A single financial line item that feeds into a feature computation."""

    tag_path: str = Field(description="Hierarchical tag path, e.g. 'Aktywa', 'RZiS.A', 'CF.D'")
    label_pl: str | None = Field(default=None, description="Polish label for the tag")
    value_current: float | None = Field(default=None, description="Value for the current reporting period")
    value_previous: float | None = Field(default=None, description="Value for the previous reporting period")
    section: str | None = Field(default=None, description="Statement section: Bilans, RZiS, or CF")
    higher_is_better: bool | None = Field(
        default=None,
        description=(
            "Semantic direction of the tag: true if higher values indicate better "
            "financial health, false if higher values are a negative signal, null "
            "if the tag is neutral or the backend has no opinion. The frontend uses "
            "this to color year-over-year change indicators."
        ),
    )


class FeatureDetail(BaseModel):
    """A computed financial feature with its value, contribution to the score, and source data."""

    feature_id: str = Field(description="Feature definition ID, e.g. 'x1_maczynska'")
    name: str = Field(description="Human-readable feature name")
    category: str | None = Field(default=None, description="Feature category (profitability, liquidity, etc.)")
    value: float | None = Field(default=None, description="Computed feature value")
    contribution: float | None = Field(default=None, description="Feature's contribution to the model score (coefficient * value)")
    formula_description: str | None = Field(default=None, description="Human-readable formula, e.g. 'Gross profit / Total liabilities'")
    source_tags: list[SourceTag] = Field(default_factory=list, description="Financial line items used to compute this feature")


class ResultDetail(BaseModel):
    """Model scoring result."""

    raw_score: float | None = Field(default=None, description="Raw model output (e.g. Z-score)")
    probability: float | None = Field(default=None, description="Calibrated probability, if available")
    classification: int | None = Field(default=None, description="Binary classification: 0 = healthy, 1 = bankruptcy risk")
    risk_category: str | None = Field(default=None, description="Risk bucket: critical, high, medium, or low")
    warnings: list[str] = Field(
        default_factory=list,
        description=(
            "Stable, machine-readable warning codes raised by the scorer for this "
            "prediction. Used to signal non-linear or out-of-distribution conditions "
            "that the model's linear output should not be trusted on alone (for "
            "example `WARNING_NON_LINEAR_LIQUIDITY` on the Poznanski model when the "
            "quick ratio is pathologically high). Clients should render these next "
            "to the risk badge so users understand why a 'safe' score was downgraded."
        ),
    )


class ThresholdDetail(BaseModel):
    """A score interpretation threshold band."""

    label: str = Field(description="Risk label for this band")
    min: float | None = Field(default=None, description="Lower bound (inclusive)")
    max: float | None = Field(default=None, description="Upper bound (exclusive)")
    summary: str = Field(description="Human-readable description of this band")
    is_current: bool = Field(default=False, description="True if the company's current score falls in this band")


class InterpretationDetail(BaseModel):
    """How to interpret a model's score."""

    score_name: str = Field(description="Name of the score metric, e.g. 'Z-score (Zm)'")
    higher_is_better: bool = Field(description="True if higher scores indicate better financial health")
    thresholds: list[ThresholdDetail] = Field(default_factory=list, description="Score interpretation bands")


class DataSourceDetail(BaseModel):
    """Financial report that the prediction was scored against."""

    report_id: str = Field(description="Internal report ID")
    fiscal_year: int = Field(description="Fiscal year of the report")
    period_start: str | None = Field(default=None, description="Reporting period start date (YYYY-MM-DD)")
    period_end: str | None = Field(default=None, description="Reporting period end date (YYYY-MM-DD)")
    report_version: int = Field(default=1, description="Report version (corrections increment this)")
    data_source_id: str = Field(default="KRS", description="Data source identifier")
    ingested_at: str | None = Field(default=None, description="When the report was ingested into the system")


class ModelInfo(BaseModel):
    """Model metadata."""

    model_id: str = Field(description="Unique model identifier, e.g. 'maczynska_1994_v1'")
    model_name: str = Field(description="Model display name")
    model_type: str = Field(description="Model type: discriminant, logistic, etc.")
    model_version: str = Field(description="Model version string")
    is_baseline: bool = Field(default=False, description="True if this is the baseline model")
    description: str | None = Field(default=None, description="Model description")


class PredictionDetail(BaseModel):
    """Complete prediction for one model: score, features, interpretation, and data source."""

    model: ModelInfo
    result: ResultDetail
    interpretation: InterpretationDetail | None = Field(default=None, description="Score interpretation guide with threshold bands")
    features: list[FeatureDetail] = Field(default_factory=list, description="Features used in scoring with values and contributions")
    data_source: DataSourceDetail
    scored_at: str | None = Field(default=None, description="When this prediction was computed")


class CompanyInfo(BaseModel):
    """Basic company identification."""

    krs: str = Field(description="KRS number (10 digits, zero-padded)")
    name: str | None = Field(default=None, description="Company name")
    nip: str | None = Field(default=None, description="Tax identification number (NIP)")
    pkd_code: str | None = Field(default=None, description="Primary PKD activity code")


class HistoryEntry(BaseModel):
    """A single historical prediction score for timeline charts."""

    model_id: str = Field(description="Model identifier")
    model_name: str = Field(description="Model display name")
    model_version: str = Field(description="Model version")
    fiscal_year: int = Field(description="Fiscal year the score applies to")
    raw_score: float | None = Field(default=None, description="Raw model score")
    probability: float | None = Field(default=None, description="Calibrated probability")
    classification: int | None = Field(default=None, description="Binary classification")
    risk_category: str | None = Field(default=None, description="Risk category label")
    scored_at: str | None = Field(default=None, description="When the score was computed")


class PredictionResponse(BaseModel):
    """Full prediction response: company info, latest predictions per model, and score history."""

    company: CompanyInfo
    predictions: list[PredictionDetail] = Field(
        default_factory=list,
        description=(
            "All scored predictions — one entry per (model, fiscal_year). "
            "Each entry carries its own features[] and data_source.fiscal_year, "
            "so historical years light up the full per-year calculation view. "
            "Rescored years are deduped: the latest scoring wins."
        ),
    )
    history: list[HistoryEntry] = Field(
        default_factory=list,
        description=(
            "Score timeline for charting: one entry per (model, fiscal_year), ordered by fiscal year ascending. "
            "When a fiscal year has been rescored multiple times, only the most recent score is returned."
        ),
    )


class HistoryResponse(BaseModel):
    """Prediction score history for a single KRS, optionally filtered by model."""

    krs: str = Field(description="KRS number")
    history: list[HistoryEntry] = Field(
        default_factory=list,
        description=(
            "One entry per (model, fiscal_year), ordered by fiscal year ascending. "
            "Rescored years are deduped — only the latest score per year is returned."
        ),
    )


class ModelCatalogEntry(BaseModel):
    """A model in the catalog with its interpretation guide."""

    model_id: str = Field(description="Unique model identifier")
    model_name: str = Field(description="Display name")
    model_type: str = Field(description="Model type (discriminant, logistic, etc.)")
    model_version: str = Field(description="Version string")
    is_baseline: bool = Field(default=False, description="True if baseline model")
    description: str | None = Field(default=None, description="Model description")
    feature_set_id: str | None = Field(default=None, description="Feature set used by this model")
    interpretation: InterpretationDetail | None = Field(default=None, description="Score interpretation guide")


class ModelsResponse(BaseModel):
    """List of all active prediction models."""

    models: list[ModelCatalogEntry] = Field(default_factory=list, description="Active models")
