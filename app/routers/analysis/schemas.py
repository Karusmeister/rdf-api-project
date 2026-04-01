from typing import List, Optional

from pydantic import BaseModel, Field


class StatementRequest(BaseModel):
    """Parse a single financial statement."""

    krs: str = Field(pattern=r"^\d{1,10}$", description="KRS number (1-10 digits)")
    period_end: Optional[str] = Field(default=None, description="Reporting period end date (YYYY-MM-DD). Omit for most recent.")

    model_config = {"json_schema_extra": {"examples": [{"krs": "694720", "period_end": "2024-12-31"}]}}


class CompareRequest(BaseModel):
    """Compare two financial statement periods side-by-side."""

    krs: str = Field(pattern=r"^\d{1,10}$", description="KRS number (1-10 digits)")
    period_end_current: str = Field(description="Current period end date (YYYY-MM-DD)")
    period_end_previous: str = Field(description="Previous period end date (YYYY-MM-DD)")

    model_config = {"json_schema_extra": {"examples": [{"krs": "694720", "period_end_current": "2024-12-31", "period_end_previous": "2023-12-31"}]}}


class TimeSeriesRequest(BaseModel):
    """Track selected financial fields across multiple years."""

    krs: str = Field(pattern=r"^\d{1,10}$", description="KRS number (1-10 digits)")
    fields: List[str] = Field(min_length=1, description="Tag paths to track, e.g. ['Aktywa', 'RZiS.A', 'CF.D']")
    period_ends: Optional[List[str]] = Field(
        default=None, description="Filter to specific period end dates (YYYY-MM-DD). Omit for all available years."
    )

    model_config = {"json_schema_extra": {"examples": [{"krs": "694720", "fields": ["Aktywa", "Pasywa_A", "RZiS.A", "RZiS.L"]}]}}
