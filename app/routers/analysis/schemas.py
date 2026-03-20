from typing import List, Optional

from pydantic import BaseModel, Field


class StatementRequest(BaseModel):
    krs: str = Field(..., pattern=r"^\d{1,10}$")
    period_end: Optional[str] = Field(None, description="YYYY-MM-DD — omit for most recent")


class CompareRequest(BaseModel):
    krs: str = Field(..., pattern=r"^\d{1,10}$")
    period_end_current: str = Field(..., description="YYYY-MM-DD")
    period_end_previous: str = Field(..., description="YYYY-MM-DD")


class TimeSeriesRequest(BaseModel):
    krs: str = Field(..., pattern=r"^\d{1,10}$")
    fields: List[str] = Field(..., min_length=1)
    period_ends: Optional[List[str]] = Field(
        None, description="Filter to these period end dates. Omit for all available years."
    )
