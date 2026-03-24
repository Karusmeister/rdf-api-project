"""Shared Pydantic models for KRS source adapters."""

from datetime import date, datetime
from typing import Annotated, Any, Optional

from pydantic import BaseModel, BeforeValidator, Field


def _normalize_krs(value: Any) -> Any:
    if isinstance(value, int):
        value = str(value)
    if isinstance(value, str):
        value = value.strip()
        if value.isdigit():
            return value.zfill(10)
    return value


KrsNumber = Annotated[
    str,
    BeforeValidator(_normalize_krs),
    Field(pattern=r"^\d{10}$", description="Zero-padded 10-digit KRS number"),
]


class KrsEntity(BaseModel):
    """Canonical representation of a KRS-registered entity."""

    krs: KrsNumber
    name: str
    legal_form: Optional[str] = None
    status: Optional[str] = None
    registered_at: Optional[date] = None
    last_changed_at: Optional[date] = None
    nip: Optional[str] = None
    regon: Optional[str] = None
    address_city: Optional[str] = None
    address_street: Optional[str] = None
    address_postal_code: Optional[str] = None
    raw: dict[str, Any] = Field(default_factory=dict, description="Full upstream JSON payload")


class SearchResult(BaseModel):
    """A single hit from a search query."""

    krs: KrsNumber
    name: str
    legal_form: Optional[str] = None
    registered_at: Optional[date] = None


class SearchResponse(BaseModel):
    """Paginated search results."""

    results: list[SearchResult]
    total_count: int
    page: int
    page_size: int


class AdapterHealth(BaseModel):
    """Health status of an adapter's underlying data source."""

    source: str
    ok: bool
    latency_ms: int
    checked_at: datetime
