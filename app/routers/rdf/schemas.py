from enum import Enum
from typing import List, Optional

from pydantic import BaseModel, Field, field_validator


class SortDir(str, Enum):
    """Sort direction for document search results."""

    DESC = "MALEJACO"
    ASC = "ROSNACO"


# --- Request models ---

class KrsRequest(BaseModel):
    """Request with a single KRS number."""

    krs: str = Field(pattern=r"^\d{1,10}$", description="KRS number (1-10 digits, auto-padded to 10)")

    model_config = {"json_schema_extra": {"examples": [{"krs": "694720"}]}}


class SearchRequest(BaseModel):
    """Paginated document search for a KRS entity."""

    krs: str = Field(pattern=r"^\d{1,10}$", description="KRS number (1-10 digits)")
    page: int = Field(0, ge=0, description="Page number (0-based)")
    page_size: int = Field(10, ge=1, le=100, description="Results per page (1-100)")
    sort_field: str = Field("id", description="Field to sort by")
    sort_dir: SortDir = Field(SortDir.DESC, description="Sort direction: MALEJACO (desc) or ROSNACO (asc)")

    model_config = {"json_schema_extra": {"examples": [{"krs": "694720", "page": 0, "page_size": 10, "sort_dir": "MALEJACO"}]}}


class DownloadRequest(BaseModel):
    """Download one or more documents as a ZIP archive."""

    document_ids: List[str] = Field(min_length=1, max_length=20, description="Document IDs to download (1-20)")

    model_config = {"json_schema_extra": {"examples": [{"document_ids": ["ZgsX8Fsncb1PFW07-T4XoQ=="]}]}}


# --- Response models ---

class PodmiotInfo(BaseModel):
    """Entity details from the KRS registry."""

    numer_krs: str = Field(description="10-digit KRS number")
    nazwa_podmiotu: str = Field(description="Entity name")
    forma_prawna: str = Field(description="Legal form")
    wykreslenie: str = Field(description="Deregistration status")


class LookupResponse(BaseModel):
    """Result of a KRS entity lookup."""

    podmiot: Optional[PodmiotInfo] = Field(default=None, description="Entity details, if found")
    czy_podmiot_znaleziony: bool = Field(description="True if the KRS number exists in the registry")
    komunikat_bledu: Optional[str] = Field(default=None, description="Error message from upstream, if any")


class DocumentTypeResponse(BaseModel):
    """A document type available for a KRS entity."""

    nazwa: str = Field(description="Document type name (Polish)")


class DocumentItem(BaseModel):
    """A financial document in the RDF registry."""

    id: str = Field(description="Document ID (Base64-encoded)")
    rodzaj: str = Field(description="Document type code")
    status: str = Field(description="Document status (NIEUSUNIETY = active)")
    status_bezpieczenstwa: Optional[str] = Field(default=None, description="Security status")
    nazwa: Optional[str] = Field(default=None, description="Document name/title")
    okres_sprawozdawczy_poczatek: Optional[str] = Field(default=None, description="Reporting period start (YYYY-MM-DD)")
    okres_sprawozdawczy_koniec: Optional[str] = Field(default=None, description="Reporting period end (YYYY-MM-DD)")
    data_usuniecia_dokumentu: Optional[str] = Field(default=None, description="Deletion date, if removed")


class PaginationMeta(BaseModel):
    """Pagination metadata for search results."""

    numer_strony: int = Field(description="Current page number")
    rozmiar_strony: int = Field(description="Page size")
    liczba_stron: int = Field(description="Total number of pages")
    calkowita_liczba_obiektow: int = Field(description="Total number of matching documents")


class SearchResponse(BaseModel):
    """Paginated document search results."""

    content: List[DocumentItem] = Field(description="Documents on this page")
    metadane_wynikow: PaginationMeta = Field(description="Pagination metadata")
