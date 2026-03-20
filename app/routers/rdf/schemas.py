from enum import Enum
from typing import List, Optional

from pydantic import BaseModel, Field, field_validator


class SortDir(str, Enum):
    DESC = "MALEJACO"
    ASC = "ROSNACO"


# --- Request models ---

class KrsRequest(BaseModel):
    krs: str = Field(..., pattern=r"^\d{1,10}$")


class SearchRequest(BaseModel):
    krs: str = Field(..., pattern=r"^\d{1,10}$")
    page: int = Field(0, ge=0)
    page_size: int = Field(10, ge=1, le=100)
    sort_field: str = Field("id")
    sort_dir: SortDir = Field(SortDir.DESC)


class DownloadRequest(BaseModel):
    document_ids: List[str] = Field(..., min_length=1, max_length=20)


# --- Response models ---

class PodmiotInfo(BaseModel):
    numer_krs: str
    nazwa_podmiotu: str
    forma_prawna: str
    wykreslenie: str


class LookupResponse(BaseModel):
    podmiot: Optional[PodmiotInfo]
    czy_podmiot_znaleziony: bool
    komunikat_bledu: Optional[str]


class DocumentTypeResponse(BaseModel):
    nazwa: str


class DocumentItem(BaseModel):
    id: str
    rodzaj: str
    status: str
    status_bezpieczenstwa: Optional[str]
    nazwa: Optional[str]
    okres_sprawozdawczy_poczatek: Optional[str]
    okres_sprawozdawczy_koniec: Optional[str]
    data_usuniecia_dokumentu: Optional[str]


class PaginationMeta(BaseModel):
    numer_strony: int
    rozmiar_strony: int
    liczba_stron: int
    calkowita_liczba_obiektow: int


class SearchResponse(BaseModel):
    content: List[DocumentItem]
    metadane_wynikow: PaginationMeta
