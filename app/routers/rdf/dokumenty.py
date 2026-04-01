import logging

from fastapi import APIRouter, HTTPException
from fastapi.responses import StreamingResponse

from app import rdf_client
from app.routers.rdf.schemas import (
    DocumentItem,
    DownloadRequest,
    PaginationMeta,
    SearchRequest,
    SearchResponse,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/dokumenty", tags=["rdf - dokumenty"])


@router.post("/search", response_model=SearchResponse, summary="Search financial documents")
async def search(body: SearchRequest):
    """Paginated search for financial documents filed by a KRS entity. KRS encryption is handled server-side."""
    logger.info("document_search", extra={"event": "document_search", "krs": body.krs, "page": body.page})
    data = await rdf_client.wyszukiwanie(
        krs=body.krs,
        page=body.page,
        page_size=body.page_size,
        sort_field=body.sort_field,
        sort_dir=body.sort_dir.value,
    )
    if "content" not in data or "metadaneWynikow" not in data:
        raise HTTPException(502, "Invalid response structure from upstream")
    content = [
        DocumentItem(
            id=item["id"],
            rodzaj=item["rodzaj"],
            status=item["status"],
            status_bezpieczenstwa=item.get("statusBezpieczenstwa"),
            nazwa=item.get("nazwa"),
            okres_sprawozdawczy_poczatek=item.get("okresSprawozdawczyPoczatek"),
            okres_sprawozdawczy_koniec=item.get("okresSprawozdawczyKoniec"),
            data_usuniecia_dokumentu=item.get("dataUsunieciaDokumentu"),
        )
        for item in data["content"]
    ]
    meta = data["metadaneWynikow"]
    return SearchResponse(
        content=content,
        metadane_wynikow=PaginationMeta(
            numer_strony=meta["numerStrony"],
            rozmiar_strony=meta["rozmiarStrony"],
            liczba_stron=meta["liczbaStron"],
            calkowita_liczba_obiektow=meta["calkowitaLiczbaObiektow"],
        ),
    )


@router.get("/metadata/{doc_id:path}", summary="Get document metadata")
async def get_metadata(doc_id: str):
    """Return raw metadata for a single document. The doc_id is Base64-encoded and must stay URL-encoded."""
    logger.info("metadata_fetch", extra={"event": "metadata_fetch", "doc_id": doc_id})
    return await rdf_client.metadata(doc_id)


@router.post("/download", summary="Download documents as ZIP")
async def download(body: DownloadRequest):
    """Download one or more financial documents as a ZIP archive. Accepts 1-20 document IDs per request."""
    logger.info("document_download", extra={"event": "document_download", "doc_count": len(body.document_ids)})
    data = await rdf_client.download(body.document_ids)
    return StreamingResponse(
        iter([data]),
        media_type="application/zip",
        headers={"Content-Disposition": "attachment; filename=documents.zip"},
    )
