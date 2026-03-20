from fastapi import APIRouter
from fastapi.responses import StreamingResponse

from app import rdf_client
from app.routers.rdf.schemas import (
    DocumentItem,
    DownloadRequest,
    PaginationMeta,
    SearchRequest,
    SearchResponse,
)

router = APIRouter(prefix="/api/dokumenty", tags=["rdf - dokumenty"])


@router.post("/search", response_model=SearchResponse)
async def search(body: SearchRequest):
    data = await rdf_client.wyszukiwanie(
        krs=body.krs,
        page=body.page,
        page_size=body.page_size,
        sort_field=body.sort_field,
        sort_dir=body.sort_dir.value,
    )
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


@router.get("/metadata/{doc_id:path}")
async def get_metadata(doc_id: str):
    return await rdf_client.metadata(doc_id)


@router.post("/download")
async def download(body: DownloadRequest):
    data = await rdf_client.download(body.document_ids)
    return StreamingResponse(
        iter([data]),
        media_type="application/zip",
        headers={"Content-Disposition": "attachment; filename=documents.zip"},
    )
