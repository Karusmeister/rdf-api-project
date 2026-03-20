from fastapi import APIRouter

from app import rdf_client
from app.routers.rdf.schemas import (
    DocumentTypeResponse,
    KrsRequest,
    LookupResponse,
    PodmiotInfo,
)

router = APIRouter(prefix="/api/podmiot", tags=["rdf - podmiot"])


@router.post("/lookup", response_model=LookupResponse)
async def lookup(body: KrsRequest):
    data = await rdf_client.dane_podstawowe(body.krs)
    podmiot_data = data.get("podmiot")
    podmiot = (
        PodmiotInfo(
            numer_krs=podmiot_data["numerKRS"],
            nazwa_podmiotu=podmiot_data["nazwaPodmiotu"],
            forma_prawna=podmiot_data["formaPrawna"],
            wykreslenie=podmiot_data["wykreslenie"],
        )
        if podmiot_data
        else None
    )
    return LookupResponse(
        podmiot=podmiot,
        czy_podmiot_znaleziony=data["czyPodmiotZnaleziony"],
        komunikat_bledu=data.get("komunikatBledu"),
    )


@router.post("/document-types", response_model=list[DocumentTypeResponse])
async def document_types(body: KrsRequest):
    data = await rdf_client.rodzaje_dokumentow(body.krs)
    return [DocumentTypeResponse(nazwa=item["nazwa"]) for item in data]
