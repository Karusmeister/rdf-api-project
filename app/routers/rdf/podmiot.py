import logging

from fastapi import APIRouter, HTTPException

from app import rdf_client
from app.routers.rdf.schemas import (
    DocumentTypeResponse,
    KrsRequest,
    LookupResponse,
    PodmiotInfo,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/podmiot", tags=["rdf - podmiot"])


@router.post("/lookup", response_model=LookupResponse)
async def lookup(body: KrsRequest):
    logger.info("entity_lookup", extra={"event": "entity_lookup", "krs": body.krs})
    data = await rdf_client.dane_podstawowe(body.krs)
    if not isinstance(data, dict):
        raise HTTPException(502, "Invalid response structure from upstream")
    podmiot_data = data.get("podmiot")
    podmiot = None
    if podmiot_data and isinstance(podmiot_data, dict):
        podmiot = PodmiotInfo(
            numer_krs=podmiot_data.get("numerKRS", ""),
            nazwa_podmiotu=podmiot_data.get("nazwaPodmiotu", ""),
            forma_prawna=podmiot_data.get("formaPrawna", ""),
            wykreslenie=podmiot_data.get("wykreslenie", False),
        )
    return LookupResponse(
        podmiot=podmiot,
        czy_podmiot_znaleziony=data.get("czyPodmiotZnaleziony", False),
        komunikat_bledu=data.get("komunikatBledu"),
    )


@router.post("/document-types", response_model=list[DocumentTypeResponse])
async def document_types(body: KrsRequest):
    logger.info("document_types_lookup", extra={"event": "document_types_lookup", "krs": body.krs})
    data = await rdf_client.rodzaje_dokumentow(body.krs)
    return [DocumentTypeResponse(nazwa=item["nazwa"]) for item in data]
