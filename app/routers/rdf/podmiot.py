import logging

from fastapi import APIRouter, BackgroundTasks, HTTPException, Request

from app import rdf_client
from app.auth import OptionalUser
from app.routers.rdf.schemas import (
    DocumentTypeResponse,
    KrsRequest,
    LookupResponse,
    PodmiotInfo,
)
from app.services.activity import activity_logger

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/podmiot", tags=["rdf - podmiot"])


@router.post("/lookup", response_model=LookupResponse, summary="Look up a KRS entity")
async def lookup(
    body: KrsRequest,
    request: Request,
    background: BackgroundTasks,
    user: OptionalUser = None,
):
    """Validate a KRS number and return entity details (name, legal form, deregistration status)."""
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
            wykreslenie=podmiot_data.get("wykreslenie"),
        )
    response = LookupResponse(
        podmiot=podmiot,
        czy_podmiot_znaleziony=data.get("czyPodmiotZnaleziony", False),
        komunikat_bledu=data.get("komunikatBledu"),
    )
    background.add_task(
        activity_logger.log,
        user["id"] if user else None,
        "krs_lookup",
        body.krs,
        {
            "found": data.get("czyPodmiotZnaleziony", False),
            "company_name": podmiot_data.get("nazwaPodmiotu") if podmiot_data else None,
        },
        request.client.host if request.client else None,
    )
    return response


@router.post("/document-types", response_model=list[DocumentTypeResponse], summary="List document types for a KRS entity")
async def document_types(body: KrsRequest):
    """Return available document categories (e.g. financial statements, reports) for the given KRS."""
    logger.info("document_types_lookup", extra={"event": "document_types_lookup", "krs": body.krs})
    data = await rdf_client.rodzaje_dokumentow(body.krs)
    return [DocumentTypeResponse(nazwa=item["nazwa"]) for item in data]
