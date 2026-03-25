import logging
from typing import Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from app.services import etl

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/etl", tags=["etl"])


class IngestRequest(BaseModel):
    document_id: Optional[str] = None


@router.post("/ingest")
async def ingest(body: IngestRequest):
    """Trigger ETL ingestion for a specific document or all pending."""
    logger.info("etl_ingest", extra={"event": "etl_ingest", "document_id": body.document_id})
    try:
        if body.document_id:
            result = etl.ingest_document(body.document_id)
        else:
            result = etl.ingest_all_pending()
        logger.info("etl_ingest_complete", extra={"event": "etl_ingest_complete", "result": result})
        return result
    except ValueError as e:
        logger.warning("etl_ingest_not_found", extra={"event": "etl_ingest_not_found", "error": str(e)})
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error("etl_ingest_error", extra={"event": "etl_ingest_error", "error": str(e)}, exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))
