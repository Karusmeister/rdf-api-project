from typing import Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from app.services import etl

router = APIRouter(prefix="/api/etl", tags=["etl"])


class IngestRequest(BaseModel):
    document_id: Optional[str] = None


@router.post("/ingest")
async def ingest(body: IngestRequest):
    """Trigger ETL ingestion for a specific document or all pending."""
    try:
        if body.document_id:
            result = etl.ingest_document(body.document_id)
        else:
            result = etl.ingest_all_pending()
        return result
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
