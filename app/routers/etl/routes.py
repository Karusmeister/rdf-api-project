import asyncio
import functools
import logging
from typing import Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from app.services import etl, training_data

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/etl", tags=["etl"])


class IngestRequest(BaseModel):
    document_id: Optional[str] = None


@router.post("/ingest")
async def ingest(body: IngestRequest):
    """Trigger ETL ingestion for a specific document or all pending."""
    logger.info("etl_ingest", extra={"event": "etl_ingest", "document_id": body.document_id})
    loop = asyncio.get_running_loop()
    try:
        if body.document_id:
            result = await loop.run_in_executor(
                None, functools.partial(etl.ingest_document, body.document_id)
            )
        else:
            result = await loop.run_in_executor(None, etl.ingest_all_pending)
        logger.info("etl_ingest_complete", extra={"event": "etl_ingest_complete", "result": result})
        return result
    except ValueError as e:
        logger.warning("etl_ingest_not_found", extra={"event": "etl_ingest_not_found", "error": str(e)})
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error("etl_ingest_error", extra={"event": "etl_ingest_error", "error": str(e)}, exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/training/dataset-stats")
async def dataset_stats(
    feature_set: str = Query(..., description="Feature set ID (e.g. 'maczynska_6')"),
    min_year: Optional[int] = Query(None),
    max_year: Optional[int] = Query(None),
):
    """Return summary statistics about the training dataset for a feature set."""
    loop = asyncio.get_running_loop()
    try:
        result = await loop.run_in_executor(
            None,
            functools.partial(
                training_data.get_dataset_stats,
                feature_set, min_year=min_year, max_year=max_year,
            ),
        )
        return result
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error("dataset_stats_error", extra={"event": "dataset_stats_error", "error": str(e)}, exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))
