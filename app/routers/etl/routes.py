import asyncio
import functools
import logging
from typing import Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field

from app.services import etl, training_data

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/etl", tags=["etl"])


class IngestRequest(BaseModel):
    """ETL ingestion trigger."""

    document_id: Optional[str] = Field(default=None, description="Specific document ID to ingest. Omit to process all pending.")


@router.post("/ingest", summary="Trigger ETL ingestion")
async def ingest(body: IngestRequest):
    """Ingest a specific downloaded document or all pending documents into the prediction tables."""
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
    except ValueError:
        # CR2-SEC-002: ValueError here means "document not found / not ready".
        # Return a stable, safe public message; keep the raw detail in logs.
        logger.warning(
            "etl_ingest_not_found",
            extra={"event": "etl_ingest_not_found", "document_id": body.document_id},
            exc_info=True,
        )
        raise HTTPException(status_code=404, detail="Document not found or not ready for ingestion")
    except Exception:
        # CR2-SEC-002: never surface the raw exception message to clients. The
        # underlying exception can embed database schema, file paths, or stack
        # context. Log the full exception server-side and return a stable
        # error code that operators can correlate via logs.
        logger.error(
            "etl_ingest_error",
            extra={"event": "etl_ingest_error", "document_id": body.document_id},
            exc_info=True,
        )
        raise HTTPException(status_code=500, detail="ETL ingestion failed")


@router.get("/training/dataset-stats", summary="Training dataset statistics")
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
    except ValueError:
        # CR2-SEC-002: the only ValueError callers can trigger here is "feature
        # set not found". Log full context and return a safe public message.
        logger.warning(
            "dataset_stats_not_found",
            extra={"event": "dataset_stats_not_found", "feature_set": feature_set},
            exc_info=True,
        )
        raise HTTPException(status_code=404, detail="Feature set not found")
    except Exception:
        logger.error(
            "dataset_stats_error",
            extra={"event": "dataset_stats_error", "feature_set": feature_set},
            exc_info=True,
        )
        raise HTTPException(status_code=500, detail="Failed to build dataset stats")
