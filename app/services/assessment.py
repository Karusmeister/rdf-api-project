"""On-demand KRS assessment pipeline.

Orchestrates: data check -> document discovery -> download -> ETL -> features -> scoring.
All progress is tracked via the assessment_jobs table so the frontend can poll.
"""

from __future__ import annotations

import asyncio
import functools
import logging
import uuid
from datetime import datetime, timezone
from typing import Optional

from app import rdf_client
from app.config import settings
from app.db import prediction_db
from app.scraper import db as scraper_db
from app.scraper.storage import LocalStorage, make_doc_dir
from app.services import etl, feature_engine
from app.services import maczynska as maczynska_scorer
from app.services import poznanski as poznanski_scorer

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Data readiness check (sync — runs in executor from route)
# ---------------------------------------------------------------------------


def check_data_readiness(krs: str) -> dict:
    """Check how much data we already have for this KRS. Returns a summary dict."""
    krs = krs.zfill(10)

    known_ids = scraper_db.get_known_document_ids(krs)
    undownloaded = scraper_db.get_undownloaded_documents(krs)
    downloaded_count = len(known_ids) - len(undownloaded)

    ingested_ids = prediction_db.get_ingested_report_ids_for_krs(krs)
    reports = prediction_db.get_reports_for_krs(krs)
    completed_reports = [r for r in reports if r.get("ingestion_status") == "completed"]

    # Check if features exist for latest report
    features_computed = False
    predictions_available = False
    latest_fiscal_year = None

    if completed_reports:
        latest = completed_reports[0]
        latest_fiscal_year = latest.get("fiscal_year")
        features = prediction_db.get_computed_features_for_report(latest["id"], valid_only=True)
        features_computed = len(features) > 0
        preds = prediction_db.get_predictions_fat(krs)
        predictions_available = len(preds) > 0

    return {
        "entity_exists": len(known_ids) > 0,
        "documents_total": len(known_ids),
        "documents_downloaded": downloaded_count,
        "reports_ingested": len(ingested_ids),
        "features_computed": features_computed,
        "predictions_available": predictions_available,
        "latest_fiscal_year": latest_fiscal_year,
    }


def is_data_ready(summary: dict) -> bool:
    """True if all pipeline stages are complete for this KRS."""
    return (
        summary["entity_exists"]
        and summary["documents_total"] > 0
        and summary["documents_downloaded"] == summary["documents_total"]
        and summary["reports_ingested"] > 0
        and summary["features_computed"]
        and summary["predictions_available"]
    )


# ---------------------------------------------------------------------------
# Pipeline orchestrator (async — runs as BackgroundTask)
# ---------------------------------------------------------------------------


def _get_storage() -> LocalStorage:
    return LocalStorage(settings.storage_local_path)


def _update_job(job_id: str, status: str, stage: Optional[str] = None,
                error_message: Optional[str] = None, result: Optional[dict] = None) -> None:
    prediction_db.update_assessment_job(
        job_id, status, stage=stage, error_message=error_message, result=result,
    )


def _update_progress(job_id: str, progress: dict) -> None:
    prediction_db.update_assessment_progress(job_id, progress)


async def _run_in_executor(fn, *args):
    """Run a blocking function in the default threadpool."""
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, functools.partial(fn, *args))


async def run_pipeline(job_id: str, krs: str) -> None:
    """Full on-demand assessment pipeline for a single KRS.

    Called as a BackgroundTask. Updates assessment_jobs at each stage.
    """
    krs = krs.zfill(10)
    delay = settings.scraper_delay_between_requests

    try:
        # ----------------------------------------------------------------
        # Stage 1: Check existing data
        # ----------------------------------------------------------------
        await _run_in_executor(_update_job, job_id, "running", "checking_data")

        summary = await _run_in_executor(check_data_readiness, krs)
        if is_data_ready(summary):
            await _run_in_executor(
                _update_job, job_id, "completed", "checking_data", None, summary,
            )
            return

        # ----------------------------------------------------------------
        # Stage 2: Discover documents from upstream RDF
        # ----------------------------------------------------------------
        await _run_in_executor(_update_job, job_id, "running", "discovering_documents")

        # Validate entity exists
        lookup = await rdf_client.dane_podstawowe(krs)
        if not lookup.get("czyPodmiotZnaleziony", False):
            await _run_in_executor(
                _update_job, job_id, "failed", "discovering_documents",
                "KRS entity not found upstream",
            )
            return

        podmiot = lookup.get("podmiot", {})
        is_active = not bool(podmiot.get("wykreslenie"))
        await _run_in_executor(
            scraper_db.upsert_krs, krs,
            podmiot.get("nazwaPodmiotu"), podmiot.get("formaPrawna"), is_active,
        )
        await asyncio.sleep(delay)

        # Paginate through all documents
        all_docs: list[dict] = []
        page = 0
        while True:
            search_result = await rdf_client.wyszukiwanie(krs, page=page, page_size=100)
            all_docs.extend(search_result.get("content", []))
            total_pages = search_result.get("metadaneWynikow", {}).get("liczbaStron", 1)
            if page + 1 >= total_pages:
                break
            page += 1
            await asyncio.sleep(delay)

        # Persist newly discovered documents
        known_ids = await _run_in_executor(scraper_db.get_known_document_ids, krs)
        new_docs = [d for d in all_docs if d["id"] not in known_ids]
        if new_docs:
            now = datetime.now(timezone.utc)
            rows = [
                {
                    "document_id": d["id"],
                    "krs": krs,
                    "rodzaj": d["rodzaj"],
                    "status": d["status"],
                    "nazwa": d.get("nazwa"),
                    "okres_start": d.get("okresSprawozdawczyPoczatek"),
                    "okres_end": d.get("okresSprawozdawczyKoniec"),
                    "discovered_at": now.isoformat(),
                }
                for d in new_docs
            ]
            await _run_in_executor(scraper_db.insert_documents, rows)

        # ----------------------------------------------------------------
        # Stage 3: Download undownloaded documents
        # ----------------------------------------------------------------
        await _run_in_executor(_update_job, job_id, "running", "downloading")

        to_download = await _run_in_executor(scraper_db.get_undownloaded_documents, krs)
        total_to_download = len(to_download)
        storage = _get_storage()

        progress = {
            "documents_total": len(all_docs),
            "documents_downloaded": len(all_docs) - total_to_download,
            "documents_ingested": 0,
            "features_computed": False,
            "predictions_scored": False,
        }
        await _run_in_executor(_update_progress, job_id, progress)

        for i, doc_id in enumerate(to_download):
            try:
                meta = await rdf_client.metadata(doc_id)
                await asyncio.sleep(delay)

                await _run_in_executor(
                    scraper_db.update_document_metadata, doc_id, {
                        "filename": meta.get("nazwaPliku"),
                        "is_ifrs": meta.get("czyMSR"),
                        "is_correction": meta.get("czyKorekta"),
                        "date_filed": meta.get("dataDodania"),
                        "date_prepared": meta.get("dataSporządzenia"),
                    },
                )

                zip_bytes = await rdf_client.download([doc_id])
                await asyncio.sleep(delay)

                doc_dir = make_doc_dir(krs, doc_id)
                manifest = await _run_in_executor(
                    storage.save_extracted, doc_dir, zip_bytes, doc_id,
                )

                total_extracted = sum(f["size"] for f in manifest["files"])
                file_types = ",".join(sorted(set(f["type"] for f in manifest["files"])))

                await _run_in_executor(
                    scraper_db.mark_downloaded,
                    doc_id, doc_dir, settings.storage_backend,
                    total_extracted, len(zip_bytes), len(manifest["files"]), file_types,
                )

                progress["documents_downloaded"] = len(all_docs) - total_to_download + i + 1
                await _run_in_executor(_update_progress, job_id, progress)

            except Exception:
                logger.warning(
                    "assessment_download_failed",
                    extra={"job_id": job_id, "krs": krs, "doc_id": doc_id},
                    exc_info=True,
                )
                # Continue with other documents

        # ----------------------------------------------------------------
        # Stage 4: ETL ingest
        # ----------------------------------------------------------------
        await _run_in_executor(_update_job, job_id, "running", "etl_ingesting")

        ingested_ids = await _run_in_executor(
            prediction_db.get_ingested_report_ids_for_krs, krs,
        )
        downloaded_ids = await _run_in_executor(scraper_db.get_known_document_ids, krs)
        undownloaded = set(await _run_in_executor(scraper_db.get_undownloaded_documents, krs))
        doc_ids_to_ingest = [
            did for did in downloaded_ids
            if did not in ingested_ids and did not in undownloaded
        ]

        ingested_count = len(ingested_ids)
        for doc_id in doc_ids_to_ingest:
            try:
                await _run_in_executor(etl.ingest_document, doc_id)
                ingested_count += 1
            except Exception:
                logger.warning(
                    "assessment_ingest_failed",
                    extra={"job_id": job_id, "krs": krs, "doc_id": doc_id},
                    exc_info=True,
                )

        progress["documents_ingested"] = ingested_count
        await _run_in_executor(_update_progress, job_id, progress)

        # ----------------------------------------------------------------
        # Stage 5: Compute features
        # ----------------------------------------------------------------
        await _run_in_executor(_update_job, job_id, "running", "computing_features")

        reports = await _run_in_executor(prediction_db.get_reports_for_krs, krs)
        completed_reports = [r for r in reports if r.get("ingestion_status") == "completed"]

        for report in completed_reports:
            try:
                await _run_in_executor(
                    feature_engine.compute_features_for_report, report["id"],
                )
            except Exception:
                logger.warning(
                    "assessment_feature_failed",
                    extra={"job_id": job_id, "krs": krs, "report_id": report["id"]},
                    exc_info=True,
                )

        progress["features_computed"] = True
        await _run_in_executor(_update_progress, job_id, progress)

        # ----------------------------------------------------------------
        # Stage 6: Score with all active models
        # ----------------------------------------------------------------
        await _run_in_executor(_update_job, job_id, "running", "scoring")

        report_ids = [r["id"] for r in completed_reports]
        if report_ids:
            try:
                await _run_in_executor(maczynska_scorer.score_batch, report_ids)
            except Exception:
                logger.warning(
                    "assessment_maczynska_scoring_failed",
                    extra={"job_id": job_id, "krs": krs},
                    exc_info=True,
                )

            try:
                await _run_in_executor(poznanski_scorer.score_batch, report_ids)
            except Exception:
                logger.warning(
                    "assessment_poznanski_scoring_failed",
                    extra={"job_id": job_id, "krs": krs},
                    exc_info=True,
                )

        progress["predictions_scored"] = True
        await _run_in_executor(_update_progress, job_id, progress)

        # ----------------------------------------------------------------
        # Done
        # ----------------------------------------------------------------
        final_summary = await _run_in_executor(check_data_readiness, krs)
        await _run_in_executor(
            _update_job, job_id, "completed", "scoring", None, final_summary,
        )

        logger.info(
            "assessment_pipeline_completed",
            extra={"event": "assessment_pipeline_completed", "job_id": job_id, "krs": krs},
        )

    except Exception:
        logger.exception(
            "assessment_pipeline_failed",
            extra={"event": "assessment_pipeline_failed", "job_id": job_id, "krs": krs},
        )
        await _run_in_executor(
            _update_job, job_id, "failed", None,
            "Internal pipeline error. Check server logs.",
        )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def start_assessment(krs: str) -> tuple[str, bool]:
    """Create or find an assessment job for this KRS.

    Returns (job_id, is_new). If a job is already running for this KRS,
    returns the existing job_id with is_new=False.
    """
    krs = krs.zfill(10)

    existing = prediction_db.get_running_assessment_for_krs(krs)
    if existing:
        return existing["id"], False

    job_id = str(uuid.uuid4())
    prediction_db.create_assessment_job(job_id, krs)
    return job_id, True


def get_job_status(job_id: str) -> Optional[dict]:
    """Get assessment job status for polling."""
    return prediction_db.get_assessment_job(job_id)
