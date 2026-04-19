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
from app.scraper.storage import GcsStorage, LocalStorage, make_doc_dir
from app.services import etl, feature_engine
from app.services import maczynska as maczynska_scorer
from app.services import poznanski as poznanski_scorer
from app.services import maczynska2006 as maczynska2006_scorer
from app.services import prusak as prusak_scorer
from app.services import poznan as poznan_scorer

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Data readiness check (sync — runs in executor from route)
# ---------------------------------------------------------------------------


def _diagnose_no_predictions(krs: str, completed_reports: list[dict]) -> str | None:
    """Determine why scoring produced no predictions for a KRS."""
    from app.db.connection import get_conn as _get_conn

    if not completed_reports:
        conn = _get_conn()
        rows = conn.execute(
            "SELECT DISTINCT file_type FROM krs_documents_current WHERE krs = %s AND rodzaj = '18'",
            [krs],
        ).fetchall()
        found = {r[0] for r in rows}
        if found <= {"other", "unknown", "xhtml"}:
            return "ifrs_xhtml"
        if found <= {"pdf", "unknown"}:
            return "pdf_only"
        return "ingestion_failed"

    schemas = {r.get("schema_code") for r in completed_reports if r.get("schema_code")}
    if schemas <= {"SFJMIZ"}:
        return "micro_entity"
    if schemas <= {"SFZURT"}:
        return "insurance_entity"

    conn = _get_conn()
    tag_check = conn.execute(
        "SELECT count(*) FROM financial_line_items fli JOIN financial_reports fr ON fr.id = fli.report_id WHERE fr.krs = %s AND fli.tag_path = 'RZiS.A' LIMIT 1",
        [krs],
    ).fetchone()
    if tag_check and tag_check[0] == 0:
        return "missing_standard_tags"
    return "scoring_failed"


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

    # Granular scoring coverage
    scoring_coverage = prediction_db.get_scoring_coverage_for_krs(krs)
    models_total = len(scoring_coverage["active_model_ids"])
    models_with_scores = len(scoring_coverage["scored_models"])

    # Diagnose why predictions may be unavailable
    diagnosis = None
    if not predictions_available:
        try:
            diagnosis = _diagnose_no_predictions(krs, completed_reports)
        except Exception:
            logger.debug("diagnosis_failed", exc_info=True)
            diagnosis = "generic"

    return {
        "entity_exists": len(known_ids) > 0,
        "documents_total": len(known_ids),
        "documents_downloaded": downloaded_count,
        "reports_ingested": len(ingested_ids),
        "features_computed": features_computed,
        "predictions_available": predictions_available,
        "models_total": models_total,
        "models_scored": models_with_scores,
        "scoring_gaps": len(scoring_coverage["missing"]),
        "latest_fiscal_year": latest_fiscal_year,
        "diagnosis": diagnosis,
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
        # All active models must have scored at least one report
        and summary.get("models_total", 0) > 0
        and summary.get("models_scored", 0) == summary.get("models_total", 0)
    )


# ---------------------------------------------------------------------------
# Pipeline orchestrator (async — runs as BackgroundTask)
# ---------------------------------------------------------------------------


def _get_storage() -> LocalStorage | GcsStorage:
    if settings.storage_backend == "gcs":
        return GcsStorage(settings.storage_gcs_bucket, settings.storage_gcs_prefix)
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
        # Update progress BEFORE changing stage so the frontend sees the
        # correct already-downloaded count immediately when polling.
        await _run_in_executor(_update_progress, job_id, progress)
        await _run_in_executor(_update_job, job_id, "running", "downloading")

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

        scorer_map = {
            "maczynska_1994_v1": maczynska_scorer,
            "poznanski_2004_v1": poznanski_scorer,
            "maczynska_2006_v1": maczynska2006_scorer,
            "prusak_p1_v1": prusak_scorer,
            "poznan_2000_v1": poznan_scorer,
        }

        report_ids = [r["id"] for r in completed_reports]
        scoring_results: dict = {}
        if report_ids:
            for model_id, scorer in scorer_map.items():
                try:
                    result = await _run_in_executor(scorer.score_batch, report_ids)
                    scoring_results[model_id] = {
                        "scored": result.get("scored", 0),
                        "skipped": result.get("skipped", 0),
                        "errors": result.get("errors", 0),
                    }
                except Exception:
                    scoring_results[model_id] = {"scored": 0, "skipped": 0, "errors": 0, "failed": True}
                    logger.warning(
                        "assessment_scoring_failed",
                        extra={"job_id": job_id, "krs": krs, "model_id": model_id},
                        exc_info=True,
                    )

        progress["predictions_scored"] = True
        progress["scoring_results"] = scoring_results
        await _run_in_executor(_update_progress, job_id, progress)

        # ----------------------------------------------------------------
        # Stage 6b: Verify scoring completeness and repair gaps
        # ----------------------------------------------------------------
        coverage = await _run_in_executor(
            prediction_db.get_scoring_coverage_for_krs, krs,
        )
        gaps = coverage.get("missing", [])

        if gaps:
            logger.info(
                "assessment_scoring_gaps_detected",
                extra={
                    "event": "assessment_scoring_gaps_detected",
                    "job_id": job_id,
                    "krs": krs,
                    "gap_count": len(gaps),
                },
            )

            # Group gaps by model for targeted re-scoring
            from collections import defaultdict
            gaps_by_model: dict[str, list[str]] = defaultdict(list)
            for gap in gaps:
                gaps_by_model[gap["model_id"]].append(gap["report_id"])

            # Re-compute features for reports with gaps (idempotent)
            gap_report_ids = list({g["report_id"] for g in gaps})
            for rid in gap_report_ids:
                try:
                    await _run_in_executor(
                        feature_engine.compute_features_for_report, rid,
                    )
                except Exception:
                    logger.warning(
                        "assessment_repair_feature_failed",
                        extra={"job_id": job_id, "report_id": rid},
                        exc_info=True,
                    )

            # Re-score only the missing report-model combinations
            for model_id, missing_report_ids in gaps_by_model.items():
                scorer = scorer_map.get(model_id)
                if scorer is None:
                    continue
                try:
                    result = await _run_in_executor(scorer.score_batch, missing_report_ids)
                    logger.info(
                        "assessment_repair_scored",
                        extra={
                            "job_id": job_id, "model_id": model_id,
                            "scored": result.get("scored", 0),
                            "skipped": result.get("skipped", 0),
                        },
                    )
                except Exception:
                    logger.warning(
                        "assessment_repair_scoring_failed",
                        extra={"job_id": job_id, "model_id": model_id},
                        exc_info=True,
                    )

        # ----------------------------------------------------------------
        # Done — final summary with scoring completeness
        # ----------------------------------------------------------------
        final_summary = await _run_in_executor(check_data_readiness, krs)
        final_summary["scoring_completeness"] = {
            "models_total": final_summary.get("models_total", 0),
            "models_scored": final_summary.get("models_scored", 0),
            "is_complete": final_summary.get("models_scored", 0) == final_summary.get("models_total", 0),
        }
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
