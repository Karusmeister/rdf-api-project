from __future__ import annotations

import asyncio
import json
import logging
import uuid
from datetime import datetime, timezone
from typing import Optional

from app import rdf_client
from app.config import settings
from app.scraper import db
from app.scraper.storage import create_storage, make_doc_dir

logger = logging.getLogger(__name__)


async def run_scraper(
    mode: str = "full_scan",
    specific_krs: Optional[list] = None,
    max_krs: int = 0,
) -> dict:
    """
    Main scraper entry point.

    Modes:
      - full_scan: iterate all KRS in registry, ordered by strategy
      - new_only: only KRS with last_checked_at IS NULL
      - retry_errors: only KRS with check_error_count > 0
      - specific_krs: only the KRS numbers passed in specific_krs list

    Returns dict with run stats.
    """
    db.connect()
    await rdf_client.start()
    storage = create_storage()

    run_id = str(uuid.uuid4())
    config_snap = json.dumps({
        "mode": mode,
        "strategy": settings.scraper_order_strategy,
        "delay_krs": settings.scraper_delay_between_krs,
        "delay_req": settings.scraper_delay_between_requests,
        "max_krs": max_krs or settings.scraper_max_krs_per_run,
    })
    db.create_run(run_id, mode, config_snap)

    stats = {
        "krs_checked": 0,
        "krs_new_found": 0,
        "documents_discovered": 0,
        "documents_downloaded": 0,
        "documents_failed": 0,
        "bytes_downloaded": 0,
    }

    try:
        if mode == "specific_krs" and specific_krs:
            krs_list = [{"krs": k.zfill(10)} for k in specific_krs]
        else:
            effective_max = max_krs or settings.scraper_max_krs_per_run
            limit = effective_max if effective_max > 0 else 999_999_999
            krs_list = db.get_krs_to_check(
                strategy=settings.scraper_order_strategy,
                limit=limit,
                error_backoff_hours=settings.scraper_error_backoff_hours,
            )

        logger.info(
            "scraper_run_started",
            extra={"event": "scraper_run_started", "run_id": run_id, "mode": mode, "krs_count": len(krs_list)},
        )

        for i, krs_entry in enumerate(krs_list):
            krs = krs_entry["krs"]
            try:
                await _process_one_krs(krs, storage, stats)
            except Exception as e:
                logger.error(
                    "scraper_krs_error",
                    extra={"event": "scraper_krs_error", "krs": krs, "error": str(e)},
                    exc_info=True,
                )
                db.update_krs_checked(krs, total_docs=0, total_downloaded=0, error=str(e))

            stats["krs_checked"] += 1
            if (i + 1) % 50 == 0:
                logger.info(
                    "scraper_progress",
                    extra={
                        "event": "scraper_progress",
                        "checked": i + 1,
                        "total": len(krs_list),
                        "docs_downloaded": stats["documents_downloaded"],
                    },
                )

            if i < len(krs_list) - 1:
                await asyncio.sleep(settings.scraper_delay_between_krs)

        db.finish_run(run_id, "completed", stats)

    except KeyboardInterrupt:
        logger.warning("scraper_interrupted", extra={"event": "scraper_interrupted", "run_id": run_id})
        db.finish_run(run_id, "interrupted", stats)
    except Exception as e:
        logger.error(
            "scraper_run_failed",
            extra={"event": "scraper_run_failed", "run_id": run_id, "error": str(e)},
            exc_info=True,
        )
        db.finish_run(run_id, "failed", {**stats, "error_message": str(e)})
        raise
    finally:
        await rdf_client.stop()
        db.close()

    return stats


async def _process_one_krs(krs: str, storage, stats: dict) -> None:
    """
    Process a single KRS:
    1. Validate entity
    2. Search all documents
    3. Find new ones and insert into DB
    4. Download missing ones
    """
    delay = settings.scraper_delay_between_requests

    # Step 1: Validate entity
    lookup = await rdf_client.dane_podstawowe(krs)
    await asyncio.sleep(delay)

    if not lookup.get("czyPodmiotZnaleziony", False):
        logger.debug("scraper_krs_inactive", extra={"event": "scraper_krs_inactive", "krs": krs})
        db.upsert_krs(krs, company_name=None, legal_form=None, is_active=False)
        db.update_krs_checked(krs, total_docs=0, total_downloaded=0, error=None)
        return

    podmiot = lookup["podmiot"]
    is_active = not bool(podmiot.get("wykreslenie"))
    db.upsert_krs(
        krs=krs,
        company_name=podmiot.get("nazwaPodmiotu"),
        legal_form=podmiot.get("formaPrawna"),
        is_active=is_active,
    )

    # Step 2: Fetch all documents (paginated)
    all_docs = []
    page = 0
    while True:
        search_result = await rdf_client.wyszukiwanie(krs, page=page, page_size=100)
        await asyncio.sleep(delay)

        content = search_result.get("content", [])
        all_docs.extend(content)

        meta = search_result.get("metadaneWynikow", {})
        total_pages = meta.get("liczbaStron", 1)
        if page + 1 >= total_pages:
            break
        page += 1

    # Step 3: Find new documents
    known_ids = db.get_known_document_ids(krs)
    new_docs = [d for d in all_docs if d["id"] not in known_ids]

    if new_docs:
        stats["documents_discovered"] += len(new_docs)
        stats["krs_new_found"] += 1

        rows = []
        for d in new_docs:
            rows.append({
                "document_id": d["id"],
                "krs": krs.zfill(10),
                "rodzaj": d["rodzaj"],
                "status": d["status"],
                "nazwa": d.get("nazwa"),
                "okres_start": d.get("okresSprawozdawczyPoczatek"),
                "okres_end": d.get("okresSprawozdawczyKoniec"),
                "discovered_at": datetime.now(timezone.utc).isoformat(),
            })
        db.insert_documents(rows)

    # Step 4: Download documents not yet downloaded
    undownloaded = db.get_undownloaded_documents(krs) or []

    for doc_id in undownloaded:
        try:
            meta = await rdf_client.metadata(doc_id)
            await asyncio.sleep(delay)

            db.update_document_metadata(doc_id, {
                "filename": meta.get("nazwaPliku"),
                "is_ifrs": meta.get("czyMSR"),
                "is_correction": meta.get("czyKorekta"),
                "date_filed": meta.get("dataDodania"),
                "date_prepared": meta.get("dataSporządzenia"),
            })

            zip_bytes = await rdf_client.download([doc_id])
            await asyncio.sleep(delay)

            doc_dir = make_doc_dir(krs, doc_id)
            manifest = storage.save_extracted(doc_dir, zip_bytes, doc_id)

            total_extracted = sum(f["size"] for f in manifest["files"])
            file_types = ",".join(sorted(set(f["type"] for f in manifest["files"])))

            db.mark_downloaded(
                document_id=doc_id,
                storage_path=doc_dir,
                storage_backend=settings.storage_backend,
                file_size=total_extracted,
                zip_size=len(zip_bytes),
                file_count=len(manifest["files"]),
                file_types=file_types,
            )
            stats["documents_downloaded"] += 1
            stats["bytes_downloaded"] += total_extracted

            logger.debug(
                "scraper_doc_extracted",
                extra={
                    "event": "scraper_doc_extracted",
                    "krs": krs,
                    "doc_id": doc_id,
                    "file_count": len(manifest["files"]),
                    "extracted_bytes": total_extracted,
                    "file_types": file_types,
                },
            )

        except Exception as e:
            logger.warning(
                "scraper_doc_failed",
                extra={"event": "scraper_doc_failed", "krs": krs, "doc_id": doc_id, "error": str(e)},
            )
            db.update_document_error(doc_id, str(e))
            stats["documents_failed"] += 1

    # Step 5: Update registry
    total_docs = len(all_docs)
    conn_downloaded = db.get_conn().execute(
        "SELECT count(*) FROM krs_documents_current WHERE krs = %s AND is_downloaded = true", [krs.zfill(10)]
    ).fetchone()[0]
    db.update_krs_checked(krs, total_docs=total_docs, total_downloaded=conn_downloaded, error=None)
