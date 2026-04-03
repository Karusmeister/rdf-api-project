"""
XML-to-PostgreSQL ETL pipeline.

Reads downloaded/extracted XML files from disk, parses via xml_parser,
and persists structured data into prediction engine tables.
"""

from __future__ import annotations

import json
import logging
import os
from typing import Optional

from app.db import prediction_db
from app.scraper import db as scraper_db
from app.scraper.storage import LocalStorage, make_doc_dir
from app.services import xml_parser

logger = logging.getLogger(__name__)


def _start_etl_attempt(document_id: str, krs: str | None = None) -> int:
    """Create a running etl_attempts row, return its attempt_id."""
    from datetime import datetime, timezone as tz
    conn = prediction_db.get_conn()
    now = datetime.now(tz.utc).isoformat()
    row = conn.execute(
        """
        INSERT INTO etl_attempts
            (document_id, krs, started_at, status)
        VALUES (%s, %s, %s, 'running')
        RETURNING attempt_id
        """,
        [document_id, krs, now],
    ).fetchone()
    return row[0]


def _finish_etl_attempt(
    attempt_id: int,
    *,
    status: str,
    reason_code: str | None = None,
    error_message: str | None = None,
    xml_path: str | None = None,
    report_id: str | None = None,
    extraction_version: int | None = None,
) -> None:
    """Finalise an etl_attempts row with outcome."""
    from datetime import datetime, timezone as tz
    conn = prediction_db.get_conn()
    now = datetime.now(tz.utc).isoformat()
    conn.execute(
        """
        UPDATE etl_attempts
        SET finished_at = %s, status = %s, reason_code = %s, error_message = %s,
            xml_path = %s, report_id = %s, extraction_version = %s
        WHERE attempt_id = %s
        """,
        [now, status, reason_code, error_message, xml_path, report_id,
         extraction_version, attempt_id],
    )


def _find_xml_in_dir(storage: LocalStorage, doc_dir: str) -> Optional[str]:
    """Find the financial statement XML file in an extracted document directory."""
    files = storage.list_files(doc_dir)
    xml_files = [f for f in files if f.lower().endswith(".xml")]
    if not xml_files:
        return None

    # Try to identify the statement XML (not digital signatures)
    for name in xml_files:
        try:
            content = storage.read(f"{doc_dir}/{name}").decode("utf-8")
            root = xml_parser.ET.fromstring(content)
            for el in root.iter():
                tag = el.tag.split("}", 1)[1] if "}" in el.tag else el.tag
                if xml_parser._is_statement_marker(tag):
                    return f"{doc_dir}/{name}"
        except xml_parser.ET.ParseError:
            continue
        except UnicodeDecodeError:
            continue

    # Fallback: return first XML file
    return f"{doc_dir}/{xml_files[0]}" if xml_files else None


def _flatten_tree(node: dict | None, section: str, report_id: str, schema_code: str = "SFJINZ") -> list[dict]:
    """Recursively flatten a parsed tree node into line item dicts."""
    if node is None:
        return []
    items = []
    tag = node.get("tag", "")
    kwota_a = node.get("kwota_a")
    kwota_b = node.get("kwota_b")

    if tag and (kwota_a is not None or kwota_b is not None):
        items.append({
            "report_id": report_id,
            "section": section,
            "tag_path": tag,
            "schema_code": schema_code,
            "label_pl": node.get("label"),
            "value_current": kwota_a,
            "value_previous": kwota_b,
            "currency": "PLN",
        })

    for child in node.get("children", []):
        items.extend(_flatten_tree(child, section, report_id, schema_code))

    return items


def _determine_fiscal_year(period_end: str) -> int:
    """Extract fiscal year from period_end date string (YYYY-MM-DD)."""
    try:
        return int(period_end[:4])
    except (ValueError, IndexError):
        raise ValueError(f"Cannot determine fiscal year from period_end: {period_end}")


def ingest_document(document_id: str, storage: Optional[LocalStorage] = None) -> dict:
    """
    Ingest a single downloaded document into prediction tables.

    Steps:
    1. Read storage_path from krs_documents
    2. Find XML file in extracted directory
    3. Parse via xml_parser
    4. Upsert company
    5. Create financial_report record
    6. Store raw parsed tree as JSON in raw_financial_data
    7. Flatten tree into financial_line_items
    8. Update ingestion_status

    Returns dict with report_id and counts.
    """
    scraper_db.connect()
    prediction_db.connect()

    if storage is None:
        from app.scraper.storage import create_storage
        storage = create_storage()

    # 1. Look up document in scraper DB
    conn = scraper_db.get_conn()
    row = conn.execute(
        "SELECT krs, storage_path, is_downloaded FROM krs_documents_current WHERE document_id = %s",
        [document_id],
    ).fetchone()

    if row is None:
        raise ValueError(f"Document {document_id} not found in krs_documents")

    krs, storage_path, is_downloaded = row

    if not is_downloaded:
        attempt_id = _start_etl_attempt(document_id, krs)
        _finish_etl_attempt(attempt_id, status="skipped", reason_code="not_downloaded")
        raise ValueError(f"Document {document_id} not yet downloaded")

    if not storage_path:
        attempt_id = _start_etl_attempt(document_id, krs)
        _finish_etl_attempt(attempt_id, status="skipped", reason_code="no_storage_path")
        raise ValueError(f"Document {document_id} has no storage_path")

    attempt_id = _start_etl_attempt(document_id, krs)

    # 2. Find XML in extracted directory
    xml_path = _find_xml_in_dir(storage, storage_path)
    if xml_path is None:
        _finish_etl_attempt(attempt_id, status="failed", reason_code="no_xml_found")
        return {"report_id": document_id, "status": "failed", "error": "no_xml_found"}

    # 3. Parse XML
    try:
        xml_bytes = storage.read(xml_path)
        xml_string = xml_bytes.decode("utf-8", errors="replace")
        parsed = xml_parser.parse_statement(xml_string)
    except Exception as e:
        _finish_etl_attempt(
            attempt_id, status="failed",
            reason_code="parse_error", error_message=str(e),
            xml_path=xml_path,
        )
        return {"report_id": document_id, "status": "failed", "error": str(e)}

    # Steps 4-8 are wrapped so that any unexpected error still finalises the attempt.
    xml_path_for_attempt = xml_path
    try:
        company = parsed["company"]
        period_start = company.get("period_start") or "1970-01-01"
        period_end = company.get("period_end") or "1970-01-01"

        try:
            fiscal_year = _determine_fiscal_year(period_end)
        except ValueError:
            fiscal_year = 0

        # 4. Upsert company
        prediction_db.upsert_company(
            krs=krs,
            nip=company.get("nip"),
            pkd_code=company.get("pkd"),
        )

        # 5. Create financial_report
        report_id = document_id
        prediction_db.create_financial_report(
            report_id=report_id,
            krs=krs,
            fiscal_year=fiscal_year,
            period_start=period_start,
            period_end=period_end,
            source_document_id=document_id,
            source_file_path=xml_path,
            schema_code=parsed["company"].get("schema_code", "SFJINZ"),
        )
        prediction_db.update_report_status(report_id, "processing")
        extraction_version = prediction_db.get_next_extraction_version(report_id)

        # 6. Store raw JSON per section
        bilans = parsed.get("bilans", {})
        if bilans.get("aktywa") or bilans.get("pasywa"):
            prediction_db.upsert_raw_financial_data(
                report_id,
                "balance_sheet",
                bilans,
                extraction_version=extraction_version,
            )

        if parsed.get("rzis"):
            prediction_db.upsert_raw_financial_data(
                report_id,
                "income_statement",
                parsed["rzis"],
                extraction_version=extraction_version,
            )

        if parsed.get("cash_flow"):
            prediction_db.upsert_raw_financial_data(
                report_id,
                "cash_flow",
                parsed["cash_flow"],
                extraction_version=extraction_version,
            )

        # 6b. Store extra sections (equity changes, off-balance-sheet, etc.)
        for section_name, section_nodes in parsed.get("extras", {}).items():
            if section_nodes:
                prediction_db.upsert_raw_financial_data(
                    report_id,
                    section_name,
                    section_nodes,
                    extraction_version=extraction_version,
                )

        # 7. Flatten into line_items
        schema_code = parsed["company"].get("schema_code", "SFJINZ")
        line_items = []

        if bilans.get("aktywa"):
            line_items.extend(_flatten_tree(bilans["aktywa"], "Bilans", report_id, schema_code))
        if bilans.get("pasywa"):
            line_items.extend(_flatten_tree(bilans["pasywa"], "Bilans", report_id, schema_code))

        for node in parsed.get("rzis", []):
            line_items.extend(_flatten_tree(node, "RZiS", report_id, schema_code))

        for node in parsed.get("cash_flow", []):
            line_items.extend(_flatten_tree(node, "CF", report_id, schema_code))

        # Flatten extra sections into line_items too
        for section_name, section_nodes in parsed.get("extras", {}).items():
            for node in section_nodes:
                line_items.extend(_flatten_tree(node, section_name, report_id, schema_code))

        if line_items:
            prediction_db.batch_insert_line_items(line_items, extraction_version=extraction_version)

        # 8. Mark completed
        prediction_db.update_report_status(report_id, "completed")

        _finish_etl_attempt(
            attempt_id, status="completed",
            xml_path=xml_path, report_id=report_id,
            extraction_version=extraction_version,
        )
    except Exception as e:
        logger.error(
            "etl_unexpected_error",
            extra={"event": "etl_unexpected_error", "document_id": document_id,
                   "attempt_id": attempt_id, "error": str(e)},
            exc_info=True,
        )
        _finish_etl_attempt(
            attempt_id, status="failed",
            reason_code="unexpected_error",
            error_message=str(e),
            xml_path=xml_path_for_attempt,
        )
        raise

    logger.info(
        "document_ingested",
        extra={
            "event": "document_ingested",
            "document_id": document_id,
            "krs": krs,
            "fiscal_year": fiscal_year,
            "line_items": len(line_items),
        },
    )

    return {
        "report_id": report_id,
        "krs": krs,
        "fiscal_year": fiscal_year,
        "status": "completed",
        "line_items_count": len(line_items),
        "sections": {
            "bilans": bool(bilans.get("aktywa") or bilans.get("pasywa")),
            "rzis": bool(parsed.get("rzis")),
            "cash_flow": bool(parsed.get("cash_flow")),
        },
    }


def ingest_all_pending(storage: Optional[LocalStorage] = None) -> dict:
    """
    Find all downloaded but not-yet-ingested documents, process each.
    Returns summary of results.
    """
    scraper_db.connect()
    prediction_db.connect()

    if storage is None:
        from app.scraper.storage import create_storage
        storage = create_storage()

    conn = scraper_db.get_conn()
    rows = conn.execute("""
        SELECT d.document_id
        FROM krs_documents_current d
        WHERE d.is_downloaded = true
          AND d.document_id NOT IN (
              SELECT fr.source_document_id FROM financial_reports fr
              WHERE fr.source_document_id IS NOT NULL
                AND fr.ingestion_status = 'completed'
          )
    """).fetchall()

    results = {"total": len(rows), "completed": 0, "failed": 0, "errors": []}

    for (doc_id,) in rows:
        try:
            result = ingest_document(doc_id, storage=storage)
            if result["status"] == "completed":
                results["completed"] += 1
            else:
                results["failed"] += 1
                results["errors"].append({"document_id": doc_id, "error": result.get("error")})
        except Exception as e:
            results["failed"] += 1
            results["errors"].append({"document_id": doc_id, "error": str(e)})
            logger.error(
                "ingest_failed",
                extra={"event": "ingest_failed", "document_id": doc_id, "error": str(e)},
                exc_info=True,
            )

    return results


def re_ingest(document_id: str, storage: Optional[LocalStorage] = None) -> dict:
    """Re-parse a document and append a new extraction version."""
    return ingest_document(document_id, storage=storage)
