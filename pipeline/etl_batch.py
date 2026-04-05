"""Batch ETL ingestion for the pipeline database.

Reads document metadata from the scraper DB (rdf-postgres, read-only),
parses XML using the existing xml_parser, and writes to the pipeline DB
(rdf-pipeline) using psycopg2 COPY for bulk performance.

Does NOT modify the scraper DB. Does NOT share code with app/services/etl.py —
that module continues to write to prediction_db (on rdf-postgres). This is a
parallel implementation that targets a different database.
"""
from __future__ import annotations

import io
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

from app.db.connection import ConnectionWrapper
from app.scraper.storage import LocalStorage, create_storage
from app.services import etl as legacy_etl
from app.services import xml_parser

logger = logging.getLogger(__name__)


@dataclass
class BatchResult:
    docs_parsed: int = 0
    docs_failed: int = 0
    line_items_written: int = 0
    report_ids: list[str] = field(default_factory=list)
    errors: list[dict] = field(default_factory=list)


def _lookup_document(scraper_conn: ConnectionWrapper, document_id: str) -> Optional[tuple[str, str, bool]]:
    row = scraper_conn.execute(
        """
        SELECT krs, storage_path, is_downloaded
        FROM krs_documents_current
        WHERE document_id = %s
        """,
        [document_id],
    ).fetchone()
    if row is None:
        return None
    return (row[0], row[1], bool(row[2]))


def _build_logical_key(krs: str, data_source_id: str, report_type: str,
                       fiscal_year: int, period_end: str) -> str:
    return f"{krs}|{data_source_id}|{report_type}|{fiscal_year}|{period_end}"


def _parse_document(
    storage: LocalStorage,
    krs: str,
    storage_path: str,
    document_id: str,
) -> Optional[dict]:
    """Locate + parse the statement XML for one document."""
    xml_path = legacy_etl._find_xml_in_dir(storage, storage_path)
    if xml_path is None:
        return None
    xml_bytes = storage.read(xml_path)
    xml_string = xml_bytes.decode("utf-8", errors="replace")
    parsed = xml_parser.parse_statement(xml_string)
    parsed["_xml_path"] = xml_path
    parsed["_document_id"] = document_id
    parsed["_krs"] = krs
    return parsed


def _copy_line_items(pipeline_conn: ConnectionWrapper, items: list[dict]) -> int:
    """Bulk-insert line items using COPY FROM.

    Fields: report_id, section, tag_path, extraction_version, label_pl,
            value_current, value_previous, currency, schema_code
    """
    if not items:
        return 0

    buf = io.StringIO()
    for it in items:
        # Escape tabs and backslashes to keep TSV safe.
        def esc(v):
            if v is None:
                return "\\N"
            s = str(v)
            return s.replace("\\", "\\\\").replace("\t", " ").replace("\n", " ").replace("\r", " ")

        buf.write("\t".join([
            esc(it["report_id"]),
            esc(it["section"]),
            esc(it["tag_path"]),
            esc(it.get("extraction_version", 1)),
            esc(it.get("label_pl")),
            esc(it.get("value_current")),
            esc(it.get("value_previous")),
            esc(it.get("currency", "PLN")),
            esc(it.get("schema_code", "SFJINZ")),
        ]))
        buf.write("\n")
    buf.seek(0)

    cur = pipeline_conn.raw.cursor()
    cur.copy_expert(
        """
        COPY financial_line_items
            (report_id, section, tag_path, extraction_version, label_pl,
             value_current, value_previous, currency, schema_code)
        FROM STDIN WITH (FORMAT text, DELIMITER E'\t', NULL '\\N')
        """,
        buf,
    )
    return len(items)


def _upsert_company(pipeline_conn: ConnectionWrapper, krs: str, company: dict) -> None:
    pipeline_conn.execute(
        """
        INSERT INTO companies (krs, nip, pkd_code, updated_at)
        VALUES (%s, %s, %s, now())
        ON CONFLICT (krs) DO UPDATE SET
            nip = COALESCE(excluded.nip, companies.nip),
            pkd_code = COALESCE(excluded.pkd_code, companies.pkd_code),
            updated_at = now()
        """,
        [krs, company.get("nip"), company.get("pkd")],
    )


class InvalidPeriodEnd(Exception):
    """Raised when the parsed XML lacks a usable period_end date."""


def _upsert_report(
    pipeline_conn: ConnectionWrapper,
    report_id: str,
    krs: str,
    parsed: dict,
) -> int:
    """Insert / update the financial_reports row. Mirrors app/services/etl.py's
    validation on main: period_end MUST be present and parseable — otherwise
    we raise InvalidPeriodEnd so the caller marks the document failed instead
    of silently writing a 1970-01-01 stub. period_start falls back to
    period_end when missing (also matches main)."""
    company = parsed["company"]
    period_start = company.get("period_start")
    period_end = company.get("period_end")

    if not period_end:
        raise InvalidPeriodEnd("period_end is missing from parsed XML")

    try:
        fiscal_year = int(period_end[:4])
        if fiscal_year < 1900 or fiscal_year > 2999:
            raise ValueError(f"fiscal_year out of range: {fiscal_year}")
    except Exception as exc:
        raise InvalidPeriodEnd(f"could not parse fiscal_year from {period_end!r}: {exc}")

    if not period_start:
        period_start = period_end
    schema_code = company.get("schema_code", "SFJINZ")

    logical_key = _build_logical_key(krs, "KRS", "annual", fiscal_year, period_end)

    existing = pipeline_conn.execute(
        "SELECT id FROM financial_reports WHERE id = %s", [report_id]
    ).fetchone()

    if existing is None:
        # Figure out the report_version based on prior filings for this logical key
        ver_row = pipeline_conn.execute(
            """
            SELECT coalesce(max(report_version), 0) FROM financial_reports
            WHERE logical_key = %s
            """,
            [logical_key],
        ).fetchone()
        report_version = int(ver_row[0] or 0) + 1

        pipeline_conn.execute(
            """
            INSERT INTO financial_reports
                (id, logical_key, report_version, krs, data_source_id, report_type,
                 fiscal_year, period_start, period_end, source_document_id,
                 source_file_path, schema_code, ingestion_status)
            VALUES (%s, %s, %s, %s, 'KRS', 'annual', %s, %s, %s, %s, %s, %s, 'completed')
            ON CONFLICT (id) DO NOTHING
            """,
            [report_id, logical_key, report_version, krs, fiscal_year,
             period_start, period_end, parsed["_document_id"], parsed["_xml_path"],
             schema_code],
        )
    else:
        pipeline_conn.execute(
            """
            UPDATE financial_reports
            SET ingestion_status = 'completed',
                ingestion_error = NULL,
                source_file_path = %s,
                schema_code = %s
            WHERE id = %s
            """,
            [parsed["_xml_path"], schema_code, report_id],
        )
    return fiscal_year


def ingest_batch(
    items: list[tuple[str, str]],
    scraper_conn: ConnectionWrapper,
    pipeline_conn: ConnectionWrapper,
    storage: Optional[LocalStorage] = None,
) -> BatchResult:
    """Ingest a batch of (krs, document_id) pairs into the pipeline database.

    - Parses all XMLs in memory.
    - Accumulates all line items.
    - Upserts reports + companies.
    - Bulk COPYs line items in one call.
    """
    if storage is None:
        storage = create_storage()

    result = BatchResult()
    all_line_items: list[dict] = []

    for krs, document_id in items:
        try:
            lookup = _lookup_document(scraper_conn, document_id)
            if lookup is None:
                result.docs_failed += 1
                result.errors.append({"document_id": document_id, "error": "not_found"})
                continue
            doc_krs, storage_path, is_downloaded = lookup
            if not is_downloaded or not storage_path:
                result.docs_failed += 1
                result.errors.append({"document_id": document_id, "error": "not_downloaded"})
                continue
            effective_krs = krs or doc_krs

            parsed = _parse_document(storage, effective_krs, storage_path, document_id)
            if parsed is None:
                result.docs_failed += 1
                result.errors.append({"document_id": document_id, "error": "no_xml_found"})
                continue

            report_id = document_id
            _upsert_company(pipeline_conn, effective_krs, parsed["company"])
            try:
                _upsert_report(pipeline_conn, report_id, effective_krs, parsed)
            except InvalidPeriodEnd as exc:
                result.docs_failed += 1
                result.errors.append({
                    "document_id": document_id,
                    "error": "invalid_period_end",
                    "detail": str(exc),
                })
                logger.warning(
                    "pipeline_etl_invalid_period_end",
                    extra={
                        "event": "pipeline_etl_invalid_period_end",
                        "document_id": document_id,
                        "detail": str(exc),
                    },
                )
                continue

            schema_code = parsed["company"].get("schema_code", "SFJINZ")
            bilans = parsed.get("bilans", {})
            if bilans.get("aktywa"):
                all_line_items.extend(
                    legacy_etl._flatten_tree(bilans["aktywa"], "Bilans", report_id, schema_code)
                )
            if bilans.get("pasywa"):
                all_line_items.extend(
                    legacy_etl._flatten_tree(bilans["pasywa"], "Bilans", report_id, schema_code)
                )
            for node in parsed.get("rzis", []):
                all_line_items.extend(
                    legacy_etl._flatten_tree(node, "RZiS", report_id, schema_code)
                )
            for node in parsed.get("cash_flow", []):
                all_line_items.extend(
                    legacy_etl._flatten_tree(node, "CF", report_id, schema_code)
                )
            for section_name, section_nodes in parsed.get("extras", {}).items():
                for node in section_nodes or []:
                    all_line_items.extend(
                        legacy_etl._flatten_tree(node, section_name, report_id, schema_code)
                    )

            result.docs_parsed += 1
            result.report_ids.append(report_id)

        except Exception as e:
            result.docs_failed += 1
            result.errors.append({"document_id": document_id, "error": str(e)})
            logger.error(
                "pipeline_etl_doc_failed",
                extra={"event": "pipeline_etl_doc_failed",
                       "document_id": document_id, "error": str(e)},
                exc_info=True,
            )

    # De-duplicate line items by primary key (some sections may produce
    # overlapping tag_paths e.g. extras + rzis). COPY will fail on dup PKs,
    # so we collapse them here and then DELETE pre-existing rows for these
    # reports/extraction versions to keep this operation idempotent.
    dedup: dict[tuple, dict] = {}
    for it in all_line_items:
        it.setdefault("extraction_version", 1)
        key = (it["report_id"], it["section"], it["tag_path"], it["extraction_version"])
        dedup[key] = it
    unique_items = list(dedup.values())

    if unique_items and result.report_ids:
        # Clear prior line items for these reports at extraction_version=1 so
        # re-runs don't conflict with the primary key.
        report_ids_tuple = tuple(set(result.report_ids))
        pipeline_conn.execute(
            """
            DELETE FROM financial_line_items
            WHERE report_id = ANY(%s) AND extraction_version = 1
            """,
            [list(report_ids_tuple)],
        )
        written = _copy_line_items(pipeline_conn, unique_items)
        result.line_items_written = written

    logger.info(
        "pipeline_etl_batch_done",
        extra={"event": "pipeline_etl_batch_done",
               "docs_parsed": result.docs_parsed,
               "docs_failed": result.docs_failed,
               "line_items": result.line_items_written},
    )
    return result
