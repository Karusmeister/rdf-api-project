"""
Analysis endpoints: parse, compare, and trend financial statements.
All XML downloading/parsing happens server-side.
"""

import asyncio
import logging
from typing import Optional

from fastapi import APIRouter, BackgroundTasks, HTTPException, Request

from app import rdf_client
from app.auth import OptionalUser
from app.routers.analysis.schemas import CompareRequest, StatementRequest, TimeSeriesRequest
from app.services import xml_parser
from app.services.activity import activity_logger

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/analysis", tags=["analysis"])


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

async def _get_available_periods(krs: str) -> list[dict]:
    """
    Return available Polish-GAAP financial statement periods for a KRS,
    preferring corrections over originals for the same period.
    Paginates through all search pages so companies with > 100 filings are complete.
    Only caches the result when all metadata calls succeed; transient upstream
    failures are not persisted as empty/partial period lists.
    """
    cache_key = f"periods:{krs}"
    cached = xml_parser.cache_get(cache_key)
    if cached is not None:
        return cached

    # Paginate through all search pages (bounded to prevent DoS)
    _MAX_SEARCH_PAGES = 100
    all_docs: list[dict] = []
    page = 0
    while True:
        if page >= _MAX_SEARCH_PAGES:
            logger.warning("max_search_pages_reached", extra={"krs": krs, "pages": page})
            break
        search_data = await rdf_client.wyszukiwanie(krs, page=page, page_size=100)
        all_docs.extend(search_data["content"])
        total_pages = search_data["metadaneWynikow"]["liczbaStron"]
        if page + 1 >= total_pages:
            break
        page += 1

    docs = [
        d for d in all_docs
        if d["rodzaj"] == "18" and d["status"] == "NIEUSUNIETY"
    ]

    if not docs:
        xml_parser.cache_set(cache_key, [])
        return []

    # Fetch all metadata in parallel
    metas = await asyncio.gather(
        *[rdf_client.metadata(d["id"]) for d in docs],
        return_exceptions=True,
    )

    has_errors = any(isinstance(m, Exception) for m in metas)

    period_map: dict[str, dict] = {}
    for doc, meta in zip(docs, metas):
        if isinstance(meta, Exception):
            continue
        if meta.get("czyMSR", True):
            continue  # skip IFRS

        period_end = doc["okresSprawozdawczyKoniec"]
        is_corr = meta.get("czyKorekta", False)
        existing = period_map.get(period_end)

        if existing is None or (is_corr and not existing["is_correction"]):
            period_map[period_end] = {
                "period_start": doc["okresSprawozdawczyPoczatek"],
                "period_end": period_end,
                "document_id": doc["id"],
                "date_filed": meta.get("dataDodania"),
                "is_correction": is_corr,
                "is_ifrs": False,
                "filename": meta.get("nazwaPliku", ""),
            }

    periods = sorted(period_map.values(), key=lambda x: x["period_end"])
    # Only cache when every metadata call succeeded to avoid poisoning the cache
    # with a partial list caused by transient upstream failures.
    if not has_errors:
        xml_parser.cache_set(cache_key, periods)
    return periods


async def _fetch_and_parse(krs: str, period_end: Optional[str]) -> dict:
    """
    Resolve period_end → document_id → ZIP → parsed statement dict.
    Caches parsed results by document_id.
    """
    periods = await _get_available_periods(krs)
    if not periods:
        raise HTTPException(404, f"No Polish-GAAP financial statements found for KRS {krs}")

    if period_end:
        match = next((p for p in periods if p["period_end"] == period_end), None)
        if match is None:
            available = [p["period_end"] for p in periods]
            raise HTTPException(
                404,
                f"No statement for period ending {period_end}. Available: {available}",
            )
        period_info = match
    else:
        period_info = periods[-1]  # most recent

    cache_key = f"parsed:{krs}:{period_info['document_id']}"
    cached = xml_parser.cache_get(cache_key)
    if cached is not None:
        return cached

    zip_bytes = await rdf_client.download([period_info["document_id"]])
    xml_string = xml_parser.extract_xml_from_zip(zip_bytes)
    parsed = xml_parser.parse_statement(xml_string)

    xml_parser.cache_set(cache_key, parsed)
    return parsed


def _tag_section(tag: str) -> str:
    if tag.startswith("CF."):
        return "cash_flow"
    if tag.startswith("RZiS."):
        return "rzis"
    return "bilans"


def _tag_label(tag: str, schema_code: str = "SFJINZ") -> str:
    from app.services.schema_labels import SCHEMA_REGISTRY
    schema = SCHEMA_REGISTRY.get(schema_code)
    labels = schema["tag_labels"] if schema else xml_parser.TAG_LABELS
    raw = tag.split(".")[-1] if "." in tag else tag
    return labels.get(tag) or labels.get(raw) or tag


def _ratio_with_change(current: Optional[float], previous: Optional[float]) -> dict:
    change = None
    if current is not None and previous is not None:
        change = round(current - previous, 4)
    return {"current": current, "previous": previous, "change": change}


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.post("/statement", summary="Parse a financial statement")
async def get_statement(
    body: StatementRequest,
    request: Request,
    background: BackgroundTasks,
    user: OptionalUser = None,
):
    """Parse a single financial statement and return the full hierarchical tree."""
    logger.info("statement_parse", extra={"event": "statement_parse", "krs": body.krs, "period_end": body.period_end})
    stmt = await _fetch_and_parse(body.krs, body.period_end)
    background.add_task(
        activity_logger.log,
        user["id"] if user else None,
        "analysis_statement",
        body.krs,
        {"period_end": body.period_end},
        request.client.host if request.client else None,
    )
    return stmt


@router.post("/compare", summary="Compare two periods")
async def compare(
    body: CompareRequest,
    request: Request,
    background: BackgroundTasks,
    user: OptionalUser = None,
):
    """
    Compare two financial statements year-over-year.
    Returns a merged tree with change calculations and financial ratios.
    """
    logger.info(
        "statement_compare",
        extra={
            "event": "statement_compare",
            "krs": body.krs,
            "current": body.period_end_current,
            "previous": body.period_end_previous,
        },
    )
    # Fetch both statements (potentially in parallel)
    current_stmt, previous_stmt = await asyncio.gather(
        _fetch_and_parse(body.krs, body.period_end_current),
        _fetch_and_parse(body.krs, body.period_end_previous),
    )

    comparison = xml_parser.build_comparison(current_stmt, previous_stmt)

    # Ratios
    current_ratios = xml_parser.compute_ratios(current_stmt)
    previous_ratios = xml_parser.compute_ratios(previous_stmt)

    current_schema = current_stmt["company"].get("schema_code", "SFJINZ")
    previous_schema = previous_stmt["company"].get("schema_code", "SFJINZ")

    def pct_chg(concept: str, kwota_curr: str = "kwota_a", kwota_prev: str = "kwota_a") -> Optional[float]:
        tag_curr = xml_parser.resolve_tag(concept, current_schema)
        tag_prev = xml_parser.resolve_tag(concept, previous_schema)
        if tag_curr is None or tag_prev is None:
            return None
        a = xml_parser.find_value(current_stmt, tag_curr, kwota_curr)
        b = xml_parser.find_value(previous_stmt, tag_prev, kwota_prev)
        if a is None or b is None or b == 0:
            return None
        return round((a - b) / abs(b) * 100, 2)

    ratios = {
        "equity_ratio": _ratio_with_change(
            current_ratios["equity_ratio"], previous_ratios["equity_ratio"]
        ),
        "current_ratio": _ratio_with_change(
            current_ratios["current_ratio"], previous_ratios["current_ratio"]
        ),
        "debt_ratio": _ratio_with_change(
            current_ratios["debt_ratio"], previous_ratios["debt_ratio"]
        ),
        "operating_margin": _ratio_with_change(
            current_ratios["operating_margin"], previous_ratios["operating_margin"]
        ),
        "net_margin": _ratio_with_change(
            current_ratios["net_margin"], previous_ratios["net_margin"]
        ),
        "revenue_change_pct": pct_chg("revenue"),
        "net_profit_change_pct": pct_chg("net_profit"),
    }

    c_company = current_stmt["company"]
    p_company = previous_stmt["company"]

    result = {
        "company": {
            "name": c_company.get("name"),
            "krs": c_company.get("krs"),
            "nip": c_company.get("nip"),
        },
        "current_period": {
            "start": c_company.get("period_start"),
            "end": c_company.get("period_end"),
        },
        "previous_period": {
            "start": p_company.get("period_start"),
            "end": p_company.get("period_end"),
        },
        "bilans": comparison["bilans"],
        "rzis": comparison["rzis"],
        "cash_flow": comparison["cash_flow"],
        "ratios": ratios,
    }
    background.add_task(
        activity_logger.log,
        user["id"] if user else None,
        "analysis_compare",
        body.krs,
        {"periods": [body.period_end_current, body.period_end_previous]},
        request.client.host if request.client else None,
    )
    return result


@router.post("/time-series", summary="Track fields across years")
async def time_series(
    body: TimeSeriesRequest,
    request: Request,
    background: BackgroundTasks,
    user: OptionalUser = None,
):
    """Track selected financial fields across multiple years."""
    logger.info(
        "time_series",
        extra={"event": "time_series", "krs": body.krs, "fields": body.fields, "period_count": len(body.period_ends or [])},
    )
    periods = await _get_available_periods(body.krs)
    if not periods:
        raise HTTPException(404, f"No financial statements found for KRS {body.krs}")

    # Filter to requested periods if specified
    if body.period_ends:
        requested = set(body.period_ends)
        periods = [p for p in periods if p["period_end"] in requested]
        if not periods:
            raise HTTPException(404, f"None of the requested periods found")

    # Download and parse all statements in parallel
    stmts = await asyncio.gather(
        *[_fetch_and_parse(body.krs, p["period_end"]) for p in periods],
        return_exceptions=True,
    )

    # Build period list (skip failed downloads, log failures)
    valid_periods = []
    valid_stmts = []
    for period, stmt in zip(periods, stmts):
        if isinstance(stmt, Exception):
            logger.warning(
                "statement_download_failed",
                extra={
                    "event": "statement_download_failed",
                    "krs": body.krs,
                    "period_end": period["period_end"],
                    "error": str(stmt),
                },
            )
            continue
        valid_periods.append(period)
        valid_stmts.append(stmt)

    if not valid_stmts:
        raise HTTPException(502, "Failed to download all requested statements")

    # For the oldest statement, also extract kwota_b (one extra year)
    oldest_stmt = valid_stmts[0]
    oldest_kwota_b_values = xml_parser.extract_flat_values(oldest_stmt, use_kwota_b=True)
    oldest_period_start = oldest_stmt["company"].get("period_start", "")
    oldest_period_end = oldest_stmt["company"].get("period_end", "")

    # Build extra (pre-oldest) period from kwota_b if not already covered
    extra_period = None
    if oldest_period_start and oldest_period_end:
        # The embedded kwota_b year is the year before the oldest statement's kwota_a
        # Heuristic: subtract 1 year from period dates
        try:
            start_year = int(oldest_period_start[:4]) - 1
            end_year = int(oldest_period_end[:4]) - 1
            extra_end = f"{end_year}{oldest_period_end[4:]}"
            extra_start = f"{start_year}{oldest_period_start[4:]}"
            # Only add if not already in our periods list
            if extra_end not in {p["period_end"] for p in valid_periods}:
                if not body.period_ends or extra_end in body.period_ends:
                    extra_period = {"start": extra_start, "end": extra_end}
        except (ValueError, IndexError):
            pass

    # Assemble full periods list (oldest extra year first, then the actual statements)
    all_periods = []
    extra_values: Optional[dict] = None
    if extra_period:
        all_periods.append(extra_period)
        extra_values = oldest_kwota_b_values

    for period, stmt in zip(valid_periods, valid_stmts):
        all_periods.append({"start": period["period_start"], "end": period["period_end"]})

    # Extract flat values for each statement (kwota_a = that year's value)
    stmt_values = [xml_parser.extract_flat_values(s) for s in valid_stmts]

    # Build series — use the most recent statement's schema for labels
    latest_schema = valid_stmts[-1]["company"].get("schema_code", "SFJINZ") if valid_stmts else "SFJINZ"
    series = []
    for field_tag in body.fields:
        values: list[Optional[float]] = []

        if extra_values is not None:
            values.append(extra_values.get(field_tag))

        for flat in stmt_values:
            values.append(flat.get(field_tag))

        # Compute changes (index N vs N-1)
        changes_abs: list[Optional[float]] = [None]
        changes_pct: list[Optional[float]] = [None]
        for i in range(1, len(values)):
            a = values[i]
            b = values[i - 1]
            if a is not None and b is not None:
                changes_abs.append(round(a - b, 2))
                changes_pct.append(
                    round((a - b) / abs(b) * 100, 2) if b != 0 else None
                )
            else:
                changes_abs.append(None)
                changes_pct.append(None)

        series.append({
            "tag": field_tag,
            "label": _tag_label(field_tag, latest_schema),
            "section": _tag_section(field_tag),
            "values": values,
            "changes_absolute": changes_abs,
            "changes_percent": changes_pct,
        })

    company = valid_stmts[-1]["company"]  # most recent for company name
    result = {
        "company": {"name": company.get("name"), "krs": company.get("krs")},
        "periods": all_periods,
        "series": series,
    }
    background.add_task(
        activity_logger.log,
        user["id"] if user else None,
        "analysis_timeseries",
        body.krs,
        {"fields": body.fields},
        request.client.host if request.client else None,
    )
    return result


@router.get("/available-periods/{krs}", summary="List available statement periods")
async def available_periods(krs: str):
    """
    List all available Polish-GAAP financial statement periods for a company.
    Does NOT download XML — only calls /search + /metadata.
    """
    logger.info("available_periods", extra={"event": "available_periods", "krs": krs})
    if not krs.isdigit() or len(krs) > 10:
        raise HTTPException(422, "krs must be 1–10 digits")

    # Get company name from lookup
    lookup_data = await rdf_client.dane_podstawowe(krs)
    company_name = None
    if lookup_data.get("podmiot"):
        company_name = lookup_data["podmiot"].get("nazwaPodmiotu")

    periods = await _get_available_periods(krs)

    return {
        "krs": krs.zfill(10),
        "company_name": company_name,
        "periods": periods,
    }
