"""Company search and health metrics endpoints.

PKR-124: GET /api/companies/search, GET /api/companies/search/popular, POST /api/companies/search/log-click
PKR-121: GET /api/companies/{krs}/health-metrics
"""

from __future__ import annotations

import logging
from typing import Annotated

from cachetools import TTLCache
from fastapi import APIRouter, BackgroundTasks, HTTPException, Path, Query

from app.db import connection as db_conn

from .schemas import (
    CompanySearchResponse,
    CompanySearchResult,
    HealthMetric,
    HealthMetricPoint,
    HealthMetricsResponse,
    PopularCompaniesResponse,
    PopularCompany,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/companies", tags=["companies"])

# TTL cache for search results: maxsize=1000, ttl=600s (10 min)
_search_cache: TTLCache = TTLCache(maxsize=1000, ttl=600)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _log_search(query: str, result_count: int) -> None:
    """Fire-and-forget: record a search query."""
    try:
        conn = db_conn.get_conn()
        conn.execute(
            "INSERT INTO search_log (query, result_count) VALUES (%s, %s)",
            [query, result_count],
        )
    except Exception:
        logger.debug("search_log_insert_failed", exc_info=True)


def _log_click(query: str, krs: str) -> None:
    """Fire-and-forget: record a clicked search result."""
    try:
        conn = db_conn.get_conn()
        conn.execute(
            "INSERT INTO search_log (query, clicked_krs) VALUES (%s, %s)",
            [query, krs],
        )
    except Exception:
        logger.debug("search_log_click_failed", exc_info=True)


# ---------------------------------------------------------------------------
# PKR-124: Company search
# ---------------------------------------------------------------------------


@router.get("/search", summary="Search companies by name or KRS number")
def search_companies(
    q: Annotated[str, Query(min_length=3, max_length=200, description="Search string (min 3 chars)")],
    background: BackgroundTasks,
    limit: Annotated[int, Query(ge=1, le=50)] = 10,
) -> CompanySearchResponse:
    """Search companies by partial name (trigram) or KRS number prefix.

    Auto-detects KRS numbers (all digits) vs name queries.
    Results ranked: companies with financial data first, then prefix match, then contains.
    """
    query = q.strip()
    cache_key = f"{query.lower()}:{limit}"
    cached = _search_cache.get(cache_key)
    if cached is not None:
        return cached

    conn = db_conn.get_conn()

    if query.isdigit():
        # KRS number search. KRS numbers are stored as zero-padded 10-digit
        # strings (e.g. '0000694720').
        #
        # Two modes based on whether the user typed leading zeros:
        #
        # 1. Zero-prefixed input (e.g. '000069'): the user is typing the
        #    canonical KRS format → treat as literal prefix: '000069%'
        #
        # 2. Non-zero-prefixed short input (e.g. '694720'): the user typed
        #    significant digits only → zero-pad to canonical form and exact
        #    match: '0000694720'
        #
        # 3. Full 10-digit input: exact match regardless.
        #
        if len(query) >= 10:
            like_pattern = query[:10]  # exact match, truncate any excess
        elif query.startswith('0'):
            like_pattern = query + '%'  # canonical prefix search
        else:
            like_pattern = query.zfill(10)  # pad → exact canonical match

        rows = conn.execute("""
            SELECT
                kr.krs,
                kr.company_name AS name,
                c.nip,
                c.pkd_code,
                kr.legal_form,
                CASE WHEN kr.is_active THEN 'active' ELSE 'inactive' END AS status,
                EXISTS(SELECT 1 FROM predictions p WHERE p.krs = kr.krs LIMIT 1) AS has_predictions
            FROM krs_registry kr
            LEFT JOIN companies c ON c.krs = kr.krs
            WHERE kr.krs LIKE %s
            ORDER BY c.krs IS NOT NULL DESC, kr.krs
            LIMIT %s
        """, [like_pattern, limit]).fetchall()

        count_row = conn.execute(
            "SELECT COUNT(*) FROM krs_registry WHERE krs LIKE %s",
            [like_pattern],
        ).fetchone()
    else:
        # Name search: try exact ILIKE first, fall back to trigram similarity
        rows = conn.execute("""
            SELECT
                kr.krs,
                kr.company_name AS name,
                c.nip,
                c.pkd_code,
                kr.legal_form,
                CASE WHEN kr.is_active THEN 'active' ELSE 'inactive' END AS status,
                EXISTS(SELECT 1 FROM predictions p WHERE p.krs = kr.krs LIMIT 1) AS has_predictions
            FROM krs_registry kr
            LEFT JOIN companies c ON c.krs = kr.krs
            WHERE kr.company_name ILIKE %s
            ORDER BY
                c.krs IS NOT NULL DESC,
                CASE
                    WHEN kr.company_name ILIKE %s THEN 1
                    ELSE 2
                END,
                kr.company_name
            LIMIT %s
        """, ['%' + query + '%', query + '%', limit]).fetchall()

        # Fuzzy fallback: if ILIKE found too few results, use trigram similarity
        if len(rows) < limit:
            fuzzy_rows = conn.execute("""
                SELECT
                    kr.krs,
                    kr.company_name AS name,
                    c.nip,
                    c.pkd_code,
                    kr.legal_form,
                    CASE WHEN kr.is_active THEN 'active' ELSE 'inactive' END AS status,
                    EXISTS(SELECT 1 FROM predictions p WHERE p.krs = kr.krs LIMIT 1) AS has_predictions
                FROM krs_registry kr
                LEFT JOIN companies c ON c.krs = kr.krs
                WHERE word_similarity(%s, kr.company_name) > 0.15
                  AND kr.company_name NOT ILIKE %s
                ORDER BY word_similarity(%s, kr.company_name) DESC, kr.company_name
                LIMIT %s
            """, [query, '%' + query + '%', query, limit - len(rows)]).fetchall()
            rows = list(rows) + list(fuzzy_rows)

        count_row = conn.execute(
            "SELECT COUNT(*) FROM krs_registry WHERE company_name ILIKE %s",
            ['%' + query + '%'],
        ).fetchone()

    cols = ["krs", "name", "nip", "pkd_code", "legal_form", "status", "has_predictions"]
    results = [CompanySearchResult(**dict(zip(cols, row))) for row in rows]
    total_count = count_row[0] if count_row else 0

    response = CompanySearchResponse(results=results, total_count=total_count, query=q)
    _search_cache[cache_key] = response

    background.add_task(_log_search, query, len(results))
    return response


@router.get("/search/popular", summary="Most clicked companies in last 30 days")
def popular_companies() -> PopularCompaniesResponse:
    """Return the top 10 most-clicked KRS numbers from search results in the last 30 days."""
    conn = db_conn.get_conn()
    rows = conn.execute("""
        SELECT sl.clicked_krs, kr.company_name, COUNT(*) AS click_count
        FROM search_log sl
        JOIN krs_registry kr ON kr.krs = sl.clicked_krs
        WHERE sl.clicked_krs IS NOT NULL
          AND sl.created_at >= current_timestamp - interval '30 days'
        GROUP BY sl.clicked_krs, kr.company_name
        ORDER BY click_count DESC
        LIMIT 10
    """).fetchall()

    results = [
        PopularCompany(krs=row[0], name=row[1], click_count=row[2])
        for row in rows
    ]
    return PopularCompaniesResponse(results=results)


@router.post("/search/log-click", summary="Log a clicked search result")
def log_click(
    q: Annotated[str, Query(description="Original search query")],
    krs: Annotated[str, Query(pattern=r"^\d{1,10}$", description="Clicked KRS number")],
    background: BackgroundTasks,
) -> dict:
    """Record that a user clicked a specific result from search. Fire-and-forget."""
    background.add_task(_log_click, q, krs.zfill(10))
    return {"status": "logged"}


# ---------------------------------------------------------------------------
# PKR-121: Health metrics
# ---------------------------------------------------------------------------

_EQUITY_RATIO_LABELS = [
    (50, "Silna baza kapitalowa"),
    (30, "Umiarkowana"),
    (None, "Slaba"),
]

_CURRENT_RATIO_LABELS = [
    (2, "Dobra plynnosc"),
    (1, "Wystarczajaca"),
    (None, "Niska"),
]

_OPERATING_MARGIN_LABELS = [
    (15, "Zdrowa rentownosc"),
    (5, "Umiarkowana"),
    (None, "Niska"),
]

_NET_MARGIN_LABELS = [
    (10, "Zyskowna"),
    (0, "Umiarkowana"),
    (None, "Strata"),
]

_REVENUE_GROWTH_LABELS = [
    (5, "Wzrost"),
    (-5, "Stabilna"),
    (None, "Spadek"),
]


def _label_for(value: float | None, thresholds: list[tuple]) -> str | None:
    if value is None:
        return None
    for floor, label in thresholds:
        if floor is None or value >= floor:
            return label
    return None


def _safe_div(num: float | None, denom: float | None) -> float | None:
    if num is None or denom is None or denom == 0:
        return None
    return num / denom


@router.get("/{krs}/health-metrics", summary="Historical financial health metrics for sparklines")
def get_health_metrics(
    krs: Annotated[str, Path(pattern=r"^\d{1,10}$")],
) -> HealthMetricsResponse:
    """Return time-series data for 5 financial health indicators across all available fiscal years.

    Metrics: equity_ratio, current_ratio, operating_margin, net_margin, revenue_growth.
    """
    padded_krs = krs.zfill(10)
    conn = db_conn.get_conn()

    # Selects ONE canonical report per (krs, fiscal_year) via ROW_NUMBER,
    # then pivots its tags into metric columns. This prevents mixing values
    # from multiple reports (e.g., different data_source_id or report_type)
    # within the same fiscal year.
    #
    # Canonical report selection: latest report_version, then latest
    # created_at, deterministic tiebreak on id DESC.
    #
    # Tag set:
    #   Pasywa_A      = equity (Bilans)
    #   Aktywa        = total assets (Bilans)
    #   Aktywa_B      = current assets (Bilans)
    #   Pasywa_B_III  = short-term liabilities (Bilans)
    #   RZiS.F        = operating profit (RZiS)
    #   RZiS.A        = net revenue from sales (RZiS)
    #   RZiS.N        = net profit (kalkulacyjny variant)
    #   RZiS.L        = net profit (porównawczy variant)
    rows = conn.execute("""
        WITH canonical AS (
            SELECT id, fiscal_year,
                   ROW_NUMBER() OVER (
                       PARTITION BY krs, fiscal_year
                       ORDER BY report_version DESC, created_at DESC, id DESC
                   ) AS rn
            FROM latest_successful_financial_reports
            WHERE krs = %s
        )
        SELECT
            c.fiscal_year,
            MAX(CASE WHEN fli.tag_path = 'Pasywa_A' THEN fli.value_current END) AS equity,
            MAX(CASE WHEN fli.tag_path = 'Aktywa' THEN fli.value_current END) AS total_assets,
            MAX(CASE WHEN fli.tag_path = 'Aktywa_B' THEN fli.value_current END) AS current_assets,
            MAX(CASE WHEN fli.tag_path = 'Pasywa_B_III' THEN fli.value_current END) AS short_liabilities,
            MAX(CASE WHEN fli.tag_path = 'RZiS.F' THEN fli.value_current END) AS operating_profit,
            MAX(CASE WHEN fli.tag_path = 'RZiS.A' THEN fli.value_current END) AS revenue,
            COALESCE(
                MAX(CASE WHEN fli.tag_path = 'RZiS.N' THEN fli.value_current END),
                MAX(CASE WHEN fli.tag_path = 'RZiS.L' THEN fli.value_current END)
            ) AS net_profit
        FROM canonical c
        JOIN financial_line_items fli ON fli.report_id = c.id
        WHERE c.rn = 1
          AND fli.tag_path IN ('Pasywa_A', 'Aktywa', 'Aktywa_B', 'Pasywa_B_III', 'RZiS.F', 'RZiS.A', 'RZiS.N', 'RZiS.L')
        GROUP BY c.fiscal_year
        ORDER BY c.fiscal_year
    """, [padded_krs]).fetchall()

    if not rows:
        raise HTTPException(status_code=404, detail=f"No financial data found for KRS {padded_krs}")

    cols = ["fiscal_year", "equity", "total_assets", "current_assets",
            "short_liabilities", "operating_profit", "revenue", "net_profit"]
    yearly = [dict(zip(cols, row)) for row in rows]

    # Compute metrics per year
    equity_history = []
    current_ratio_history = []
    operating_margin_history = []
    net_margin_history = []
    revenue_growth_history = []

    prev_revenue: float | None = None

    for yd in yearly:
        fy = yd["fiscal_year"]

        eq_ratio = _safe_div(yd["equity"], yd["total_assets"])
        equity_history.append(HealthMetricPoint(
            fiscal_year=fy,
            value=round(eq_ratio * 100, 2) if eq_ratio is not None else None,
        ))

        cr = _safe_div(yd["current_assets"], yd["short_liabilities"])
        current_ratio_history.append(HealthMetricPoint(
            fiscal_year=fy,
            value=round(cr, 2) if cr is not None else None,
        ))

        op_margin = _safe_div(yd["operating_profit"], yd["revenue"])
        operating_margin_history.append(HealthMetricPoint(
            fiscal_year=fy,
            value=round(op_margin * 100, 2) if op_margin is not None else None,
        ))

        net_margin = _safe_div(yd["net_profit"], yd["revenue"])
        net_margin_history.append(HealthMetricPoint(
            fiscal_year=fy,
            value=round(net_margin * 100, 2) if net_margin is not None else None,
        ))

        # Revenue growth: year-over-year change
        rev_growth = None
        if prev_revenue is not None and prev_revenue != 0 and yd["revenue"] is not None:
            rev_growth = round(((yd["revenue"] - prev_revenue) / abs(prev_revenue)) * 100, 2)
        revenue_growth_history.append(HealthMetricPoint(fiscal_year=fy, value=rev_growth))
        prev_revenue = yd["revenue"]

    def _build_metric(history: list[HealthMetricPoint], thresholds: list[tuple]) -> HealthMetric:
        current = history[-1].value if history else None
        return HealthMetric(
            current_value=current,
            label=_label_for(current, thresholds),
            history=history,
        )

    return HealthMetricsResponse(
        krs=padded_krs,
        metrics={
            "equity_ratio": _build_metric(equity_history, _EQUITY_RATIO_LABELS),
            "current_ratio": _build_metric(current_ratio_history, _CURRENT_RATIO_LABELS),
            "operating_margin": _build_metric(operating_margin_history, _OPERATING_MARGIN_LABELS),
            "net_margin": _build_metric(net_margin_history, _NET_MARGIN_LABELS),
            "revenue_growth": _build_metric(revenue_growth_history, _REVENUE_GROWTH_LABELS),
        },
    )
