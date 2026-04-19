"""Admin dashboard endpoints: pipeline stats, KRS management, user activity."""

from __future__ import annotations

import logging
from typing import Annotated

from fastapi import APIRouter, BackgroundTasks, HTTPException, Path, Query

from app.auth import CurrentUser, require_admin
from app.db.connection import get_conn
from app.services.activity import activity_logger

from .schemas import (
    ActivityEntry,
    KrsCoverageItem,
    KrsCoverageResponse,
    KrsDetailResponse,
    RefreshResponse,
    StatsOverviewResponse,
    KrsEntityStats,
    DocumentStats,
    ScannerStats,
    SyncStats,
    PredictionStats,
    UserStats,
    UserActivityResponse,
    UsersResponse,
    UserWithStats,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/admin", tags=["admin-dashboard"])


# ---------------------------------------------------------------------------
# PKR-74: Stats overview
# ---------------------------------------------------------------------------

@router.get("/stats/overview", summary="Pipeline health overview")
def stats_overview(user: CurrentUser) -> StatsOverviewResponse:
    """Aggregate stats across all pipeline stages. Admin only."""
    require_admin(user)
    conn = get_conn()

    # KRS entities
    entity_count = conn.execute("SELECT COUNT(*) FROM krs_companies").fetchone()[0]
    entities_with_docs = conn.execute(
        "SELECT COUNT(DISTINCT krs) FROM krs_documents_current"
    ).fetchone()[0]

    # Documents by rodzaj
    doc_rows = conn.execute(
        "SELECT rodzaj, COUNT(*) FROM krs_documents_current GROUP BY rodzaj"
    ).fetchall()
    total_docs = sum(r[1] for r in doc_rows)
    by_rodzaj = {r[0]: r[1] for r in doc_rows}

    # Scanner status
    cursor_row = conn.execute(
        "SELECT next_krs_int FROM krs_scan_cursor WHERE id = TRUE"
    ).fetchone()
    cursor_position = cursor_row[0] if cursor_row else None

    last_run_row = conn.execute(
        "SELECT started_at, finished_at, krs_from, krs_to, "
        "probed_count, valid_count, error_count, stopped_reason "
        "FROM krs_scan_runs ORDER BY started_at DESC LIMIT 1"
    ).fetchone()
    last_run = None
    is_running = False
    if last_run_row:
        last_run = {
            "started_at": str(last_run_row[0]) if last_run_row[0] else None,
            "finished_at": str(last_run_row[1]) if last_run_row[1] else None,
            "krs_from": last_run_row[2],
            "krs_to": last_run_row[3],
            "probed": last_run_row[4],
            "valid": last_run_row[5],
            "errors": last_run_row[6],
            "stopped_reason": last_run_row[7],
        }
        is_running = last_run_row[1] is None

    # Sync health
    runs_24h = conn.execute(
        "SELECT COUNT(*) FROM krs_sync_log WHERE started_at > current_timestamp - interval '24 hours'"
    ).fetchone()[0]
    runs_7d = conn.execute(
        "SELECT COUNT(*) FROM krs_sync_log WHERE started_at > current_timestamp - interval '7 days'"
    ).fetchone()[0]

    # Predictions
    pred_count = conn.execute(
        "SELECT COUNT(DISTINCT krs) FROM predictions"
    ).fetchone()[0]

    # Users
    user_row = conn.execute(
        "SELECT COUNT(*), COUNT(*) FILTER (WHERE has_full_access) "
        "FROM users WHERE is_active = true"
    ).fetchone()

    return StatsOverviewResponse(
        krs_entities=KrsEntityStats(
            total_entities=entity_count,
            entities_with_documents=entities_with_docs,
        ),
        documents=DocumentStats(total_documents=total_docs, by_rodzaj=by_rodzaj),
        scanner=ScannerStats(
            cursor_position=cursor_position,
            is_running=is_running,
            last_run=last_run,
        ),
        sync=SyncStats(runs_24h=runs_24h, runs_7d=runs_7d),
        predictions=PredictionStats(companies_with_predictions=pred_count),
        users=UserStats(active_users=user_row[0], admin_users=user_row[1]),
    )


# ---------------------------------------------------------------------------
# PKR-74: KRS coverage
# ---------------------------------------------------------------------------

@router.get("/stats/krs-coverage", summary="KRS coverage breakdown")
def krs_coverage(
    user: CurrentUser,
    page: Annotated[int, Query(ge=0)] = 0,
    size: Annotated[int, Query(ge=1, le=200)] = 50,
    sort: Annotated[str, Query(pattern=r"^(name|freshness|docs)$")] = "name",
    filter: Annotated[str, Query(pattern=r"^(all|missing_docs|stale|errors)$")] = "all",
) -> KrsCoverageResponse:
    """Paginated KRS entity list with document coverage stats. Admin only."""
    require_admin(user)
    conn = get_conn()

    sort_clause = {
        "name": "r.name ASC NULLS LAST",
        "freshness": "r.last_checked_at ASC NULLS FIRST",
        "docs": "doc_types_count DESC",
    }[sort]

    filter_clause = ""
    if filter == "missing_docs":
        filter_clause = "HAVING COUNT(DISTINCT d.rodzaj) < 5"
    elif filter == "stale":
        filter_clause = (
            "HAVING r.last_checked_at IS NULL "
            "OR r.last_checked_at < current_timestamp - interval '30 days'"
        )
    elif filter == "errors":
        filter_clause = "HAVING r.check_error_count > 0"

    base_query = f"""
        SELECT
            r.krs,
            r.name AS company_name,
            r.legal_form,
            r.first_seen_at,
            r.last_checked_at,
            EXTRACT(DAY FROM current_timestamp - r.last_checked_at)::int AS freshness_days,
            COUNT(DISTINCT d.rodzaj) AS doc_types_count,
            ARRAY_AGG(DISTINCT d.rodzaj) FILTER (WHERE d.rodzaj IS NOT NULL) AS doc_types_present,
            COUNT(DISTINCT d.okres_end)
                FILTER (WHERE d.rodzaj = '18') AS financial_periods
        FROM krs_companies r
        LEFT JOIN krs_documents_current d ON d.krs = r.krs
        GROUP BY r.krs, r.name, r.legal_form,
                 r.first_seen_at, r.last_checked_at, r.check_error_count
        {filter_clause}
    """

    # Total count
    total = conn.execute(f"SELECT COUNT(*) FROM ({base_query}) sub").fetchone()[0]

    # Paginated items
    rows = conn.execute(
        f"{base_query} ORDER BY {sort_clause} LIMIT %s OFFSET %s",
        (size, page * size),
    ).fetchall()

    items = [
        KrsCoverageItem(
            krs=row[0],
            company_name=row[1],
            legal_form=row[2],
            first_seen_at=row[3],
            last_checked_at=row[4],
            freshness_days=row[5],
            doc_types_count=row[6] or 0,
            doc_types_present=row[7] or [],
            financial_periods=row[8] or 0,
        )
        for row in rows
    ]

    return KrsCoverageResponse(items=items, total=total, page=page, size=size)


# ---------------------------------------------------------------------------
# PKR-75: KRS detail
# ---------------------------------------------------------------------------

@router.get("/krs/{krs_number}", summary="KRS entity detail")
def krs_detail(
    krs_number: Annotated[str, Path(pattern=r"^\d{1,10}$")],
    user: CurrentUser,
    doc_page: Annotated[int, Query(ge=0)] = 0,
    doc_size: Annotated[int, Query(ge=1, le=200)] = 20,
) -> KrsDetailResponse:
    """Everything known about a single KRS: company info, documents, sync history, user activity. Admin only."""
    require_admin(user)
    conn = get_conn()
    krs = krs_number.zfill(10)

    # Company info
    company_cur = conn.execute(
        "SELECT * FROM krs_companies WHERE krs = %s", (krs,)
    )
    company_row = company_cur.fetchone()
    company = None
    if company_row:
        cols = [desc[0] for desc in company_cur.description]
        company = dict(zip(cols, company_row))
        # Alias name -> company_name so legacy response consumers keep working.
        if "name" in company and "company_name" not in company:
            company["company_name"] = company["name"]
        for k, v in company.items():
            if hasattr(v, "isoformat"):
                company[k] = v.isoformat()

    # Documents (paginated)
    doc_total = conn.execute(
        "SELECT COUNT(*) FROM krs_documents_current WHERE krs = %s", (krs,)
    ).fetchone()[0]

    doc_rows = conn.execute(
        "SELECT * FROM krs_documents_current WHERE krs = %s "
        "ORDER BY okres_end DESC NULLS LAST, document_id DESC LIMIT %s OFFSET %s",
        (krs, doc_size, doc_page * doc_size),
    )
    doc_cols = [desc[0] for desc in doc_rows.description]
    documents = []
    for row in doc_rows.fetchall():
        d = dict(zip(doc_cols, row))
        for k, v in d.items():
            if hasattr(v, "isoformat"):
                d[k] = v.isoformat()
        documents.append(d)

    # Sync history
    sync_rows = conn.execute(
        "SELECT * FROM krs_sync_log ORDER BY started_at DESC LIMIT 10",
    )
    sync_cols = [desc[0] for desc in sync_rows.description]
    sync_history = []
    for row in sync_rows.fetchall():
        s = dict(zip(sync_cols, row))
        for k, v in s.items():
            if hasattr(v, "isoformat"):
                s[k] = v.isoformat()
        sync_history.append(s)

    # User activity
    activity_rows = conn.execute(
        "SELECT al.*, u.email FROM activity_log al "
        "LEFT JOIN users u ON al.user_id = u.id "
        "WHERE al.krs_number = %s "
        "ORDER BY al.created_at DESC LIMIT 20",
        (krs,),
    )
    activity_cols = [desc[0] for desc in activity_rows.description]
    user_activity = []
    for row in activity_rows.fetchall():
        a = dict(zip(activity_cols, row))
        for k, v in a.items():
            if hasattr(v, "isoformat"):
                a[k] = v.isoformat()
        user_activity.append(a)

    return KrsDetailResponse(
        company=company,
        documents=documents,
        doc_total=doc_total,
        sync_history=sync_history,
        user_activity=user_activity,
    )


# ---------------------------------------------------------------------------
# PKR-75: KRS refresh trigger
# ---------------------------------------------------------------------------

@router.post("/krs/{krs_number}/refresh", summary="Trigger KRS refresh")
def refresh_krs(
    krs_number: Annotated[str, Path(pattern=r"^\d{1,10}$")],
    background: BackgroundTasks,
    user: CurrentUser,
) -> RefreshResponse:
    """Queue a single-entity re-sync from the upstream KRS API. Admin only."""
    require_admin(user)
    krs = krs_number.zfill(10)

    background.add_task(_do_refresh, krs)
    background.add_task(
        activity_logger.log, user["id"], "admin_refresh_krs", krs, None, None
    )
    return RefreshResponse(krs=krs, status="queued")


_REFRESH_MAX_PAGES = 50


async def _do_refresh(krs: str) -> None:
    """Background task: re-sync a single KRS entity from upstream.

    Paginates through all search result pages (bounded by _REFRESH_MAX_PAGES)
    and persists documents via the append-only scraper_db.insert_documents path.
    """
    from datetime import datetime, timezone

    from app import rdf_client
    from app.db.connection import get_db
    from app.scraper import db as scraper_db

    try:
        # Verify entity exists upstream
        lookup = await rdf_client.dane_podstawowe(krs)
        if not lookup.get("czyPodmiotZnaleziony"):
            logger.warning("refresh_krs_not_found", extra={"krs": krs})
            return

        # Paginate through all search pages
        all_docs: list[dict] = []
        page = 0
        while page < _REFRESH_MAX_PAGES:
            search_data = await rdf_client.wyszukiwanie(krs, page=page, page_size=100)
            all_docs.extend(search_data.get("content", []))
            total_pages = search_data.get("metadaneWynikow", {}).get("liczbaStron", 1)
            if page + 1 >= total_pages:
                break
            page += 1

        # Upsert into krs_companies (the post-dedupe unified entity table)
        podmiot = lookup.get("podmiot", {})
        with get_db() as conn:
            conn.execute(
                """
                INSERT INTO krs_companies (krs, name, legal_form, first_seen_at, last_checked_at, synced_at)
                VALUES (%s, %s, %s, current_timestamp, current_timestamp, current_timestamp)
                ON CONFLICT (krs) DO UPDATE SET
                    name = EXCLUDED.name,
                    legal_form = EXCLUDED.legal_form,
                    last_checked_at = current_timestamp,
                    synced_at = current_timestamp
                """,
                (krs, podmiot.get("nazwaPodmiotu", ""), podmiot.get("formaPrawna")),
            )

        # Persist documents via scraper_db.insert_documents (append-only/idempotent)
        now = datetime.now(timezone.utc)
        mapped_docs = [
            {
                "document_id": doc["id"],
                "krs": krs,
                "rodzaj": doc.get("rodzaj"),
                "status": doc.get("status"),
                "nazwa": doc.get("nazwa"),
                "okres_start": doc.get("okresSprawozdawczyPoczatek"),
                "okres_end": doc.get("okresSprawozdawczyKoniec"),
                "discovered_at": now,
            }
            for doc in all_docs
        ]
        if mapped_docs:
            scraper_db.insert_documents(mapped_docs)

        logger.info(
            "refresh_krs_complete",
            extra={"krs": krs, "docs": len(all_docs), "pages": page + 1},
        )

    except Exception:
        logger.exception("refresh_krs_failed", extra={"krs": krs})


# ---------------------------------------------------------------------------
# PKR-76: User management
# ---------------------------------------------------------------------------

@router.get("/users", summary="List all users with activity stats")
def list_users(
    user: CurrentUser,
    page: Annotated[int, Query(ge=0)] = 0,
    size: Annotated[int, Query(ge=1, le=200)] = 50,
) -> UsersResponse:
    """Paginated user list with aggregated activity statistics. Admin only."""
    require_admin(user)
    conn = get_conn()

    total = conn.execute("""
        SELECT COUNT(*) FROM users
    """).fetchone()[0]

    rows = conn.execute("""
        SELECT
            u.id, u.email, u.name, u.has_full_access, u.is_active,
            u.auth_method, u.created_at, u.last_login_at,
            COALESCE(a.total_actions, 0) AS total_actions,
            COALESCE(a.unique_krs, 0) AS unique_krs_viewed,
            a.last_active_at
        FROM users u
        LEFT JOIN (
            SELECT user_id,
                   COUNT(*) AS total_actions,
                   COUNT(DISTINCT krs_number) AS unique_krs,
                   MAX(created_at) AS last_active_at
            FROM activity_log
            GROUP BY user_id
        ) a ON u.id = a.user_id
        ORDER BY u.created_at DESC
        LIMIT %s OFFSET %s
    """, (size, page * size)).fetchall()

    items = [
        UserWithStats(
            id=r[0],
            email=r[1],
            name=r[2],
            has_full_access=r[3],
            is_active=r[4],
            auth_method=r[5],
            created_at=r[6],
            last_login_at=r[7],
            total_actions=r[8],
            unique_krs_viewed=r[9],
            last_active_at=r[10],
        )
        for r in rows
    ]

    return UsersResponse(items=items, total=total, page=page, size=size)


@router.get("/users/{user_id}/activity", summary="User activity log")
def user_activity(
    user_id: str,
    user: CurrentUser,
    page: Annotated[int, Query(ge=0)] = 0,
    size: Annotated[int, Query(ge=1, le=200)] = 50,
    action: Annotated[str | None, Query()] = None,
) -> UserActivityResponse:
    """Paginated activity log for a specific user. Admin only."""
    require_admin(user)
    conn = get_conn()

    # Verify user exists
    target_row = conn.execute(
        "SELECT id, email, name, has_full_access, is_active, auth_method, created_at, last_login_at "
        "FROM users WHERE id = %s",
        (user_id,),
    ).fetchone()
    if not target_row:
        raise HTTPException(status_code=404, detail="User not found")

    # Activity stats for this user
    stats_row = conn.execute(
        "SELECT COALESCE(COUNT(*), 0), COUNT(DISTINCT krs_number), MAX(created_at) "
        "FROM activity_log WHERE user_id = %s",
        (user_id,),
    ).fetchone()

    target_user = UserWithStats(
        id=target_row[0],
        email=target_row[1],
        name=target_row[2],
        has_full_access=target_row[3],
        is_active=target_row[4],
        auth_method=target_row[5],
        created_at=target_row[6],
        last_login_at=target_row[7],
        total_actions=stats_row[0],
        unique_krs_viewed=stats_row[1] or 0,
        last_active_at=stats_row[2],
    )

    # Paginated activity entries
    total = conn.execute(
        "SELECT COUNT(*) FROM activity_log WHERE user_id = %s AND (%s IS NULL OR action = %s)",
        (user_id, action, action),
    ).fetchone()[0]

    rows = conn.execute(
        "SELECT action, krs_number, detail, ip_address, created_at "
        "FROM activity_log "
        "WHERE user_id = %s AND (%s IS NULL OR action = %s) "
        "ORDER BY created_at DESC LIMIT %s OFFSET %s",
        (user_id, action, action, size, page * size),
    ).fetchall()

    items = [
        ActivityEntry(
            action=r[0],
            krs_number=r[1],
            detail=r[2],
            ip_address=str(r[3]) if r[3] else None,
            created_at=r[4],
        )
        for r in rows
    ]

    # KRS breakdown
    krs_rows = conn.execute(
        "SELECT krs_number, COUNT(*) AS count "
        "FROM activity_log "
        "WHERE user_id = %s AND krs_number IS NOT NULL "
        "GROUP BY krs_number ORDER BY count DESC LIMIT 10",
        (user_id,),
    ).fetchall()
    krs_breakdown = {r[0]: r[1] for r in krs_rows}

    return UserActivityResponse(
        user=target_user,
        items=items,
        total=total,
        krs_breakdown=krs_breakdown,
    )
