import logging
import time
from contextlib import asynccontextmanager

import httpx
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from slowapi import _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded

from app import krs_client, rdf_client
from app.adapters.ms_gov import MsGovKrsAdapter
from app.adapters.registry import get as get_adapter
from app.adapters.registry import register as register_adapter
from app.config import settings
from app.db import connection as db_conn
from app.db import migrations as db_migrations
from app.db import prediction_db
from app.jobs import krs_scanner, krs_sync
from app.logging_config import configure_logging
from app.monitoring import metrics
from app.repositories import krs_repo
from app.scraper import db as scraper_db
from app.routers.rdf import router as rdf_router
from app.routers.analysis import router as analysis_router
from app.routers.scraper import router as scraper_router
from app.routers.etl.routes import router as etl_router
from app.routers.jobs.routes import router as jobs_router
from app.routers.predictions import router as predictions_router
from app.routers.auth import router as auth_router
from app.routers.admin import router as admin_router
from app.routers.assessment import router as assessment_router
from app.rate_limit import limiter
from app.services import predictions as predictions_service

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    configure_logging()
    logger.info("app_startup", extra={"event": "startup"})
    settings.validate_jwt_secret()
    settings.validate_auth_security()
    db_conn.connect()
    db_conn.init_pool(settings.database_url, settings.db_pool_min, settings.db_pool_max)
    scraper_db.connect()
    prediction_db.connect()
    krs_repo.connect()
    # CR2-OPS-004: apply versioned SQL migrations after bootstrap tables exist
    # but before any write path runs. Migrations carry the mutating operations
    # (ALTER TABLE, backfills, FK constraints) that used to run implicitly on
    # every startup. Tracked in `schema_migrations`, idempotent across reboots.
    db_migrations.apply_pending(db_conn.get_conn())
    # CR-PZN-001: deterministically register built-in models BEFORE warming
    # caches, so `/api/predictions/models` reflects them immediately even on a
    # fresh environment where no scoring run has happened yet.
    predictions_service.register_builtin_models()
    predictions_service.warm_caches()
    await rdf_client.start()
    await krs_client.start()
    register_adapter("ms_gov", MsGovKrsAdapter())

    # Start KRS schedulers (sync + scanner)
    scheduler = AsyncIOScheduler()
    try:
        sync_trigger = CronTrigger.from_crontab(settings.krs_sync_cron)
        scheduler.add_job(krs_sync.run_sync, sync_trigger, id="krs_sync", replace_existing=True)

        scan_trigger = CronTrigger.from_crontab(settings.krs_scan_cron)
        scheduler.add_job(krs_scanner.run_scan, scan_trigger, id="krs_scan", replace_existing=True)

        scheduler.start()
        logger.info("scheduler_started", extra={
            "event": "scheduler_started",
            "krs_sync_cron": settings.krs_sync_cron,
            "krs_scan_cron": settings.krs_scan_cron,
        })
    except Exception:
        logger.error(
            "scheduler_start_failed — KRS sync/scan jobs will NOT run",
            extra={"event": "scheduler_start_failed"},
            exc_info=True,
        )

    logger.info("app_ready", extra={"event": "ready"})
    yield
    logger.info("app_shutdown", extra={"event": "shutdown"})
    if scheduler.running:
        scheduler.shutdown(wait=False)
    await krs_client.stop()
    await rdf_client.stop()
    db_conn.close()
    db_conn.close_pool()


tags_metadata = [
    {"name": "health", "description": "Liveness and dependency health checks."},
    {"name": "rdf - podmiot", "description": "KRS entity lookup via the upstream RDF registry."},
    {"name": "rdf - dokumenty", "description": "Financial document search, metadata, and download."},
    {"name": "analysis", "description": "Parse and compare Polish GAAP financial statements."},
    {"name": "scraper", "description": "Scraper status dashboard."},
    {"name": "etl", "description": "ETL ingestion trigger and training dataset statistics."},
    {"name": "jobs", "description": "Scheduled KRS sync and sequential scanner jobs."},
    {"name": "auth", "description": "User authentication: signup, login, email verification, Google SSO."},
    {"name": "predictions", "description": "Bankruptcy prediction scores, feature detail, and model catalog."},
    {"name": "admin", "description": "Admin-only operations (cache flush, access grants)."},
    {"name": "admin-dashboard", "description": "Admin dashboard: pipeline stats, KRS management, user activity."},
    {"name": "assessment", "description": "On-demand KRS assessment: data readiness check and pipeline trigger."},
]

app = FastAPI(
    title="RDF API",
    description=(
        "FastAPI service for the Polish financial document registry (RDF). "
        "Provides KRS entity lookup, financial statement analysis, ETL ingestion, "
        "bankruptcy prediction scores, and user authentication with per-KRS access control."
    ),
    version="1.0.0",
    lifespan=lifespan,
    openapi_tags=tags_metadata,
)
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ---------------------------------------------------------------------------
# Request / response logging middleware
# ---------------------------------------------------------------------------

@app.middleware("http")
async def db_connection_middleware(request: Request, call_next):
    """Acquire a per-request pooled DB connection and release it after the response.

    Skip for OPTIONS (CORS preflight) and /health — these don't need DB access.
    """
    if request.method == "OPTIONS" or request.url.path == "/health":
        return await call_next(request)
    db_conn.acquire_request_conn()
    try:
        response = await call_next(request)
    finally:
        db_conn.release_request_conn()
    return response


@app.middleware("http")
async def request_logging_middleware(request: Request, call_next):
    t0 = time.monotonic()
    method = request.method
    path = request.url.path

    logger.debug(
        "request_started",
        extra={"event": "request_started", "method": method, "path": path},
    )

    response = await call_next(request)

    latency_ms = int((time.monotonic() - t0) * 1000)
    logger.info(
        "request_finished",
        extra={
            "event": "request_finished",
            "method": method,
            "path": path,
            "status_code": response.status_code,
            "latency_ms": latency_ms,
        },
    )
    return response


app.include_router(rdf_router)
app.include_router(analysis_router)
app.include_router(scraper_router)
app.include_router(etl_router)
app.include_router(jobs_router)
app.include_router(predictions_router)
app.include_router(auth_router)
app.include_router(admin_router)
app.include_router(assessment_router)


@app.exception_handler(httpx.HTTPStatusError)
async def upstream_error_handler(request: Request, exc: httpx.HTTPStatusError):
    logger.error(
        "upstream_http_error",
        extra={
            "event": "upstream_http_error",
            "upstream_status": exc.response.status_code,
            "upstream_url": str(exc.request.url),
            "method": request.method,
            "path": request.url.path,
        },
    )
    return JSONResponse(
        status_code=502,
        content={"detail": "Upstream API error"},
    )


@app.exception_handler(httpx.RequestError)
async def upstream_request_error_handler(request: Request, exc: httpx.RequestError):
    logger.error(
        "upstream_connection_error",
        extra={
            "event": "upstream_connection_error",
            "error_type": type(exc).__name__,
            "upstream_url": str(exc.request.url) if exc.request else None,
            "method": request.method,
            "path": request.url.path,
        },
    )
    return JSONResponse(
        status_code=502,
        content={"detail": "Upstream connection error"},
    )


@app.get("/health", tags=["health"], summary="Liveness check")
async def health():
    """Returns `{"status": "ok"}` if the service is running."""
    return {"status": "ok"}


@app.get("/health/predictions", tags=["health"], summary="Predictions subsystem readiness")
async def health_predictions():
    """Readiness check for the predictions subsystem.

    CR2-REL-006: returns 503 when the built-in model catalog is incomplete
    (e.g. a registrar raised during startup in local dev, where we fall open
    rather than crashing the process). Alerts should page on a sustained 503
    here because `/api/predictions/models` will be missing rows.

    In non-local environments a registration failure aborts startup outright
    (see `predictions_service.register_builtin_models`), so this endpoint
    only ever reports `degraded` in local dev.
    """
    registration = predictions_service.get_builtin_models_health()
    body = {
        "status": "ok" if registration["ok"] else "degraded",
        "builtin_models": {
            "ok": registration["ok"],
            "failed_registrars": registration["failed_registrars"],
        },
    }
    status_code = 200 if registration["ok"] else 503
    return JSONResponse(status_code=status_code, content=body)


@app.get("/health/krs", tags=["health"], summary="KRS adapter health")
async def health_krs():
    """Check KRS adapter connectivity. Returns 200 if reachable, 503 otherwise."""
    try:
        adapter = get_adapter("ms_gov")
    except KeyError:
        return JSONResponse(
            status_code=500,
            content={"source": "ms_gov", "ok": False, "detail": "Adapter not registered (misconfiguration)"},
        )

    health = await adapter.health_check()
    status_code = 200 if health.ok else 503
    return JSONResponse(
        status_code=status_code,
        content=health.model_dump(mode="json"),
    )


@app.get("/metrics/krs", tags=["health"], summary="KRS call metrics")
async def metrics_krs():
    """Return last-N call stats: p50/p95 latency, error rate, calls per source."""
    return metrics.get_stats()
