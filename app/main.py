import logging
import time
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from app import krs_client, rdf_client
from app.adapters.ms_gov import MsGovKrsAdapter
from app.adapters.registry import get as get_adapter
from app.adapters.registry import register as register_adapter
from app.config import settings
from app.db import connection as db_conn
from app.db import prediction_db
from app.logging_config import configure_logging
from app.monitoring import metrics
from app.repositories import krs_repo
from app.scraper import db as scraper_db
from app.routers.rdf import router as rdf_router
from app.routers.analysis import router as analysis_router
from app.routers.scraper import router as scraper_router
from app.routers.etl.routes import router as etl_router

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    configure_logging()
    logger.info("app_startup", extra={"event": "startup"})
    db_conn.connect()
    scraper_db.connect()
    prediction_db.connect()
    krs_repo.connect()
    await rdf_client.start()
    await krs_client.start()
    register_adapter("ms_gov", MsGovKrsAdapter())
    logger.info("app_ready", extra={"event": "ready"})
    yield
    logger.info("app_shutdown", extra={"event": "shutdown"})
    await krs_client.stop()
    await rdf_client.stop()
    db_conn.close()


app = FastAPI(title="RDF API Proxy", lifespan=lifespan)

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
        content={
            "detail": "Upstream API error",
            "upstream_status": exc.response.status_code,
            "upstream_url": str(exc.request.url),
        },
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
        content={
            "detail": "Upstream connection error",
            "error_type": type(exc).__name__,
            "upstream_url": str(exc.request.url) if exc.request else None,
        },
    )


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.get("/health/krs")
async def health_krs():
    """Check KRS adapter connectivity. Returns 200 if reachable, 503 otherwise."""
    try:
        adapter = get_adapter("ms_gov")
    except KeyError:
        return JSONResponse(
            status_code=503,
            content={"source": "ms_gov", "ok": False, "detail": "Adapter not registered"},
        )

    health = await adapter.health_check()
    status_code = 200 if health.ok else 503
    return JSONResponse(
        status_code=status_code,
        content=health.model_dump(mode="json"),
    )


@app.get("/metrics/krs")
async def metrics_krs():
    """Return last-N call stats: p50/p95 latency, error rate, calls per source."""
    return metrics.get_stats()
