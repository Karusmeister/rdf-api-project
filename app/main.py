from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from app import rdf_client
from app.config import settings
from app.db import connection as db_conn
from app.db import prediction_db
from app.scraper import db as scraper_db
from app.routers.rdf import router as rdf_router
from app.routers.analysis import router as analysis_router
from app.routers.scraper import router as scraper_router
from app.routers.etl.routes import router as etl_router


@asynccontextmanager
async def lifespan(app: FastAPI):
    db_conn.connect()
    scraper_db.connect()
    prediction_db.connect()
    await rdf_client.start()
    yield
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

app.include_router(rdf_router)
app.include_router(analysis_router)
app.include_router(scraper_router)
app.include_router(etl_router)


@app.exception_handler(httpx.HTTPStatusError)
async def upstream_error_handler(request: Request, exc: httpx.HTTPStatusError):
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
