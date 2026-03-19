from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from app import rdf_client
from app.config import settings
from app.routers import analysis, dokumenty, podmiot


@asynccontextmanager
async def lifespan(app: FastAPI):
    await rdf_client.start()
    yield
    await rdf_client.stop()


app = FastAPI(title="RDF API Proxy", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(podmiot.router)
app.include_router(dokumenty.router)
app.include_router(analysis.router)


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


@app.get("/health")
async def health():
    return {"status": "ok"}
