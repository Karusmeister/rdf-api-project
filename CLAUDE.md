# RDF API Proxy - Project Instructions

## What this is

PoC backend proxy for Repozytorium Dokumentow Finansowych (rdf-przegladarka.ms.gov.pl).
FastAPI service that handles KRS encryption and proxies requests to the government API.

## Key context files - READ THESE FIRST

- `docs/RDF_API_DOCUMENTATION.md` - Full upstream API docs (endpoints, payloads, responses)
- `docs/AGENT_INSTRUCTIONS.md` - Step-by-step build guide with architecture decisions
- `docs/LOVABLE_UI_SPEC.md` - Frontend spec (for understanding what the API serves)

## Tech stack

- Python 3.12, FastAPI, uvicorn, httpx (async), pycryptodome, pydantic v2
- NO requests library - everything async with httpx
- NO manual threading - use async + uvicorn --workers

## Critical: KRS encryption

The `/dokumenty/wyszukiwanie` endpoint requires AES-128-CBC encrypted KRS token.
Full algorithm is in `docs/RDF_API_DOCUMENTATION.md` section 3 and `docs/AGENT_INSTRUCTIONS.md` step 3.
Short version:

```
plaintext = krs.zfill(10) + now.strftime("%Y-%m-%d-%H-%M-%S")
key = iv = now.strftime("%Y-%m-%d-%H").rjust(16, "1")
token = base64(AES-CBC(plaintext, key, iv, PKCS7))
```

Generate fresh token for EVERY request. Never cache it.

## Project structure

```
app/
  main.py           - FastAPI app, lifespan, CORS, exception handlers
  config.py         - pydantic-settings (env vars)
  crypto.py         - encrypt_nrkrs()
  rdf_client.py     - httpx.AsyncClient wrapper (singleton, created in lifespan)
  schemas.py        - Pydantic request/response models
  routers/
    podmiot.py      - /api/podmiot/* (entity lookup)
    dokumenty.py    - /api/dokumenty/* (search, metadata, download)
tests/
  test_crypto.py
  test_endpoints.py
```

## Commands

```bash
# Install
pip install -r requirements.txt

# Run dev
uvicorn app.main:app --reload --port 8000

# Run prod
uvicorn app.main:app --workers 4 --port 8000

# Test
pytest tests/ -v

# Test single module
pytest tests/test_crypto.py -v
```

## API endpoints to implement

| Method | Path | Upstream | Notes |
|--------|------|----------|-------|
| POST | /api/podmiot/lookup | dane-podstawowe | Plain KRS |
| POST | /api/podmiot/document-types | rodzajeDokWyszukiwanie | Plain KRS |
| POST | /api/dokumenty/search | wyszukiwanie | ENCRYPTED KRS |
| GET | /api/dokumenty/metadata/{id} | dokumenty/{id} | URL-encode Base64 ID |
| POST | /api/dokumenty/download | dokumenty/tresc | Returns ZIP |
| GET | /health | - | Simple healthcheck |

## Gotchas

1. Document IDs are Base64 with `=`, `+`, `/` - must URL-encode in path params
2. The `nrKRS` field name differs between endpoints (numerKRS vs nrKRS)
3. Download endpoint needs Accept: application/octet-stream header override
4. Use StreamingResponse for download endpoint
5. CORS must be enabled (frontend runs on different port)
