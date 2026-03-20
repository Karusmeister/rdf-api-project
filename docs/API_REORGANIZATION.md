# API Reorganization - Claude Code Instructions

> **Goal:** Restructure routers into domain directories so adding new API domains is clean.
> **Constraint:** Zero behavior changes. All endpoints keep their exact paths. All tests pass unchanged.
> **Time:** This is pure file-move + import-rewrite. No logic changes anywhere.

---

## Why

Current flat structure:

```
app/
  schemas.py                    # RDF-specific but named generically
  routers/
    podmiot.py                  # /api/podmiot/*
    dokumenty.py                # /api/dokumenty/*
    analysis.py                 # /api/analysis/* (has inline schemas)
```

Problems:
- `schemas.py` sits at the app root but only serves RDF proxy routes
- `analysis.py` defines its own inline Pydantic models instead of using a shared schemas file
- Adding scraper, ML, or any new API domain means more files dumped into a flat `routers/` dir
- Swagger UI shows tags like "podmiot", "dokumenty" - no grouping by domain

---

## Target structure

```
app/
  main.py                       # unchanged (just import paths update)
  config.py                     # unchanged
  crypto.py                     # unchanged
  rdf_client.py                 # unchanged (shared infra)

  routers/
    __init__.py                 # empty (unchanged)
    rdf/                        # Domain: RDF upstream proxy
      __init__.py               # exports `router` combining podmiot + dokumenty
      podmiot.py                # moved from routers/podmiot.py
      dokumenty.py              # moved from routers/dokumenty.py
      schemas.py                # moved from app/schemas.py
    analysis/                   # Domain: financial statement analysis
      __init__.py               # exports `router`
      routes.py                 # moved from routers/analysis.py
      schemas.py                # extracted from analysis.py inline models

  services/                     # unchanged
    __init__.py
    xml_parser.py
```

Future additions will follow the same pattern:

```
    scraper/                    # Domain: scraper monitoring
      __init__.py
      routes.py
    ml/                         # Domain: ML predictions (future)
      __init__.py
      routes.py
      schemas.py
```

---

## Step-by-step

### Step 1: Create directory structure

```bash
mkdir -p app/routers/rdf
mkdir -p app/routers/analysis
touch app/routers/rdf/__init__.py
touch app/routers/analysis/__init__.py
```

### Step 2: Move RDF files

```bash
git mv app/routers/podmiot.py app/routers/rdf/podmiot.py
git mv app/routers/dokumenty.py app/routers/rdf/dokumenty.py
git mv app/schemas.py app/routers/rdf/schemas.py
```

### Step 3: Update imports in `app/routers/rdf/podmiot.py`

Change:
```python
from app.schemas import (
```
To:
```python
from app.routers.rdf.schemas import (
```

### Step 4: Update imports in `app/routers/rdf/dokumenty.py`

Same change:
```python
from app.schemas import (
```
To:
```python
from app.routers.rdf.schemas import (
```

### Step 5: Create `app/routers/rdf/__init__.py`

This file creates a parent router and includes the sub-routers:

```python
from fastapi import APIRouter

from app.routers.rdf.podmiot import router as podmiot_router
from app.routers.rdf.dokumenty import router as dokumenty_router

router = APIRouter()
router.include_router(podmiot_router)
router.include_router(dokumenty_router)
```

No prefix change here - podmiot.py and dokumenty.py already define their own prefixes
(`/api/podmiot` and `/api/dokumenty`). The parent router just groups them.

### Step 6: Move analysis.py and extract schemas

Move the file:
```bash
git mv app/routers/analysis.py app/routers/analysis/routes.py
```

Create `app/routers/analysis/schemas.py` with the Pydantic models currently defined
inline at the top of `analysis.py`:

```python
from typing import List, Optional
from pydantic import BaseModel, Field


class StatementRequest(BaseModel):
    krs: str = Field(..., pattern=r"^\d{1,10}$")
    period_end: Optional[str] = Field(None, description="YYYY-MM-DD - omit for most recent")


class CompareRequest(BaseModel):
    krs: str = Field(..., pattern=r"^\d{1,10}$")
    period_end_current: str = Field(..., description="YYYY-MM-DD")
    period_end_previous: str = Field(..., description="YYYY-MM-DD")


class TimeSeriesRequest(BaseModel):
    krs: str = Field(..., pattern=r"^\d{1,10}$")
    fields: List[str] = Field(..., min_length=1)
    period_ends: Optional[List[str]] = Field(
        None, description="Filter to these period end dates. Omit for all available years."
    )
```

Then in `app/routers/analysis/routes.py`:
- **Remove** the three class definitions above (StatementRequest, CompareRequest, TimeSeriesRequest)
- **Add** at the top:
```python
from app.routers.analysis.schemas import StatementRequest, CompareRequest, TimeSeriesRequest
```
- Everything else stays exactly the same

### Step 7: Create `app/routers/analysis/__init__.py`

```python
from app.routers.analysis.routes import router
```

That's it - just re-export.

### Step 8: Update `app/main.py`

Current:
```python
from app.routers import analysis, dokumenty, podmiot

# ...
app.include_router(podmiot.router)
app.include_router(dokumenty.router)
app.include_router(analysis.router)
```

Change to:
```python
from app.routers.rdf import router as rdf_router
from app.routers.analysis import router as analysis_router

# ...
app.include_router(rdf_router)
app.include_router(analysis_router)
```

### Step 9: Update Swagger tags (optional but recommended)

In `app/routers/rdf/podmiot.py`, change:
```python
router = APIRouter(prefix="/api/podmiot", tags=["podmiot"])
```
To:
```python
router = APIRouter(prefix="/api/podmiot", tags=["rdf - podmiot"])
```

In `app/routers/rdf/dokumenty.py`, change:
```python
router = APIRouter(prefix="/api/dokumenty", tags=["dokumenty"])
```
To:
```python
router = APIRouter(prefix="/api/dokumenty", tags=["rdf - dokumenty"])
```

This groups them visually in the Swagger UI under the "rdf" prefix.

### Step 10: Update test imports

In `tests/test_endpoints.py`, check for any direct imports of `app.schemas` or
`app.routers.podmiot` etc. These tests mock `rdf_client` at the module level and
import `app.main:app`, so they should work without changes. But verify:

```bash
grep -r "from app.schemas\|from app.routers.podmiot\|from app.routers.dokumenty\|from app.routers.analysis" tests/
```

If any test file imports directly from the old paths, update them.

### Step 11: Clean up old files

After moving, ensure no stale files remain:

```bash
# These should NOT exist anymore:
test ! -f app/schemas.py || echo "STALE: app/schemas.py still exists"
test ! -f app/routers/podmiot.py || echo "STALE: app/routers/podmiot.py still exists"
test ! -f app/routers/dokumenty.py || echo "STALE: app/routers/dokumenty.py still exists"
test ! -f app/routers/analysis.py || echo "STALE: app/routers/analysis.py still exists"
```

### Step 12: Verify

```bash
# All tests pass
pytest tests/ -v

# API starts and all endpoints respond
uvicorn app.main:app --port 8000 &
sleep 2
curl -s http://localhost:8000/health | grep ok
curl -s http://localhost:8000/docs | grep "rdf - podmiot"
curl -s -X POST http://localhost:8000/api/podmiot/lookup \
  -H "Content-Type: application/json" \
  -d '{"krs": "694720"}' | python -m json.tool | head -5
kill %1

echo "All checks passed"
```

---

## Convention for future API domains

When adding a new API domain (e.g. scraper, ML):

1. Create `app/routers/{domain}/`
2. Add `__init__.py` that exports `router`
3. Add `routes.py` with endpoints
4. Add `schemas.py` if it has request/response models
5. In `app/main.py`, add one line: `app.include_router({domain}_router)`

The domain directory is self-contained. You can understand it by reading just those 2-3 files.

---

## Things NOT to do

- Do NOT change any endpoint paths (`/api/podmiot/lookup` stays exactly the same)
- Do NOT change request/response shapes
- Do NOT refactor business logic (xml_parser, rdf_client) - only move router files
- Do NOT create abstract base routers or router factories - keep it simple
- Do NOT add versioning (like `/api/v1/`) - premature for this stage
- Do NOT touch `app/services/`, `app/crypto.py`, `app/rdf_client.py`, or `app/config.py`
- Do NOT split the existing test files - just update imports if needed
