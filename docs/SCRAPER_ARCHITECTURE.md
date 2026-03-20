# RDF Scraper - Architecture & Implementation Spec

> **Status:** Draft for review before implementation
> **Date:** 2026-03-20
> **Scope:** Bulk KRS document collection system + GCP deployment

---

## 1. Executive Summary

### What we're building

An extension to the existing RDF API Proxy that:

1. **Tracks** all known KRS numbers and their document inventory in a local DuckDB database
2. **Downloads** financial documents (ZIPs with XML/PDF) and stores them on configurable storage (local disk or GCS)
3. **Runs a periodic job** that iterates through KRS numbers, discovers new documents, and downloads missing ones
4. **Deploys** the whole stack on a single GCP Compute Engine VM with cron

### What we're NOT building

- No real-time processing of documents (analysis endpoints already exist)
- No multi-node distributed scraping
- No web UI for the scraper (monitoring via logs + DB queries)

---

## 2. Database Design (DuckDB)

### Why DuckDB

- Single file, zero server - identical behavior on a laptop and on a GCP VM
- Fast analytical queries for monitoring ("how many KRS have we scraped?", "which ones are stale?")
- Native Parquet/CSV export if we ever want to move data
- Python-native via `duckdb` pip package
- WAL mode supports concurrent readers with a single writer (our job is single-process)

### Database file location

Configured via env var:

```
SCRAPER_DB_PATH=data/scraper.duckdb    # local default
```

### Schema

```sql
-- ============================================================
-- Table: krs_registry
-- Purpose: Master list of all KRS numbers we track.
--          One row per KRS. Source of truth for "what do we know?"
-- ============================================================
CREATE TABLE krs_registry (
    krs                 VARCHAR(10) PRIMARY KEY,  -- zero-padded, e.g. '0000694720'
    company_name        VARCHAR,                  -- from dane-podstawowe lookup
    legal_form          VARCHAR,                  -- forma_prawna
    is_active           BOOLEAN DEFAULT true,     -- false if wykreslony
    
    -- Tracking timestamps
    first_seen_at       TIMESTAMP NOT NULL,       -- when we first added this KRS
    last_checked_at     TIMESTAMP,                -- last time we checked for new docs
    last_download_at    TIMESTAMP,                -- last time we downloaded a file
    
    -- Scraper state
    check_priority      INTEGER DEFAULT 0,        -- higher = checked sooner (configurable)
    check_error_count   INTEGER DEFAULT 0,        -- consecutive errors (for backoff)
    last_error_message  VARCHAR,                  -- last error if any
    
    -- Stats (denormalized for fast monitoring queries)
    total_documents     INTEGER DEFAULT 0,        -- count of rows in krs_documents
    total_downloaded    INTEGER DEFAULT 0         -- count of downloaded files
);

-- ============================================================
-- Table: krs_documents
-- Purpose: Every document we've seen in the RDF registry for a KRS.
--          One row per document ID. Tracks download status.
-- ============================================================
CREATE TABLE krs_documents (
    document_id         VARCHAR PRIMARY KEY,       -- Base64 ID from RDF API
    krs                 VARCHAR(10) NOT NULL,      -- FK to krs_registry
    
    -- Document metadata (from /search response)
    rodzaj              VARCHAR NOT NULL,           -- doc type code: '18', '3', '4', etc.
    status              VARCHAR NOT NULL,           -- 'NIEUSUNIETY' or other
    nazwa               VARCHAR,                    -- document name (often null)
    okres_start         VARCHAR,                    -- reporting period start YYYY-MM-DD
    okres_end           VARCHAR,                    -- reporting period end YYYY-MM-DD
    
    -- Extended metadata (from /metadata/{id} - fetched lazily)
    filename            VARCHAR,                    -- nazwaPliku - original filename
    is_ifrs             BOOLEAN,                    -- czyMSR
    is_correction       BOOLEAN,                    -- czyKorekta
    date_filed          VARCHAR,                    -- dataDodania
    date_prepared       VARCHAR,                    -- dataSporządzenia
    
    -- Download tracking
    is_downloaded       BOOLEAN DEFAULT false,
    downloaded_at       TIMESTAMP,
    storage_path        VARCHAR,                    -- relative dir: 'krs/0000694720/doc_id_safe/'
    storage_backend     VARCHAR,                    -- 'local' or 'gcs'
    file_size_bytes     BIGINT,                     -- total extracted size (all files)
    zip_size_bytes      BIGINT,                     -- original ZIP size from API
    file_count          INTEGER,                    -- files extracted from ZIP
    file_types          VARCHAR,                    -- comma-separated: 'xml' or 'pdf' or 'xml,xhtml'
    
    -- Tracking
    discovered_at       TIMESTAMP NOT NULL,         -- when we first saw this doc
    metadata_fetched_at TIMESTAMP,                  -- when we fetched extended metadata
    download_error      VARCHAR,                    -- last download error if any
    
    -- Referential
    FOREIGN KEY (krs) REFERENCES krs_registry(krs)
);

-- ============================================================
-- Table: scraper_runs
-- Purpose: Audit log of every scraper job execution.
--          Enables monitoring, debugging, performance tracking.
-- ============================================================
CREATE TABLE scraper_runs (
    run_id              VARCHAR PRIMARY KEY,        -- UUID
    started_at          TIMESTAMP NOT NULL,
    finished_at         TIMESTAMP,
    status              VARCHAR DEFAULT 'running',  -- 'running', 'completed', 'failed', 'interrupted'
    
    -- What was done
    mode                VARCHAR NOT NULL,            -- 'full_scan', 'new_only', 'retry_errors', 'specific_krs'
    krs_checked         INTEGER DEFAULT 0,
    krs_new_found       INTEGER DEFAULT 0,
    documents_discovered INTEGER DEFAULT 0,
    documents_downloaded INTEGER DEFAULT 0,
    documents_failed    INTEGER DEFAULT 0,
    bytes_downloaded    BIGINT DEFAULT 0,
    
    -- Config snapshot (for reproducibility)
    config_snapshot     VARCHAR,                     -- JSON of relevant config at run time
    error_message       VARCHAR                      -- if status = 'failed'
);

-- ============================================================
-- Indexes for common query patterns
-- ============================================================
CREATE INDEX idx_registry_last_checked ON krs_registry(last_checked_at);
CREATE INDEX idx_registry_priority ON krs_registry(check_priority DESC, last_checked_at ASC);
CREATE INDEX idx_documents_krs ON krs_documents(krs);
CREATE INDEX idx_documents_not_downloaded ON krs_documents(is_downloaded) WHERE is_downloaded = false;
CREATE INDEX idx_runs_started ON scraper_runs(started_at DESC);
```

### Key monitoring queries

```sql
-- Dashboard: overall progress
SELECT
    count(*) AS total_krs,
    count(*) FILTER (WHERE last_checked_at IS NOT NULL) AS checked,
    count(*) FILTER (WHERE last_checked_at IS NULL) AS unchecked,
    count(*) FILTER (WHERE check_error_count > 0) AS with_errors,
    sum(total_documents) AS total_docs,
    sum(total_downloaded) AS total_downloaded
FROM krs_registry;

-- Stale KRS (not checked in 30 days)
SELECT krs, company_name, last_checked_at
FROM krs_registry
WHERE last_checked_at < now() - INTERVAL '30 days'
ORDER BY last_checked_at ASC
LIMIT 50;

-- Documents pending download
SELECT d.krs, r.company_name, d.document_id, d.rodzaj, d.okres_end
FROM krs_documents d
JOIN krs_registry r ON d.krs = r.krs
WHERE d.is_downloaded = false AND d.status = 'NIEUSUNIETY'
ORDER BY d.discovered_at DESC;

-- Recent scraper runs (performance tracking)
SELECT
    run_id,
    started_at,
    finished_at,
    finished_at - started_at AS duration,
    status,
    krs_checked,
    documents_downloaded,
    bytes_downloaded,
    documents_failed
FROM scraper_runs
ORDER BY started_at DESC
LIMIT 10;

-- Error hotspots
SELECT krs, company_name, check_error_count, last_error_message
FROM krs_registry
WHERE check_error_count > 3
ORDER BY check_error_count DESC;
```

---

## 3. File Storage Abstraction

### Design principle: extract on download, store raw files

The RDF API returns ZIP archives, but inside each ZIP is typically 1-2 files: `.xml` (Polish GAAP),
`.xhtml` (IFRS), or `.pdf`. We **extract on download** and store the raw files directly. This matters
because the files will be parsed for ML training data - unzipping on every read wastes compute.
It also lets you `grep`/`find` across the corpus, open files directly in an editor, and simplifies
the data pipeline.

Size trade-off: ~3x larger than compressed (~250 GB vs ~75 GB at full scale), but disk at $0.04/GB
means the difference is about $7/month. Not worth the pipeline complexity.

### Storage path convention

Each document gets its own directory with extracted files plus a manifest:

```
{base_path}/
  krs/
    0000694720/
      ZgsX8Fsncb1PFW07-T4XoQ/           # directory per document (safe chars from doc ID)
        Bjwk_SF_za_2024.xml              # original filename from inside the ZIP
        manifest.json                     # extraction metadata
      Bj4U6NqM2-gdavchRK5COw/
        Bjwk_SF_za_2019.xml
        manifest.json
    0000012345/
      abc123def/
        sprawozdanie.pdf                  # some documents are PDFs
        manifest.json
```

The `manifest.json` tracks provenance:
```json
{
  "document_id": "ZgsX8Fsncb1PFW07-T4XoQ==",
  "source_zip_size": 45230,
  "extracted_at": "2026-03-20T02:15:00Z",
  "files": [
    {"name": "Bjwk_SF_za_2024.xml", "size": 187420, "type": "xml"}
  ]
}
```

The `document_id` is made filesystem-safe (replace `+` with `-`, `/` with `_`, strip `=`).

### Config

```env
# Storage backend: 'local' or 'gcs'
STORAGE_BACKEND=local

# Local storage
STORAGE_LOCAL_PATH=data/documents

# GCS storage
STORAGE_GCS_BUCKET=rdf-documents-prod
STORAGE_GCS_PREFIX=krs/
```

### Interface (Python protocol)

```python
from typing import Protocol

class StorageBackend(Protocol):
    def save_extracted(self, doc_dir: str, zip_bytes: bytes, document_id: str) -> dict:
        """Extract ZIP, save raw files + manifest. Returns manifest dict."""
        ...

    def exists(self, path: str) -> bool:
        """Check if path exists."""
        ...

    def read(self, path: str) -> bytes:
        """Read bytes from path."""
        ...

    def list_files(self, dir_path: str) -> list[str]:
        """List files in a directory."""
        ...

    def get_full_path(self, path: str) -> str:
        """Return the full URI/path for logging/display."""
        ...
```

### Implementations

**LocalStorage** - uses synchronous `pathlib.Path` operations. Extracts ZIP in memory, writes raw files to disk.

**GCSStorage** - uses `gcloud-aio-storage` for GCS access. Authentication via Application Default Credentials (ADC) - automatic on Compute Engine.

### Factory

```python
def create_storage(settings: Settings) -> StorageBackend:
    if settings.storage_backend == "gcs":
        return GCSStorage(
            bucket=settings.storage_gcs_bucket,
            prefix=settings.storage_gcs_prefix,
        )
    return LocalStorage(base_path=settings.storage_local_path)
```

---

## 4. Scraper Job Design

### Core flow

```
┌─────────────────────────────────────────────────────┐
│                    SCRAPER JOB                       │
│                                                      │
│  1. Create scraper_runs record (status=running)      │
│  2. Load KRS list from krs_registry                  │
│  3. Sort by configured ordering strategy             │
│  4. For each KRS (with rate limiting):               │
│     a. POST /podmiot/lookup - validate               │
│     b. POST /dokumenty/search - list all docs        │
│     c. Compare with krs_documents - find new ones    │
│     d. INSERT new documents into krs_documents       │
│     e. For each new document:                        │
│        - GET /dokumenty/metadata/{id}                │
│        - POST /dokumenty/download (ZIP)              │
│        - Extract ZIP -> raw files + manifest.json    │
│        - Save extracted files to storage backend     │
│        - UPDATE krs_documents (is_downloaded=true)   │
│     f. UPDATE krs_registry timestamps & stats        │
│  5. Update scraper_runs (status=completed)           │
└─────────────────────────────────────────────────────┘
```

### Ordering strategies (configurable)

```env
# How to order KRS iteration
SCRAPER_ORDER_STRATEGY=priority_then_oldest

# Options:
#   priority_then_oldest  - high priority first, then oldest last_checked_at
#   oldest_first          - least recently checked first
#   newest_first          - most recently added first
#   random                - random order each run (good for fairness)
#   sequential            - KRS number ascending (predictable)
```

### Rate limiting & politeness

```env
SCRAPER_DELAY_BETWEEN_KRS=2.0        # seconds between KRS entities
SCRAPER_DELAY_BETWEEN_REQUESTS=0.5   # seconds between individual API calls
SCRAPER_MAX_KRS_PER_RUN=0            # 0 = unlimited, >0 = stop after N
SCRAPER_MAX_ERRORS_BEFORE_SKIP=3     # skip KRS after N consecutive errors
SCRAPER_ERROR_BACKOFF_HOURS=24       # don't retry errored KRS for N hours
SCRAPER_DOWNLOAD_TIMEOUT=60          # timeout for document download (seconds)
```

### Error handling

The scraper never crashes on a single KRS failure. Each KRS is wrapped in try/except:

- On transient error (timeout, 429, 503): increment `check_error_count`, log, continue
- On permanent error (404 for previously-valid KRS): mark `is_active=false`, log, continue
- On download error for specific document: set `download_error` on that document, continue to next
- On 3+ consecutive errors for same KRS: skip until `error_backoff_hours` passes

### KRS source: how to populate krs_registry

The scraper needs an initial list of KRS numbers. Three approaches (all supported):

1. **CSV import** - load from a CSV file with KRS numbers:
   ```bash
   python -m app.scraper.cli import-krs --file krs_list.csv --column krs
   ```

2. **Range scan** - KRS numbers are sequential 1 to ~1,000,000. Scan a range:
   ```bash
   python -m app.scraper.cli import-range --from 1 --to 1000000
   ```
   This adds entries to `krs_registry` but does NOT validate them yet - validation happens during the scan.

3. **API discovery** - use the official Open API KRS (`prs.ms.gov.pl/krs/openApi`) to get valid KRS numbers. This is a separate optional step.

### CLI interface

```bash
# Run a full scan (all KRS in registry, ordered by strategy)
python -m app.scraper.cli run

# Run for specific KRS numbers only
python -m app.scraper.cli run --krs 694720 --krs 12345

# Run only for KRS not yet checked
python -m app.scraper.cli run --mode new_only

# Retry only errored KRS
python -m app.scraper.cli run --mode retry_errors

# Import KRS numbers from CSV
python -m app.scraper.cli import-krs --file data/krs_numbers.csv

# Import a numeric range
python -m app.scraper.cli import-range --from 1 --to 100000

# Show stats
python -m app.scraper.cli status

# Export monitoring report
python -m app.scraper.cli report --format csv --output report.csv
```

---

## 5. Project Structure (new modules)

```
app/
  main.py                    # registers domain routers
  config.py                  # EXTENDED with new env vars
  crypto.py                  # KRS encryption (unchanged)
  rdf_client.py              # upstream API client (unchanged)

  routers/                   # API domains (see docs/API_REORGANIZATION.md)
    rdf/                     # RDF proxy endpoints
      __init__.py
      podmiot.py
      dokumenty.py
      schemas.py
    analysis/                # Financial statement analysis
      __init__.py
      routes.py
      schemas.py
    scraper/                 # Scraper monitoring (NEW)
      __init__.py
      routes.py

  services/
    xml_parser.py            # unchanged

  scraper/                   # Scraper job logic (NOT an API router)
    __init__.py
    cli.py                   # Click-based CLI entrypoint
    db.py                    # DuckDB connection manager + schema init
    storage.py               # StorageBackend protocol + LocalStorage
    job.py                   # Main scraper job logic (the big loop)

data/                        # default local storage (gitignored)
  scraper.duckdb
  documents/
    krs/
      0000694720/
        ZgsX8Fsncb1PFW07-T4XoQ/
          Bjwk_SF_za_2024.xml
          manifest.json
```

### Config additions (app/config.py)

```python
class Settings(BaseSettings):
    # ... existing fields ...
    
    # DuckDB
    scraper_db_path: str = "data/scraper.duckdb"
    
    # Storage
    storage_backend: str = "local"          # 'local' or 'gcs'
    storage_local_path: str = "data/documents"
    storage_gcs_bucket: str = ""
    storage_gcs_prefix: str = "krs/"
    
    # Scraper behavior
    scraper_order_strategy: str = "priority_then_oldest"
    scraper_delay_between_krs: float = 2.0
    scraper_delay_between_requests: float = 0.5
    scraper_max_krs_per_run: int = 0
    scraper_max_errors_before_skip: int = 3
    scraper_error_backoff_hours: int = 24
    scraper_download_timeout: int = 60
```

---

## 6. GCP Deployment

### Architecture

```
┌─────────────────────────────────────────────────┐
│              GCP Project: rdf-scraper            │
│                                                   │
│  ┌──────────────────────────────────────────┐    │
│  │     Compute Engine VM (e2-small)          │    │
│  │                                            │    │
│  │  ┌──────────────────────────────────┐     │    │
│  │  │   FastAPI (uvicorn, port 8000)    │     │    │
│  │  │   - Existing API proxy            │     │    │
│  │  │   - Analysis endpoints            │     │    │
│  │  │   - Runs as systemd service       │     │    │
│  │  └──────────────────────────────────┘     │    │
│  │                                            │    │
│  │  ┌──────────────────────────────────┐     │    │
│  │  │   Scraper Job (cron)              │     │    │
│  │  │   - Runs daily at 02:00 UTC       │     │    │
│  │  │   - python -m app.scraper.cli run │     │    │
│  │  └──────────────────────────────────┘     │    │
│  │                                            │    │
│  │  ┌──────────────┐  ┌─────────────────┐   │    │
│  │  │  DuckDB file  │  │ Local docs dir  │   │    │
│  │  │  (SSD disk)   │  │ OR GCS bucket   │   │    │
│  │  └──────────────┘  └─────────────────┘   │    │
│  └──────────────────────────────────────────┘    │
│                                                   │
│  ┌────────────────────┐                           │
│  │  GCS Bucket         │  (optional, for docs)    │
│  │  rdf-documents-prod │                           │
│  └────────────────────┘                           │
│                                                   │
│  ┌────────────────────┐                           │
│  │  Cloud Monitoring   │  (VM metrics + custom)   │
│  └────────────────────┘                           │
└─────────────────────────────────────────────────┘
```

### VM Specification

| Setting | Value | Rationale |
|---------|-------|-----------|
| Machine type | `e2-small` (2 vCPU, 2 GB RAM) | Sufficient for single-process scraper + API |
| OS | Ubuntu 24.04 LTS | Matches dev environment |
| Boot disk | 20 GB SSD (`pd-balanced`) | For OS + DuckDB + Python |
| Data disk | 200-500 GB `pd-standard` | For extracted documents (raw XML/PDF), size depends on scope |
| Region | `europe-central2` (Warsaw) | Closest to rdf-przegladarka.ms.gov.pl |
| Service account | Custom SA with `storage.objectAdmin` | Only if using GCS |

### Estimated monthly cost

| Resource | Spec | Monthly cost (USD) |
|----------|------|--------------------|
| e2-small VM | 730 hrs/mo | ~$12 |
| Boot disk (20 GB SSD) | pd-balanced | ~$2 |
| Data disk (500 GB HDD) | pd-standard | ~$20 |
| GCS bucket (optional, 500 GB) | Standard | ~$10 |
| Egress | Minimal (API only) | ~$1 |
| **Total** | | **~$35-45/mo** |

### Setup script

```bash
#!/bin/bash
# setup-vm.sh - run after SSH into fresh VM

# System packages
sudo apt update && sudo apt install -y python3.12 python3.12-venv python3-pip git

# App directory
sudo mkdir -p /opt/rdf-api
sudo chown $USER:$USER /opt/rdf-api
cd /opt/rdf-api

# Clone and install
git clone <REPO_URL> .
python3.12 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pip install duckdb gcloud-aio-storage aiofiles click

# Create data directories
mkdir -p data/documents

# Copy config
cp .env.example .env
# Edit .env with production values
```

### Systemd service (API)

```ini
# /etc/systemd/system/rdf-api.service
[Unit]
Description=RDF API Proxy
After=network.target

[Service]
Type=simple
User=rdf
WorkingDirectory=/opt/rdf-api
Environment="PATH=/opt/rdf-api/.venv/bin"
EnvironmentFile=/opt/rdf-api/.env
ExecStart=/opt/rdf-api/.venv/bin/uvicorn app.main:app \
    --host 0.0.0.0 --port 8000 --workers 2
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

### Cron job (Scraper)

```bash
# /etc/cron.d/rdf-scraper
# Run scraper daily at 02:00 UTC, log to file
0 2 * * * rdf cd /opt/rdf-api && /opt/rdf-api/.venv/bin/python -m app.scraper.cli run \
    >> /var/log/rdf-scraper/scraper.log 2>&1
```

### Log rotation

```
# /etc/logrotate.d/rdf-scraper
/var/log/rdf-scraper/*.log {
    daily
    rotate 30
    compress
    missingok
    notifempty
    create 0644 rdf rdf
}
```

### Firewall rules

```bash
# Allow API access (adjust source range as needed)
gcloud compute firewall-rules create allow-rdf-api \
    --allow tcp:8000 \
    --source-ranges 0.0.0.0/0 \
    --target-tags rdf-api

# SSH (default, already exists)
```

### Monitoring

The VM gets automatic Cloud Monitoring metrics (CPU, memory, disk). For application-level monitoring, the scraper writes to `scraper_runs` table. A simple health endpoint exposes key stats:

```
GET /api/scraper/status
```
```json
{
  "db_path": "data/scraper.duckdb",
  "storage_backend": "local",
  "total_krs": 150000,
  "krs_checked": 42000,
  "krs_unchecked": 108000,
  "total_documents": 185000,
  "total_downloaded": 120000,
  "last_run": {
    "run_id": "abc123",
    "started_at": "2026-03-20T02:00:00",
    "finished_at": "2026-03-20T04:15:00",
    "status": "completed",
    "krs_checked": 500,
    "documents_downloaded": 1200
  },
  "disk_usage_gb": 45.2,
  "errors_last_24h": 3
}
```

---

## 7. Dependency Additions

New packages to add to `requirements.txt`:

```
# Existing
fastapi>=0.115
uvicorn[standard]>=0.34
httpx>=0.28
pycryptodome>=3.21
pydantic>=2.10
pydantic-settings>=2.7

# New - scraper
duckdb>=1.2
aiofiles>=24.1
click>=8.1

# New - GCS (optional, only if STORAGE_BACKEND=gcs)
gcloud-aio-storage>=9.3

# Testing
pytest>=8.0
pytest-asyncio>=0.24
```

---

## 8. Implementation Order

Recommended phased approach:

### Phase 1: Database + Storage (foundation)

1. `app/scraper/db.py` - DuckDB connection manager, schema creation, basic CRUD
2. `app/scraper/storage.py` - StorageBackend protocol, LocalStorage implementation
3. `app/scraper/models.py` - Pydantic models for KRS registry, documents, runs
4. Tests for DB operations and storage

### Phase 2: Scraper job (core logic)

5. `app/scraper/strategies.py` - KRS ordering strategies
6. `app/scraper/job.py` - Main scraper loop with rate limiting and error handling
7. `app/scraper/cli.py` - CLI entrypoint (import-krs, import-range, run, status)
8. Integration tests with mocked RDF client

### Phase 3: Monitoring + API

9. Status endpoint (`GET /api/scraper/status`)
10. Extend `app/config.py` with new settings
11. End-to-end test with real API (small KRS range)

### Phase 4: GCS + GCP deployment

12. `GCSStorage` implementation
13. VM setup scripts, systemd config, cron
14. Deployment documentation + runbook

---

## 9. Open Questions for Review

Before starting implementation, please confirm:

1. **KRS range scope** - Do you have a specific list of KRS numbers to start with, or should we begin with a range scan (e.g. 1-1,000,000)? Range scan will encounter many invalid numbers (~50% will return "not found"), but it's the most comprehensive approach.

2. **Document types** - Should we download ALL document types (rodzaj 3, 4, 18, 19, 20) or only annual financial statements (rodzaj 18)?

3. **Storage sizing** - A typical extracted XML is 50-200 KB (ZIP was 15-60 KB). With ~500K active companies and ~5 docs each, that's roughly 125-500 GB uncompressed. Is 500 GB data disk a good starting point?

4. **Rate limiting sensitivity** - The upstream API has no documented rate limits. The default 2s delay between KRS means ~43,000 KRS/day. At this rate, a full scan of 500K would take ~12 days. Should we be more aggressive or more conservative?

5. **Access control** - Should the `/api/scraper/status` endpoint be protected (API key, IP whitelist), or is it fine to leave open on the VM?

6. **Backup strategy** - DuckDB file can be backed up with a simple copy. Should we set up daily GCS backup of the DB file?
