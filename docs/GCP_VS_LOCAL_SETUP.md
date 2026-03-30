# GCP vs Local Development Setup

**Date:** 2026-03-30

This document explains how the same codebase runs in two modes: **local development** (your Mac, API + experimentation) and **GCP cloud** (batch scraping at scale).

## What Runs Where

| Component | Local | GCP | Why |
|-----------|-------|-----|-----|
| FastAPI app (`uvicorn`) | Yes | No (later) | You're actively developing endpoints, analysis, ETL |
| KRS batch scanner (`batch/runner`) | No (done enough) | Yes | 708K probed locally; finish the remaining ~300K in cloud |
| RDF doc discovery (`batch/rdf_runner`) | No | Yes | 315K entities need document discovery — heavy, long-running |
| RDF doc download | No | Yes | Tens of thousands of ZIPs to download and extract |
| DuckDB database | Local file | VM local SSD | Each environment has its own copy; sync via GCS snapshots |
| Document storage | `data/documents/` | GCS bucket | Cloud uses `STORAGE_BACKEND=gcs`; local stays `local` |
| Tests | Yes | No | Tests run on your machine and in CI (if added later) |

## Environment Configuration

### Local `.env`

```bash
# --- API ---
RDF_BASE_URL=https://rdf-przegladarka.ms.gov.pl/services/rdf/przegladarka-dokumentow-finansowych
REQUEST_TIMEOUT=30
CORS_ORIGINS=["http://localhost:3000","http://localhost:5173"]

# --- Database ---
SCRAPER_DB_PATH=data/scraper.duckdb
BATCH_DB_PATH=data/scraper.duckdb

# --- Storage ---
STORAGE_BACKEND=local
STORAGE_LOCAL_PATH=data/documents

# --- Batch (not typically run locally anymore) ---
BATCH_USE_VPN=false
BATCH_WORKERS=4
BATCH_DELAY_SECONDS=2.5
```

### GCP `.env` (on the VM)

```bash
# --- Database ---
SCRAPER_DB_PATH=/data/scraper.duckdb
BATCH_DB_PATH=/data/scraper.duckdb

# --- Storage ---
STORAGE_BACKEND=gcs
STORAGE_GCS_BUCKET=rdf-project-documents
STORAGE_GCS_PREFIX=krs/

# --- KRS Batch Scanner ---
BATCH_USE_VPN=false
BATCH_WORKERS=4
BATCH_START_KRS=1
BATCH_CONCURRENCY_PER_WORKER=3
BATCH_DELAY_SECONDS=2.0

# --- RDF Batch (document discovery + download) ---
RDF_BATCH_CONCURRENCY=3
RDF_BATCH_DELAY_SECONDS=2.0
RDF_BATCH_PAGE_SIZE=100

# --- Timeouts ---
REQUEST_TIMEOUT=30
SCRAPER_DOWNLOAD_TIMEOUT=60
```

Key differences:
- `STORAGE_BACKEND=gcs` — documents go to a GCS bucket, not local disk
- `BATCH_DELAY_SECONDS` may be tuned lower/higher depending on rate limiting from cloud IPs
- No CORS, no API server config — the VM only runs batch workers
- VPN is optional — cloud IPs provide natural IP diversity; enable if rate-limited

## How the VM Runs Batch Workers

### Starting KRS Scanner

```bash
# SSH into the VM
gcloud compute ssh rdf-batch-vm --zone=europe-central2-a

# Activate the environment
cd /opt/rdf-api-project
source .venv/bin/activate

# Run KRS scanner (resumes from where batch_progress left off)
nohup python -m batch.runner \
  --workers 4 \
  --concurrency 3 \
  --delay 2.0 \
  > /var/log/krs-scanner.log 2>&1 &

# Check progress
tail -f /var/log/krs-scanner.log
```

### Starting RDF Document Worker

```bash
# Run RDF discovery + download
nohup python -m batch.rdf_runner \
  --workers 4 \
  --concurrency 3 \
  --delay 2.0 \
  > /var/log/rdf-worker.log 2>&1 &

# Check progress
tail -f /var/log/rdf-worker.log
```

### Checking Progress from Local Machine

```bash
# View live logs without SSH
gcloud compute ssh rdf-batch-vm --zone=europe-central2-a \
  --command="tail -20 /var/log/krs-scanner.log"

# Or stream logs
gcloud compute ssh rdf-batch-vm --zone=europe-central2-a \
  --command="tail -f /var/log/rdf-worker.log"

# Query progress directly on VM
gcloud compute ssh rdf-batch-vm --zone=europe-central2-a --command="
cd /opt/rdf-api-project && source .venv/bin/activate && python3 -c \"
import duckdb
conn = duckdb.connect('/data/scraper.duckdb', read_only=True)
for row in conn.execute('SELECT status, COUNT(*) FROM batch_progress GROUP BY status').fetchall():
    print(f'{row[0]}: {row[1]:,}')
print('---')
for row in conn.execute('SELECT status, COUNT(*), COALESCE(SUM(documents_found),0) FROM batch_rdf_progress GROUP BY status').fetchall():
    print(f'{row[0]}: {row[1]:,} krs, {int(row[2]):,} docs')
conn.close()
\""
```

## Data Synchronization

### Cloud → Local (pull DB snapshot to your Mac)

When you want to analyze the latest cloud data locally:

```bash
# On the VM: backup DuckDB to GCS
gcloud compute ssh rdf-batch-vm --zone=europe-central2-a --command="
  gsutil cp /data/scraper.duckdb gs://rdf-project-data/backups/scraper-$(date +%Y%m%d-%H%M).duckdb
"

# On your Mac: download the snapshot
gsutil cp gs://rdf-project-data/backups/scraper-latest.duckdb data/scraper-cloud.duckdb

# Query the cloud DB locally (read-only, separate from your dev DB)
python3 -c "
import duckdb
conn = duckdb.connect('data/scraper-cloud.duckdb', read_only=True)
print(conn.execute('SELECT COUNT(*) FROM krs_entities').fetchone())
conn.close()
"
```

### Local → Cloud (push local DB to the VM)

Initial migration of your 315K entities + 35K documents metadata:

```bash
# On your Mac: upload to GCS
gsutil cp data/scraper.duckdb gs://rdf-project-data/seed/scraper.duckdb

# On the VM: download and use
gcloud compute ssh rdf-batch-vm --zone=europe-central2-a --command="
  gsutil cp gs://rdf-project-data/seed/scraper.duckdb /data/scraper.duckdb
"
```

### Document Access

Documents live in GCS in cloud mode. To access them locally:

```bash
# List documents for a KRS
gsutil ls gs://rdf-project-documents/krs/0000694720/

# Download a specific document's files
gsutil -m cp -r gs://rdf-project-documents/krs/0000694720/ZgsX8Fsncb1PFW07-T4XoQ/ data/documents/krs/0000694720/
```

## Workflow: Typical Day

### Morning (check cloud progress)
```bash
# Quick status check — no SSH needed
gcloud compute ssh rdf-batch-vm --zone=europe-central2-a \
  --command="tail -5 /var/log/krs-scanner.log; echo '---'; tail -5 /var/log/rdf-worker.log"
```

### During the day (local development)
```bash
# Work on API, ETL, analysis — everything uses local DuckDB
uvicorn app.main:app --reload --port 8000
pytest tests/ -v
```

### When you need fresh cloud data
```bash
# Pull latest DB snapshot
gsutil cp gs://rdf-project-data/backups/scraper-latest.duckdb data/scraper-cloud.duckdb
```

### If workers crash or stall
```bash
# SSH in, check logs, restart
gcloud compute ssh rdf-batch-vm --zone=europe-central2-a
tail -100 /var/log/rdf-worker.log
# Restart if needed — workers resume automatically from last checkpoint
nohup python -m batch.rdf_runner --workers 4 > /var/log/rdf-worker.log 2>&1 &
```

## Cost Summary

| Resource | Spec | Monthly Cost |
|----------|------|-------------|
| GCE VM (spot) | e2-standard-2 (2 vCPU, 8GB RAM) | ~$15-20 |
| Persistent disk | 50GB SSD (DuckDB + OS) | ~$8 |
| GCS storage | ~500GB (documents, growing) | ~$10-15 |
| GCS operations | PUT/GET for documents | ~$2-5 |
| Network egress | Minimal (data stays in GCP) | ~$1 |
| **Total** | | **~$35-50/month** |

Notes:
- Spot VM saves ~60-70% vs on-demand. If preempted, workers resume from checkpoint.
- GCS Standard class. Could use Nearline ($0.01/GB) for old documents if needed.
- No load balancer, no managed DB, no Kubernetes overhead.
