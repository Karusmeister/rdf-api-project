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
| PostgreSQL | Docker Compose locally | Cloud SQL or VM-local | Each environment has its own instance |
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
DATABASE_URL=postgresql://rdf:rdf_dev@localhost:5432/rdf

# --- Storage ---
STORAGE_BACKEND=local
STORAGE_LOCAL_PATH=data/documents

# --- Auth ---
ENVIRONMENT=local
JWT_SECRET=change-me-in-production
VERIFICATION_EMAIL_MODE=log

# --- Batch (not typically run locally anymore) ---
BATCH_USE_VPN=false
BATCH_WORKERS=4
BATCH_DELAY_SECONDS=2.5
```

### GCP `.env` (on the VM)

```bash
# --- Database ---
DATABASE_URL=postgresql://rdf:rdf_prod@localhost:5432/rdf

# --- Storage ---
STORAGE_BACKEND=gcs
STORAGE_GCS_BUCKET=rdf-project-documents
STORAGE_GCS_PREFIX=krs/

# --- Auth (required if running the API server on GCP) ---
ENVIRONMENT=production
JWT_SECRET=<generate with: python -c "import secrets; print(secrets.token_hex(32))">
VERIFICATION_EMAIL_MODE=smtp
SMTP_HOST=smtp.example.com
SMTP_PORT=587
SMTP_USER=noreply@example.com
SMTP_PASSWORD=<smtp-password>
SMTP_FROM=noreply@example.com
GOOGLE_CLIENT_ID=<google-oauth-client-id>

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
- `DATABASE_URL` points to the VM-local or Cloud SQL PostgreSQL instance
- `STORAGE_BACKEND=gcs` — documents go to a GCS bucket, not local disk
- `ENVIRONMENT=production` — enforces JWT secret validation at startup
- Auth vars (`JWT_SECRET`, SMTP, `GOOGLE_CLIENT_ID`) required if running the API server
- `BATCH_DELAY_SECONDS` may be tuned lower/higher depending on rate limiting from cloud IPs
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
import psycopg2
conn = psycopg2.connect('postgresql://rdf:rdf_prod@localhost:5432/rdf')
cur = conn.cursor()
cur.execute('SELECT status, COUNT(*) FROM batch_progress GROUP BY status')
for row in cur.fetchall():
    print(f'{row[0]}: {row[1]:,}')
conn.close()
\""
```

## Data Synchronization

### Cloud → Local (pull DB snapshot to your Mac)

When you want to analyze the latest cloud data locally, use `pg_dump` / `pg_restore`:

```bash
# On the VM: dump PostgreSQL to GCS
gcloud compute ssh rdf-batch-vm --zone=europe-central2-a --command="
  pg_dump -U rdf rdf | gzip > /tmp/rdf-dump.sql.gz
  gsutil cp /tmp/rdf-dump.sql.gz gs://rdf-project-data/backups/rdf-$(date +%Y%m%d-%H%M).sql.gz
"

# On your Mac: download and restore into local dev DB
gsutil cp gs://rdf-project-data/backups/rdf-latest.sql.gz /tmp/rdf-cloud.sql.gz
gunzip -c /tmp/rdf-cloud.sql.gz | psql postgresql://rdf:rdf_dev@localhost:5432/rdf_cloud
```

### Local → Cloud (push local DB to the VM)

Initial migration of your entities + document metadata:

```bash
# On your Mac: dump and upload to GCS
pg_dump -U rdf rdf | gzip > /tmp/rdf-seed.sql.gz
gsutil cp /tmp/rdf-seed.sql.gz gs://rdf-project-data/seed/rdf-seed.sql.gz

# On the VM: download and restore
gcloud compute ssh rdf-batch-vm --zone=europe-central2-a --command="
  gsutil cp gs://rdf-project-data/seed/rdf-seed.sql.gz /tmp/rdf-seed.sql.gz
  gunzip -c /tmp/rdf-seed.sql.gz | psql -U rdf rdf
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
# Work on API, ETL, analysis — everything uses local PostgreSQL
uvicorn app.main:app --reload --port 8000
pytest tests/ -v
```

### When you need fresh cloud data
```bash
# Pull latest DB snapshot (see Data Synchronization above)
gsutil cp gs://rdf-project-data/backups/rdf-latest.sql.gz /tmp/
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
| Persistent disk | 50GB SSD (PostgreSQL + OS) | ~$8 |
| GCS storage | ~500GB (documents, growing) | ~$10-15 |
| GCS operations | PUT/GET for documents | ~$2-5 |
| Network egress | Minimal (data stays in GCP) | ~$1 |
| **Total** | | **~$35-50/month** |

Notes:
- Spot VM saves ~60-70% vs on-demand. If preempted, workers resume from checkpoint.
- GCS Standard class. Could use Nearline ($0.01/GB) for old documents if needed.
- No load balancer, no managed DB, no Kubernetes overhead.
