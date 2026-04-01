# Pre-Deployment Tasks for GCP

**Date:** 2026-03-30

Tasks that must be completed before batch workers can run on GCP, ordered by dependency.

## Phase 1: Code Changes (run locally, commit)

### Task 1: Implement GCS Storage Backend
**Priority:** Blocker
**Effort:** 2-3 hours

The `StorageBackend` protocol exists in `app/scraper/storage.py` but the GCS implementation raises `NotImplementedError`.

**What to implement:**
```python
class GcsStorage:
    def __init__(self, bucket: str, prefix: str):
        from google.cloud import storage
        self._client = storage.Client()
        self._bucket = self._client.bucket(bucket)
        self._prefix = prefix

    def save_extracted(self, doc_dir: str, zip_bytes: bytes, document_id: str) -> dict:
        # Extract ZIP in memory, upload each file to GCS
        # Upload manifest.json
        # Return manifest dict

    def exists(self, path: str) -> bool:
        # Check blob exists

    def read(self, path: str) -> bytes:
        # Download blob

    def list_files(self, dir_path: str) -> list:
        # List blobs with prefix

    def get_full_path(self, path: str) -> str:
        # Return gs:// URI
```

**Files to modify:**
- `app/scraper/storage.py` — add `GcsStorage` class, update `create_storage()`
- `requirements.txt` — add `google-cloud-storage>=2.18`

**Test plan:**
- Unit test with mocked GCS client
- Manual test: upload one document to a test bucket, verify files appear

### Task 2: Add `google-cloud-storage` Dependency
**Priority:** Blocker (part of Task 1)
**Effort:** 5 minutes

```bash
pip install google-cloud-storage>=2.18
pip freeze | grep google-cloud-storage >> requirements.txt
# Or better: add google-cloud-storage>=2.18 to requirements.txt manually
```

### Task 3: Harden Dockerfile for Batch Workers
**Priority:** Nice-to-have (works without this)
**Effort:** 30 minutes

Current Dockerfile only copies `app/` — batch workers also need `batch/` and `scripts/`.

**New Dockerfile** (or `Dockerfile.batch`):
```dockerfile
FROM python:3.12-slim AS base
WORKDIR /opt/rdf-api-project

# Install deps
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY app/ app/
COPY batch/ batch/
COPY scripts/ scripts/

# Non-root user
RUN useradd -r -s /bin/false worker
USER worker

# Health check (optional — useful if managed by systemd)
HEALTHCHECK --interval=60s --timeout=5s \
  CMD python -c "import psycopg2; psycopg2.connect('postgresql://rdf:rdf_prod@localhost:5432/rdf').close()"

# Default: run KRS scanner
CMD ["python", "-m", "batch.runner"]
```

**Files to create/modify:**
- `Dockerfile.batch` — new file for batch workers
- `.dockerignore` — add `data/`, `tests/`, `.venv/`, `__pycache__/`, `.git/`

### Task 4: Add Structured JSON Logging (Optional)
**Priority:** Nice-to-have
**Effort:** 1 hour

Cloud Logging parses JSON log lines automatically. Current workers use `logging.basicConfig` with plain text. Adding JSON formatting makes logs searchable in Cloud Console.

**Minimal change:**
```python
import json, logging

class JsonFormatter(logging.Formatter):
    def format(self, record):
        return json.dumps({
            "severity": record.levelname,
            "message": record.getMessage(),
            "worker": getattr(record, "worker_id", None),
            "timestamp": self.formatTime(record),
            **(record.__dict__.get("extra", {})),
        })
```

Not a blocker — plain text logs are readable in Cloud Logging too.

## Phase 2: GCP Project Setup (manual or Terraform)

> **IMPORTANT — gcloud project isolation:**
> This machine runs multiple GCP projects. ALL gcloud/gsutil commands in this document
> MUST be prefixed with `CLOUDSDK_ACTIVE_CONFIG=rdf-project` to ensure they target the
> correct project. See Task A-6 in `GCP_AGENT_TASKS.md` for setup.
>
> Shorthand used below: `GC="CLOUDSDK_ACTIVE_CONFIG=rdf-project"`

### Task 5: Create GCP Project and Enable APIs
**Priority:** Blocker
**Effort:** 15 minutes

```bash
# Create project (or use existing)
CLOUDSDK_ACTIVE_CONFIG=rdf-project gcloud projects create rdf-api-project --name="RDF API Project"
CLOUDSDK_ACTIVE_CONFIG=rdf-project gcloud config set project rdf-api-project

# Enable required APIs
CLOUDSDK_ACTIVE_CONFIG=rdf-project gcloud services enable compute.googleapis.com
CLOUDSDK_ACTIVE_CONFIG=rdf-project gcloud services enable storage.googleapis.com

# Set default region (Warsaw)
CLOUDSDK_ACTIVE_CONFIG=rdf-project gcloud config set compute/region europe-central2
CLOUDSDK_ACTIVE_CONFIG=rdf-project gcloud config set compute/zone europe-central2-a
```

### Task 6: Create GCS Buckets
**Priority:** Blocker
**Effort:** 10 minutes

```bash
# Document storage bucket (main, long-lived)
CLOUDSDK_ACTIVE_CONFIG=rdf-project gsutil mb -l europe-central2 gs://rdf-project-documents/

# Data/backup bucket (DB snapshots, seeds)
CLOUDSDK_ACTIVE_CONFIG=rdf-project gsutil mb -l europe-central2 gs://rdf-project-data/

# Lifecycle: auto-delete old backups after 30 days
CLOUDSDK_ACTIVE_CONFIG=rdf-project gsutil lifecycle set <(cat <<'EOF'
{
  "rule": [{"action": {"type": "Delete"}, "condition": {"age": 30, "matchesPrefix": ["backups/"]}}]
}
EOF
) gs://rdf-project-data/
```

### Task 7: Create GCE VM
**Priority:** Blocker
**Effort:** 15 minutes

```bash
CLOUDSDK_ACTIVE_CONFIG=rdf-project gcloud compute instances create rdf-batch-vm \
  --zone=europe-central2-a \
  --machine-type=e2-standard-2 \
  --provisioning-model=SPOT \
  --instance-termination-action=STOP \
  --boot-disk-size=50GB \
  --boot-disk-type=pd-ssd \
  --image-family=debian-12 \
  --image-project=debian-cloud \
  --scopes=storage-full \
  --metadata=startup-script='#!/bin/bash
    apt-get update && apt-get install -y python3-pip python3-venv git
    mkdir -p /data /opt/rdf-api-project
  '
```

Key flags:
- `--provisioning-model=SPOT` — 60-70% cheaper, VM may be preempted (workers resume from checkpoint)
- `--instance-termination-action=STOP` — on preemption, stop instead of delete (preserves disk)
- `--scopes=storage-full` — VM can read/write GCS without separate service account key
- `europe-central2` — Warsaw region, closest to `rdf-przegladarka.ms.gov.pl`

## Phase 3: Database Migration (local → cloud)

### Task 8: Migrate Local PostgreSQL to Cloud
**Priority:** Blocker
**Effort:** 30 minutes

This carries your entities, document metadata, and progress records to the cloud VM.

```bash
# Step 1: On your Mac — dump and upload to GCS
pg_dump -U rdf rdf | gzip > /tmp/rdf-seed.sql.gz
CLOUDSDK_ACTIVE_CONFIG=rdf-project gsutil cp /tmp/rdf-seed.sql.gz gs://rdf-project-data/seed/rdf-seed.sql.gz

# Step 2: On the VM — install PostgreSQL, download and restore
CLOUDSDK_ACTIVE_CONFIG=rdf-project gcloud compute ssh rdf-batch-vm --zone=europe-central2-a --command="
  sudo apt-get install -y postgresql postgresql-client
  sudo -u postgres createuser rdf && sudo -u postgres createdb -O rdf rdf
  gsutil cp gs://rdf-project-data/seed/rdf-seed.sql.gz /tmp/rdf-seed.sql.gz
  gunzip -c /tmp/rdf-seed.sql.gz | psql -U rdf rdf
"

# Step 3: Verify data integrity on VM
CLOUDSDK_ACTIVE_CONFIG=rdf-project gcloud compute ssh rdf-batch-vm --zone=europe-central2-a --command="
  psql -U rdf rdf -c \"
    SELECT 'batch_progress', COUNT(*) FROM batch_progress
    UNION ALL SELECT 'krs_entities_current', COUNT(*) FROM krs_entities_current
    UNION ALL SELECT 'krs_documents_current', COUNT(*) FROM krs_documents_current;
  \"
"
```

### Task 9: Decide What to Do with Locally Downloaded Documents
**Priority:** Low
**Effort:** 15 minutes

You have 264 downloaded documents locally (168MB). Options:

1. **Upload to GCS** — preserves them, small effort:
   ```bash
   CLOUDSDK_ACTIVE_CONFIG=rdf-project gsutil -m cp -r data/documents/krs/ gs://rdf-project-documents/krs/
   ```

2. **Re-download in cloud** — the 264 docs are a tiny fraction. The cloud workers will skip already-marked documents in PostgreSQL and won't re-download them. But the files won't be in GCS unless uploaded.

3. **Ignore** — keep locally for development/testing. Cloud workers will discover the same documents independently.

**Recommendation:** Option 1 (upload). 168MB is trivial. Then mark the `storage_backend` column to `gcs` for those 264 rows, or just let the cloud workers handle it — they check `is_downloaded` in the DB, not the actual file existence.

## Phase 4: Deploy and Start Workers

### Task 10: Deploy Code to VM
**Priority:** Blocker
**Effort:** 20 minutes

```bash
# SSH into VM
CLOUDSDK_ACTIVE_CONFIG=rdf-project gcloud compute ssh rdf-batch-vm --zone=europe-central2-a

# Clone repo (or rsync — repo is private?)
cd /opt
git clone <your-repo-url> rdf-api-project
cd rdf-api-project

# Setup Python environment
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# Create .env (use the GCP config from GCP_VS_LOCAL_SETUP.md)
cat > .env << 'EOF'
DATABASE_URL=postgresql://rdf:rdf_prod@localhost:5432/rdf
STORAGE_BACKEND=gcs
STORAGE_GCS_BUCKET=rdf-project-documents
STORAGE_GCS_PREFIX=krs/
BATCH_USE_VPN=false
BATCH_WORKERS=4
BATCH_CONCURRENCY_PER_WORKER=3
BATCH_DELAY_SECONDS=2.0
RDF_BATCH_CONCURRENCY=3
RDF_BATCH_DELAY_SECONDS=2.0
RDF_BATCH_PAGE_SIZE=100
REQUEST_TIMEOUT=30
SCRAPER_DOWNLOAD_TIMEOUT=60

# Auth (only needed if running the API server on this VM)
ENVIRONMENT=production
JWT_SECRET=<generate: python -c "import secrets; print(secrets.token_hex(32))">
VERIFICATION_EMAIL_MODE=log
EOF
```

### Task 11: Create systemd Services (Optional but Recommended)
**Priority:** Nice-to-have
**Effort:** 30 minutes

Using systemd instead of `nohup` gives you:
- Auto-restart on crash or VM preemption recovery
- Log management via journalctl
- Clean start/stop with `systemctl`

```ini
# /etc/systemd/system/krs-scanner.service
[Unit]
Description=KRS Batch Scanner
After=network.target

[Service]
Type=simple
User=worker
WorkingDirectory=/opt/rdf-api-project
EnvironmentFile=/opt/rdf-api-project/.env
ExecStart=/opt/rdf-api-project/.venv/bin/python -m batch.runner
Restart=on-failure
RestartSec=30

[Install]
WantedBy=multi-user.target
```

```ini
# /etc/systemd/system/rdf-worker.service
[Unit]
Description=RDF Document Discovery & Download Worker
After=network.target

[Service]
Type=simple
User=worker
WorkingDirectory=/opt/rdf-api-project
EnvironmentFile=/opt/rdf-api-project/.env
ExecStart=/opt/rdf-api-project/.venv/bin/python -m batch.rdf_runner
Restart=on-failure
RestartSec=30

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl daemon-reload
sudo systemctl enable krs-scanner rdf-worker
sudo systemctl start krs-scanner
sudo systemctl start rdf-worker

# Check status
sudo systemctl status krs-scanner rdf-worker
sudo journalctl -u rdf-worker -f
```

### Task 12: Set Up DB Backup Cron
**Priority:** Important
**Effort:** 15 minutes

```bash
# /etc/cron.d/rdf-backup
# Back up DuckDB to GCS every 6 hours
0 */6 * * * root gsutil cp /data/scraper.duckdb gs://rdf-project-data/backups/scraper-$(date +\%Y\%m\%d-\%H\%M).duckdb && gsutil cp /data/scraper.duckdb gs://rdf-project-data/backups/scraper-latest.duckdb
```

## Task Dependency Graph

```
Phase 1 (code, local)         Phase 2 (GCP setup)        Phase 3 (migration)
─────────────────────         ──────────────────          ──────────────────
Task 1: GCS backend ──┐      Task 5: Project setup ──┐
Task 2: Dependency ───┤      Task 6: GCS buckets ────┤
Task 3: Dockerfile ───┤      Task 7: Create VM ──────┤
Task 4: JSON logging ─┘                              │
        │                              │              │
        └──────────────────────────────┴──────────────┘
                                       │
                                Phase 3 (migration)
                                ──────────────────
                                Task 8:  Upload DB
                                Task 9:  Upload docs
                                       │
                                Phase 4 (deploy)
                                ──────────────────
                                Task 10: Deploy code
                                Task 11: systemd services
                                Task 12: DB backup cron
```

**Critical path:** Tasks 1 → 5 → 6 → 7 → 8 → 10 (can start workers)

**Parallel work:** Tasks 2-4 alongside Task 5-7. Task 11-12 after workers are running.
