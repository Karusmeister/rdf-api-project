# Claude Agent Tasks: GCP Cloud Operations Setup

**Date:** 2026-03-30

This file contains discrete tasks for Claude agents to execute. Each task is self-contained with clear inputs, outputs, and acceptance criteria.

---

## Deployment Mechanism

### How Code Gets to the Cloud

```
Local dev (your Mac)
  │
  │  git push
  v
GitHub repo
  │
  │  gcloud compute ssh + git pull   (manual for now)
  v
GCE VM: /opt/rdf-api-project
  │
  │  systemctl restart krs-scanner / rdf-worker
  v
Workers running with latest code
```

**Current model: manual deploy via SSH + git pull.** This is appropriate for a single VM running batch jobs. No CI/CD pipeline needed yet.

**Future upgrade path:** If you add a second VM or want zero-touch deploys:
1. GitHub Actions builds Docker image → pushes to Artifact Registry
2. VM pulls latest image on restart (or use Cloud Run Jobs)
3. Not worth the complexity for a single worker VM

### How to Deploy a Code Change

```bash
# On your Mac: commit and push
git add -A && git commit -m "fix: adjust backoff for cloud IPs" && git push

# On the VM: pull and restart
gcloud compute ssh rdf-batch-vm --zone=europe-central2-a --command="
  cd /opt/rdf-api-project &&
  git pull &&
  source .venv/bin/activate &&
  pip install -r requirements.txt &&
  sudo systemctl restart krs-scanner rdf-worker
"
```

---

## Monitoring: What to Watch

### 1. Worker Logs (Cloud Logging)

If workers write to stdout/stderr and are managed by systemd, logs go to journald. To also send them to Cloud Logging:

```bash
# Install the Cloud Logging agent (once, on VM setup)
curl -sSO https://dl.google.com/cloudagents/add-logging-agent-repo.sh
sudo bash add-logging-agent-repo.sh
sudo apt-get update && sudo apt-get install -y google-fluentd
sudo systemctl start google-fluentd
```

Or skip the agent and just use `journalctl` + SSH. For a single VM, this is fine.

### 2. Key Metrics to Track

| Metric | How to Check | Alert Threshold |
|--------|-------------|-----------------|
| Workers alive | `systemctl status krs-scanner rdf-worker` | Any service not `active (running)` |
| KRS scan rate | Grep log for `entities_found` rate | < 1/s sustained (possible rate limiting) |
| RDF doc download rate | Grep log for `documents_downloaded` | < 10/hour (stalled) |
| DuckDB size | `ls -lh /data/scraper.duckdb` | > 2GB (check if growing as expected) |
| GCS document count | `gsutil du -s gs://rdf-project-documents/` | Growing daily |
| Disk usage | `df -h /data` | > 80% (DuckDB might outgrow disk) |
| VM preemption | GCE console or `gcloud compute operations list` | Any STOP event |

### 3. Quick Health Check Script

Create this on the VM at `/opt/rdf-api-project/scripts/cloud_status.sh`:

```bash
#!/bin/bash
echo "=== Services ==="
systemctl is-active krs-scanner rdf-worker

echo -e "\n=== Disk ==="
df -h /data

echo -e "\n=== DuckDB Size ==="
ls -lh /data/scraper.duckdb

echo -e "\n=== GCS Documents ==="
gsutil du -s gs://rdf-project-documents/ 2>/dev/null || echo "bucket not accessible"

echo -e "\n=== DB Progress ==="
cd /opt/rdf-api-project && source .venv/bin/activate 2>/dev/null
python3 -c "
import duckdb
c = duckdb.connect('/data/scraper.duckdb', read_only=True)
print('batch_progress:')
for r in c.execute('SELECT status, COUNT(*) FROM batch_progress GROUP BY status ORDER BY status').fetchall():
    print(f'  {r[0]}: {r[1]:,}')
print('rdf_progress:')
for r in c.execute('SELECT status, COUNT(*), COALESCE(SUM(documents_found),0) FROM batch_rdf_progress GROUP BY status ORDER BY status').fetchall():
    print(f'  {r[0]}: {r[1]:,} krs, {int(r[2]):,} docs')
try:
    r = c.execute('SELECT COUNT(*) FROM krs_document_versions WHERE is_downloaded=true').fetchone()
    print(f'downloaded documents: {r[0]:,}')
except: pass
c.close()
"

echo -e "\n=== Recent Logs (scanner) ==="
journalctl -u krs-scanner --no-pager -n 5 2>/dev/null || tail -5 /var/log/krs-scanner.log 2>/dev/null

echo -e "\n=== Recent Logs (rdf-worker) ==="
journalctl -u rdf-worker --no-pager -n 5 2>/dev/null || tail -5 /var/log/rdf-worker.log 2>/dev/null
```

Run from your Mac:
```bash
gcloud compute ssh rdf-batch-vm --zone=europe-central2-a --command="bash /opt/rdf-api-project/scripts/cloud_status.sh"
```

---

## Cost Breakdown

### Monthly Estimate (Steady State)

| Resource | Spec | Unit Cost | Monthly |
|----------|------|-----------|---------|
| GCE VM (spot) | e2-standard-2 (2 vCPU, 8GB) | ~$0.02/hr | **$15-20** |
| Boot disk | 50GB pd-ssd | $0.17/GB/mo | **$8.50** |
| GCS Standard | ~500GB documents (growing) | $0.02/GB/mo | **$10** |
| GCS operations | ~1M Class A (writes) | $0.05/10K | **$5** |
| GCS operations | ~500K Class B (reads) | $0.004/10K | **$0.20** |
| Network egress | Minimal (within GCP) | - | **$0-1** |
| **Total** | | | **~$35-45/mo** |

### Cost Controls

- **Spot VM:** 60-70% cheaper than on-demand. Preemption risk mitigated by checkpoint-based resume.
- **Budget alert:** Set a $50/month budget alert in GCP Console → Billing → Budgets.
- **Auto-shutdown:** If workers finish, the VM idles. Stop it manually or via cron:
  ```bash
  # Cron: stop VM if no worker has been active for 1 hour
  # (add to /etc/cron.d/rdf-autostop)
  */30 * * * * root pgrep -f "batch.runner\|batch.rdf_runner" > /dev/null || gcloud compute instances stop $(hostname) --zone=europe-central2-a
  ```
- **GCS lifecycle:** Auto-delete old DB backups after 30 days (configured in Task 6).

### Scaling Cost Scenarios

| Scenario | Change | Additional Cost |
|----------|--------|----------------|
| Faster scanning (8 workers) | Upgrade to e2-standard-4 | +$15/mo |
| 1TB documents in GCS | More storage | +$10/mo |
| Keep VM running 24/7 (on-demand) | No spot discount | +$30/mo |
| Add Cloud SQL (future) | db-f1-micro PostgreSQL | +$10/mo |

---

## Agent Tasks

### TASK A-1: Implement GCS Storage Backend
**Type:** Code change
**Prerequisite:** None
**Files:** `app/scraper/storage.py`, `requirements.txt`

**Instructions:**
1. Read `app/scraper/storage.py` to understand the `StorageBackend` protocol and `LocalStorage` implementation.
2. Add `google-cloud-storage>=2.18` to `requirements.txt`.
3. Implement `GcsStorage` class in `app/scraper/storage.py`:
   - Constructor takes `bucket_name: str` and `prefix: str`. Creates `storage.Client()` and gets bucket reference.
   - `save_extracted()`: extract ZIP in memory (same logic as `LocalStorage`), upload each file as a blob to `{prefix}{doc_dir}/{filename}`, upload `manifest.json`, return manifest dict.
   - `exists()`: check if blob exists at `{prefix}{path}`.
   - `read()`: download blob bytes.
   - `list_files()`: list blobs with prefix `{prefix}{dir_path}/`, return filenames.
   - `get_full_path()`: return `gs://{bucket_name}/{prefix}{path}`.
4. Update `create_storage()` to return `GcsStorage` when `settings.storage_backend == "gcs"`.
5. Write tests in `tests/services/test_gcs_storage.py` using `unittest.mock` to mock the GCS client.
6. Run `pytest tests/ -v` to ensure nothing is broken.

**Acceptance criteria:**
- `create_storage()` returns `GcsStorage` when `STORAGE_BACKEND=gcs`
- `GcsStorage.save_extracted()` uploads files and manifest to GCS
- All existing tests still pass

---

### TASK A-2: Create `.dockerignore` File
**Type:** Code change
**Prerequisite:** None
**Files:** `.dockerignore` (new)

**Instructions:**
Create `.dockerignore` with:
```
data/
tests/
.venv/
__pycache__/
*.pyc
.git/
.env
.env.example
docs/
*.md
.claude/
```

---

### TASK A-3: Create Batch Worker Dockerfile
**Type:** Code change
**Prerequisite:** A-2
**Files:** `Dockerfile.batch` (new)

**Instructions:**
1. Read the existing `Dockerfile` for reference.
2. Create `Dockerfile.batch`:
   - Base: `python:3.12-slim`
   - Install requirements
   - Copy `app/`, `batch/`, `scripts/`
   - Create non-root user `worker`
   - Default CMD: `python -m batch.runner`
3. Ensure it builds: `docker build -f Dockerfile.batch -t rdf-batch .`

---

### TASK A-4: Create Cloud Status Script
**Type:** Code change
**Prerequisite:** None
**Files:** `scripts/cloud_status.sh` (new)

**Instructions:**
Create the health check script from the monitoring section above. Make it executable.

---

### TASK A-5: Create DB Migration Script
**Type:** Code change
**Prerequisite:** None
**Files:** `scripts/migrate_db_to_cloud.sh` (new)

**Instructions:**
Create a script that:
1. Takes arguments: `--direction` (up/down), `--bucket` (GCS bucket name)
2. `up` (local→cloud): uploads `data/scraper.duckdb` to `gs://{bucket}/seed/scraper.duckdb`
3. `down` (cloud→local): downloads latest backup from `gs://{bucket}/backups/scraper-latest.duckdb` to `data/scraper-cloud.duckdb`
4. Prints row counts after transfer for verification
5. Does NOT overwrite the local dev `data/scraper.duckdb` on download — saves as `data/scraper-cloud.duckdb`

---

### TASK A-6: Configure Per-Repo gcloud Configuration
**Type:** Local setup
**Prerequisite:** None
**Files:** `.envrc` (new) — already gitignored

**Context:** The developer works on multiple GCP projects from the same machine. Each repo must automatically target the correct GCP project. Two isolation layers are in place:

**Layer 1 — Claude Code agents:** `.claude/settings.json` only permits gcloud/gsutil commands prefixed with `CLOUDSDK_ACTIVE_CONFIG=rdf-project`. This means agents physically cannot talk to the wrong project. All gcloud/gsutil commands in task documents MUST use this prefix:
```bash
CLOUDSDK_ACTIVE_CONFIG=rdf-project gcloud compute instances list
CLOUDSDK_ACTIVE_CONFIG=rdf-project gsutil ls gs://rdf-project-documents/
```

**Layer 2 — Interactive shell (your terminal):** `direnv` auto-sets the env var when you `cd` into the repo.

**Instructions:**
1. Create a named gcloud configuration for this project:
   ```bash
   gcloud config configurations create rdf-project
   CLOUDSDK_ACTIVE_CONFIG=rdf-project gcloud config set project <GCP_PROJECT_ID>
   CLOUDSDK_ACTIVE_CONFIG=rdf-project gcloud config set compute/region europe-central2
   CLOUDSDK_ACTIVE_CONFIG=rdf-project gcloud config set compute/zone europe-central2-a
   CLOUDSDK_ACTIVE_CONFIG=rdf-project gcloud config set account <GCP_EMAIL>
   ```
2. Create `.envrc` in the project root:
   ```bash
   export CLOUDSDK_ACTIVE_CONFIG=rdf-project
   ```
3. Ensure `direnv` is installed (`brew install direnv`) and hooked into the shell (`eval "$(direnv hook zsh)"` in `.zshrc`).
4. Run `direnv allow` in the project root.
5. Verify: `gcloud config configurations list` shows `rdf-project` as active inside the repo, and your other project's config active when you cd elsewhere.

**Note:** The actual GCP project ID and email are machine-specific. This task creates the wiring; the developer fills in values during initial setup.

---

### TASK A-7: Create GCP Setup Script
**Type:** Infrastructure script
**Prerequisite:** A-6
**Files:** `scripts/gcp_setup.sh` (new)

**Instructions:**
Create a script that automates Phase 2 from `GCP_PRE_DEPLOYMENT_TASKS.md`:
1. Validates that `CLOUDSDK_ACTIVE_CONFIG` is set (i.e., running inside the repo with direnv)
2. Creates GCS buckets (`rdf-project-documents`, `rdf-project-data`)
3. Sets lifecycle policy on backup bucket
4. Creates GCE VM with the spec from Task 7
5. Prints next steps (upload DB, deploy code)

The script should:
- Accept `--project` and `--zone` arguments (defaults: inferred from gcloud config)
- Be idempotent (skip resources that already exist)
- Print costs estimate at the end

---

### TASK A-8: Create VM Bootstrap Script
**Type:** Infrastructure script
**Prerequisite:** A-7
**Files:** `scripts/vm_bootstrap.sh` (new)

**Instructions:**
Create a script to run ON the VM after SSH-ing in. It should:
1. Install system deps (python3, pip, venv, git)
2. Clone the repo to `/opt/rdf-api-project`
3. Create venv and install requirements
4. Create `/data` directory
5. Download seed DB from GCS
6. Prompt for `.env` configuration (or accept `--env-file` argument)
7. Install systemd service files for `krs-scanner` and `rdf-worker`
8. Set up the DB backup cron job
9. Print status and next steps

---

### TASK A-9: Create systemd Service Files
**Type:** Code change
**Prerequisite:** None
**Files:** `deploy/krs-scanner.service` (new), `deploy/rdf-worker.service` (new), `deploy/rdf-backup.cron` (new)

**Instructions:**
Create a `deploy/` directory with:
1. `krs-scanner.service` — systemd unit for `batch.runner`
2. `rdf-worker.service` — systemd unit for `batch.rdf_runner`
3. `rdf-backup.cron` — cron file for periodic DuckDB backup to GCS

Use the templates from `GCP_PRE_DEPLOYMENT_TASKS.md` Task 11-12.

---

### TASK A-10: Add Budget Alert Documentation
**Type:** Documentation
**Prerequisite:** None
**Files:** Update `docs/GCP_VS_LOCAL_SETUP.md`

**Instructions:**
Add a section on setting up GCP billing alerts:
```bash
# CLI command to create budget alert
gcloud billing budgets create \
  --billing-account=BILLING_ACCOUNT_ID \
  --display-name="RDF Project Monthly" \
  --budget-amount=50 \
  --threshold-rule=percent=0.5 \
  --threshold-rule=percent=0.8 \
  --threshold-rule=percent=1.0
```

---

## Task Execution Order

**Phase 0 — Local setup (do first):**
- A-6: Per-repo gcloud configuration (direnv + CLOUDSDK_ACTIVE_CONFIG)

**Phase 1 — Code changes (can run in parallel):**
- A-1: GCS storage backend (blocker)
- A-2: .dockerignore
- A-3: Batch Dockerfile
- A-4: Cloud status script
- A-5: DB migration script
- A-9: systemd service files

**Phase 2 — Infrastructure setup (sequential, requires A-6):**
- A-7: GCP setup script → run it
- A-8: VM bootstrap script → run it on VM

**Phase 3 — Monitoring and polish:**
- A-10: Budget alert docs

**Estimated total effort:** 4-6 hours for a Claude agent to implement all code tasks (A-1 through A-9). Manual GCP setup (running scripts, creating project) is ~30 minutes of your time.
