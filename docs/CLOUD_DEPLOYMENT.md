# Cloud Deployment Guide

Production deployment of the RDF API on Google Cloud Platform.

## Architecture

```text
                          ┌─────────────────────────────────────────────────────────┐
                          │                    GCP (europe-central2)                 │
                          │                                                         │
  Internet ──────────────>│  Cloud Run (rdf-api)                                    │
                          │    FastAPI app, 1 CPU / 1 GiB, 0-3 instances            │
                          │    Serves: proxy, analysis, ETL, auth, predictions API   │
                          │      │                                                  │
                          │      │ Cloud SQL Auth Proxy (built-in TLS)              │
                          │      v                                                  │
                          │  Cloud SQL (rdf-postgres)                               │
                          │    PostgreSQL 16, db-f1-micro                           │
                          │    Public IP: 34.118.73.120                             │
                          │      ^         ^                                        │
                          │      │         │                                        │
                          │  GCE VM (rdf-batch-vm)                                  │
                          │    e2-standard-2 (preemptible), europe-central2-a       │
                          │    ┌────────────────────────────────────────┐            │
                          │    │  KRS Scanner (batch.runner)            │            │
                          │    │    7 workers via NordVPN SOCKS5        │──> KRS API │
                          │    │    Probes KRS integers 1..N            │   (ms.gov) │
                          │    ├────────────────────────────────────────┤            │
                          │    │  RDF Worker (batch.rdf_runner)         │            │
                          │    │    5 workers, concurrency=5            │──> RDF API │
                          │    │    Discovery + download + GCS upload   │   (ms.gov) │
                          │    └────────────────────────────────────────┘            │
                          │      │                                                  │
                          │      v                                                  │
                          │  GCS Bucket (rdf-project-documents)                     │
                          │    Extracted ZIPs stored as files + manifest.json        │
                          │                                                         │
                          │  Secret Manager                                         │
                          │    database-url, jwt-secret, nordvpn-*                  │
                          └─────────────────────────────────────────────────────────┘
```

## Current deployment

| Resource | Value |
|----------|-------|
| GCP project | `rdf-api-project` |
| GCP account | `piotr.kraus01@gmail.com` |
| Region | `europe-central2` (Warsaw) |
| **Cloud Run** | |
| Service URL | `https://rdf-api-448201086881.europe-central2.run.app` |
| Service account | `448201086881-compute@developer.gserviceaccount.com` |
| **Cloud SQL** | |
| Instance | `rdf-postgres` (PostgreSQL 16, db-f1-micro) |
| Connection name | `rdf-api-project:europe-central2:rdf-postgres` |
| Public IP | `34.118.73.120` |
| Database / user | `rdf` / `postgres` |
| **Batch VM** | |
| Instance | `rdf-batch-vm` (e2-standard-2, preemptible) |
| Zone | `europe-central2-a` |
| External IP | `34.116.141.233` |
| Code path | `/opt/rdf-api-project/` (git clone of main) |
| Venv | `/opt/rdf-api-project/.venv/` |
| Config | `/opt/rdf-api-project/.env` |
| **GCS** | |
| Documents bucket | `rdf-project-documents` |

## Secrets (Secret Manager)

All sensitive values are stored in Secret Manager, not as plain env vars:

| Secret name | Contents |
|-------------|----------|
| `database-url` | Full PostgreSQL connection string via Cloud SQL Unix socket |
| `jwt-secret` | HMAC-SHA256 signing key for JWT tokens |
| `nordvpn-username` | NordVPN SOCKS5 proxy username |
| `nordvpn-password` | NordVPN SOCKS5 proxy password |

Cloud Run mounts these as environment variables at runtime. To rotate a secret:

```bash
# Create a new version
echo -n 'new-value' | gcloud secrets versions add jwt-secret --data-file=-

# Redeploy to pick up the new version (secrets use :latest)
gcloud run deploy rdf-api --source . --region europe-central2
```

## Environment variables

These are set as plain env vars (non-sensitive):

| Variable | Value | Purpose |
|----------|-------|---------|
| `WORKERS` | `2` | Uvicorn worker count |
| `LOG_LEVEL` | `INFO` | Python logging level |
| `ENVIRONMENT` | `production` | Enables JWT secret validation |
| `BATCH_USE_VPN` | `true` | Route batch workers through NordVPN |
| `BATCH_USE_PUBLIC_PROXIES` | `true` | Include prioritized `proxies.json` pool after NordVPN proxies |
| `BATCH_REQUIRE_VPN_ONLY` | `true` | Strict mode: never fall back to direct egress |
| `CORS_ORIGINS` | `["*"]` | Allowed CORS origins (tighten for prod frontend) |
| `NORDVPN_SERVERS` | `["amsterdam.nl.socks.nordhold.net", ...]` | VPN server pool |

### Batch proxy env profiles

Use one of these explicit profiles depending on your risk/availability policy.

#### Strict profile (recommended for cloud)

```bash
BATCH_USE_VPN=true
BATCH_USE_PUBLIC_PROXIES=true
BATCH_REQUIRE_VPN_ONLY=true
```

Behavior:
- All batch workers must use proxy egress.
- No direct fallback is allowed.
- Job fails fast if no proxy survives preflight/dead-proxy filtering.

#### Non-strict profile (local/dev fallback)

```bash
BATCH_USE_VPN=true
BATCH_USE_PUBLIC_PROXIES=false
BATCH_REQUIRE_VPN_ONLY=false
```

Behavior:
- VPN is preferred.
- Direct egress can be used as last-resort fallback.
- Better resilience, weaker egress policy guarantees.

## How to deploy a new version

```bash
# From the project root:
gcloud run deploy rdf-api --source . --region europe-central2
```

Cloud Build reads the `Dockerfile`, builds the image, pushes to Artifact Registry, and updates the Cloud Run revision. Takes 2-4 minutes.

The `Dockerfile` copies `app/`, `batch/`, and `scripts/` into the image. The `.gcloudignore` excludes `tests/`, `docs/`, `data/`, `.env`, `.git/`, and other non-runtime files from the upload.

## How to update environment variables

```bash
# Single variable
gcloud run services update rdf-api --region europe-central2 \
  --set-env-vars KEY=VALUE

# For values with special characters (JSON arrays), use an env vars file:
cat > /tmp/env-vars.yaml << 'EOF'
CORS_ORIGINS: '["https://my-app.lovable.app"]'
EOF
gcloud run services update rdf-api --region europe-central2 \
  --env-vars-file /tmp/env-vars.yaml
```

## Database

### Connection path

Cloud Run connects to Cloud SQL via the built-in Cloud SQL Auth Proxy:
- No public IP needed from Cloud Run's perspective
- Connection goes through a Unix socket at `/cloudsql/rdf-api-project:europe-central2:rdf-postgres`
- The `DATABASE_URL` secret uses this format: `postgresql://postgres:<password>@/rdf?host=/cloudsql/rdf-api-project:europe-central2:rdf-postgres`

### Schema initialization

The app auto-creates all tables on startup via lifespan hooks. All DDL uses `CREATE TABLE IF NOT EXISTS` and `CREATE INDEX IF NOT EXISTS` -- no destructive operations. Deploying a new version will never drop data.

### Connecting locally to Cloud SQL

```bash
# Option 1: Cloud SQL Auth Proxy (recommended)
cloud-sql-proxy rdf-api-project:europe-central2:rdf-postgres &
psql "postgresql://postgres:<password>@localhost:5432/rdf"

# Option 2: Direct via public IP (requires authorized network)
psql "postgresql://postgres:<password>@34.118.73.120/rdf"
```

### Resetting the database password

```bash
gcloud sql users set-password postgres --instance=rdf-postgres --password='<new-password>'

# Then update the secret:
echo -n 'postgresql://postgres:<new-password>@/rdf?host=/cloudsql/rdf-api-project:europe-central2:rdf-postgres' \
  | gcloud secrets versions add database-url --data-file=-

# Redeploy to pick up the new secret version:
gcloud run deploy rdf-api --source . --region europe-central2
```

## IAM permissions

The default Compute Engine service account has these roles:

| Role | Purpose |
|------|---------|
| `roles/editor` | Default GCP project role |
| `roles/cloudbuild.builds.builder` | Build container images |
| `roles/storage.admin` | Access Cloud Build source bucket |
| `roles/artifactregistry.writer` | Push built images |
| `roles/logging.logWriter` | Write application logs |
| `roles/secretmanager.secretAccessor` | Read secrets at runtime |

The Cloud SQL connection is authorized via the `--add-cloudsql-instances` flag on the Cloud Run service, which uses the service account's implicit Cloud SQL Client role.

## Monitoring and debugging

```bash
# View recent logs
gcloud run services logs read rdf-api --region europe-central2 --limit 50

# Stream logs in real-time
gcloud run services logs tail rdf-api --region europe-central2

# Check service status
gcloud run services describe rdf-api --region europe-central2

# Health check
curl https://rdf-api-448201086881.europe-central2.run.app/health

# KRS upstream health
curl https://rdf-api-448201086881.europe-central2.run.app/health/krs

# Scraper/data status
curl https://rdf-api-448201086881.europe-central2.run.app/api/scraper/status
```

## Cost estimate

| Resource | Cost |
|----------|------|
| Cloud Run | Free tier: 2M requests/mo. Beyond: ~$0.40/M requests + CPU/memory while handling requests |
| Cloud SQL (db-f1-micro) | ~$7-10/mo |
| Cloud Build | 120 free build-minutes/day |
| Secret Manager | 6 secret versions, well within free tier |
| Artifact Registry | Minimal storage cost for Docker images |
| **Total** | **~$7-15/mo for low traffic** |

## Batch VM — Deployment and Operations

The batch VM runs two long-lived services that scrape the Polish KRS/RDF registries.
Code lives at `/opt/rdf-api-project/` and is deployed via git pull from GitHub.

### Systemd services

| Service | Unit file | What it runs |
|---------|-----------|--------------|
| `krs-scanner` | `deploy/krs-scanner.service` | `python -m batch.runner` — probes KRS integers to find valid entities |
| `rdf-worker` | `deploy/rdf-worker.service` | `python -m batch.rdf_runner` — discovers + downloads documents for found entities |

Both services run as the `worker` user, read config from `/opt/rdf-api-project/.env`,
and are set to `Restart=on-failure`.

### Deploying new code to the batch VM

```bash
# 1. SSH into the VM
gcloud compute ssh rdf-batch-vm --zone=europe-central2-a --project=rdf-api-project

# 2. Pull latest code
cd /opt/rdf-api-project
sudo -u worker git pull origin main

# 3. Install any new dependencies
sudo -u worker .venv/bin/pip install -r requirements.txt

# 4. Restart affected services
sudo systemctl restart rdf-worker       # RDF document discovery + download
sudo systemctl restart krs-scanner      # KRS integer scanner

# 5. Verify
sudo systemctl status rdf-worker --no-pager
sudo systemctl status krs-scanner --no-pager
```

For code changes that only affect `batch/rdf_worker.py` or `batch/rdf_runner.py`,
you only need to restart `rdf-worker`. The KRS scanner can keep running.

### First-time VM setup

If starting from a fresh VM:

```bash
# Install system dependencies
sudo apt-get update && sudo apt-get install -y python3.12 python3.12-venv git postgresql-client

# Create worker user
sudo useradd --create-home --shell /bin/bash worker

# Clone the repo
sudo mkdir -p /opt/rdf-api-project
sudo chown worker:worker /opt/rdf-api-project
sudo -u worker git clone https://github.com/Karusmeister/rdf-api-project.git /opt/rdf-api-project

# Set up Python venv
cd /opt/rdf-api-project
sudo -u worker python3.12 -m venv .venv
sudo -u worker .venv/bin/pip install -r requirements.txt

# Create .env with production values
sudo -u worker cp .env.example .env
# Edit .env: set DATABASE_URL, STORAGE_BACKEND=gcs, NORDVPN_*, BATCH_* etc.

# Install systemd services
sudo cp deploy/krs-scanner.service /etc/systemd/system/
sudo cp deploy/rdf-worker.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now krs-scanner rdf-worker
```

### Viewing logs

```bash
# Real-time logs
sudo journalctl -u rdf-worker -f
sudo journalctl -u krs-scanner -f

# Last 100 lines
sudo journalctl -u rdf-worker -n 100 --no-pager

# Check process tree
ps aux | grep 'batch\.' | grep -v grep
```

### Batch VM .env configuration

| Variable | Value | Purpose |
|----------|-------|---------|
| `DATABASE_URL` | `postgresql://postgres:<pw>@34.118.73.120:5432/rdf` | Cloud SQL via public IP |
| `STORAGE_BACKEND` | `gcs` | Store documents in GCS |
| `STORAGE_GCS_BUCKET` | `rdf-project-documents` | GCS bucket for extracted files |
| `BATCH_USE_VPN` | `true` | Route through NordVPN SOCKS5 |
| `BATCH_WORKERS` | `5` | KRS scanner worker count |
| `BATCH_CONCURRENCY_PER_WORKER` | `3` | Async concurrency for KRS scanner |
| `BATCH_DELAY_SECONDS` | `1.5` | Delay between KRS probes |
| `RDF_BATCH_CONCURRENCY` | `5` | Async concurrency per RDF worker |
| `RDF_BATCH_DELAY_SECONDS` | `1.5` | Delay for discovery (encrypted search) |
| `RDF_BATCH_DOWNLOAD_DELAY` | `0.3` | Delay for metadata/ZIP downloads |
| `RDF_BATCH_PAGE_SIZE` | `100` | Documents per search page |
| `NORDVPN_USERNAME` | _(secret)_ | NordVPN SOCKS5 credentials |
| `NORDVPN_PASSWORD` | _(secret)_ | NordVPN SOCKS5 credentials |
| `NORDVPN_SERVERS` | JSON array of hostnames | VPN server pool (Polish servers for low latency) |

## Scaling

Current config: `--min-instances 0 --max-instances 3`. This means:
- Scales to zero when idle (saves cost)
- Cold start on first request after idle (~5-10s)
- Max 3 concurrent instances for traffic spikes

To reduce cold starts, set `--min-instances 1` (~$20-30/mo additional). To handle more traffic, increase `--max-instances`.

## Tightening for production

1. **CORS**: Replace `["*"]` with your actual frontend domain:
   ```bash
   gcloud run services update rdf-api --region europe-central2 \
     --set-env-vars 'CORS_ORIGINS=["https://your-app.lovable.app"]'
   ```

2. **Authentication**: The API is publicly accessible (`--allow-unauthenticated`). Auth-protected endpoints use JWT tokens. Consider adding Cloud IAP or API Gateway for additional protection.

3. **Cloud SQL tier**: Upgrade from `db-f1-micro` to `db-g1-small` if you need more headroom.
