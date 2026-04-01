# Cloud Deployment Guide

Production deployment of the RDF API on Google Cloud Platform.

## Architecture

```
Internet
  |
  v
Cloud Run (rdf-api)          <-- FastAPI app, 1 CPU / 1 GiB, 0-3 instances
  |
  |-- Cloud SQL Auth Proxy    <-- built-in, automatic TLS
  |     |
  |     v
  |   Cloud SQL (rdf-postgres) <-- PostgreSQL 16, db-f1-micro, europe-central2
  |
  |-- NordVPN SOCKS5           <-- batch workers route through VPN to ms.gov.pl
  |
  v
Secret Manager                <-- DATABASE_URL, JWT_SECRET, NordVPN credentials
```

## Current deployment

| Resource | Value |
|----------|-------|
| GCP project | `rdf-api-project` |
| GCP account | `piotr.kraus01@gmail.com` |
| Region | `europe-central2` (Warsaw) |
| Service URL | `https://rdf-api-448201086881.europe-central2.run.app` |
| Cloud SQL instance | `rdf-postgres` (PostgreSQL 16, db-f1-micro) |
| Cloud SQL connection name | `rdf-api-project:europe-central2:rdf-postgres` |
| Cloud SQL public IP | `34.118.73.120` |
| Database name | `rdf` |
| Database user | `postgres` |
| Service account | `448201086881-compute@developer.gserviceaccount.com` |

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
| `CORS_ORIGINS` | `["*"]` | Allowed CORS origins (tighten for prod frontend) |
| `NORDVPN_SERVERS` | `["amsterdam.nl.socks.nordhold.net", ...]` | VPN server pool |

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
