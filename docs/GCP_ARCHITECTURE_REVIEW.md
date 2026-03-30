# Architecture Readiness Review: Cloud Deployment

**Date:** 2026-03-30
**Scope:** Evaluate whether the batch KRS scanner and RDF document workflows are mature enough to run on GCP.

## Current Data Scale

| Metric | Value |
|--------|-------|
| KRS numbers probed | 708,163 |
| Entities found | 315,779 |
| Entities with RDF docs discovered | 1,461 |
| Documents discovered | 35,454 |
| Documents downloaded | 264 |
| DuckDB file size | 253 MB |
| Extracted documents on disk | 168 MB |

The scanning is ~70% complete (708K of ~1M KRS integers). Document discovery has barely started (1,461 of 315K entities). Downloads are negligible (264 of 35K). **The heavy work is ahead** — this is the right time to move to cloud before the dataset grows by 100x+.

## Readiness Assessment

### What's Ready

| Area | Status | Notes |
|------|--------|-------|
| **Env-driven config** | Ready | All settings via `.env` / pydantic-settings. No hardcoded paths. |
| **Async HTTP** | Ready | httpx with SOCKS5, connection health, backoff. Cloud-native pattern. |
| **Graceful shutdown** | Ready | SIGINT/SIGTERM handlers, worker join, exit code reporting. |
| **Idempotent workers** | Ready | `is_done()` checks prevent reprocessing. Safe to restart anytime. |
| **Append-only versioning** | Ready | Entity and document stores use snapshot hashing. No data loss on reruns. |
| **Progress tracking** | Ready | `batch_progress` and `batch_rdf_progress` tables enable resume from any point. |
| **Stride partitioning** | Ready | Workers divide work by modulo. Adding/removing workers is safe. |
| **Lock contention handling** | Ready | DuckDB file lock retry with exponential backoff + jitter (20 retries). |
| **Dockerfile** | Partial | Exists but minimal — no health check, runs as root, single stage. |
| **Storage abstraction** | Partial | `StorageBackend` protocol exists, `LocalStorage` works, GCS is a stub. |

### What Needs Work Before Cloud

| Area | Effort | Blocker? | Notes |
|------|--------|----------|-------|
| **GCS storage backend** | 2-3 hours | **Yes** for RDF downloads | `create_storage()` raises `NotImplementedError` for GCS. Must implement. |
| **Dockerfile hardening** | 1 hour | No (works as-is) | Add non-root user, health check, `.dockerignore`, multi-stage build. |
| **Cloud Logging integration** | 1 hour | No | Workers log to stdout — Cloud Run/GCE captures this automatically. Minor formatting changes for structured JSON logs. |
| **DB migration to cloud** | 2-3 hours | **Yes** | Upload local DuckDB to GCS, download to VM on startup. Script needed. |
| **Secrets management** | 30 min | No | Use GCP Secret Manager for NordVPN creds (if VPN used in cloud). Env vars work initially. |

### Architecture Risks

#### 1. DuckDB File Locking in Multi-Worker Setup
**Risk: Medium** — DuckDB uses exclusive file-level locks. Multiple worker processes on one VM contend for writes.

**Current mitigation:** Short-lived connections with retry backoff. Works locally with 4 workers.

**Cloud impact:** Same pattern works on a single VM. Does NOT work across multiple VMs/containers sharing a network filesystem. Keep all workers on one VM.

#### 2. Document Storage at Scale
**Risk: Low** — 315K entities × ~25 docs average = ~8M documents. At ~50KB average ZIP, that's ~400GB of extracted files.

**Cloud impact:** GCS handles this trivially. Local disk on a single VM would need a 500GB+ persistent disk. GCS is the right answer.

#### 3. Upstream Rate Limiting
**Risk: Medium** — RDF API returns 429/503 under load. Current backoff caps at 60s.

**Cloud impact:** Cloud egress IPs are more likely to be rate-limited than residential IPs. May need to tune delays. VPN rotation via NordVPN SOCKS5 works from cloud VMs (tested pattern).

#### 4. No Monitoring Dashboard
**Risk: Low** — Workers log stats every 100/50 items. Good enough to start; can add Cloud Monitoring later.

**Cloud impact:** Logs go to Cloud Logging automatically. Can create log-based metrics and alerts without code changes.

## Verdict

**The batch workflows are ready for cloud deployment with two prerequisites:**

1. **Implement GCS storage backend** — required for document downloads at scale
2. **Create DB migration script** — to carry the 315K entity dataset to the cloud VM

Everything else (Dockerfile polish, structured logging, monitoring dashboards) can be done incrementally after the workers are running.

## Recommended GCP Architecture

```
┌─────────────────────────────────────────────────────┐
│  GCE VM (e2-standard-2, spot)                       │
│  ┌───────────────┐  ┌───────────────────────────┐   │
│  │ batch/runner   │  │ batch/rdf_runner           │   │
│  │ (KRS scanner)  │  │ (doc discovery+download)   │   │
│  │ 4 workers      │  │ 4 workers                  │   │
│  └───────┬───────┘  └──────────┬────────────────┘   │
│          │                     │                     │
│          v                     v                     │
│  ┌──────────────┐    ┌─────────────────┐            │
│  │ DuckDB file  │    │ GCS bucket      │            │
│  │ (local SSD)  │    │ (documents)     │            │
│  └──────┬───────┘    └─────────────────┘            │
│         │                                            │
│         v (periodic backup)                          │
│  ┌──────────────┐                                    │
│  │ GCS bucket   │                                    │
│  │ (db backups) │                                    │
│  └──────────────┘                                    │
└─────────────────────────────────────────────────────┘

Local dev machine:
  - FastAPI app (uvicorn --reload)
  - Local DuckDB for development
  - Syncs DB snapshots from cloud when needed
```

**Why a single VM, not Cloud Run Jobs or GKE:**
- DuckDB requires a local file — can't share across containers without NFS
- Workers are long-running (hours/days), not short batch jobs
- A single e2-standard-2 spot VM costs ~$15-25/month
- Simplest possible setup — SSH in, check logs, restart if needed
- No orchestration overhead to manage

**When to upgrade:**
- If you need >8 concurrent workers → larger VM or split scanner/RDF to separate VMs
- If DuckDB becomes a bottleneck → migrate to CloudSQL PostgreSQL
- If you want zero-ops → Cloud Run Jobs + CloudSQL + GCS (but significantly more complex)
