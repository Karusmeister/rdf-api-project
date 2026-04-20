# KRS Sync Pipeline — Operations Runbook

## Overview

The KRS sync job runs on a cron schedule (default: 3 AM daily), discovers new KRS numbers from `krs_companies` (rows the scraper has seen but not yet enriched), enriches them via the MS KRS Open API, and upserts the results back into `krs_companies`.

## Configuration

| Env var | Default | Description |
|---------|---------|-------------|
| `KRS_SYNC_CRON` | `0 3 * * *` | Cron expression for the sync schedule |
| `KRS_SYNC_BATCH_SIZE` | `100` | Max entities processed per run |
| `KRS_SYNC_STALE_HOURS` | `168` | Re-sync entities older than this (7 days) |

## Checking sync status

```bash
# Last sync run summary
curl http://localhost:8000/jobs/krs-sync/status

# Response:
# {
#   "id": 42,
#   "started_at": "2026-03-25T03:00:01",
#   "finished_at": "2026-03-25T03:02:15",
#   "krs_count": 50,
#   "new_count": 12,
#   "updated_count": 38,
#   "error_count": 0,
#   "source": "ms_gov",
#   "status": "completed"
# }
```

## Triggering a manual run

```bash
curl -X POST http://localhost:8000/jobs/krs-sync/trigger

# Returns 202 Accepted with {"status": "scheduled"} when the run is queued.
# Returns 409 if a run is already in progress.
```

## Reading the metrics endpoint

```bash
curl http://localhost:8000/metrics/krs

# Shows p50/p95 latency, error rate, and call count
# for all KRS API interactions (including sync job calls).
```

## Failure modes and remediation

### 1. Government API is down (UpstreamUnavailableError)

**Symptoms:**
- `/jobs/krs-sync/status` shows `error_count > 0` with `status: completed`
- `/health/krs` returns 503
- Logs contain `krs_sync_entity_error` entries with `UpstreamUnavailableError`

**What happens:** The sync job continues processing remaining entities, recording each failure. It does not crash.

**Remediation:**
1. Check `/health/krs` — if 503, the API is down.
2. Check https://api-krs.ms.gov.pl status externally.
3. Wait for recovery. The next scheduled run will retry failed entities.
4. Once the API is back, trigger a manual run: `POST /jobs/krs-sync/trigger`

### 2. Rate limited (HTTP 429)

**Symptoms:**
- `error_count > 0` in sync status
- Logs show `RateLimitedError`
- `/metrics/krs` shows elevated error rate

**What happens:** The KRS client has built-in retry with exponential backoff (up to `KRS_MAX_RETRIES` attempts). If all retries are exhausted, the entity is counted as an error and the job moves on.

**Remediation:**
1. Reduce `KRS_SYNC_BATCH_SIZE` to lower API pressure.
2. Increase `KRS_REQUEST_DELAY_MS` (default: 1500ms) for more polite pacing.
3. If persistent, spread the load by scheduling multiple smaller runs:
   ```
   KRS_SYNC_CRON="0 2,8,14,20 * * *"
   KRS_SYNC_BATCH_SIZE=25
   ```

### 3. Database full or DuckDB errors

**Symptoms:**
- Sync status shows `status: failed`
- Logs contain `krs_sync_failed` with DuckDB exceptions

**Remediation:**
1. Check disk space: `df -h` on the data volume.
2. Check DuckDB file size: `ls -lh data/scraper.duckdb`
3. If disk is full, free space and trigger a manual run.
4. If the DB file is corrupt, restore from backup and restart the app.

## Job behavior notes

- **Idempotent:** Running the job twice with no upstream changes produces the same result.
- **Concurrent-safe:** Only one run can execute at a time. A second trigger returns HTTP 409.
- **Half-budget split:** Discovery (new KRS numbers) gets half the batch budget; re-enrichment of stale entities gets the other half.
- **No search:** The KRS Open API has no search endpoint. Discovery relies on KRS numbers already present in `krs_companies` (populated by the scanner + RDF document-discovery worker).
