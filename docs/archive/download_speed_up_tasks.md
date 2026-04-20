# Download Speed-Up Tasks

## Problem

The RDF document download pipeline currently runs at ~3,000 docs/hr. The main
bottleneck is that each document requires **3 sequential HTTP calls**:

1. **Metadata fetch** (`GET /dokumenty/{id}`) + 0.3s delay
2. **ZIP download** (`POST /dokumenty/tresc`) + 0.3s delay
3. **GCS upload** (synchronous, blocks the async event loop)

Steps 1 and 3 are unnecessary for the core download path and can be decoupled.

## Design Principle

**Do not modify existing workers or runners.** All changes are additive:
- New `--skip-metadata` flag on the existing runner (default: off, preserves current behavior)
- New standalone metadata backfill job (`batch/metadata_backfill.py`)
- New runner for the backfill job (`batch/metadata_runner.py`)
- Async GCS uploads via thread executor (internal optimization, no behavior change)

After these changes, the recommended production workflow becomes:

```text
1. Run rdf_runner with --skip-metadata     (fast: ZIP download + GCS only)
2. Run metadata_runner in parallel          (backfills metadata for downloaded docs)
```

Both can run simultaneously without interfering with each other.

---

## Task 1: Add `--skip-metadata` flag to RDF download worker

### What
Add a boolean `skip_metadata` parameter that flows from CLI → runner → worker.
When enabled, `_download_one_document` skips the metadata fetch and its delay,
going straight to the ZIP download. This cuts per-document HTTP calls from 2 to 1
and removes one 0.3s delay per document.

### Why
The metadata fetch (`GET /dokumenty/{id}`) returns 5 fields: `nazwaPliku`,
`czyMSR`, `czyKorekta`, `dataDodania`, `dataSporządzenia`. These are useful
for filtering (IFRS vs GAAP, corrections) but **not required for downloading
the actual file**. Deferring metadata to a separate process lets the download
pipeline focus purely on throughput.

### How

#### File: `app/config.py`
Add one new setting:
```python
rdf_batch_skip_metadata: bool = False  # skip metadata fetch during download (backfill later)
```

#### File: `batch/rdf_worker.py`

1. Add `skip_metadata: bool` parameter to `_download_one_document`:
```python
async def _download_one_document(
    client, krs, doc_id, doc_store, storage,
    delay, worker_id, stats,
    skip_metadata: bool = False,        # <-- NEW
) -> bool:
```

2. Wrap the existing metadata block (lines 326-337) in a conditional:
```python
    if not skip_metadata:
        # 1. Fetch metadata (existing code, unchanged)
        meta = await _fetch_metadata_with_backoff(client, doc_id, worker_id)
        await asyncio.sleep(delay)
        if meta is not None:
            try:
                doc_store.update_metadata(doc_id, meta)
            except Exception as exc:
                logger.warning(...)

    # 2. Download ZIP (existing code, unchanged)
    zip_bytes = await _download_zip_with_backoff(client, doc_id, worker_id)
    ...
```

3. Add `skip_metadata: bool` parameter to `_worker_loop` and `run_rdf_worker`.
   Pass it through to `_download_one_document` in the download phase (line ~495).

#### File: `batch/rdf_runner.py`

1. Add `skip_metadata` parameter to `run_rdf_batch()`:
```python
def run_rdf_batch(
    *, workers=None, use_vpn=None, concurrency=None,
    delay=None, download_delay=None, page_size=None,
    dsn=None,
    skip_metadata: bool | None = None,   # <-- NEW
) -> None:
```

2. Resolve from settings:
```python
_skip_meta = skip_metadata if skip_metadata is not None else settings.rdf_batch_skip_metadata
```

3. Pass to each worker process kwargs:
```python
kwargs=dict(
    ...,
    skip_metadata=_skip_meta,
)
```

4. Add CLI flag in `_build_parser()`:
```python
parser.add_argument(
    "--skip-metadata", action="store_true", default=None,
    help="Skip per-document metadata fetch (backfill later with metadata_runner)",
)
```

5. Pass in `main()`:
```python
run_rdf_batch(
    ...,
    skip_metadata=args.skip_metadata,
)
```

### Expected impact
- Per-document time drops from ~0.9s (metadata + delay + zip + delay) to ~0.5s (zip + delay)
- **~1.8x throughput increase** on the download phase

### Testing
- Existing tests pass unchanged (default `skip_metadata=False`)
- Add one new test in `tests/batch/test_rdf_worker.py`:

```python
def test_download_one_document_skip_metadata(self):
    """When skip_metadata=True, no metadata HTTP call is made."""
    # Mock only the ZIP endpoint (no metadata mock)
    # Call _download_one_document with skip_metadata=True
    # Assert: ZIP downloaded, metadata_fetched_at is NULL, document marked downloaded
```

---

## Task 2: New standalone metadata backfill job

### What
A new module `batch/metadata_backfill.py` that finds documents where
`is_downloaded = true AND metadata_fetched_at IS NULL` and backfills the
5 metadata fields by calling `GET /dokumenty/{id}` for each.

### Why
After Task 1, documents downloaded with `--skip-metadata` will have their
files in GCS but missing `filename`, `is_ifrs`, `is_correction`, `date_filed`,
and `date_prepared`. This job fills that gap independently, without blocking
downloads.

### How

#### New file: `batch/metadata_backfill.py`

```python
"""Standalone metadata backfill for downloaded documents missing metadata.

Finds documents where is_downloaded=true AND metadata_fetched_at IS NULL,
fetches metadata from the RDF API, and updates the document store.

Can run concurrently with the download workers without interference —
it only touches documents that are already fully downloaded.
"""

import asyncio
import logging
import time
from dataclasses import dataclass, field

import httpx

from app.config import settings
from app.db.connection import make_connection
from batch.connections import Connection
from batch.rdf_document_store import RdfDocumentStore
from batch.rdf_worker import (
    _fetch_metadata_with_backoff,
    _make_client,
    _RDF_BASE,
)

logger = logging.getLogger(__name__)


@dataclass
class BackfillStats:
    total: int = 0
    success: int = 0
    failed: int = 0
    skipped: int = 0
    start_time: float = field(default_factory=time.monotonic)

    def log(self, worker_id: int) -> None:
        elapsed = time.monotonic() - self.start_time
        rate = self.total / elapsed if elapsed > 0 else 0
        logger.info(
            "metadata_backfill=%d total=%d success=%d failed=%d "
            "skipped=%d elapsed=%.0fs rate=%.2f/s",
            worker_id, self.total, self.success, self.failed,
            self.skipped, elapsed, rate,
        )


def _get_needs_metadata(dsn: str, worker_id: int, total_workers: int) -> list[tuple[str, str]]:
    """Return (document_id, krs) pairs for downloaded docs missing metadata."""
    conn = make_connection(dsn)
    try:
        rows = conn.execute("""
            SELECT document_id, krs
            FROM krs_document_versions
            WHERE is_current = true
              AND is_downloaded = true
              AND metadata_fetched_at IS NULL
              AND CAST(
                  SUBSTRING(krs FROM '[0-9]+') AS BIGINT
              ) %% %s = %s
            ORDER BY krs, document_id
        """, [total_workers, worker_id]).fetchall()
        return [(row[0], row[1]) for row in rows]
    finally:
        conn.close()


async def _backfill_loop(
    worker_id: int,
    total_workers: int,
    connection: Connection,
    concurrency: int,
    delay: float,
    dsn: str,
) -> None:
    """Fetch metadata for all downloaded docs missing it."""
    doc_store = RdfDocumentStore(dsn, init_schema=False)
    stats = BackfillStats()
    sem = asyncio.Semaphore(concurrency)

    pending_docs = _get_needs_metadata(dsn, worker_id, total_workers)
    logger.info(
        "metadata_backfill=%d docs_to_backfill=%d",
        worker_id, len(pending_docs),
    )

    if not pending_docs:
        return

    async with _make_client(connection) as client:

        async def _do_one(doc_id: str, krs: str) -> None:
            async with sem:
                meta = await _fetch_metadata_with_backoff(
                    client, doc_id, worker_id,
                )
                await asyncio.sleep(delay)

            stats.total += 1

            if meta is None:
                stats.failed += 1
                return

            try:
                doc_store.update_metadata(doc_id, meta)
                stats.success += 1
            except Exception as exc:
                logger.warning(
                    "metadata_backfill=%d doc=%s error=%s",
                    worker_id, doc_id[:20], exc,
                )
                stats.failed += 1

            if stats.total % 100 == 0:
                stats.log(worker_id)

        await asyncio.gather(
            *(_do_one(doc_id, krs) for doc_id, krs in pending_docs),
            return_exceptions=True,
        )

    stats.log(worker_id)
    logger.info("metadata_backfill=%d finished", worker_id)


def run_metadata_backfill(
    worker_id: int,
    total_workers: int,
    connection: Connection,
    concurrency: int,
    delay: float,
    dsn: str,
) -> None:
    """Entrypoint for multiprocessing.Process."""
    logging.basicConfig(
        level=logging.INFO,
        format=f"%(asctime)s [meta-backfill-{worker_id}] %(levelname)s %(message)s",
    )
    asyncio.run(
        _backfill_loop(
            worker_id=worker_id,
            total_workers=total_workers,
            connection=connection,
            concurrency=concurrency,
            delay=delay,
            dsn=dsn,
        )
    )
```

### Key design decisions

1. **Reads `krs_document_versions` directly** — finds docs where `is_downloaded=true`
   AND `metadata_fetched_at IS NULL`. This is the exact gap left by `--skip-metadata`.

2. **Reuses `_fetch_metadata_with_backoff` and `_make_client` from `rdf_worker.py`** —
   no code duplication for HTTP calls, retries, or backoff.

3. **Reuses `RdfDocumentStore.update_metadata()`** — same append-only versioning,
   same legacy cache update. No new DB code needed.

4. **Worker partitioning** — uses modulo on KRS integer, same pattern as existing
   workers. Multiple backfill workers can run in parallel.

5. **No interference with download workers** — only touches documents that are
   already `is_downloaded=true`. Download workers only touch documents where
   `is_downloaded=false`. No overlap.

---

## Task 3: New runner for metadata backfill

### What
A new module `batch/metadata_runner.py` that spawns N backfill worker processes,
similar to `batch/rdf_runner.py`.

### How

#### New file: `batch/metadata_runner.py`

Same structure as `rdf_runner.py` but simpler:

```python
"""Multiprocessing orchestrator for metadata backfill.

Spawns N workers to backfill metadata for downloaded documents
that were fetched with --skip-metadata.

Usage:
    python -m batch.metadata_runner [options]
"""

import argparse
import logging
import multiprocessing
import signal

from app.config import settings
from batch.connections import Connection, build_pool
from batch.metadata_backfill import run_metadata_backfill

logger = logging.getLogger(__name__)


def _pick_connection(worker_id: int, use_vpn: bool) -> Connection:
    pool = build_pool()
    if not use_vpn:
        return pool[0]
    return pool[worker_id % len(pool)]


def run_metadata_batch(
    *, workers: int = 3,
    use_vpn: bool = False,
    concurrency: int = 10,
    delay: float = 0.2,
    dsn: str | None = None,
) -> None:
    _db = dsn or settings.database_url

    logger.info(
        "metadata_batch_start workers=%d vpn=%s concurrency=%d delay=%.1f",
        workers, use_vpn, concurrency, delay,
    )

    processes = []
    for wid in range(workers):
        conn = _pick_connection(wid, use_vpn)
        p = multiprocessing.Process(
            target=run_metadata_backfill,
            name=f"meta-backfill-{wid}",
            kwargs=dict(
                worker_id=wid,
                total_workers=workers,
                connection=conn,
                concurrency=concurrency,
                delay=delay,
                dsn=_db,
            ),
        )
        processes.append(p)

    def _shutdown(signum, frame):
        for p in processes:
            if p.is_alive():
                p.terminate()

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    for p in processes:
        p.start()

    for p in processes:
        p.join()

    logger.info("metadata_batch_complete")


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [meta-runner] %(levelname)s %(message)s",
    )
    parser = argparse.ArgumentParser(
        prog="python -m batch.metadata_runner",
        description="Backfill metadata for downloaded documents missing it.",
    )
    parser.add_argument("--workers", type=int, default=3)
    parser.add_argument("--concurrency", type=int, default=10)
    parser.add_argument("--delay", type=float, default=0.2)
    parser.add_argument("--no-vpn", action="store_true")
    parser.add_argument("--db", type=str, default=None)
    args = parser.parse_args()

    run_metadata_batch(
        workers=args.workers,
        use_vpn=not args.no_vpn,
        concurrency=args.concurrency,
        delay=args.delay,
        dsn=args.db,
    )


if __name__ == "__main__":
    main()
```

### Design notes

- **Higher concurrency (10) and lower delay (0.2s)** than download workers.
  Metadata is a lightweight `GET` returning ~200 bytes of JSON — much cheaper
  than ZIP downloads. The RDF API is unlikely to rate-limit these.

- **Fewer workers (3)** — metadata backfill is not the bottleneck.
  3 workers × 10 concurrency = 30 concurrent metadata fetches.

- **Runs as a separate systemd service or manual command.** Does not need
  to run continuously — just invoke it periodically or after a download batch.

---

## Task 4: Make GCS uploads non-blocking

### What
Wrap the synchronous `blob.upload_from_string()` calls in `GcsStorage.save_extracted()`
with `asyncio.loop.run_in_executor()` so they don't block the async event loop.

### Why
Currently, when a worker downloads a ZIP and uploads 3 files to GCS, the
entire event loop is blocked for ~100-300ms per file. During that time, no
other downloads can proceed on that worker. With concurrency=5, this means
up to 5 concurrent GCS uploads can stall all HTTP activity.

### How

#### File: `app/scraper/storage.py`

Add an async wrapper method to `GcsStorage`:

```python
async def async_save_extracted(self, doc_dir: str, zip_bytes: bytes, document_id: str) -> dict:
    """Non-blocking version of save_extracted. Runs GCS I/O in a thread."""
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(
        None, self.save_extracted, doc_dir, zip_bytes, document_id,
    )
```

Add the same to `LocalStorage` for interface consistency:
```python
async def async_save_extracted(self, doc_dir: str, zip_bytes: bytes, document_id: str) -> dict:
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(
        None, self.save_extracted, doc_dir, zip_bytes, document_id,
    )
```

#### File: `batch/rdf_worker.py`

In `_download_one_document`, change line 351 from:
```python
manifest = storage.save_extracted(doc_dir, zip_bytes, doc_id)
```
to:
```python
if hasattr(storage, 'async_save_extracted'):
    manifest = await storage.async_save_extracted(doc_dir, zip_bytes, doc_id)
else:
    manifest = storage.save_extracted(doc_dir, zip_bytes, doc_id)
```

This is backwards-compatible: if the storage backend doesn't have the async
method (e.g., in tests with a mock), it falls back to the sync version.

### Expected impact
- Unblocks the event loop during GCS uploads (~100-300ms per file)
- Other download coroutines can proceed while files upload
- **~30-50% throughput increase** depending on file count per document

### Testing
- Existing `test_download_one_document_full_flow` passes unchanged (mock has no
  `async_save_extracted`, so the sync fallback is used)
- Add one new test:

```python
@pytest.mark.asyncio
async def test_async_save_extracted_gcs():
    """async_save_extracted delegates to save_extracted in a thread."""
    # Use LocalStorage (no GCS needed), call async_save_extracted
    # Assert: returns same manifest as save_extracted
```

---

## Task 5: New systemd service for metadata backfill

### What
Add a systemd service file for the metadata backfill job and document the
new production workflow.

### How

#### New file: `deploy/metadata-backfill.service`

```ini
[Unit]
Description=RDF Metadata Backfill Worker
After=network.target

[Service]
Type=simple
User=worker
WorkingDirectory=/opt/rdf-api-project
EnvironmentFile=/opt/rdf-api-project/.env
ExecStart=/opt/rdf-api-project/.venv/bin/python -m batch.metadata_runner
Restart=on-failure
RestartSec=60

[Install]
WantedBy=multi-user.target
```

#### Deploy on VM

```bash
sudo cp deploy/metadata-backfill.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now metadata-backfill
```

---

## Combined production workflow

After all 5 tasks are implemented, the recommended production setup is:

```text
┌────────────────────────────────────────────────────────────────┐
│  rdf-batch-vm                                                  │
│                                                                │
│  Service 1: krs-scanner     (batch.runner)                     │
│    7 workers, VPN — probes KRS integers, writes batch_progress │
│                                                                │
│  Service 2: rdf-worker      (batch.rdf_runner --skip-metadata) │
│    5 workers, concurrency=5 — discovery + ZIP download + GCS   │
│    NO metadata fetch → ~2x faster downloads                    │
│    GCS uploads are non-blocking → ~1.3x on top                 │
│                                                                │
│  Service 3: metadata-backfill (batch.metadata_runner)          │
│    3 workers, concurrency=10 — lightweight GET requests        │
│    Backfills filename, is_ifrs, is_correction, dates           │
│    Runs independently, no interference with downloads          │
└────────────────────────────────────────────────────────────────┘
```

### No interference guarantee

| Resource | rdf-worker writes | metadata-backfill writes | Conflict? |
|----------|-------------------|--------------------------|-----------|
| `krs_document_versions` | Sets `is_downloaded=true`, storage fields | Sets `metadata_fetched_at`, filename, flags | **No** — different columns, append-only versioning handles concurrent writes |
| `krs_documents` (legacy) | Sets `is_downloaded`, storage fields | Sets `filename`, `is_ifrs`, `metadata_fetched_at` | **No** — different columns, no WHERE overlap |
| `batch_rdf_progress` | Sets discovery status | Not touched | **No** |
| HTTP endpoints | `POST /dokumenty/tresc` (ZIP) | `GET /dokumenty/{id}` (metadata) | **No** — different endpoints |

### Expected combined speedup

| Change | Individual impact | Cumulative |
|--------|------------------|------------|
| Baseline (before today) | ~2,800 docs/hr | 1.0x |
| Parallel downloads + lower delay (deployed) | ~4,500 docs/hr | 1.6x |
| Skip metadata (`--skip-metadata`) | ~1.8x on download phase | ~2.9x |
| Async GCS uploads | ~1.3x | ~3.7x |
| **Total estimated** | | **~10,000 docs/hr** |

Metadata backfill runs separately and does not affect download throughput.
At 30 concurrent metadata fetches with 0.2s delay, it processes ~150 docs/sec
(~540K docs/hr) — far faster than the download pipeline produces work.

---

## Implementation order

Tasks are independent and can be implemented in any order, but the recommended
sequence is:

1. **Task 4** (async GCS) — smallest change, immediate benefit, no new files
2. **Task 1** (`--skip-metadata` flag) — additive, no behavior change when flag is off
3. **Task 2** (metadata backfill module) — depends on Task 1 being useful
4. **Task 3** (metadata runner) — depends on Task 2
5. **Task 5** (systemd service + docs) — depends on Task 3

Tasks 1 and 4 can be implemented in parallel.
Tasks 2 and 3 can be implemented together.

---

## Files changed (summary)

| File | Change type | Task |
|------|-------------|------|
| `app/config.py` | Modified — add `rdf_batch_skip_metadata` | 1 |
| `batch/rdf_worker.py` | Modified — add `skip_metadata` param, async storage | 1, 4 |
| `batch/rdf_runner.py` | Modified — add `--skip-metadata` CLI flag | 1 |
| `app/scraper/storage.py` | Modified — add `async_save_extracted` method | 4 |
| `batch/metadata_backfill.py` | **New** — backfill worker | 2 |
| `batch/metadata_runner.py` | **New** — backfill orchestrator | 3 |
| `deploy/metadata-backfill.service` | **New** — systemd unit | 5 |
| `tests/batch/test_rdf_worker.py` | Modified — add skip_metadata + async storage tests | 1, 4 |
| `tests/batch/test_metadata_backfill.py` | **New** — backfill tests | 2 |
