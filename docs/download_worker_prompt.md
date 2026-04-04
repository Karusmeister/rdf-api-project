# Download-Only Worker — Coding Agent Prompt

## TASK

Create a download-only worker for the RDF batch pipeline.

## CONTEXT

The existing `batch/rdf_worker.py` couples two phases in one loop:
- Phase 1 (discovery): for each KRS, call the encrypted search API to find documents
- Phase 2 (download): for each discovered document, fetch metadata + ZIP + upload to GCS

When workers restart, they spend hours on discovery before downloads ramp up.
We need a standalone download-only worker that pulls from the existing backlog
of discovered-but-not-downloaded documents, independent of the discovery workers.

**DO NOT modify any existing files. All changes are additive (new files only).**

## FILES TO CREATE

1. `batch/download_worker.py` — async download-only worker
2. `batch/download_runner.py` — multiprocessing orchestrator (CLI entrypoint)
3. `deploy/rdf-downloader.service` — systemd unit file
4. `tests/batch/test_download_worker.py` — tests

## DETAILED SPEC

### batch/download_worker.py

Create an async worker that ONLY does Phase 2 (download). No discovery logic.

The worker loop should:

1. Query `krs_document_versions` for documents where `is_current=true AND
   (is_downloaded=false OR is_downloaded IS NULL) AND download_error IS NULL`.
   Partition by `CAST(SUBSTRING(krs FROM '[0-9]+') AS BIGINT) % total_workers = worker_id`.
   Return `(document_id, krs)` pairs ordered by krs.
2. For each document, call `_download_one_document()` — reuse this function directly
   from `batch.rdf_worker` (import it, don't copy it).
3. Use `asyncio.Semaphore(concurrency)` to bound parallel downloads.
4. Process documents with bounded concurrency using asyncio tasks, same pattern as
   lines 522-537 of `batch/rdf_worker.py`.

**Reuse from `batch/rdf_worker.py` (import, don't duplicate):**
- `_make_client(connection)` — creates httpx.AsyncClient
- `_download_one_document(client, krs, doc_id, doc_store, storage, delay, worker_id, stats, skip_metadata)`
- `RdfWorkerStats` — for logging
- `ConnectionHealth` — for adaptive backoff

**Reuse from existing modules:**
- `batch.rdf_document_store.RdfDocumentStore` — for `get_undownloaded` and marking results
- `batch.connections.Connection` — connection dataclass
- `app.scraper.storage.create_storage` — GCS/local storage factory
- `app.db.connection.make_connection` — for DB queries

**Create a new function for the DB query:**

```python
def get_all_undownloaded(dsn: str, worker_id: int, total_workers: int) -> list[tuple[str, str]]:
    """Return (document_id, krs) pairs for all undownloaded docs in this worker's partition."""
```

**Worker function signature:**

```python
async def _download_only_loop(
    worker_id: int,
    total_workers: int,
    connection: Connection,
    concurrency: int,
    delay: float,
    dsn: str,
    skip_metadata: bool = True,
) -> None:
```

**Entrypoint for multiprocessing:**

```python
def run_download_worker(
    worker_id: int,
    total_workers: int,
    connection: Connection,
    concurrency: int,
    delay: float,
    dsn: str,
    skip_metadata: bool = True,
) -> None:
```

Default `skip_metadata=True` since this worker is optimized for throughput.
Use log format: `%(asctime)s [dl-worker-{worker_id}] %(levelname)s %(message)s`
Log stats every 50 documents processed.

### batch/download_runner.py

Multiprocessing orchestrator, same pattern as `batch/rdf_runner.py`.

CLI interface:

```
python -m batch.download_runner [options]

--workers N          Number of worker processes (default: 5)
--concurrency N      Async concurrency per worker (default: 5)
--delay FLOAT        Delay between download requests (default: 0.3)
--no-vpn             Disable VPN
--vpn                Enable VPN
--skip-metadata      Skip metadata fetch (default: on)
--fetch-metadata     Fetch metadata before each download
--db DSN             PostgreSQL DSN
```

Add `if __name__ == "__main__": main()` so it works as `python -m batch.download_runner`.

Reuse `_pick_connection` and `_validate_vpn_config` patterns from `batch/rdf_runner.py`
(reimplement them, don't import — they're trivial and it keeps the module self-contained).

### deploy/rdf-downloader.service

```ini
[Unit]
Description=RDF Document Download Worker (download-only)
After=network.target

[Service]
Type=simple
User=worker
WorkingDirectory=/opt/rdf-api-project
EnvironmentFile=/opt/rdf-api-project/.env
ExecStart=/opt/rdf-api-project/.venv/bin/python -m batch.download_runner --skip-metadata
Restart=on-failure
RestartSec=30

[Install]
WantedBy=multi-user.target
```

### tests/batch/test_download_worker.py

Write tests using the same patterns as `tests/batch/test_rdf_worker.py`:
- Use `conftest.py` fixtures for test DB (`pg_dsn`, `clean_pg`, `isolated_db`)
- Use `respx` for HTTP mocking
- Use real PostgreSQL for DB assertions

**Tests to write:**

1. `test_get_all_undownloaded` — insert documents into `krs_document_versions` with
   mixed `is_downloaded` states, verify only undownloaded ones returned, verify
   partition filtering works.
2. `test_get_all_undownloaded_excludes_errored` — documents with `download_error`
   should not be returned.
3. `test_download_only_loop_processes_backlog` — mock HTTP endpoints, seed DB with
   discovered-but-undownloaded documents, run the loop, verify documents get
   downloaded and marked in DB. Use `respx` to mock `/dokumenty/tresc` and
   `/dokumenty/{id}`. Use `LocalStorage` (not GCS) for test.
4. `test_download_only_loop_empty_backlog` — no undownloaded docs, worker exits
   immediately without errors.

## IMPORTANT CONSTRAINTS

- Do NOT modify `batch/rdf_worker.py`, `batch/rdf_runner.py`, or any existing file
- Import and reuse functions from `rdf_worker.py` — do not copy/paste them
- Follow the exact same coding patterns (logging, error handling, stats) as `rdf_worker.py`
- All DB queries use parameterized `%s`, no f-strings for SQL
- Test with `pytest tests/batch/test_download_worker.py -v`

## PRODUCTION WORKFLOW

After this change, the recommended production setup is:

```
Service 1: krs-scanner      (batch.runner)           — discovers valid KRS numbers (unchanged)
Service 2: rdf-worker       (batch.rdf_runner)        — discovery + download (existing, unchanged)
Service 3: rdf-downloader   (batch.download_runner)   — download-only from backlog (NEW)
```

Services 2 and 3 can run simultaneously. They both read from
`krs_document_versions` but the download function uses DB-level
conflict handling (append-only versioning with `snapshot_hash` dedup),
so concurrent writes to the same document are safe.
