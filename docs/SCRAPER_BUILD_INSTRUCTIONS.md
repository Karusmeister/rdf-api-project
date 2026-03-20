# Scraper Module - Claude Code Build Instructions

> **Scope:** Implement the scraper module for local development. NO GCP deployment.
> **Approach:** Read existing code first, then build incrementally. Test each phase before moving on.
> **Style:** Match existing codebase conventions exactly (async, pydantic v2, type hints, no classes where modules suffice).

---

## IMPORTANT - Read before writing any code

1. Read `CLAUDE.md` for project conventions
2. Read `docs/API_REORGANIZATION.md` for the router directory convention - follow it for the scraper router
3. Read `docs/SCRAPER_ARCHITECTURE.md` for the full design spec (DB schema, storage abstraction, job flow, config)
3. Read `app/rdf_client.py` - you will reuse its methods directly from the scraper
4. Read `app/config.py` - you will extend it with new settings
5. Read `app/routers/analysis.py` - see how `_get_available_periods()` already does discovery. Your scraper generalizes this pattern to ALL document types
6. Run existing tests first: `pytest tests/ -v` - make sure everything passes before you touch anything

---

## Dependency changes

Add to `requirements.txt` (append, do not reorder existing lines):

```
duckdb>=1.2
aiofiles>=24.1
click>=8.1
```

After editing, run:
```bash
pip install -r requirements.txt
```

---

## Phase 1: Config extension

### File: `app/config.py`

Add these fields to the existing `Settings` class. Do NOT remove or reorder existing fields:

```python
# --- Scraper ---
scraper_db_path: str = "data/scraper.duckdb"

# Storage
storage_backend: str = "local"           # 'local' or 'gcs'
storage_local_path: str = "data/documents"
storage_gcs_bucket: str = ""
storage_gcs_prefix: str = "krs/"

# Scraper behavior
scraper_order_strategy: str = "priority_then_oldest"
scraper_delay_between_krs: float = 2.0
scraper_delay_between_requests: float = 0.5
scraper_max_krs_per_run: int = 0          # 0 = unlimited
scraper_max_errors_before_skip: int = 3
scraper_error_backoff_hours: int = 24
scraper_download_timeout: int = 60
```

Also update `.env.example` - add all new vars with comments.

### Verify

```bash
python -c "from app.config import settings; print(settings.scraper_db_path)"
```

---

## Phase 2: DuckDB database module

### File: `app/scraper/__init__.py`

Empty file.

### File: `app/scraper/db.py`

This module manages the DuckDB connection and schema. Follow these rules:

- Use a MODULE-LEVEL connection, not a class. Mirror the pattern from `app/rdf_client.py` (module-level `_client` with `start()`/`stop()`).
- DuckDB does not need async - its operations are fast enough to call synchronously. Wrap in `asyncio.to_thread()` only if you measure a problem (you won't).
- Connection is opened with `duckdb.connect(path)`. DuckDB uses WAL mode by default.
- Schema is created via `CREATE TABLE IF NOT EXISTS` on connect - idempotent, safe to run every startup.

#### Required functions

```python
import duckdb
from app.config import settings

_conn: duckdb.DuckDBPyConnection | None = None

def connect() -> None:
    """Open DB connection and ensure schema exists. Call once at startup."""
    global _conn
    import os
    os.makedirs(os.path.dirname(settings.scraper_db_path) or ".", exist_ok=True)
    _conn = duckdb.connect(settings.scraper_db_path)
    _init_schema()

def close() -> None:
    """Close DB connection."""
    global _conn
    if _conn:
        _conn.close()
        _conn = None

def get_conn() -> duckdb.DuckDBPyConnection:
    if _conn is None:
        raise RuntimeError("Scraper DB not connected - call connect() first")
    return _conn

def _init_schema() -> None:
    """Create tables if they don't exist. Idempotent."""
    # Use the EXACT SQL from docs/SCRAPER_ARCHITECTURE.md section 2
    # Copy all 3 CREATE TABLE statements and all CREATE INDEX statements
    ...
```

#### CRUD helper functions (same file)

```python
def upsert_krs(krs: str, company_name: str | None, legal_form: str | None, is_active: bool) -> None:
    """Insert or update a KRS in the registry."""
    ...

def get_krs_to_check(strategy: str, limit: int, error_backoff_hours: int) -> list[dict]:
    """Return KRS entries to check, ordered by strategy. Skip recently-errored ones."""
    ...

def get_known_document_ids(krs: str) -> set[str]:
    """Return set of document_ids we already know about for this KRS."""
    ...

def insert_documents(docs: list[dict]) -> None:
    """Batch insert new documents. Each dict has keys matching krs_documents columns."""
    ...

def mark_downloaded(
    document_id: str, storage_path: str, storage_backend: str,
    file_size: int, zip_size: int, file_count: int, file_types: str,
) -> None:
    """Mark a document as downloaded. file_size is total extracted size, zip_size is original ZIP."""
    ...

def update_krs_checked(krs: str, total_docs: int, total_downloaded: int, error: str | None = None) -> None:
    """Update krs_registry after checking a KRS. Resets or increments error count."""
    ...

def create_run(run_id: str, mode: str, config_snapshot: str) -> None:
    """Insert a new scraper_runs record with status='running'."""
    ...

def finish_run(run_id: str, status: str, stats: dict) -> None:
    """Update a scraper_runs record with final stats."""
    ...

def get_stats() -> dict:
    """Return dashboard stats (total KRS, checked, unchecked, errors, docs, downloaded)."""
    ...

def get_last_run() -> dict | None:
    """Return the most recent scraper_runs record."""
    ...
```

**IMPORTANT:** For `get_krs_to_check`, implement these ordering strategies:

- `priority_then_oldest` - `ORDER BY check_priority DESC, last_checked_at ASC NULLS FIRST`
- `oldest_first` - `ORDER BY last_checked_at ASC NULLS FIRST`
- `newest_first` - `ORDER BY first_seen_at DESC`
- `random` - `ORDER BY random()`
- `sequential` - `ORDER BY krs ASC`

Also filter out KRS where `check_error_count >= max_errors` AND last_checked_at is within `error_backoff_hours`. And filter out `is_active = false`.

### File: `tests/test_scraper_db.py`

Test with an in-memory DuckDB (override `settings.scraper_db_path` to `:memory:` or a temp file). Tests:

1. `test_schema_creation` - connect, verify all 3 tables exist
2. `test_upsert_krs` - insert, then update same KRS - verify upsert behavior
3. `test_insert_and_get_documents` - insert docs, verify `get_known_document_ids` returns them
4. `test_mark_downloaded` - mark a doc, verify `is_downloaded=true`, `storage_path`, `file_count`, and `file_types` set
5. `test_ordering_strategies` - insert 5 KRS with different priorities/dates, verify each strategy orders correctly
6. `test_error_backoff` - set high error count, verify KRS is skipped by `get_krs_to_check`
7. `test_run_lifecycle` - create run, finish run, verify `get_last_run()` returns it
8. `test_stats` - insert mix of data, verify `get_stats()` counts are correct

Run:
```bash
pytest tests/test_scraper_db.py -v
```

All tests must pass before proceeding.

---

## Phase 3: Storage backends

### Design: store extracted files, NOT ZIPs

The RDF API returns ZIP archives, but inside each ZIP is typically 1 file - an `.xml`
(Polish GAAP), `.xhtml` (IFRS), or `.pdf`. We **extract on download** and store the
raw files. This is critical because:

- Files will be parsed for ML training data - no point unzipping every read
- You can `grep`, `find`, `xargs` across the corpus directly
- Debugging is instant - open any file in an editor
- PDFs, XMLs, XHTMLs are all stored as-is, ready to process
- Size overhead is ~3x vs ZIP (~250 GB vs ~75 GB) but disk is cheap ($0.04/GB)

### Directory structure

```
data/documents/
  krs/
    0000694720/
      ZgsX8Fsncb1PFW07-T4XoQ/         # folder named after document_id (safe chars)
        Bjwk_SF_za_2024.xml            # original filename preserved from ZIP
        manifest.json                   # extraction metadata
      Bj4U6NqM2-gdavchRK5COw/
        Bjwk_SF_za_2019.xml
        manifest.json
    0000012345/
      abc123def/
        sprawozdanie.pdf               # some docs are PDFs
        manifest.json
```

The `manifest.json` for each document:
```json
{
  "document_id": "ZgsX8Fsncb1PFW07-T4XoQ==",
  "source_zip_size": 45230,
  "extracted_at": "2026-03-20T02:15:00Z",
  "files": [
    {"name": "Bjwk_SF_za_2024.xml", "size": 187420, "type": "xml"}
  ]
}
```

### File: `app/scraper/storage.py`

Follow these rules:

- `StorageBackend` is a `typing.Protocol` (NOT an ABC, NOT a base class)
- `LocalStorage` uses synchronous `pathlib.Path` operations (fast enough)
- Do NOT implement `GCSStorage` yet - just define the protocol and local backend
- The `document_id` contains unsafe chars (`+`, `/`, `=`). Make dir names safe: replace `+` with `-`, `/` with `_`, strip `=`
- The `save_extracted` method takes raw ZIP bytes, extracts them, writes each file + manifest
- Keep a low-level `save` for writing individual files (manifest uses it)

```python
from typing import Protocol
import io
import json
import zipfile
from datetime import datetime, timezone
from pathlib import Path


class StorageBackend(Protocol):
    def save_extracted(self, doc_dir: str, zip_bytes: bytes, document_id: str) -> dict:
        """Extract ZIP, save raw files + manifest. Returns manifest dict."""
        ...
    def exists(self, path: str) -> bool: ...
    def read(self, path: str) -> bytes: ...
    def list_files(self, dir_path: str) -> list[str]: ...
    def get_full_path(self, path: str) -> str: ...


def safe_dirname(document_id: str) -> str:
    """Convert Base64 document ID to a filesystem-safe directory name."""
    return document_id.replace("+", "-").replace("/", "_").rstrip("=")


def make_doc_dir(krs: str, document_id: str) -> str:
    """Build relative directory path: krs/0000694720/ZgsX8Fsncb1PFW07-T4XoQ"""
    return f"krs/{krs.zfill(10)}/{safe_dirname(document_id)}"


def _classify_file(filename: str) -> str:
    """Return file type from extension."""
    ext = filename.rsplit(".", 1)[-1].lower() if "." in filename else "unknown"
    return ext  # xml, xhtml, pdf, etc.


class LocalStorage:
    def __init__(self, base_path: str):
        self._base = Path(base_path)
        self._base.mkdir(parents=True, exist_ok=True)

    def save_extracted(self, doc_dir: str, zip_bytes: bytes, document_id: str) -> dict:
        """
        Extract ZIP contents into doc_dir, write manifest.json.
        Returns the manifest dict with file list and sizes.
        """
        target = self._base / doc_dir
        target.mkdir(parents=True, exist_ok=True)

        files_info = []
        total_extracted_size = 0

        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
            for entry in zf.infolist():
                if entry.is_dir():
                    continue
                # Flatten any subdirectories in ZIP - just use the filename
                filename = Path(entry.filename).name
                if not filename:
                    continue

                data = zf.read(entry.filename)
                (target / filename).write_bytes(data)

                file_size = len(data)
                total_extracted_size += file_size
                files_info.append({
                    "name": filename,
                    "size": file_size,
                    "type": _classify_file(filename),
                })

        manifest = {
            "document_id": document_id,
            "source_zip_size": len(zip_bytes),
            "extracted_at": datetime.now(timezone.utc).isoformat(),
            "files": files_info,
        }

        (target / "manifest.json").write_text(
            json.dumps(manifest, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )

        return manifest

    def exists(self, path: str) -> bool:
        return (self._base / path).exists()

    def read(self, path: str) -> bytes:
        return (self._base / path).read_bytes()

    def list_files(self, dir_path: str) -> list[str]:
        target = self._base / dir_path
        if not target.is_dir():
            return []
        return [f.name for f in target.iterdir() if f.is_file()]

    def get_full_path(self, path: str) -> str:
        return str(self._base / path)


def create_storage() -> StorageBackend:
    from app.config import settings
    if settings.storage_backend == "gcs":
        raise NotImplementedError("GCS backend not yet implemented. Set STORAGE_BACKEND=local")
    return LocalStorage(settings.storage_local_path)
```

### File: `tests/test_storage.py`

Use `tmp_path` fixture (pytest built-in).

To test extraction, build a real in-memory ZIP:

```python
import io, zipfile

def make_test_zip(files: dict[str, bytes]) -> bytes:
    """Create a ZIP in memory. files = {"filename.xml": b"content", ...}"""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        for name, data in files.items():
            zf.writestr(name, data)
    return buf.getvalue()
```

Tests:

1. `test_save_extracted_xml` - ZIP with one XML file, verify XML extracted + manifest written
2. `test_save_extracted_pdf` - ZIP with one PDF file, verify extracted and manifest says type "pdf"
3. `test_save_extracted_multiple_files` - ZIP with XML + PDF, verify both extracted, manifest has 2 entries
4. `test_manifest_content` - verify manifest has correct document_id, source_zip_size, files list
5. `test_exists` - false before save, true after (check doc_dir and a file inside it)
6. `test_read` - extract, then read back the XML file, verify identical content
7. `test_list_files` - extract ZIP with 2 files, verify list_files returns both + manifest.json
8. `test_safe_dirname` - test with various Base64 IDs including `+`, `/`, `=`
9. `test_make_doc_dir` - verify output format matches `krs/0000694720/ZgsX8Fsncb1PFW07-T4XoQ`
10. `test_zip_with_subdirectories` - ZIP containing `subdir/file.xml`, verify file is flattened to just `file.xml`

```bash
pytest tests/test_storage.py -v
```

---

## Phase 4: Scraper job

### File: `app/scraper/job.py`

This is the core scraper loop. It is a standalone async function, NOT a FastAPI endpoint. It creates its own `rdf_client` connection and tears it down when done.

#### Key design decisions

- The job imports and uses `app.rdf_client` directly (same functions the API uses)
- Rate limiting: use `asyncio.sleep()` between requests
- Each KRS is wrapped in try/except - never crash the whole run
- The job writes to DuckDB synchronously (fast enough)
- Progress is logged to stdout (click.echo or logging)

#### Structure

```python
import asyncio
import json
import logging
import uuid
from datetime import datetime, timezone

from app import rdf_client
from app.config import settings
from app.scraper import db
from app.scraper.storage import create_storage, make_doc_dir

logger = logging.getLogger("scraper")

async def run_scraper(
    mode: str = "full_scan",
    specific_krs: list[str] | None = None,
    max_krs: int = 0,
) -> dict:
    """
    Main scraper entry point.

    Modes:
      - full_scan: iterate all KRS in registry, ordered by strategy
      - new_only: only KRS with last_checked_at IS NULL
      - retry_errors: only KRS with check_error_count > 0
      - specific_krs: only the KRS numbers passed in specific_krs list

    Returns dict with run stats.
    """
    # 1. Init
    db.connect()
    await rdf_client.start()
    storage = create_storage()

    run_id = str(uuid.uuid4())
    config_snap = json.dumps({
        "mode": mode,
        "strategy": settings.scraper_order_strategy,
        "delay_krs": settings.scraper_delay_between_krs,
        "delay_req": settings.scraper_delay_between_requests,
        "max_krs": max_krs or settings.scraper_max_krs_per_run,
    })
    db.create_run(run_id, mode, config_snap)

    stats = {
        "krs_checked": 0,
        "krs_new_found": 0,
        "documents_discovered": 0,
        "documents_downloaded": 0,
        "documents_failed": 0,
        "bytes_downloaded": 0,
    }

    try:
        # 2. Determine which KRS to process
        if mode == "specific_krs" and specific_krs:
            krs_list = [{"krs": k.zfill(10)} for k in specific_krs]
        else:
            effective_max = max_krs or settings.scraper_max_krs_per_run
            limit = effective_max if effective_max > 0 else 999_999_999
            krs_list = db.get_krs_to_check(
                strategy=settings.scraper_order_strategy,
                limit=limit,
                error_backoff_hours=settings.scraper_error_backoff_hours,
            )

        logger.info(f"Run {run_id}: processing {len(krs_list)} KRS entries (mode={mode})")

        # 3. Main loop
        for i, krs_entry in enumerate(krs_list):
            krs = krs_entry["krs"]
            try:
                await _process_one_krs(krs, storage, stats)
            except Exception as e:
                logger.error(f"KRS {krs}: unhandled error: {e}")
                db.update_krs_checked(krs, total_docs=-1, total_downloaded=-1, error=str(e))

            stats["krs_checked"] += 1
            if (i + 1) % 50 == 0:
                logger.info(f"Progress: {i+1}/{len(krs_list)} KRS checked, "
                            f"{stats['documents_downloaded']} docs downloaded")

            # Rate limit between KRS
            if i < len(krs_list) - 1:
                await asyncio.sleep(settings.scraper_delay_between_krs)

        db.finish_run(run_id, "completed", stats)

    except KeyboardInterrupt:
        logger.warning("Interrupted by user")
        db.finish_run(run_id, "interrupted", stats)
    except Exception as e:
        logger.error(f"Run failed: {e}")
        db.finish_run(run_id, "failed", {**stats, "error_message": str(e)})
        raise
    finally:
        await rdf_client.stop()
        db.close()

    return stats


async def _process_one_krs(krs: str, storage, stats: dict) -> None:
    """
    Process a single KRS:
    1. Validate entity
    2. Search all documents
    3. Find new ones
    4. Download missing ones
    """
    delay = settings.scraper_delay_between_requests

    # Step 1: Validate entity (also updates company name)
    lookup = await rdf_client.dane_podstawowe(krs)
    await asyncio.sleep(delay)

    if not lookup.get("czyPodmiotZnaleziony", False):
        logger.debug(f"KRS {krs}: not found in registry, marking inactive")
        db.upsert_krs(krs, company_name=None, legal_form=None, is_active=False)
        return

    podmiot = lookup["podmiot"]
    is_active = not bool(podmiot.get("wykreslenie"))
    db.upsert_krs(
        krs=krs,
        company_name=podmiot.get("nazwaPodmiotu"),
        legal_form=podmiot.get("formaPrawna"),
        is_active=is_active,
    )

    # Step 2: Fetch all documents (paginated)
    all_docs = []
    page = 0
    while True:
        search_result = await rdf_client.wyszukiwanie(krs, page=page, page_size=100)
        await asyncio.sleep(delay)

        content = search_result.get("content", [])
        all_docs.extend(content)

        meta = search_result.get("metadaneWynikow", {})
        total_pages = meta.get("liczbaStron", 1)
        if page + 1 >= total_pages:
            break
        page += 1

    # Step 3: Find new documents
    known_ids = db.get_known_document_ids(krs)
    new_docs = [d for d in all_docs if d["id"] not in known_ids]

    if new_docs:
        stats["documents_discovered"] += len(new_docs)
        stats["krs_new_found"] += 1

        # Insert all new documents into DB (metadata from search)
        rows = []
        for d in new_docs:
            rows.append({
                "document_id": d["id"],
                "krs": krs.zfill(10),
                "rodzaj": d["rodzaj"],
                "status": d["status"],
                "nazwa": d.get("nazwa"),
                "okres_start": d.get("okresSprawozdawczyPoczatek"),
                "okres_end": d.get("okresSprawozdawczyKoniec"),
                "discovered_at": datetime.now(timezone.utc).isoformat(),
            })
        db.insert_documents(rows)

    # Step 4: Download documents not yet downloaded
    to_download = [
        d for d in all_docs
        if d["id"] not in known_ids or d["id"] in _get_not_downloaded_ids(krs)
    ]
    # Also include previously-known but not-downloaded docs
    # Simpler approach: query DB for this KRS's undownloaded docs
    undownloaded = db.get_undownloaded_documents(krs)

    for doc_id in undownloaded:
        try:
            # Fetch metadata (useful for filename, czyMSR, etc.)
            meta = await rdf_client.metadata(doc_id)
            await asyncio.sleep(delay)

            db.update_document_metadata(doc_id, {
                "filename": meta.get("nazwaPliku"),
                "is_ifrs": meta.get("czyMSR"),
                "is_correction": meta.get("czyKorekta"),
                "date_filed": meta.get("dataDodania"),
                "date_prepared": meta.get("dataSporządzenia"),
            })

            # Download ZIP from API
            zip_bytes = await rdf_client.download([doc_id])
            await asyncio.sleep(delay)

            # Extract ZIP and save raw files (xml, xhtml, pdf, etc.)
            doc_dir = make_doc_dir(krs, doc_id)
            manifest = storage.save_extracted(doc_dir, zip_bytes, doc_id)

            # Calculate totals from manifest
            total_extracted = sum(f["size"] for f in manifest["files"])
            file_types = ",".join(sorted(set(f["type"] for f in manifest["files"])))

            db.mark_downloaded(
                document_id=doc_id,
                storage_path=doc_dir,
                storage_backend=settings.storage_backend,
                file_size=total_extracted,
                zip_size=len(zip_bytes),
                file_count=len(manifest["files"]),
                file_types=file_types,
            )
            stats["documents_downloaded"] += 1
            stats["bytes_downloaded"] += total_extracted

            logger.debug(f"KRS {krs}: extracted {doc_id} -> {len(manifest['files'])} files "
                         f"({total_extracted} bytes, types: {file_types})")

        except Exception as e:
            logger.warning(f"KRS {krs}: failed to download {doc_id}: {e}")
            db.update_document_error(doc_id, str(e))
            stats["documents_failed"] += 1

    # Step 5: Update registry
    total_docs = len(all_docs)
    total_downloaded = total_docs - len(undownloaded) + stats["documents_downloaded"]
    db.update_krs_checked(krs, total_docs=total_docs, total_downloaded=total_downloaded, error=None)
```

#### Additional DB helpers needed

Looking at the job code above, you'll need these extra functions in `db.py`:

```python
def get_undownloaded_documents(krs: str) -> list[str]:
    """Return list of document_ids for this KRS where is_downloaded = false."""
    ...

def update_document_metadata(document_id: str, meta: dict) -> None:
    """Update extended metadata fields on krs_documents."""
    ...

def update_document_error(document_id: str, error: str) -> None:
    """Set download_error on a document."""
    ...
```

Add these when you build `db.py` - don't wait for Phase 4.

### IMPORTANT: _process_one_krs simplification

The function above is intentionally verbose so you understand the flow. When implementing, you may refactor, but keep these invariants:

- Each API call is followed by `asyncio.sleep(delay)`
- Each document download is in its own try/except
- Stats dict is updated atomically per operation
- KRS registry is updated at the end even if some downloads failed

---

## Phase 5: CLI

### File: `app/scraper/cli.py`

Use `click` for the CLI. This is the entrypoint users run directly.

```python
import asyncio
import click
import csv
import json
import logging

@click.group()
def cli():
    """RDF Scraper - bulk KRS document collector."""
    pass

@cli.command()
@click.option("--mode", type=click.Choice(["full_scan", "new_only", "retry_errors"]), default="full_scan")
@click.option("--krs", multiple=True, help="Specific KRS numbers to process (overrides mode)")
@click.option("--max-krs", type=int, default=0, help="Max KRS to process (0=unlimited)")
@click.option("--verbose", "-v", is_flag=True)
def run(mode, krs, max_krs, verbose):
    """Run the scraper job."""
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    if krs:
        mode = "specific_krs"

    from app.scraper.job import run_scraper
    stats = asyncio.run(run_scraper(
        mode=mode,
        specific_krs=list(krs) if krs else None,
        max_krs=max_krs,
    ))

    click.echo(f"\nRun completed:")
    for k, v in stats.items():
        click.echo(f"  {k}: {v}")

@cli.command("import-krs")
@click.option("--file", "filepath", required=True, type=click.Path(exists=True))
@click.option("--column", default="krs", help="Column name containing KRS numbers")
def import_krs(filepath, column):
    """Import KRS numbers from a CSV file."""
    from app.scraper import db
    from datetime import datetime, timezone

    db.connect()
    count = 0
    with open(filepath, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            krs_val = row[column].strip().zfill(10)
            if krs_val.isdigit() and len(krs_val) == 10:
                db.upsert_krs(krs_val, company_name=None, legal_form=None, is_active=True)
                count += 1
    db.close()
    click.echo(f"Imported {count} KRS numbers from {filepath}")

@cli.command("import-range")
@click.option("--start", "start_num", required=True, type=int)
@click.option("--end", "end_num", required=True, type=int)
def import_range(start_num, end_num):
    """Import a range of KRS numbers (will be validated during scan)."""
    from app.scraper import db
    from datetime import datetime, timezone

    db.connect()
    count = 0
    for n in range(start_num, end_num + 1):
        krs = str(n).zfill(10)
        db.upsert_krs(krs, company_name=None, legal_form=None, is_active=True)
        count += 1
        if count % 10000 == 0:
            click.echo(f"  ...{count} inserted")
    db.close()
    click.echo(f"Imported {count} KRS numbers (range {start_num}-{end_num})")

@cli.command()
def status():
    """Show scraper statistics."""
    from app.scraper import db

    db.connect()
    stats = db.get_stats()
    last_run = db.get_last_run()
    db.close()

    click.echo("=== Scraper Status ===")
    for k, v in stats.items():
        click.echo(f"  {k}: {v}")

    if last_run:
        click.echo(f"\nLast run: {last_run['started_at']} ({last_run['status']})")
        click.echo(f"  KRS checked: {last_run.get('krs_checked', '?')}")
        click.echo(f"  Docs downloaded: {last_run.get('documents_downloaded', '?')}")

if __name__ == "__main__":
    cli()
```

### Running the CLI

```bash
# All commands
python -m app.scraper.cli --help

# Import a small test range
python -m app.scraper.cli import-range --start 694720 --end 694720

# Run scraper for that KRS
python -m app.scraper.cli run --krs 694720 -v

# Check status
python -m app.scraper.cli status
```

---

## Phase 6: Scraper status API endpoint

### File: `app/routers/scraper/__init__.py`

Follow the domain directory convention from `docs/API_REORGANIZATION.md`:

```python
from app.routers.scraper.routes import router
```

### File: `app/routers/scraper/routes.py`

Read-only status endpoint. Does NOT start the scraper - just reads DuckDB.

```python
from fastapi import APIRouter
from app.scraper import db

router = APIRouter(prefix="/api/scraper", tags=["scraper"])

@router.get("/status")
async def scraper_status():
    """Return scraper dashboard stats. Read-only, fast."""
    db.connect()  # idempotent
    stats = db.get_stats()
    last_run = db.get_last_run()
    db.close()
    return {
        **stats,
        "last_run": last_run,
    }
```

### Register in `app/main.py`

Add alongside the other domain routers:

```python
from app.routers.scraper import router as scraper_router
# ...
app.include_router(scraper_router)
```

---

## Phase 7: Integration test

### File: `tests/test_scraper_integration.py`

An end-to-end test with mocked `rdf_client`. This verifies the whole flow: DB init, KRS insert, scraper run, file saved, DB updated.

```python
import pytest
import io
import zipfile
import json
import tempfile
import os
from unittest.mock import AsyncMock, patch
from app.scraper import db
from app.scraper.storage import LocalStorage
from app.config import settings


def _make_test_zip(files: dict[str, bytes]) -> bytes:
    """Create a real in-memory ZIP archive."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        for name, data in files.items():
            zf.writestr(name, data)
    return buf.getvalue()


@pytest.fixture
def temp_env(tmp_path):
    """Set up temp DB and storage for testing."""
    db_path = str(tmp_path / "test.duckdb")
    storage_path = str(tmp_path / "documents")

    original_db = settings.scraper_db_path
    original_storage = settings.storage_local_path
    settings.scraper_db_path = db_path
    settings.storage_local_path = storage_path

    yield tmp_path

    settings.scraper_db_path = original_db
    settings.storage_local_path = original_storage


@pytest.fixture
def mock_rdf():
    """Mock all rdf_client functions."""
    # Build a real ZIP containing an XML file
    test_xml = b'<?xml version="1.0"?><JednostkaInna><Naglowek/></JednostkaInna>'
    test_zip = _make_test_zip({"Bjwk_SF_za_2024.xml": test_xml})

    with patch("app.rdf_client.start", new_callable=AsyncMock), \
         patch("app.rdf_client.stop", new_callable=AsyncMock), \
         patch("app.rdf_client.dane_podstawowe", new_callable=AsyncMock) as mock_lookup, \
         patch("app.rdf_client.wyszukiwanie", new_callable=AsyncMock) as mock_search, \
         patch("app.rdf_client.metadata", new_callable=AsyncMock) as mock_meta, \
         patch("app.rdf_client.download", new_callable=AsyncMock) as mock_dl:

        mock_lookup.return_value = {
            "podmiot": {
                "numerKRS": "0000694720",
                "nazwaPodmiotu": "TEST SP. Z O.O.",
                "formaPrawna": "SP. Z O.O.",
                "wykreslenie": "",
            },
            "czyPodmiotZnaleziony": True,
        }

        mock_search.return_value = {
            "content": [
                {
                    "id": "ZgsX8Fsncb1PFW07-T4XoQ==",
                    "rodzaj": "18",
                    "status": "NIEUSUNIETY",
                    "nazwa": None,
                    "okresSprawozdawczyPoczatek": "2024-01-01",
                    "okresSprawozdawczyKoniec": "2024-12-31",
                }
            ],
            "metadaneWynikow": {
                "numerStrony": 0,
                "rozmiarStrony": 100,
                "liczbaStron": 1,
                "calkowitaLiczbaObiektow": 1,
            },
        }

        mock_meta.return_value = {
            "nazwaPliku": "Bjwk_SF_za_2024.xml",
            "czyMSR": False,
            "czyKorekta": False,
            "dataDodania": "2025-05-20",
        }

        mock_dl.return_value = test_zip

        yield {
            "lookup": mock_lookup,
            "search": mock_search,
            "metadata": mock_meta,
            "download": mock_dl,
        }


@pytest.mark.asyncio
async def test_full_scraper_flow(temp_env, mock_rdf):
    """End-to-end: import KRS, run scraper, verify DB and extracted files."""
    from app.scraper.job import run_scraper

    # Import a KRS
    db.connect()
    db.upsert_krs("0000694720", None, None, True)
    db.close()

    # Run scraper
    stats = await run_scraper(mode="specific_krs", specific_krs=["694720"])

    assert stats["krs_checked"] == 1
    assert stats["documents_discovered"] >= 1
    assert stats["documents_downloaded"] >= 1
    assert stats["bytes_downloaded"] > 0

    # Verify DB state
    db.connect()
    s = db.get_stats()
    assert s["total_krs"] >= 1
    assert s["total_downloaded"] >= 1

    last_run = db.get_last_run()
    assert last_run is not None
    assert last_run["status"] == "completed"
    db.close()

    # Verify extracted files on disk (NOT a .zip - raw XML + manifest)
    storage = LocalStorage(str(temp_env / "documents"))
    doc_dir = "krs/0000694720/ZgsX8Fsncb1PFW07-T4XoQ"

    assert storage.exists(doc_dir)
    files = storage.list_files(doc_dir)
    assert "Bjwk_SF_za_2024.xml" in files
    assert "manifest.json" in files

    # Verify the XML was extracted correctly (not still zipped)
    xml_bytes = storage.read(f"{doc_dir}/Bjwk_SF_za_2024.xml")
    assert xml_bytes.startswith(b"<?xml")
    assert b"JednostkaInna" in xml_bytes

    # Verify manifest content
    manifest_bytes = storage.read(f"{doc_dir}/manifest.json")
    manifest = json.loads(manifest_bytes)
    assert manifest["document_id"] == "ZgsX8Fsncb1PFW07-T4XoQ=="
    assert manifest["source_zip_size"] > 0
    assert len(manifest["files"]) == 1
    assert manifest["files"][0]["type"] == "xml"
```

Run:
```bash
pytest tests/test_scraper_integration.py -v
```

---

## Acceptance checklist

Before you say "done", verify ALL of these:

```bash
# 1. All existing tests still pass
pytest tests/test_crypto.py tests/test_endpoints.py -v

# 2. New unit tests pass
pytest tests/test_scraper_db.py tests/test_storage.py -v

# 3. Integration test passes
pytest tests/test_scraper_integration.py -v

# 4. CLI works end-to-end with a real KRS (manual test)
python -m app.scraper.cli import-range --start 694720 --end 694720
python -m app.scraper.cli run --krs 694720 -v
python -m app.scraper.cli status

# 5. API still works with scraper status endpoint
uvicorn app.main:app --reload --port 8000
# Then in another terminal:
curl http://localhost:8000/health
curl http://localhost:8000/api/scraper/status

# 6. Downloaded files are extracted (NOT zipped) on disk
ls -la data/documents/krs/0000694720/
# Should show directories (one per document), each containing:
#   - original files (xml, xhtml, pdf)
#   - manifest.json
find data/documents/krs/0000694720/ -name "manifest.json" -exec cat {} \;
# Verify manifest shows file types and sizes
find data/documents/krs/0000694720/ -name "*.xml" | head -5
# XMLs should be readable directly (not zipped)

# 7. DuckDB has correct data
python -c "
import duckdb
conn = duckdb.connect('data/scraper.duckdb')
print(conn.sql('SELECT * FROM krs_registry').df())
print(conn.sql('SELECT * FROM krs_documents').df())
print(conn.sql('SELECT * FROM scraper_runs').df())
conn.close()
"
```

---

## Things NOT to do

- Do NOT modify existing routers (`rdf/podmiot.py`, `rdf/dokumenty.py`, `analysis/routes.py`) except to register the new scraper router in `main.py`
- Do NOT add DuckDB to the FastAPI lifespan - the scraper manages its own connection
- Do NOT implement GCS storage - just the protocol + LocalStorage
- Do NOT add any async to DuckDB operations - synchronous is fine
- Do NOT use SQLAlchemy, alembic, or any ORM - raw DuckDB SQL
- Do NOT add new dependencies beyond `duckdb`, `aiofiles`, `click`
- Do NOT rename or restructure existing files
- Do NOT change the existing test files
