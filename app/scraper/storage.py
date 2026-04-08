from __future__ import annotations

import asyncio
import io
import json
import logging
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Protocol

logger = logging.getLogger(__name__)


class StorageBackend(Protocol):
    def save_extracted(self, doc_dir: str, zip_bytes: bytes, document_id: str) -> dict:
        """Extract ZIP, save raw files + manifest. Returns manifest dict."""
        ...

    def exists(self, path: str) -> bool: ...

    def read(self, path: str) -> bytes: ...

    def list_files(self, dir_path: str) -> list: ...

    def get_full_path(self, path: str) -> str: ...


def safe_dirname(document_id: str) -> str:
    """Convert Base64 document ID to a filesystem-safe directory name."""
    return document_id.replace("+", "-").replace("/", "_").rstrip("=")


def make_doc_dir(krs: str, document_id: str) -> str:
    """Build relative directory path: krs/0000694720/ZgsX8Fsncb1PFW07-T4XoQ"""
    return f"krs/{krs.zfill(10)}/{safe_dirname(document_id)}"


def _classify_file(filename: str) -> str:
    """Return file type from extension.

    XAdES-signed XML files (.xml.xades, .xml.XAdES) are classified as 'xml'
    since they contain the actual financial statement inside a signature envelope.
    """
    lower = filename.lower()
    if lower.endswith((".xml.xades", ".xml.xades.xml")):
        return "xml"
    ext = filename.rsplit(".", 1)[-1].lower() if "." in filename else "unknown"
    if ext == "xades":
        return "xml"
    return ext


class GcsStorage:
    def __init__(self, bucket_name: str, prefix: str, project: str | None = None):
        from google.cloud import storage as gcs

        self._bucket_name = bucket_name
        self._prefix = prefix
        self._client = gcs.Client(project=project or "rdf-api-project")
        self._bucket = self._client.bucket(bucket_name)

    def _blob_path(self, path: str) -> str:
        return f"{self._prefix}{path}"

    def save_extracted(self, doc_dir: str, zip_bytes: bytes, document_id: str) -> dict:
        """Extract ZIP contents, upload to GCS, write manifest.json. Returns manifest dict."""
        files_info = []
        total_extracted_size = 0

        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
            for entry in zf.infolist():
                if entry.is_dir():
                    continue
                filename = Path(entry.filename).name
                if not filename:
                    continue

                data = zf.read(entry.filename)
                blob_key = self._blob_path(f"{doc_dir}/{filename}")
                blob = self._bucket.blob(blob_key)
                blob.upload_from_string(data)

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

        manifest_blob = self._bucket.blob(self._blob_path(f"{doc_dir}/manifest.json"))
        manifest_blob.upload_from_string(
            json.dumps(manifest, indent=2, ensure_ascii=False),
            content_type="application/json",
        )

        logger.info(
            "document_extracted",
            extra={
                "event": "document_extracted",
                "doc_dir": doc_dir,
                "document_id": document_id,
                "file_count": len(files_info),
                "zip_size": len(zip_bytes),
                "extracted_size": total_extracted_size,
            },
        )

        return manifest

    def exists(self, path: str) -> bool:
        return self._bucket.blob(self._blob_path(path)).exists()

    def read(self, path: str) -> bytes:
        return self._bucket.blob(self._blob_path(path)).download_as_bytes()

    def list_files(self, dir_path: str) -> list:
        prefix = self._blob_path(f"{dir_path}/")
        blobs = self._client.list_blobs(self._bucket, prefix=prefix)
        filenames = []
        for blob in blobs:
            name = blob.name[len(prefix):]
            # Skip entries in subdirectories
            if name and "/" not in name:
                filenames.append(name)
        return filenames

    def get_full_path(self, path: str) -> str:
        return f"gs://{self._bucket_name}/{self._blob_path(path)}"

    async def async_save_extracted(self, doc_dir: str, zip_bytes: bytes, document_id: str) -> dict:
        """Non-blocking version of save_extracted. Runs GCS I/O in a thread."""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            None, self.save_extracted, doc_dir, zip_bytes, document_id,
        )


class LocalStorage:
    def __init__(self, base_path: str):
        self._base = Path(base_path)
        self._base.mkdir(parents=True, exist_ok=True)

    def save_extracted(self, doc_dir: str, zip_bytes: bytes, document_id: str) -> dict:
        """Extract ZIP contents into doc_dir, write manifest.json. Returns manifest dict."""
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

        logger.info(
            "document_extracted",
            extra={
                "event": "document_extracted",
                "doc_dir": doc_dir,
                "document_id": document_id,
                "file_count": len(files_info),
                "zip_size": len(zip_bytes),
                "extracted_size": total_extracted_size,
            },
        )

        return manifest

    def exists(self, path: str) -> bool:
        return (self._base / path).exists()

    def read(self, path: str) -> bytes:
        return (self._base / path).read_bytes()

    def list_files(self, dir_path: str) -> list:
        target = self._base / dir_path
        if not target.is_dir():
            return []
        return [f.name for f in target.iterdir() if f.is_file()]

    def get_full_path(self, path: str) -> str:
        return str(self._base / path)

    async def async_save_extracted(self, doc_dir: str, zip_bytes: bytes, document_id: str) -> dict:
        """Non-blocking version of save_extracted. Runs I/O in a thread."""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            None, self.save_extracted, doc_dir, zip_bytes, document_id,
        )


def create_storage() -> LocalStorage | GcsStorage:
    from app.config import settings
    if settings.storage_backend == "gcs":
        return GcsStorage(settings.storage_gcs_bucket, settings.storage_gcs_prefix)
    return LocalStorage(settings.storage_local_path)
