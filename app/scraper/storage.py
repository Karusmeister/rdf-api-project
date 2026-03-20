from __future__ import annotations

import io
import json
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Protocol


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
    """Return file type from extension."""
    ext = filename.rsplit(".", 1)[-1].lower() if "." in filename else "unknown"
    return ext


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


def create_storage() -> LocalStorage:
    from app.config import settings
    if settings.storage_backend == "gcs":
        raise NotImplementedError("GCS backend not yet implemented. Set STORAGE_BACKEND=local")
    return LocalStorage(settings.storage_local_path)
