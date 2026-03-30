"""Tests for GcsStorage backend."""
from __future__ import annotations

import io
import json
import zipfile
from unittest.mock import MagicMock, patch

import pytest

from app.scraper.storage import GcsStorage, create_storage


def _make_zip(files: dict[str, bytes]) -> bytes:
    """Create a ZIP archive in memory from a dict of {filename: content}."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        for name, data in files.items():
            zf.writestr(name, data)
    return buf.getvalue()


@pytest.fixture
def mock_gcs():
    """Patch google.cloud.storage and return (mock_client, mock_bucket, storage)."""
    with patch("google.cloud.storage.Client") as mock_client_cls:
        mock_client = MagicMock()
        mock_client_cls.return_value = mock_client
        mock_bucket = MagicMock()
        mock_client.bucket.return_value = mock_bucket

        storage = GcsStorage(bucket_name="test-bucket", prefix="krs/")

        yield mock_client, mock_bucket, storage


class TestGcsStorageSaveExtracted:
    def test_uploads_extracted_files_and_manifest(self, mock_gcs):
        mock_client, mock_bucket, storage = mock_gcs
        mock_blob = MagicMock()
        mock_bucket.blob.return_value = mock_blob

        zip_bytes = _make_zip({
            "report.xml": b"<xml>data</xml>",
            "subdir/nested.pdf": b"%PDF-content",
        })

        manifest = storage.save_extracted(
            doc_dir="krs/0000000001/abc123",
            zip_bytes=zip_bytes,
            document_id="abc123==",
        )

        # Should have uploaded 2 files + 1 manifest = 3 blob uploads
        assert mock_blob.upload_from_string.call_count == 3

        # Check blob paths created
        blob_paths = [call.args[0] for call in mock_bucket.blob.call_args_list]
        assert "krs/krs/0000000001/abc123/report.xml" in blob_paths
        assert "krs/krs/0000000001/abc123/nested.pdf" in blob_paths
        assert "krs/krs/0000000001/abc123/manifest.json" in blob_paths

        # Check manifest structure
        assert manifest["document_id"] == "abc123=="
        assert manifest["source_zip_size"] == len(zip_bytes)
        assert len(manifest["files"]) == 2
        assert manifest["files"][0]["name"] == "report.xml"
        assert manifest["files"][0]["type"] == "xml"
        assert manifest["files"][1]["name"] == "nested.pdf"
        assert manifest["files"][1]["type"] == "pdf"

    def test_skips_directory_entries(self, mock_gcs):
        mock_client, mock_bucket, storage = mock_gcs
        mock_blob = MagicMock()
        mock_bucket.blob.return_value = mock_blob

        # Create ZIP with a directory entry
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as zf:
            zf.writestr("somedir/", "")  # directory entry
            zf.writestr("file.txt", b"content")
        zip_bytes = buf.getvalue()

        manifest = storage.save_extracted("dir", zip_bytes, "doc1")

        assert len(manifest["files"]) == 1
        assert manifest["files"][0]["name"] == "file.txt"


class TestGcsStorageExists:
    def test_exists_true(self, mock_gcs):
        _, mock_bucket, storage = mock_gcs
        mock_blob = MagicMock()
        mock_blob.exists.return_value = True
        mock_bucket.blob.return_value = mock_blob

        assert storage.exists("some/path.xml") is True
        mock_bucket.blob.assert_called_with("krs/some/path.xml")

    def test_exists_false(self, mock_gcs):
        _, mock_bucket, storage = mock_gcs
        mock_blob = MagicMock()
        mock_blob.exists.return_value = False
        mock_bucket.blob.return_value = mock_blob

        assert storage.exists("missing/file.xml") is False


class TestGcsStorageRead:
    def test_read_returns_bytes(self, mock_gcs):
        _, mock_bucket, storage = mock_gcs
        mock_blob = MagicMock()
        mock_blob.download_as_bytes.return_value = b"file-content"
        mock_bucket.blob.return_value = mock_blob

        result = storage.read("some/file.xml")

        assert result == b"file-content"
        mock_bucket.blob.assert_called_with("krs/some/file.xml")


class TestGcsStorageListFiles:
    def test_list_files_returns_filenames(self, mock_gcs):
        mock_client, mock_bucket, storage = mock_gcs

        # Simulate blobs returned by list_blobs
        blob1 = MagicMock()
        blob1.name = "krs/some/dir/file1.xml"
        blob2 = MagicMock()
        blob2.name = "krs/some/dir/file2.pdf"
        mock_client.list_blobs.return_value = [blob1, blob2]

        result = storage.list_files("some/dir")

        mock_client.list_blobs.assert_called_with(mock_bucket, prefix="krs/some/dir/")
        assert result == ["file1.xml", "file2.pdf"]

    def test_list_files_skips_subdirectory_entries(self, mock_gcs):
        mock_client, mock_bucket, storage = mock_gcs

        blob1 = MagicMock()
        blob1.name = "krs/dir/file.xml"
        blob_subdir = MagicMock()
        blob_subdir.name = "krs/dir/sub/nested.xml"
        mock_client.list_blobs.return_value = [blob1, blob_subdir]

        result = storage.list_files("dir")

        assert result == ["file.xml"]

    def test_list_files_empty(self, mock_gcs):
        mock_client, _, storage = mock_gcs
        mock_client.list_blobs.return_value = []

        assert storage.list_files("empty/dir") == []


class TestGcsStorageGetFullPath:
    def test_returns_gs_uri(self, mock_gcs):
        _, _, storage = mock_gcs
        result = storage.get_full_path("some/path/file.xml")
        assert result == "gs://test-bucket/krs/some/path/file.xml"


class TestCreateStorageGcs:
    def test_create_storage_returns_gcs_when_configured(self):
        with patch("google.cloud.storage.Client"):
            with patch("app.config.settings") as mock_settings:
                mock_settings.storage_backend = "gcs"
                mock_settings.storage_gcs_bucket = "my-bucket"
                mock_settings.storage_gcs_prefix = "prefix/"

                result = create_storage()

                assert isinstance(result, GcsStorage)

    def test_create_storage_returns_local_by_default(self, tmp_path):
        with patch("app.config.settings") as mock_settings:
            mock_settings.storage_backend = "local"
            mock_settings.storage_local_path = str(tmp_path)

            from app.scraper.storage import LocalStorage
            result = create_storage()

            assert isinstance(result, LocalStorage)
