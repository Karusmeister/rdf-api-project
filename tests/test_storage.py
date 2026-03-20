"""Tests for app/scraper/storage.py"""
import io
import json
import zipfile

import pytest

from app.scraper.storage import LocalStorage, safe_dirname, make_doc_dir


def make_test_zip(files: dict) -> bytes:
    """Create a ZIP in memory. files = {"filename.xml": b"content", ...}"""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        for name, data in files.items():
            zf.writestr(name, data)
    return buf.getvalue()


@pytest.fixture
def storage(tmp_path):
    return LocalStorage(str(tmp_path / "documents"))


def test_save_extracted_xml(storage, tmp_path):
    xml_content = b'<?xml version="1.0"?><root/>'
    zip_bytes = make_test_zip({"statement.xml": xml_content})
    manifest = storage.save_extracted("krs/0000000001/docABC", zip_bytes, "docABC==")

    assert storage.exists("krs/0000000001/docABC/statement.xml")
    assert storage.read("krs/0000000001/docABC/statement.xml") == xml_content
    assert manifest["files"][0]["type"] == "xml"


def test_save_extracted_pdf(storage, tmp_path):
    pdf_content = b"%PDF-1.4 fake pdf content"
    zip_bytes = make_test_zip({"report.pdf": pdf_content})
    manifest = storage.save_extracted("krs/0000000001/docPDF", zip_bytes, "docPDF==")

    assert storage.exists("krs/0000000001/docPDF/report.pdf")
    assert manifest["files"][0]["type"] == "pdf"


def test_save_extracted_multiple_files(storage):
    xml_content = b"<xml/>"
    pdf_content = b"%PDF"
    zip_bytes = make_test_zip({"doc.xml": xml_content, "doc.pdf": pdf_content})
    manifest = storage.save_extracted("krs/0000000001/docMULTI", zip_bytes, "docMULTI")

    assert len(manifest["files"]) == 2
    types = {f["type"] for f in manifest["files"]}
    assert types == {"xml", "pdf"}
    assert storage.exists("krs/0000000001/docMULTI/doc.xml")
    assert storage.exists("krs/0000000001/docMULTI/doc.pdf")


def test_manifest_content(storage):
    xml_content = b"<root>hello</root>"
    zip_bytes = make_test_zip({"file.xml": xml_content})
    manifest = storage.save_extracted("krs/0000000001/docMAN", zip_bytes, "docMAN==")

    assert manifest["document_id"] == "docMAN=="
    assert manifest["source_zip_size"] == len(zip_bytes)
    assert "extracted_at" in manifest
    assert len(manifest["files"]) == 1
    assert manifest["files"][0]["name"] == "file.xml"
    assert manifest["files"][0]["size"] == len(xml_content)

    # Also verify manifest.json was written to disk
    manifest_bytes = storage.read("krs/0000000001/docMAN/manifest.json")
    loaded = json.loads(manifest_bytes)
    assert loaded["document_id"] == "docMAN=="


def test_exists(storage):
    zip_bytes = make_test_zip({"x.xml": b"<x/>"})
    assert not storage.exists("krs/0000000001/docEXISTS")

    storage.save_extracted("krs/0000000001/docEXISTS", zip_bytes, "docEXISTS")

    assert storage.exists("krs/0000000001/docEXISTS")
    assert storage.exists("krs/0000000001/docEXISTS/x.xml")


def test_read(storage):
    content = b"<?xml version='1.0'?><Statement/>"
    zip_bytes = make_test_zip({"stmt.xml": content})
    storage.save_extracted("krs/0000000001/docREAD", zip_bytes, "docREAD")

    read_back = storage.read("krs/0000000001/docREAD/stmt.xml")
    assert read_back == content


def test_list_files(storage):
    zip_bytes = make_test_zip({"a.xml": b"<a/>", "b.xml": b"<b/>"})
    storage.save_extracted("krs/0000000001/docLIST", zip_bytes, "docLIST")

    files = storage.list_files("krs/0000000001/docLIST")
    assert "a.xml" in files
    assert "b.xml" in files
    assert "manifest.json" in files
    assert len(files) == 3


def test_safe_dirname():
    assert safe_dirname("ZgsX8Fsncb1PFW07-T4XoQ==") == "ZgsX8Fsncb1PFW07-T4XoQ"
    assert safe_dirname("abc+def/ghi=") == "abc-def_ghi"
    assert safe_dirname("plain") == "plain"
    assert safe_dirname("a+b/c+d/e==") == "a-b_c-d_e"


def test_make_doc_dir():
    result = make_doc_dir("694720", "ZgsX8Fsncb1PFW07-T4XoQ==")
    assert result == "krs/0000694720/ZgsX8Fsncb1PFW07-T4XoQ"

    result2 = make_doc_dir("0000694720", "ZgsX8Fsncb1PFW07-T4XoQ==")
    assert result2 == "krs/0000694720/ZgsX8Fsncb1PFW07-T4XoQ"


def test_zip_with_subdirectories(storage):
    """Files nested inside subdirs in the ZIP should be flattened."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("subdir/file.xml", b"<xml/>")
    zip_bytes = buf.getvalue()

    manifest = storage.save_extracted("krs/0000000001/docFLAT", zip_bytes, "docFLAT")

    assert storage.exists("krs/0000000001/docFLAT/file.xml")
    assert not storage.exists("krs/0000000001/docFLAT/subdir/file.xml")
    assert manifest["files"][0]["name"] == "file.xml"
