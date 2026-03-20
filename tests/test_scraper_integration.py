"""End-to-end integration test for the scraper module with mocked rdf_client."""
import io
import json
import zipfile
from unittest.mock import AsyncMock, patch

import pytest

from app.config import settings
from app.scraper import db as scraper_db
from app.scraper.storage import LocalStorage


def _make_test_zip(files: dict) -> bytes:
    """Create a real in-memory ZIP archive."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        for name, data in files.items():
            zf.writestr(name, data)
    return buf.getvalue()


@pytest.fixture
def temp_env(tmp_path, monkeypatch):
    """Override DB and storage paths to temp dirs."""
    monkeypatch.setattr(settings, "scraper_db_path", str(tmp_path / "test.duckdb"))
    monkeypatch.setattr(settings, "storage_local_path", str(tmp_path / "documents"))
    monkeypatch.setattr(settings, "scraper_delay_between_krs", 0.0)
    monkeypatch.setattr(settings, "scraper_delay_between_requests", 0.0)
    scraper_db._conn = None
    yield tmp_path
    scraper_db._conn = None


@pytest.fixture
def mock_rdf():
    """Mock all rdf_client functions."""
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
    scraper_db.connect()
    scraper_db.upsert_krs("0000694720", None, None, True)
    scraper_db.close()

    # Run scraper
    stats = await run_scraper(mode="specific_krs", specific_krs=["694720"])

    assert stats["krs_checked"] == 1
    assert stats["documents_discovered"] >= 1
    assert stats["documents_downloaded"] >= 1
    assert stats["bytes_downloaded"] > 0

    # Verify DB state
    scraper_db.connect()
    s = scraper_db.get_stats()
    assert s["total_krs"] >= 1
    assert s["total_downloaded"] >= 1

    last_run = scraper_db.get_last_run()
    assert last_run is not None
    assert last_run["status"] == "completed"
    scraper_db.close()

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
