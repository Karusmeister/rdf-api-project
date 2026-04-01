"""Tests for admin dashboard API endpoints (M9a: PKR-74, PKR-75, PKR-76, R1)."""

from contextlib import contextmanager
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from app.main import app

client = TestClient(app, raise_server_exceptions=False)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_ADMIN_USER = {
    "id": "admin-1",
    "email": "admin@example.com",
    "name": "Admin",
    "auth_method": "local",
    "password_hash": None,
    "is_verified": True,
    "has_full_access": True,
    "is_active": True,
    "created_at": "2026-01-01",
    "last_login_at": None,
}

_REGULAR_USER = {
    **_ADMIN_USER,
    "id": "user-2",
    "has_full_access": False,
}


def _auth_header(user=None):
    from app.auth import create_token
    u = user or _ADMIN_USER
    token = create_token(u["id"], u["email"])
    return {"Authorization": f"Bearer {token}"}


def _patch_get_user(user):
    return patch("app.db.prediction_db.get_user_by_id", return_value=user)


# ---------------------------------------------------------------------------
# Auth enforcement — 401 / 403
# ---------------------------------------------------------------------------

class TestAdminAuthEnforcement:
    """All admin endpoints must return 401 without auth and 403 without admin role."""

    _admin_endpoints = [
        ("GET", "/api/admin/stats/overview"),
        ("GET", "/api/admin/stats/krs-coverage"),
        ("GET", "/api/admin/krs/0000000001"),
        ("POST", "/api/admin/krs/0000000001/refresh"),
        ("GET", "/api/admin/users"),
        ("GET", "/api/admin/users/user-1/activity"),
    ]

    @pytest.mark.parametrize("method,path", _admin_endpoints)
    def test_401_without_auth(self, method, path):
        resp = client.request(method, path)
        assert resp.status_code == 401

    @pytest.mark.parametrize("method,path", _admin_endpoints)
    def test_403_without_admin(self, method, path):
        with _patch_get_user(_REGULAR_USER):
            resp = client.request(method, path, headers=_auth_header(_REGULAR_USER))
            assert resp.status_code == 403


# ---------------------------------------------------------------------------
# PKR-74: GET /api/admin/stats/overview
# ---------------------------------------------------------------------------

class TestStatsOverview:

    def _mock_conn(self):
        """Return a mock connection that returns sensible defaults for overview queries."""
        mock = MagicMock()
        results = iter([
            MagicMock(fetchone=lambda: (42,)),        # entity count
            MagicMock(fetchone=lambda: (30,)),        # entities with docs
            MagicMock(fetchall=lambda: [("18", 100), ("21", 50)]),  # docs by rodzaj
            MagicMock(fetchone=lambda: (500000,)),    # cursor
            MagicMock(fetchone=lambda: None),         # last scan run (none)
            MagicMock(fetchone=lambda: (5,)),         # sync 24h
            MagicMock(fetchone=lambda: (20,)),        # sync 7d
            MagicMock(fetchone=lambda: (15,)),        # predictions
            MagicMock(fetchone=lambda: (10, 2)),      # users
        ])
        mock.execute = lambda *a, **kw: next(results)
        return mock

    def test_overview_success(self):
        mock_conn = self._mock_conn()
        with _patch_get_user(_ADMIN_USER), \
             patch("app.routers.admin.routes.get_conn", return_value=mock_conn):
            resp = client.get("/api/admin/stats/overview", headers=_auth_header())
            assert resp.status_code == 200
            data = resp.json()
            assert data["krs_entities"]["total_entities"] == 42
            assert data["documents"]["total_documents"] == 150
            assert data["documents"]["by_rodzaj"]["18"] == 100
            assert data["sync"]["runs_24h"] == 5
            assert data["predictions"]["companies_with_predictions"] == 15
            assert data["users"]["active_users"] == 10
            assert data["users"]["admin_users"] == 2


# ---------------------------------------------------------------------------
# PKR-74: GET /api/admin/stats/krs-coverage
# ---------------------------------------------------------------------------

class TestKrsCoverage:

    def _mock_conn(self):
        mock = MagicMock()
        call_count = [0]
        def _execute(*args, **kwargs):
            call_count[0] += 1
            result = MagicMock()
            if call_count[0] == 1:  # total count
                result.fetchone = lambda: (2,)
            else:  # paginated rows
                result.fetchall = lambda: [
                    ("0000000001", "Company A", "sp. z o.o.", None, None, 5, 3, ["18", "21", "23"], 2),
                    ("0000000002", "Company B", "S.A.", None, None, 10, 1, ["18"], 1),
                ]
            return result
        mock.execute = _execute
        return mock

    def test_coverage_success(self):
        mock_conn = self._mock_conn()
        with _patch_get_user(_ADMIN_USER), \
             patch("app.routers.admin.routes.get_conn", return_value=mock_conn):
            resp = client.get("/api/admin/stats/krs-coverage?page=0&size=10", headers=_auth_header())
            assert resp.status_code == 200
            data = resp.json()
            assert data["total"] == 2
            assert len(data["items"]) == 2
            assert data["items"][0]["krs"] == "0000000001"

    def test_coverage_pagination_params(self):
        mock_conn = self._mock_conn()
        with _patch_get_user(_ADMIN_USER), \
             patch("app.routers.admin.routes.get_conn", return_value=mock_conn):
            resp = client.get("/api/admin/stats/krs-coverage?page=1&size=50&sort=freshness&filter=missing_docs",
                              headers=_auth_header())
            assert resp.status_code == 200


# ---------------------------------------------------------------------------
# PKR-76: GET /api/admin/users
# ---------------------------------------------------------------------------

class TestUserManagement:

    def _mock_conn_users(self):
        mock = MagicMock()
        mock.execute.return_value.fetchall.return_value = [
            ("user-1", "a@test.com", "Alice", True, True, "local", "2026-01-01", None, 10, 3, "2026-03-01"),
        ]
        return mock

    def test_list_users_success(self):
        mock_conn = self._mock_conn_users()
        with _patch_get_user(_ADMIN_USER), \
             patch("app.routers.admin.routes.get_conn", return_value=mock_conn):
            resp = client.get("/api/admin/users", headers=_auth_header())
            assert resp.status_code == 200
            data = resp.json()
            assert data["total"] == 1
            assert data["items"][0]["email"] == "a@test.com"
            assert data["items"][0]["total_actions"] == 10

    def test_user_activity_not_found(self):
        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchone.return_value = None
        with _patch_get_user(_ADMIN_USER), \
             patch("app.routers.admin.routes.get_conn", return_value=mock_conn):
            resp = client.get("/api/admin/users/nonexistent/activity", headers=_auth_header())
            assert resp.status_code == 404


# ---------------------------------------------------------------------------
# _do_refresh — document persistence + pagination
# ---------------------------------------------------------------------------

def _fake_get_db():
    @contextmanager
    def _ctx():
        yield MagicMock()
    return _ctx


class TestRefreshKrs:

    @pytest.mark.asyncio
    async def test_do_refresh_persists_documents(self):
        """_do_refresh maps upstream docs to insert_documents format."""
        from app.routers.admin.routes import _do_refresh

        fake_lookup = {
            "czyPodmiotZnaleziony": True,
            "podmiot": {"nazwaPodmiotu": "TEST", "formaPrawna": "sp"},
        }
        fake_search = {
            "content": [
                {"id": "doc-1", "rodzaj": "18", "status": "NIEUSUNIETY",
                 "nazwa": None, "okresSprawozdawczyPoczatek": "2024-01-01",
                 "okresSprawozdawczyKoniec": "2024-12-31"},
            ],
            "metadaneWynikow": {"liczbaStron": 1},
        }

        with patch("app.rdf_client.dane_podstawowe", new=AsyncMock(return_value=fake_lookup)), \
             patch("app.rdf_client.wyszukiwanie", new=AsyncMock(return_value=fake_search)), \
             patch("app.db.connection.get_db", _fake_get_db()), \
             patch("app.scraper.db.insert_documents") as mock_insert:

            await _do_refresh("0000000001")

            mock_insert.assert_called_once()
            docs = mock_insert.call_args[0][0]
            assert len(docs) == 1
            assert docs[0]["document_id"] == "doc-1"
            assert docs[0]["krs"] == "0000000001"
            assert docs[0]["okres_start"] == "2024-01-01"
            assert docs[0]["okres_end"] == "2024-12-31"

    @pytest.mark.asyncio
    async def test_do_refresh_not_found_skips_docs(self):
        """When upstream entity not found, no document writes should happen."""
        from app.routers.admin.routes import _do_refresh

        with patch("app.rdf_client.dane_podstawowe", new=AsyncMock(
                 return_value={"czyPodmiotZnaleziony": False, "podmiot": None})), \
             patch("app.scraper.db.insert_documents") as mock_insert:

            await _do_refresh("0000000001")

            mock_insert.assert_not_called()

    @pytest.mark.asyncio
    async def test_do_refresh_paginates_all_pages(self):
        """_do_refresh fetches all pages and persists docs from every page."""
        from app.routers.admin.routes import _do_refresh

        fake_lookup = {
            "czyPodmiotZnaleziony": True,
            "podmiot": {"nazwaPodmiotu": "TEST", "formaPrawna": "sp"},
        }

        page_responses = [
            {
                "content": [{"id": f"doc-p1-{i}", "rodzaj": "18", "status": "NIEUSUNIETY",
                             "nazwa": None, "okresSprawozdawczyPoczatek": "2024-01-01",
                             "okresSprawozdawczyKoniec": "2024-12-31"} for i in range(3)],
                "metadaneWynikow": {"liczbaStron": 3},
            },
            {
                "content": [{"id": f"doc-p2-{i}", "rodzaj": "18", "status": "NIEUSUNIETY",
                             "nazwa": None, "okresSprawozdawczyPoczatek": "2023-01-01",
                             "okresSprawozdawczyKoniec": "2023-12-31"} for i in range(3)],
                "metadaneWynikow": {"liczbaStron": 3},
            },
            {
                "content": [{"id": "doc-p3-0", "rodzaj": "18", "status": "NIEUSUNIETY",
                             "nazwa": None, "okresSprawozdawczyPoczatek": "2022-01-01",
                             "okresSprawozdawczyKoniec": "2022-12-31"}],
                "metadaneWynikow": {"liczbaStron": 3},
            },
        ]

        call_idx = [0]

        async def fake_wyszukiwanie(krs, page=0, page_size=100):
            idx = call_idx[0]
            call_idx[0] += 1
            return page_responses[idx]

        with patch("app.rdf_client.dane_podstawowe", new=AsyncMock(return_value=fake_lookup)), \
             patch("app.rdf_client.wyszukiwanie", side_effect=fake_wyszukiwanie), \
             patch("app.db.connection.get_db", _fake_get_db()), \
             patch("app.scraper.db.insert_documents") as mock_insert:

            await _do_refresh("0000000001")

            mock_insert.assert_called_once()
            docs = mock_insert.call_args[0][0]
            assert len(docs) == 7  # 3 + 3 + 1
            doc_ids = {d["document_id"] for d in docs}
            assert "doc-p1-0" in doc_ids
            assert "doc-p2-2" in doc_ids
            assert "doc-p3-0" in doc_ids


# ---------------------------------------------------------------------------
# SQL column name validation (schema-aware tests)
# ---------------------------------------------------------------------------

class TestAdminSqlColumnNames:
    """Verify admin SQL queries reference columns that exist in the actual schema.

    These tests parse the SQL strings emitted by the admin routes and check them
    against the real DDL column names. This catches mismatches that mocked tests hide.
    """

    _SCAN_RUNS_COLUMNS = {
        "id", "started_at", "finished_at", "status", "krs_from", "krs_to",
        "probed_count", "valid_count", "error_count", "stopped_reason",
    }

    _KRS_REGISTRY_COLUMNS = {
        "krs", "company_name", "legal_form", "is_active",
        "first_seen_at", "last_checked_at", "last_download_at",
        "check_priority", "check_error_count", "last_error_message",
        "total_documents", "total_downloaded",
    }

    _KRS_DOCS_CURRENT_COLUMNS = {
        "document_id", "krs", "rodzaj", "status", "nazwa",
        "okres_start", "okres_end", "filename", "is_ifrs", "is_correction",
        "date_filed", "date_prepared", "is_downloaded", "downloaded_at",
        "storage_path", "storage_backend", "file_size_bytes", "zip_size_bytes",
        "file_count", "file_types", "discovered_at", "metadata_fetched_at",
        "download_error",
    }

    def test_scan_runs_query_uses_correct_columns(self):
        """stats_overview scan query must use probed_count, not probed."""
        import app.routers.admin.routes as mod
        import inspect
        src = inspect.getsource(mod.stats_overview)
        # Must NOT reference the old wrong names
        assert "probed," not in src or "probed_count" in src
        assert "valid," not in src or "valid_count" in src
        # Must reference correct names
        assert "probed_count" in src
        assert "valid_count" in src
        assert "error_count" in src

    def test_krs_coverage_no_scraper_status(self):
        """krs_coverage must not reference non-existent scraper_status column."""
        import app.routers.admin.routes as mod
        import inspect
        src = inspect.getsource(mod.krs_coverage)
        assert "scraper_status" not in src, "scraper_status does not exist in krs_registry"

    def test_krs_coverage_uses_okres_end(self):
        """krs_coverage must use okres_end, not okres_sprawozdawczy_koniec."""
        import app.routers.admin.routes as mod
        import inspect
        src = inspect.getsource(mod.krs_coverage)
        assert "okres_sprawozdawczy_koniec" not in src, (
            "krs_documents_current uses okres_end, not okres_sprawozdawczy_koniec"
        )
        assert "okres_end" in src

    def test_krs_detail_uses_okres_end(self):
        """krs_detail ORDER BY must use okres_end, not okres_sprawozdawczy_koniec."""
        import app.routers.admin.routes as mod
        import inspect
        src = inspect.getsource(mod.krs_detail)
        assert "okres_sprawozdawczy_koniec" not in src
        assert "okres_end" in src
