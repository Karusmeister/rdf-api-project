"""Tests for company search and health metrics endpoints (PKR-124, PKR-121)."""

from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from app.main import app

client = TestClient(app, raise_server_exceptions=False)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _mock_conn():
    """Create a mock DB connection with chainable execute().fetchall()/fetchone()."""
    conn = MagicMock()
    return conn


# ---------------------------------------------------------------------------
# GET /api/companies/search — no auth required
# ---------------------------------------------------------------------------

class TestSearchCompanies:
    @patch("app.routers.companies.routes._search_cache", {})
    @patch("app.routers.companies.routes.db_conn")
    def test_name_search_returns_results(self, mock_db):
        mock_conn = MagicMock()
        mock_db.get_conn.return_value = mock_conn

        # Main query result
        mock_conn.execute.return_value.fetchall.side_effect = [
            # Search results
            [("0000694720", "Digital Software sp. z o.o.", "1234567890",
              "62.01.Z", "sp. z o.o.", "active", True)],
            # Count query
            [(1,)],
        ]
        mock_conn.execute.return_value.fetchone = MagicMock(return_value=(1,))

        resp = client.get("/api/companies/search?q=Digital")
        assert resp.status_code == 200
        data = resp.json()
        assert data["query"] == "Digital"
        assert len(data["results"]) == 1
        assert data["results"][0]["krs"] == "0000694720"
        assert data["results"][0]["has_predictions"] is True

    def test_search_rejects_short_query(self):
        resp = client.get("/api/companies/search?q=ab")
        assert resp.status_code == 422  # validation error (min_length=3)

    @patch("app.routers.companies.routes._search_cache", {})
    @patch("app.routers.companies.routes.db_conn")
    def test_numeric_search_normalizes_krs(self, mock_db):
        mock_conn = MagicMock()
        mock_db.get_conn.return_value = mock_conn
        mock_conn.execute.return_value.fetchall.side_effect = [
            [("0000694720", "Test Corp", None, None, "sp. z o.o.", "active", False)],
            [(1,)],
        ]
        mock_conn.execute.return_value.fetchone = MagicMock(return_value=(1,))

        resp = client.get("/api/companies/search?q=694720")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data["results"]) == 1
        assert data["results"][0]["krs"] == "0000694720"

    @patch("app.routers.companies.routes._search_cache", {})
    @patch("app.routers.companies.routes.db_conn")
    def test_full_krs_search(self, mock_db):
        mock_conn = MagicMock()
        mock_db.get_conn.return_value = mock_conn
        mock_conn.execute.return_value.fetchall.side_effect = [
            [("0000694720", "Test Corp", None, None, None, "active", True)],
            [(1,)],
        ]
        mock_conn.execute.return_value.fetchone = MagicMock(return_value=(1,))

        resp = client.get("/api/companies/search?q=0000694720")
        assert resp.status_code == 200
        data = resp.json()
        assert data["results"][0]["krs"] == "0000694720"

    @patch("app.routers.companies.routes._search_cache", {})
    @patch("app.routers.companies.routes.db_conn")
    def test_unpadded_numeric_uses_zfill_exact_not_suffix(self, mock_db):
        """Typing '694720' (no leading zero) → zero-pad to canonical '0000694720'."""
        mock_conn = MagicMock()
        mock_db.get_conn.return_value = mock_conn
        mock_conn.execute.return_value.fetchall.side_effect = [[], [(0,)]]
        mock_conn.execute.return_value.fetchone = MagicMock(return_value=(0,))

        client.get("/api/companies/search?q=694720")

        # Inspect the SQL parameters passed to the first execute call (search query)
        first_call_args = mock_conn.execute.call_args_list[0]
        params = first_call_args[0][1]  # second positional arg = params list
        # Unpadded input: zero-pad to canonical exact match, no wildcards
        like_param = params[0]
        assert like_param == "0000694720", f"Expected '0000694720', got '{like_param}'"
        assert "%694720" not in str(params), "Suffix wildcard '%694720' must not appear"

    @patch("app.routers.companies.routes._search_cache", {})
    @patch("app.routers.companies.routes.db_conn")
    def test_zero_prefixed_input_uses_canonical_prefix(self, mock_db):
        """Typing '000069' (starts with 0) → canonical prefix search '000069%'.
        The user is typing the zero-padded KRS format and wants prefix matching."""
        mock_conn = MagicMock()
        mock_db.get_conn.return_value = mock_conn
        mock_conn.execute.return_value.fetchall.side_effect = [[], [(0,)]]
        mock_conn.execute.return_value.fetchone = MagicMock(return_value=(0,))

        client.get("/api/companies/search?q=000069")

        first_call_args = mock_conn.execute.call_args_list[0]
        params = first_call_args[0][1]
        like_param = params[0]
        assert like_param == "000069%", f"Expected '000069%', got '{like_param}'"

    def test_search_respects_limit(self):
        resp = client.get("/api/companies/search?q=test&limit=0")
        assert resp.status_code == 422  # ge=1

        resp = client.get("/api/companies/search?q=test&limit=51")
        assert resp.status_code == 422  # le=50


# ---------------------------------------------------------------------------
# GET /api/companies/search/popular
# ---------------------------------------------------------------------------

class TestPopularCompanies:
    @patch("app.routers.companies.routes.db_conn")
    def test_returns_popular(self, mock_db):
        mock_conn = MagicMock()
        mock_db.get_conn.return_value = mock_conn
        mock_conn.execute.return_value.fetchall.return_value = [
            ("0000694720", "Digital Software", 15),
            ("0000000001", "Big Corp", 10),
        ]

        resp = client.get("/api/companies/search/popular")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data["results"]) == 2
        assert data["results"][0]["click_count"] == 15


# ---------------------------------------------------------------------------
# POST /api/companies/search/log-click
# ---------------------------------------------------------------------------

class TestLogClick:
    def test_log_click_returns_logged(self):
        resp = client.post("/api/companies/search/log-click?q=test&krs=694720")
        assert resp.status_code == 200
        assert resp.json()["status"] == "logged"

    def test_log_click_rejects_invalid_krs(self):
        resp = client.post("/api/companies/search/log-click?q=test&krs=abc")
        assert resp.status_code == 422


# ---------------------------------------------------------------------------
# GET /api/companies/{krs}/health-metrics
# ---------------------------------------------------------------------------

class TestHealthMetrics:
    @patch("app.routers.companies.routes.db_conn")
    def test_returns_metrics(self, mock_db):
        mock_conn = MagicMock()
        mock_db.get_conn.return_value = mock_conn
        mock_conn.execute.return_value.fetchall.return_value = [
            # fiscal_year, equity, total_assets, current_assets, short_liab, op_profit, revenue, net_profit
            (2022, 500000, 1000000, 300000, 50000, 150000, 800000, 100000),
            (2023, 600000, 1100000, 350000, 60000, 180000, 900000, 120000),
        ]

        resp = client.get("/api/companies/0000694720/health-metrics")
        assert resp.status_code == 200
        data = resp.json()
        assert data["krs"] == "0000694720"
        assert "equity_ratio" in data["metrics"]
        assert "current_ratio" in data["metrics"]
        assert "operating_margin" in data["metrics"]
        assert "net_margin" in data["metrics"]
        assert "revenue_growth" in data["metrics"]

        # Check equity_ratio computation: 600000/1100000 * 100 ≈ 54.55
        eq = data["metrics"]["equity_ratio"]
        assert eq["current_value"] == pytest.approx(54.55, abs=0.01)
        assert eq["label"] == "Silna baza kapitalowa"
        assert len(eq["history"]) == 2

        # Revenue growth: (900000-800000)/800000*100 = 12.5
        rg = data["metrics"]["revenue_growth"]
        assert rg["history"][0]["value"] is None  # first year has no prior
        assert rg["history"][1]["value"] == pytest.approx(12.5, abs=0.01)
        assert rg["label"] == "Wzrost"

    @patch("app.routers.companies.routes.db_conn")
    def test_404_when_no_data(self, mock_db):
        mock_conn = MagicMock()
        mock_db.get_conn.return_value = mock_conn
        mock_conn.execute.return_value.fetchall.return_value = []

        resp = client.get("/api/companies/0000694720/health-metrics")
        assert resp.status_code == 404

    @patch("app.routers.companies.routes.db_conn")
    def test_null_handling_for_missing_tags(self, mock_db):
        mock_conn = MagicMock()
        mock_db.get_conn.return_value = mock_conn
        # Year with missing current_assets (None)
        mock_conn.execute.return_value.fetchall.return_value = [
            (2023, 500000, 1000000, None, 50000, None, 800000, 100000),
        ]

        resp = client.get("/api/companies/0000694720/health-metrics")
        assert resp.status_code == 200
        data = resp.json()
        assert data["metrics"]["current_ratio"]["current_value"] is None
        assert data["metrics"]["operating_margin"]["current_value"] is None
