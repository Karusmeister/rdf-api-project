"""Tests for the ActivityLogger service."""

from __future__ import annotations

import json
from contextlib import contextmanager
from unittest.mock import MagicMock

import pytest

from app.services.activity import ActivityLogger


class TestActivityLogger:
    """Unit tests for ActivityLogger."""

    def _make_logger(self, monkeypatch, *, enabled: bool = True) -> tuple[ActivityLogger, MagicMock]:
        """Return an ActivityLogger with a mocked get_db() context manager."""
        mock_conn = MagicMock()

        @contextmanager
        def fake_get_db():
            yield mock_conn

        monkeypatch.setattr("app.services.activity.get_db", fake_get_db)
        monkeypatch.setattr("app.services.activity.settings.activity_logging_enabled", enabled)
        return ActivityLogger(), mock_conn

    def test_log_inserts_row(self, monkeypatch):
        al, mock_conn = self._make_logger(monkeypatch)
        al.log("user-1", "krs_lookup", "0000012345", {"found": True}, "127.0.0.1")

        mock_conn.execute.assert_called_once()
        sql, params = mock_conn.execute.call_args[0]
        assert "INSERT INTO activity_log" in sql
        assert params[0] == "user-1"
        assert params[1] == "krs_lookup"
        assert params[2] == "0000012345"
        assert json.loads(params[3]) == {"found": True}
        assert params[4] == "127.0.0.1"

    def test_log_with_none_user(self, monkeypatch):
        al, mock_conn = self._make_logger(monkeypatch)
        al.log(None, "krs_lookup", "0000012345")

        _, params = mock_conn.execute.call_args[0]
        assert params[0] is None

    def test_log_with_none_detail(self, monkeypatch):
        al, mock_conn = self._make_logger(monkeypatch)
        al.log("user-1", "krs_lookup")

        _, params = mock_conn.execute.call_args[0]
        assert params[3] is None  # detail is None, not "null"

    def test_kill_switch_prevents_insert(self, monkeypatch):
        al, mock_conn = self._make_logger(monkeypatch, enabled=False)
        al.log("user-1", "krs_lookup", "0000012345")

        mock_conn.execute.assert_not_called()

    def test_exception_does_not_propagate(self, monkeypatch):
        al, mock_conn = self._make_logger(monkeypatch)
        mock_conn.execute.side_effect = RuntimeError("DB down")

        # Should not raise
        al.log("user-1", "krs_lookup", "0000012345")

    def test_uses_pooled_connection_via_get_db(self, monkeypatch):
        """Verify that activity logger obtains connection via get_db, not get_conn."""
        mock_conn = MagicMock()
        get_db_called = [False]

        @contextmanager
        def fake_get_db():
            get_db_called[0] = True
            yield mock_conn

        monkeypatch.setattr("app.services.activity.get_db", fake_get_db)
        monkeypatch.setattr("app.services.activity.settings.activity_logging_enabled", True)

        al = ActivityLogger()
        al.log("user-1", "krs_lookup", "0000012345")

        assert get_db_called[0], "ActivityLogger should use get_db() context manager"
        mock_conn.execute.assert_called_once()
