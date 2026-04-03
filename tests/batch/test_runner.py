"""Tests for batch/runner.py — multiprocessing orchestrator and CLI."""

import subprocess
import sys
from unittest.mock import patch, MagicMock

import pytest

from app.config import settings
from batch.connections import Connection, build_pool
from batch.runner import _pick_connection, _validate_vpn_config, run_batch, _build_parser


# ---------------------------------------------------------------------------
# _pick_connection
# ---------------------------------------------------------------------------

def test_pick_connection_direct_when_vpn_disabled(monkeypatch):
    monkeypatch.setattr(settings, "nordvpn_servers", [])
    conn = _pick_connection(0, use_vpn=False)
    assert conn.name == "direct"
    assert conn.proxy_url is None


def test_pick_connection_vpn_round_robin(monkeypatch):
    monkeypatch.setattr(settings, "nordvpn_username", "u")
    monkeypatch.setattr(settings, "nordvpn_password", "p")
    monkeypatch.setattr(settings, "nordvpn_servers", ["pl1", "pl2"])
    # Pool = [direct, pl1, pl2] — round-robin across all 3
    c0 = _pick_connection(0, use_vpn=True)
    c1 = _pick_connection(1, use_vpn=True)
    c2 = _pick_connection(2, use_vpn=True)
    c3 = _pick_connection(3, use_vpn=True)  # wraps around
    assert c0.name == "direct"
    assert c1.name == "pl1"
    assert c2.name == "pl2"
    assert c3.name == "direct"  # extra worker falls back to direct


def test_pick_connection_vpn_no_servers_raises(monkeypatch):
    monkeypatch.setattr(settings, "nordvpn_servers", [])
    with pytest.raises(RuntimeError, match="NORDVPN_SERVERS is empty"):
        _pick_connection(0, use_vpn=True)


# ---------------------------------------------------------------------------
# _validate_vpn_config
# ---------------------------------------------------------------------------

def test_validate_vpn_missing_username(monkeypatch):
    monkeypatch.setattr(settings, "nordvpn_username", "")
    monkeypatch.setattr(settings, "nordvpn_password", "pass")
    monkeypatch.setattr(settings, "nordvpn_servers", ["pl1"])
    with pytest.raises(RuntimeError, match="NORDVPN_USERNAME is empty"):
        _validate_vpn_config()


def test_validate_vpn_missing_password(monkeypatch):
    monkeypatch.setattr(settings, "nordvpn_username", "user")
    monkeypatch.setattr(settings, "nordvpn_password", "")
    monkeypatch.setattr(settings, "nordvpn_servers", ["pl1"])
    with pytest.raises(RuntimeError, match="NORDVPN_PASSWORD is empty"):
        _validate_vpn_config()


def test_validate_vpn_missing_servers(monkeypatch):
    monkeypatch.setattr(settings, "nordvpn_username", "user")
    monkeypatch.setattr(settings, "nordvpn_password", "pass")
    monkeypatch.setattr(settings, "nordvpn_servers", [])
    with pytest.raises(RuntimeError, match="NORDVPN_SERVERS is empty"):
        _validate_vpn_config()


# ---------------------------------------------------------------------------
# CLI argument parsing
# ---------------------------------------------------------------------------

def test_cli_help():
    """python -m batch.runner --help exits cleanly."""
    result = subprocess.run(
        [sys.executable, "-m", "batch.runner", "--help"],
        capture_output=True, text=True,
    )
    assert result.returncode == 0
    assert "--start" in result.stdout
    assert "--workers" in result.stdout
    assert "--vpn" in result.stdout


def test_parser_defaults():
    parser = _build_parser()
    args = parser.parse_args([])
    assert args.start is None
    assert args.workers is None
    assert args.vpn is None
    assert args.concurrency is None
    assert args.delay is None
    assert args.db is None


def test_parser_all_flags():
    parser = _build_parser()
    args = parser.parse_args(["--start", "500", "--workers", "5", "--vpn", "--concurrency", "2", "--delay", "0.5", "--db", "postgresql://localhost/test"])
    assert args.start == 500
    assert args.workers == 5
    assert args.vpn is True
    assert args.concurrency == 2
    assert args.delay == 0.5
    assert args.db == "postgresql://localhost/test"


def test_parser_vpn_no_vpn_mutually_exclusive():
    parser = _build_parser()
    with pytest.raises(SystemExit):
        parser.parse_args(["--vpn", "--no-vpn"])


# ---------------------------------------------------------------------------
# run_batch — mock worker processes
# ---------------------------------------------------------------------------

@patch("batch.runner.EntityStore")
@patch("batch.runner.ProgressStore")
@patch("batch.runner.multiprocessing.Process")
def test_run_batch_spawns_correct_workers(mock_process_cls, _mock_ps, _mock_es, monkeypatch):
    """run_batch spawns N workers with correct stride offsets."""
    monkeypatch.setattr(settings, "nordvpn_servers", [])
    mock_proc = MagicMock()
    mock_proc.pid = 12345
    mock_proc.exitcode = 0
    mock_proc.is_alive.return_value = False  # process exits normally
    mock_process_cls.return_value = mock_proc

    run_batch(start_krs=100, workers=3, use_vpn=False, dsn="postgresql://localhost:5432/rdf_test")

    assert mock_process_cls.call_count == 3
    # Verify stride offsets
    calls = mock_process_cls.call_args_list
    for i, call in enumerate(calls):
        kwargs = call.kwargs["kwargs"]
        assert kwargs["start_krs"] == 100 + i
        assert kwargs["stride"] == 3
        assert kwargs["worker_id"] == i

    assert mock_proc.start.call_count == 3
    assert mock_proc.join.call_count == 3


@patch("batch.runner.EntityStore")
@patch("batch.runner.ProgressStore")
@patch("batch.runner.multiprocessing.Process")
def test_run_batch_defaults_from_settings(mock_process_cls, _mock_ps, _mock_es, monkeypatch):
    """run_batch with no args uses settings defaults."""
    monkeypatch.setattr(settings, "nordvpn_servers", [])
    monkeypatch.setattr(settings, "batch_start_krs", 42)
    monkeypatch.setattr(settings, "batch_workers", 2)
    monkeypatch.setattr(settings, "batch_use_vpn", False)
    mock_proc = MagicMock()
    mock_proc.pid = 1
    mock_proc.exitcode = 0
    mock_process_cls.return_value = mock_proc

    run_batch()

    assert mock_process_cls.call_count == 2
    first_call_kwargs = mock_process_cls.call_args_list[0].kwargs["kwargs"]
    assert first_call_kwargs["start_krs"] == 42


def test_run_batch_vpn_validation_fails(monkeypatch):
    """run_batch with VPN enabled but no credentials raises."""
    monkeypatch.setattr(settings, "nordvpn_username", "")
    monkeypatch.setattr(settings, "nordvpn_password", "")
    monkeypatch.setattr(settings, "nordvpn_servers", [])
    with pytest.raises(RuntimeError, match="NORDVPN_USERNAME is empty"):
        run_batch(use_vpn=True)


@patch("batch.runner.EntityStore")
@patch("batch.runner.ProgressStore")
@patch("batch.runner.multiprocessing.Process")
def test_run_batch_raises_on_worker_crash(mock_process_cls, _mock_ps, _mock_es, monkeypatch):
    """run_batch raises SystemExit(1) when any worker exits non-zero."""
    monkeypatch.setattr(settings, "nordvpn_servers", [])
    mock_proc = MagicMock()
    mock_proc.pid = 1
    mock_proc.exitcode = 1  # simulate crash
    mock_proc.name = "krs-worker-0"
    mock_process_cls.return_value = mock_proc

    with pytest.raises(SystemExit):
        run_batch(start_krs=1, workers=1, use_vpn=False, dsn="postgresql://localhost:5432/rdf_test")
