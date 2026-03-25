"""Tests for batch/runner.py — multiprocessing orchestrator and CLI."""

import subprocess
import sys
from unittest.mock import patch, MagicMock

import pytest

from app.config import settings
from batch.runner import run_batch, _build_parser


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


def test_parser_defaults():
    parser = _build_parser()
    args = parser.parse_args([])
    assert args.start is None
    assert args.workers is None
    assert args.concurrency is None
    assert args.delay is None
    assert args.db is None


def test_parser_all_flags():
    parser = _build_parser()
    args = parser.parse_args(["--start", "500", "--workers", "5", "--concurrency", "2", "--delay", "0.5", "--db", "/tmp/test.duckdb"])
    assert args.start == 500
    assert args.workers == 5
    assert args.concurrency == 2
    assert args.delay == 0.5
    assert args.db == "/tmp/test.duckdb"


# ---------------------------------------------------------------------------
# run_batch — mock worker processes
# ---------------------------------------------------------------------------

@patch("batch.runner.multiprocessing.Process")
def test_run_batch_spawns_correct_workers(mock_process_cls):
    """run_batch spawns N workers with correct stride offsets."""
    mock_proc = MagicMock()
    mock_proc.pid = 12345
    mock_proc.exitcode = 0
    mock_process_cls.return_value = mock_proc

    run_batch(start_krs=100, workers=3, db_path="/tmp/test.duckdb")

    assert mock_process_cls.call_count == 3
    calls = mock_process_cls.call_args_list
    for i, call in enumerate(calls):
        kwargs = call.kwargs["kwargs"]
        assert kwargs["start_krs"] == 100 + i
        assert kwargs["stride"] == 3
        assert kwargs["worker_id"] == i

    assert mock_proc.start.call_count == 3
    assert mock_proc.join.call_count == 3


@patch("batch.runner.multiprocessing.Process")
def test_run_batch_defaults_from_settings(mock_process_cls, monkeypatch):
    """run_batch with no args uses settings defaults."""
    monkeypatch.setattr(settings, "batch_start_krs", 42)
    monkeypatch.setattr(settings, "batch_workers", 2)
    mock_proc = MagicMock()
    mock_proc.pid = 1
    mock_proc.exitcode = 0
    mock_process_cls.return_value = mock_proc

    run_batch()

    assert mock_process_cls.call_count == 2
    first_call_kwargs = mock_process_cls.call_args_list[0].kwargs["kwargs"]
    assert first_call_kwargs["start_krs"] == 42


@patch("batch.runner.multiprocessing.Process")
def test_run_batch_raises_on_worker_crash(mock_process_cls):
    """run_batch raises SystemExit(1) when any worker exits non-zero."""
    mock_proc = MagicMock()
    mock_proc.pid = 1
    mock_proc.exitcode = 1  # simulate crash
    mock_proc.name = "krs-worker-0"
    mock_process_cls.return_value = mock_proc

    with pytest.raises(SystemExit):
        run_batch(start_krs=1, workers=1, db_path="/tmp/test.duckdb")
