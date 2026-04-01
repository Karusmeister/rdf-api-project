"""Tests for batch/metadata_runner.py — orchestrator failure semantics and defaults."""

import multiprocessing
from unittest.mock import MagicMock, patch

import pytest

from batch.metadata_runner import run_metadata_batch


def _make_mock_process(exitcode: int, name: str = "mock"):
    """Create a mock Process with the given exit code."""
    p = MagicMock()
    p.name = name
    p.exitcode = exitcode
    p.is_alive.return_value = False
    return p


# ---------------------------------------------------------------------------
# Exit-code propagation
# ---------------------------------------------------------------------------

def test_runner_exits_nonzero_on_worker_failure():
    """If any metadata worker exits non-zero, the runner raises SystemExit(1)."""
    failing_proc = _make_mock_process(exitcode=1, name="meta-backfill-0")

    with patch("batch.metadata_runner.multiprocessing.Process", return_value=failing_proc):
        with pytest.raises(SystemExit) as exc_info:
            run_metadata_batch(
                workers=1,
                use_vpn=False,
                concurrency=1,
                delay=0,
                dsn="postgresql://localhost:5432/rdf_test",
            )
        assert exc_info.value.code == 1

    failing_proc.start.assert_called_once()
    failing_proc.join.assert_called_once()


def test_runner_exits_zero_on_success():
    """Healthy all-green run completes without raising SystemExit."""
    procs = [_make_mock_process(exitcode=0, name=f"meta-backfill-{i}") for i in range(2)]
    proc_iter = iter(procs)

    with patch("batch.metadata_runner.multiprocessing.Process", side_effect=lambda **kw: next(proc_iter)):
        run_metadata_batch(
            workers=2,
            use_vpn=False,
            concurrency=1,
            delay=0,
            dsn="postgresql://localhost:5432/rdf_test",
        )

    for p in procs:
        p.start.assert_called_once()
        p.join.assert_called_once()


def test_runner_mixed_workers_fails_on_any_crash():
    """If one of N workers crashes, the runner still fails."""
    procs = [
        _make_mock_process(exitcode=1, name="meta-backfill-0"),
        _make_mock_process(exitcode=0, name="meta-backfill-1"),
        _make_mock_process(exitcode=0, name="meta-backfill-2"),
    ]
    proc_iter = iter(procs)

    with patch("batch.metadata_runner.multiprocessing.Process", side_effect=lambda **kw: next(proc_iter)):
        with pytest.raises(SystemExit) as exc_info:
            run_metadata_batch(
                workers=3,
                use_vpn=False,
                concurrency=1,
                delay=0,
                dsn="postgresql://localhost:5432/rdf_test",
            )
        assert exc_info.value.code == 1

    for p in procs:
        p.start.assert_called_once()
        p.join.assert_called_once()


# ---------------------------------------------------------------------------
# VPN fail-fast validation
# ---------------------------------------------------------------------------

def test_runner_raises_before_spawn_when_vpn_config_invalid():
    """Runner raises RuntimeError before spawning processes when VPN config is missing."""
    with patch("batch.metadata_runner.multiprocessing.Process") as mock_proc:
        with patch.object(
            __import__("app.config", fromlist=["settings"]).settings,
            "nordvpn_username", "",
        ):
            with pytest.raises(RuntimeError, match="NORDVPN_USERNAME"):
                run_metadata_batch(
                    workers=1,
                    use_vpn=True,
                    concurrency=1,
                    delay=0,
                    dsn="postgresql://localhost:5432/rdf_test",
                )

        # No processes should have been created
        mock_proc.assert_not_called()


def test_runner_succeeds_without_vpn_config_when_vpn_disabled():
    """Runner works fine with empty VPN config when use_vpn=False."""
    proc = _make_mock_process(exitcode=0, name="meta-backfill-0")

    with patch.object(
        __import__("app.config", fromlist=["settings"]).settings,
        "nordvpn_username", "",
    ):
        with patch("batch.metadata_runner.multiprocessing.Process", return_value=proc):
            # Should not raise — VPN validation is skipped
            run_metadata_batch(
                workers=1,
                use_vpn=False,
                concurrency=1,
                delay=0,
                dsn="postgresql://localhost:5432/rdf_test",
            )

    proc.start.assert_called_once()


def test_pick_connection_raises_when_vpn_but_no_servers():
    """_pick_connection raises RuntimeError when VPN requested but pool has no VPN entries."""
    from batch.metadata_runner import _pick_connection

    with patch("batch.metadata_runner.build_pool", return_value=[MagicMock(name="direct")]):
        with pytest.raises(RuntimeError, match="no VPN entries"):
            _pick_connection(worker_id=0, use_vpn=True)


# ---------------------------------------------------------------------------
# VPN default behavior (CLI)
# ---------------------------------------------------------------------------

def test_cli_vpn_enabled_by_default():
    """Default CLI behavior (no --no-vpn flag) keeps VPN enabled."""
    with patch("sys.argv", ["metadata_runner", "--db", "postgresql://x:5432/y"]):
        with patch("batch.metadata_runner.run_metadata_batch") as mock_run:
            from batch.metadata_runner import main
            try:
                main()
            except (RuntimeError, SystemExit):
                pass

            if mock_run.called:
                _, kwargs = mock_run.call_args
                assert kwargs["use_vpn"] is True


def test_cli_no_vpn_flag_disables_vpn():
    """--no-vpn flag explicitly disables VPN."""
    with patch("sys.argv", ["metadata_runner", "--no-vpn", "--db", "postgresql://x:5432/y"]):
        with patch("batch.metadata_runner.run_metadata_batch") as mock_run:
            from batch.metadata_runner import main
            try:
                main()
            except (RuntimeError, SystemExit):
                pass

            if mock_run.called:
                _, kwargs = mock_run.call_args
                assert kwargs["use_vpn"] is False
