"""Tests for batch/rdf_runner.py — RDF document discovery orchestrator."""

import pytest

from batch.rdf_runner import _build_parser, _pick_connection
from batch.connections import Connection


def test_pick_connection_no_vpn():
    conn = _pick_connection(worker_id=0, use_vpn=False)
    assert conn.name == "direct"
    assert conn.proxy_url is None


def test_pick_connection_no_vpn_any_worker():
    conn = _pick_connection(worker_id=3, use_vpn=False)
    assert conn.name == "direct"


def test_parser_defaults():
    parser = _build_parser()
    args = parser.parse_args([])
    assert args.workers is None
    assert args.concurrency is None
    assert args.delay is None
    assert args.page_size is None
    assert args.db is None


def test_parser_all_flags():
    parser = _build_parser()
    args = parser.parse_args([
        "--workers", "2",
        "--concurrency", "1",
        "--delay", "3.0",
        "--page-size", "50",
        "--db", "/tmp/test.duckdb",
        "--no-vpn",
    ])
    assert args.workers == 2
    assert args.concurrency == 1
    assert args.delay == 3.0
    assert args.page_size == 50
    assert args.db == "/tmp/test.duckdb"
    assert args.no_vpn is True


def test_parser_vpn_mutually_exclusive():
    parser = _build_parser()
    with pytest.raises(SystemExit):
        parser.parse_args(["--vpn", "--no-vpn"])
