"""Tests for batch/connections.py — connection pool and SOCKS5 URL building."""

import pytest

from app.config import settings
from batch.connections import Connection, build_pool


def test_pool_always_starts_with_direct(monkeypatch):
    monkeypatch.setattr(settings, "nordvpn_servers", [])
    pool = build_pool()
    assert pool[0].name == "direct"
    assert pool[0].proxy_url is None


def test_pool_includes_vpn_connections(monkeypatch):
    monkeypatch.setattr(settings, "nordvpn_username", "testuser")
    monkeypatch.setattr(settings, "nordvpn_password", "testpass")
    monkeypatch.setattr(settings, "nordvpn_servers", ["pl192", "de887"])
    pool = build_pool()
    assert len(pool) == 3
    assert pool[1].name == "pl192"
    assert pool[1].proxy_url == "socks5://testuser:testpass@pl192.nordvpn.com:1080"
    assert pool[2].name == "de887"
    assert pool[2].proxy_url == "socks5://testuser:testpass@de887.nordvpn.com:1080"


def test_pool_empty_servers_gives_only_direct(monkeypatch):
    monkeypatch.setattr(settings, "nordvpn_servers", [])
    pool = build_pool()
    assert len(pool) == 1


def test_connection_is_immutable():
    conn = Connection(name="direct")
    with pytest.raises(Exception):
        conn.name = "other"
