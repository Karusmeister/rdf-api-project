"""Tests for batch/proxy_pool.py and batch/connections.ProxyRotator."""

import json
import tempfile
from pathlib import Path

import pytest

from batch.connections import Connection, DeadProxyRegistry, ProxyRotator
from batch.proxy_pool import (
    _BANNED_COUNTRIES,
    _COUNTRY_PRIORITY,
    _load_public_proxies,
    build_full_pool,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_proxies_json(entries: list[dict], tmp_path: Path) -> Path:
    path = tmp_path / "proxies.json"
    path.write_text(json.dumps(entries))
    return path


def _proxy_entry(ip: str, port: int, country: str, score: int = 1) -> dict:
    return {
        "proxy": f"socks5://{ip}:{port}",
        "protocol": "socks5",
        "ip": ip,
        "port": port,
        "https": False,
        "anonymity": "transparent",
        "score": score,
        "geolocation": {"country": country, "city": "Test"},
    }


# ---------------------------------------------------------------------------
# _load_public_proxies — filtering
# ---------------------------------------------------------------------------

def test_load_filters_banned_countries(tmp_path):
    entries = [
        _proxy_entry("1.1.1.1", 1000, "PL"),
        _proxy_entry("2.2.2.2", 2000, "RU"),
        _proxy_entry("3.3.3.3", 3000, "BY"),
        _proxy_entry("4.4.4.4", 4000, "KP"),
        _proxy_entry("5.5.5.5", 5000, "AF"),
        _proxy_entry("6.6.6.6", 6000, "ZZ"),
        _proxy_entry("7.7.7.7", 7000, "DE"),
    ]
    path = _make_proxies_json(entries, tmp_path)
    result = _load_public_proxies(path)
    countries = {c.name.split("/")[0] for c in result}
    assert "RU" not in countries
    assert "BY" not in countries
    assert "KP" not in countries
    assert "AF" not in countries
    assert "ZZ" not in countries
    assert "PL" in countries
    assert "DE" in countries
    assert len(result) == 2


def test_load_filters_non_socks5(tmp_path):
    entries = [
        {**_proxy_entry("1.1.1.1", 1000, "PL"), "protocol": "http"},
        _proxy_entry("2.2.2.2", 2000, "PL"),
    ]
    path = _make_proxies_json(entries, tmp_path)
    result = _load_public_proxies(path)
    assert len(result) == 1
    assert "2.2.2.2" in result[0].proxy_url


# ---------------------------------------------------------------------------
# _load_public_proxies — priority sorting
# ---------------------------------------------------------------------------

def test_load_sorts_by_country_priority(tmp_path):
    entries = [
        _proxy_entry("1.1.1.1", 1000, "ES"),   # priority 8
        _proxy_entry("2.2.2.2", 2000, "PL"),   # priority 0
        _proxy_entry("3.3.3.3", 3000, "DE"),   # priority 1
        _proxy_entry("4.4.4.4", 4000, "US"),   # priority 99
        _proxy_entry("5.5.5.5", 5000, "CZ"),   # priority 2
    ]
    path = _make_proxies_json(entries, tmp_path)
    result = _load_public_proxies(path)
    countries = [c.name.split("/")[0] for c in result]
    assert countries == ["PL", "DE", "CZ", "ES", "US"]


def test_load_sorts_by_score_within_country(tmp_path):
    entries = [
        _proxy_entry("1.1.1.1", 1000, "PL", score=1),
        _proxy_entry("2.2.2.2", 2000, "PL", score=5),
        _proxy_entry("3.3.3.3", 3000, "PL", score=3),
    ]
    path = _make_proxies_json(entries, tmp_path)
    result = _load_public_proxies(path)
    # Higher score should come first (sorted by -score)
    ips = [c.proxy_url.split("//")[1].split(":")[0] for c in result]
    assert ips == ["2.2.2.2", "3.3.3.3", "1.1.1.1"]


def test_load_missing_file():
    result = _load_public_proxies(Path("/nonexistent/proxies.json"))
    assert result == []


def test_load_corrupt_json(tmp_path):
    path = tmp_path / "proxies.json"
    path.write_text("not valid json {{{")
    result = _load_public_proxies(path)
    assert result == []


def test_load_not_a_list(tmp_path):
    path = tmp_path / "proxies.json"
    path.write_text('{"proxies": []}')
    result = _load_public_proxies(path)
    assert result == []


def test_load_skips_malformed_entries(tmp_path):
    entries = [
        _proxy_entry("1.1.1.1", 1000, "PL"),
        {"protocol": "socks5", "geolocation": {"country": "DE"}},  # missing ip/port
        {"protocol": "socks5"},  # missing geolocation entirely
        _proxy_entry("2.2.2.2", 2000, "DE"),
    ]
    path = _make_proxies_json(entries, tmp_path)
    result = _load_public_proxies(path)
    assert len(result) == 2
    ips = [c.proxy_url.split("//")[1].split(":")[0] for c in result]
    assert "1.1.1.1" in ips
    assert "2.2.2.2" in ips


# ---------------------------------------------------------------------------
# build_full_pool
# ---------------------------------------------------------------------------

def test_build_full_pool_direct_is_last(tmp_path, monkeypatch):
    monkeypatch.setattr("batch.proxy_pool.settings.nordvpn_servers", [])
    path = _make_proxies_json([], tmp_path)
    pool = build_full_pool(path)
    assert pool[-1].name == "direct"
    assert pool[-1].proxy_url is None


def test_build_full_pool_nordvpn_first_then_public_then_direct(tmp_path, monkeypatch):
    monkeypatch.setattr("batch.proxy_pool.settings.nordvpn_servers", ["test.nordhold.net"])
    monkeypatch.setattr("batch.proxy_pool.settings.nordvpn_username", "u")
    monkeypatch.setattr("batch.proxy_pool.settings.nordvpn_password", "p")
    entries = [_proxy_entry("1.1.1.1", 1000, "PL")]
    path = _make_proxies_json(entries, tmp_path)
    pool = build_full_pool(path, include_public=True, run_preflight=False)
    assert "nordvpn" in pool[0].name  # NordVPN first
    assert "PL" in pool[1].name       # public second
    assert pool[-1].name == "direct"   # direct last


def test_build_full_pool_public_proxies_off_by_default(tmp_path, monkeypatch):
    monkeypatch.setattr("batch.proxy_pool.settings.nordvpn_servers", [])
    monkeypatch.setattr("batch.proxy_pool.settings.batch_use_public_proxies", False)
    entries = [_proxy_entry("1.1.1.1", 1000, "PL")]
    path = _make_proxies_json(entries, tmp_path)
    pool = build_full_pool(path, run_preflight=False)
    # Only direct, no public proxies
    assert len(pool) == 1
    assert pool[0].name == "direct"


def test_build_full_pool_public_proxies_opt_in(tmp_path, monkeypatch):
    monkeypatch.setattr("batch.proxy_pool.settings.nordvpn_servers", [])
    entries = [_proxy_entry("1.1.1.1", 1000, "PL")]
    path = _make_proxies_json(entries, tmp_path)
    pool = build_full_pool(path, include_public=True, run_preflight=False)
    assert len(pool) == 2
    assert "PL" in pool[0].name
    assert pool[-1].name == "direct"


# ---------------------------------------------------------------------------
# Strict VPN-only mode (allow_direct_fallback=False)
# ---------------------------------------------------------------------------

def test_strict_mode_excludes_direct(tmp_path, monkeypatch):
    monkeypatch.setattr("batch.proxy_pool.settings.nordvpn_servers", ["test.nordhold.net"])
    monkeypatch.setattr("batch.proxy_pool.settings.nordvpn_username", "u")
    monkeypatch.setattr("batch.proxy_pool.settings.nordvpn_password", "p")
    path = _make_proxies_json([], tmp_path)
    pool = build_full_pool(path, run_preflight=False, allow_direct_fallback=False)
    names = [c.name for c in pool]
    assert "direct" not in names
    assert any("nordvpn" in n for n in names)


def test_strict_mode_raises_when_pool_empty(tmp_path, monkeypatch):
    monkeypatch.setattr("batch.proxy_pool.settings.nordvpn_servers", [])
    path = _make_proxies_json([], tmp_path)
    with pytest.raises(RuntimeError, match="BATCH_REQUIRE_VPN_ONLY"):
        build_full_pool(path, run_preflight=False, allow_direct_fallback=False)


def test_strict_mode_with_public_proxies(tmp_path, monkeypatch):
    monkeypatch.setattr("batch.proxy_pool.settings.nordvpn_servers", [])
    entries = [_proxy_entry("1.1.1.1", 1000, "PL")]
    path = _make_proxies_json(entries, tmp_path)
    pool = build_full_pool(
        path, include_public=True, run_preflight=False, allow_direct_fallback=False,
    )
    assert len(pool) == 1
    assert "PL" in pool[0].name
    assert "direct" not in [c.name for c in pool]


# ---------------------------------------------------------------------------
# ProxyRotator
# ---------------------------------------------------------------------------

def test_rotator_current_returns_start_index():
    pool = [
        Connection(name="a", proxy_url="socks5://a:1"),
        Connection(name="b", proxy_url="socks5://b:1"),
        Connection(name="c", proxy_url="socks5://c:1"),
    ]
    r = ProxyRotator(pool, start_index=1)
    assert r.current.name == "b"


def test_rotator_success_resets_failures():
    pool = [Connection(name="a", proxy_url="socks5://a:1")]
    r = ProxyRotator(pool)
    r.record_failure()
    r.record_failure()
    r.record_success()
    # Should not rotate on next failure (counter was reset)
    assert r.record_failure() is None


def test_rotator_rotates_after_3_failures():
    pool = [
        Connection(name="a", proxy_url="socks5://a:1"),
        Connection(name="b", proxy_url="socks5://b:1"),
    ]
    r = ProxyRotator(pool, start_index=0, max_failures=3)
    r.record_failure()
    r.record_failure()
    new = r.record_failure()  # 3rd failure → rotate
    assert new is not None
    assert new.name == "b"
    assert r.current.name == "b"
    assert r.remaining == 1  # "a" was removed


def test_rotator_does_not_remove_direct():
    pool = [
        Connection(name="direct"),  # proxy_url=None
        Connection(name="a", proxy_url="socks5://a:1"),
    ]
    r = ProxyRotator(pool, start_index=0, max_failures=3)
    r.record_failure()
    r.record_failure()
    new = r.record_failure()  # 3rd failure on direct → advance, don't remove
    assert new is not None
    assert new.name == "a"
    assert r.remaining == 2  # direct is still in pool


def test_rotator_exhausted_when_all_proxies_removed():
    pool = [
        Connection(name="a", proxy_url="socks5://a:1"),
    ]
    r = ProxyRotator(pool, start_index=0, max_failures=3)
    r.record_failure()
    r.record_failure()
    r.record_failure()  # removes "a"
    assert r.exhausted


def test_rotator_cycles_through_multiple_proxies():
    pool = [
        Connection(name="a", proxy_url="socks5://a:1"),
        Connection(name="b", proxy_url="socks5://b:1"),
        Connection(name="c", proxy_url="socks5://c:1"),
    ]
    r = ProxyRotator(pool, start_index=0, max_failures=3)

    # Kill proxy a
    for _ in range(3):
        r.record_failure()
    assert r.current.name == "b"
    assert r.remaining == 2

    # Kill proxy b
    for _ in range(3):
        r.record_failure()
    assert r.current.name == "c"
    assert r.remaining == 1


def test_rotator_rotated_flag():
    pool = [
        Connection(name="a", proxy_url="socks5://a:1"),
        Connection(name="b", proxy_url="socks5://b:1"),
    ]
    r = ProxyRotator(pool, start_index=0, max_failures=3)
    assert r.rotated is False

    for _ in range(3):
        r.record_failure()
    assert r.rotated is True  # first read returns True
    assert r.rotated is False  # second read returns False (reset on read)


# ---------------------------------------------------------------------------
# DeadProxyRegistry + global eviction
# ---------------------------------------------------------------------------

def test_dead_proxy_registry_mark_and_check():
    from app.config import settings
    reg = DeadProxyRegistry(settings.database_url)
    # Clean up from previous runs
    from app.db.connection import make_connection
    conn = make_connection(settings.database_url)
    try:
        conn.execute("DELETE FROM dead_proxies WHERE proxy_name LIKE 'test-%%'")
    finally:
        conn.close()

    assert reg.is_dead("test-proxy-a") is False
    reg.mark_dead("test-proxy-a", worker_id=0)
    assert reg.is_dead("test-proxy-a") is True
    assert "test-proxy-a" in reg.get_all_dead()


def test_dead_proxy_registry_idempotent():
    from app.config import settings
    reg = DeadProxyRegistry(settings.database_url)
    reg.mark_dead("test-proxy-idem", worker_id=0)
    reg.mark_dead("test-proxy-idem", worker_id=1)  # should not raise
    assert reg.is_dead("test-proxy-idem") is True


def test_rotator_filters_already_dead_proxies_on_init():
    from app.config import settings
    reg = DeadProxyRegistry(settings.database_url)
    reg.mark_dead("test-dead-init", worker_id=0)

    pool = [
        Connection(name="test-dead-init", proxy_url="socks5://dead:1"),
        Connection(name="alive", proxy_url="socks5://alive:1"),
        Connection(name="direct"),
    ]
    r = ProxyRotator(pool, start_index=0, registry=reg)
    # "test-dead-init" should be excluded from pool
    assert r.remaining == 2
    names = {r._pool[i].name for i in range(r.remaining)}
    assert "test-dead-init" not in names
    assert "alive" in names
    assert "direct" in names


def test_rotator_publishes_to_registry_on_rotation():
    from app.config import settings
    reg = DeadProxyRegistry(settings.database_url)
    # Clean up
    from app.db.connection import make_connection
    conn = make_connection(settings.database_url)
    try:
        conn.execute("DELETE FROM dead_proxies WHERE proxy_name = 'test-kill-pub'")
    finally:
        conn.close()

    pool = [
        Connection(name="test-kill-pub", proxy_url="socks5://a:1"),
        Connection(name="backup", proxy_url="socks5://b:1"),
    ]
    r = ProxyRotator(pool, start_index=0, max_failures=3, registry=reg, worker_id=5)
    for _ in range(3):
        r.record_failure()
    # Should be published to global registry
    assert reg.is_dead("test-kill-pub") is True


def test_two_rotators_share_dead_proxy_state():
    """Worker A kills a proxy, Worker B sees it as dead."""
    from app.config import settings
    reg_a = DeadProxyRegistry(settings.database_url)
    reg_b = DeadProxyRegistry(settings.database_url)

    from app.db.connection import make_connection
    conn = make_connection(settings.database_url)
    try:
        conn.execute("DELETE FROM dead_proxies WHERE proxy_name = 'test-shared-kill'")
    finally:
        conn.close()

    pool = [
        Connection(name="test-shared-kill", proxy_url="socks5://a:1"),
        Connection(name="good", proxy_url="socks5://b:1"),
    ]

    # Worker A kills the proxy
    rot_a = ProxyRotator(pool, start_index=0, max_failures=3, registry=reg_a, worker_id=0)
    for _ in range(3):
        rot_a.record_failure()

    # Worker B creates a new rotator — dead proxy should be excluded
    rot_b = ProxyRotator(pool, start_index=0, registry=reg_b, worker_id=1)
    assert rot_b.remaining == 1
    assert rot_b.current.name == "good"


# ---------------------------------------------------------------------------
# Integration: KRS scanner _worker_loop with proxy rotation
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_krs_worker_retries_item_after_rotation(monkeypatch):
    """When rotation triggers, the failed KRS item is retried on the new proxy."""
    import batch.worker as worker_mod
    from batch.worker import _worker_loop
    from batch.progress import ProgressStore

    async def _fake_sleep(_):
        pass
    monkeypatch.setattr("batch.worker.asyncio.sleep", _fake_sleep)
    # Limit scan range so the loop terminates quickly
    monkeypatch.setattr(worker_mod, "_MAX_KRS", 999992)

    call_count = {"n": 0}

    import httpx
    import respx
    from app.config import settings

    rdf_base = settings.rdf_base_url

    def _side_effect(request):
        call_count["n"] += 1
        if call_count["n"] <= 3:
            raise httpx.ConnectError("proxy dead")
        # After rotation: succeed with "not_found" (empty entity)
        return httpx.Response(200, json={})

    pool = [
        Connection(name="bad-proxy", proxy_url="socks5://bad:1"),
        Connection(name="good-proxy", proxy_url="socks5://good:1"),
    ]

    dsn = settings.database_url

    # Clean up test KRS from any previous run
    from app.db.connection import make_connection
    conn = make_connection(dsn)
    try:
        for k in range(999990, 999993):
            conn.execute("DELETE FROM batch_progress WHERE krs = %s", [k])
    finally:
        conn.close()

    with respx.mock:
        respx.post(f"{rdf_base}/podmioty/wyszukiwanie/dane-podstawowe").mock(
            side_effect=_side_effect
        )
        await _worker_loop(
            worker_id=0,
            start_krs=999990,
            stride=1,
            connection=pool[0],
            concurrency=1,
            delay=0,
            dsn=dsn,
            proxy_pool=pool,
        )

    store = ProgressStore(dsn, init_schema=False)
    # The item that triggered rotation (999990) must be processed, not dropped
    assert store.is_done(999990), "KRS 999990 should be processed (not dropped)"


# ---------------------------------------------------------------------------
# Integration: RDF _worker_loop with proxy rotation
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_rdf_worker_retries_item_after_rotation(monkeypatch):
    """When rotation triggers in RDF worker, failed KRS is retried on new proxy."""
    from batch.rdf_worker import _worker_loop
    from batch.rdf_progress import RdfProgressStore

    async def _fake_sleep(_):
        pass
    monkeypatch.setattr("batch.rdf_worker.asyncio.sleep", _fake_sleep)

    call_count = {"n": 0}

    import httpx
    import respx
    from app.config import settings

    rdf_base = settings.rdf_base_url

    def _side_effect(request):
        call_count["n"] += 1
        if call_count["n"] <= 3:
            raise httpx.ConnectError("proxy dead")
        # After rotation: return empty result (valid response)
        return httpx.Response(200, json={
            "content": [],
            "metadaneWynikow": {
                "numerStrony": 0, "rozmiarStrony": 100,
                "liczbaStron": 0, "calkowitaLiczbaObiektow": 0,
            },
        })

    pool = [
        Connection(name="bad-proxy", proxy_url="socks5://bad:1"),
        Connection(name="good-proxy", proxy_url="socks5://good:1"),
    ]

    dsn = settings.database_url

    # Seed a KRS that needs processing in batch_progress
    from app.db.connection import make_connection
    conn = make_connection(dsn)
    try:
        conn.execute("""
            INSERT INTO batch_progress (krs, status, worker_id)
            VALUES (999999, 'found', 0)
            ON CONFLICT (krs) DO UPDATE SET status = 'found'
        """)
        conn.execute(
            "DELETE FROM batch_rdf_progress WHERE krs = '0000999999'"
        )
    finally:
        conn.close()

    with respx.mock:
        respx.post(f"{rdf_base}/dokumenty/wyszukiwanie").mock(
            side_effect=_side_effect
        )
        await _worker_loop(
            worker_id=0,
            total_workers=1,
            connection=pool[0],
            concurrency=1,
            delay=0,
            download_delay=0,
            page_size=100,
            dsn=dsn,
            skip_metadata=True,
            proxy_pool=pool,
        )

    progress = RdfProgressStore(dsn)
    assert progress.is_done("0000999999"), "KRS 0000999999 should be processed (not dropped)"
