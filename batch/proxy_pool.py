"""Load, filter, and prioritize SOCKS5 proxies from proxies.json.

Builds a priority-ordered list of Connection objects:
  1. NordVPN proxies (authenticated, from settings)
  2. Public proxies from proxies.json, sorted by country priority

Banned countries (ZZ/unknown, RU, BY, KP, AF) are excluded.
Priority countries: PL, DE, CZ, SK, SE, NL, FR, AT, ES — then everything else.
"""

import asyncio
import json
import logging
import socket
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from batch.connections import Connection, _socks5_url

from app.config import settings

logger = logging.getLogger(__name__)

_BANNED_COUNTRIES = {"ZZ", "RU", "BY", "KP", "AF"}

# Lower number = higher priority
_COUNTRY_PRIORITY = {
    "PL": 0,
    "DE": 1,
    "CZ": 2,
    "SK": 3,
    "SE": 4,
    "NL": 5,
    "FR": 6,
    "AT": 7,
    "ES": 8,
}
_DEFAULT_PRIORITY = 99


def _load_public_proxies(path: Path | None = None) -> list[Connection]:
    """Load proxies.json, filter, sort by country priority."""
    if path is None:
        path = Path(__file__).resolve().parent.parent / "proxies.json"

    if not path.exists():
        logger.warning("proxies.json not found at %s — no public proxies loaded", path)
        return []

    try:
        with open(path) as f:
            raw = json.load(f)
    except (OSError, json.JSONDecodeError) as exc:
        logger.error("failed to load proxies.json: %s — no public proxies loaded", exc)
        return []

    if not isinstance(raw, list):
        logger.error("proxies.json root is not a list — no public proxies loaded")
        return []

    filtered: list[dict] = []
    skipped = 0
    for p in raw:
        try:
            if p.get("protocol") != "socks5":
                skipped += 1
                continue
            country = str(p.get("geolocation", {}).get("country", "ZZ")).upper()
            if country in _BANNED_COUNTRIES:
                skipped += 1
                continue
            # Validate required fields
            _ = p["ip"], p["port"]
            filtered.append(p)
        except (KeyError, TypeError):
            skipped += 1
            continue

    filtered.sort(
        key=lambda p: (
            _COUNTRY_PRIORITY.get(
                str(p["geolocation"]["country"]).upper(), _DEFAULT_PRIORITY,
            ),
            -p.get("score", 0),
        )
    )

    connections = []
    for p in filtered:
        country = str(p["geolocation"]["country"]).upper()
        city = p["geolocation"].get("city", "?")
        ip = p["ip"]
        port = p["port"]
        name = f"{country}/{city}/{ip}:{port}"
        proxy_url = f"socks5://{ip}:{port}"
        connections.append(Connection(name=name, proxy_url=proxy_url))

    logger.info(
        "loaded %d public proxies from %s (skipped %d banned/invalid/non-socks5)",
        len(connections), path.name, skipped,
    )
    return connections


_PREFLIGHT_TIMEOUT = 3.0  # seconds per proxy TCP connect check
_PREFLIGHT_WORKERS = 50   # concurrent health checks


def _check_proxy_reachable(conn: Connection) -> tuple[Connection, bool]:
    """TCP connect to proxy host:port. Returns (connection, reachable)."""
    if conn.proxy_url is None:
        return conn, True  # direct always reachable
    try:
        # Extract host:port from socks5://[user:pass@]host:port
        url = conn.proxy_url
        # Strip scheme
        hostport = url.split("://", 1)[1]
        # Strip credentials if present
        if "@" in hostport:
            hostport = hostport.split("@", 1)[1]
        host, port_str = hostport.rsplit(":", 1)
        port = int(port_str)

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(_PREFLIGHT_TIMEOUT)
        sock.connect((host, port))
        sock.close()
        return conn, True
    except (OSError, ValueError):
        return conn, False


def preflight_check(pool: list[Connection], dsn: str | None = None) -> list[Connection]:
    """Run TCP connect checks on all proxied connections in parallel.

    Unreachable proxies are removed from the pool and marked dead in
    the global registry (if dsn is provided).
    """
    # Separate direct (skip check) from proxied
    direct = [c for c in pool if c.proxy_url is None]
    proxied = [c for c in pool if c.proxy_url is not None]

    if not proxied:
        return pool

    logger.info("preflight_check starting for %d proxies...", len(proxied))

    alive: list[Connection] = []
    dead_names: list[str] = []

    with ThreadPoolExecutor(max_workers=_PREFLIGHT_WORKERS) as executor:
        results = executor.map(_check_proxy_reachable, proxied)
        for conn, reachable in results:
            if reachable:
                alive.append(conn)
            else:
                dead_names.append(conn.name)

    if dead_names:
        logger.info(
            "preflight_check removed %d unreachable proxies (%d alive)",
            len(dead_names), len(alive),
        )
        # Persist to global registry (batch insert for speed)
        if dsn:
            from batch.connections import DeadProxyRegistry
            try:
                registry = DeadProxyRegistry(dsn)
                registry.mark_dead_batch(dead_names, worker_id=-1)
            except Exception as exc:
                logger.warning("preflight_check could not persist dead proxies: %s", exc)
    else:
        logger.info("preflight_check all %d proxies reachable", len(alive))

    # Preserve original order: alive proxied + direct
    return alive + direct


def build_full_pool(
    proxies_path: Path | None = None,
    include_public: bool | None = None,
    dsn: str | None = None,
    run_preflight: bool = True,
    allow_direct_fallback: bool = True,
) -> list[Connection]:
    """Build the complete priority-ordered proxy pool.

    Order: NordVPN proxies → public proxies (by country priority) → direct.
    Direct is last so workers prefer proxied connections for geo-distribution,
    falling back to the VM's own IP only when all proxies are exhausted.

    When ``allow_direct_fallback`` is False (strict VPN mode), the direct
    connection is never added. If all proxies are dead/unreachable after
    filtering and preflight, a RuntimeError is raised.

    Public proxies are only included when ``include_public`` is True (or
    ``settings.batch_use_public_proxies`` is True). Default is off to avoid
    routing traffic through untrusted intermediaries.

    If ``dsn`` is provided, proxies previously marked dead in the
    ``dead_proxies`` table (within TTL) are excluded up front.

    If ``run_preflight`` is True (default), a TCP connect check is run
    against all proxied connections in parallel. Unreachable proxies are
    removed from the pool and marked dead in the global registry.
    """
    pool: list[Connection] = []

    # NordVPN proxies first (authenticated, most reliable)
    for server in settings.nordvpn_servers:
        pool.append(Connection(name=f"nordvpn/{server}", proxy_url=_socks5_url(server)))

    # Public proxies sorted by country priority (opt-in)
    _use_public = include_public if include_public is not None else settings.batch_use_public_proxies
    if _use_public:
        pool.extend(_load_public_proxies(proxies_path))

    # Direct connection as last-resort fallback (unless strict VPN mode)
    if allow_direct_fallback:
        pool.append(Connection(name="direct"))

    # Filter out proxies already known dead from previous runs
    if dsn:
        from batch.connections import DeadProxyRegistry
        try:
            registry = DeadProxyRegistry(dsn)
            dead = registry.get_all_dead()
            if dead:
                before = len(pool)
                pool = [c for c in pool if c.name not in dead or c.proxy_url is None]
                removed = before - len(pool)
                if removed:
                    logger.info(
                        "excluded %d known-dead proxies from pool (%d remaining)",
                        removed, len(pool),
                    )
        except Exception as exc:
            logger.warning("could not check dead_proxies table: %s", exc)

    # Pre-flight: TCP-ping all proxies, remove unreachable
    if run_preflight:
        pool = preflight_check(pool, dsn=dsn)

    # Fail fast in strict mode if no proxies survived
    proxied = [c for c in pool if c.proxy_url is not None]
    if not allow_direct_fallback and not proxied:
        raise RuntimeError(
            "BATCH_REQUIRE_VPN_ONLY is enabled but no proxies survived "
            "dead-proxy filtering and preflight checks. Cannot start workers "
            "without proxy connections."
        )

    return pool
