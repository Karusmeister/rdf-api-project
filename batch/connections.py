"""Connection pool for batch KRS scanner.

POOL[0] is always the direct (no proxy) connection.
POOL[1..] are NordVPN SOCKS5 connections, one per NORDVPN_SERVERS entry.
"""

import logging
import time
from dataclasses import dataclass
from typing import Optional

from app.config import settings

logger = logging.getLogger(__name__)

_MAX_CONSECUTIVE_FAILURES = 3


@dataclass(frozen=True)
class Connection:
    name: str
    proxy_url: Optional[str] = None  # None = direct (no VPN)


def _socks5_url(server_hostname: str) -> str:
    """Build NordVPN SOCKS5 URL from config credentials and a server hostname.

    Accepts both full hostnames (amsterdam.nl.socks.nordhold.net) and
    short stubs (pl192) — appends .nordvpn.com only for short stubs.
    """
    u = settings.nordvpn_username
    p = settings.nordvpn_password
    if "." in server_hostname:
        host = server_hostname  # full hostname
    else:
        host = f"{server_hostname}.nordvpn.com"  # short stub
    return f"socks5://{u}:{p}@{host}:1080"


def build_pool() -> list[Connection]:
    """Build connection pool: direct first, then one per VPN server."""
    pool = [Connection(name="direct")]
    for server in settings.nordvpn_servers:
        pool.append(Connection(name=server, proxy_url=_socks5_url(server)))
    return pool


def validate_vpn_config() -> None:
    """Fail fast if VPN mode is requested but credentials/servers are missing.

    Shared across all runners (rdf_runner, metadata_runner) so validation
    is consistent. Call this *before* spawning any worker processes.
    """
    if not settings.nordvpn_username:
        raise RuntimeError("VPN enabled but NORDVPN_USERNAME is empty.")
    if not settings.nordvpn_password:
        raise RuntimeError("VPN enabled but NORDVPN_PASSWORD is empty.")
    if not settings.nordvpn_servers:
        raise RuntimeError("VPN enabled but NORDVPN_SERVERS is empty.")


POOL: list[Connection] = build_pool()


_DEAD_PROXY_TTL_HOURS = 6  # entries older than this are expired


class DeadProxyRegistry:
    """Shared dead-proxy registry backed by a PostgreSQL table.

    When a worker kills a proxy, it writes the proxy name here.
    All workers check this registry before using a proxy, so a proxy
    killed by one worker is immediately skipped by all others.

    Entries have a TTL (default 6 hours). On init, stale entries are
    purged so transient outages don't blacklist proxies permanently.
    """

    def __init__(self, dsn: str, ttl_hours: int = _DEAD_PROXY_TTL_HOURS):
        from app.db.connection import make_connection
        self._dsn = dsn
        self._ttl_hours = ttl_hours
        conn = make_connection(dsn)
        try:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS dead_proxies (
                    proxy_name  TEXT PRIMARY KEY,
                    killed_by   INTEGER NOT NULL,
                    killed_at   TIMESTAMP DEFAULT NOW()
                )
            """)
            # Purge stale entries on startup
            conn.execute(
                "DELETE FROM dead_proxies WHERE killed_at < NOW() - INTERVAL '%s hours'",
                [self._ttl_hours],
            )
        finally:
            conn.close()

    def mark_dead(self, proxy_name: str, worker_id: int) -> None:
        from app.db.connection import make_connection
        conn = make_connection(self._dsn)
        try:
            conn.execute("""
                INSERT INTO dead_proxies (proxy_name, killed_by, killed_at)
                VALUES (%s, %s, NOW())
                ON CONFLICT (proxy_name) DO UPDATE SET killed_at = NOW(), killed_by = %s
            """, [proxy_name, worker_id, worker_id])
        finally:
            conn.close()

    def is_dead(self, proxy_name: str) -> bool:
        from app.db.connection import make_connection
        conn = make_connection(self._dsn)
        try:
            row = conn.execute(
                "SELECT 1 FROM dead_proxies WHERE proxy_name = %s "
                "AND killed_at >= NOW() - INTERVAL '%s hours'",
                [proxy_name, self._ttl_hours],
            ).fetchone()
            return row is not None
        finally:
            conn.close()

    def get_all_dead(self) -> set[str]:
        from app.db.connection import make_connection
        conn = make_connection(self._dsn)
        try:
            rows = conn.execute(
                "SELECT proxy_name FROM dead_proxies "
                "WHERE killed_at >= NOW() - INTERVAL '%s hours'",
                [self._ttl_hours],
            ).fetchall()
            return {r[0] for r in rows}
        finally:
            conn.close()


class ProxyRotator:
    """Manages a list of proxy connections with auto-rotation on failure.

    Each worker gets a ProxyRotator with the full pool, starting at a
    different index (round-robin by worker_id). When the current proxy
    accumulates ``max_failures`` consecutive errors, it is permanently
    removed and the rotator advances to the next proxy.

    The ``direct`` connection (proxy_url=None) is never removed.

    Dead-proxy eviction is **global**: when a proxy is killed, it is
    recorded in ``DeadProxyRegistry`` (PostgreSQL table) so all workers
    skip it immediately. Workers also prune globally-dead proxies from
    their local pool on each rotation check.
    """

    def __init__(
        self,
        pool: list[Connection],
        start_index: int = 0,
        max_failures: int = _MAX_CONSECUTIVE_FAILURES,
        registry: DeadProxyRegistry | None = None,
        worker_id: int = 0,
    ):
        self._max_failures = max_failures
        self._consecutive_failures = 0
        self._registry = registry
        self._worker_id = worker_id
        self._rotated = False

        # Filter out already-dead proxies from the initial pool
        if registry:
            dead = registry.get_all_dead()
            self._pool = [c for c in pool if c.name not in dead or c.proxy_url is None]
        else:
            self._pool = list(pool)

        self._index = start_index % len(self._pool) if self._pool else 0
        self._last_global_check = time.monotonic()

    @property
    def current(self) -> Connection:
        return self._pool[self._index]

    @property
    def exhausted(self) -> bool:
        return len(self._pool) == 0

    @property
    def rotated(self) -> bool:
        """True if a rotation happened since last call. Resets on read."""
        r = self._rotated
        self._rotated = False
        return r

    @property
    def remaining(self) -> int:
        return len(self._pool)

    def record_success(self) -> None:
        self._consecutive_failures = 0
        self._prune_globally_dead()

    def _prune_globally_dead(self) -> None:
        """Remove proxies killed by other workers (check every 30s)."""
        if not self._registry:
            return
        now = time.monotonic()
        if now - self._last_global_check < 30.0:
            return
        self._last_global_check = now

        dead = self._registry.get_all_dead()
        before = len(self._pool)
        current_name = self._pool[self._index].name if self._pool else None
        self._pool = [c for c in self._pool if c.name not in dead or c.proxy_url is None]
        removed = before - len(self._pool)
        if removed > 0:
            logger.info(
                "proxy_rotator pruned %d globally-dead proxies, remaining=%d",
                removed, len(self._pool),
            )
            # Re-find current connection index
            if self._pool:
                try:
                    self._index = next(
                        i for i, c in enumerate(self._pool) if c.name == current_name
                    )
                except StopIteration:
                    self._index = 0

    def record_failure(self) -> Connection | None:
        """Record a failure. Returns the new Connection if rotation happened, else None."""
        self._consecutive_failures += 1
        if self._consecutive_failures < self._max_failures:
            return None

        # Rotate: remove current proxy (unless it's direct)
        removed = self._pool[self._index]
        if removed.proxy_url is not None:
            logger.warning(
                "proxy_rotator removing dead proxy=%s after %d consecutive failures",
                removed.name, self._consecutive_failures,
            )
            # Publish to global registry so all workers skip it
            if self._registry:
                self._registry.mark_dead(removed.name, self._worker_id)
            self._pool.pop(self._index)
            if not self._pool:
                return None
            self._index = self._index % len(self._pool)
        else:
            # Direct connection — don't remove, just advance
            self._index = (self._index + 1) % len(self._pool)

        self._consecutive_failures = 0
        self._rotated = True
        return self._pool[self._index]
