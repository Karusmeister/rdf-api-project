"""Connection pool for batch KRS scanner.

POOL[0] is always the direct (no proxy) connection.
POOL[1..] are NordVPN SOCKS5 connections, one per NORDVPN_SERVERS entry.
"""

from dataclasses import dataclass
from typing import Optional

from app.config import settings


@dataclass(frozen=True)
class Connection:
    name: str
    proxy_url: Optional[str] = None  # None = direct (no VPN)


def _socks5_url(server_hostname: str) -> str:
    """Build NordVPN SOCKS5 URL from config credentials and a hostname stub."""
    u = settings.nordvpn_username
    p = settings.nordvpn_password
    return f"socks5://{u}:{p}@{server_hostname}.nordvpn.com:1080"


def build_pool() -> list[Connection]:
    """Build connection pool: direct first, then one per VPN server."""
    pool = [Connection(name="direct")]
    for server in settings.nordvpn_servers:
        pool.append(Connection(name=server, proxy_url=_socks5_url(server)))
    return pool


POOL: list[Connection] = build_pool()
