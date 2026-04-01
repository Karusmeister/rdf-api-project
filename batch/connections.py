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
