"""Load, filter, and prioritize SOCKS5 proxies from proxies.json.

Builds a priority-ordered list of Connection objects:
  1. NordVPN proxies (authenticated, from settings)
  2. Public proxies from proxies.json, sorted by country priority

Banned countries (ZZ/unknown, RU, BY, KP, AF) are excluded.
Priority countries: PL, DE, CZ, SK, SE, NL, FR, AT, ES — then everything else.
"""

import json
import logging
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


def build_full_pool(
    proxies_path: Path | None = None,
    include_public: bool | None = None,
) -> list[Connection]:
    """Build the complete priority-ordered proxy pool.

    Order: NordVPN proxies → public proxies (by country priority) → direct.
    Direct is last so workers prefer proxied connections for geo-distribution,
    falling back to the VM's own IP only when all proxies are exhausted.

    Public proxies are only included when ``include_public`` is True (or
    ``settings.batch_use_public_proxies`` is True). Default is off to avoid
    routing traffic through untrusted intermediaries.
    """
    pool: list[Connection] = []

    # NordVPN proxies first (authenticated, most reliable)
    for server in settings.nordvpn_servers:
        pool.append(Connection(name=f"nordvpn/{server}", proxy_url=_socks5_url(server)))

    # Public proxies sorted by country priority (opt-in)
    _use_public = include_public if include_public is not None else settings.batch_use_public_proxies
    if _use_public:
        pool.extend(_load_public_proxies(proxies_path))

    # Direct connection as last-resort fallback
    pool.append(Connection(name="direct"))

    return pool
