"""Connection layer for batch KRS scanner.

VPN is handled at the OS level (e.g. `nordvpn connect pl157`).
All workers share the same system VPN tunnel — no per-connection proxy.
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class Connection:
    name: str
