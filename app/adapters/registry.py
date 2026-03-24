"""Adapter registry — maps source names to adapter instances.

Usage:
    from app.adapters.registry import adapters
    entity = await adapters["ms_gov"].get_entity("0000694720")
"""

from app.adapters.base import KrsSourceAdapter

adapters: dict[str, KrsSourceAdapter] = {}


def register(name: str, adapter: KrsSourceAdapter) -> None:
    """Register an adapter instance under the given source name."""
    adapters[name] = adapter


def get(name: str) -> KrsSourceAdapter:
    """Get a registered adapter by name. Raises KeyError if not found."""
    return adapters[name]
