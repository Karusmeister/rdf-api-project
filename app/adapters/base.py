"""KRS source adapter protocol — the contract every data provider must satisfy.

Zero HTTP code here. This file defines only the interface.
"""

from typing import Optional, Protocol, runtime_checkable

from app.adapters.models import AdapterHealth, KrsEntity, SearchResponse


@runtime_checkable
class KrsSourceAdapter(Protocol):
    """Abstract adapter for fetching KRS entity data from any source."""

    async def get_entity(self, krs: str) -> Optional[KrsEntity]:
        """Fetch a single entity by KRS number.

        Returns None if the entity does not exist in the source.
        Raises on transient/network errors.
        """
        ...

    async def search(
        self,
        *,
        name: Optional[str] = None,
        nip: Optional[str] = None,
        regon: Optional[str] = None,
        page: int = 0,
        page_size: int = 20,
    ) -> SearchResponse:
        """Search for entities by name, NIP, or REGON.

        Not all adapters support search — those that don't should raise
        NotImplementedError.
        """
        ...

    async def health_check(self) -> AdapterHealth:
        """Check connectivity to the underlying data source."""
        ...
