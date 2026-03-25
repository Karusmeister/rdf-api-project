"""MsGovKrsAdapter — concrete adapter for the official MS KRS Open API.

Wires ``app.krs_client`` (transport) into the ``KrsSourceAdapter`` protocol
so callers get a uniform interface regardless of data source.
"""

import logging
import time
from datetime import datetime, timezone
from typing import Optional

import httpx
from pydantic import TypeAdapter, ValidationError

from app import krs_client
from app.adapters.exceptions import (
    InvalidKrsError,
    RateLimitedError,
    UpstreamUnavailableError,
)
from app.adapters.models import AdapterHealth, KrsEntity, KrsNumber, SearchResponse
from app.monitoring.metrics import record_api_call

logger = logging.getLogger(__name__)

SOURCE = "ms_gov"
_KRS_NUMBER_ADAPTER = TypeAdapter(KrsNumber)


def _parse_date_dd_mm_yyyy(value: str | None) -> Optional[object]:
    """Parse a DD.MM.YYYY date string from the KRS API."""
    if not value:
        return None
    try:
        from datetime import date as _date

        parts = value.split(".")
        if len(parts) == 3:
            return _date(int(parts[2]), int(parts[1]), int(parts[0]))
    except (ValueError, IndexError):
        pass
    return None


def _extract_entity(krs: str, payload: dict) -> KrsEntity:
    """Map the raw KRS API JSON envelope into a KrsEntity."""
    odpis = payload.get("odpis", {})
    header = odpis.get("naglowekA", {})
    dane = odpis.get("dane", {})
    dzial1 = dane.get("dzial1", {})

    # Entity core data
    dane_podmiotu = dzial1.get("danePodmiotu", {})
    identyfikatory = dane_podmiotu.get("identyfikatory", {})

    # Address
    siedziba_i_adres = dzial1.get("siedzibaIAdres", {})
    siedziba = siedziba_i_adres.get("siedziba", {})
    adres = siedziba_i_adres.get("adres", {})

    return KrsEntity(
        krs=header.get("numerKRS", krs),
        name=dane_podmiotu.get("nazwa", ""),
        legal_form=dane_podmiotu.get("formaPrawna"),
        status=None,  # Not directly in OdpisAktualny; dzial6 has dissolution info
        registered_at=_parse_date_dd_mm_yyyy(header.get("dataRejestracjiWKRS")),
        last_changed_at=_parse_date_dd_mm_yyyy(header.get("dataOstatniegoWpisu")),
        nip=identyfikatory.get("nip"),
        regon=identyfikatory.get("regon"),
        address_city=siedziba.get("miejscowosc") or adres.get("miejscowosc"),
        address_street=adres.get("ulica"),
        address_postal_code=adres.get("kodPocztowy"),
        raw=payload,
    )


def _normalize_requested_krs(krs: str) -> str:
    """Validate and canonicalize adapter input before any upstream call."""
    try:
        return _KRS_NUMBER_ADAPTER.validate_python(krs)
    except ValidationError as exc:
        raise InvalidKrsError(SOURCE, krs) from exc


class MsGovKrsAdapter:
    """Adapter for the official MS KRS Open API at api-krs.ms.gov.pl.

    Implements ``KrsSourceAdapter`` protocol.
    """

    async def get_entity(self, krs: str) -> Optional[KrsEntity]:
        """Fetch a single entity by KRS number via OdpisAktualny.

        Returns None if the entity does not exist (404).
        Raises on transient/network errors.
        """
        padded = _normalize_requested_krs(krs)
        t0 = time.monotonic()
        try:
            resp = await krs_client.get(
                f"/OdpisAktualny/{padded}",
                params={"rejestr": "P", "format": "json"},
                allowed_statuses={404},
            )
            latency_ms = int((time.monotonic() - t0) * 1000)
            record_api_call(
                source=SOURCE,
                operation="get_entity",
                status_code=resp.status_code,
                latency_ms=latency_ms,
            )
        except httpx.HTTPStatusError as exc:
            latency_ms = int((time.monotonic() - t0) * 1000)
            record_api_call(
                source=SOURCE,
                operation="get_entity",
                status_code=exc.response.status_code,
                latency_ms=latency_ms,
                error=str(exc),
            )
            status = exc.response.status_code
            if status == 429:
                raise RateLimitedError(SOURCE) from exc
            raise UpstreamUnavailableError(
                SOURCE, f"HTTP {status}"
            ) from exc
        except httpx.RequestError as exc:
            latency_ms = int((time.monotonic() - t0) * 1000)
            record_api_call(
                source=SOURCE,
                operation="get_entity",
                status_code=0,
                latency_ms=latency_ms,
                error=str(exc),
            )
            raise UpstreamUnavailableError(
                SOURCE, str(exc)
            ) from exc

        if resp.status_code == 404:
            return None

        payload = resp.json()
        return _extract_entity(padded, payload)

    async def search(
        self,
        *,
        name: Optional[str] = None,
        nip: Optional[str] = None,
        regon: Optional[str] = None,
        page: int = 0,
        page_size: int = 20,
    ) -> SearchResponse:
        """Not supported — the KRS Open API has no search endpoint.

        See docs/KRS_OPEN_API.md § Limitations.
        """
        raise NotImplementedError(
            "The official KRS Open API does not provide a search endpoint. "
            "Use a different adapter or look up entities by KRS number directly."
        )

    async def health_check(self) -> AdapterHealth:
        """Delegate to krs_client.health_check() and wrap the result."""
        t0 = time.monotonic()
        try:
            result = await krs_client.health_check()
            return AdapterHealth(
                source=SOURCE,
                ok=result["ok"],
                latency_ms=result["latency_ms"],
                checked_at=datetime.now(timezone.utc),
            )
        except Exception:
            latency_ms = int((time.monotonic() - t0) * 1000)
            return AdapterHealth(
                source=SOURCE,
                ok=False,
                latency_ms=latency_ms,
                checked_at=datetime.now(timezone.utc),
            )
