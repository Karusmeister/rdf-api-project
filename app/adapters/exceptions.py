"""Common exceptions for KRS source adapters.

These provide a uniform error surface so callers don't need to know
which upstream API an adapter wraps.
"""


class AdapterError(Exception):
    """Base class for all adapter errors."""

    def __init__(self, source: str, message: str) -> None:
        self.source = source
        super().__init__(f"[{source}] {message}")


class EntityNotFoundError(AdapterError):
    """The requested KRS entity does not exist in the data source."""

    def __init__(self, source: str, krs: str) -> None:
        self.krs = krs
        super().__init__(source, f"Entity {krs} not found")


class InvalidKrsError(AdapterError):
    """The provided KRS identifier is malformed."""

    def __init__(self, source: str, krs: object) -> None:
        self.krs = krs
        super().__init__(source, f"Invalid KRS {krs!r}; expected 1 to 10 digits")


class UpstreamUnavailableError(AdapterError):
    """The upstream data source is unreachable or returning errors."""

    def __init__(self, source: str, detail: str = "") -> None:
        msg = "Upstream unavailable"
        if detail:
            msg += f": {detail}"
        super().__init__(source, msg)


class RateLimitedError(AdapterError):
    """The upstream data source is rate-limiting our requests."""

    def __init__(self, source: str, retry_after: float | None = None) -> None:
        self.retry_after = retry_after
        detail = f"retry after {retry_after}s" if retry_after else ""
        super().__init__(source, f"Rate limited{': ' + detail if detail else ''}")
