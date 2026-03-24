"""Per-call metrics for KRS API interactions.

In-memory ring buffer (last N calls). No external metrics system needed yet.
Every adapter call should be recorded via ``record_api_call()``.
"""

from __future__ import annotations

import logging
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Optional

logger = logging.getLogger(__name__)

_BUFFER_SIZE = 1000


@dataclass(frozen=True, slots=True)
class ApiCallRecord:
    """A single recorded API call."""

    source: str
    operation: str
    status_code: int
    latency_ms: int
    cached: bool = False
    error: str | None = None
    timestamp: float = field(default_factory=time.monotonic)


_buffer: deque[ApiCallRecord] = deque(maxlen=_BUFFER_SIZE)


def record_api_call(
    *,
    source: str,
    operation: str,
    status_code: int,
    latency_ms: int,
    cached: bool = False,
    error: str | None = None,
) -> None:
    """Record an API call. Always emits a structured log line."""
    record = ApiCallRecord(
        source=source,
        operation=operation,
        status_code=status_code,
        latency_ms=latency_ms,
        cached=cached,
        error=error,
    )
    _buffer.append(record)

    log_extra = {
        "source": source,
        "operation": operation,
        "status_code": status_code,
        "latency_ms": latency_ms,
        "cached": cached,
    }
    if error:
        log_extra["error"] = error
        logger.warning("krs_adapter_call", extra=log_extra)
    else:
        logger.info("krs_adapter_call", extra=log_extra)


def get_stats(source: Optional[str] = None) -> dict:
    """Compute summary statistics over the ring buffer.

    Returns p50/p95 latency, error rate, call count per source, cache hit rate.
    """
    records = list(_buffer)
    if source:
        records = [r for r in records if r.source == source]

    total = len(records)
    if total == 0:
        return {
            "total_calls": 0,
            "error_rate": 0.0,
            "cache_hit_rate": 0.0,
            "p50_latency_ms": 0,
            "p95_latency_ms": 0,
            "calls_per_source": {},
        }

    errors = sum(1 for r in records if r.error is not None)
    cached = sum(1 for r in records if r.cached)
    latencies = sorted(r.latency_ms for r in records)

    # Calls per source
    source_counts: dict[str, int] = {}
    for r in records:
        source_counts[r.source] = source_counts.get(r.source, 0) + 1

    return {
        "total_calls": total,
        "error_rate": round(errors / total, 4),
        "cache_hit_rate": round(cached / total, 4),
        "p50_latency_ms": _percentile(latencies, 50),
        "p95_latency_ms": _percentile(latencies, 95),
        "calls_per_source": source_counts,
    }


def _percentile(sorted_values: list[int], pct: int) -> int:
    """Compute a percentile from a pre-sorted list."""
    if not sorted_values:
        return 0
    idx = int(len(sorted_values) * pct / 100)
    idx = min(idx, len(sorted_values) - 1)
    return sorted_values[idx]


def clear() -> None:
    """Clear the buffer. For testing only."""
    _buffer.clear()
