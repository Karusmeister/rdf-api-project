"""Login brute-force lockout store (CR2-AUTH-001 → hardened in CR3-AUTH-003).

Design requirements from the third-pass review:

  1. **Sliding failure window.** Failures that happened long ago must not count
     toward a lockout. The old implementation kept a running counter that only
     reset on success, so a trickle of sub-threshold failures would eventually
     lock the account even though no realistic attack was in progress.

  2. **Bounded state growth.** Attacker-generated identities (random emails,
     random source IPs) must not grow the in-process dict forever. The store
     evicts the least-recently-touched key when the cap is reached.

  3. **Pluggable backend.** Single-worker dev/staging runs with an in-process
     `OrderedDict`, which is fast and deterministic for tests. Multi-worker
     production deployments can substitute a shared Redis backend later by
     implementing the same `LockoutStore` protocol without touching the auth
     router. The protocol is intentionally small: `record_failure`, `is_locked`,
     and `clear`.

All timestamps use `time.monotonic()` to avoid clock-skew surprises.
"""

from __future__ import annotations

import logging
import time
from collections import OrderedDict, deque
from threading import Lock
from typing import Deque, Protocol

logger = logging.getLogger(__name__)


# Tunables — module-level so tests can monkeypatch.
LOGIN_MAX_FAILURES = 5
LOGIN_WINDOW_SECONDS = 900  # 15 minutes sliding window for "consecutive" failures
LOGIN_LOCKOUT_SECONDS = 300  # lockout duration once the threshold is crossed
LOGIN_STATE_MAX_KEYS = 10_000  # hard ceiling on in-process state size


class LockoutStore(Protocol):
    """Minimal contract the auth router relies on.

    A Redis-backed implementation would subclass this and use ZADD + ZCARD +
    EXPIRE for the sliding window and a simple KEY for the active lockout
    token. Current in-process implementation is `InMemoryLockoutStore`.
    """

    def record_failure(self, key: str) -> None: ...
    def record_success(self, key: str) -> None: ...
    def is_locked(self, key: str) -> int: ...
    def clear(self) -> None: ...


class InMemoryLockoutStore:
    """Single-process LRU-bounded sliding-window failure tracker.

    For each key we maintain:
      * A deque of recent failure timestamps (oldest first). Timestamps older
        than `window_seconds` are evicted on access.
      * An optional `locked_until` monotonic timestamp once the failure count
        inside the window crosses `max_failures`. Entries past that lockout
        are cleared automatically on the next access.

    The outer OrderedDict provides LRU eviction: on every mutation we
    `move_to_end(key)` and, if the map exceeds `max_keys`, we `popitem(last=False)`
    to drop the least-recently-touched entry. Attackers emitting a flood of
    random identities cannot balloon the process memory — at most `max_keys`
    entries exist at any time.
    """

    def __init__(
        self,
        max_failures: int = LOGIN_MAX_FAILURES,
        window_seconds: float = LOGIN_WINDOW_SECONDS,
        lockout_seconds: float = LOGIN_LOCKOUT_SECONDS,
        max_keys: int = LOGIN_STATE_MAX_KEYS,
    ) -> None:
        self._max_failures = max_failures
        self._window_seconds = window_seconds
        self._lockout_seconds = lockout_seconds
        self._max_keys = max_keys

        self._lock = Lock()
        # key -> (failure_deque, locked_until_monotonic_or_None)
        self._state: "OrderedDict[str, tuple[Deque[float], float | None]]" = OrderedDict()

    # -----------------------------------------------------------------
    # Mutation
    # -----------------------------------------------------------------

    def record_failure(self, key: str) -> None:
        """Append a failure timestamp for `key` and re-evaluate the lockout.

        Drops timestamps older than the sliding window before counting. If
        the remaining count exceeds the threshold we set `locked_until` to
        now + lockout_seconds.
        """
        now = time.monotonic()
        with self._lock:
            failures, locked_until = self._state.get(
                key, (deque(), None)
            )
            self._prune(failures, now)
            failures.append(now)
            # Promote to "locked" once the sliding window crosses threshold.
            if len(failures) >= self._max_failures:
                locked_until = now + self._lockout_seconds
            self._state[key] = (failures, locked_until)
            self._state.move_to_end(key)
            self._enforce_cap()

    def record_success(self, key: str) -> None:
        """A successful login wipes every failure counter tied to this key.

        Required so a legitimate user whose password correction comes after
        a couple of typos doesn't drag the failure streak into the next
        session.
        """
        with self._lock:
            self._state.pop(key, None)

    def clear(self) -> None:
        """Drop all tracked state. Used by test fixtures."""
        with self._lock:
            self._state.clear()

    # -----------------------------------------------------------------
    # Read
    # -----------------------------------------------------------------

    def is_locked(self, key: str) -> int:
        """Return the number of seconds remaining in the lockout, or 0.

        Automatically garbage-collects entries whose lockout has expired AND
        whose sliding window is empty so `record_failure` callers and
        `is_locked` checkers don't accumulate stale rows.
        """
        now = time.monotonic()
        with self._lock:
            entry = self._state.get(key)
            if entry is None:
                return 0
            failures, locked_until = entry

            if locked_until is not None and locked_until > now:
                # Still inside the active lockout window.
                self._state.move_to_end(key)
                return int(locked_until - now) + 1

            # Lockout expired (or was never set). Prune the window and decide
            # whether there is anything left to keep.
            self._prune(failures, now)
            if not failures:
                # Completely idle — drop the row so random-identity floods
                # don't accumulate.
                self._state.pop(key, None)
                return 0
            # Still tracking recent (sub-threshold) failures — keep the row
            # but treat the key as not locked.
            self._state[key] = (failures, None)
            self._state.move_to_end(key)
            return 0

    # -----------------------------------------------------------------
    # Introspection helpers (tests + ops)
    # -----------------------------------------------------------------

    def size(self) -> int:
        with self._lock:
            return len(self._state)

    # -----------------------------------------------------------------
    # Internal
    # -----------------------------------------------------------------

    def _prune(self, failures: "Deque[float]", now: float) -> None:
        cutoff = now - self._window_seconds
        while failures and failures[0] < cutoff:
            failures.popleft()

    def _enforce_cap(self) -> None:
        # LRU eviction: the oldest inserted/touched key is at the front.
        while len(self._state) > self._max_keys:
            evicted, _ = self._state.popitem(last=False)
            logger.info(
                "lockout_state_evicted",
                extra={
                    "event": "lockout_state_evicted",
                    "key": evicted,
                    "reason": "max_keys_exceeded",
                },
            )


# Module-level singletons: separate stores for accounts and IPs so the cap
# applies independently. The router constructs its checks against these.
account_lockout_store: LockoutStore = InMemoryLockoutStore()
ip_lockout_store: LockoutStore = InMemoryLockoutStore()
