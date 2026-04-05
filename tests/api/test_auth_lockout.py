"""Regression tests for the login brute-force lockout store (CR3-AUTH-003).

These cover the three acceptance criteria from the third-pass review:
  * Low-rate failures outside the configured window do not accumulate forever
    into a lockout.
  * State growth is bounded under random-email attack traffic.
  * The store's contract (sliding window + LRU bound) is covered by direct
    unit tests, not only indirectly through the auth HTTP integration tests.
"""

from __future__ import annotations

import time

import pytest

from app.auth_lockout import InMemoryLockoutStore


class TestSlidingWindow:
    """Acceptance criterion #1: failures outside the window must expire."""

    def test_failures_outside_window_do_not_lock(self, monkeypatch):
        """Two failures per window, below the 5-failure threshold, must never
        lock the account no matter how long the attacker probes.
        """
        current = [1_000.0]
        monkeypatch.setattr(time, "monotonic", lambda: current[0])

        store = InMemoryLockoutStore(
            max_failures=5,
            window_seconds=60,
            lockout_seconds=30,
            max_keys=1024,
        )

        # Simulate a trickle attacker: 2 failures, wait out the window,
        # 2 more, repeat 10 times. In the pre-CR3-AUTH-003 design this
        # would accumulate to 20 failures and lock the account despite
        # never crossing the threshold inside any single window.
        for _ in range(10):
            store.record_failure("victim@example.com")
            store.record_failure("victim@example.com")
            current[0] += 120  # Advance past window_seconds

        assert store.is_locked("victim@example.com") == 0, (
            "Sub-threshold trickle must never produce a lockout"
        )

    def test_threshold_crossed_inside_window_locks(self, monkeypatch):
        """Five failures inside the window must lock the account."""
        current = [2_000.0]
        monkeypatch.setattr(time, "monotonic", lambda: current[0])

        store = InMemoryLockoutStore(
            max_failures=5,
            window_seconds=60,
            lockout_seconds=30,
            max_keys=1024,
        )

        for _ in range(5):
            store.record_failure("target@example.com")
            current[0] += 1  # 1 second between attempts — well inside window

        wait = store.is_locked("target@example.com")
        assert wait > 0, "Threshold crossed inside window must lock"
        assert wait <= 31, f"Reported wait ({wait}s) should be ~lockout_seconds"

    def test_lockout_expires_and_state_auto_clears(self, monkeypatch):
        """After the lockout duration elapses, the store no longer reports
        the key as locked AND (if no recent failures remain) the row is
        evicted so random-identity floods don't linger.
        """
        current = [3_000.0]
        monkeypatch.setattr(time, "monotonic", lambda: current[0])

        store = InMemoryLockoutStore(
            max_failures=3,
            window_seconds=60,
            lockout_seconds=10,
            max_keys=1024,
        )

        for _ in range(3):
            store.record_failure("expire@example.com")
        assert store.is_locked("expire@example.com") > 0
        assert store.size() == 1

        # Advance past both the lockout AND the window.
        current[0] += 120
        assert store.is_locked("expire@example.com") == 0
        # Automatic garbage collection keeps state bounded.
        assert store.size() == 0, (
            "Expired + windowed-out entries must be dropped from the store"
        )


class TestBoundedGrowth:
    """Acceptance criterion #2: random-identity floods must not grow state
    past the configured cap."""

    def test_lru_eviction_caps_state_size(self):
        store = InMemoryLockoutStore(
            max_failures=5,
            window_seconds=600,
            lockout_seconds=60,
            max_keys=50,
        )

        # Emit 500 unique keys — that is 10x the cap. The store must evict
        # older entries rather than grow unbounded.
        for i in range(500):
            store.record_failure(f"key-{i}")

        assert store.size() <= 50, (
            f"State must stay within max_keys=50, got {store.size()}"
        )

    def test_lru_eviction_preserves_recently_touched_keys(self):
        """LRU semantics: recently-touched keys survive eviction even when
        older keys keep getting added."""
        store = InMemoryLockoutStore(
            max_failures=5,
            window_seconds=600,
            lockout_seconds=60,
            max_keys=3,
        )

        store.record_failure("a")
        store.record_failure("b")
        store.record_failure("c")
        # Promote "a" to most-recently-used, then add "d" → "b" must evict.
        store.record_failure("a")
        store.record_failure("d")

        # "a", "c", "d" remain (sub-threshold — not locked but still tracked);
        # "b" was evicted.
        assert store.size() == 3
        # Touching via is_locked is enough to tell whether the row exists in
        # the internal map — after the store auto-drops empty rows, a key
        # with 0 failures that was never touched should report 0 wait.
        # Instead of introspecting internals we probe by adding one more
        # failure and verifying "b" starts fresh (count=1 < threshold).
        store.record_failure("b")
        store.record_failure("b")
        store.record_failure("b")
        store.record_failure("b")
        # Only 4 failures for "b" — still under the threshold of 5.
        assert store.is_locked("b") == 0


class TestSuccessClearsState:
    def test_record_success_drops_all_failures_for_key(self):
        store = InMemoryLockoutStore(max_failures=5, window_seconds=60, lockout_seconds=60)

        store.record_failure("alice@example.com")
        store.record_failure("alice@example.com")
        store.record_failure("alice@example.com")
        store.record_success("alice@example.com")

        # After success the key must look completely fresh.
        assert store.is_locked("alice@example.com") == 0
        # And a subsequent failure must start counting from zero.
        for _ in range(4):
            store.record_failure("alice@example.com")
        assert store.is_locked("alice@example.com") == 0, (
            "Success should have reset the streak — 4 new failures must not lock"
        )

    def test_record_success_is_safe_on_unknown_key(self):
        store = InMemoryLockoutStore()
        # No crash, no side effects.
        store.record_success("never-seen@example.com")
        assert store.size() == 0
