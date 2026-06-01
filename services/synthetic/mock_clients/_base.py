"""Shared fault-injection machinery for X2 mock clients.

Each mock subclasses `_MockBase` and calls `self._check_fault()` at
the start of every public method. The base resolves which fault (if
any) should fire and dispatches to a source-specific raiser. Sources
override `_raise_rate_limit()`, `_raise_5xx()`, `_raise_auth_error()`,
`_raise_transient()` to surface the right error type (so callers in
M6 fetcher / reconciler / planner code see the same exception types
they'd see from the real client).
"""
from __future__ import annotations

import random
import time
from typing import NoReturn

from services.synthetic.fault_profiles import FaultProfile, HAPPY_PATH


class _MockBase:
    """Base class for X2 mock clients.

    Subclasses MUST override:
      - `_raise_rate_limit()`
      - `_raise_5xx()`
      - `_raise_auth_error()`
      - `_raise_transient()`

    Subclasses MAY use:
      - `self.request_count` (incremented automatically by `_check_fault`).
    """

    def __init__(self, *, profile: FaultProfile = HAPPY_PATH) -> None:
        self._profile = profile
        self.request_count = 0
        self._first_call_at: float | None = None
        self._rng = random.Random(profile.rng_seed)

    def _check_fault(self) -> None:
        """Called at the start of every public method. Raises one of
        the source-specific error types if a fault is configured and
        triggers."""
        self.request_count += 1
        now = time.monotonic()
        if self._first_call_at is None:
            self._first_call_at = now

        p = self._profile
        # Threshold faults first (deterministic).
        if (p.rate_limit_after_n_requests is not None
                and self.request_count > p.rate_limit_after_n_requests):
            self._raise_rate_limit()  # NoReturn

        if (p.auth_expires_after_n_seconds is not None
                and now - self._first_call_at > p.auth_expires_after_n_seconds):
            self._raise_auth_error()  # NoReturn

        # Probabilistic faults (seeded RNG → deterministic per profile).
        if (p.random_5xx_probability > 0.0
                and self._rng.random() < p.random_5xx_probability):
            self._raise_5xx()  # NoReturn

        if (p.transient_network_error_probability > 0.0
                and self._rng.random()
                    < p.transient_network_error_probability):
            self._raise_transient()  # NoReturn

    # Subclasses override these.
    def _raise_rate_limit(self) -> NoReturn:
        raise NotImplementedError

    def _raise_5xx(self) -> NoReturn:
        raise NotImplementedError

    def _raise_auth_error(self) -> NoReturn:
        raise NotImplementedError

    def _raise_transient(self) -> NoReturn:
        raise NotImplementedError
