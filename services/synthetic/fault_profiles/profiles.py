"""FaultProfile dataclass + preset profiles.

A21 codifies the fault-injection contract: per-call probability checks
(5xx, transient) and threshold checks (rate-limit after N requests,
auth-expires after N seconds). Per-mock state is held by the mock
client itself (request counter, first-call timestamp); the profile is
read-only configuration.
"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class FaultProfile:
    """Configuration for mock-client fault injection.

    All fields default to "no fault." A test that wants only rate
    limits sets `rate_limit_after_n_requests` and leaves the others.

    Attributes:
      rate_limit_after_n_requests:
        After this many method calls, the mock raises the source's
        rate-limit error on every subsequent call until reset. `None`
        disables.
      random_5xx_probability:
        Per-call probability [0.0, 1.0] of raising the source's
        generic API error (simulating a 5xx). `0.0` disables.
      auth_expires_after_n_seconds:
        After this many seconds (wall-clock from first call), the mock
        raises the source's auth error on every call. `None` disables.
      transient_network_error_probability:
        Per-call probability [0.0, 1.0] of raising a transport-level
        error (simulating connection reset / DNS hiccup). `0.0` disables.
      rng_seed:
        Seed for the per-mock RNG used to sample probabilistic faults.
        Deterministic across test runs when set.
    """

    rate_limit_after_n_requests: int | None = None
    random_5xx_probability: float = 0.0
    auth_expires_after_n_seconds: float | None = None
    transient_network_error_probability: float = 0.0
    rng_seed: int = 0


HAPPY_PATH = FaultProfile()
RATE_LIMITED = FaultProfile(rate_limit_after_n_requests=50)
FLAKY = FaultProfile(random_5xx_probability=0.10, rng_seed=42)
AUTH_EXPIRED = FaultProfile(auth_expires_after_n_seconds=30.0)
