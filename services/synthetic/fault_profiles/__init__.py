"""Fault profiles for X2 mock clients.

Per A21: each mock client accepts a FaultProfile that controls when
faults fire (rate limit, 5xx, auth expiration, transient network). The
profile is consulted on each method call; mocks raise the source's
real error types when a fault is triggered.

Presets cover the four most common test scenarios:
  - HAPPY_PATH:    no faults; mocks always serve fixture data.
  - RATE_LIMITED:  rate-limit error after 50 requests.
  - FLAKY:         10% probability of 5xx per call.
  - AUTH_EXPIRED:  auth error after 30 seconds wall-clock.
"""
from services.synthetic.fault_profiles.profiles import (
    AUTH_EXPIRED,
    FLAKY,
    HAPPY_PATH,
    RATE_LIMITED,
    FaultProfile,
)


__all__ = [
    "AUTH_EXPIRED",
    "FLAKY",
    "FaultProfile",
    "HAPPY_PATH",
    "RATE_LIMITED",
]
