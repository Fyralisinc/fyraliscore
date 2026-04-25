"""
services/falsifiers — authoritative falsifier adequacy package.

BUILD-PLAN §5 Prompt 4.C calls for a "thin re-export layer" so that:

  * Wave 4-A's Deadline resolver (services/workers/deadline_resolver)
  * Wave 4-B's Anomaly processor T3 handler
  * Wave 3-B's Think validator
  * Wave 4-C's Contestability standing flow

all import the adequacy check from one place. Wave 1-C implemented the
logic in services/models/falsifier.py; this package re-exports that
module unmodified. Any change to the rules must land in
services/models/falsifier.py (the authoritative implementation) and not
here.

Public surface
--------------
`is_adequate(falsifier, *, now=None)`
    Pure function. Returns `(ok: bool, reason: str | None)`.

`is_adequate_async(falsifier, *, conn=None, now=None)`
    Coroutine. Additionally verifies that
    `commitment_outcome.commitment_ref` exists when a connection is
    supplied.

`LEGAL_FALSIFIER_KINDS`
    Canonical frozenset of the five spec §10 kinds:
      observation_pattern, commitment_outcome, prediction_deadline,
      resource_threshold, explicit_contestation.

Callers that still import from `services.models.falsifier` continue to
work — we do NOT deprecate the original path. Both imports resolve to
the exact same function object.
"""
from __future__ import annotations

from services.models.falsifier import (
    LEGAL_FALSIFIER_KINDS,
    is_adequate_falsifier as is_adequate,
    is_adequate_falsifier_async as is_adequate_async,
)


__all__ = [
    "LEGAL_FALSIFIER_KINDS",
    "is_adequate",
    "is_adequate_async",
]
