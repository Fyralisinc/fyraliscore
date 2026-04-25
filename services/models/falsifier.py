"""
services/models/falsifier.py — falsifier adequacy check per spec §10.

Authoritative kind names per ARCHITECTURE-FINAL.md §10:

  1. observation_pattern   — a specific signal shape would contradict
  2. commitment_outcome    — a Commitment resolution would contradict
  3. prediction_deadline   — prediction evaluated at specific time
  4. resource_threshold    — Resource crosses a boundary
  5. explicit_contestation — authoritative contestation from specified actors

NOTE on naming discrepancy: BUILD-PLAN.md Prompt 1-C lists an alternate
set of five falsifier kinds (`resolution_criteria`, `observation_contradicts`,
`time_bound_absence`, `threshold_cross`, `counterfactual_required`). Those
names do not appear in the spec; spec §10 wins. Documented in BUILD-LOG.md
Deviations.

Adequacy rules exactly as spec §10:

  observation_pattern    — pattern >= 20 chars AND within_window set
  commitment_outcome     — commitment_ref set AND contradicting_state set
                           AND referenced commitment exists (caller-side lookup
                           optional; omitted in-pipeline for synchronous use
                           since the pure `is_adequate_falsifier` is called
                           without a DB handle. DB-side verification happens
                           inside repo.insert via `is_adequate_falsifier_async`)
  prediction_deadline    — evaluate_at set AND in future AND check set
  resource_threshold     — resource_ref set AND threshold set
  explicit_contestation  — contesting_actors non-empty list

Return value: `(ok: bool, reason: str | None)`.

The pure function takes the falsifier dict (or None) and returns a
tuple. Callers that need DB-backed verification (i.e. does the
commitment_ref actually exist?) can call `is_adequate_falsifier_async`
which accepts a connection and runs the extra checks.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import asyncpg


LEGAL_FALSIFIER_KINDS: frozenset[str] = frozenset(
    (
        "observation_pattern",
        "commitment_outcome",
        "prediction_deadline",
        "resource_threshold",
        "explicit_contestation",
    )
)


def _parse_dt(value: Any) -> datetime | None:
    """Accept a str (ISO) or a datetime. Return a timezone-aware datetime or None."""
    if value is None:
        return None
    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, str):
        try:
            # fromisoformat handles `2026-05-15T00:00:00+00:00`; the
            # trailing 'Z' is tolerated on 3.11+.
            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
    else:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def is_adequate_falsifier(
    falsifier: dict[str, Any] | None,
    *,
    now: datetime | None = None,
) -> tuple[bool, str | None]:
    """
    Validate a falsifier JSON payload by spec §10 rules.

    Pure function — no DB access. Optional `now` injectable for
    deterministic tests of `prediction_deadline.evaluate_at` comparisons.
    """
    if falsifier is None:
        return False, "no falsifier specified"
    if not isinstance(falsifier, dict):
        return False, f"falsifier must be dict; got {type(falsifier).__name__}"
    kind = falsifier.get("kind")
    if not kind:
        return False, "falsifier missing 'kind' field"
    if kind not in LEGAL_FALSIFIER_KINDS:
        return False, f"unknown falsifier kind: {kind}"

    if kind == "observation_pattern":
        pattern = falsifier.get("pattern")
        if not isinstance(pattern, str) or len(pattern) < 20:
            return False, "pattern too vague"
        if not falsifier.get("within_window"):
            return False, "no window specified"
        return True, None

    if kind == "commitment_outcome":
        if not falsifier.get("commitment_ref"):
            return False, "no commitment reference"
        contradicting = falsifier.get("contradicting_state")
        # Either a list or a string is acceptable in the spec example.
        if contradicting is None or (
            isinstance(contradicting, (list, str)) and len(contradicting) == 0
        ):
            return False, "no contradicting state"
        return True, None

    if kind == "prediction_deadline":
        evaluate_at = _parse_dt(falsifier.get("evaluate_at"))
        if evaluate_at is None:
            return False, "no evaluate_at time"
        reference = now or datetime.now(tz=timezone.utc)
        if reference.tzinfo is None:
            reference = reference.replace(tzinfo=timezone.utc)
        if evaluate_at < reference:
            return False, "evaluate_at in past"
        if not falsifier.get("check"):
            return False, "no check specification"
        return True, None

    if kind == "resource_threshold":
        if not falsifier.get("resource_ref"):
            return False, "no resource reference"
        if not falsifier.get("threshold"):
            return False, "no threshold"
        return True, None

    if kind == "explicit_contestation":
        actors = falsifier.get("contesting_actors")
        if not isinstance(actors, list) or len(actors) == 0:
            return False, "no contesting actors"
        return True, None

    # Unreachable — kind was validated above.
    return False, f"unknown falsifier kind: {kind}"


async def is_adequate_falsifier_async(
    falsifier: dict[str, Any] | None,
    *,
    conn: asyncpg.Connection | None = None,
    now: datetime | None = None,
) -> tuple[bool, str | None]:
    """
    Like `is_adequate_falsifier`, but additionally verifies the
    `commitment_outcome.commitment_ref` exists when a connection is
    supplied. Follows spec §10: "referenced commitment does not exist".
    """
    ok, reason = is_adequate_falsifier(falsifier, now=now)
    if not ok:
        return ok, reason
    assert falsifier is not None  # narrowed by the check above
    if falsifier.get("kind") == "commitment_outcome" and conn is not None:
        ref = falsifier.get("commitment_ref")
        exists = await conn.fetchval(
            "SELECT 1 FROM commitments WHERE id = $1::uuid", ref
        )
        if not exists:
            return False, "referenced commitment does not exist"
    return True, None


__all__ = [
    "LEGAL_FALSIFIER_KINDS",
    "is_adequate_falsifier",
    "is_adequate_falsifier_async",
]
