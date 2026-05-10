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

import re
from datetime import datetime, timedelta, timezone
from typing import Any

import asyncpg

from lib.shared.errors import MalformedFalsifierError


# ---------------------------------------------------------------------
# `within_window` parser — single source of truth.
#
# Policy decision (T1a, see tests/synthesis_harness/REPORT.md §5):
# accept *both* ISO-8601 duration strings (e.g. `P7D`, `PT4H`,
# `P1M`, `P2W`, `PT30M`) AND the legacy human-readable form
# (e.g. `7 days`, `4 weeks`, `any 4-week period`). The Think prompt
# tells the LLM to emit ISO-8601 (services/think/prompt.py:55), but
# the deadline-resolver evaluator historically only accepted the
# human form, so falsifiers carrying valid ISO durations were
# silently collapsed to `inconclusive`. Accepting both removes the
# silent gap.
#
# Returns a `timedelta` on success; raises `MalformedFalsifierError`
# for non-empty strings that fail both grammars. Returns `None` for
# `None` / empty input — that's "missing", not "malformed", and
# the adequacy check below decides whether missing is fatal.
# ---------------------------------------------------------------------

_HUMAN_WINDOW_RE = re.compile(
    r"""
    ^\s*
    (?:any\s+)?
    (\d+(?:\.\d+)?)    # number
    [- ]?              # optional separator
    (second|minute|hour|day|week|month|year)
    s?                 # optional plural
    (?:\s+period)?     # optional "period"
    \s*$
    """,
    re.IGNORECASE | re.VERBOSE,
)

_HUMAN_UNIT_SECONDS = {
    "second": 1.0,
    "minute": 60.0,
    "hour": 3600.0,
    "day": 86400.0,
    "week": 7 * 86400.0,
    "month": 30 * 86400.0,
    "year": 365 * 86400.0,
}

# ISO-8601 duration: P[nY][nM][nW][nD][T[nH][nM][nS]]. We support the
# common subset Y/M/W/D and HH/MM/SS. Months and years are
# approximated (30 / 365 days) consistent with the human grammar so
# the two formats produce the same timedelta for the same intent.
_ISO_DURATION_RE = re.compile(
    r"""
    ^\s*P
    (?:(\d+(?:\.\d+)?)Y)?
    (?:(\d+(?:\.\d+)?)M)?
    (?:(\d+(?:\.\d+)?)W)?
    (?:(\d+(?:\.\d+)?)D)?
    (T)?                              # capture whether 'T' is present
    (?:(\d+(?:\.\d+)?)H)?
    (?:(\d+(?:\.\d+)?)M)?
    (?:(\d+(?:\.\d+)?)S)?
    \s*$
    """,
    re.VERBOSE,
)


def parse_within_window(spec: Any) -> timedelta | None:
    """Parse a falsifier `within_window` value into a timedelta.

    Accepts:
      * ISO-8601 duration: `P7D`, `PT4H`, `PT30M`, `P2W`, `P1Y6M`, `P1DT12H`.
      * Human-readable: `7 days`, `4 weeks`, `6 hours`, `any 4-week period`.

    Returns `None` for `None`, empty string, or non-string input — the
    "missing" case. Raises `MalformedFalsifierError` for non-empty
    strings that match neither grammar — the "malformed" case.

    The duration must be strictly positive; zero or negative values
    raise `MalformedFalsifierError`.
    """
    if spec is None:
        return None
    if not isinstance(spec, str):
        raise MalformedFalsifierError(
            f"within_window must be a string; got {type(spec).__name__}",
            field="within_window",
            value=spec,
        )
    s = spec.strip()
    if not s:
        return None

    # Try ISO-8601 first (it's stricter — starts with `P`).
    if s[0] in ("P", "p"):
        m = _ISO_DURATION_RE.match(s.upper())
        if m is None:
            raise MalformedFalsifierError(
                f"within_window {spec!r} starts with 'P' but is not a "
                f"valid ISO-8601 duration",
                field="within_window",
                value=spec,
            )
        years, months, weeks, days, t_marker, hours, minutes, seconds = m.groups()
        # Reject the empty `P` / `PT` cases — they parse but have zero
        # length, and a zero-length window is never useful.
        if not any((years, months, weeks, days, hours, minutes, seconds)):
            raise MalformedFalsifierError(
                f"within_window {spec!r} has no duration components",
                field="within_window",
                value=spec,
            )
        # `T` separator requires at least one time component (H/M/S).
        # `P7DT` is malformed because the `T` is dangling.
        if t_marker and not any((hours, minutes, seconds)):
            raise MalformedFalsifierError(
                f"within_window {spec!r} has 'T' separator with no "
                f"time components",
                field="within_window",
                value=spec,
            )
        total_seconds = (
            (float(years or 0) * _HUMAN_UNIT_SECONDS["year"])
            + (float(months or 0) * _HUMAN_UNIT_SECONDS["month"])
            + (float(weeks or 0) * _HUMAN_UNIT_SECONDS["week"])
            + (float(days or 0) * _HUMAN_UNIT_SECONDS["day"])
            + (float(hours or 0) * _HUMAN_UNIT_SECONDS["hour"])
            + (float(minutes or 0) * _HUMAN_UNIT_SECONDS["minute"])
            + (float(seconds or 0) * _HUMAN_UNIT_SECONDS["second"])
        )
        if total_seconds <= 0:
            raise MalformedFalsifierError(
                f"within_window {spec!r} resolves to non-positive duration",
                field="within_window",
                value=spec,
            )
        return timedelta(seconds=total_seconds)

    # Human-readable grammar.
    m = _HUMAN_WINDOW_RE.match(s)
    if m is None:
        raise MalformedFalsifierError(
            f"within_window {spec!r} does not match either the "
            f"ISO-8601 duration grammar (P7D, PT4H, …) or the "
            f"human-readable grammar (\"7 days\", \"4 weeks\", …)",
            field="within_window",
            value=spec,
        )
    n = float(m.group(1))
    unit = m.group(2).lower()
    total = n * _HUMAN_UNIT_SECONDS[unit]
    if total <= 0:
        raise MalformedFalsifierError(
            f"within_window {spec!r} resolves to non-positive duration",
            field="within_window",
            value=spec,
        )
    return timedelta(seconds=total)


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
        raw_window = falsifier.get("within_window")
        if not raw_window:
            return False, "no window specified"
        # Parse strictly. A malformed window is a structural defect,
        # not an adequacy judgment — propagate the parser's exception
        # so the validator records `failure_reason='malformed_falsifier'`
        # rather than the more generic `inadequate_falsifier`.
        parse_within_window(raw_window)
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
        # `within_window` is optional here (the evaluator falls back to
        # `within_days` or to "since prediction_created_at"); but if
        # supplied, it must be parseable.
        raw_window = falsifier.get("within_window")
        if raw_window:
            parse_within_window(raw_window)
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
