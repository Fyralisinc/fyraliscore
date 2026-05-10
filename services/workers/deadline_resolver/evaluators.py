"""services/workers/deadline_resolver/evaluators.py

Pure(-ish) evaluators per falsifier kind, per spec §10 and §17. Each
evaluator takes the prediction's `falsifier` dict plus a small
`EvaluationContext` (DB/repos + the prediction's `created_at` +
tenant_id) and returns one of the three provisional outcomes:

    'confirmed'    — evidence supports the prediction
    'violated'     — evidence contradicts the prediction
    'inconclusive' — not enough signal in the window to decide

These functions DO NOT touch the Models row. The resolver enqueues a
T2 trigger with the result; Think's deterministic T2 handler is the
only path that mutates `models` on resolution.

Kinds covered (all five from spec §10):

    1. observation_pattern   — GIN scan over observations since
                               prediction.created_at within
                               `falsifier.within_window`.
    2. commitment_outcome    — compare the referenced Commitment's
                               current state against
                               `contradicting_state`.
    3. prediction_deadline   — evaluate `falsifier.check` via a
                               minimal grammar (see _CHECK_GRAMMAR).
    4. resource_threshold    — read the referenced Resource and
                               evaluate `falsifier.threshold` with the
                               same minimal grammar.
    5. explicit_contestation — count contestation observations in the
                               window from `contesting_actors`.

Grammar for `prediction_deadline.check` and `resource_threshold.threshold`:

    check       :=  "Commitment" ID "in state" IDENT
                 |  "Model" ID "confidence" OP FLOAT
    threshold   :=  IDENT OP FLOAT             # e.g. "available_capacity < 0.20"
                 |  IDENT "." IDENT OP FLOAT   # e.g. "current_value.amount < 1000"

    OP          :=  "<" | "<=" | ">" | ">=" | "==" | "!="
    ID          :=  UUID string
    IDENT       :=  \\w+

The grammar is intentionally tiny. Anything that doesn't match yields
`'inconclusive'` rather than raising — matches the spec §17 policy
("fall back to LLM" at the Think layer; here we just don't decide).
"""
from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Literal
from uuid import UUID

import asyncpg


logger = logging.getLogger(__name__)


ProvisionalOutcome = Literal["confirmed", "violated", "inconclusive"]


# ---------------------------------------------------------------------
# Public context structure
# ---------------------------------------------------------------------


@dataclass
class EvaluationContext:
    """Everything an evaluator needs to make a determination.

    `conn` is a live asyncpg connection scoped to the resolver's
    per-prediction work. The resolver opens a connection, runs the
    evaluator, then closes the connection; evaluators never acquire
    their own.
    """

    conn: asyncpg.Connection
    tenant_id: UUID
    prediction_id: UUID
    prediction_created_at: datetime
    # Optional — only set when a resolver test wants to pin `now`.
    now: datetime | None = None

    def clock(self) -> datetime:
        return self.now or datetime.now(timezone.utc)


# ---------------------------------------------------------------------
# Window parser — delegates to the canonical parser in
# services.models.falsifier so adequacy and evaluation share grammar.
# Accepts both ISO-8601 (P7D, PT4H) and human-readable (7 days,
# any 4-week period) shapes.
#
# Wrapped here in a swallow-and-return-None form because evaluators
# run inside the deadline-resolver worker and `inconclusive` is the
# right behavior on truly unparseable input *at evaluation time*: by
# then any malformed value should already have been rejected at
# Model insert time (services/think/validator.py + repo.insert),
# so the only way we reach here with garbage is a row that
# pre-dates the validator change. Treating it as `inconclusive`
# matches the spec's "fall back to LLM" policy.
# ---------------------------------------------------------------------


def parse_window(spec: str | None) -> timedelta | None:
    """Parse a window spec string into a timedelta, or None if invalid.

    Returns `None` for None, empty, or unparseable input. Validation-time
    rejection happens upstream — see
    `services.models.falsifier.parse_within_window`.
    """
    from services.models.falsifier import parse_within_window
    from lib.shared.errors import MalformedFalsifierError
    try:
        return parse_within_window(spec)
    except MalformedFalsifierError:
        # Worker can't repair this; the resolver returns 'inconclusive'.
        return None


# ---------------------------------------------------------------------
# Check-expression grammar (tiny) — returns True | False | None
# ---------------------------------------------------------------------


_COMMITMENT_STATE_RE = re.compile(
    r"^\s*Commitment\s+([0-9a-fA-F-]{36})\s+in\s+state\s+(\w+)\s*$"
)
_MODEL_CONF_RE = re.compile(
    r"^\s*Model\s+([0-9a-fA-F-]{36})\s+confidence\s+(<=|>=|<|>|==|!=)\s+"
    r"([-+]?\d+(?:\.\d+)?)\s*$"
)


async def evaluate_check_expression(
    check: str,
    ctx: EvaluationContext,
) -> bool | None:
    """Evaluate a `prediction_deadline.check` expression.

    Returns True when the check holds, False when it fails cleanly,
    None when it can't be parsed / evaluated.
    """
    if not isinstance(check, str):
        return None

    m = _COMMITMENT_STATE_RE.match(check)
    if m:
        try:
            cid = UUID(m.group(1))
        except ValueError:
            return None
        expected_state = m.group(2)
        state = await ctx.conn.fetchval(
            "SELECT state FROM commitments WHERE id = $1 AND tenant_id = $2",
            cid,
            ctx.tenant_id,
        )
        if state is None:
            return None
        return state == expected_state

    m = _MODEL_CONF_RE.match(check)
    if m:
        try:
            mid = UUID(m.group(1))
        except ValueError:
            return None
        op = m.group(2)
        threshold = float(m.group(3))
        conf = await ctx.conn.fetchval(
            "SELECT confidence FROM models WHERE id = $1 AND tenant_id = $2",
            mid,
            ctx.tenant_id,
        )
        if conf is None:
            return None
        return _compare(float(conf), op, threshold)

    return None


def _compare(left: float, op: str, right: float) -> bool:
    if op == "<":
        return left < right
    if op == "<=":
        return left <= right
    if op == ">":
        return left > right
    if op == ">=":
        return left >= right
    if op == "==":
        return left == right
    if op == "!=":
        return left != right
    return False


# ---------------------------------------------------------------------
# Threshold-expression grammar for resource_threshold
# ---------------------------------------------------------------------


_THRESHOLD_RE = re.compile(
    r"""
    ^\s*
    (\w+(?:\.\w+)?)                         # ident or ident.ident
    \s*
    (<=|>=|<|>|==|!=)
    \s*
    ([-+]?\d+(?:\.\d+)?)
    \s*$
    """,
    re.VERBOSE,
)


def _lookup_resource_value(row: asyncpg.Record, path: str) -> float | None:
    """Resolve a threshold lhs path against a resource row.

    Paths supported:
      * bare column (matches top-level column when present)
      * `current_value.<key>` — look into the current_value JSONB
      * `metadata.<key>`      — look into the metadata JSONB
      * bare key falling back to current_value[<key>] if no column
    """
    parts = path.split(".", 1)
    if len(parts) == 1:
        key = parts[0]
        if key in row.keys():
            v = row[key]
            return _coerce_float(v)
        # Fall back to current_value.<key>
        cv = row["current_value"] if "current_value" in row.keys() else None
        return _coerce_float(_json_get(cv, key))
    lhs, rhs = parts
    if lhs == "current_value":
        return _coerce_float(
            _json_get(row["current_value"], rhs)
        )
    if lhs == "metadata":
        return _coerce_float(_json_get(row["metadata"], rhs))
    return None


def _json_get(blob: Any, key: str) -> Any:
    """Look up a key on a JSONB blob that may already be a dict or a
    bytes/str JSON payload."""
    if blob is None:
        return None
    if isinstance(blob, (bytes, bytearray)):
        blob = blob.decode()
    if isinstance(blob, str):
        import json as _json
        try:
            blob = _json.loads(blob)
        except ValueError:
            return None
    if isinstance(blob, dict):
        return blob.get(key)
    return None


def _coerce_float(v: Any) -> float | None:
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


# ---------------------------------------------------------------------
# Kind-specific evaluators
# ---------------------------------------------------------------------


async def evaluate_observation_pattern(
    falsifier: dict[str, Any],
    ctx: EvaluationContext,
) -> ProvisionalOutcome:
    """Look for observations matching the pattern within the window.

    The spec only mandates that the pattern be a text blob and
    `within_window` be parseable; the actual "does it match" heuristic
    is application-specific. Wave 4-A uses a deliberately-simple
    matcher:

      * content_text ILIKE %pattern_keyword%    (up to 3 keywords pulled
                                                 from the pattern text)
      * and optionally entities_mentioned @> target_entities
      * optional `direction` flag in the falsifier:
            "direction": "confirms" | "violates"
        defaults to 'violates' (the spec's example phrasings are
        contradictory patterns — `'<3 PRs/week'` is a falsifier, so a
        match means the prediction is violated).

    Returns:
      * 'violated' when matches exist AND direction=violates (default)
      * 'confirmed' when matches exist AND direction=confirms
      * 'inconclusive' when window can't be parsed OR no matches
        (the spec collapses the "no evidence" case to inconclusive,
        not confirmed — callers who want the opposite set
        `direction: "confirms"` which flips the semantics).
    """
    pattern = falsifier.get("pattern")
    if not isinstance(pattern, str) or len(pattern) < 3:
        return "inconclusive"

    window_td = parse_window(falsifier.get("within_window"))
    if window_td is None:
        return "inconclusive"

    # Determine keywords to scan.
    keywords = _pattern_keywords(pattern)
    if not keywords:
        return "inconclusive"

    entities = falsifier.get("entities_mentioned") or []
    direction = falsifier.get("direction", "violates")

    # Scan window is [prediction_created_at, prediction_created_at+window]
    # or [prediction_created_at, now], whichever ends first. This matches
    # the spec's "within_window since created_at" phrasing.
    start = ctx.prediction_created_at
    now = ctx.clock()
    end = min(start + window_td, now)
    if end <= start:
        return "inconclusive"

    params: list[Any] = [ctx.tenant_id, start, end]
    clauses = [
        "tenant_id = $1",
        "occurred_at >= $2",
        "occurred_at < $3",
    ]
    # At least one keyword must appear in content_text.
    keyword_clauses = []
    for kw in keywords[:3]:
        params.append(f"%{kw}%")
        keyword_clauses.append(f"content_text ILIKE ${len(params)}")
    clauses.append("(" + " OR ".join(keyword_clauses) + ")")
    if entities:
        import json as _json
        params.append(_json.dumps(entities))
        clauses.append(f"entities_mentioned @> ${len(params)}::jsonb")

    row = await ctx.conn.fetchrow(
        "SELECT count(*)::int AS n FROM observations WHERE "
        + " AND ".join(clauses),
        *params,
    )
    count = int(row["n"]) if row else 0

    if count <= 0:
        return "inconclusive"
    if direction == "confirms":
        return "confirmed"
    return "violated"


def _pattern_keywords(pattern: str) -> list[str]:
    """Extract up to three meaningful keywords from a pattern string.

    Drops stop-words and short tokens. Deterministic.
    """
    tokens = re.findall(r"[A-Za-z][A-Za-z0-9_]{3,}", pattern)
    stop = {
        "from", "this", "that", "with", "into", "when", "were", "will",
        "have", "been", "being", "does", "than", "then", "them", "they",
        "there", "here", "would", "could", "should", "some", "such",
        "each", "also", "only", "about", "above", "below", "within",
        "period", "window",
    }
    out: list[str] = []
    seen: set[str] = set()
    for t in tokens:
        lo = t.lower()
        if lo in stop:
            continue
        if lo in seen:
            continue
        seen.add(lo)
        out.append(lo)
        if len(out) >= 3:
            break
    return out


async def evaluate_commitment_outcome(
    falsifier: dict[str, Any],
    ctx: EvaluationContext,
) -> ProvisionalOutcome:
    """Compare the referenced Commitment's state to contradicting_state."""
    ref = falsifier.get("commitment_ref")
    contradicting = falsifier.get("contradicting_state")
    if not ref or contradicting is None:
        return "inconclusive"

    try:
        cid = UUID(str(ref))
    except (ValueError, TypeError):
        return "inconclusive"

    row = await ctx.conn.fetchrow(
        "SELECT state FROM commitments WHERE id = $1 AND tenant_id = $2",
        cid,
        ctx.tenant_id,
    )
    if row is None:
        return "inconclusive"

    state = row["state"]

    contradicting_list: list[str]
    if isinstance(contradicting, list):
        contradicting_list = [str(x) for x in contradicting]
    else:
        contradicting_list = [str(contradicting)]

    # Quote from spec §3.5 Commitment terminal states: 'doneverified', 'closed'.
    terminal = {"doneverified", "closed"}

    if state in contradicting_list:
        return "violated"
    if state in terminal:
        return "confirmed"
    return "inconclusive"


async def evaluate_prediction_deadline(
    falsifier: dict[str, Any],
    ctx: EvaluationContext,
) -> ProvisionalOutcome:
    """Evaluate `falsifier.check` per the grammar.

    `check` holding → confirmed. Parseable but failing → violated.
    Unparseable or referenced row missing → inconclusive.
    """
    check = falsifier.get("check")
    if not check:
        return "inconclusive"
    result = await evaluate_check_expression(check, ctx)
    if result is None:
        return "inconclusive"
    return "confirmed" if result else "violated"


async def evaluate_resource_threshold(
    falsifier: dict[str, Any],
    ctx: EvaluationContext,
) -> ProvisionalOutcome:
    """Evaluate `resource_threshold` falsifier.

    If the referenced resource's value crosses the threshold → violated.
    If within_window is specified and has elapsed w/o crossing →
    confirmed. If within_window hasn't elapsed yet → inconclusive
    (we can't rule out a future crossing).
    """
    ref = falsifier.get("resource_ref")
    threshold = falsifier.get("threshold")
    if not ref or not threshold:
        return "inconclusive"

    try:
        rid = UUID(str(ref))
    except (ValueError, TypeError):
        return "inconclusive"

    m = _THRESHOLD_RE.match(threshold) if isinstance(threshold, str) else None
    if m is None:
        return "inconclusive"
    lhs, op, rhs = m.group(1), m.group(2), float(m.group(3))

    row = await ctx.conn.fetchrow(
        """
        SELECT id, tenant_id, kind, identity, current_value, metadata
        FROM resources
        WHERE id = $1 AND tenant_id = $2
        """,
        rid,
        ctx.tenant_id,
    )
    if row is None:
        return "inconclusive"

    value = _lookup_resource_value(row, lhs)
    if value is None:
        return "inconclusive"

    if _compare(value, op, rhs):
        return "violated"

    window_td = parse_window(falsifier.get("within_window"))
    if window_td is None:
        return "confirmed"

    # within_window present — if it's not yet elapsed since the
    # prediction was created, we can't be sure it won't cross later.
    elapsed = ctx.clock() - ctx.prediction_created_at
    if elapsed < window_td:
        return "inconclusive"
    return "confirmed"


async def evaluate_explicit_contestation(
    falsifier: dict[str, Any],
    ctx: EvaluationContext,
) -> ProvisionalOutcome:
    """Count contestation observations from `contesting_actors`.

    If count >= len(contesting_actors) → violated; else confirmed.

    Within-window handling: the spec §10 example uses `within_days: 90`.
    We accept either `within_days` (int) or `within_window` (string).
    """
    actors = falsifier.get("contesting_actors") or []
    if not isinstance(actors, list) or not actors:
        return "inconclusive"

    actor_ids: list[UUID] = []
    for a in actors:
        try:
            actor_ids.append(UUID(str(a)))
        except (ValueError, TypeError):
            pass

    within_days = falsifier.get("within_days")
    window_td: timedelta | None = None
    if isinstance(within_days, (int, float)) and within_days > 0:
        window_td = timedelta(days=float(within_days))
    else:
        window_td = parse_window(falsifier.get("within_window"))

    start = ctx.prediction_created_at
    end = ctx.clock()
    if window_td is not None:
        end = min(start + window_td, end)
    if end <= start:
        return "inconclusive"

    # Count contestation observations whose `content.contested_model_id`
    # references our prediction id within the window. Prefer actor_id
    # match; fall back to source_actor_ref match.
    row = await ctx.conn.fetchrow(
        """
        SELECT COUNT(DISTINCT COALESCE(actor_id::text, source_actor_ref))::int
               AS n
        FROM observations
        WHERE tenant_id = $1
          AND kind = 'contestation'
          AND occurred_at >= $2 AND occurred_at < $3
          AND (content->>'contested_model_id') = $4
          AND (
            ($5::uuid[] IS NULL OR cardinality($5::uuid[]) = 0)
            OR actor_id = ANY($5::uuid[])
          )
        """,
        ctx.tenant_id,
        start,
        end,
        str(ctx.prediction_id),
        actor_ids if actor_ids else None,
    )
    count = int(row["n"]) if row else 0

    if count >= len(actors):
        return "violated"
    return "confirmed"


# ---------------------------------------------------------------------
# Kind dispatch
# ---------------------------------------------------------------------


async def evaluate_falsifier(
    falsifier: dict[str, Any] | None,
    ctx: EvaluationContext,
) -> ProvisionalOutcome:
    """Top-level dispatcher — routes to the kind-specific evaluator.

    Unknown / missing / malformed falsifier → 'inconclusive'.
    """
    if not isinstance(falsifier, dict):
        return "inconclusive"
    kind = falsifier.get("kind")
    if kind == "observation_pattern":
        return await evaluate_observation_pattern(falsifier, ctx)
    if kind == "commitment_outcome":
        return await evaluate_commitment_outcome(falsifier, ctx)
    if kind == "prediction_deadline":
        return await evaluate_prediction_deadline(falsifier, ctx)
    if kind == "resource_threshold":
        return await evaluate_resource_threshold(falsifier, ctx)
    if kind == "explicit_contestation":
        return await evaluate_explicit_contestation(falsifier, ctx)
    return "inconclusive"


__all__ = [
    "EvaluationContext",
    "ProvisionalOutcome",
    "evaluate_falsifier",
    "evaluate_observation_pattern",
    "evaluate_commitment_outcome",
    "evaluate_prediction_deadline",
    "evaluate_resource_threshold",
    "evaluate_explicit_contestation",
    "evaluate_check_expression",
    "parse_window",
]
