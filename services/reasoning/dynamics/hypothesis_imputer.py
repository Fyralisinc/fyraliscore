"""Deterministic hypothesis imputation for missing-transition anomalies.

This is the "imaginary node" pattern's load-bearing layer. When the substrate
detects a discontinuity in a Model's audit chain
(`event_i.new_state != event_j.previous_state` modulo volatile fields),
the imputer synthesizes a low-confidence `claim_role='hypothesis'` Model
that represents the system's best guess at what happened between the two
observed states.

Two key design constraints:

1. **Pure function.** No database I/O. The imputer takes a fully-enriched
   `MissingTransitionDiscontinuity` (from
   `services/reasoning/dynamics/detectors.py:fetch_missing_transition_discontinuity`)
   plus the source Model's snapshot, and returns an `ImputedHypothesis`
   that the Think deterministic handler will turn into a `ClaimOp.insert`.
   This makes the calibration/property tests run in-memory at thousands
   of iterations per second.

2. **System-hypothesized cap.** Raw confidence is clipped to `[0.20, 0.50]`
   *before* the substrate's standard `[0.05, 0.95]` clip. This is the
   load-bearing invariant of the imaginary-node pattern: the system's own
   guesses must never out-confidence directly-observed claims. A CEO who
   later Approves a hypothesis can only raise it through ratification —
   the imputer alone cannot mint a high-confidence Model.

The Wrong-but-Useful Principle: even a confidently-wrong hypothesis is
productive, because it inverts the user's burden from "what happened?" to
"is this right?" — the cheaper cognitive act. See
MODEL-THINK-RETRIEVAL-DEEP-DIVE.md and the imaginary-node design memo.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any
from uuid import UUID

from .detectors import MissingTransitionDiscontinuity


# ---------------------------------------------------------------------
# Confidence policy
# ---------------------------------------------------------------------

# Floor and ceiling for the imputer's raw confidence. The ceiling is the
# load-bearing invariant: system-hypothesized claims must never out-confidence
# directly-observed claims. The substrate's downstream clip ([0.05, 0.95])
# is wider; this clip applies first and is the strict ceiling for the
# system-hypothesized lineage.
SYSTEM_HYPOTHESIS_CONFIDENCE_FLOOR: float = 0.20
SYSTEM_HYPOTHESIS_CONFIDENCE_CEILING: float = 0.50

# Falsifier window — the imputed hypothesis is considered falsified if
# explicit evidence describing the unrecorded mutation arrives within
# this window. Two weeks balances responsiveness against the rate at
# which off-system context tends to surface in practice.
_FALSIFIER_WINDOW: str = "14d"

# Field-count diminishing returns: each additional differing field beyond
# the first adds 0.15 to the field factor, capped at 1.0. So 5+ differing
# fields saturate the field factor; 1 field gives the minimum.
_FIELD_FACTOR_BASE: float = 0.40
_FIELD_FACTOR_PER_ADDITIONAL: float = 0.15

# Gap penalty: a 30-day gap zeroes the gap factor. Anything within minutes
# is at full strength because the substrate's audit invariant is strong —
# a near-instantaneous discontinuity is almost certainly a missed
# mutation, not an intentional gap in observation.
_GAP_PENALTY_HORIZON_DAYS: float = 30.0


def compute_imputed_confidence(
    *,
    differing_fields: tuple[str, ...] | int,
    gap_seconds: float,
) -> float:
    """Deterministic, calibrated confidence in [floor, ceiling].

    Monotonically increasing in field count (with diminishing returns).
    Monotonically decreasing in gap size (zero past `HORIZON_DAYS`).

    The function is exposed publicly so calibration tests can probe its
    surface directly without constructing full discontinuity objects.
    """
    n_fields = (
        len(differing_fields)
        if isinstance(differing_fields, tuple)
        else int(differing_fields)
    )
    if n_fields < 1:
        return SYSTEM_HYPOTHESIS_CONFIDENCE_FLOOR

    field_factor = min(
        1.0,
        _FIELD_FACTOR_BASE
        + _FIELD_FACTOR_PER_ADDITIONAL * (n_fields - 1),
    )

    gap_days = max(0.0, float(gap_seconds)) / 86400.0
    gap_factor = max(0.0, 1.0 - gap_days / _GAP_PENALTY_HORIZON_DAYS)

    span = SYSTEM_HYPOTHESIS_CONFIDENCE_CEILING - SYSTEM_HYPOTHESIS_CONFIDENCE_FLOOR
    raw = SYSTEM_HYPOTHESIS_CONFIDENCE_FLOOR + span * field_factor * gap_factor
    # Defensive clip: in case any caller passes pathological inputs that
    # break the math invariants above.
    return max(
        SYSTEM_HYPOTHESIS_CONFIDENCE_FLOOR,
        min(SYSTEM_HYPOTHESIS_CONFIDENCE_CEILING, raw),
    )


# ---------------------------------------------------------------------
# Hypothesis output shape
# ---------------------------------------------------------------------


@dataclass(frozen=True)
class ImputedHypothesis:
    """A fully-formed hypothesis Model awaiting a `born_from_event_id`.

    All substrate fields except `born_from_event_id` (which depends on
    when/where the imputation is persisted) are populated. The caller —
    the T3 deterministic handler in Phase 2 — supplies that event id,
    which is the synthetic state_change observation announcing the
    discontinuity was detected. This gives the audit chain a clean
    causal link: anomaly observation → hypothesis Model → user ratification.

    `to_claim_op_entry(born_from_event_id)` returns the
    ModelCreate-compatible dict the diff schema's `ClaimOp(op='insert',
    entry=...)` requires. The substrate validator + applier handle
    falsifier-adequacy, calibration, scope-existence, and embedding
    generation downstream.
    """

    proposition: dict[str, Any]
    natural: str
    confidence: float
    confidence_at_assertion: float
    falsifier: dict[str, Any]
    scope_actors: list[UUID]
    scope_entities: list[dict[str, Any]]
    scope_temporal: dict[str, Any]
    supporting_event_ids: list[UUID]
    supporting_model_ids: list[UUID]
    differing_fields: tuple[str, ...] = field(default_factory=tuple)
    gap_seconds: float = 0.0

    def to_claim_op_entry(self, *, born_from_event_id: UUID) -> dict[str, Any]:
        return {
            "proposition": dict(self.proposition),
            "natural": self.natural,
            "confidence": self.confidence,
            "confidence_at_assertion": self.confidence_at_assertion,
            "falsifier": dict(self.falsifier),
            "scope_actors": list(self.scope_actors),
            "scope_entities": [dict(e) for e in self.scope_entities],
            "scope_temporal": dict(self.scope_temporal),
            "supporting_event_ids": list(self.supporting_event_ids),
            "supporting_model_ids": list(self.supporting_model_ids),
            "born_from_event_id": born_from_event_id,
        }


# ---------------------------------------------------------------------
# Source-Model snapshot — minimal contract from the substrate
# ---------------------------------------------------------------------


@dataclass(frozen=True)
class SourceModelSnapshot:
    """The subset of source Model state the imputer needs.

    Kept narrow so the imputer can be exercised in tests without staging
    a full Model row. The T3 handler in Phase 2 will populate this from
    a SELECT on the `models` table.
    """

    model_id: UUID
    natural: str
    scope_actors: list[UUID] = field(default_factory=list)
    scope_entities: list[dict[str, Any]] = field(default_factory=list)
    scope_temporal: dict[str, Any] = field(default_factory=dict)


# ---------------------------------------------------------------------
# Natural-language rendering
# ---------------------------------------------------------------------


def _format_value_for_natural(value: Any) -> str:
    """Render a JSONB-derived value compactly for the hypothesis text.

    Strings are quoted, primitives are repr'd, dict/list compressed to a
    short summary. Goal: legible single-line summary even for complex
    snapshot fragments.
    """
    if value is None:
        return "null"
    if isinstance(value, str):
        s = value.strip()
        if len(s) > 32:
            s = s[:29] + "..."
        return f"\u201c{s}\u201d"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, dict):
        keys = list(value.keys())[:3]
        more = "" if len(value) <= 3 else f", +{len(value) - 3} more"
        return "{" + ", ".join(str(k) for k in keys) + more + "}"
    if isinstance(value, (list, tuple)):
        more = "" if len(value) <= 3 else f", +{len(value) - 3} more"
        sample = ", ".join(_format_value_for_natural(v) for v in list(value)[:3])
        return f"[{sample}{more}]"
    return repr(value)


def _format_field_diff(
    field_name: str,
    prev_value: Any,
    next_value: Any,
) -> str:
    return (
        f"{field_name}: {_format_value_for_natural(prev_value)} \u2192 "
        f"{_format_value_for_natural(next_value)}"
    )


def _format_ts(ts: datetime) -> str:
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


def _build_hypothesis_text(
    discontinuity: MissingTransitionDiscontinuity,
    source: SourceModelSnapshot,
) -> str:
    diffs = [
        _format_field_diff(
            f,
            discontinuity.prev_state.get(f),
            discontinuity.next_state.get(f),
        )
        for f in discontinuity.differing_fields[:5]
    ]
    diff_str = "; ".join(diffs)
    if len(discontinuity.differing_fields) > 5:
        diff_str += f"; +{len(discontinuity.differing_fields) - 5} more"
    return (
        f"Between {_format_ts(discontinuity.prev_event_occurred_at)} and "
        f"{_format_ts(discontinuity.next_event_occurred_at)}, an unrecorded "
        f"mutation of model \u201c{source.natural[:60]}\u201d likely "
        f"occurred. Observed diff across the gap: {diff_str}."
    )


def _build_falsifier(
    discontinuity: MissingTransitionDiscontinuity,
    natural: str,
) -> dict[str, Any]:
    """Construct an `observation_pattern` falsifier.

    The pattern asks for explicit evidence (memo, message, ticket, voice
    memo) describing what happened in the bracketed window. If such
    evidence arrives within the window, the substrate's falsifier
    evaluator will downgrade or archive the hypothesis. Length is well
    above the substrate's 20-char minimum.
    """
    pattern = (
        "Explicit ingestion evidence (memo, message, ticket, conversation, "
        "or actor-attested observation) describing the state change of "
        f"fields [{', '.join(discontinuity.differing_fields)}] between "
        f"{_format_ts(discontinuity.prev_event_occurred_at)} and "
        f"{_format_ts(discontinuity.next_event_occurred_at)}."
    )
    return {
        "kind": "observation_pattern",
        "pattern": pattern,
        "within_window": _FALSIFIER_WINDOW,
    }


def _build_proposition(
    hypothesis_text: str,
    discontinuity: MissingTransitionDiscontinuity,
) -> dict[str, Any]:
    """Build the proposition dict.

    `legacy_kind='hypothesis'` preserves compatibility with older
    readers, but the current proposition validator only derives grammar
    defaults when `kind` itself is legacy. Because this imputer emits
    canonical `kind='belief'`, it must include the memory-grammar fields
    explicitly so validation and the ratification surface both see a
    real hypothesis.

    `is_system_hypothesis=True` is the provenance flag downstream code
    uses to (a) cap calibration confidence, (b) drive faster decay, and
    (c) route to the ratification surface. It rides inside `proposition`
    rather than as a new column so no migration is required for the
    vertical slice.
    """
    return {
        "kind": "belief",
        "legacy_kind": "hypothesis",
        "claim_role": "hypothesis",
        "abstraction_level": "atomic",
        "time_mode": "unspecified",
        "modality": "inferred",
        "polarity": "neutral",
        "hypothesis_text": hypothesis_text,
        "is_system_hypothesis": True,
        "imputation_source": "missing_transition_detector_v1",
        "bracketed_event_ids": [
            int(discontinuity.prev_event_id)
            if discontinuity.prev_event_id is not None else None,
            int(discontinuity.next_event_id)
            if discontinuity.next_event_id is not None else None,
        ],
        "differing_fields": list(discontinuity.differing_fields),
    }


# ---------------------------------------------------------------------
# Public entrypoint
# ---------------------------------------------------------------------


def impute_hypothesis(
    discontinuity: MissingTransitionDiscontinuity,
    source: SourceModelSnapshot,
) -> ImputedHypothesis:
    """Synthesize a hypothesis Model from a substrate discontinuity.

    Pure function. Caller assigns `born_from_event_id` at persistence
    time via `ImputedHypothesis.to_claim_op_entry()`.

    Invariants enforced here (and verified by stepped-up tests):
      - confidence \u2208 [floor, ceiling] (system-hypothesized cap)
      - confidence == confidence_at_assertion (both immutable post-insert)
      - proposition.kind == 'belief'
      - proposition.legacy_kind == 'hypothesis' (legacy compatibility)
      - proposition.claim_role == 'hypothesis' (modern grammar contract)
      - proposition.is_system_hypothesis is True
      - hypothesis_text is non-empty and references the bracketed timestamps
      - falsifier.kind == 'observation_pattern' with pattern >= 20 chars
      - supporting_model_ids includes the source model
      - scope inherited from source
      - differing_fields is non-empty
    """
    if not discontinuity.differing_fields:
        raise ValueError(
            "impute_hypothesis requires a discontinuity with at least one "
            "differing field (call sites must pre-filter)"
        )
    if discontinuity.model_id != source.model_id:
        raise ValueError(
            f"source.model_id {source.model_id} does not match "
            f"discontinuity.model_id {discontinuity.model_id}"
        )

    natural = _build_hypothesis_text(discontinuity, source)
    confidence = compute_imputed_confidence(
        differing_fields=discontinuity.differing_fields,
        gap_seconds=discontinuity.gap_seconds,
    )
    proposition = _build_proposition(natural, discontinuity)
    falsifier = _build_falsifier(discontinuity, natural)

    supporting_event_ids: list[UUID] = []
    if discontinuity.prev_event_cause_id is not None:
        supporting_event_ids.append(discontinuity.prev_event_cause_id)
    if discontinuity.next_event_cause_id is not None:
        supporting_event_ids.append(discontinuity.next_event_cause_id)

    return ImputedHypothesis(
        proposition=proposition,
        natural=natural,
        confidence=confidence,
        confidence_at_assertion=confidence,
        falsifier=falsifier,
        scope_actors=list(source.scope_actors),
        scope_entities=[dict(e) for e in source.scope_entities],
        scope_temporal=dict(source.scope_temporal),
        supporting_event_ids=supporting_event_ids,
        supporting_model_ids=[source.model_id],
        differing_fields=discontinuity.differing_fields,
        gap_seconds=discontinuity.gap_seconds,
    )


__all__ = [
    "ImputedHypothesis",
    "SourceModelSnapshot",
    "SYSTEM_HYPOTHESIS_CONFIDENCE_CEILING",
    "SYSTEM_HYPOTHESIS_CONFIDENCE_FLOOR",
    "compute_imputed_confidence",
    "impute_hypothesis",
]
