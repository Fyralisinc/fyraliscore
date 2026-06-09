"""Pure-function tests for the hypothesis imputer.

No database required — the imputer is a pure transform from
`(MissingTransitionDiscontinuity, SourceModelSnapshot)` to
`ImputedHypothesis`. That purity is the whole point: it makes the
calibration regression tests below tractable at 200+ hypothesis
examples per property.

Test bar (stepped-up):
  - calibration ordering: confidence monotonic in discontinuity strength
  - bounded confidence: NEVER exceeds the system-hypothesized ceiling,
    NEVER falls below the floor
  - structural invariants: output is ModelCreate-compatible and would
    derive `claim_role='hypothesis'` via memory_grammar
  - falsifier adequacy: well-formed observation_pattern that the
    substrate validator will accept
  - provenance: supporting_model_ids includes source; scope inherited
  - adversarial: empty diff raises; mismatched IDs raise; volatile-only
    diff cannot survive (already filtered upstream but defensive)
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from uuid import UUID, uuid4

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from lib.shared.memory_grammar import derive_memory_grammar
from services.models.propositions import validate_proposition
from services.dynamics.detectors import MissingTransitionDiscontinuity
from services.dynamics.hypothesis_imputer import (
    ImputedHypothesis,
    SourceModelSnapshot,
    SYSTEM_HYPOTHESIS_CONFIDENCE_CEILING,
    SYSTEM_HYPOTHESIS_CONFIDENCE_FLOOR,
    compute_imputed_confidence,
    impute_hypothesis,
)


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------


def _disc(
    *,
    model_id: UUID | None = None,
    differing_fields: tuple[str, ...] = ("status",),
    gap: timedelta = timedelta(hours=6),
    prev_state: dict | None = None,
    next_state: dict | None = None,
) -> MissingTransitionDiscontinuity:
    model_id = model_id or uuid4()
    now = datetime(2026, 5, 31, 12, 0, tzinfo=timezone.utc)
    prev_state = prev_state or {"status": "active", "score": 5}
    next_state = next_state or {"status": "blocked", "score": 5}
    return MissingTransitionDiscontinuity(
        model_id=model_id,
        prev_event_id=101,
        next_event_id=102,
        prev_event_occurred_at=now - gap,
        next_event_occurred_at=now,
        prev_event_cause_id=uuid4(),
        next_event_cause_id=uuid4(),
        prev_state=prev_state,
        next_state=next_state,
        differing_fields=differing_fields,
    )


def _src(model_id: UUID) -> SourceModelSnapshot:
    return SourceModelSnapshot(
        model_id=model_id,
        natural="Commitment to ship the dashboard rewrite by Q3",
        scope_actors=[uuid4(), uuid4()],
        scope_entities=[
            {"type": "commitment", "id": str(uuid4())},
            {"type": "customer", "id": str(uuid4())},
        ],
        scope_temporal={
            "valid_from": "2026-01-01T00:00:00Z",
            "valid_until": None,
        },
    )


# =====================================================================
# compute_imputed_confidence — calibration regression
# =====================================================================


@settings(max_examples=300)
@given(
    n_fields=st.integers(min_value=1, max_value=30),
    gap_seconds=st.floats(
        min_value=0.0, max_value=60.0 * 60.0 * 24.0 * 365.0,
        allow_nan=False, allow_infinity=False,
    ),
)
def test_confidence_in_system_hypothesis_band(
    n_fields: int, gap_seconds: float,
) -> None:
    """The load-bearing invariant: imputed confidence is strictly in
    [floor, ceiling]. The system-hypothesized lineage must never out-
    confidence directly-observed claims."""
    differing = tuple(f"f{i}" for i in range(n_fields))
    c = compute_imputed_confidence(
        differing_fields=differing, gap_seconds=gap_seconds
    )
    assert SYSTEM_HYPOTHESIS_CONFIDENCE_FLOOR <= c
    assert c <= SYSTEM_HYPOTHESIS_CONFIDENCE_CEILING


@settings(max_examples=200)
@given(
    base_fields=st.integers(min_value=1, max_value=4),
    extra_fields=st.integers(min_value=1, max_value=20),
    gap_seconds=st.floats(
        min_value=0.0, max_value=60.0 * 60.0 * 24.0 * 7.0,
        allow_nan=False, allow_infinity=False,
    ),
)
def test_confidence_monotonic_in_field_count(
    base_fields: int, extra_fields: int, gap_seconds: float,
) -> None:
    """Wider divergence (more differing fields) cannot lower the
    confidence — this is the calibration ordering for the field axis."""
    base = tuple(f"f{i}" for i in range(base_fields))
    wider = tuple(f"f{i}" for i in range(base_fields + extra_fields))
    c_base = compute_imputed_confidence(
        differing_fields=base, gap_seconds=gap_seconds
    )
    c_wider = compute_imputed_confidence(
        differing_fields=wider, gap_seconds=gap_seconds
    )
    assert c_wider >= c_base


@settings(max_examples=200)
@given(
    n_fields=st.integers(min_value=1, max_value=8),
    tight_gap=st.floats(
        min_value=0.0, max_value=60.0 * 60.0,
        allow_nan=False, allow_infinity=False,
    ),
    extra_gap=st.floats(
        min_value=60.0 * 60.0 * 24.0,
        max_value=60.0 * 60.0 * 24.0 * 25.0,
        allow_nan=False, allow_infinity=False,
    ),
)
def test_confidence_decays_with_gap(
    n_fields: int, tight_gap: float, extra_gap: float,
) -> None:
    """A larger gap (same divergence) cannot increase confidence — this
    is the calibration ordering for the temporal axis."""
    differing = tuple(f"f{i}" for i in range(n_fields))
    c_tight = compute_imputed_confidence(
        differing_fields=differing, gap_seconds=tight_gap
    )
    c_huge = compute_imputed_confidence(
        differing_fields=differing, gap_seconds=tight_gap + extra_gap,
    )
    assert c_tight >= c_huge


def test_confidence_zero_fields_returns_floor() -> None:
    """Pathological input: caller forgot to pre-filter (the public
    imputer raises, but the math function must still degrade
    gracefully)."""
    assert (
        compute_imputed_confidence(differing_fields=(), gap_seconds=0.0)
        == SYSTEM_HYPOTHESIS_CONFIDENCE_FLOOR
    )


def test_confidence_int_count_accepted() -> None:
    """`differing_fields` accepts either a tuple or a bare int count for
    quick probing — tests assume this convenience exists."""
    c_tuple = compute_imputed_confidence(
        differing_fields=("a", "b", "c"), gap_seconds=0.0,
    )
    c_int = compute_imputed_confidence(
        differing_fields=3, gap_seconds=0.0,
    )
    assert c_tuple == c_int


def test_confidence_strict_ceiling_under_unbounded_fields() -> None:
    """Even with 1000 differing fields and zero gap, confidence must not
    exceed the ceiling. This guards against accidental coefficient
    explosion in future refactors."""
    huge = tuple(f"f{i}" for i in range(1000))
    c = compute_imputed_confidence(differing_fields=huge, gap_seconds=0.0)
    assert c <= SYSTEM_HYPOTHESIS_CONFIDENCE_CEILING


# =====================================================================
# Expected Calibration Error (ECE) regression
#
# The test simulates a discrete grid of (n_fields, gap_seconds) inputs
# and verifies the imputer's confidence ordering correlates with a
# canonical "discontinuity intensity" score. We use rank correlation
# rather than absolute calibration because v1 has no user-approval
# data yet — the bar is "the model's confidence ordering tracks an
# intuitive intensity ordering."
# =====================================================================


def _intensity_score(n_fields: int, gap_seconds: float) -> float:
    """Canonical 'intuitive intensity' of a discontinuity used as the
    ground-truth ordering for the rank-correlation regression test.
    Designed to be monotone in field count and inversely monotone in
    gap, so a well-calibrated imputer should match this ordering."""
    gap_days = max(0.0, gap_seconds) / 86400.0
    field_score = min(1.0, 0.4 + 0.15 * (n_fields - 1))
    gap_score = max(0.0, 1.0 - gap_days / 30.0)
    return field_score * gap_score


def test_confidence_rank_correlation_with_intensity() -> None:
    """Rank-order correlation between imputed confidence and intuitive
    intensity. Spearman ρ ≥ 0.95 across a 6×6 grid (36 points) keeps
    the imputer firmly in 'calibrated ordering' territory.

    This is the closest we can get to a calibration regression without
    user-approval data. When we have it (Phase 5+), this test should be
    replaced with an Expected Calibration Error (ECE) regression."""
    field_counts = [1, 2, 3, 5, 8, 15]
    gaps_seconds = [
        60.0,
        60.0 * 60.0,
        60.0 * 60.0 * 6.0,
        60.0 * 60.0 * 24.0,
        60.0 * 60.0 * 24.0 * 7.0,
        60.0 * 60.0 * 24.0 * 25.0,
    ]
    rows: list[tuple[float, float]] = []
    for n in field_counts:
        for g in gaps_seconds:
            c = compute_imputed_confidence(
                differing_fields=tuple(f"f{i}" for i in range(n)),
                gap_seconds=g,
            )
            i = _intensity_score(n, g)
            rows.append((c, i))

    rho = _spearman(
        [r[0] for r in rows], [r[1] for r in rows]
    )
    assert rho >= 0.95, f"calibration ordering regressed: rho={rho:.3f}"


def _spearman(xs: list[float], ys: list[float]) -> float:
    """Spearman rank correlation. Hand-rolled to avoid a scipy
    dependency in unit tests — the suite stays runnable without scipy
    even when the dev extra is incomplete."""
    n = len(xs)
    if n != len(ys) or n < 2:
        raise ValueError("vectors must be same length and ≥ 2")

    def ranks(vs: list[float]) -> list[float]:
        idx = sorted(range(n), key=lambda i: vs[i])
        out = [0.0] * n
        i = 0
        while i < n:
            j = i
            while j + 1 < n and vs[idx[j + 1]] == vs[idx[i]]:
                j += 1
            avg = (i + j) / 2.0 + 1.0  # 1-indexed midrank
            for k in range(i, j + 1):
                out[idx[k]] = avg
            i = j + 1
        return out

    rx = ranks(xs)
    ry = ranks(ys)
    mean_rx = sum(rx) / n
    mean_ry = sum(ry) / n
    num = sum((rx[i] - mean_rx) * (ry[i] - mean_ry) for i in range(n))
    denx = (sum((rx[i] - mean_rx) ** 2 for i in range(n))) ** 0.5
    deny = (sum((ry[i] - mean_ry) ** 2 for i in range(n))) ** 0.5
    if denx == 0 or deny == 0:
        return 0.0
    return num / (denx * deny)


def test_spearman_helper_self_check() -> None:
    """Sanity-check the hand-rolled Spearman against known cases so the
    calibration regression test above can't false-pass."""
    assert _spearman([1, 2, 3, 4], [1, 2, 3, 4]) == pytest.approx(1.0)
    assert _spearman([1, 2, 3, 4], [4, 3, 2, 1]) == pytest.approx(-1.0)


# =====================================================================
# impute_hypothesis — structural invariants
# =====================================================================


def test_impute_returns_imputed_hypothesis_dataclass() -> None:
    model_id = uuid4()
    disc = _disc(model_id=model_id)
    src = _src(model_id)
    out = impute_hypothesis(disc, src)
    assert isinstance(out, ImputedHypothesis)


def test_impute_confidence_matches_at_assertion() -> None:
    """Substrate invariant: `confidence` and `confidence_at_assertion`
    are equal at insert time (the latter is the immutable snapshot)."""
    model_id = uuid4()
    out = impute_hypothesis(_disc(model_id=model_id), _src(model_id))
    assert out.confidence == out.confidence_at_assertion


def test_impute_proposition_derives_to_hypothesis_role() -> None:
    """Round-trip through `derive_memory_grammar`: the proposition the
    imputer builds must derive to claim_role='hypothesis', so the
    substrate's claim-role registry validates it as a hypothesis card.

    This is the load-bearing structural test: the entire ratification
    surface depends on the substrate seeing claim_role='hypothesis'."""
    model_id = uuid4()
    out = impute_hypothesis(_disc(model_id=model_id), _src(model_id))
    grammar = derive_memory_grammar(
        out.proposition,
        natural=out.natural,
        scope_entities=out.scope_entities,
    )
    assert grammar.claim_role == "hypothesis"
    assert grammar.modality == "inferred"
    assert grammar.abstraction_level == "atomic"
    assert grammar.time_mode == "unspecified"
    assert out.proposition["claim_role"] == "hypothesis"
    assert out.proposition["abstraction_level"] == "atomic"
    assert out.proposition["time_mode"] == "unspecified"
    assert out.proposition["modality"] == "inferred"
    assert out.proposition["polarity"] == "neutral"
    parsed = validate_proposition(out.proposition)
    assert parsed.claim_role == "hypothesis"


def test_impute_proposition_carries_provenance_flag() -> None:
    """`is_system_hypothesis=True` is the provenance flag downstream
    code uses to (a) cap calibration, (b) drive faster decay, (c) route
    to the ratification surface. Must be present."""
    model_id = uuid4()
    out = impute_hypothesis(_disc(model_id=model_id), _src(model_id))
    assert out.proposition["is_system_hypothesis"] is True


def test_impute_proposition_carries_imputation_source() -> None:
    """Audit telemetry: which imputation strategy produced this
    hypothesis? Versioned so future strategy swaps remain traceable."""
    model_id = uuid4()
    out = impute_hypothesis(_disc(model_id=model_id), _src(model_id))
    assert out.proposition["imputation_source"] == "missing_transition_detector_v1"


def test_impute_proposition_carries_bracketed_event_ids() -> None:
    model_id = uuid4()
    out = impute_hypothesis(_disc(model_id=model_id), _src(model_id))
    assert out.proposition["bracketed_event_ids"] == [101, 102]


def test_impute_natural_contains_timestamps_and_diff() -> None:
    """The natural text must be self-explanatory: a CEO seeing the
    hypothesis card needs the *what* (diff) and *when* (bracket) without
    drilling into the JSONB."""
    model_id = uuid4()
    out = impute_hypothesis(_disc(model_id=model_id), _src(model_id))
    assert "2026" in out.natural
    assert "status" in out.natural
    assert "active" in out.natural and "blocked" in out.natural


def test_impute_falsifier_observation_pattern() -> None:
    model_id = uuid4()
    out = impute_hypothesis(_disc(model_id=model_id), _src(model_id))
    assert out.falsifier["kind"] == "observation_pattern"
    # Substrate falsifier adequacy requires pattern length >= 20 chars.
    assert len(out.falsifier["pattern"]) >= 20
    assert out.falsifier["within_window"] == "14d"


def test_impute_supporting_includes_source_model() -> None:
    """The source Model must appear in supporting_model_ids — this is
    the back-reference the reconciler uses when the user later corrects
    the hypothesis."""
    model_id = uuid4()
    out = impute_hypothesis(_disc(model_id=model_id), _src(model_id))
    assert out.supporting_model_ids == [model_id]


def test_impute_supporting_event_ids_bracket() -> None:
    """Both bracketing cause_ids are forwarded as supporting evidence
    so the reconciler can dedupe against re-emissions."""
    model_id = uuid4()
    disc = _disc(model_id=model_id)
    out = impute_hypothesis(disc, _src(model_id))
    assert out.supporting_event_ids == [
        disc.prev_event_cause_id,
        disc.next_event_cause_id,
    ]


def test_impute_inherits_source_scope() -> None:
    model_id = uuid4()
    src = _src(model_id)
    out = impute_hypothesis(_disc(model_id=model_id), src)
    assert out.scope_actors == src.scope_actors
    assert out.scope_entities == src.scope_entities
    assert out.scope_temporal == src.scope_temporal


def test_impute_to_claim_op_entry_complete() -> None:
    """The dict returned by `to_claim_op_entry` must carry every column
    the substrate INSERT needs (modulo `born_from_event_id` which the
    caller supplies)."""
    model_id = uuid4()
    out = impute_hypothesis(_disc(model_id=model_id), _src(model_id))
    born = uuid4()
    entry = out.to_claim_op_entry(born_from_event_id=born)
    required = {
        "proposition", "natural", "confidence", "confidence_at_assertion",
        "falsifier", "scope_actors", "scope_entities", "scope_temporal",
        "supporting_event_ids", "supporting_model_ids", "born_from_event_id",
    }
    assert required.issubset(entry.keys())
    assert entry["born_from_event_id"] == born


def test_impute_to_claim_op_entry_is_a_deep_copy() -> None:
    """Mutating the returned entry must not affect the immutable
    ImputedHypothesis dataclass — defensive against downstream
    bookkeeping pollution."""
    model_id = uuid4()
    out = impute_hypothesis(_disc(model_id=model_id), _src(model_id))
    entry = out.to_claim_op_entry(born_from_event_id=uuid4())
    entry["proposition"]["mutated"] = True
    entry["scope_entities"].append({"type": "rogue", "id": "x"})
    assert "mutated" not in out.proposition
    assert {"type": "rogue", "id": "x"} not in out.scope_entities


# =====================================================================
# Adversarial: pre-conditions
# =====================================================================


def test_impute_raises_on_empty_differing_fields() -> None:
    model_id = uuid4()
    disc = MissingTransitionDiscontinuity(
        model_id=model_id,
        prev_event_id=1, next_event_id=2,
        prev_event_occurred_at=datetime(2026, 5, 1, tzinfo=timezone.utc),
        next_event_occurred_at=datetime(2026, 5, 2, tzinfo=timezone.utc),
        prev_event_cause_id=None, next_event_cause_id=None,
        prev_state={"a": 1}, next_state={"a": 1},
        differing_fields=(),
    )
    with pytest.raises(ValueError, match="at least one differing field"):
        impute_hypothesis(disc, _src(model_id))


def test_impute_raises_on_model_id_mismatch() -> None:
    """A discontinuity for model A must never be paired with snapshot
    of model B — catches caller bugs where the wrong Model was loaded."""
    disc = _disc()
    other = _src(uuid4())
    with pytest.raises(ValueError, match="does not match"):
        impute_hypothesis(disc, other)


# =====================================================================
# Adversarial: edge-case state shapes
# =====================================================================


def test_impute_handles_null_prev_state() -> None:
    """`previous_state` for create events is NULL; if the imputer is
    called against a discontinuity whose `prev_state` ended up empty,
    it should still produce a coherent hypothesis."""
    model_id = uuid4()
    disc = MissingTransitionDiscontinuity(
        model_id=model_id,
        prev_event_id=1, next_event_id=2,
        prev_event_occurred_at=datetime(2026, 5, 1, tzinfo=timezone.utc),
        next_event_occurred_at=datetime(2026, 5, 2, tzinfo=timezone.utc),
        prev_event_cause_id=None, next_event_cause_id=None,
        prev_state={},
        next_state={"status": "live"},
        differing_fields=("status",),
    )
    out = impute_hypothesis(disc, _src(model_id))
    assert "status" in out.natural


def test_impute_handles_complex_nested_values() -> None:
    """Substrate snapshots can contain nested dicts/lists. The natural-
    text formatter must produce a single-line legible string."""
    model_id = uuid4()
    disc = _disc(
        model_id=model_id,
        differing_fields=("scope_entities",),
        prev_state={
            "scope_entities": [
                {"type": "customer", "id": "1"},
                {"type": "commitment", "id": "2"},
            ],
        },
        next_state={
            "scope_entities": [
                {"type": "customer", "id": "3"},
                {"type": "goal", "id": "4"},
                {"type": "resource", "id": "5"},
            ],
        },
    )
    out = impute_hypothesis(disc, _src(model_id))
    assert "\n" not in out.natural
    assert "scope_entities" in out.natural


@settings(
    max_examples=50,
    suppress_health_check=[HealthCheck.too_slow],
)
@given(
    n_fields=st.integers(min_value=1, max_value=12),
    gap_hours=st.floats(min_value=0.5, max_value=24.0 * 30.0,
                        allow_nan=False, allow_infinity=False),
)
def test_impute_natural_text_always_nonempty(
    n_fields: int, gap_hours: float,
) -> None:
    """The natural text must NEVER be empty — content_text is NOT NULL
    in the substrate observations and Models tables, and the natural
    field is what UI renders."""
    model_id = uuid4()
    fields = tuple(f"f{i}" for i in range(n_fields))
    prev_state = {f: f"old-{i}" for i, f in enumerate(fields)}
    next_state = {f: f"new-{i}" for i, f in enumerate(fields)}
    disc = MissingTransitionDiscontinuity(
        model_id=model_id,
        prev_event_id=1, next_event_id=2,
        prev_event_occurred_at=datetime(2026, 5, 1, tzinfo=timezone.utc),
        next_event_occurred_at=(
            datetime(2026, 5, 1, tzinfo=timezone.utc)
            + timedelta(hours=gap_hours)
        ),
        prev_event_cause_id=None, next_event_cause_id=None,
        prev_state=prev_state, next_state=next_state,
        differing_fields=fields,
    )
    out = impute_hypothesis(disc, _src(model_id))
    assert out.natural.strip()
    assert len(out.natural) > 20


# =====================================================================
# Frozen dataclass safety
# =====================================================================


def test_imputed_hypothesis_is_frozen() -> None:
    """Defensive: callers should not be able to mutate the dataclass in
    place. The dataclass is `frozen=True`, so attribute assignment
    raises."""
    model_id = uuid4()
    out = impute_hypothesis(_disc(model_id=model_id), _src(model_id))
    with pytest.raises(Exception):
        out.confidence = 0.99  # type: ignore[misc]
