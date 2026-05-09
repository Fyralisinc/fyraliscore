"""Pure-Python tests for the phase-event detector
(services.topology.events_repo.detect_phase_events). Exercises the
emergence / dissolution / split / merge / drift taxonomy without
hitting a database."""
from __future__ import annotations

from uuid import uuid4

import pytest

from services.topology.events_repo import (
    DRIFT_JACCARD_THRESHOLD,
    PrevSnapshot,
    detect_phase_events,
)


def _ids(n):
    return [uuid4() for _ in range(n)]


def test_emergence_when_new_community_has_no_prior_overlap():
    tenant = uuid4()
    a, b = _ids(2)
    new_label = 0
    new_neighborhood_id = uuid4()
    events = detect_phase_events(
        tenant_id=tenant,
        prev_neighborhoods=[],
        new_communities={new_label: {a, b}},
        label_to_neighborhood_id={new_label: new_neighborhood_id},
        matched_prev_ids_by_label={new_label: None},
    )
    assert len(events) == 1
    e = events[0]
    assert e.kind == "emergence"
    assert e.neighborhood_id == new_neighborhood_id
    assert set(e.member_model_ids) == {a, b}
    assert e.magnitude == 2.0


def test_dissolution_when_prior_has_no_new_overlap():
    tenant = uuid4()
    prev_id = uuid4()
    a, b = _ids(2)
    events = detect_phase_events(
        tenant_id=tenant,
        prev_neighborhoods=[
            PrevSnapshot(id=prev_id, members=frozenset({a, b}))
        ],
        new_communities={},
        label_to_neighborhood_id={},
        matched_prev_ids_by_label={},
    )
    assert len(events) == 1
    assert events[0].kind == "dissolution"
    assert events[0].neighborhood_id == prev_id
    assert events[0].magnitude == 2.0


def test_drift_when_matched_prior_has_high_jaccard_distance():
    """Matched neighborhood whose membership churned > threshold."""
    tenant = uuid4()
    prev_id = uuid4()
    new_label = 0
    new_id = uuid4()
    # 3 prev members, 2 new members; only 1 shared → Jaccard = 1/4 = 0.25
    # → distance = 0.75 > 0.4 threshold → drift.
    a, b, c = _ids(3)
    d, e = _ids(2)
    events = detect_phase_events(
        tenant_id=tenant,
        prev_neighborhoods=[
            PrevSnapshot(id=prev_id, members=frozenset({a, b, c}))
        ],
        new_communities={new_label: {a, d, e}},
        label_to_neighborhood_id={new_label: new_id},
        matched_prev_ids_by_label={new_label: prev_id},
    )
    drifts = [ev for ev in events if ev.kind == "drift"]
    assert len(drifts) == 1
    assert drifts[0].neighborhood_id == new_id
    assert drifts[0].magnitude is not None
    assert drifts[0].magnitude > DRIFT_JACCARD_THRESHOLD


def test_no_drift_when_membership_mostly_stable():
    tenant = uuid4()
    prev_id = uuid4()
    new_label = 0
    new_id = uuid4()
    a, b, c = _ids(3)
    # 3 prev, 3 new, share 3 → Jaccard 1.0, distance 0 — no drift.
    events = detect_phase_events(
        tenant_id=tenant,
        prev_neighborhoods=[
            PrevSnapshot(id=prev_id, members=frozenset({a, b, c}))
        ],
        new_communities={new_label: {a, b, c}},
        label_to_neighborhood_id={new_label: new_id},
        matched_prev_ids_by_label={new_label: prev_id},
    )
    assert events == []


def test_merge_when_two_priors_collapse_into_one_new():
    tenant = uuid4()
    prev1, prev2 = uuid4(), uuid4()
    new_label = 0
    new_id = uuid4()
    a, b, c, d = _ids(4)
    events = detect_phase_events(
        tenant_id=tenant,
        prev_neighborhoods=[
            PrevSnapshot(id=prev1, members=frozenset({a, b})),
            PrevSnapshot(id=prev2, members=frozenset({c, d})),
        ],
        new_communities={new_label: {a, b, c, d}},
        label_to_neighborhood_id={new_label: new_id},
        matched_prev_ids_by_label={new_label: None},
    )
    merges = [ev for ev in events if ev.kind == "merge"]
    assert len(merges) == 1
    m = merges[0]
    assert m.neighborhood_id == new_id
    assert set(m.predecessor_neighborhood_ids) == {prev1, prev2}
    assert m.magnitude == 4.0


def test_split_when_one_prior_spans_multiple_new_communities():
    tenant = uuid4()
    prev_id = uuid4()
    a, b, c, d = _ids(4)
    label1, label2 = 0, 1
    n1, n2 = uuid4(), uuid4()
    events = detect_phase_events(
        tenant_id=tenant,
        prev_neighborhoods=[
            PrevSnapshot(id=prev_id, members=frozenset({a, b, c, d}))
        ],
        new_communities={label1: {a, b}, label2: {c, d}},
        label_to_neighborhood_id={label1: n1, label2: n2},
        matched_prev_ids_by_label={label1: None, label2: None},
    )
    splits = [ev for ev in events if ev.kind == "split"]
    assert len(splits) == 1
    s = splits[0]
    # The "about" neighborhood is the largest-share child; ties on
    # share size break alphabetically by label, which means label1.
    assert s.neighborhood_id in (n1, n2)
    assert prev_id in s.predecessor_neighborhood_ids
    other_id = n2 if s.neighborhood_id == n1 else n1
    assert other_id in s.sibling_neighborhood_ids
    # split balance for an even 50/50 split = 1 - 2/4 = 0.5.
    assert s.magnitude == pytest.approx(0.5)


def test_split_does_not_double_emit_emergence():
    """Children of a split should NOT also emit emergence (they have
    overlap with priors)."""
    tenant = uuid4()
    prev_id = uuid4()
    a, b, c, d = _ids(4)
    events = detect_phase_events(
        tenant_id=tenant,
        prev_neighborhoods=[
            PrevSnapshot(id=prev_id, members=frozenset({a, b, c, d}))
        ],
        new_communities={0: {a, b}, 1: {c, d}},
        label_to_neighborhood_id={0: uuid4(), 1: uuid4()},
        matched_prev_ids_by_label={0: None, 1: None},
    )
    kinds = sorted(ev.kind for ev in events)
    # We expect exactly one split, no emergence (not a typo: the
    # detector treats prior-overlapping new communities as split
    # children, not emergence).
    assert "emergence" not in kinds


def test_concurrent_emergence_dissolution_and_drift_in_one_sweep():
    tenant = uuid4()
    # Prior 1 dissolves, prior 2 drifts, plus a brand-new community.
    p1, p2 = uuid4(), uuid4()
    a, b, c, d, e = _ids(5)
    f, g, h = _ids(3)
    events = detect_phase_events(
        tenant_id=tenant,
        prev_neighborhoods=[
            PrevSnapshot(id=p1, members=frozenset({a, b})),
            PrevSnapshot(id=p2, members=frozenset({c, d, e})),
        ],
        new_communities={
            # p2 drifts: only c remains; d&e gone, plus fresh members.
            0: {c, f},
            # brand new (no overlap with any prior)
            1: {g, h},
        },
        label_to_neighborhood_id={0: uuid4(), 1: uuid4()},
        matched_prev_ids_by_label={0: p2, 1: None},
    )
    kinds = sorted(ev.kind for ev in events)
    assert "dissolution" in kinds
    assert "emergence" in kinds
    assert "drift" in kinds


def test_member_signature_populated_when_summaries_provided():
    """The detector calls into the namer for each event."""
    from lib.topology.naming import MemberSummary

    tenant = uuid4()
    a, b = _ids(2)
    summaries = {
        a: MemberSummary(
            model_id=a,
            proposition_kind="state",
            scope_actor_ids=(),
            scope_entity_refs=(),
        ),
        b: MemberSummary(
            model_id=b,
            proposition_kind="recommendation",
            scope_actor_ids=(),
            scope_entity_refs=(),
        ),
    }
    events = detect_phase_events(
        tenant_id=tenant,
        prev_neighborhoods=[],
        new_communities={0: {a, b}},
        label_to_neighborhood_id={0: uuid4()},
        matched_prev_ids_by_label={0: None},
        member_summaries_by_id=summaries,
    )
    assert len(events) == 1
    sig = events[0].named_signature
    assert sig is not None
    assert "state" in sig
    assert "recommendation" in sig
