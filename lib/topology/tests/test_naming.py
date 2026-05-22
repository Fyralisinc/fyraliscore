"""Unit tests for lib.topology.naming.derive_signature."""
from __future__ import annotations

from uuid import uuid4

import pytest

from lib.topology.naming import (
    MemberSummary,
    derive_signature,
    member_summaries_from_rows,
)


def _summary(
    proposition_kind: str | None = "state",
    actors=(),
    entities=(),
    actor_labels=None,
    entity_labels=None,
) -> MemberSummary:
    return MemberSummary(
        model_id=uuid4(),
        proposition_kind=proposition_kind,
        scope_actor_ids=tuple(actors),
        scope_entity_refs=tuple(entities),
        actor_labels=actor_labels,
        entity_labels=entity_labels,
    )


def test_empty_returns_unnamed():
    assert derive_signature([]) == "unnamed"


def test_single_kind_only():
    members = [_summary(proposition_kind="state") for _ in range(3)]
    assert derive_signature(members) == "state"


def test_kinds_combined_top_n():
    # 3 state, 2 recommendation, 1 prediction → all three kept (cap=3).
    members = (
        [_summary(proposition_kind="state") for _ in range(3)]
        + [_summary(proposition_kind="recommendation") for _ in range(2)]
        + [_summary(proposition_kind="prediction")]
    )
    sig = derive_signature(members)
    assert sig.startswith("state+recommendation+prediction")


def test_kinds_alphabetical_tiebreak():
    # Equal counts → alphabetical.
    members = [
        _summary(proposition_kind="recommendation"),
        _summary(proposition_kind="state"),
    ]
    sig = derive_signature(members)
    # Two kinds tie at 1 each → alpha sort puts recommendation first.
    assert sig == "recommendation+state"


def test_entity_scope():
    cust_id = "abcd-cust-id-aaaa"
    members = [
        _summary(
            proposition_kind="state",
            entities=[("customer", cust_id)],
            entity_labels={("customer", cust_id): "Globex"},
        )
        for _ in range(3)
    ]
    sig = derive_signature(members)
    assert "customer:Globex" in sig
    assert sig.startswith("state @")


def test_actor_scope_with_labels():
    a = uuid4()
    b = uuid4()
    members = [
        _summary(
            proposition_kind="recommendation",
            actors=[a, b],
            actor_labels={a: "Carmen", b: "Sarah"},
        ),
        _summary(
            proposition_kind="recommendation",
            actors=[a],
            actor_labels={a: "Carmen"},
        ),
    ]
    sig = derive_signature(members)
    # Carmen appears twice (count=2) before Sarah (count=1).
    assert "Carmen" in sig
    assert sig.index("Carmen") < sig.index("Sarah")


def test_actor_uuid_fallback_when_no_label():
    a = uuid4()
    members = [
        _summary(proposition_kind="state", actors=[a]),
    ]
    sig = derive_signature(members)
    # First 8 chars of the UUID land in the signature.
    assert str(a)[:8] in sig


def test_no_kinds_no_scope_returns_unnamed():
    members = [_summary(proposition_kind=None)]
    assert derive_signature(members) == "unnamed"


def test_combined_kinds_entities_and_actors():
    cid = "11111111-2222-3333-4444-555555555555"
    a = uuid4()
    members = [
        _summary(
            proposition_kind="state",
            actors=[a],
            entities=[("commitment", cid)],
            actor_labels={a: "Bob"},
            entity_labels={("commitment", cid): "Refund flow"},
        )
        for _ in range(2)
    ]
    sig = derive_signature(members)
    assert "state @" in sig
    assert "commitment:Refund flow" in sig
    assert "Bob" in sig
    assert " / " in sig


def test_signature_truncated_when_long():
    # The top-3 entity cap limits raw expansion, so to force truncation
    # we use very long entity labels that overflow the 120-char cap.
    long_label = "x" * 200
    eid = "11111111-2222-3333-4444-555555555555"
    members = [
        _summary(
            proposition_kind="state",
            entities=[("commitment", eid)],
            entity_labels={("commitment", eid): long_label},
        ),
    ]
    sig = derive_signature(members)
    assert len(sig) <= 120
    assert sig.endswith("…")


def test_member_summaries_from_rows_handles_uuid_strings():
    a = uuid4()
    rows = [
        {
            "id": uuid4(),
            "proposition_kind": "state",
            "scope_actors": [str(a)],
            "scope_entities": [{"type": "commitment", "id": "11"}],
        }
    ]
    summaries = member_summaries_from_rows(rows)
    assert summaries[0].scope_actor_ids[0] == a


def test_member_summaries_from_rows_skips_invalid_actors():
    rows = [
        {
            "id": uuid4(),
            "proposition_kind": "state",
            "scope_actors": ["not-a-uuid"],
            "scope_entities": [],
        }
    ]
    summaries = member_summaries_from_rows(rows)
    assert summaries[0].scope_actor_ids == ()


def test_member_summaries_from_rows_skips_invalid_entities():
    rows = [
        {
            "id": uuid4(),
            "proposition_kind": "state",
            "scope_actors": [],
            "scope_entities": [{"type": "commitment"}, "garbage", None],
        }
    ]
    summaries = member_summaries_from_rows(rows)
    assert summaries[0].scope_entity_refs == ()
