from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace
from uuid import UUID, uuid4

import pytest

from services.platform.execution import inquiry, retrieval_actions
from services.reasoning.retrieval.primary import TriggerContext


def _trigger(text: str = "Generic customer dependency status") -> TriggerContext:
    return TriggerContext(
        kind="T1",
        tenant_id=uuid4(),
        seed_entity_ids=[],
        scope_actors=[],
        seed_natural_text=text,
        seed_occurred_at=datetime(2026, 6, 17, 8, 0, tzinfo=timezone.utc),
    )


class _ExplodingConn:
    async def fetchval(self, *_args: object, **_kwargs: object) -> object:
        raise AssertionError("generic hybrid lookup should not touch the database")

    async def fetch(self, *_args: object, **_kwargs: object) -> object:
        raise AssertionError("generic hybrid lookup should not touch the database")


def test_inquiry_private_aliases_point_to_retrieval_actions_module() -> None:
    assert (
        inquiry._execute_focused_index_action
        is retrieval_actions.execute_focused_index_action
    )
    assert (
        inquiry._execute_semantic_hybrid_action
        is retrieval_actions.execute_semantic_hybrid_action
    )
    assert inquiry._cap_pathway_models is retrieval_actions.cap_pathway_models
    assert (
        inquiry._fetch_bounded_lookup_rows
        is retrieval_actions.fetch_bounded_lookup_rows
    )
    assert (
        inquiry._merge_hybrid_semantic_lexical_models
        is retrieval_actions.merge_hybrid_semantic_lexical_models
    )


def test_focused_seed_entity_pairs_expands_customer_resource_aliases() -> None:
    entity_id = uuid4()

    pairs = retrieval_actions.focused_seed_entity_pairs(
        [
            {"type": "customer", "id": str(entity_id)},
            {"type": "resource", "id": str(entity_id)},
            {"type": "commitment", "id": str(uuid4())},
            {"type": "bad", "id": "not-a-uuid"},
        ]
    )

    pair_set = {(kind, UUID(str(raw_id))) for kind, raw_id in pairs}
    assert {
        ("customer", entity_id),
        ("customer_resource", entity_id),
        ("resource", entity_id),
    } <= pair_set
    assert len([pair for pair in pairs if pair[1] == entity_id]) == 3


def test_merge_hybrid_semantic_lexical_models_prefers_cross_signal_hits() -> None:
    semantic_first = SimpleNamespace(id=uuid4())
    cross_signal = SimpleNamespace(id=uuid4())
    lexical_only = SimpleNamespace(id=uuid4())

    merged = retrieval_actions.merge_hybrid_semantic_lexical_models(
        [semantic_first, cross_signal],
        [(lexical_only, 3), (cross_signal, 2)],
        limit=2,
    )

    assert [model.id for model in merged] == [cross_signal.id, lexical_only.id]


@pytest.mark.asyncio
async def test_hybrid_lexical_scan_skips_generic_lookup_terms() -> None:
    hits = await retrieval_actions.hybrid_lexical_model_scan(
        _trigger(),
        _ExplodingConn(),  # type: ignore[arg-type]
        terms=["owner responsible assigned dependency evidence blocker customer"],
        limit=8,
        per_term_limit=4,
    )

    assert hits == []
