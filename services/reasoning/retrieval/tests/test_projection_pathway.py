from __future__ import annotations

import pytest

from lib.shared.ids import uuid7
from services.reasoning.retrieval.primary import TriggerContext
from services.reasoning.retrieval import projection_pathway
from services.reasoning.retrieval.projection_pathway import (
    pathway_projection_context,
    projection_subject_candidates,
)


class _NoSnapshotProjectionRepo:
    def __init__(self) -> None:
        self.snapshot_calls = 0

    async def list_snapshots_for_subjects(self, *_args, **_kwargs) -> list[object]:
        self.snapshot_calls += 1
        return []

    async def list_staleness(self, *_args, **_kwargs) -> list[object]:
        raise AssertionError("freshness should not be loaded on projection misses")


def test_projection_subject_candidates_use_text_and_entity_scope() -> None:
    tenant_id = uuid7()
    customer_id = uuid7()
    actor_id = uuid7()
    trigger = TriggerContext(
        kind="T1",
        tenant_id=tenant_id,
        seed_natural_text="Cash runway and hiring capacity affect customer renewal.",
    )

    subjects = projection_subject_candidates(
        trigger,
        effective_seed_entities=[
            {"type": "customer_resource", "id": str(customer_id)},
        ],
        effective_scope_actors=[actor_id],
    )

    assert ("constraints", "company:runway") in subjects
    assert ("constraints", "company:financial_capacity") in subjects
    assert ("constraints", "company:capacity") in subjects
    assert ("resources", "company:financial") in subjects
    assert ("resources", "company:capacity") in subjects
    assert ("resources", "company:relational") in subjects
    assert ("constraints", f"customer:{customer_id}:constraints") in subjects
    assert ("resources", f"customer:{customer_id}:resources") in subjects
    assert ("employee_profiles", f"employee:{actor_id}:profile") in subjects


@pytest.mark.asyncio
async def test_projection_context_skips_freshness_when_no_snapshots(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tenant_id = uuid7()
    repo = _NoSnapshotProjectionRepo()

    async def fake_table_exists(_conn: object, table_name: str) -> bool:
        return table_name == "projection_snapshots"

    monkeypatch.setattr(projection_pathway, "_PROJECTIONS", repo)
    monkeypatch.setattr(projection_pathway, "_table_exists", fake_table_exists)
    trigger = TriggerContext(
        kind="T4",
        tenant_id=tenant_id,
        seed_natural_text="runway constraint planning",
    )

    result = await pathway_projection_context(
        trigger,
        tenant_id,
        object(),  # type: ignore[arg-type]
        effective_seed_entities=[],
        effective_scope_actors=[],
        max_snapshots=4,
        max_models=4,
    )

    assert result.models == []
    assert repo.snapshot_calls == 1
    assert result.notes["reason"] == "no_projection_snapshots"
    assert result.notes["freshness"] == {
        "available": False,
        "reason": "skipped_no_projection_snapshots",
    }
