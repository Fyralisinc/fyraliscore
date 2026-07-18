from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timedelta, timezone
from uuid import UUID

import pytest

from lib.shared.errors import InvariantViolation
from services.domain.truth_kernel.memory_snapshot import (
    build_accepted_memory_snapshot,
    validate_accepted_memory_snapshot,
)


NOW = datetime(2026, 7, 18, 12, tzinfo=timezone.utc)
TENANT = UUID("00000000-0000-0000-0000-000000000001")
M1 = UUID("00000000-0000-0000-0000-000000000011")
M2 = UUID("00000000-0000-0000-0000-000000000012")
V1 = UUID("00000000-0000-0000-0000-000000000021")
V2 = UUID("00000000-0000-0000-0000-000000000022")
R1 = UUID("00000000-0000-0000-0000-000000000031")
RV1 = UUID("00000000-0000-0000-0000-000000000041")


def model_row(model_id: UUID, version_id: UUID, version: int = 1) -> dict:
    return {
        "model_id": model_id, "tenant_id": TENANT, "version_id": version_id,
        "version": version, "semantic_digest": f"{version:064x}",
        "lifecycle": "active", "created_at": NOW - timedelta(minutes=2),
        "advanced_at": NOW - timedelta(minutes=1),
        "canonical_scope_refs": [f"project:{model_id}"],
        "all_scope_refs_canonical": True,
    }


def relation_row() -> dict:
    return {
        "relation_id": R1, "tenant_id": TENANT, "relation_version_id": RV1,
        "version": 1, "semantic_digest": "a" * 64, "lifecycle": "active",
        "created_at": NOW - timedelta(minutes=2),
        "advanced_at": NOW - timedelta(minutes=1),
    }


class FakeTransaction:
    def __init__(self, models: list[dict], relations: list[dict]) -> None:
        self.models = models
        self.relations = relations

    async def fetch(self, sql: str, tenant_id: UUID, requested: list[UUID]):
        rows = self.models if "accepted-memory:model-heads" in sql else self.relations
        key = "model_id" if rows is self.models else "relation_id"
        return [deepcopy(row) for row in rows
                if row["tenant_id"] == tenant_id and row[key] in requested]


@pytest.mark.asyncio
async def test_build_is_exact_order_independent_and_deterministic() -> None:
    tx = FakeTransaction(
        [model_row(M2, V2), model_row(M1, V1)], [relation_row()],
    )
    first = await build_accepted_memory_snapshot(
        tx, tenant_id=TENANT, cutoff_at=NOW,
        model_ids=[M2, M1, M2], relation_ids=[R1],
        retrieval_receipt_ids=[V2, V1, V2],
    )
    second = await build_accepted_memory_snapshot(
        tx, tenant_id=TENANT, cutoff_at=NOW,
        model_ids=[M1, M2], relation_ids=[R1],
        retrieval_receipt_ids=[V1, V2],
    )

    assert tuple(head.model_id for head in first.model_heads) == (M1, M2)
    assert first == second
    assert first.snapshot_digest == second.snapshot_digest
    assert "accepted_current_models" in __import__(
        "services.domain.truth_kernel.memory_snapshot", fromlist=["_MODEL_HEADS_SQL"]
    )._MODEL_HEADS_SQL


@pytest.mark.asyncio
async def test_build_rejects_missing_or_nonaccepted_requested_head() -> None:
    tx = FakeTransaction([model_row(M1, V1)], [])
    with pytest.raises(InvariantViolation) as error:
        await build_accepted_memory_snapshot(
            tx, tenant_id=TENANT, cutoff_at=NOW, model_ids=[M1, M2],
        )
    assert error.value.invariant == "accepted_memory_model_head_missing"
    assert error.value.context["model_ids"] == (str(M2),)


@pytest.mark.asyncio
async def test_build_rejects_head_newer_than_cutoff_and_noncanonical_scope() -> None:
    too_new = model_row(M1, V1)
    too_new["advanced_at"] = NOW + timedelta(seconds=1)
    with pytest.raises(InvariantViolation) as cutoff_error:
        await build_accepted_memory_snapshot(
            FakeTransaction([too_new], []), tenant_id=TENANT,
            cutoff_at=NOW, model_ids=[M1],
        )
    assert cutoff_error.value.invariant == "accepted_memory_head_after_cutoff"

    incomplete = model_row(M1, V1)
    incomplete["all_scope_refs_canonical"] = False
    with pytest.raises(InvariantViolation) as scope_error:
        await build_accepted_memory_snapshot(
            FakeTransaction([incomplete], []), tenant_id=TENANT,
            cutoff_at=NOW, model_ids=[M1],
        )
    assert scope_error.value.invariant == "accepted_memory_scope_not_canonical"


@pytest.mark.asyncio
async def test_validation_rejects_advanced_or_no_longer_accepted_head() -> None:
    tx = FakeTransaction([model_row(M1, V1)], [relation_row()])
    snapshot = await build_accepted_memory_snapshot(
        tx, tenant_id=TENANT, cutoff_at=NOW, model_ids=[M1], relation_ids=[R1],
    )

    tx.models[0] = model_row(M1, V2, version=2)
    with pytest.raises(InvariantViolation) as stale_error:
        await validate_accepted_memory_snapshot(tx, snapshot)
    assert stale_error.value.invariant == "accepted_memory_snapshot_stale"

    tx.models.clear()
    with pytest.raises(InvariantViolation) as missing_error:
        await validate_accepted_memory_snapshot(tx, snapshot)
    assert missing_error.value.invariant == "accepted_memory_model_head_missing"
