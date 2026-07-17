from __future__ import annotations

from uuid import uuid4

import pytest

from lib.shared.errors import InvariantViolation
from services.domain.company_learning.barrier import CompanyLearningBarrierService


class _NoReplayTx:
    def __init__(self):
        self.calls = []

    async def fetchrow(self, sql, *args):
        self.calls.append((sql, args))
        return {"barrier_id": None}


async def test_lock_and_replay_lookup_share_one_round_trip() -> None:
    from uuid import uuid4

    tx = _NoReplayTx()
    assert await CompanyLearningBarrierService()._lock_and_find(
        tx=tx, tenant_id=uuid4(), batch_id="batch-1",
    ) is None
    assert len(tx.calls) == 1
    assert "pg_advisory_xact_lock" in tx.calls[0][0]
    assert "company_learning_barriers" in tx.calls[0][0]


class _VisibilityTx:
    def __init__(self, *, model_ids=(), relation_ids=(), stale=0):
        self.model_ids = model_ids
        self.relation_ids = relation_ids
        self.stale = stale
        self.statements: list[str] = []

    async def fetch(self, sql, *_args):
        self.statements.append(sql)
        if "accepted_current_relations" in sql:
            return [{"truth_relation_version_id": value} for value in self.relation_ids]
        return [{"truth_version_id": value} for value in self.model_ids]

    async def fetchval(self, sql, *_args):
        self.statements.append(sql)
        return self.stale


@pytest.mark.asyncio
async def test_empty_relation_and_invalidation_sets_issue_no_noop_reads() -> None:
    tenant_id, model_version = uuid4(), uuid4()
    tx = _VisibilityTx(model_ids=(model_version,))
    await CompanyLearningBarrierService()._assert_visibility(
        tx=tx, tenant_id=tenant_id, model_versions=(model_version,),
        relation_versions=(), invalidated_versions=(),
    )
    assert len(tx.statements) == 1
    assert "accepted_current_models" in tx.statements[0]


@pytest.mark.asyncio
async def test_nonempty_relation_and_invalidation_sets_preserve_both_checks() -> None:
    tenant_id, model_version, relation_version, invalidated = uuid4(), uuid4(), uuid4(), uuid4()
    tx = _VisibilityTx(model_ids=(model_version,), relation_ids=(relation_version,), stale=0)
    await CompanyLearningBarrierService()._assert_visibility(
        tx=tx, tenant_id=tenant_id, model_versions=(model_version,),
        relation_versions=(relation_version,), invalidated_versions=(invalidated,),
    )
    assert len(tx.statements) == 3
    assert any("accepted_current_relations" in sql for sql in tx.statements)
    assert any("truth_version_id=ANY" in sql for sql in tx.statements)


@pytest.mark.asyncio
async def test_nonempty_checks_still_fail_closed() -> None:
    tenant_id, model_version, relation_version, invalidated = uuid4(), uuid4(), uuid4(), uuid4()
    with pytest.raises(InvariantViolation, match="relations are not current"):
        await CompanyLearningBarrierService()._assert_visibility(
            tx=_VisibilityTx(model_ids=(model_version,)), tenant_id=tenant_id,
            model_versions=(model_version,), relation_versions=(relation_version,),
            invalidated_versions=(),
        )
    with pytest.raises(InvariantViolation, match="invalidated Model remains current"):
        await CompanyLearningBarrierService()._assert_visibility(
            tx=_VisibilityTx(model_ids=(model_version,), stale=1), tenant_id=tenant_id,
            model_versions=(model_version,), relation_versions=(),
            invalidated_versions=(invalidated,),
        )
