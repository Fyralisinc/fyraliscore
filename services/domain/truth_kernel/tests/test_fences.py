from __future__ import annotations

from datetime import datetime, timezone
from uuid import uuid4

import pytest

from lib.contracts.truth_admission import ModelHead, ModelTruthLifecycle, ModelVersion
from services.domain.truth_kernel.fences import (
    AsyncpgDependentTruthFence,
    build_default_truth_kernel,
)
from services.domain.truth_kernel.service import FenceContext


class _Tx:
    def __init__(self):
        self.calls = []

    async def execute(self, sql, *args):
        self.calls.append((sql, args))


def _context(lifecycle):
    now = datetime.now(timezone.utc)
    tenant_id, model_id, prior_id = uuid4(), uuid4(), uuid4()
    prior = ModelHead(
        tenant_id=tenant_id,
        model_id=model_id,
        version_id=prior_id,
        version=1,
        semantic_digest="a" * 64,
        lifecycle=ModelTruthLifecycle.ACTIVE,
        advanced_at=now,
    )
    version = ModelVersion.model_construct(
        tenant_id=tenant_id,
        model_id=model_id,
        version_id=uuid4(),
        version=2,
        lifecycle=lifecycle,
        created_at=now,
    )
    return FenceContext(
        tenant_id=tenant_id,
        model_id=model_id,
        prior_head=prior,
        next_version=version,
        command_id=uuid4(),
        cause_digest="b" * 64,
    )


@pytest.mark.asyncio
async def test_terminal_transition_fences_models_relations_and_projections():
    tx = _Tx()
    context = _context(ModelTruthLifecycle.FALSIFIED)

    await AsyncpgDependentTruthFence().apply(tx=tx, context=context)

    assert len(tx.calls) == 3
    combined = " ".join(sql for sql, _ in tx.calls)
    assert "'model_version'" in combined
    assert "'relation_version'" in combined
    assert "'projection'" in combined
    assert combined.count("ON CONFLICT") == 3
    assert all(args[1] == context.prior_head.version_id for _, args in tx.calls)


@pytest.mark.asyncio
async def test_nonterminal_transition_creates_no_repair_obligation():
    tx = _Tx()
    await AsyncpgDependentTruthFence().apply(
        tx=tx, context=_context(ModelTruthLifecycle.DISPUTED)
    )
    assert tx.calls == []


def test_default_factory_cannot_omit_the_truth_fence():
    service = build_default_truth_kernel()
    assert [fence.name for fence in service._fences] == [
        "dependent_truth_repair_obligations"
    ]
