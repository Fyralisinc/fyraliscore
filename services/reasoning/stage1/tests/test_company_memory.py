from __future__ import annotations

from uuid import uuid4

import pytest

from lib.shared.errors import InvariantViolation
from services.reasoning.stage1 import company_memory
from services.reasoning.stage1.company_memory import Stage1CompanyMemoryBatch


def test_batch_requires_unique_observations() -> None:
    tenant_id = uuid4()
    observation_id = uuid4()
    with pytest.raises(InvariantViolation, match="at least one Observation"):
        Stage1CompanyMemoryBatch(tenant_id=tenant_id, observation_ids=())
    with pytest.raises(InvariantViolation, match="unique Observation ids"):
        Stage1CompanyMemoryBatch(
            tenant_id=tenant_id,
            observation_ids=(observation_id, observation_id),
        )


@pytest.mark.asyncio
async def test_composition_root_builds_one_t1_batch_and_uses_stage1_policy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tenant_id = uuid4()
    observation_ids = (uuid4(), uuid4())
    actor_id = uuid4()
    captured = {}

    async def fake_think(trigger, pool, **kwargs):
        captured.update(trigger=trigger, pool=pool, kwargs=kwargs)
        return "applied"

    monkeypatch.setattr(company_memory, "think", fake_think)
    pool = object()
    provider = object()
    batch = Stage1CompanyMemoryBatch(
        tenant_id=tenant_id,
        observation_ids=observation_ids,
        scope_actors=(actor_id,),
        seed_natural_text="Atlas renewal evidence changed.",
    )

    result = await company_memory.process_stage1_company_memory(
        batch,
        pool,  # type: ignore[arg-type]
        llm_provider=provider,  # type: ignore[arg-type]
    )

    assert result == "applied"
    trigger = captured["trigger"]
    assert trigger.kind == "T1"
    assert trigger.subkind == "event_batch"
    assert trigger.observation_ids == list(observation_ids)
    assert trigger.scope_actors == [actor_id]
    assert trigger.seed_signature["execution_profile"] == "stage1_company_memory"
    policy = captured["kwargs"]["execution_policy"]
    assert policy.is_stage1_company_memory is True
