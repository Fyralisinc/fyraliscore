from __future__ import annotations

from uuid import uuid4

import pytest

from services.reasoning.think.company_learning_feedback import (
    record_company_learning_context_credit,
)


class _Tx:
    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple[object, ...]]] = []

    async def execute(self, sql: str, *args: object) -> str:
        self.calls.append((sql, args))
        return "INSERT 0 1"


@pytest.mark.asyncio
async def test_records_exact_model_and_historical_observation_fates():
    tx = _Tx()
    model_id, observation_id = uuid4(), uuid4()
    decision_ids = await record_company_learning_context_credit(
        tx,
        tenant_id=uuid4(), run_id=uuid4(), batch_id="batch-2",
        route_id="trigger_batch", context_use={
            "selected_model_ids": [str(model_id)],
            "referenced_model_ids": [str(model_id)],
            "selected_observation_ids": [str(observation_id)],
            "referenced_observation_ids": [],
            "selected_historical_observation_count": 1,
            "raw_observation_reopening_reasons": ["contradiction"],
        },
        applied={"claim_ops": [{"op": "create"}], "applied_model_ids": [str(uuid4())]},
    )
    assert len(decision_ids) == 2
    assert len(tx.calls) == 2
    model_args = tx.calls[0][1]
    observation_args = tx.calls[1][1]
    assert model_args[4] == "accepted_model"
    assert model_args[10] is True
    assert model_args[15] == "mutation"
    assert observation_args[4] == "historical_observation"
    assert observation_args[14] == "contradiction"
    assert observation_args[15] == "unused"


@pytest.mark.asyncio
async def test_unreferenced_selected_model_gets_unused_fate():
    tx = _Tx()
    await record_company_learning_context_credit(
        tx,
        tenant_id=uuid4(), run_id=uuid4(), batch_id="batch-1",
        route_id="trigger_batch", context_use={
            "selected_model_ids": [str(uuid4())],
            "referenced_model_ids": [],
        },
        applied={},
    )
    assert tx.calls[0][1][15] == "unused"
