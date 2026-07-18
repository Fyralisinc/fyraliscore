from __future__ import annotations

from uuid import uuid4

import pytest

from services.reasoning.think.company_learning_feedback import (
    record_company_learning_context_credit,
    record_uncertainty_dispositions,
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


@pytest.mark.asyncio
async def test_records_uncertainty_as_nonselected_justified_noop_candidate():
    tx = _Tx()
    tenant_id, run_id, observation_id = uuid4(), uuid4(), uuid4()
    signal = {
        "uncertainty_id": "MDU_atlas_question",
        "kind": "open_question",
        "observation_id": str(observation_id),
        "routing": "open_question",
    }

    first = await record_uncertainty_dispositions(
        tx,
        tenant_id=tenant_id,
        run_id=run_id,
        batch_id="batch-uncertainty",
        route_id="T1:event_batch",
        uncertainty_signals=[signal],
    )
    second = await record_uncertainty_dispositions(
        tx,
        tenant_id=tenant_id,
        run_id=run_id,
        batch_id="batch-uncertainty",
        route_id="T1:event_batch",
        uncertainty_signals=[signal],
    )

    assert first == second
    args = tx.calls[0][1]
    assert args[4] == "candidate"
    assert args[5] == str(observation_id)
    assert args[6] == "MDU_atlas_question"
    assert args[7:11] == (False, False, False, False)
    assert args[15] == "justified_noop"
    assert args[16] == "open_question"
    assert args[17] is None
    assert "nonassertable_signal_retained_outside_truth" in str(args[18])
