from __future__ import annotations

from uuid import uuid4

import pytest

from services.reasoning.sage.topology_optimizer.optimizer import (
    _insert_canonical_operation_candidate,
)


class _Conn:
    def __init__(self) -> None:
        self.fetchvals: list[tuple[str, tuple]] = []
        self.fetchval_results: list = []

    async def fetchval(self, query: str, *args):
        self.fetchvals.append((query, args))
        if self.fetchval_results:
            return self.fetchval_results.pop(0)
        return None


@pytest.mark.asyncio
async def test_split_operation_is_persisted_for_validation_review() -> None:
    conn = _Conn()
    tenant_id = uuid4()
    session_id = uuid4()
    model_id = uuid4()
    inserted_id = uuid4()
    conn.fetchval_results.extend([None, inserted_id])

    inserted = await _insert_canonical_operation_candidate(
        conn,
        tenant_id=tenant_id,
        session_id=session_id,
        payload={
            "op": "split",
            "source_model_id": str(model_id),
            "proposed_kind": "split_overloaded",
            "reason": "prediction_error=0.91 with many neighbors",
        },
    )

    assert inserted is True
    insert_query, insert_args = conn.fetchvals[1]
    assert "INSERT INTO canonical_operation_candidates" in insert_query
    assert insert_args[1] == tenant_id
    assert insert_args[2] == "split"
    assert insert_args[5] == model_id
    assert insert_args[6] == [model_id]
    assert "prediction_error=0.91" in insert_args[7]
    assert "canonical_op_key" in insert_args[8]


@pytest.mark.asyncio
async def test_existing_canonical_operation_is_not_reinserted() -> None:
    conn = _Conn()
    conn.fetchval_results.append(uuid4())

    inserted = await _insert_canonical_operation_candidate(
        conn,
        tenant_id=uuid4(),
        session_id=uuid4(),
        payload={
            "op": "demote",
            "source_model_id": str(uuid4()),
            "proposed_kind": "demote_low_utility",
            "reason": "ignored repeatedly",
        },
    )

    assert inserted is False
    assert len(conn.fetchvals) == 1


@pytest.mark.asyncio
async def test_merge_operation_stays_on_relationship_candidate_path() -> None:
    conn = _Conn()

    inserted = await _insert_canonical_operation_candidate(
        conn,
        tenant_id=uuid4(),
        session_id=uuid4(),
        payload={
            "op": "merge",
            "source_model_ids": [str(uuid4()), str(uuid4())],
            "proposed_kind": "merge_duplicates",
        },
    )

    assert inserted is False
    assert conn.fetchvals == []
