from __future__ import annotations

import asyncpg
import pytest

from lib.shared.ids import uuid7
from services.reasoning.edge_intelligence.context_feedback import (
    record_context_use_pair_feedback,
)


pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


async def test_context_feedback_records_selected_referenced_and_no_edge_pairs(
    fresh_db: asyncpg.Pool,
) -> None:
    tenant_id = uuid7()
    a = uuid7()
    b = uuid7()
    c = uuid7()

    async with fresh_db.acquire() as conn:
        await record_context_use_pair_feedback(
            conn,
            tenant_id=tenant_id,
            trigger_ref=uuid7(),
            primitive="DEPENDENCY",
            context_use={
                "context_use_grade": "graph_context_used",
                "selected_model_ids": [str(a), str(b), str(c)],
                "referenced_model_ids": [str(a), str(b)],
                "graph_selected_model_ids": [str(a), str(c)],
                "graph_no_edge_rationale_present": True,
                "graph_relation_contract_basis": "no_edge_rationale",
            },
        )
        rows = await conn.fetch(
            """
            SELECT model_a_id, model_b_id, co_retrieved_count,
                   co_used_valid_diff_count, positive_outcome_count,
                   no_edge_count
            FROM model_pair_evidence
            WHERE tenant_id = $1
            """,
            tenant_id,
        )

    assert len(rows) == 3
    by_pair = {frozenset((row["model_a_id"], row["model_b_id"])): row for row in rows}
    assert by_pair[frozenset((a, b))]["co_retrieved_count"] == 1
    assert by_pair[frozenset((a, b))]["co_used_valid_diff_count"] == 1
    assert by_pair[frozenset((a, b))]["positive_outcome_count"] == 1
    assert by_pair[frozenset((a, c))]["no_edge_count"] == 1


async def test_context_feedback_ignores_malformed_ids(
    fresh_db: asyncpg.Pool,
) -> None:
    tenant_id = uuid7()

    async with fresh_db.acquire() as conn:
        await record_context_use_pair_feedback(
            conn,
            tenant_id=tenant_id,
            trigger_ref=uuid7(),
            context_use={
                "selected_model_ids": ["not-a-uuid", str(uuid7())],
                "referenced_model_ids": [],
            },
        )
        count = await conn.fetchval(
            "SELECT COUNT(*) FROM model_pair_evidence WHERE tenant_id = $1",
            tenant_id,
        )

    assert count == 0
