from __future__ import annotations

import json

import asyncpg
import pytest

from lib.shared.ids import uuid7
from services.reasoning.edge_intelligence import (
    EdgeIntelligenceRepo,
    PairEvidenceObservation,
    promote_pair_evidence_candidates,
)


pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


async def test_promote_pair_evidence_inserts_candidate_once(
    fresh_db: asyncpg.Pool,
) -> None:
    repo = EdgeIntelligenceRepo()
    tenant_id = uuid7()
    source_model_id = uuid7()
    target_model_id = uuid7()

    async with fresh_db.acquire() as conn:
        await repo.record_pair_observation(
            conn,
            PairEvidenceObservation(
                tenant_id=tenant_id,
                left_model_id=source_model_id,
                right_model_id=target_model_id,
                primitive="DEPENDENCY",
                co_used_valid_diff_delta=1,
                explicit_relation_delta=1,
                think_edge_op_delta=1,
                directed_source_model_id=source_model_id,
                directed_target_model_id=target_model_id,
                edge_kind_hint="blocks",
            ),
        )
        first = await promote_pair_evidence_candidates(conn, tenant_id=tenant_id)
        second = await promote_pair_evidence_candidates(conn, tenant_id=tenant_id)
        rows = await conn.fetch(
            """
            SELECT source_model_id, target_model_id, edge_kind, source, metadata
            FROM relationship_candidates
            WHERE tenant_id = $1
            """,
            tenant_id,
        )

    assert first.candidates_inserted == 1
    assert second.candidates_inserted == 0
    assert second.candidates_skipped == 1
    assert len(rows) == 1
    assert rows[0]["source_model_id"] == source_model_id
    assert rows[0]["target_model_id"] == target_model_id
    assert rows[0]["edge_kind"] == "blocks"
    assert rows[0]["source"] == "edge_intelligence_kernel"
    metadata = rows[0]["metadata"]
    if isinstance(metadata, str):
        metadata = json.loads(metadata)
    assert metadata["edge_intelligence"]["source"] == "model_pair_evidence"


async def test_promote_pair_evidence_can_scope_to_changed_models(
    fresh_db: asyncpg.Pool,
) -> None:
    repo = EdgeIntelligenceRepo()
    tenant_id = uuid7()
    changed_source_id = uuid7()
    changed_target_id = uuid7()
    unrelated_source_id = uuid7()
    unrelated_target_id = uuid7()

    async with fresh_db.acquire() as conn:
        for source_model_id, target_model_id in (
            (changed_source_id, changed_target_id),
            (unrelated_source_id, unrelated_target_id),
        ):
            await repo.record_pair_observation(
                conn,
                PairEvidenceObservation(
                    tenant_id=tenant_id,
                    left_model_id=source_model_id,
                    right_model_id=target_model_id,
                    primitive="DEPENDENCY",
                    co_used_valid_diff_delta=1,
                    explicit_relation_delta=1,
                    think_edge_op_delta=1,
                    directed_source_model_id=source_model_id,
                    directed_target_model_id=target_model_id,
                    edge_kind_hint="blocks",
                ),
            )

        report = await promote_pair_evidence_candidates(
            conn,
            tenant_id=tenant_id,
            model_ids=[changed_source_id],
        )
        rows = await conn.fetch(
            """
            SELECT source_model_id, target_model_id
            FROM relationship_candidates
            WHERE tenant_id = $1
            ORDER BY created_at ASC
            """,
            tenant_id,
        )

    assert report.scanned_pair_evidence == 1
    assert report.candidates_inserted == 1
    assert len(rows) == 1
    assert rows[0]["source_model_id"] == changed_source_id
    assert rows[0]["target_model_id"] == changed_target_id


async def test_promote_pair_evidence_skips_existing_active_edge(
    fresh_db: asyncpg.Pool,
) -> None:
    repo = EdgeIntelligenceRepo()
    tenant_id = uuid7()
    source_model_id = uuid7()
    target_model_id = uuid7()

    async with fresh_db.acquire() as conn:
        await repo.record_pair_observation(
            conn,
            PairEvidenceObservation(
                tenant_id=tenant_id,
                left_model_id=source_model_id,
                right_model_id=target_model_id,
                primitive="DEPENDENCY",
                co_used_valid_diff_delta=1,
                explicit_relation_delta=1,
                think_edge_op_delta=1,
                directed_source_model_id=source_model_id,
                directed_target_model_id=target_model_id,
                edge_kind_hint="blocks",
            ),
        )
        await conn.execute(
            """
            INSERT INTO model_edges (
              id, tenant_id, source_model_id, target_model_id, edge_kind,
              metadata, status, detected_by
            )
            VALUES ($1, $2, $3, $4, 'blocks', '{}'::jsonb, 'active',
                    'think_edge_op')
            """,
            uuid7(),
            tenant_id,
            source_model_id,
            target_model_id,
        )
        report = await promote_pair_evidence_candidates(conn, tenant_id=tenant_id)
        candidate_count = await conn.fetchval(
            "SELECT COUNT(*) FROM relationship_candidates WHERE tenant_id = $1",
            tenant_id,
        )

    assert report.scanned_pair_evidence == 1
    assert report.candidates_inserted == 0
    assert report.candidates_skipped == 1
    assert candidate_count == 0
