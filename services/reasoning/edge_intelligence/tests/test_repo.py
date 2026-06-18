from __future__ import annotations

import asyncpg
import pytest

from lib.shared.ids import uuid7
from services.reasoning.edge_intelligence import (
    EdgeIntelligenceRepo,
    PairEvidenceObservation,
    RelationClaim,
    RelationEdgeProjection,
    RelationEvidence,
    RelationFrame,
    RelationParticipant,
)


pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


async def test_relation_evidence_round_trips(fresh_db: asyncpg.Pool) -> None:
    repo = EdgeIntelligenceRepo()
    tenant_id = uuid7()
    source_model_id = uuid7()
    target_model_id = uuid7()

    async with fresh_db.acquire() as conn:
        inserted = await repo.insert_relation_evidence(
            conn,
            RelationEvidence(
                tenant_id=tenant_id,
                source_model_id=source_model_id,
                target_model_id=target_model_id,
                predicate="blocks",
                edge_kind_hint="blocks",
                direction="source_to_target",
                scope_entities=[
                    {"type": "customer", "id": str(uuid7())},
                    {"type": "commitment", "id": str(uuid7())},
                ],
                confidence=0.84,
                extraction_method="test",
                evidence_text="DPA approval blocks the HubSpot import.",
            ),
        )

    assert inserted["tenant_id"] == tenant_id
    assert inserted["source_model_id"] == source_model_id
    assert inserted["target_model_id"] == target_model_id
    assert inserted["predicate"] == "blocks"
    assert inserted["edge_kind_hint"] == "blocks"
    assert inserted["scope_entities"][0]["type"] == "customer"


async def test_relation_claim_round_trips_and_metrics(
    fresh_db: asyncpg.Pool,
) -> None:
    repo = EdgeIntelligenceRepo()
    tenant_id = uuid7()
    source_model_id = uuid7()
    target_model_id = uuid7()
    event_id = uuid7()

    async with fresh_db.acquire() as conn:
        inserted = await repo.insert_relation_claim(
            conn,
            RelationClaim(
                tenant_id=tenant_id,
                source_observation_id=event_id,
                source_model_id=source_model_id,
                target_model_id=target_model_id,
                subject_ref={"kind": "model", "model_id": str(source_model_id)},
                object_ref={"kind": "model", "model_id": str(target_model_id)},
                predicate="blocks",
                edge_kind="blocks",
                endpoint_binding_status="bound",
                write_policy="accepted_edge",
                status="accepted",
                confidence=0.84,
                weight=0.73,
                binding_confidence=0.91,
                evidence_event_ids=(event_id,),
                evidence_model_ids=(source_model_id, target_model_id),
                evidence_text="DPA approval blocks the HubSpot import.",
            ),
        )
        decided = await repo.mark_relation_claim_decided(
            conn,
            claim_id=inserted["id"],
            tenant_id=tenant_id,
            status="accepted",
            accepted_edge_ids=(uuid7(),),
            decision_metadata={"reason": "test"},
        )
        metrics = await repo.metrics(conn, tenant_id=tenant_id)

    assert inserted["tenant_id"] == tenant_id
    assert inserted["source_model_id"] == source_model_id
    assert inserted["target_model_id"] == target_model_id
    assert inserted["edge_kind"] == "blocks"
    assert inserted["weight"] == 0.73
    assert inserted["endpoint_binding_status"] == "bound"
    assert decided is not None
    assert decided["accepted_edge_ids"]
    assert decided["metadata"]["latest_adjudication"]["reason"] == "test"
    assert metrics.relation_claims_total == 1
    assert metrics.relation_claims_bound == 1
    assert metrics.relation_claims_accepted == 1
    assert metrics.relation_claims_by_edge_kind == {"blocks": 1}


async def test_pair_observation_aggregates_votes_and_counts(
    fresh_db: asyncpg.Pool,
) -> None:
    repo = EdgeIntelligenceRepo()
    tenant_id = uuid7()
    source_model_id = uuid7()
    target_model_id = uuid7()

    async with fresh_db.acquire() as conn:
        first = await repo.record_pair_observation(
            conn,
            PairEvidenceObservation(
                tenant_id=tenant_id,
                left_model_id=source_model_id,
                right_model_id=target_model_id,
                primitive="dependency",
                co_retrieved_delta=1,
                explicit_relation_delta=1,
                directed_source_model_id=source_model_id,
                directed_target_model_id=target_model_id,
                edge_kind_hint="blocks",
            ),
        )
        second = await repo.record_pair_observation(
            conn,
            PairEvidenceObservation(
                tenant_id=tenant_id,
                left_model_id=target_model_id,
                right_model_id=source_model_id,
                primitive="DEPENDENCY",
                co_used_valid_diff_delta=1,
                think_edge_op_delta=1,
                directed_source_model_id=source_model_id,
                directed_target_model_id=target_model_id,
                edge_kind_hint="blocks",
                metadata={"latest_source": "test"},
            ),
        )
        promotable = await repo.list_promotable_pair_evidence(
            conn,
            tenant_id=tenant_id,
            min_confidence=0.1,
        )
        metrics = await repo.metrics(conn, tenant_id=tenant_id)

    assert first.id == second.id
    assert second.primitive == "DEPENDENCY"
    assert second.co_retrieved_count == 1
    assert second.co_used_valid_diff_count == 1
    assert second.explicit_relation_count == 1
    assert second.think_edge_op_count == 1
    assert second.edge_kind_votes == {"blocks": 2}
    assert sum(second.direction_votes.values()) == 2
    assert second.confidence_score > first.confidence_score
    assert second.metadata["latest_source"] == "test"
    assert promotable and promotable[0].id == second.id
    assert metrics.relation_evidence_total == 0
    assert metrics.pair_evidence_total == 1
    assert metrics.pair_evidence_promotable == 1
    assert metrics.pair_evidence_by_primitive == {"DEPENDENCY": 1}


async def test_relation_frame_round_trips_participants_projections_and_metrics(
    fresh_db: asyncpg.Pool,
) -> None:
    repo = EdgeIntelligenceRepo()
    tenant_id = uuid7()
    blocker_id = uuid7()
    work_id = uuid7()
    owner_id = uuid7()
    event_id = uuid7()

    async with fresh_db.acquire() as conn:
        frame = await repo.insert_relation_frame(
            conn,
            RelationFrame(
                tenant_id=tenant_id,
                source_observation_id=event_id,
                relation_kind="blocked_workstream",
                status="accepted",
                participant_binding_status="bound",
                write_policy="project_edges",
                confidence=0.86,
                evidence_event_ids=(event_id,),
                evidence_model_ids=(blocker_id, work_id, owner_id),
                evidence_text="DPA approval blocks HubSpot import; Priya owns it.",
            ),
            participants=(
                RelationParticipant(
                    model_id=blocker_id,
                    role="blocker",
                    binding_confidence=0.92,
                ),
                RelationParticipant(
                    model_id=work_id,
                    role="blocked_work",
                    binding_confidence=0.9,
                ),
                RelationParticipant(
                    model_id=owner_id,
                    role="owner",
                    binding_confidence=0.74,
                ),
            ),
        )
        projection = await repo.insert_relation_edge_projection(
            conn,
            RelationEdgeProjection(
                relation_id=frame["id"],
                tenant_id=tenant_id,
                edge_id=uuid7(),
                projection_rule="blocker_blocks_work",
                source_role="blocker",
                target_role="blocked_work",
                source_model_id=blocker_id,
                target_model_id=work_id,
                edge_kind="blocks",
            ),
        )
        loaded = await repo.get_relation_frame(
            conn,
            tenant_id=tenant_id,
            relation_id=frame["id"],
        )
        metrics = await repo.metrics(conn, tenant_id=tenant_id)

    assert loaded["relation_kind"] == "blocked_workstream"
    assert loaded["status"] == "accepted"
    assert loaded["participant_binding_status"] == "bound"
    assert {p["role"] for p in loaded["participants"]} == {
        "blocked_work",
        "blocker",
        "owner",
    }
    assert projection["edge_kind"] == "blocks"
    assert loaded["edge_projections"][0]["projection_rule"] == "blocker_blocks_work"
    assert metrics.relation_frames_total == 1
    assert metrics.relation_frames_bound == 1
    assert metrics.relation_frames_accepted == 1
    assert metrics.relation_edge_projections_total == 1
    assert metrics.relation_frames_by_kind == {"blocked_workstream": 1}
