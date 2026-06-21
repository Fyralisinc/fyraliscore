from __future__ import annotations

import json

import asyncpg
import pytest
from pgvector.asyncpg import register_vector

from lib.shared.ids import uuid7
from services.reasoning.edge_intelligence import (
    EdgeIntelligenceRepo,
    RelationFrame,
    RelationParticipant,
    project_relation_frame,
)
from services.reasoning.think.tests.conftest import make_embedding


pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


async def _insert_observation(
    conn: asyncpg.Connection,
    tenant_id,
    content_text: str,
):
    obs_id = uuid7()
    await register_vector(conn)
    await conn.execute(
        """
        INSERT INTO observations
          (id, tenant_id, occurred_at, ingested_at, kind, source_channel,
           content, content_text, embedding, embedding_pending, trust_tier)
        VALUES ($1, $2, now(), now(), 'signal', 'test',
                $3::jsonb, $4, $5, FALSE, 'authoritative')
        """,
        obs_id,
        tenant_id,
        json.dumps({"text": content_text}),
        content_text,
        make_embedding(content_text),
    )
    return obs_id


async def _insert_model(
    conn: asyncpg.Connection,
    tenant_id,
    observation_id,
    natural: str,
):
    model_id = uuid7()
    await register_vector(conn)
    await conn.execute(
        """
        INSERT INTO models
          (id, tenant_id, born_from_event_id, proposition, "natural",
           embedding, scope_actors, scope_entities, scope_temporal,
           confidence, activation, status, confidence_at_assertion,
           activation_coefficient)
        VALUES ($1, $2, $3, $4::jsonb, $5, $6, '{}'::uuid[], '[]'::jsonb,
                '{}'::jsonb, 0.74, 1.0, 'active', 0.74, 1.0)
        """,
        model_id,
        tenant_id,
        observation_id,
        json.dumps({"kind": "belief", "claim_role": "fact", "assertion": natural}),
        natural,
        make_embedding(natural),
    )
    return model_id


async def test_blocked_workstream_frame_projects_useful_binary_edges(
    fresh_db: asyncpg.Pool,
) -> None:
    repo = EdgeIntelligenceRepo()
    tenant_id = uuid7()

    async with fresh_db.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO tenants (id, name, is_demo)
            VALUES ($1, 'relation frame test', FALSE)
            ON CONFLICT (id) DO NOTHING
            """,
            tenant_id,
        )
        observation_id = await _insert_observation(
            conn,
            tenant_id,
            (
                "DPA approval blocks HubSpot import; Priya owns the blocker; "
                "the import delay may move launch; security packet can resolve it."
            ),
        )
        blocker = await _insert_model(conn, tenant_id, observation_id, "DPA approval")
        work = await _insert_model(conn, tenant_id, observation_id, "HubSpot import")
        owner = await _insert_model(conn, tenant_id, observation_id, "Priya/legal owner")
        risk = await _insert_model(conn, tenant_id, observation_id, "Friday launch slip")
        resolution = await _insert_model(
            conn,
            tenant_id,
            observation_id,
            "Security packet approval",
        )
        frame = await repo.insert_relation_frame(
            conn,
            RelationFrame(
                tenant_id=tenant_id,
                source_observation_id=observation_id,
                relation_kind="blocked_workstream",
                status="accepted",
                participant_binding_status="bound",
                write_policy="project_edges",
                confidence=0.86,
                evidence_event_ids=(observation_id,),
                evidence_model_ids=(blocker, work, owner, risk, resolution),
                evidence_text="DPA approval blocks HubSpot import.",
            ),
            participants=(
                RelationParticipant(model_id=blocker, role="blocker", binding_confidence=0.9),
                RelationParticipant(model_id=work, role="blocked_work", binding_confidence=0.9),
                RelationParticipant(model_id=owner, role="owner", binding_confidence=0.8),
                RelationParticipant(
                    model_id=risk,
                    role="downstream_risk",
                    binding_confidence=0.82,
                ),
                RelationParticipant(
                    model_id=resolution,
                    role="possible_resolution",
                    binding_confidence=0.78,
                ),
            ),
        )
        report = await project_relation_frame(
            conn,
            tenant_id=tenant_id,
            relation_id=frame["id"],
            created_by_event_id=observation_id,
        )
        edges = await conn.fetch(
            """
            SELECT source_model_id, target_model_id, edge_kind, metadata
            FROM model_edges
            WHERE tenant_id = $1
            ORDER BY edge_kind
            """,
            tenant_id,
        )
        projections = await repo.list_relation_edge_projections(
            conn,
            tenant_id=tenant_id,
            relation_id=frame["id"],
        )

    edge_tuples = {
        (row["source_model_id"], row["target_model_id"], row["edge_kind"])
        for row in edges
    }
    assert len(report.edge_ids) == 3
    assert report.skipped == []
    assert (blocker, work, "blocks") in edge_tuples
    assert (work, risk, "early_warning_for") in edge_tuples
    assert (resolution, blocker, "contributes_to_resolution") in edge_tuples
    assert all(owner not in {row["source_model_id"], row["target_model_id"]} for row in edges)
    assert {row["projection_rule"] for row in projections} == {
        "blocked_work_warns_downstream_risk",
        "blocker_blocks_work",
        "resolution_contributes_to_blocker_resolution",
    }
