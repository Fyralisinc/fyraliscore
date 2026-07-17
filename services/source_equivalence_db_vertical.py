"""Fresh PostgreSQL-backed normalized-source semantic equivalence proof."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from uuid import UUID

import asyncpg

from lib.evaluation.source_equivalence import evaluate_normalized_source_equivalence
from lib.shared.ids import uuid7
from services.domain.entity_grounding.episode import (
    GroundingCandidateInput,
    build_grounding_episode,
    candidate_id_for_ref,
    prepare_context_selection,
)
from services.domain.entity_grounding.mentions import prepare_entity_mention_detection
from services.domain.entity_grounding.repo import EntityGroundingRepo
from services.domain.source_semantics.repo import SourceSemanticRepo
from services.workers.source_semantic_worker import (
    SourceSemanticWorker,
    SourceSemanticWorkerStats,
)


ATLAS_REF = {"type": "project", "id": "project:atlas"}
SOURCE_CASES = (
    ("slack", "slack:normalized", "slack:user:alice",
     ("channel:eng", "thread:launch-42", "message:17")),
    ("email", "email:normalized", "email:alice@example.test",
     ("mailbox:ops", "thread:launch", "message:m-17")),
    ("jira", "jira:normalized", "jira:account:alice",
     ("project:ENG", "issue:ENG-42", "comment:17")),
    ("document_meeting", "document_meeting:normalized", "meeting:speaker:alice",
     ("meeting:weekly-7", "transcript:segment-17")),
)


async def run_source_equivalence_db_vertical(
    *, pool: asyncpg.Pool, tenant_id: UUID, output_path: Path | None = None,
) -> dict[str, Any]:
    now = datetime.now(timezone.utc) - timedelta(minutes=2)
    observation_ids: dict[str, UUID] = {}
    async with pool.acquire() as conn, conn.transaction():
        for offset, (source, channel, author, boundaries) in enumerate(SOURCE_CASES):
            observation_id = uuid7()
            occurred_at = now + timedelta(seconds=offset)
            text = "AtlasProject is blocked"
            await conn.execute(
                """INSERT INTO observations (
                     id,tenant_id,occurred_at,kind,source_channel,source_actor_ref,
                     content,content_text,embedding,embedding_pending,trust_tier,
                     entities_mentioned
                   ) VALUES ($1,$2,$3,'signal',$4,$5,$6::jsonb,$7,$8,FALSE,
                     'ordinary','[]'::jsonb)""",
                observation_id, tenant_id, occurred_at, channel, author,
                json.dumps({"text": text, "source_boundary_refs": boundaries}),
                text, "[" + ",".join(["0.01"] * 768) + "]",
            )
            context_command, context_outcome = prepare_context_selection(
                tenant_id=tenant_id, observation_id=observation_id,
                phrase="AtlasProject", occurred_at=occurred_at,
                source_channel=channel, source_space=f"{source}:sealed-space",
                topology_incomplete=False,
                boundary_hypotheses=({"kind": "source_topology"},),
                context_observations=(), selection_dependency_refs=(),
                now=occurred_at + timedelta(minutes=1),
            )
            mention_command = prepare_entity_mention_detection(
                tenant_id=tenant_id, observation_id=observation_id,
                phrase="AtlasProject", content_text=text, source_channel=channel,
                context_command=context_command, context_outcome=context_outcome,
                now=occurred_at + timedelta(minutes=1),
            )
            episode = build_grounding_episode(
                tenant_id=tenant_id, observation_id=observation_id,
                phrase="AtlasProject", occurred_at=occurred_at, source_channel=channel,
                source_space=f"{source}:sealed-space", topology_incomplete=False,
                boundary_hypotheses=({"kind": "source_topology"},),
                context_observations=(), selection_dependency_refs=(),
                candidates=(GroundingCandidateInput(
                    canonical_ref=ATLAS_REF, candidate_source="tenant_aliases",
                    positive_evidence_refs=(f"sealed-alias:{source}:AtlasProject",),
                    independent_identity_evidence_refs=(
                        f"sealed-identity:{source}:AtlasProject",
                    ),
                ),),
                model_candidate_id=candidate_id_for_ref(ATLAS_REF),
                model_canonical_ref=ATLAS_REF, model_confidence=0.93,
                model_reasoning="sealed exact identity candidate",
                high_confidence=0.8, review_min=0.5,
                prepared_context_command=context_command,
                prepared_context_outcome=context_outcome,
                prepared_mention_detection_command=mention_command,
                now=occurred_at + timedelta(minutes=1),
            )
            await EntityGroundingRepo(pool=object()).append_episode(  # type: ignore[arg-type]
                episode=episode, tenant_id=tenant_id,
                source_observation_id=observation_id, phrase="AtlasProject", conn=conn,
            )
            trace_id = await conn.fetchval(
                "SELECT id FROM grounding_traces WHERE tenant_id=$1 AND source_observation_id=$2",
                tenant_id, observation_id,
            )
            observation_ids[source] = observation_id
            await SourceSemanticRepo().enqueue_work(
                conn, tenant_id=tenant_id, grounding_trace_id=trace_id,
                now=occurred_at + timedelta(minutes=1),
            )

    stats = SourceSemanticWorkerStats()
    worker = SourceSemanticWorker(
        pool=pool, worker_id=f"source-equivalence-db:{tenant_id}"
    )
    claimed = await worker.process_batch(limit=len(SOURCE_CASES), stats=stats)
    if claimed != len(SOURCE_CASES) or stats.belief_applied != len(SOURCE_CASES):
        raise AssertionError(
            f"source semantic batch incomplete: claimed={claimed}, stats={stats}"
        )

    rows: list[dict[str, Any]] = []
    async with pool.acquire() as conn:
        for source, _channel, expected_author, expected_boundaries in SOURCE_CASES:
            observation_id = observation_ids[source]
            row = await conn.fetchrow(
                """SELECT o.content, gt.selected_referent, i.source_assertion,
                          i.semantic_frame, i.grounding_continuity,
                          a.admitted_model_id, m.proposition, m.scope_entities
                   FROM observations o
                   JOIN grounding_traces gt ON gt.tenant_id=o.tenant_id
                    AND gt.source_observation_id=o.id
                   JOIN source_semantic_interpretations i ON i.tenant_id=gt.tenant_id
                    AND i.grounding_trace_id=gt.id
                   JOIN source_semantic_admission_decisions a
                    ON a.tenant_id=i.tenant_id AND a.interpretation_id=i.id
                   JOIN models m ON m.tenant_id=a.tenant_id
                    AND m.id=a.admitted_model_id
                   WHERE o.tenant_id=$1 AND o.id=$2""",
                tenant_id, observation_id,
            )
            if row is None:
                raise AssertionError(f"missing durable semantic row for {source}")
            content = _json(row["content"])
            assertion = _json(row["source_assertion"])
            frame = _json(row["semantic_frame"])
            proposition = _json(row["proposition"])
            selected = _json(row["selected_referent"])
            continuity = _json(row["grounding_continuity"])
            relation_rows = await conn.fetch(
                """SELECT edge_kind,source_model_id,target_model_id,metadata
                   FROM model_edges WHERE tenant_id=$1 AND status='active'
                     AND (source_model_id=$2 OR target_model_id=$2)""",
                tenant_id, row["admitted_model_id"],
            )
            coordinate = assertion["coordinates"][0]
            rows.append({
                "semantic_case_id": "atlas-blocked",
                "source_kind": source,
                "batch_signal_count": len(SOURCE_CASES),
                "entity_refs": [
                    f"{selected['type']}:{selected['id']}"
                ],
                "model_signatures": [
                    f"{proposition['kind']}:{proposition['assertion']}:"
                    f"{frame['predicate_or_event_type']}"
                ],
                "relation_signatures": [
                    f"{edge['edge_kind']}:{edge['source_model_id']}:{edge['target_model_id']}"
                    for edge in relation_rows
                ],
                "authority_ref": assertion["current_speaker_or_author"],
                "expected_authority_ref": expected_author,
                "assertion_source_system": coordinate["source_system"],
                "expected_source_system": source,
                "boundary_refs": content["source_boundary_refs"],
                "expected_boundary_refs": list(expected_boundaries),
                "entity_lineage_complete": bool(
                    continuity.get("mention_ref")
                    and continuity.get("resolution_assessment_ref")
                    and continuity.get("grounding_admission_ref")
                    and continuity.get("selected_referent")
                ),
                "model_lineage_complete": bool(
                    proposition.get("source_semantic_interpretation_id")
                    and continuity.get("downstream_object_ref")
                    == f"model:{row['admitted_model_id']}"
                ),
                "relation_lineage_complete": all(
                    bool(_json(edge.get("metadata"))) for edge in relation_rows
                ),
            })

    evaluation = evaluate_normalized_source_equivalence(
        rows, require_relation_exposure=True
    )
    objective: dict[str, Any] = {
        "schema_version": "source-equivalence-db-objective-v1",
        "tenant_id": str(tenant_id),
        "population": {"signal_batches": 1, "signals": 4, "sources": 4},
        "worker": {
            "claimed": stats.claimed, "belief_applied": stats.belief_applied,
            "no_admission": stats.no_admission,
            "terminal_failures": stats.terminal_failures,
        },
        "rows": rows,
        "evaluation": evaluation,
    }
    body = json.dumps(objective, sort_keys=True, separators=(",", ":"))
    objective["objective_sha256"] = hashlib.sha256(body.encode()).hexdigest()
    if output_path is not None:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        temporary = output_path.with_suffix(".tmp")
        temporary.write_text(json.dumps(objective, indent=2, sort_keys=True) + "\n")
        temporary.replace(output_path)
    return objective


def _json(value: Any) -> dict[str, Any]:
    if isinstance(value, str):
        value = json.loads(value)
    if not isinstance(value, dict):
        raise ValueError("expected JSON object")
    return value


__all__ = ["run_source_equivalence_db_vertical"]
