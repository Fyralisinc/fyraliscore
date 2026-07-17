"""Fresh PostgreSQL-backed normalized-source semantic equivalence proof.

The vertical starts after connectors with one persisted eight-signal batch. It
uses the ordinary grounding and source-semantic worker path, then sends an
explicit, bound relation claim through Think's production diff applier. The
sealed relation decision replaces an LLM call; the persistence, validation,
lineage, and graph mutation paths are production paths.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from uuid import UUID

import asyncpg

from lib.evaluation.source_equivalence import evaluate_normalized_source_equivalence
from lib.contracts.kernel import canonical_sha256
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
from services.reasoning.think.applier import apply_diff
from services.reasoning.think.diff_schema import RelationClaimOp, ValidatedDiff
from services.workers.source_semantic_worker import (
    SourceSemanticWorker,
    SourceSemanticWorkerStats,
)


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
MODEL_SIGNALS = (
    ("vendor", "VendorDependency", "VendorDependency is blocked",
     {"type": "dependency", "id": "dependency:vendor"}),
    ("atlas", "AtlasProject", "AtlasProject is blocked",
     {"type": "project", "id": "project:atlas"}),
)
RELATION_SIGNAL = (
    "relation", "VendorDependency", "VendorDependency blocks AtlasProject",
    {"type": "dependency", "id": "dependency:vendor"},
)
SIGNALS = (*MODEL_SIGNALS, RELATION_SIGNAL)


async def run_source_equivalence_db_vertical(
    *, pool: asyncpg.Pool, tenant_id: UUID, output_path: Path | None = None,
) -> dict[str, Any]:
    now = datetime.now(timezone.utc) - timedelta(minutes=3)
    signal_records: dict[str, dict[str, dict[str, Any]]] = {}
    async with pool.acquire() as conn, conn.transaction():
        for source_offset, (source, channel, author, boundaries) in enumerate(SOURCE_CASES):
            signal_records[source] = {}
            for signal_offset, (role, phrase, text, referent) in enumerate(SIGNALS):
                occurred_at = now + timedelta(seconds=source_offset * 2 + signal_offset)
                signal_records[source][role] = await _persist_grounded_signal(
                    conn,
                    tenant_id=tenant_id,
                    occurred_at=occurred_at,
                    source=source,
                    channel=channel,
                    author=author,
                    boundaries=boundaries,
                    role=role,
                    phrase=phrase,
                    text=text,
                    referent=referent,
                )

    stats = SourceSemanticWorkerStats()
    worker = SourceSemanticWorker(
        pool=pool, worker_id=f"source-equivalence-db:{tenant_id}"
    )
    expected_signals = len(SOURCE_CASES) * len(SIGNALS)
    expected_models = len(SOURCE_CASES) * len(MODEL_SIGNALS)
    claimed = await worker.process_batch(limit=expected_signals, stats=stats)
    if (
        claimed != expected_signals
        or stats.belief_applied != expected_models
        or stats.no_admission != len(SOURCE_CASES)
    ):
        raise AssertionError(
            f"source semantic batch incomplete: claimed={claimed}, stats={stats}"
        )

    rows: list[dict[str, Any]] = []
    async with pool.acquire() as conn:
        for source, _channel, expected_author, expected_boundaries in SOURCE_CASES:
            durable = {}
            for role, *_unused in MODEL_SIGNALS:
                durable[role] = await _load_semantic_outcome(
                    conn,
                    tenant_id=tenant_id,
                    observation_id=signal_records[source][role]["observation_id"],
                )
            vendor_model = durable["vendor"]["admitted_model_id"]
            atlas_model = durable["atlas"]["admitted_model_id"]
            evidence_ids = [
                signal_records[source]["vendor"]["observation_id"],
                signal_records[source]["atlas"]["observation_id"],
                signal_records[source]["relation"]["observation_id"],
            ]
            diff = ValidatedDiff(
                trigger_ref=uuid7(),
                tenant_id=tenant_id,
                relation_claim_ops=[RelationClaimOp(
                    source_model_id=vendor_model,
                    target_model_id=atlas_model,
                    subject_ref={"kind": "model", "model_id": str(vendor_model)},
                    object_ref={"kind": "model", "model_id": str(atlas_model)},
                    predicate="blocks",
                    edge_kind="blocks",
                    endpoint_binding_status="bound",
                    write_policy="accepted_edge",
                    status="accepted",
                    confidence=0.91,
                    binding_confidence=0.96,
                    evidence_event_ids=evidence_ids,
                    evidence_model_ids=[vendor_model, atlas_model],
                    evidence_text="AtlasProject is blocked by VendorDependency",
                    explanation="The delayed vendor dependency blocks AtlasProject.",
                    metadata={
                        "sealed_decision_provider": "source-equivalence-db-v2",
                        "source_kind": source,
                    },
                )],
            )
            async with conn.transaction():
                await apply_diff(
                    diff,
                    conn,
                    "T1",
                    evidence_ids[-1],
                    trigger_supporting_event_ids=evidence_ids,
                )
            edge = await conn.fetchrow(
                """SELECT edge_kind, source_model_id, target_model_id, metadata,
                          evidence_event_ids, evidence_model_ids, review_status
                   FROM model_edges
                   WHERE tenant_id=$1 AND source_model_id=$2 AND target_model_id=$3
                     AND edge_kind='blocks' AND status='active'""",
                tenant_id, vendor_model, atlas_model,
            )
            atlas = durable["atlas"]
            assertion = _json(atlas["source_assertion"])
            coordinate = assertion["coordinates"][0]
            content = _json(atlas["content"])
            rows.append({
                "semantic_case_id": "vendor-blocks-atlas",
                "source_kind": source,
                "batch_signal_count": expected_signals,
                "entity_refs": sorted(
                    f"{_json(item['selected_referent'])['type']}:"
                    f"{_json(item['selected_referent'])['id']}"
                    for item in durable.values()
                ),
                "model_signatures": sorted(
                    _model_signature(item) for item in durable.values()
                ),
                "relation_signatures": (
                    ["blocks:dependency:vendor:project:atlas"] if edge else []
                ),
                "authority_ref": assertion["current_speaker_or_author"],
                "expected_authority_ref": expected_author,
                "assertion_source_system": coordinate["source_system"],
                "expected_source_system": source,
                "boundary_refs": content["source_boundary_refs"],
                "expected_boundary_refs": list(expected_boundaries),
                "entity_lineage_complete": all(
                    _grounding_lineage_complete(item) for item in durable.values()
                ),
                "model_lineage_complete": all(
                    _model_lineage_complete(item) for item in durable.values()
                ),
                "relation_lineage_complete": _relation_lineage_complete(
                    edge, evidence_ids=evidence_ids,
                    model_ids=[vendor_model, atlas_model],
                ),
            })

    evaluation = evaluate_normalized_source_equivalence(
        rows, require_relation_exposure=True
    )
    objective: dict[str, Any] = {
        "schema_version": "source-equivalence-db-objective-v1",
        "tenant_id": str(tenant_id),
        "population": {
            "signal_batches": 1, "signals": expected_signals,
            "sources": len(SOURCE_CASES), "accepted_relation_claims": len(rows),
        },
        "worker": {
            "claimed": stats.claimed, "belief_applied": stats.belief_applied,
            "no_admission": stats.no_admission,
            "terminal_failures": stats.terminal_failures,
        },
        "relation_path": {
            "decision": "sealed_bound_relation_claim",
            "production_apply_path": "ValidatedDiff -> apply_diff -> relation_claims -> EdgesRepo",
            "accepted_edges": sum(bool(row["relation_signatures"]) for row in rows),
        },
        "proof_boundary": [
            "connector transport is excluded; inputs are persisted normalized signals",
            "relation extraction judgment is sealed and deterministic; relation persistence, validation, lineage, and graph mutation use production paths",
        ],
        "rows": rows,
        "evaluation": evaluation,
    }
    objective["objective_sha256"] = canonical_sha256(objective)
    if output_path is not None:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        temporary = output_path.with_suffix(".tmp")
        temporary.write_text(json.dumps(objective, indent=2, sort_keys=True) + "\n")
        temporary.replace(output_path)
    return objective


async def _persist_grounded_signal(
    conn: asyncpg.Connection,
    *, tenant_id: UUID, occurred_at: datetime, source: str, channel: str,
    author: str, boundaries: tuple[str, ...], role: str, phrase: str,
    text: str, referent: dict[str, str],
) -> dict[str, Any]:
    observation_id = uuid7()
    await conn.execute(
        """INSERT INTO observations (
             id,tenant_id,occurred_at,kind,source_channel,source_actor_ref,
             content,content_text,embedding,embedding_pending,trust_tier,
             entities_mentioned
           ) VALUES ($1,$2,$3,'signal',$4,$5,$6::jsonb,$7,$8,FALSE,
             'ordinary','[]'::jsonb)""",
        observation_id, tenant_id, occurred_at, channel, author,
        json.dumps({"text": text, "source_boundary_refs": boundaries, "role": role}),
        text, "[" + ",".join(["0.01"] * 768) + "]",
    )
    context_command, context_outcome = prepare_context_selection(
        tenant_id=tenant_id, observation_id=observation_id, phrase=phrase,
        occurred_at=occurred_at, source_channel=channel,
        source_space=f"{source}:sealed-space", topology_incomplete=False,
        boundary_hypotheses=({"kind": "source_topology"},),
        context_observations=(), selection_dependency_refs=(),
        now=occurred_at + timedelta(minutes=1),
    )
    mention_command = prepare_entity_mention_detection(
        tenant_id=tenant_id, observation_id=observation_id, phrase=phrase,
        content_text=text, source_channel=channel,
        context_command=context_command, context_outcome=context_outcome,
        now=occurred_at + timedelta(minutes=1),
    )
    episode = build_grounding_episode(
        tenant_id=tenant_id, observation_id=observation_id, phrase=phrase,
        occurred_at=occurred_at, source_channel=channel,
        source_space=f"{source}:sealed-space", topology_incomplete=False,
        boundary_hypotheses=({"kind": "source_topology"},),
        context_observations=(), selection_dependency_refs=(),
        candidates=(GroundingCandidateInput(
            canonical_ref=referent, candidate_source="tenant_aliases",
            positive_evidence_refs=(f"sealed-alias:{source}:{phrase}",),
            independent_identity_evidence_refs=(f"sealed-identity:{source}:{phrase}",),
        ),),
        model_candidate_id=candidate_id_for_ref(referent),
        model_canonical_ref=referent, model_confidence=0.93,
        model_reasoning="sealed exact identity candidate",
        high_confidence=0.8, review_min=0.5,
        prepared_context_command=context_command,
        prepared_context_outcome=context_outcome,
        prepared_mention_detection_command=mention_command,
        now=occurred_at + timedelta(minutes=1),
    )
    await EntityGroundingRepo(pool=object()).append_episode(  # type: ignore[arg-type]
        episode=episode, tenant_id=tenant_id,
        source_observation_id=observation_id, phrase=phrase, conn=conn,
    )
    trace_id = await conn.fetchval(
        "SELECT id FROM grounding_traces WHERE tenant_id=$1 AND source_observation_id=$2",
        tenant_id, observation_id,
    )
    await SourceSemanticRepo().enqueue_work(
        conn, tenant_id=tenant_id, grounding_trace_id=trace_id,
        now=occurred_at + timedelta(minutes=1),
    )
    return {"observation_id": observation_id, "trace_id": trace_id}


async def _load_semantic_outcome(
    conn: asyncpg.Connection, *, tenant_id: UUID, observation_id: UUID,
) -> asyncpg.Record:
    row = await conn.fetchrow(
        """SELECT o.content, gt.selected_referent, i.source_assertion,
                  i.semantic_frame, i.grounding_continuity,
                  a.admitted_model_id, m.proposition
           FROM observations o
           JOIN grounding_traces gt ON gt.tenant_id=o.tenant_id
            AND gt.source_observation_id=o.id
           JOIN source_semantic_interpretations i ON i.tenant_id=gt.tenant_id
            AND i.grounding_trace_id=gt.id
           JOIN source_semantic_admission_decisions a
            ON a.tenant_id=i.tenant_id AND a.interpretation_id=i.id
           JOIN models m ON m.tenant_id=a.tenant_id AND m.id=a.admitted_model_id
           WHERE o.tenant_id=$1 AND o.id=$2""",
        tenant_id, observation_id,
    )
    if row is None:
        raise AssertionError(f"missing durable semantic row for {observation_id}")
    return row


def _model_signature(row: asyncpg.Record) -> str:
    proposition = _json(row["proposition"])
    frame = _json(row["semantic_frame"])
    selected = _json(row["selected_referent"])
    return (
        f"{selected['type']}:{selected['id']}:"
        f"{proposition['kind']}:{proposition['assertion']}:"
        f"{frame['predicate_or_event_type']}"
    )


def _grounding_lineage_complete(row: asyncpg.Record) -> bool:
    continuity = _json(row["grounding_continuity"])
    return bool(
        continuity.get("mention_ref")
        and continuity.get("resolution_assessment_ref")
        and continuity.get("grounding_admission_ref")
        and continuity.get("selected_referent")
    )


def _model_lineage_complete(row: asyncpg.Record) -> bool:
    proposition = _json(row["proposition"])
    continuity = _json(row["grounding_continuity"])
    return bool(
        proposition.get("source_semantic_interpretation_id")
        and continuity.get("downstream_object_ref")
        == f"model:{row['admitted_model_id']}"
    )


def _relation_lineage_complete(
    edge: asyncpg.Record | None, *, evidence_ids: list[UUID], model_ids: list[UUID],
) -> bool:
    if edge is None:
        return False
    metadata = _json(edge["metadata"])
    return bool(
        metadata.get("relation_claim_id")
        and metadata.get("source") == "relation_claim_op"
        and edge["review_status"] == "accepted"
        and set(edge["evidence_event_ids"] or []) == set(evidence_ids)
        and set(edge["evidence_model_ids"] or []) == set(model_ids)
    )


def _json(value: Any) -> dict[str, Any]:
    if isinstance(value, str):
        value = json.loads(value)
    if not isinstance(value, dict):
        raise ValueError("expected JSON object")
    return value


__all__ = ["run_source_equivalence_db_vertical"]
