from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import asyncpg
import pytest

from lib.contracts.kernel import canonical_sha256
from lib.evaluation.correction_propagation import (
    CorrectionDependencyKind,
    CorrectionPropagationScope,
    evaluate_correction_propagation,
)
from lib.shared.ids import uuid7
from scripts.run_company_learning_pair_harness import run_pair_experiment


pytestmark = pytest.mark.integration


async def test_real_correction_audit_reports_stale_readable_debt_without_writes(
    resolver_db: asyncpg.Pool,
    tmp_path: Path,
) -> None:
    payload = await run_pair_experiment(
        pool=resolver_db,
        output_dir=tmp_path / "pair-evidence",
        run_id="pytest-correction-propagation-audit",
        system_version="pytest-correction-propagation-system",
        llm_call_cost_usd=0.001,
    )
    adaptive = payload["report"]["pairs"][0]["adaptive"]
    tenant_id = adaptive["tenant_id"]
    source_observation_id = adaptive["lineage"]["training_observation_id"]
    unrelated_tenant_id = uuid7()
    wrong_trace_id = uuid7()
    corrected_trace_id = uuid7()
    interpretation_id = uuid7()
    admission_id = uuid7()
    stale_model_id = uuid7()
    unrelated_subject_key = f"unrelated:{uuid7()}"
    stale_subject_key = f"stale-customer:{uuid7()}"

    async with resolver_db.acquire() as conn, conn.transaction():
        traces = await conn.fetch(
            """
            SELECT *
            FROM grounding_traces
            WHERE tenant_id=$1 AND source_observation_id=$2
            ORDER BY created_at, id
            """,
            tenant_id,
            source_observation_id,
        )
        predecessor = next(
            row
            for row in traces
            if not (row["trace"] or {}).get("supersedes_grounding_trace_id")
        )
        successor = next(
            row
            for row in traces
            if (row["trace"] or {}).get("supersedes_grounding_trace_id")
            == str(predecessor["id"])
        )
        wrong_referent = {"type": "customer", "id": str(uuid7()), "version": 1}
        corrected_referent = successor["selected_referent"]
        await conn.execute(
            """
            INSERT INTO grounding_traces (
              id, tenant_id, source_observation_id, phrase,
              context_snapshot_id, entity_mention_detection_id,
              entity_mention_id, candidate_request_id, candidate_set_id,
              resolution_assessment_id, grounding_admission_id,
              current_fate, selected_referent, identity_registry_mutated,
              source_observation_mutated, trace, created_at
            )
            SELECT
              $3, tenant_id, source_observation_id, phrase,
              context_snapshot_id, entity_mention_detection_id,
              entity_mention_id, candidate_request_id, candidate_set_id,
              resolution_assessment_id, grounding_admission_id,
              'resolved_for_consumer', $4::jsonb, FALSE, FALSE,
              $5::jsonb, now()
            FROM grounding_traces
            WHERE tenant_id=$1 AND id=$2
            """,
            tenant_id,
            predecessor["id"],
            wrong_trace_id,
            json.dumps(wrong_referent),
            json.dumps(
                {
                    **predecessor["trace"],
                    "seeded_wrong_grounding": True,
                }
            ),
        )
        await conn.execute(
            """
            INSERT INTO grounding_traces (
              id, tenant_id, source_observation_id, phrase,
              context_snapshot_id, entity_mention_detection_id,
              entity_mention_id, candidate_request_id, candidate_set_id,
              resolution_assessment_id, grounding_admission_id,
              current_fate, selected_referent, identity_registry_mutated,
              source_observation_mutated, trace, created_at
            )
            SELECT
              $3, tenant_id, source_observation_id, phrase,
              context_snapshot_id, entity_mention_detection_id,
              entity_mention_id, candidate_request_id, candidate_set_id,
              resolution_assessment_id, grounding_admission_id,
              'resolved_for_consumer', $4::jsonb, FALSE, FALSE,
              $5::jsonb, now() + interval '1 microsecond'
            FROM grounding_traces
            WHERE tenant_id=$1 AND id=$2
            """,
            tenant_id,
            successor["id"],
            corrected_trace_id,
            json.dumps(corrected_referent),
            json.dumps(
                {
                    **successor["trace"],
                    "supersedes_grounding_trace_id": str(wrong_trace_id),
                    "adjudication_ref": f"pytest-correction:{wrong_trace_id}",
                    "correction_kind": "entity_clarification_adjudication",
                }
            ),
        )
        source_content = await conn.fetchval(
            """
            SELECT content_text
            FROM observations
            WHERE tenant_id=$1 AND id=$2
            """,
            tenant_id,
            source_observation_id,
        )
        source_hash = canonical_sha256(source_content)
        proposition = {
            "kind": "state",
            "claim_role": "fact",
            "abstraction_level": "atomic",
            "time_mode": "current",
            "modality": "observed",
            "polarity": "negative",
            "subject": "NBI",
            "predicate": "is",
            "object": "incorrectly grounded",
        }
        embedding = [0.01] * 768
        await conn.execute(
            """
            INSERT INTO models (
              id, tenant_id, born_from_event_id,
              proposition, "natural", embedding,
              scope_temporal, confidence, activation,
              confidence_at_assertion, status, visible_to_subjects
            ) VALUES (
              $1, $2, $3, $4::jsonb, $5, $6::vector,
              '{"type":"now"}'::jsonb, 0.7, 1.0, 0.7,
              'active', TRUE
            )
            """,
            stale_model_id,
            tenant_id,
            source_observation_id,
            json.dumps(proposition),
            "NBI is incorrectly grounded",
            embedding,
        )
        await conn.execute(
            """
            INSERT INTO source_semantic_interpretations (
              id, tenant_id, grounding_trace_id, source_observation_id,
              context_snapshot_id, entity_mention_id,
              resolution_assessment_id, grounding_admission_id,
              source_content_hash, source_assertion, semantic_frame,
              speech_act, grounding_continuity, bundle_digest,
              extractor_version, recorded_at
            ) VALUES (
              $1, $2, $3, $4, $5, $6, $7, $8,
              $9, $10::jsonb, $11::jsonb, $12::jsonb, $13::jsonb,
              $14, 'pytest-correction-audit-v1', now()
            )
            """,
            interpretation_id,
            tenant_id,
            wrong_trace_id,
            source_observation_id,
            predecessor["context_snapshot_id"],
            predecessor["entity_mention_id"],
            predecessor["resolution_assessment_id"],
            predecessor["grounding_admission_id"],
            source_hash,
            json.dumps({"expressed_content": source_content}),
            json.dumps({"kind": "asserted_state"}),
            json.dumps({"kind": "assertion"}),
            json.dumps({"grounding_trace_id": str(wrong_trace_id)}),
            canonical_sha256(
                {
                    "tenant_id": tenant_id,
                    "grounding_trace_id": wrong_trace_id,
                    "model_id": stale_model_id,
                }
            ),
        )
        await conn.execute(
            """
            INSERT INTO source_semantic_admission_decisions (
              id, tenant_id, interpretation_id, disposition, reason_codes,
              proposed_belief_assertion, admitted_model_id,
              decision_digest, decided_at
            ) VALUES (
              $1, $2, $3, 'belief_applied',
              ARRAY['pytest_seeded_wrong_grounding'],
              $4::jsonb, $5, $6, now()
            )
            """,
            admission_id,
            tenant_id,
            interpretation_id,
            json.dumps(proposition),
            stale_model_id,
            canonical_sha256(
                {
                    "tenant_id": tenant_id,
                    "interpretation_id": interpretation_id,
                    "model_id": stale_model_id,
                }
            ),
        )
        await conn.execute(
            """
            INSERT INTO projection_snapshots (
              tenant_id, projection_name, projection_version, subject_key,
              payload, confidence, source_model_ids, source_event_ids
            ) VALUES (
              $1, 'customers', 'v1', $2,
              '{"status":"stale"}'::jsonb, 0.8, ARRAY[$3]::uuid[], '{}'
            )
            """,
            tenant_id,
            stale_subject_key,
            stale_model_id,
        )
        await conn.execute(
            "INSERT INTO tenants (id) VALUES ($1)",
            unrelated_tenant_id,
        )
        await conn.execute(
            """
            INSERT INTO projection_snapshots (
              tenant_id, projection_name, projection_version, subject_key,
              payload, confidence, source_model_ids, source_event_ids
            ) VALUES (
              $1, 'customers', 'v1', $2,
              '{"status":"unrelated"}'::jsonb, 0.5, '{}', '{}'
            )
            """,
            unrelated_tenant_id,
            unrelated_subject_key,
        )

    async with resolver_db.acquire() as conn:
        source_before = await conn.fetchrow(
            """
            SELECT content_text, content, entities_mentioned
            FROM observations
            WHERE tenant_id=$1 AND id=$2
            """,
            tenant_id,
            source_observation_id,
        )
        unrelated_before = await conn.fetchval(
            """
            SELECT to_jsonb(snapshot)
            FROM projection_snapshots snapshot
            WHERE tenant_id=$1 AND projection_name='customers'
              AND projection_version='v1' AND subject_key=$2
            """,
            unrelated_tenant_id,
            unrelated_subject_key,
        )
        async with conn.transaction(isolation="repeatable_read", readonly=True):
            audit = await evaluate_correction_propagation(
                conn,
                scope=CorrectionPropagationScope(
                    tenant_id=tenant_id,
                    predecessor_grounding_trace_id=wrong_trace_id,
                    run_id="pytest-real-correction-propagation-audit",
                    observed_at=datetime.now(timezone.utc),
                ),
                artifact_refs=("pytest:real-correction-propagation-audit",),
            )
        source_after = await conn.fetchrow(
            """
            SELECT content_text, content, entities_mentioned
            FROM observations
            WHERE tenant_id=$1 AND id=$2
            """,
            tenant_id,
            source_observation_id,
        )
        unrelated_after = await conn.fetchval(
            """
            SELECT to_jsonb(snapshot)
            FROM projection_snapshots snapshot
            WHERE tenant_id=$1 AND projection_name='customers'
              AND projection_version='v1' AND subject_key=$2
            """,
            unrelated_tenant_id,
            unrelated_subject_key,
        )

    assert audit.correction_found is True
    assert audit.correction_grounding_trace_id == corrected_trace_id
    assert audit.source_immutable is True
    assert audit.cross_tenant_reference_count == 0
    assert audit.cross_tenant_change_count == 0
    assert audit.unsafe_readable_count >= 2
    assert audit.residual_repair_debt_count >= 2
    assert audit.converged is False
    assert CorrectionDependencyKind.MODEL in {
        item.kind for item in audit.dependencies
    }
    assert CorrectionDependencyKind.MODEL_BELIEF_ADDRESS in {
        item.kind for item in audit.dependencies
    }
    assert CorrectionDependencyKind.PROJECTION_SNAPSHOT in {
        item.kind for item in audit.dependencies
    }
    assert source_after == source_before
    assert unrelated_after == unrelated_before
