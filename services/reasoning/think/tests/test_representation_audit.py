from __future__ import annotations

import pathlib
import json
from datetime import datetime, timezone
from types import SimpleNamespace
from uuid import uuid4

import pytest

from services.reasoning.retrieval.primary import TriggerContext
from services.reasoning.think.diff_schema import (
    ClaimOp,
    MemoryLifecycleOp,
    RelationClaimOp,
    ValidatedDiff,
)
from services.reasoning.think.representation_audit import (
    RepresentationAudit,
    build_representation_audit,
    persist_representation_audit,
)


def _obs(source: str = "github:webhook", text: str = "event") -> SimpleNamespace:
    return SimpleNamespace(
        id=uuid4(),
        source_channel=source,
        content_text=text,
        occurred_at=datetime(2026, 6, 17, tzinfo=timezone.utc),
    )


def test_representation_audit_flags_large_noop_batch() -> None:
    tenant_id = uuid4()
    observations = [_obs("github:webhook", "PR event") for _ in range(30)]
    trigger = TriggerContext(
        kind="T1",
        subkind="event_batch",
        tenant_id=tenant_id,
        observation_id=observations[0].id,
        observation_ids=[obs.id for obs in observations],
    )
    validated = ValidatedDiff(trigger_ref=uuid4(), tenant_id=tenant_id)
    applied = {
        "memory_aggregation": {
            "model_inserts": 0,
            "model_updates": 0,
            "evidence_attachments": 0,
            "near_duplicate_absorptions": 0,
        }
    }

    audit = build_representation_audit(
        trigger=trigger,
        run_id=uuid4(),
        trigger_id=uuid4(),
        trigger_kind_full="T1:event_batch",
        validated=validated,
        bundle=SimpleNamespace(models=[], observations=observations),
        applied=applied,
    )

    assert audit.budget_status == "warning"
    codes = {warning["code"] for warning in audit.warnings}
    assert "large_batch_low_representation" in codes
    assert "coverage_roles_below_floor" in codes
    assert "retrieval_tags_below_floor" in codes
    assert "missing_source_coverage" in codes
    assert "missing_discovered_pattern_coverage" in codes


def test_representation_audit_records_noop_outcome_metrics() -> None:
    tenant_id = uuid4()
    observations = [_obs("slack:event", "lunch logistics") for _ in range(30)]
    trigger = TriggerContext(
        kind="T1",
        subkind="event_batch",
        tenant_id=tenant_id,
        observation_id=observations[0].id,
        observation_ids=[obs.id for obs in observations],
    )
    validated = ValidatedDiff(
        trigger_ref=uuid4(),
        tenant_id=tenant_id,
        reasoning_trace="discard_as_noise: noise-only batch",
    )
    applied = {
        "memory_aggregation": {
            "model_inserts": 0,
            "model_updates": 0,
            "evidence_attachments": 0,
            "near_duplicate_absorptions": 0,
        },
        "context_use": {"context_use_grade": "justified_noop_context_used"},
        "reasoning_trace": "discard_as_noise: noise-only batch",
        "state_changes_emitted": 0,
    }

    audit = build_representation_audit(
        trigger=trigger,
        run_id=uuid4(),
        trigger_id=uuid4(),
        trigger_kind_full="T1:event_batch",
        validated=validated,
        bundle=SimpleNamespace(models=[], observations=observations),
        applied=applied,
    )

    assert audit.metrics["context_use_grade"] == "justified_noop_context_used"
    assert audit.metrics["state_changes_emitted"] == 0
    assert "discard_as_noise" in audit.metrics["reasoning_trace"]


def test_representation_audit_accepts_source_digest_pattern_batch() -> None:
    tenant_id = uuid4()
    observations = [_obs("aws:event", "iam:CreateAccessKey") for _ in range(30)]
    trigger = TriggerContext(
        kind="T1",
        subkind="event_batch",
        tenant_id=tenant_id,
        observation_id=observations[0].id,
        observation_ids=[obs.id for obs in observations],
    )
    validated = ValidatedDiff(
        trigger_ref=uuid4(),
        tenant_id=tenant_id,
        claim_ops=[
            ClaimOp(
                op="insert",
                entry={
                    "born_from_event_id": observations[0].id,
                    "natural": "The aws:event source is showing a recurring signal shape.",
                    "proposition": {
                        "kind": "belief",
                        "claim_role": "pattern",
                        "coverage_roles": [
                            "source",
                            "discovered_pattern",
                            "contextual_recurrence",
                        ],
                        "retrieval_tags": [
                            "source_digest",
                            "contextual_recurrence",
                            "source_observability",
                        ],
                    },
                },
            )
        ],
    )
    applied = {
        "memory_aggregation": {
            "model_inserts": 1,
            "model_updates": 0,
            "evidence_attachments": 0,
            "near_duplicate_absorptions": 0,
        }
    }

    audit = build_representation_audit(
        trigger=trigger,
        run_id=uuid4(),
        trigger_id=uuid4(),
        trigger_kind_full="T1:event_batch",
        validated=validated,
        bundle=SimpleNamespace(models=[], observations=observations),
        applied=applied,
    )

    assert audit.budget_status == "ok"
    assert audit.warnings == []
    assert audit.source_digest_count == 1
    assert audit.model_adaptiveness == 1


def test_representation_audit_counts_primary_only_event_batch_from_bundle() -> None:
    tenant_id = uuid4()
    observations = [_obs("github:webhook", f"pull request event {i}") for i in range(8)]
    trigger = TriggerContext(
        kind="T1",
        subkind="event_batch",
        tenant_id=tenant_id,
        observation_id=observations[0].id,
    )
    validated = ValidatedDiff(trigger_ref=uuid4(), tenant_id=tenant_id)
    applied = {"memory_aggregation": {}}

    audit = build_representation_audit(
        trigger=trigger,
        run_id=uuid4(),
        trigger_id=uuid4(),
        trigger_kind_full="T1:event_batch",
        validated=validated,
        bundle=SimpleNamespace(models=[], observations=observations),
        applied=applied,
    )

    assert audit.observation_count == 8
    assert audit.source_coverage == {"github:webhook": 8}


def test_representation_audit_counts_full_batch_fragments_when_bundle_is_pruned() -> None:
    tenant_id = uuid4()
    aws_rows = [_obs("aws:event", f"aws event {i}") for i in range(8)]
    github_rows = [_obs("github:webhook", f"github event {i}") for i in range(8)]
    observations = [*aws_rows, *github_rows]
    trigger = TriggerContext(
        kind="T1",
        subkind="event_batch",
        tenant_id=tenant_id,
        observation_id=observations[0].id,
        observation_ids=[obs.id for obs in observations],
        seed_signature={
            "batch": True,
            "signal_type": "event_batch",
            "batch_signal_fragments": [
                {
                    "observation_id": str(obs.id),
                    "occurred_at": obs.occurred_at.isoformat(),
                    "source_channel": obs.source_channel,
                    "kind": "signal",
                    "text": obs.content_text,
                }
                for obs in observations
            ],
        },
    )
    validated = ValidatedDiff(trigger_ref=uuid4(), tenant_id=tenant_id)
    applied = {"memory_aggregation": {}}

    audit = build_representation_audit(
        trigger=trigger,
        run_id=uuid4(),
        trigger_id=uuid4(),
        trigger_kind_full="T1:event_batch",
        validated=validated,
        bundle=SimpleNamespace(models=[], observations=aws_rows),
        applied=applied,
    )

    assert audit.observation_count == 16
    assert audit.source_coverage == {"aws:event": 8, "github:webhook": 8}


def test_representation_audit_flags_missing_curiosity_for_unresolved_unknowns() -> None:
    tenant_id = uuid4()
    observations = [_obs("slack:message", "Atlas blocker discussion") for _ in range(30)]
    trigger = TriggerContext(
        kind="T1",
        subkind="event_batch",
        tenant_id=tenant_id,
        observation_id=observations[0].id,
        observation_ids=[obs.id for obs in observations],
    )
    validated = ValidatedDiff(
        trigger_ref=uuid4(),
        tenant_id=tenant_id,
        claim_ops=[
            ClaimOp(
                op="insert",
                entry={
                    "born_from_event_id": observations[0].id,
                    "natural": "The slack source is showing a recurring blocker cadence.",
                    "proposition": {
                        "kind": "belief",
                        "claim_role": "pattern",
                        "coverage_roles": [
                            "source",
                            "discovered_pattern",
                            "contextual_recurrence",
                        ],
                        "retrieval_tags": [
                            "source_digest",
                            "contextual_recurrence",
                            "source_chat",
                        ],
                    },
                },
            )
        ],
    )
    applied = {
        "memory_aggregation": {
            "model_inserts": 1,
            "model_updates": 0,
            "evidence_attachments": 0,
            "near_duplicate_absorptions": 0,
        }
    }

    audit = build_representation_audit(
        trigger=trigger,
        run_id=uuid4(),
        trigger_id=uuid4(),
        trigger_kind_full="T1:event_batch",
        validated=validated,
        bundle=SimpleNamespace(
            models=[],
            observations=observations,
            notes={
                "inquiry_context_packet": {
                    "important_unknowns": [
                        "affected commitment",
                        "responsible owner",
                    ],
                    "answer_obligations": {
                        "missing_slots": ["whether the blocker is on the critical path"],
                    },
                }
            },
        ),
        applied=applied,
    )

    assert audit.budget_status == "warning"
    assert audit.metrics["important_unknown_count"] == 3
    assert audit.metrics["curiosity_count"] == 0
    assert "missing_curiosity_coverage" in {warning["code"] for warning in audit.warnings}


def test_representation_audit_flags_large_batch_with_too_little_selected_raw_evidence() -> None:
    tenant_id = uuid4()
    observations = [_obs("github:webhook", f"PR event {i}") for i in range(30)]
    trigger = TriggerContext(
        kind="T1",
        subkind="event_batch",
        tenant_id=tenant_id,
        observation_id=observations[0].id,
        observation_ids=[obs.id for obs in observations],
    )
    validated = ValidatedDiff(trigger_ref=uuid4(), tenant_id=tenant_id)
    applied = {
        "memory_aggregation": {
            "model_inserts": 1,
            "model_updates": 0,
            "evidence_attachments": 0,
            "near_duplicate_absorptions": 0,
        }
    }

    audit = build_representation_audit(
        trigger=trigger,
        run_id=uuid4(),
        trigger_id=uuid4(),
        trigger_kind_full="T1:event_batch",
        validated=validated,
        bundle=SimpleNamespace(models=[], observations=[]),
        applied=applied,
    )

    assert audit.metrics["selected_observation_count"] == 0
    assert "prompt_raw_evidence_below_floor" in {
        warning["code"] for warning in audit.warnings
    }


def test_representation_audit_flags_selected_model_support_runaway() -> None:
    tenant_id = uuid4()
    observations = [_obs("slack:message", f"Atlas update {i}") for i in range(30)]
    trigger = TriggerContext(
        kind="T1",
        subkind="event_batch",
        tenant_id=tenant_id,
        observation_id=observations[0].id,
        observation_ids=[obs.id for obs in observations],
    )
    support_ids = [uuid4() for _ in range(550)]
    validated = ValidatedDiff(trigger_ref=uuid4(), tenant_id=tenant_id)
    applied = {"memory_aggregation": {"model_inserts": 1}}

    audit = build_representation_audit(
        trigger=trigger,
        run_id=uuid4(),
        trigger_id=uuid4(),
        trigger_kind_full="T1:event_batch",
        validated=validated,
        bundle=SimpleNamespace(
            models=[SimpleNamespace(supporting_event_ids=support_ids)],
            observations=observations,
        ),
        applied=applied,
    )

    assert audit.metrics["max_selected_model_supporting_events"] == 550
    assert "selected_model_support_runaway" in {
        warning["code"] for warning in audit.warnings
    }


def test_representation_audit_accepts_curiosity_coverage_for_unresolved_unknowns() -> None:
    tenant_id = uuid4()
    observations = [_obs("slack:message", "Atlas blocker discussion") for _ in range(30)]
    trigger = TriggerContext(
        kind="T1",
        subkind="event_batch",
        tenant_id=tenant_id,
        observation_id=observations[0].id,
        observation_ids=[obs.id for obs in observations],
    )
    validated = ValidatedDiff(
        trigger_ref=uuid4(),
        tenant_id=tenant_id,
        claim_ops=[
            ClaimOp(
                op="insert",
                entry={
                    "born_from_event_id": observations[0].id,
                    "natural": "Open operating questions remain for Atlas.",
                    "proposition": {
                        "kind": "belief",
                        "claim_role": "hypothesis",
                        "hypothesis_text": "Open operating questions remain for Atlas.",
                        "coverage_roles": [
                            "curiosity",
                            "epistemic",
                            "intervention",
                        ],
                        "retrieval_tags": [
                            "open_question",
                            "unresolved_unknown",
                            "success_driver",
                            "coverage_curiosity",
                        ],
                    },
                    "domain_tags": ["open_question", "coverage_curiosity"],
                },
            )
        ],
    )
    applied = {
        "memory_aggregation": {
            "model_inserts": 1,
            "model_updates": 0,
            "evidence_attachments": 0,
            "near_duplicate_absorptions": 0,
        }
    }

    audit = build_representation_audit(
        trigger=trigger,
        run_id=uuid4(),
        trigger_id=uuid4(),
        trigger_kind_full="T1:event_batch",
        validated=validated,
        bundle=SimpleNamespace(
            models=[],
            observations=observations,
            notes={
                "inquiry_context_packet": {
                    "important_unknowns": [
                        "affected commitment",
                        "responsible owner",
                    ],
                }
            },
        ),
        applied=applied,
    )

    assert audit.metrics["important_unknown_count"] == 2
    assert audit.metrics["curiosity_count"] == 1
    assert "missing_curiosity_coverage" not in {
        warning["code"] for warning in audit.warnings
    }


def test_representation_audit_flags_prediction_memory_without_truth_pressure() -> None:
    tenant_id = uuid4()
    observations = [_obs("github:webhook", f"release signal {i}") for i in range(30)]
    trigger = TriggerContext(
        kind="T1",
        subkind="event_batch",
        tenant_id=tenant_id,
        observation_id=observations[0].id,
        observation_ids=[obs.id for obs in observations],
    )
    prediction_model = SimpleNamespace(
        id=uuid4(),
        claim_role="prediction",
        proposition={"claim_role": "prediction", "retrieval_tags": ["role_prediction"]},
        reading_contestable=True,
        supporting_event_ids=[],
    )

    audit = build_representation_audit(
        trigger=trigger,
        run_id=uuid4(),
        trigger_id=uuid4(),
        trigger_kind_full="T1:event_batch",
        validated=ValidatedDiff(trigger_ref=uuid4(), tenant_id=tenant_id),
        bundle=SimpleNamespace(models=[prediction_model], observations=observations),
        applied={"memory_aggregation": {"model_inserts": 1}},
    )

    assert audit.metrics["truth_maintenance"]["selected_prediction_models"] == 1
    assert "prediction_lifecycle_not_exercised" in {
        warning["code"] for warning in audit.warnings
    }


def test_representation_audit_truth_pressure_warning_clears_with_lifecycle_op() -> None:
    tenant_id = uuid4()
    observations = [_obs("github:webhook", f"release signal {i}") for i in range(30)]
    trigger = TriggerContext(
        kind="T1",
        subkind="event_batch",
        tenant_id=tenant_id,
        observation_id=observations[0].id,
        observation_ids=[obs.id for obs in observations],
    )
    model_id = uuid4()
    prediction_model = SimpleNamespace(
        id=model_id,
        claim_role="prediction",
        proposition={"claim_role": "prediction"},
        reading_contestable=True,
        supporting_event_ids=[],
    )
    validated = ValidatedDiff(
        trigger_ref=uuid4(),
        tenant_id=tenant_id,
        memory_lifecycle_ops=[
            MemoryLifecycleOp(
                model_id=model_id,
                action="falsify",
                evidence_event_ids=[observations[0].id],
                rationale="The promised release did not happen in the observed window.",
            )
        ],
    )

    audit = build_representation_audit(
        trigger=trigger,
        run_id=uuid4(),
        trigger_id=uuid4(),
        trigger_kind_full="T1:event_batch",
        validated=validated,
        bundle=SimpleNamespace(models=[prediction_model], observations=observations),
        applied={"memory_aggregation": {"model_inserts": 1}},
    )

    assert audit.metrics["truth_maintenance"]["falsify_or_revise_down_ops"] == 1
    assert "prediction_lifecycle_not_exercised" not in {
        warning["code"] for warning in audit.warnings
    }


def test_representation_audit_flags_contestable_memory_without_counter_pressure() -> None:
    tenant_id = uuid4()
    observations = [_obs("slack:message", f"ops signal {i}") for i in range(30)]
    trigger = TriggerContext(
        kind="T1",
        subkind="event_batch",
        tenant_id=tenant_id,
        observation_id=observations[0].id,
        observation_ids=[obs.id for obs in observations],
    )
    models = [
        SimpleNamespace(
            id=uuid4(),
            proposition={"claim_role": "fact"},
            reading_contestable=True,
            supporting_event_ids=[],
        )
        for _ in range(6)
    ]

    audit = build_representation_audit(
        trigger=trigger,
        run_id=uuid4(),
        trigger_id=uuid4(),
        trigger_kind_full="T1:event_batch",
        validated=ValidatedDiff(trigger_ref=uuid4(), tenant_id=tenant_id),
        bundle=SimpleNamespace(models=models, observations=observations),
        applied={"memory_aggregation": {"model_inserts": 1}},
    )

    assert audit.metrics["truth_maintenance"]["selected_contestable_models"] == 6
    assert "truth_pressure_absent_for_contestable_memory" in {
        warning["code"] for warning in audit.warnings
    }


def test_representation_audit_counter_relation_satisfies_truth_pressure() -> None:
    tenant_id = uuid4()
    observations = [_obs("slack:message", f"ops signal {i}") for i in range(30)]
    trigger = TriggerContext(
        kind="T1",
        subkind="event_batch",
        tenant_id=tenant_id,
        observation_id=observations[0].id,
        observation_ids=[obs.id for obs in observations],
    )
    source_model_id = uuid4()
    target_model_id = uuid4()
    models = [
        SimpleNamespace(
            id=uuid4(),
            proposition={"claim_role": "fact"},
            reading_contestable=True,
            supporting_event_ids=[],
        )
        for _ in range(6)
    ]
    validated = ValidatedDiff(
        trigger_ref=uuid4(),
        tenant_id=tenant_id,
        relation_claim_ops=[
            RelationClaimOp(
                source_model_id=source_model_id,
                target_model_id=target_model_id,
                predicate="weakens",
                edge_kind="weakens",
                endpoint_binding_status="bound",
                evidence_event_ids=[observations[0].id],
                explanation="The fresh operational signal weakens the older claim.",
            )
        ],
    )

    audit = build_representation_audit(
        trigger=trigger,
        run_id=uuid4(),
        trigger_id=uuid4(),
        trigger_kind_full="T1:event_batch",
        validated=validated,
        bundle=SimpleNamespace(models=models, observations=observations),
        applied={"memory_aggregation": {"model_inserts": 1}},
    )

    assert audit.metrics["truth_maintenance"]["counter_relation_ops"] == 1
    assert "truth_pressure_absent_for_contestable_memory" not in {
        warning["code"] for warning in audit.warnings
    }


@pytest.mark.integration
async def test_persist_representation_audit_writes_ledger(
    fresh_db,
    tenant,
    tenant_cleanup,
) -> None:
    run_id = uuid4()
    trigger_id = uuid4()
    audit = RepresentationAudit(
        tenant_id=tenant,
        run_id=run_id,
        trigger_id=trigger_id,
        trigger_kind="T1:event_batch",
        observation_count=30,
        model_context_count=10,
        claim_insert_count=1,
        model_update_count=2,
        evidence_attachment_count=3,
        near_duplicate_absorption_count=4,
        relation_claim_count=5,
        relation_frame_count=1,
        edge_op_count=6,
        source_digest_count=1,
        model_adaptiveness=10,
        edge_adaptiveness=12,
        source_channels=["github:webhook"],
        coverage_roles=["source", "discovered_pattern"],
        retrieval_tags=["source_digest"],
        source_coverage={"github:webhook": 30},
        budget_status="warning",
        warnings=[{"code": "example"}],
        metrics={"source_count": 1},
    )

    async with fresh_db.acquire() as conn:
        migration = (
            pathlib.Path(__file__).resolve().parents[4]
            / "db"
            / "migrations"
            / "0151_think_representation_ledger.sql"
        )
        await conn.execute(migration.read_text())
        await conn.execute(
            """
            INSERT INTO think_runs (id, tenant_id, trigger_id, trigger_kind)
            VALUES ($1, $2, $3, 'T1:event_batch')
            """,
            run_id,
            tenant,
            trigger_id,
        )
        await persist_representation_audit(conn, audit)
        row = await conn.fetchrow(
            """
            SELECT budget_status, model_adaptiveness, edge_adaptiveness, warnings
            FROM think_representation_ledger
            WHERE run_id = $1
            """,
            run_id,
        )

    assert row is not None
    assert row["budget_status"] == "warning"
    assert row["model_adaptiveness"] == 10
    assert row["edge_adaptiveness"] == 12
    warnings = row["warnings"]
    if isinstance(warnings, str):
        warnings = json.loads(warnings)
    assert warnings[0]["code"] == "example"
