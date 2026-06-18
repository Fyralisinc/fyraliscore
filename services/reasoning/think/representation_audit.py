"""Representation-budget audits for Think runs.

The Think applier already answers "what did we mutate?". This module answers
the more important large-run question: "did this evidence window improve the
company representation enough, and where did coverage go missing?"
"""
from __future__ import annotations

import json
import os
from collections import Counter
from dataclasses import dataclass, field
from typing import Any
from uuid import UUID

import asyncpg
import structlog

from lib.shared.ids import uuid7
from services.reasoning.retrieval.primary import TriggerContext

from .representation_contract import trigger_observations_for_representation


_log = structlog.get_logger(__name__)


@dataclass
class RepresentationAudit:
    tenant_id: UUID
    run_id: UUID
    trigger_id: UUID
    trigger_kind: str
    observation_count: int
    model_context_count: int
    claim_insert_count: int
    model_update_count: int
    evidence_attachment_count: int
    near_duplicate_absorption_count: int
    relation_claim_count: int
    relation_frame_count: int
    edge_op_count: int
    source_digest_count: int
    model_adaptiveness: int
    edge_adaptiveness: int
    source_channels: list[str] = field(default_factory=list)
    coverage_roles: list[str] = field(default_factory=list)
    retrieval_tags: list[str] = field(default_factory=list)
    source_coverage: dict[str, int] = field(default_factory=dict)
    budget_status: str = "ok"
    warnings: list[dict[str, Any]] = field(default_factory=list)
    metrics: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "tenant_id": str(self.tenant_id),
            "run_id": str(self.run_id),
            "trigger_id": str(self.trigger_id),
            "trigger_kind": self.trigger_kind,
            "observation_count": self.observation_count,
            "model_context_count": self.model_context_count,
            "claim_insert_count": self.claim_insert_count,
            "model_update_count": self.model_update_count,
            "evidence_attachment_count": self.evidence_attachment_count,
            "near_duplicate_absorption_count": self.near_duplicate_absorption_count,
            "relation_claim_count": self.relation_claim_count,
            "relation_frame_count": self.relation_frame_count,
            "edge_op_count": self.edge_op_count,
            "source_digest_count": self.source_digest_count,
            "model_adaptiveness": self.model_adaptiveness,
            "edge_adaptiveness": self.edge_adaptiveness,
            "source_channels": list(self.source_channels),
            "coverage_roles": list(self.coverage_roles),
            "retrieval_tags": list(self.retrieval_tags),
            "source_coverage": dict(self.source_coverage),
            "budget_status": self.budget_status,
            "warnings": list(self.warnings),
            "metrics": dict(self.metrics),
        }


def build_representation_audit(
    *,
    trigger: TriggerContext,
    run_id: UUID,
    trigger_id: UUID,
    trigger_kind_full: str,
    validated: Any,
    bundle: Any,
    applied: dict[str, Any],
) -> RepresentationAudit:
    """Build the representation audit for a successfully applied diff."""
    observation_count, source_coverage = _source_coverage_for_trigger(trigger, bundle)
    memory = applied.get("memory_aggregation") if isinstance(applied, dict) else {}
    if not isinstance(memory, dict):
        memory = {}

    claim_insert_count = _int(memory.get("model_inserts"))
    model_update_count = _int(memory.get("model_updates"))
    evidence_attachment_count = _int(memory.get("evidence_attachments"))
    near_duplicate_absorption_count = _int(memory.get("near_duplicate_absorptions"))
    relation_claim_count = len(getattr(validated, "relation_claim_ops", []) or [])
    relation_frame_count = len(getattr(validated, "relation_frame_ops", []) or [])
    edge_op_count = len(getattr(validated, "edge_ops", []) or [])
    source_digest_count = _source_digest_count(validated)
    curiosity_count = _curiosity_count(validated)
    coverage_roles = _collect_claim_list_values(validated, "coverage_roles")
    retrieval_tags = _collect_claim_list_values(validated, "retrieval_tags")
    model_context_count = len(getattr(bundle, "models", []) or [])
    important_unknown_count = _important_unknown_count(bundle)
    selected_observation_count = len(getattr(bundle, "observations", []) or [])
    support_stats = _supporting_event_stats(getattr(bundle, "models", []) or [])
    question_coverage = _question_coverage(
        coverage_roles=coverage_roles,
        retrieval_tags=retrieval_tags,
    )
    truth_stats = _truth_maintenance_stats(validated, getattr(bundle, "models", []) or [])

    lifecycle_count = len(getattr(validated, "memory_lifecycle_ops", []) or [])
    model_adaptiveness = (
        claim_insert_count
        + model_update_count
        + evidence_attachment_count
        + near_duplicate_absorption_count
        + lifecycle_count
    )
    edge_adaptiveness = (
        edge_op_count
        + relation_claim_count
        + relation_frame_count
        + len(getattr(validated, "ontology_gap_ops", []) or [])
    )
    source_channels = sorted(source_coverage)

    metrics = {
        "lifecycle_ops": lifecycle_count,
        "ontology_gap_ops": len(getattr(validated, "ontology_gap_ops", []) or []),
        "act_ops": len(getattr(validated, "act_ops", []) or []),
        "resource_ops": len(getattr(validated, "resource_ops", []) or []),
        "claim_ops_validated": len(getattr(validated, "claim_ops", []) or []),
        "source_count": len(source_coverage),
        "largest_source_count": max(source_coverage.values(), default=0),
        "model_new_pressure": float(memory.get("new_model_pressure") or 0.0),
        "absorption_ratio": float(memory.get("absorption_ratio") or 0.0),
        "curiosity_count": curiosity_count,
        "important_unknown_count": important_unknown_count,
        "selected_observation_count": selected_observation_count,
        "max_selected_model_supporting_events": support_stats["max"],
        "avg_selected_model_supporting_events": support_stats["avg"],
        "question_coverage": question_coverage,
        "truth_maintenance": truth_stats,
    }
    warnings = _budget_warnings(
        trigger=trigger,
        observation_count=observation_count,
        source_coverage=source_coverage,
        coverage_roles=coverage_roles,
        retrieval_tags=retrieval_tags,
        model_adaptiveness=model_adaptiveness,
        edge_adaptiveness=edge_adaptiveness,
        source_digest_count=source_digest_count,
        curiosity_count=curiosity_count,
        important_unknown_count=important_unknown_count,
        selected_observation_count=selected_observation_count,
        support_stats=support_stats,
        question_coverage=question_coverage,
        truth_stats=truth_stats,
    )
    budget_status = "ok"
    if warnings:
        budget_status = "failed" if _strict_representation_budget_enabled() else "warning"

    return RepresentationAudit(
        tenant_id=trigger.tenant_id,
        run_id=run_id,
        trigger_id=trigger_id,
        trigger_kind=trigger_kind_full,
        observation_count=observation_count,
        model_context_count=model_context_count,
        claim_insert_count=claim_insert_count,
        model_update_count=model_update_count,
        evidence_attachment_count=evidence_attachment_count,
        near_duplicate_absorption_count=near_duplicate_absorption_count,
        relation_claim_count=relation_claim_count,
        relation_frame_count=relation_frame_count,
        edge_op_count=edge_op_count,
        source_digest_count=source_digest_count,
        model_adaptiveness=model_adaptiveness,
        edge_adaptiveness=edge_adaptiveness,
        source_channels=source_channels,
        coverage_roles=coverage_roles,
        retrieval_tags=retrieval_tags,
        source_coverage=source_coverage,
        budget_status=budget_status,
        warnings=warnings,
        metrics=metrics,
    )


async def persist_representation_audit(
    conn: asyncpg.Connection,
    audit: RepresentationAudit,
) -> None:
    """Persist the audit if the ledger migration is present.

    This is intentionally best-effort and savepoint-wrapped. Observability
    should never poison a successful Think transaction.
    """
    try:
        exists = await conn.fetchval(
            "SELECT to_regclass('public.think_representation_ledger')"
        )
    except asyncpg.PostgresError:
        return
    if exists is None:
        return

    payload = audit.to_dict()
    try:
        async with conn.transaction():
            await conn.execute(
                """
                INSERT INTO think_representation_ledger (
                  id, tenant_id, run_id, trigger_id, trigger_kind,
                  observation_count, model_context_count, claim_insert_count,
                  model_update_count, evidence_attachment_count,
                  near_duplicate_absorption_count, relation_claim_count,
                  relation_frame_count, edge_op_count, source_digest_count,
                  model_adaptiveness, edge_adaptiveness, source_channels,
                  coverage_roles, retrieval_tags, source_coverage,
                  budget_status, warnings, metrics
                )
                VALUES (
                  $1, $2, $3, $4, $5,
                  $6, $7, $8,
                  $9, $10,
                  $11, $12,
                  $13, $14, $15,
                  $16, $17, $18::jsonb,
                  $19::jsonb, $20::jsonb, $21::jsonb,
                  $22, $23::jsonb, $24::jsonb
                )
                """,
                uuid7(),
                audit.tenant_id,
                audit.run_id,
                audit.trigger_id,
                audit.trigger_kind,
                audit.observation_count,
                audit.model_context_count,
                audit.claim_insert_count,
                audit.model_update_count,
                audit.evidence_attachment_count,
                audit.near_duplicate_absorption_count,
                audit.relation_claim_count,
                audit.relation_frame_count,
                audit.edge_op_count,
                audit.source_digest_count,
                audit.model_adaptiveness,
                audit.edge_adaptiveness,
                _jsonb(payload["source_channels"]),
                _jsonb(payload["coverage_roles"]),
                _jsonb(payload["retrieval_tags"]),
                _jsonb(payload["source_coverage"]),
                audit.budget_status,
                _jsonb(payload["warnings"]),
                _jsonb(payload["metrics"]),
            )
    except asyncpg.PostgresError as exc:
        _log.warning(
            "think.representation_audit_persist_failed",
            run_id=str(audit.run_id),
            error=str(exc),
        )


def _budget_warnings(
    *,
    trigger: TriggerContext,
    observation_count: int,
    source_coverage: dict[str, int],
    coverage_roles: list[str],
    retrieval_tags: list[str],
    model_adaptiveness: int,
    edge_adaptiveness: int,
    source_digest_count: int,
    curiosity_count: int,
    important_unknown_count: int,
    selected_observation_count: int,
    support_stats: dict[str, float | int],
    question_coverage: dict[str, Any],
    truth_stats: dict[str, Any],
) -> list[dict[str, Any]]:
    warnings: list[dict[str, Any]] = []
    large_batch_min = _env_int("THINK_REPRESENTATION_LARGE_BATCH_MIN_OBS", 25)
    major_source_min = _env_int("THINK_REPRESENTATION_MAJOR_SOURCE_MIN_OBS", 8)
    min_large_batch_roles = _env_int(
        "THINK_REPRESENTATION_MIN_COVERAGE_ROLES_FOR_LARGE_BATCH", 3
    )
    min_large_batch_tags = _env_int(
        "THINK_REPRESENTATION_MIN_RETRIEVAL_TAGS_FOR_LARGE_BATCH", 3
    )
    min_large_batch_mutations = _env_int(
        "THINK_REPRESENTATION_MIN_MUTATIONS_PER_LARGE_BATCH", 1
    )
    min_large_batch_raw = _env_int(
        "THINK_REPRESENTATION_MIN_SELECTED_OBSERVATIONS_FOR_LARGE_BATCH", 4
    )
    support_runaway_max = _env_int(
        "THINK_REPRESENTATION_SUPPORT_EVENT_RUNAWAY_MAX", 500
    )
    support_runaway_avg = _env_int(
        "THINK_REPRESENTATION_SUPPORT_EVENT_RUNAWAY_AVG", 150
    )

    is_large_batch = trigger.kind == "T1" and observation_count >= large_batch_min
    total_adaptiveness = model_adaptiveness + edge_adaptiveness
    if is_large_batch and total_adaptiveness < min_large_batch_mutations:
        warnings.append(
            {
                "code": "large_batch_low_representation",
                "message": "Large evidence window produced too little durable representation change.",
                "observation_count": observation_count,
                "model_adaptiveness": model_adaptiveness,
                "edge_adaptiveness": edge_adaptiveness,
                "minimum_mutations": min_large_batch_mutations,
            }
        )
    if is_large_batch and len(set(coverage_roles)) < min_large_batch_roles:
        warnings.append(
            {
                "code": "coverage_roles_below_floor",
                "message": "Large evidence window did not cover enough representation spaces.",
                "coverage_roles": coverage_roles,
                "minimum_roles": min_large_batch_roles,
            }
        )
    if is_large_batch and len(set(retrieval_tags)) < min_large_batch_tags:
        warnings.append(
            {
                "code": "retrieval_tags_below_floor",
                "message": "Large evidence window produced too few deep retrieval hooks.",
                "retrieval_tags": retrieval_tags,
                "minimum_tags": min_large_batch_tags,
            }
        )
    if is_large_batch and selected_observation_count < min_large_batch_raw:
        warnings.append(
            {
                "code": "prompt_raw_evidence_below_floor",
                "message": (
                    "Large evidence window exposed too few raw observations to "
                    "Think; old memory may dominate fresh company evidence."
                ),
                "selected_observation_count": selected_observation_count,
                "minimum_selected_observations": min_large_batch_raw,
            }
        )

    major_sources = {
        source: count
        for source, count in source_coverage.items()
        if count >= major_source_min
    }
    if major_sources and "source" not in coverage_roles and source_digest_count <= 0:
        warnings.append(
            {
                "code": "missing_source_coverage",
                "message": "Major sources were present but no source-coverage model survived validation.",
                "major_sources": major_sources,
            }
        )
    repeated_like = any(count >= major_source_min for count in source_coverage.values())
    has_pattern = (
        "discovered_pattern" in coverage_roles
        or "contextual_recurrence" in coverage_roles
        or "contextual_recurrence" in retrieval_tags
        or "source_digest" in retrieval_tags
        or source_digest_count > 0
    )
    if is_large_batch and repeated_like and not has_pattern:
        warnings.append(
            {
                "code": "missing_discovered_pattern_coverage",
                "message": "Large/repetitive source window produced no discovered-pattern coverage.",
                "source_coverage": source_coverage,
            }
        )
    has_curiosity = (
        "curiosity" in coverage_roles
        or "coverage_curiosity" in retrieval_tags
        or "open_question" in retrieval_tags
        or "unresolved_unknown" in retrieval_tags
        or curiosity_count > 0
    )
    if is_large_batch and important_unknown_count > 0 and not has_curiosity:
        warnings.append(
            {
                "code": "missing_curiosity_coverage",
                "message": "Inquiry surfaced important unknowns, but no durable curiosity/open-question model survived validation.",
                "important_unknown_count": important_unknown_count,
            }
        )
    if (
        support_stats["max"] >= support_runaway_max
        or support_stats["avg"] >= support_runaway_avg
    ):
        warnings.append(
            {
                "code": "selected_model_support_runaway",
                "message": (
                    "Selected memory contains very large supporting-event arrays; "
                    "representation may be aggregating evidence into black-hole models."
                ),
                "max_selected_model_supporting_events": support_stats["max"],
                "avg_selected_model_supporting_events": support_stats["avg"],
                "max_threshold": support_runaway_max,
                "avg_threshold": support_runaway_avg,
            }
        )
    missing_question_spaces = [
        name
        for name, covered in question_coverage.get("spaces", {}).items()
        if not covered
    ]
    if (
        is_large_batch
        and len(missing_question_spaces) >= 4
        and source_digest_count <= 0
        and not has_pattern
    ):
        warnings.append(
            {
                "code": "company_question_coverage_too_thin",
                "message": (
                    "Large evidence window did not cover enough company-member "
                    "question spaces such as ownership, work, risk, customers, "
                    "truth, and next action."
                ),
                "missing_spaces": missing_question_spaces,
                "score": question_coverage.get("score"),
            }
        )
    if (
        is_large_batch
        and truth_stats["selected_prediction_models"] > 0
        and truth_stats["truth_pressure_ops"] <= 0
    ):
        warnings.append(
            {
                "code": "prediction_lifecycle_not_exercised",
                "message": (
                    "Prediction-like memory was selected, but the run emitted "
                    "no confirmation, falsification, resolution, or counter-edge."
                ),
                "selected_prediction_models": truth_stats["selected_prediction_models"],
            }
        )
    if (
        is_large_batch
        and truth_stats["selected_contestable_models"] >= 5
        and truth_stats["counter_relation_ops"] <= 0
        and truth_stats["falsify_or_revise_down_ops"] <= 0
    ):
        warnings.append(
            {
                "code": "truth_pressure_absent_for_contestable_memory",
                "message": (
                    "Many contestable memories were selected, but no explicit "
                    "counter-evidence or negative reconciliation was emitted."
                ),
                "selected_contestable_models": truth_stats["selected_contestable_models"],
            }
        )
    return warnings


def _truth_maintenance_stats(validated: Any, models: list[Any]) -> dict[str, Any]:
    lifecycle_ops = list(getattr(validated, "memory_lifecycle_ops", []) or [])
    claim_ops = list(getattr(validated, "claim_ops", []) or [])
    relation_claim_ops = list(getattr(validated, "relation_claim_ops", []) or [])
    edge_ops = list(getattr(validated, "edge_ops", []) or [])

    falsify_or_revise_down = 0
    confirm_or_resolution = 0
    for op in lifecycle_ops:
        action = str(getattr(op, "action", "") or "")
        confidence_delta = getattr(op, "confidence_delta", None)
        if action in {"falsify", "archive", "supersede"}:
            falsify_or_revise_down += 1
        elif action == "revise" and confidence_delta is not None and confidence_delta < 0:
            falsify_or_revise_down += 1
        elif action in {"confirm", "unchanged"}:
            confirm_or_resolution += 1
        if getattr(op, "resolution_outcome", None) is not None:
            confirm_or_resolution += 1

    counter_relation_ops = 0
    for op in [*relation_claim_ops, *edge_ops]:
        edge_kind = str(getattr(op, "edge_kind", "") or "")
        if edge_kind in {"contradicts", "weakens", "alternative_to"}:
            counter_relation_ops += 1

    claim_resolution_updates = 0
    contested_updates = 0
    for op in claim_ops:
        if getattr(op, "op", None) != "update":
            continue
        changes = getattr(op, "changes", None) or {}
        if not isinstance(changes, dict):
            continue
        if "resolution_outcome" in changes or "resolved_at" in changes:
            claim_resolution_updates += 1
        if "contested_count" in changes or "reading_contestable" in changes:
            contested_updates += 1

    selected_prediction_models = 0
    selected_contestable_models = 0
    selected_resolved_models = 0
    for model in models:
        if _model_is_prediction_like(model):
            selected_prediction_models += 1
        if _model_is_contestable(model):
            selected_contestable_models += 1
        if _model_value(model, "resolved_at") is not None or _model_value(
            model,
            "resolution_outcome",
        ) is not None:
            selected_resolved_models += 1

    truth_pressure_ops = (
        falsify_or_revise_down
        + confirm_or_resolution
        + counter_relation_ops
        + claim_resolution_updates
        + contested_updates
    )
    return {
        "selected_prediction_models": selected_prediction_models,
        "selected_contestable_models": selected_contestable_models,
        "selected_resolved_models": selected_resolved_models,
        "lifecycle_ops": len(lifecycle_ops),
        "falsify_or_revise_down_ops": falsify_or_revise_down,
        "confirm_or_resolution_ops": confirm_or_resolution,
        "counter_relation_ops": counter_relation_ops,
        "claim_resolution_updates": claim_resolution_updates,
        "contested_updates": contested_updates,
        "truth_pressure_ops": truth_pressure_ops,
    }


def _supporting_event_stats(models: list[Any]) -> dict[str, float | int]:
    counts: list[int] = []
    for model in models:
        values = getattr(model, "supporting_event_ids", None)
        if values is None and isinstance(model, dict):
            values = model.get("supporting_event_ids")
        if isinstance(values, (list, tuple, set)):
            counts.append(len(values))
    if not counts:
        return {"max": 0, "avg": 0.0}
    return {
        "max": max(counts),
        "avg": round(sum(counts) / len(counts), 4),
    }


def _question_coverage(
    *,
    coverage_roles: list[str],
    retrieval_tags: list[str],
) -> dict[str, Any]:
    tags = {_tagify(value) for value in [*coverage_roles, *retrieval_tags]}
    spaces = {
        "ownership": bool(tags & {"actor", "entity", "question_ownership", "unknown_responsible_owner", "coordination_debt"}),
        "work": bool(tags & {"workstream", "progress_signal", "delivery_risk", "role_recommendation"}),
        "risk": bool(tags & {"epistemic", "delivery_risk", "operational_churn", "role_concern"}),
        "customer": bool(tags & {"customer", "source_finance", "unknown_affected_customer", "candidate_customer_question"}),
        "truth": bool(tags & {"epistemic", "open_question", "unresolved_unknown", "coverage_curiosity"}),
        "pattern": bool(tags & {"discovered_pattern", "contextual_recurrence", "source_digest"}),
        "next_action": bool(tags & {"intervention", "success_driver", "operator_question", "manager_question"}),
        "temporal": bool(tags & {"temporal", "role_prediction", "major_source_window"}),
    }
    covered = sum(1 for value in spaces.values() if value)
    return {
        "spaces": spaces,
        "covered": covered,
        "total": len(spaces),
        "score": round(covered / max(1, len(spaces)), 4),
    }


def _source_coverage_for_trigger(
    trigger: TriggerContext,
    bundle: Any,
) -> tuple[int, dict[str, int]]:
    observations = trigger_observations_for_representation(trigger, bundle)
    trigger_ids = _trigger_observation_ids(trigger)
    if trigger_ids and not _batch_fragment_observations_present(trigger):
        scoped = [obs for obs in observations if getattr(obs, "id", None) in trigger_ids]
        primary_only_event_batch = (
            _is_event_batch_trigger(trigger)
            and not list(trigger.observation_ids or [])
            and len(scoped) <= 1
        )
        if scoped and not primary_only_event_batch:
            observations = scoped
    coverage = Counter(
        str(getattr(obs, "source_channel", "") or "unknown") for obs in observations
    )
    observation_count = len(observations) if (
        _is_event_batch_trigger(trigger) and not list(trigger.observation_ids or [])
    ) else (len(trigger_ids) if trigger_ids else len(observations))
    return observation_count, dict(sorted(coverage.items()))


def _batch_fragment_observations_present(trigger: TriggerContext) -> bool:
    signature = trigger.seed_signature if isinstance(trigger.seed_signature, dict) else {}
    return isinstance(signature.get("batch_signal_fragments"), list)


def _trigger_observation_ids(trigger: TriggerContext) -> set[UUID]:
    ids: set[UUID] = set()
    for value in [trigger.observation_id, *list(trigger.observation_ids or [])]:
        if value is None:
            continue
        try:
            ids.add(value if isinstance(value, UUID) else UUID(str(value)))
        except (TypeError, ValueError):
            continue
    return ids


def _is_event_batch_trigger(trigger: TriggerContext) -> bool:
    signature = trigger.seed_signature or {}
    return bool(
        trigger.is_batch
        or getattr(trigger, "subkind", None) == "event_batch"
        or signature.get("signal_type") == "event_batch"
        or signature.get("batch") is True
    )


def _collect_claim_list_values(validated: Any, key: str) -> list[str]:
    values: list[str] = []
    seen: set[str] = set()
    for op in getattr(validated, "claim_ops", []) or []:
        if getattr(op, "op", None) == "insert":
            entry = getattr(op, "entry", None) or {}
            prop = entry.get("proposition") if isinstance(entry, dict) else {}
        elif getattr(op, "op", None) == "update":
            changes = getattr(op, "changes", None) or {}
            prop = changes.get("proposition") if isinstance(changes, dict) else {}
        else:
            prop = {}
        if not isinstance(prop, dict):
            continue
        for raw in _string_list(prop.get(key)):
            value = _tagify(raw)
            if value and value not in seen:
                seen.add(value)
                values.append(value)
    return values


def _source_digest_count(validated: Any) -> int:
    count = 0
    for op in getattr(validated, "claim_ops", []) or []:
        if getattr(op, "op", None) != "insert":
            continue
        entry = getattr(op, "entry", None) or {}
        prop = entry.get("proposition") if isinstance(entry, dict) else {}
        if not isinstance(prop, dict):
            continue
        tags = set(_string_list(prop.get("retrieval_tags"))) | set(
            _string_list(prop.get("domain_tags"))
        )
        if "source_digest" in {_tagify(tag) for tag in tags}:
            count += 1
    return count


def _curiosity_count(validated: Any) -> int:
    count = 0
    curiosity_tags = {
        "curiosity",
        "coverage_curiosity",
        "open_question",
        "operating_question",
        "strategic_question",
        "unresolved_unknown",
        "success_driver",
    }
    for op in getattr(validated, "claim_ops", []) or []:
        if getattr(op, "op", None) != "insert":
            continue
        entry = getattr(op, "entry", None) or {}
        prop = entry.get("proposition") if isinstance(entry, dict) else {}
        if not isinstance(prop, dict):
            continue
        tags = {
            _tagify(tag)
            for tag in (
                _string_list(prop.get("coverage_roles"))
                + _string_list(prop.get("retrieval_tags"))
                + _string_list(prop.get("domain_tags"))
                + _string_list(entry.get("domain_tags"))
            )
        }
        if tags & curiosity_tags:
            count += 1
    return count


def _important_unknown_count(bundle: Any) -> int:
    notes = getattr(bundle, "notes", None)
    if not isinstance(notes, dict):
        return 0
    packet = notes.get("inquiry_context_packet")
    if not isinstance(packet, dict):
        return 0
    seen: set[str] = set()
    for key in ("important_unknowns",):
        value = packet.get(key)
        if isinstance(value, list):
            for item in value:
                tag = _tagify(str(item or ""))
                if tag:
                    seen.add(tag)
    obligations = packet.get("answer_obligations")
    if isinstance(obligations, dict) and isinstance(obligations.get("missing_slots"), list):
        for item in obligations["missing_slots"]:
            tag = _tagify(str(item or ""))
            if tag:
                seen.add(tag)
    verdict = packet.get("sufficiency_verdict")
    if isinstance(verdict, dict) and isinstance(verdict.get("remaining_unknowns"), list):
        for item in verdict["remaining_unknowns"]:
            tag = _tagify(str(item or ""))
            if tag:
                seen.add(tag)
    return len(seen)


def _model_is_prediction_like(model: Any) -> bool:
    claim_role = _tagify(str(_model_value(model, "claim_role") or ""))
    if claim_role == "prediction":
        return True
    prop = _model_value(model, "proposition")
    if isinstance(prop, dict):
        tags = {
            _tagify(str(value))
            for value in [
                prop.get("claim_role"),
                prop.get("kind"),
                prop.get("proposition_kind"),
                *(_string_list(prop.get("coverage_roles"))),
                *(_string_list(prop.get("retrieval_tags"))),
            ]
            if value is not None
        }
        if tags & {"prediction", "role_prediction"}:
            return True
    return bool(_model_value(model, "evaluate_at") or _model_value(model, "resolution_criteria"))


def _model_is_contestable(model: Any) -> bool:
    value = _model_value(model, "reading_contestable")
    if value is None:
        return True
    return bool(value)


def _model_value(model: Any, key: str) -> Any:
    if isinstance(model, dict):
        return model.get(key)
    try:
        return model[key]
    except (TypeError, KeyError, IndexError):
        return getattr(model, key, None)


def _strict_representation_budget_enabled() -> bool:
    return os.environ.get("THINK_REPRESENTATION_STRICT", "0").strip().lower() in {
        "1",
        "on",
        "true",
        "yes",
    }


def _env_int(name: str, default: int) -> int:
    try:
        return max(0, int(os.environ.get(name, str(default))))
    except ValueError:
        return default


def _int(value: Any) -> int:
    try:
        return max(0, int(value or 0))
    except (TypeError, ValueError):
        return 0


def _string_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value]
    if isinstance(value, (bytes, bytearray)):
        try:
            return [value.decode()]
        except UnicodeDecodeError:
            return []
    if isinstance(value, list):
        return [str(item) for item in value if item is not None and str(item).strip()]
    if isinstance(value, (tuple, set)):
        return [str(item) for item in value if item is not None and str(item).strip()]
    return []


def _tagify(value: str) -> str:
    return (
        str(value)
        .strip()
        .casefold()
        .replace(" ", "_")
        .replace("-", "_")
        .replace(":", "_")
    )


def _jsonb(value: Any) -> str:
    return json.dumps(value, sort_keys=True, default=str)


__all__ = [
    "RepresentationAudit",
    "build_representation_audit",
    "persist_representation_audit",
]
