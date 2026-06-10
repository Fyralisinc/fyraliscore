"""Unified reasoning frame for Think triggers.

The trigger kind says *what woke Think up*. The reasoning job says what
kind of belief work is needed. The reasoning frame is the prompt-facing
version of that job.

T2/T3/T4 intentionally collapse into one internal-reflection family:
prediction due, anomaly, and background maintenance differ by source
and intent, not by having separate cognitive engines.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any
from uuid import UUID

from services.reasoning.retrieval.primary import RetrievalResult, TriggerContext


@dataclass(frozen=True)
class ReasoningJob:
    """Normalized Think job produced from legacy trigger labels."""

    family: str
    source: str
    intent: str
    legacy_trigger_kind: str
    legacy_trigger_subkind: str | None = None
    question_to_answer: str = (
        "What minimal memory/action update should this trigger produce?"
    )
    allowed_ops: tuple[str, ...] = (
        "claim_ops",
        "edge_ops",
        "act_ops",
        "resource_ops",
    )
    priority_dimensions: tuple[str, ...] = (
        "cross_model_relationships",
        "composite_situations",
        "decision_leverage",
    )
    budget: dict[str, int] = field(
        default_factory=lambda: {
            "claim_ops": 3,
            "edge_ops": 4,
            "act_ops": 1,
            "resource_ops": 1,
        }
    )

    @property
    def frame_kind(self) -> str:
        return self.family


@dataclass(frozen=True)
class ReasoningFrame:
    frame_kind: str
    trigger_kind: str
    stimulus_kind: str
    job_source: str
    job_intent: str
    question_to_answer: str
    seed_model_ids: tuple[str, ...] = ()
    seed_entity_ids: tuple[dict[str, str], ...] = ()
    candidate_model_ids: tuple[str, ...] = ()
    time_window_days: float | None = None
    topology_event_kind: str | None = None
    neighborhood_id: str | None = None
    dynamic_signals: tuple[dict[str, Any], ...] = ()
    allowed_ops: tuple[str, ...] = (
        "claim_ops",
        "edge_ops",
        "act_ops",
        "resource_ops",
    )
    priority_dimensions: tuple[str, ...] = (
        "cross_model_relationships",
        "composite_situations",
        "decision_leverage",
    )
    budget: dict[str, int] = field(
        default_factory=lambda: {
            "claim_ops": 3,
            "edge_ops": 4,
            "act_ops": 1,
            "resource_ops": 1,
        }
    )
    policy: dict[str, Any] = field(
        default_factory=lambda: {
            "prefer_existing_models": True,
            "emit_edges_for_pairwise_relationships": True,
            "relationship_decision_required_when_graph_selected": True,
            "emit_situation_for_composite_conditions": True,
            "situation_requires_compositional_fields": True,
            "treat_topology_as_evidence_not_truth": True,
            "do_not_invent_ids": True,
            "minimize_diff": True,
        }
    )

    @classmethod
    def from_trigger(
        cls,
        trigger: TriggerContext,
        *,
        retrieval_result: RetrievalResult | None = None,
    ) -> "ReasoningFrame":
        job = reasoning_job_from_trigger(trigger)
        seed_model_ids = _seed_model_ids(trigger)
        candidate_model_ids = _candidate_model_ids(retrieval_result)
        seed_entity_ids = _seed_entity_ids(trigger)
        time_window_days = None
        if getattr(trigger, "temporal_window", None) is not None:
            time_window_days = trigger.temporal_window.total_seconds() / 86400.0

        policy = {
            "prefer_existing_models": True,
            "emit_edges_for_pairwise_relationships": True,
            "relationship_decision_required_when_graph_selected": True,
            "emit_situation_for_composite_conditions": True,
            "do_not_invent_ids": True,
            "minimize_diff": True,
            "internal_reflection_family": job.family == "internal_reflection",
            "treat_topology_as_evidence_not_truth": (
                job.source == "topology"
            ),
            "situation_requires_multiple_existing_models": True,
            "situation_requires_compositional_fields": True,
        }

        return cls(
            frame_kind=job.frame_kind,
            trigger_kind=trigger.kind,
            stimulus_kind=_stimulus_kind(trigger),
            job_source=job.source,
            job_intent=job.intent,
            question_to_answer=job.question_to_answer,
            seed_model_ids=seed_model_ids,
            seed_entity_ids=seed_entity_ids,
            candidate_model_ids=candidate_model_ids,
            time_window_days=time_window_days,
            topology_event_kind=trigger.topology_event_kind,
            neighborhood_id=(
                str(trigger.neighborhood_id)
                if trigger.neighborhood_id is not None
                else None
            ),
            allowed_ops=job.allowed_ops,
            priority_dimensions=job.priority_dimensions,
            budget=job.budget,
            policy=policy,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "frame_kind": self.frame_kind,
            "trigger_kind": self.trigger_kind,
            "stimulus_kind": self.stimulus_kind,
            "job_source": self.job_source,
            "job_intent": self.job_intent,
            "question_to_answer": self.question_to_answer,
            "seed_model_ids": list(self.seed_model_ids),
            "seed_entity_ids": [dict(e) for e in self.seed_entity_ids],
            "candidate_model_ids": list(self.candidate_model_ids),
            "time_window_days": self.time_window_days,
            "topology_event_kind": self.topology_event_kind,
            "neighborhood_id": self.neighborhood_id,
            "dynamic_signals": [dict(v) for v in self.dynamic_signals],
            "allowed_ops": list(self.allowed_ops),
            "priority_dimensions": list(self.priority_dimensions),
            "budget": dict(self.budget),
            "policy": dict(self.policy),
        }

    def with_dynamic_signals(
        self,
        dynamic_signals: list[dict[str, Any]],
    ) -> "ReasoningFrame":
        return ReasoningFrame(
            frame_kind=self.frame_kind,
            trigger_kind=self.trigger_kind,
            stimulus_kind=self.stimulus_kind,
            job_source=self.job_source,
            job_intent=self.job_intent,
            question_to_answer=self.question_to_answer,
            seed_model_ids=self.seed_model_ids,
            seed_entity_ids=self.seed_entity_ids,
            candidate_model_ids=self.candidate_model_ids,
            time_window_days=self.time_window_days,
            topology_event_kind=self.topology_event_kind,
            neighborhood_id=self.neighborhood_id,
            dynamic_signals=tuple(dynamic_signals),
            allowed_ops=self.allowed_ops,
            priority_dimensions=self.priority_dimensions,
            budget=self.budget,
            policy=self.policy,
        )

    def to_prompt_section(self) -> str:
        lines = [
            "<reasoning_frame>",
            f"  frame_kind: {self.frame_kind}",
            f"  stimulus_kind: {self.stimulus_kind}",
            f"  job_source: {self.job_source}",
            f"  job_intent: {self.job_intent}",
            f"  question_to_answer: {self.question_to_answer}",
        ]
        if self.seed_model_ids:
            lines.append(
                "  seed_model_ids: " + ", ".join(self.seed_model_ids[:12])
            )
        if self.candidate_model_ids:
            lines.append(
                "  candidate_model_ids: "
                + ", ".join(self.candidate_model_ids[:16])
            )
        if self.seed_entity_ids:
            rendered = ", ".join(
                f"{e.get('type')}:{e.get('id')}"
                for e in self.seed_entity_ids[:12]
            )
            lines.append(f"  seed_entity_ids: {rendered}")
        if self.topology_event_kind:
            lines.append(f"  topology_event_kind: {self.topology_event_kind}")
        if self.neighborhood_id:
            lines.append(f"  neighborhood_id: {self.neighborhood_id}")
        if self.time_window_days is not None:
            lines.append(f"  time_window_days: {self.time_window_days:.2f}")
        if self.dynamic_signals:
            lines.append("  dynamic_signals:")
            for signal in self.dynamic_signals[:6]:
                kind = signal.get("dynamic_kind", "dynamic")
                strength = signal.get("strength")
                confidence = signal.get("confidence")
                summary = str(signal.get("summary") or "")[:240]
                score = ""
                if strength is not None and confidence is not None:
                    score = (
                        f" strength={float(strength):.2f}"
                        f" confidence={float(confidence):.2f}"
                    )
                lines.append(f"    - {kind}:{score} {summary}")
        lines.append("  allowed_ops: " + ", ".join(self.allowed_ops))
        lines.append(
            "  priority_dimensions: " + ", ".join(self.priority_dimensions)
        )
        lines.append(
            "  budget: "
            + ", ".join(f"{k}<={v}" for k, v in sorted(self.budget.items()))
        )
        policy_true = [
            str(k) for k, v in sorted(self.policy.items()) if bool(v)
        ]
        if policy_true:
            lines.append("  policy: " + ", ".join(policy_true))
        lines.append("</reasoning_frame>")
        return "\n".join(lines)


def reasoning_job_from_trigger(trigger: TriggerContext) -> ReasoningJob:
    if trigger.kind == "T1":
        return ReasoningJob(
            family="external_signal_integration",
            source="external_signal",
            intent="integrate",
            legacy_trigger_kind=trigger.kind,
            legacy_trigger_subkind=trigger.subkind,
            question_to_answer=(
                "What changed, which existing Models does it connect to, "
                "what composite situation is forming, and what action is "
                "warranted?"
            ),
        )
    if trigger.kind == "T2" and trigger.subkind == "belief_updated":
        return _internal_reflection_job(
            trigger,
            source="model_change",
            intent="propagate_consequence",
            priority=("downstream_consequences",),
            allowed_ops=("claim_ops", "edge_ops", "act_ops"),
            budget={
                "claim_ops": 2,
                "edge_ops": 2,
                "act_ops": 1,
                "resource_ops": 0,
            },
        )
    if trigger.kind == "T2":
        return _internal_reflection_job(
            trigger,
            source="due_timer",
            intent="evaluate_existing_belief",
            priority=("downstream_consequences",),
        )
    if trigger.kind == "T3":
        return _internal_reflection_job(
            trigger,
            source="anomaly_detector",
            intent="explain_inconsistency",
            priority=("structural_explanation",),
            allowed_ops=("claim_ops", "edge_ops"),
            budget={
                "claim_ops": 3,
                "edge_ops": 3,
                "act_ops": 0,
                "resource_ops": 0,
            },
        )
    if trigger.kind == "T4":
        if trigger.subkind == "latent_relationship_candidate":
            return _internal_reflection_job(
                trigger,
                source="topology",
                intent="adjudicate_candidate",
                priority=(
                    "structural_explanation",
                    "impact_signature_interaction",
                ),
                allowed_ops=("claim_ops", "edge_ops"),
                budget={
                    "claim_ops": 2,
                    "edge_ops": 2,
                    "act_ops": 0,
                    "resource_ops": 0,
                },
            )
        return _internal_reflection_job(
            trigger,
            source="maintenance",
            intent="reorganize_memory",
            priority=("memory_quality",),
            allowed_ops=("claim_ops", "edge_ops"),
            budget={
                "claim_ops": 3,
                "edge_ops": 3,
                "act_ops": 0,
                "resource_ops": 0,
            },
        )
    if trigger.kind == "T6":
        return ReasoningJob(
            family="topology_shift",
            source="topology",
            intent="integrate_topology_shift",
            legacy_trigger_kind=trigger.kind,
            legacy_trigger_subkind=trigger.subkind,
            question_to_answer=(
                "What relationship or composite situation changed because "
                "this topology region shifted?"
            ),
            allowed_ops=("claim_ops", "edge_ops"),
            priority_dimensions=_priority_dimensions_with(
                "structural_explanation",
            ),
            budget={
                "claim_ops": 2,
                "edge_ops": 2,
                "act_ops": 0,
                "resource_ops": 0,
            },
        )
    return ReasoningJob(
        family="general_reasoning",
        source="unknown",
        intent="reason",
        legacy_trigger_kind=trigger.kind,
        legacy_trigger_subkind=trigger.subkind,
    )


def _internal_reflection_job(
    trigger: TriggerContext,
    *,
    source: str,
    intent: str,
    priority: tuple[str, ...] = (),
    allowed_ops: tuple[str, ...] = (
        "claim_ops",
        "edge_ops",
        "act_ops",
        "resource_ops",
    ),
    budget: dict[str, int] | None = None,
) -> ReasoningJob:
    return ReasoningJob(
        family="internal_reflection",
        source=source,
        intent=intent,
        legacy_trigger_kind=trigger.kind,
        legacy_trigger_subkind=trigger.subkind,
        question_to_answer=(
            "Given the current model layer and this internal signal, what "
            "belief should be revised, connected, downgraded, promoted, "
            "explained, or acted on?"
        ),
        allowed_ops=allowed_ops,
        priority_dimensions=_priority_dimensions_with(*priority),
        budget=budget
        if budget is not None
        else {
            "claim_ops": 3,
            "edge_ops": 2,
            "act_ops": 1,
            "resource_ops": 1,
        },
    )


def _stimulus_kind(trigger: TriggerContext) -> str:
    if trigger.subkind:
        return f"{trigger.kind}:{trigger.subkind}"
    if trigger.kind == "T6" and trigger.topology_event_kind:
        return f"T6:{trigger.topology_event_kind}"
    return trigger.kind


def _priority_dimensions_with(*front: str) -> tuple[str, ...]:
    base = [
        "cross_model_relationships",
        "composite_situations",
        "decision_leverage",
    ]
    for dimension in reversed(front):
        base.insert(0, dimension)
    return tuple(dict.fromkeys(base))


def _seed_model_ids(trigger: TriggerContext) -> tuple[str, ...]:
    ids: list[UUID] = []
    if trigger.model_id is not None:
        ids.append(trigger.model_id)
    ids.extend(trigger.member_model_ids or [])
    return tuple(str(v) for v in dict.fromkeys(ids))


def _candidate_model_ids(
    retrieval_result: RetrievalResult | None,
    *,
    limit: int = 16,
) -> tuple[str, ...]:
    if retrieval_result is None:
        return ()
    out: list[str] = []
    for model in retrieval_result.models[:limit]:
        model_id = getattr(model, "id", None)
        if model_id is None:
            continue
        out.append(str(model_id))
    return tuple(dict.fromkeys(out))


def _seed_entity_ids(trigger: TriggerContext) -> tuple[dict[str, str], ...]:
    out: list[dict[str, str]] = []
    for raw in trigger.seed_entity_ids or []:
        if not isinstance(raw, dict):
            continue
        entity_type = raw.get("type") or raw.get("kind") or raw.get("entity_kind")
        entity_id = raw.get("id")
        if entity_type is None or entity_id is None:
            continue
        out.append({"type": str(entity_type), "id": str(entity_id)})
    return tuple(out)


__all__ = ["ReasoningFrame", "ReasoningJob", "reasoning_job_from_trigger"]
