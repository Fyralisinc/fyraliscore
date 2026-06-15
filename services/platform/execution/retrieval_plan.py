"""Retrieval action plan compilation for inquiry questions."""

from __future__ import annotations

from services.reasoning.retrieval.primary import TriggerContext

from .config import InquiryConfig
from .lexical_terms import focused_index_terms
from .question_policy import policy_budget
from .reflective_rules import (
    ReflectiveRetrievalRule,
    apply_reflective_rules_to_actions,
)
from .routing import trigger_text
from .types import (
    InquiryQuestion,
    LearnedRetrievalMotif,
    QuestionPolicySignal,
    RetrievalAction,
)


def compile_retrieval_plan(
    question: InquiryQuestion,
    trigger: TriggerContext,
    cfg: InquiryConfig,
    *,
    policy_signal: QuestionPolicySignal | None = None,
    learned_motif: LearnedRetrievalMotif | None = None,
    reflective_rules: tuple[ReflectiveRetrievalRule, ...] = (),
    apply_reflective_rules: bool = True,
) -> list[RetrievalAction]:
    static_actions = compile_static_retrieval_plan(
        question,
        trigger,
        cfg,
        policy_signal=policy_signal,
    )
    if learned_motif is None:
        actions = static_actions
    else:
        actions = (
            compile_motif_retrieval_plan(
                question,
                static_actions,
                learned_motif,
                cfg,
            )
            or static_actions
        )
    if not apply_reflective_rules or not cfg.reflective_rules_enabled:
        return actions
    return apply_reflective_rules_to_actions(
        question,
        actions,
        rules=reflective_rules,
    )


def compile_static_retrieval_plan(
    question: InquiryQuestion,
    trigger: TriggerContext,
    cfg: InquiryConfig,
    *,
    policy_signal: QuestionPolicySignal | None = None,
) -> list[RetrievalAction]:
    q = question.question
    seed_text = trigger_text(trigger)
    semantic_query = f"{q} {seed_text}".strip()
    common = {"seed_entities": list(trigger.seed_entity_ids)}
    semantic_budget = policy_budget(cfg.semantic_budget, policy_signal)
    focused_actions = focused_index_actions(
        question,
        trigger,
        cfg,
        policy_signal=policy_signal,
    )

    if question.primitive == "DEPENDENCY":
        return focused_actions + [
            RetrievalAction(
                question.question_id, "structural", "commitment_graph", filters=common
            ),
            RetrievalAction(
                question.question_id,
                "model_edge",
                "dependency_model_edges",
                filters=common,
                budget=policy_budget(60, policy_signal),
            ),
            RetrievalAction(
                question.question_id,
                "temporal",
                "recent_observations",
                query=semantic_query,
                filters={"window_days": cfg.temporal_window_days},
                budget=policy_budget(40, policy_signal),
            ),
            RetrievalAction(
                question.question_id,
                "semantic",
                "dependency_evidence",
                query=semantic_query,
                budget=semantic_budget,
            ),
        ]
    if question.primitive == "COMMITMENT":
        return focused_actions + [
            RetrievalAction(
                question.question_id, "structural", "active_commitments", filters=common
            ),
            RetrievalAction(
                question.question_id,
                "semantic",
                "commitment_evidence",
                query=f"active commitment promised outcome {seed_text}",
                budget=semantic_budget,
            ),
        ]
    if question.primitive == "COUNTEREVIDENCE":
        return focused_actions + [
            RetrievalAction(
                question.question_id,
                "semantic",
                "counterevidence",
                query=f"alternate explanation counterevidence not blocked not caused {seed_text}",
                budget=semantic_budget,
            ),
            RetrievalAction(
                question.question_id,
                "temporal",
                "recent_counterevidence",
                query=semantic_query,
                filters={"window_days": cfg.temporal_window_days},
                budget=policy_budget(30, policy_signal),
            ),
        ]
    if question.primitive == "CONSTRAINT":
        return focused_actions + [
            RetrievalAction(
                question.question_id,
                "structural",
                "goal_resource_bridge",
                filters=common,
            ),
            RetrievalAction(
                question.question_id,
                "model_edge",
                "constraint_resource_edges",
                filters=common,
                budget=policy_budget(60, policy_signal),
            ),
            RetrievalAction(
                question.question_id,
                "temporal",
                "recent_constraint_observations",
                query=semantic_query,
                filters={"window_days": cfg.temporal_window_days},
                budget=policy_budget(30, policy_signal),
            ),
            RetrievalAction(
                question.question_id,
                "semantic",
                "constraint_evidence",
                query=(
                    "constraint scarce resource capacity quota policy blocker "
                    f"{seed_text}"
                ),
                budget=semantic_budget,
            ),
        ]
    if question.primitive == "OWNERSHIP":
        return focused_actions + [
            RetrievalAction(
                question.question_id, "structural", "ownership_graph", filters=common
            ),
            RetrievalAction(
                question.question_id,
                "semantic",
                "owner_evidence",
                query=f"owner responsible assigned owns dependency {seed_text}",
                budget=semantic_budget,
            ),
        ]
    if question.primitive == "RECURRENCE":
        return focused_actions + [
            RetrievalAction(
                question.question_id,
                "pattern",
                "pattern_models",
                query=semantic_query,
                budget=policy_budget(80, policy_signal),
            ),
            RetrievalAction(
                question.question_id,
                "model_edge",
                "related_model_edges",
                filters=common,
                budget=policy_budget(80, policy_signal),
            ),
            RetrievalAction(
                question.question_id,
                "semantic",
                "recurrence_evidence",
                query=f"recurring pattern repeated similar issue {seed_text}",
                budget=semantic_budget,
            ),
        ]
    return focused_actions + [
        RetrievalAction(
            question.question_id, "structural", "goal_resource_bridge", filters=common
        ),
        RetrievalAction(
            question.question_id,
            "model_edge",
            "goal_resource_edges",
            filters=common,
            budget=policy_budget(60, policy_signal),
        ),
        RetrievalAction(
            question.question_id,
            "semantic",
            "goal_customer_resource_evidence",
            query=f"goal customer resource impact {seed_text}",
            budget=semantic_budget,
        ),
    ]


def compile_motif_retrieval_plan(
    question: InquiryQuestion,
    static_actions: list[RetrievalAction],
    motif: LearnedRetrievalMotif,
    cfg: InquiryConfig,
) -> list[RetrievalAction]:
    raw_actions = motif.plan.get("actions")
    if not isinstance(raw_actions, list):
        return []
    by_exact = {(action.path, action.target): action for action in static_actions}
    by_path: dict[str, RetrievalAction] = {}
    for action in static_actions:
        by_path.setdefault(action.path, action)

    compiled: list[RetrievalAction] = []
    seen: set[tuple[str, str, int]] = set()
    for raw in raw_actions[: max(1, int(cfg.retrieval_motif_max_actions))]:
        if not isinstance(raw, dict):
            continue
        path = str(raw.get("path") or "")
        target = str(raw.get("target") or "")
        base = by_exact.get((path, target)) or by_path.get(path)
        if base is None:
            continue
        try:
            stage = max(1, int(raw.get("stage") or 1))
        except (TypeError, ValueError):
            stage = 1
        key = (base.path, base.target, stage)
        if key in seen:
            continue
        seen.add(key)
        try:
            budget = int(raw.get("budget") or base.budget)
        except (TypeError, ValueError):
            budget = base.budget
        filters = dict(base.filters or {})
        filters.update(
            {
                "_motif_id": str(motif.id),
                "_motif_stage": stage,
                "_motif_match_score": round(float(motif.match_score), 4),
                "_motif_utility_score": round(float(motif.utility_score), 4),
            }
        )
        if bool(raw.get("bind_previous_scope")) and stage > 1:
            filters["_bind_previous_scope"] = True
        compiled.append(
            RetrievalAction(
                question_id=question.question_id,
                path=base.path,
                target=base.target,
                query=base.query,
                filters=filters,
                budget=max(1, budget),
            )
        )
    return compiled


def focused_index_actions(
    question: InquiryQuestion,
    trigger: TriggerContext,
    cfg: InquiryConfig,
    *,
    policy_signal: QuestionPolicySignal | None,
) -> list[RetrievalAction]:
    if not cfg.focused_index_enabled:
        return []
    terms = focused_index_terms(
        question.question,
        trigger,
        max_terms=int(cfg.focused_index_terms),
    )
    return [
        RetrievalAction(
            question.question_id,
            "focused_index",
            "question_answerability_scope",
            query=question.question,
            filters={
                "seed_entities": list(trigger.seed_entity_ids),
                "primitive": question.primitive,
                "terms": terms,
            },
            budget=policy_budget(cfg.focused_index_max_candidates, policy_signal),
        )
    ]
