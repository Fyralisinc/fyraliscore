"""Retrieval action plan compilation for inquiry questions."""

from __future__ import annotations

from dataclasses import dataclass

from services.reasoning.retrieval.primary import TriggerContext

from .config import InquiryConfig
from .lexical_terms import focused_index_terms
from .question_policy import policy_budget
from .reflective_rules import (
    ReflectiveRetrievalRule,
    apply_reflective_rules_to_actions,
)
from .types import (
    InquiryQuestion,
    LearnedRetrievalMotif,
    QuestionPolicySignal,
    RetrievalAction,
)


@dataclass(frozen=True, slots=True)
class _RetrievalQueryContext:
    focused_terms: tuple[str, ...]
    compact_terms: tuple[str, ...]


def _build_retrieval_query_context(
    question: InquiryQuestion,
    trigger: TriggerContext,
    cfg: InquiryConfig,
) -> _RetrievalQueryContext:
    focused_limit = max(1, int(cfg.focused_index_terms))
    compact_limit = max(4, min(12, focused_limit + 4))
    terms = tuple(
        focused_index_terms(
            question.question,
            trigger,
            max_terms=max(focused_limit, compact_limit),
        )
    )
    return _RetrievalQueryContext(
        focused_terms=terms[:focused_limit],
        compact_terms=terms[:compact_limit],
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
    query_context = _build_retrieval_query_context(question, trigger, cfg)
    semantic_query = compact_action_query(question, trigger, cfg, query_context=query_context)
    common = {"seed_entities": list(trigger.seed_entity_ids)}
    semantic_budget = policy_budget(cfg.semantic_budget, policy_signal)
    focused_actions = focused_index_actions(
        question,
        trigger,
        cfg,
        policy_signal=policy_signal,
        query_context=query_context,
    )

    def semantic_terms_action(target: str, *, query: str | None = None) -> RetrievalAction:
        return RetrievalAction(
            question.question_id,
            "semantic_terms",
            target,
            query=query or semantic_query,
            filters={
                **common,
                "_sage_policy_stage": 1,
                "_sage_policy_mode": "preferred",
                "_sage_policy_reason": "semantic_terms_first",
            },
            budget=semantic_budget,
        )

    def dense_semantic_fallback(
        target: str,
        *,
        query: str,
        min_models: int | None = None,
    ) -> RetrievalAction:
        return RetrievalAction(
            question.question_id,
            "semantic",
            target,
            query=query,
            filters={
                "_sage_policy_stage": 2,
                "_sage_policy_mode": "probe",
                "_sage_policy_reason": "dense_semantic_fallback_after_semantic_terms",
                "_semantic_fallback_after_terms": True,
                "_bind_previous_scope": True,
                "_fallback_min_semantic_terms_models": (
                    int(min_models)
                    if min_models is not None
                    else int(cfg.semantic_terms_fallback_min_models)
                ),
                "_fallback_min_cheap_context_models": max(
                    6,
                    int(cfg.semantic_terms_fallback_min_models) * 2,
                ),
            },
            budget=semantic_budget,
        )

    def temporal_nearby_action(
        target: str,
        *,
        budget: int,
    ) -> RetrievalAction:
        return RetrievalAction(
            question.question_id,
            "temporal",
            target,
            query=semantic_query,
            filters={
                "window_days": max(1, int(cfg.temporal_nearby_window_days)),
                "_temporal_lane": "nearby",
                "_temporal_nearby_fallback_after_cheap_context": True,
                "_fallback_min_temporal_semantic_terms_models": max(
                    1,
                    int(cfg.semantic_terms_fallback_min_models),
                ),
                "_fallback_min_temporal_cheap_context_models": max(
                    8,
                    int(cfg.semantic_terms_fallback_min_models) * 2,
                ),
                "_temporal_scope_filter_strategy": "time_prefilter",
                "_temporal_include_entity_mentions": True,
                "_sage_policy_stage": 2,
                "_sage_policy_mode": "probe",
                "_sage_policy_reason": "narrow_temporal_after_cheap_context",
            },
            budget=budget,
        )

    def temporal_broad_fallback(
        target: str,
        *,
        budget: int,
    ) -> RetrievalAction:
        return RetrievalAction(
            question.question_id,
            "temporal",
            target,
            query=semantic_query,
            filters={
                "window_days": max(1, int(cfg.temporal_broad_window_days)),
                "_temporal_lane": "broad",
                "_temporal_scope_filter_strategy": "indexed_or",
                "_temporal_include_entity_mentions": True,
                "_sage_policy_stage": 3,
                "_sage_policy_mode": "probe",
                "_sage_policy_reason": "broad_temporal_fallback_after_nearby",
                "_temporal_broad_fallback_after_nearby": True,
                "_fallback_min_temporal_records": max(
                    1,
                    int(cfg.temporal_broad_fallback_min_records),
                ),
                "_bind_previous_scope": True,
            },
            budget=budget,
        )

    if question.primitive == "DEPENDENCY":
        dependency_query = semantic_query
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
            temporal_nearby_action(
                "recent_observations",
                budget=policy_budget(40, policy_signal),
            ),
            semantic_terms_action("dependency_semantic_terms", query=dependency_query),
            dense_semantic_fallback(
                "dependency_evidence",
                query=dependency_query,
            ),
        ]
    if question.primitive == "COMMITMENT":
        commitment_query = compact_action_query(
            question,
            trigger,
            cfg,
            prefix="active commitment promised outcome",
            query_context=query_context,
        )
        return focused_actions + [
            RetrievalAction(
                question.question_id, "structural", "active_commitments", filters=common
            ),
            semantic_terms_action("commitment_semantic_terms", query=commitment_query),
            dense_semantic_fallback(
                "commitment_evidence",
                query=commitment_query,
            ),
        ]
    if question.primitive == "COUNTEREVIDENCE":
        counter_query = compact_action_query(
            question,
            trigger,
            cfg,
            prefix="alternate explanation counterevidence not blocked not caused",
            query_context=query_context,
        )
        return focused_actions + [
            temporal_nearby_action(
                "nearby_counterevidence",
                budget=policy_budget(30, policy_signal),
            ),
            temporal_broad_fallback(
                "recent_counterevidence",
                budget=policy_budget(30, policy_signal),
            ),
            semantic_terms_action("counterevidence_semantic_terms", query=counter_query),
            dense_semantic_fallback(
                "counterevidence",
                query=counter_query,
                min_models=max(4, int(cfg.semantic_terms_fallback_min_models)),
            ),
        ]
    if question.primitive == "CONSTRAINT":
        constraint_query = compact_action_query(
            question,
            trigger,
            cfg,
            prefix="constraint scarce resource capacity quota policy blocker",
            query_context=query_context,
        )
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
            temporal_nearby_action(
                "recent_constraint_observations",
                budget=policy_budget(30, policy_signal),
            ),
            semantic_terms_action("constraint_semantic_terms", query=constraint_query),
            dense_semantic_fallback(
                "constraint_evidence",
                query=constraint_query,
            ),
        ]
    if question.primitive == "OWNERSHIP":
        owner_query = compact_action_query(
            question,
            trigger,
            cfg,
            prefix="owner responsible assigned owns dependency",
            query_context=query_context,
        )
        return focused_actions + [
            RetrievalAction(
                question.question_id, "structural", "ownership_graph", filters=common
            ),
            semantic_terms_action("owner_semantic_terms", query=owner_query),
            dense_semantic_fallback(
                "owner_evidence",
                query=owner_query,
            ),
        ]
    if question.primitive == "RECURRENCE":
        recurrence_query = compact_action_query(
            question,
            trigger,
            cfg,
            prefix="recurring pattern repeated similar issue",
            query_context=query_context,
        )
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
            semantic_terms_action("recurrence_semantic_terms", query=recurrence_query),
            dense_semantic_fallback(
                "recurrence_evidence",
                query=recurrence_query,
            ),
        ]
    goal_query = compact_action_query(
        question,
        trigger,
        cfg,
        prefix="goal customer resource impact",
        query_context=query_context,
    )
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
        semantic_terms_action("goal_customer_resource_semantic_terms", query=goal_query),
        dense_semantic_fallback(
            "goal_customer_resource_evidence",
            query=goal_query,
        ),
    ]


def compact_action_query(
    question: InquiryQuestion,
    trigger: TriggerContext,
    cfg: InquiryConfig,
    *,
    prefix: str = "",
    query_context: _RetrievalQueryContext | None = None,
) -> str:
    """Build a short retrieval query from the question plus material anchors."""
    anchors = (
        list(query_context.compact_terms)
        if query_context is not None
        else focused_index_terms(
            question.question,
            trigger,
            max_terms=max(4, min(12, int(cfg.focused_index_terms) + 4)),
        )
    )
    parts: list[str] = []
    for raw in (prefix, question.question, " ".join(anchors)):
        clean = " ".join(str(raw or "").split())
        if clean and clean not in parts:
            parts.append(clean)
    query = " ".join(parts).strip()
    if len(query) <= 420:
        return query
    return query[:420].rsplit(" ", 1)[0].strip()


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
    query_context: _RetrievalQueryContext | None = None,
) -> list[RetrievalAction]:
    if not cfg.focused_index_enabled:
        return []
    terms = list(
        query_context.focused_terms
        if query_context is not None
        else focused_index_terms(
            question.question,
            trigger,
            max_terms=int(cfg.focused_index_terms),
        )
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
