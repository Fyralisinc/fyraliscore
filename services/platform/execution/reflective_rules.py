"""Reflective retrieval policy rules for inquiry planning.

These rules are learned policy hints. They do not retrieve data themselves and
they do not replace safe static retrieval operators; they only steer question
priority, question wording, action ordering, and query terms.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, replace
from typing import Any
from uuid import UUID

import asyncpg

from services.reasoning.retrieval.primary import TriggerContext

from .motif_utils import json_obj, motif_domain_terms, set_overlap_ratio
from .question_text import question_anchors, truncate_text
from .routing import signal_class_for_trigger, trigger_text
from .types import InquiryQuestion, RetrievalAction


@dataclass(frozen=True, slots=True)
class ReflectiveRetrievalRule:
    id: UUID
    signature: dict[str, Any]
    rule_pack: dict[str, Any]
    utility_score: float
    success_count: int
    match_score: float


def reflective_signature_for(trigger: TriggerContext) -> dict[str, Any]:
    return {
        "signal_type": trigger.kind,
        "signal_class": signal_class_for_trigger(trigger),
        "entity_types": sorted(
            {
                str(entity.get("type") or "").casefold()
                for entity in trigger.seed_entity_ids
                if isinstance(entity, dict) and entity.get("type")
            }
        ),
        "domain_terms": motif_domain_terms(trigger_text(trigger)),
    }


def reflective_signature_match_score(
    stored: dict[str, Any],
    current: dict[str, Any],
) -> float:
    score = 0.0
    if stored.get("signal_type") == current.get("signal_type"):
        score += 0.24
    if stored.get("signal_class") == current.get("signal_class"):
        score += 0.24
    score += 0.22 * set_overlap_ratio(
        stored.get("entity_types"),
        current.get("entity_types"),
    )
    domain_overlap = set_overlap_ratio(
        stored.get("domain_terms"),
        current.get("domain_terms"),
    )
    if domain_overlap == 0.0 and not stored.get("domain_terms"):
        domain_overlap = 0.45
    score += 0.30 * domain_overlap
    return round(min(score, 1.0), 4)


async def load_reflective_retrieval_rules(
    conn: asyncpg.Connection,
    trigger: TriggerContext,
    *,
    enabled: bool,
    limit: int,
    match_threshold: float,
) -> tuple[ReflectiveRetrievalRule, ...]:
    if not enabled or limit <= 0:
        return ()
    table_name = await conn.fetchval(
        "SELECT to_regclass('public.reflective_retrieval_rules')"
    )
    if table_name is None:
        return ()
    rows = await conn.fetch(
        """
        SELECT id, signature, rule_pack, utility_score, success_count
        FROM reflective_retrieval_rules
        WHERE tenant_id = $1
          AND maturity = 'active'
          AND utility_score > 0
          AND (expires_at IS NULL OR expires_at > now())
        ORDER BY utility_score DESC, success_count DESC, updated_at DESC
        LIMIT 64
        """,
        trigger.tenant_id,
    )
    if not rows:
        return ()

    current = reflective_signature_for(trigger)
    matches: list[ReflectiveRetrievalRule] = []
    for row in rows:
        signature = json_obj(row["signature"])
        match_score = reflective_signature_match_score(signature, current)
        if match_score < match_threshold:
            continue
        matches.append(
            ReflectiveRetrievalRule(
                id=row["id"],
                signature=signature,
                rule_pack=json_obj(row["rule_pack"]),
                utility_score=float(row["utility_score"] or 0.0),
                success_count=int(row["success_count"] or 0),
                match_score=match_score,
            )
        )
    matches.sort(
        key=lambda rule: (
            -rule.match_score,
            -rule.utility_score,
            -rule.success_count,
            str(rule.id),
        )
    )
    return tuple(matches[: max(1, int(limit))])


def reflective_rules_note(
    rules: tuple[ReflectiveRetrievalRule, ...],
    *,
    applied: bool,
    shadow_only: bool,
) -> dict[str, Any]:
    if not rules:
        return {
            "reflective_rules": {
                "loaded": 0,
                "applied": False,
                "shadow_only": bool(shadow_only),
            }
        }
    return {
        "reflective_rules": {
            "loaded": len(rules),
            "applied": bool(applied),
            "shadow_only": bool(shadow_only),
            "rule_ids": [str(rule.id) for rule in rules],
            "match_scores": [round(rule.match_score, 4) for rule in rules],
        }
    }


def reflective_rules_prompt_payload(
    rules: tuple[ReflectiveRetrievalRule, ...],
) -> list[dict[str, Any]]:
    payload: list[dict[str, Any]] = []
    for rule in rules[:5]:
        pack = rule.rule_pack
        payload.append(
            {
                "id": str(rule.id),
                "utility": round(float(rule.utility_score), 4),
                "match": round(float(rule.match_score), 4),
                "question_rules": _bounded_rule_list(pack.get("question_rules"), 3),
                "avoid_rules": _bounded_rule_list(pack.get("avoid_rules"), 3),
            }
        )
    return payload


def apply_reflective_rules_to_questions(
    questions: list[InquiryQuestion],
    trigger: TriggerContext,
    *,
    unknowns: set[str],
    rules: tuple[ReflectiveRetrievalRule, ...],
    score_boost: float = 0.12,
) -> list[InquiryQuestion]:
    if not questions or not rules:
        return questions
    anchors = question_anchors(trigger)
    out = list(questions)
    for rule in rules:
        pack = rule.rule_pack
        for raw in _bounded_rule_list(pack.get("question_rules"), 8):
            primitive = _primitive(raw.get("prefer_primitive") or raw.get("primitive"))
            if not primitive or not _condition_matches(raw, unknowns):
                continue
            template = str(raw.get("question_template") or "").strip()
            for index, question in enumerate(out):
                if question.primitive != primitive:
                    continue
                bonus = min(0.22, max(0.0, float(score_boost)) * rule.match_score)
                new_text = (
                    _render_question_template(template, anchors)
                    if template
                    else question.question
                )
                out[index] = replace(
                    question,
                    question=new_text,
                    expected_value=min(1.0, question.expected_value + bonus * 0.6),
                    score=round(question.score + bonus, 4),
                )
                break
        for raw in _bounded_rule_list(pack.get("avoid_rules"), 8):
            primitive = _primitive(raw.get("primitive") or raw.get("avoid_primitive"))
            if not primitive or not _condition_matches(raw, unknowns):
                continue
            for index, question in enumerate(out):
                if question.primitive != primitive:
                    continue
                penalty = min(0.28, 0.16 * max(0.25, rule.match_score))
                out[index] = replace(
                    question,
                    expected_value=max(0.0, question.expected_value - penalty * 0.7),
                    expected_cost=min(1.0, question.expected_cost + penalty * 0.25),
                    score=round(question.score - penalty, 4),
                )
    return sorted(out, key=lambda question: (-question.score, question.expected_cost))


def apply_reflective_rules_to_actions(
    question: InquiryQuestion,
    actions: list[RetrievalAction],
    *,
    rules: tuple[ReflectiveRetrievalRule, ...],
) -> list[RetrievalAction]:
    if not actions or not rules:
        return actions
    active_rules: list[ReflectiveRetrievalRule] = []
    prefer_paths: list[str] = []
    skip_paths: set[str] = set()
    semantic_terms: list[str] = []
    query_paths: set[str] = set()
    for rule in rules:
        for raw in _bounded_rule_list(rule.rule_pack.get("action_rules"), 8):
            primitive = _primitive(raw.get("primitive"))
            if primitive and primitive != question.primitive:
                continue
            active_rules.append(rule)
            prefer_paths.extend(_string_list(raw.get("prefer_paths"), limit=8))
            skip_paths.update(_string_list(raw.get("skip_paths"), limit=8))
            semantic_terms.extend(
                _string_list(
                    raw.get("semantic_terms") or raw.get("append_query_terms"),
                    limit=10,
                )
            )
            query_paths.update(_string_list(raw.get("query_paths"), limit=6))
    if not active_rules:
        return actions

    filtered = [action for action in actions if action.path not in skip_paths]
    if not filtered:
        filtered = actions
    rule_ids = sorted({str(rule.id) for rule in active_rules})
    max_match = max((rule.match_score for rule in active_rules), default=0.0)
    terms_suffix = " ".join(dict.fromkeys(semantic_terms))
    out: list[tuple[int, RetrievalAction]] = []
    original_index = {id(action): index for index, action in enumerate(actions)}
    for action in filtered:
        filters = dict(action.filters or {})
        filters["_reflective_rule_ids"] = rule_ids
        filters["_reflective_rule_match_score"] = round(float(max_match), 4)
        query = action.query
        should_extend_query = bool(terms_suffix) and (
            (not query_paths and action.path in {"semantic", "temporal"})
            or action.path in query_paths
        )
        if query and should_extend_query:
            query = f"{query} {terms_suffix}".strip()
        out.append(
            (
                original_index.get(id(action), len(actions)),
                replace(action, query=query, filters=filters),
            )
        )

    if prefer_paths:
        order = {path: index for index, path in enumerate(prefer_paths)}
        out.sort(
            key=lambda item: (
                order.get(item[1].path, len(order) + 1),
                item[0],
            )
        )
    return [action for _index, action in out]


def _bounded_rule_list(value: Any, limit: int) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    out: list[dict[str, Any]] = []
    for item in value[: max(0, int(limit))]:
        if isinstance(item, dict):
            out.append(item)
    return out


def _string_list(value: Any, *, limit: int) -> list[str]:
    if isinstance(value, str):
        raw_values = [value]
    elif isinstance(value, list):
        raw_values = value
    else:
        return []
    out: list[str] = []
    for raw in raw_values[: max(0, int(limit))]:
        clean = str(raw or "").strip()
        if clean:
            out.append(clean)
    return out


def _primitive(value: Any) -> str:
    return re.sub(r"[^A-Z_]+", "_", str(value or "").upper()).strip("_")


def _condition_matches(rule: dict[str, Any], unknowns: set[str]) -> bool:
    required_unknown = str(rule.get("when_unknown_contains") or "").casefold().strip()
    if required_unknown:
        return any(required_unknown in unknown.casefold() for unknown in unknowns)
    when = str(rule.get("when") or "always").casefold().strip()
    if when in {"", "always", "default"}:
        return True
    if when == "current_status_unknown":
        needles = ("status", "counterevidence", "critical path", "binding")
        return any(
            any(needle in unknown.casefold() for needle in needles)
            for unknown in unknowns
        )
    return True


def _render_question_template(template: str, anchors: Any) -> str:
    values = {
        "subject": anchors.subject or "this signal",
        "focus": anchors.focus or anchors.claim or "this signal",
        "claim": anchors.claim or anchors.focus or "this signal",
        "constraint": anchors.constraint or anchors.focus or "this signal",
    }
    try:
        rendered = template.format(**values)
    except (KeyError, ValueError):
        rendered = template
    rendered = " ".join(rendered.split())
    if rendered and rendered[-1] not in "?!.":
        rendered += "?"
    return truncate_text(rendered, 220)


__all__ = [
    "ReflectiveRetrievalRule",
    "apply_reflective_rules_to_actions",
    "apply_reflective_rules_to_questions",
    "load_reflective_retrieval_rules",
    "reflective_rules_note",
    "reflective_rules_prompt_payload",
    "reflective_signature_for",
    "reflective_signature_match_score",
]
