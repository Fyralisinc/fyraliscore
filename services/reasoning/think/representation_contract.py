"""Representation enrichment for Think diffs.

This module turns the Alpen lessons into deterministic substrate behavior:
repeated wording is bound to context before it can be compressed, every new
model carries coverage metadata, and repetitive source streams can still
produce compact source/pattern memory instead of becoming silent no-ops.
"""
from __future__ import annotations

import json
import os
import re
from collections.abc import Iterable
from collections import Counter, defaultdict
from datetime import datetime
from types import SimpleNamespace
from typing import Any
from uuid import UUID

from services.reasoning.retrieval.primary import TriggerContext

from .diff_schema import ClaimOp


_MAX_TAGS = 24
_SOURCE_DIGEST_MIN_ROWS = 8
_SOURCE_DIGEST_MAX_PER_DIFF = 3
_SOURCE_DIGEST_MAX_UNIQUE_RATIO = 0.45
_SOURCE_DIGEST_MIN_TOP_COUNT = 5
_SOURCE_DIGEST_EXCLUDED_SOURCES = frozenset({"internal:state_change"})
_CURIOSITY_MAX_PER_DIFF = 1
_CURIOSITY_MIN_OBSERVATIONS = 8
_CURIOSITY_LOW_VALUE_UNKNOWNS = frozenset({
    "",
    "unknown",
    "n/a",
    "none",
})

_UUID_RE = re.compile(
    r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-"
    r"[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
)
_PR_RE = re.compile(r"\b(?:PR|pull request)\s*#?(\d+)\b", re.I)
_ISSUE_RE = re.compile(r"\b([A-Z][A-Z0-9]{1,12}-\d+|issue\s*#?\d+)\b", re.I)
_REPO_RE = re.compile(r"\b([a-zA-Z0-9_.-]+/[a-zA-Z0-9_.-]+)\b")
_URL_RE = re.compile(r"https?://[^\s)]+", re.I)
_HEX_RE = re.compile(r"\b[0-9a-f]{7,40}\b", re.I)
_NUMBER_RE = re.compile(r"[$]?[0-9][0-9,]*([.][0-9]+)?")
_WS_RE = re.compile(r"\s+")


def enrich_raw_diff_representation(raw_diff: Any, trigger: TriggerContext, bundle: Any) -> Any:
    """Mutate ``raw_diff`` with representation metadata and source digests."""
    observation_index = _observation_index(bundle)
    _maybe_add_source_digest_claims(raw_diff, trigger, bundle)
    _maybe_add_curiosity_claims(raw_diff, trigger, bundle)

    enriched = 0
    for op in _iter_insert_ops(raw_diff):
        if _enrich_claim_insert(op, trigger, observation_index):
            enriched += 1

    if enriched:
        _append_trace(raw_diff, f"representation_contract enriched {enriched} claim insert(s)")
    return raw_diff


def contextual_frames_compatible(entry: dict[str, Any], row: dict[str, Any]) -> tuple[bool, dict[str, Any]]:
    """Return whether two claims can be contextually compressed together.

    A false result means they may have similar wording, but their bound
    work/object/source frame says they are different company facts.
    """
    left = _frame_from_entry(entry)
    right = _frame_from_row(row)
    details: dict[str, Any] = {}
    compared = False

    for key in ("object_refs", "work_item_refs", "repo_refs", "source_threads"):
        lvals = set(_string_list(left.get(key)))
        rvals = set(_string_list(right.get(key)))
        if not lvals or not rvals:
            continue
        compared = True
        overlap = lvals & rvals
        details[f"{key}_overlap"] = sorted(overlap)
        if not overlap:
            details["incompatible_key"] = key
            details["left"] = sorted(lvals)[:8]
            details["right"] = sorted(rvals)[:8]
            return False, details

    left_action = left.get("action")
    right_action = right.get("action")
    if left_action and right_action:
        compared = True
        details["action_match"] = left_action == right_action
        if left_action != right_action:
            return False, {**details, "incompatible_key": "action"}

    details["compared"] = compared
    return True, details


def _iter_insert_ops(raw_diff: Any) -> list[ClaimOp]:
    ops: list[ClaimOp] = []
    for attr in ("claim_ops", "new_predictions"):
        for op in getattr(raw_diff, attr, []) or []:
            if getattr(op, "op", None) == "insert" and isinstance(getattr(op, "entry", None), dict):
                ops.append(op)
    return ops


def _enrich_claim_insert(
    op: ClaimOp,
    trigger: TriggerContext,
    observation_index: dict[UUID, Any],
) -> bool:
    entry = dict(op.entry or {})
    prop = dict(entry.get("proposition") or {})
    if not prop:
        return False

    observations = _observations_for_entry(entry, trigger, observation_index)
    frame = _build_contextual_frame(entry, prop, trigger, observations)
    coverage_roles = _coverage_roles(entry, prop, frame)
    retrieval_tags = _retrieval_tags(entry, prop, frame, coverage_roles)

    prop["coverage_roles"] = _merge_strings(prop.get("coverage_roles"), coverage_roles)
    prop["retrieval_tags"] = _merge_strings(prop.get("retrieval_tags"), retrieval_tags)
    prop["contextual_frame"] = _merge_frame(prop.get("contextual_frame"), frame)
    prop["domain_tags"] = _merge_strings(prop.get("domain_tags"), _domain_tags_for_entry(entry, retrieval_tags))

    entry["proposition"] = prop
    entry["domain_tags"] = _merge_strings(entry.get("domain_tags"), prop.get("domain_tags"))
    if not isinstance(entry.get("falsifier"), dict):
        default_falsifier = _default_source_bound_falsifier(entry, prop, frame)
        if default_falsifier is not None:
            entry["falsifier"] = default_falsifier
    op.entry = entry
    return True


def _maybe_add_source_digest_claims(raw_diff: Any, trigger: TriggerContext, bundle: Any) -> None:
    if trigger.kind != "T1" or not _is_event_batch_trigger(trigger):
        return
    if not _source_digest_enabled():
        return

    observations = trigger_observations_for_representation(trigger, bundle)
    if not observations:
        return

    by_source: dict[str, list[Any]] = defaultdict(list)
    for obs in observations:
        source = str(getattr(obs, "source_channel", "") or "")
        if not source or source in _SOURCE_DIGEST_EXCLUDED_SOURCES:
            continue
        by_source[source].append(obs)

    covered_sources = _existing_recurrence_sources(raw_diff, by_source.keys())
    digests: list[ClaimOp] = []
    for source, rows in sorted(by_source.items(), key=lambda item: len(item[1]), reverse=True):
        if source in covered_sources:
            continue
        if len(rows) < _SOURCE_DIGEST_MIN_ROWS:
            continue
        summary = _source_digest_summary(source, rows)
        if summary is None:
            continue
        digests.append(_source_digest_claim(trigger, rows, summary))
        if len(digests) >= _SOURCE_DIGEST_MAX_PER_DIFF:
            break

    if not digests:
        return
    raw_diff.claim_ops = [*list(getattr(raw_diff, "claim_ops", []) or []), *digests]
    _append_trace(raw_diff, f"source_digest synthesized {len(digests)} recurrence claim(s)")


def _maybe_add_curiosity_claims(raw_diff: Any, trigger: TriggerContext, bundle: Any) -> None:
    if trigger.kind != "T1" or not _is_event_batch_trigger(trigger):
        return
    if not _curiosity_enabled():
        return
    if _existing_curiosity_claim(raw_diff):
        return

    packet = _inquiry_context_packet(bundle)
    if not packet:
        return

    observations = trigger_observations_for_representation(trigger, bundle)
    if len(observations) < _CURIOSITY_MIN_OBSERVATIONS:
        return

    unknowns = _important_unknowns(packet)
    if not unknowns:
        return

    candidates = _substrate_candidates_for_curiosity(bundle)
    claims = [_curiosity_claim(trigger, observations, packet, unknowns, candidates)]
    raw_diff.claim_ops = [*list(getattr(raw_diff, "claim_ops", []) or []), *claims[:_CURIOSITY_MAX_PER_DIFF]]
    _append_trace(raw_diff, f"curiosity synthesized {len(claims[:_CURIOSITY_MAX_PER_DIFF])} open-question claim(s)")


def _source_digest_enabled() -> bool:
    raw = os.environ.get("THINK_SOURCE_DIGEST_FALLBACK", "1")
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _curiosity_enabled() -> bool:
    raw = os.environ.get("THINK_CURIOSITY_FALLBACK", "1")
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _inquiry_context_packet(bundle: Any) -> dict[str, Any] | None:
    notes = getattr(bundle, "notes", None)
    if not isinstance(notes, dict):
        return None
    packet = notes.get("inquiry_context_packet")
    return packet if isinstance(packet, dict) else None


def _existing_curiosity_claim(raw_diff: Any) -> bool:
    curiosity_tags = {
        "curiosity",
        "coverage_curiosity",
        "open_question",
        "operating_question",
        "strategic_question",
        "unresolved_unknown",
        "success_driver",
    }
    for op in getattr(raw_diff, "claim_ops", []) or []:
        if getattr(op, "op", None) != "insert":
            continue
        entry = getattr(op, "entry", None) or {}
        prop = entry.get("proposition") if isinstance(entry, dict) else {}
        if not isinstance(prop, dict):
            continue
        tags = {
            _tagify(tag)
            for tag in _merge_strings(
                prop.get("coverage_roles"),
                prop.get("retrieval_tags"),
                prop.get("domain_tags"),
                entry.get("domain_tags"),
            )
        }
        if tags & curiosity_tags:
            return True
    return False


def _important_unknowns(packet: dict[str, Any]) -> list[str]:
    raw_unknowns: list[Any] = []
    for key in ("important_unknowns",):
        value = packet.get(key)
        if isinstance(value, list):
            raw_unknowns.extend(value)
    obligations = packet.get("answer_obligations")
    if isinstance(obligations, dict) and isinstance(obligations.get("missing_slots"), list):
        raw_unknowns.extend(obligations["missing_slots"])
    verdict = packet.get("sufficiency_verdict")
    if isinstance(verdict, dict) and isinstance(verdict.get("remaining_unknowns"), list):
        raw_unknowns.extend(verdict["remaining_unknowns"])

    out: list[str] = []
    seen: set[str] = set()
    for raw in raw_unknowns:
        text = _WS_RE.sub(" ", str(raw or "")).strip()
        tag = _tagify(text)
        if not tag or tag in _CURIOSITY_LOW_VALUE_UNKNOWNS or tag in seen:
            continue
        seen.add(tag)
        out.append(text)
        if len(out) >= 8:
            break
    return out


def _substrate_candidates_for_curiosity(bundle: Any) -> list[dict[str, Any]]:
    notes = getattr(bundle, "notes", None)
    if not isinstance(notes, dict):
        return []
    raw = notes.get("substrate_candidates")
    if not isinstance(raw, list):
        return []
    candidates = [candidate for candidate in raw if isinstance(candidate, dict)]
    return sorted(
        candidates,
        key=lambda candidate: (
            _candidate_kind_priority(str(candidate.get("kind") or "")),
            -float(candidate.get("confidence") or 0.0),
            -len(candidate.get("evidence_observation_ids") or []),
            str(candidate.get("label") or ""),
        ),
    )


def _candidate_kind_priority(kind: str) -> int:
    return {
        "customer": 0,
        "workstream": 1,
        "commitment": 2,
        "actor": 3,
        "actor_alias": 4,
        "system": 5,
        "vendor": 6,
        "pattern": 7,
    }.get(kind, 99)


def _curiosity_candidate_bindings(
    candidates: list[dict[str, Any]],
    *,
    unknowns: list[str],
    limit: int = 8,
) -> list[dict[str, Any]]:
    if not candidates:
        return []
    wanted = _wanted_candidate_kinds_for_unknowns(unknowns)
    selected: list[dict[str, Any]] = []
    seen: set[str] = set()
    for candidate in candidates:
        kind = str(candidate.get("kind") or "")
        if wanted and kind not in wanted and len(selected) < max(2, limit // 2):
            continue
        scope_ref = candidate.get("scope_ref")
        if not isinstance(scope_ref, dict):
            cid = candidate.get("id")
            if cid:
                scope_ref = {"type": f"candidate_{kind}", "id": str(cid)}
        if not isinstance(scope_ref, dict) or not scope_ref.get("id"):
            continue
        key = f"{scope_ref.get('type')}:{scope_ref.get('id')}"
        if key in seen:
            continue
        seen.add(key)
        selected.append(
            {
                "kind": kind,
                "label": str(candidate.get("label") or "")[:160],
                "scope_ref": {
                    "type": str(scope_ref.get("type") or f"candidate_{kind}"),
                    "id": str(scope_ref.get("id")),
                },
                "confidence": float(candidate.get("confidence") or 0.0),
                "status": str(candidate.get("status") or "proposed"),
            }
        )
        if len(selected) >= limit:
            break
    if selected or not wanted:
        return selected
    return _curiosity_candidate_bindings(candidates, unknowns=[], limit=limit)


def _wanted_candidate_kinds_for_unknowns(unknowns: list[str]) -> set[str]:
    tags = {_tagify(unknown) for unknown in unknowns}
    wanted: set[str] = set()
    if any("owner" in tag or "actor" in tag or "responsible" in tag for tag in tags):
        wanted.update({"actor", "actor_alias"})
    if any("customer" in tag or "account" in tag or "counterparty" in tag for tag in tags):
        wanted.add("customer")
    if any("commitment" in tag or "work" in tag or "critical_path" in tag for tag in tags):
        wanted.update({"commitment", "workstream"})
    if any("goal" in tag or "decision" in tag or "next_action" in tag for tag in tags):
        wanted.update({"workstream", "commitment"})
    if any("system" in tag or "vendor" in tag or "resource" in tag for tag in tags):
        wanted.update({"system", "vendor"})
    if any("pattern" in tag or "recurring" in tag for tag in tags):
        wanted.add("pattern")
    return wanted


def _curiosity_claim(
    trigger: TriggerContext,
    rows: list[Any],
    packet: dict[str, Any],
    unknowns: list[str],
    candidates: list[dict[str, Any]],
) -> ClaimOp:
    first = sorted(rows, key=lambda row: getattr(row, "occurred_at", datetime.max))[0]
    actors = _dedupe_uuid_values(getattr(row, "actor_id", None) for row in rows)
    entities = _scope_entities_from_observations(rows)
    source_channels = _merge_strings(getattr(row, "source_channel", None) for row in rows)
    question_items = _curiosity_questions(packet)
    question_texts = [item["question"] for item in question_items if item.get("question")]
    primitives = _merge_strings(item.get("primitive") for item in question_items)
    focus = _curiosity_focus(packet, source_channels)
    candidate_bindings = _curiosity_candidate_bindings(candidates, unknowns=unknowns)
    candidate_scope_entities = [
        binding["scope_ref"]
        for binding in candidate_bindings
        if isinstance(binding.get("scope_ref"), dict)
    ]
    unknown_phrase = _human_join([_unknown_to_question(unknown) for unknown in unknowns[:4]])
    hypothesis_text = (
        f"Open operating questions remain for {focus}: {unknown_phrase}."
        if unknown_phrase
        else f"Open operating questions remain for {focus}."
    )
    test_conditions = (
        "Resolve by finding authoritative evidence for the missing owner, affected "
        "goal or commitment, counterevidence, critical path status, and next action."
    )
    natural = (
        f"{hypothesis_text} These questions should stay durable because answering "
        "them can change prioritization, ownership, risk judgment, or the next best "
        "action for the company."
    )
    question_tags = _curiosity_tags(unknowns, primitives)
    if candidate_bindings:
        question_tags = _merge_strings(
            question_tags,
            "candidate_bound_curiosity",
            *[
                f"candidate_{binding['kind']}_question"
                for binding in candidate_bindings
                if binding.get("kind")
            ],
        )
    prop = {
        "kind": "belief",
        "claim_role": "hypothesis",
        "abstraction_level": "atomic",
        "time_mode": "current",
        "modality": "inferred",
        "polarity": "neutral",
        "hypothesis_text": hypothesis_text,
        "test_conditions": test_conditions,
        "open_questions": question_texts[:5],
        "important_unknowns": unknowns[:8],
        "question_primitives": primitives[:8],
        "candidate_bindings": candidate_bindings,
        "coverage_roles": [
            "curiosity",
            "epistemic",
            "intervention",
            "workstream",
            "temporal",
            "source",
            *([] if not candidate_bindings else ["entity"]),
        ],
        "retrieval_tags": question_tags,
        "domain_tags": question_tags,
        "contextual_frame": {
            "source_channels": source_channels[:8],
            "observation_ids": [
                str(getattr(row, "id"))
                for row in rows[:50]
                if getattr(row, "id", None)
            ],
            "question_primitives": primitives[:8],
            "important_unknowns": unknowns[:8],
            "candidate_scope_refs": candidate_scope_entities[:8],
        },
    }
    return ClaimOp(
        op="insert",
        entry={
            "born_from_event_id": getattr(first, "id"),
            "proposition": prop,
            "natural": natural,
            "confidence": 0.58,
            "scope_actors": actors[:8],
            "scope_entities": _merge_scope_entities(
                entities[:8],
                candidate_scope_entities[:8],
            ),
            "scope_temporal": {
                "valid_from": _iso(getattr(first, "occurred_at", None)),
                "valid_until": None,
            },
            "falsifier": {
                "kind": "observation_pattern",
                "pattern": (
                    "Authoritative evidence resolves or makes irrelevant the open "
                    f"operating questions for {focus}."
                ),
                "within_window": "P14D",
            },
            "supporting_event_ids": [
                getattr(row, "id") for row in rows[:24] if getattr(row, "id", None)
            ],
            "domain_tags": question_tags,
        },
    )


def _curiosity_focus(packet: dict[str, Any], source_channels: list[str]) -> str:
    summary = str(packet.get("signal_summary") or "").strip()
    if summary:
        compact = _WS_RE.sub(" ", summary)
        return compact[:160].rstrip()
    if source_channels:
        return "the recent " + ", ".join(source_channels[:3]) + " evidence window"
    return "the recent evidence window"


def _curiosity_questions(packet: dict[str, Any]) -> list[dict[str, str]]:
    raw = packet.get("question_path")
    if not isinstance(raw, list):
        return []
    items: list[dict[str, str]] = []
    seen: set[str] = set()
    for item in raw:
        if not isinstance(item, dict):
            continue
        question = _WS_RE.sub(" ", str(item.get("question") or "")).strip()
        if not question:
            continue
        key = question.casefold()
        if key in seen:
            continue
        seen.add(key)
        items.append(
            {
                "question": question[:240],
                "primitive": str(item.get("primitive") or "").upper(),
            }
        )
        if len(items) >= 5:
            break
    return items


def _unknown_to_question(unknown: str) -> str:
    tag = _tagify(unknown)
    mapping = {
        "affected_commitment": "which commitment is affected",
        "affected_goal": "which company goal is affected",
        "responsible_owner": "who owns the next action",
        "counterevidence": "what would disconfirm the current interpretation",
        "blocking_constraint": "which constraint is actually blocking progress",
        "whether_the_blocker_is_on_the_critical_path": "whether this is on the critical path",
        "whether_this_is_part_of_a_broader_recurring_pattern": "whether this is part of a broader recurring pattern",
    }
    return mapping.get(tag, unknown)


def _curiosity_tags(unknowns: list[str], primitives: list[str]) -> list[str]:
    tags = [
        "open_question",
        "operating_question",
        "strategic_question",
        "executive_question",
        "manager_question",
        "operator_question",
        "unresolved_unknown",
        "success_driver",
        "coverage_curiosity",
        "coverage_epistemic",
        "coverage_intervention",
        "question_policy",
    ]
    for primitive in primitives:
        if primitive:
            tags.append(f"question_{_tagify(primitive)}")
    for unknown in unknowns:
        tag = _tagify(unknown)
        if tag:
            tags.append(f"unknown_{tag}")
        if tag in {"affected_goal", "affected_commitment", "counterevidence"}:
            tags.append("executive_question")
        if tag in {"responsible_owner", "blocking_constraint"}:
            tags.append("manager_question")
        if tag in {
            "whether_the_blocker_is_on_the_critical_path",
            "whether_this_is_part_of_a_broader_recurring_pattern",
        }:
            tags.append("operator_question")
    return _merge_strings(tags)


def _human_join(items: list[str]) -> str:
    cleaned = [str(item).strip() for item in items if str(item).strip()]
    if not cleaned:
        return ""
    if len(cleaned) == 1:
        return cleaned[0]
    if len(cleaned) == 2:
        return f"{cleaned[0]} and {cleaned[1]}"
    return ", ".join(cleaned[:-1]) + f", and {cleaned[-1]}"


def _existing_recurrence_sources(
    raw_diff: Any,
    candidate_sources: Iterable[str],
) -> set[str]:
    covered: set[str] = set()
    source_terms = {
        source: {source.casefold(), _tagify(source)}
        for source in candidate_sources
        if source
    }
    for op in getattr(raw_diff, "claim_ops", []) or []:
        if getattr(op, "op", None) != "insert":
            continue
        entry = getattr(op, "entry", None) or {}
        prop = entry.get("proposition") if isinstance(entry, dict) else {}
        if not isinstance(prop, dict):
            continue
        tags = set(
            _merge_strings(
                prop.get("coverage_roles"),
                prop.get("retrieval_tags"),
                prop.get("domain_tags"),
                entry.get("domain_tags"),
            )
        )
        has_recurrence = (
            prop.get("claim_role") == "pattern"
            or prop.get("abstraction_level") == "pattern"
            or bool(tags & {"discovered_pattern", "contextual_recurrence", "source_digest"})
        )
        if not has_recurrence:
            continue
        text = _entry_text(entry, prop).casefold()
        text_tag = _tagify(text)
        for source, terms in source_terms.items():
            if any(term and (term in text or term in text_tag) for term in terms):
                covered.add(source)
    return covered


def _is_event_batch_trigger(trigger: TriggerContext) -> bool:
    return bool(
        trigger.is_batch
        or getattr(trigger, "subkind", None) == "event_batch"
        or (trigger.seed_signature or {}).get("signal_type") == "event_batch"
        or (trigger.seed_signature or {}).get("batch") is True
    )


def _source_digest_summary(source: str, rows: list[Any]) -> dict[str, Any] | None:
    signatures = Counter(_normalize_observation_text(getattr(row, "content_text", "")) for row in rows)
    signatures.pop("", None)
    total = len(rows)
    if not signatures:
        return {
            "source": source,
            "total": total,
            "distinct": 0,
            "unique_ratio": 0.0,
            "top_signature": f"{source} emitted {total} source signals with empty text payloads",
            "top_count": total,
            "repetition_mode": "source_cadence",
            "sample_signatures": [],
        }
    distinct = len(signatures)
    top_sig, top_count = signatures.most_common(1)[0]
    unique_ratio = distinct / max(1, total)
    is_repetitive = (
        unique_ratio <= _SOURCE_DIGEST_MAX_UNIQUE_RATIO
        or top_count >= _SOURCE_DIGEST_MIN_TOP_COUNT
    )
    return {
        "source": source,
        "total": total,
        "distinct": distinct,
        "unique_ratio": unique_ratio,
        "top_signature": top_sig,
        "top_count": top_count,
        "repetition_mode": "normalized_repetition" if is_repetitive else "source_cadence",
        "sample_signatures": [sig for sig, _ in signatures.most_common(4)],
    }


def _source_digest_claim(trigger: TriggerContext, rows: list[Any], summary: dict[str, Any]) -> ClaimOp:
    first = sorted(rows, key=lambda row: getattr(row, "occurred_at", datetime.max))[0]
    actors = _dedupe_uuid_values(getattr(row, "actor_id", None) for row in rows)
    entities = _scope_entities_from_observations(rows)
    source = str(summary["source"])
    repetition_mode = str(summary.get("repetition_mode") or "source_cadence")
    if repetition_mode == "normalized_repetition":
        tendency = (
            f"{summary['top_count']} of {summary['total']} recent observations match "
            f"'{str(summary['top_signature'])[:120]}'."
        )
    else:
        sample_bits = [
            str(value)[:90]
            for value in (summary.get("sample_signatures") or [])
            if str(value).strip()
        ][:3]
        sample_text = "; ".join(sample_bits) if sample_bits else str(summary["top_signature"])[:120]
        tendency = (
            f"{summary['total']} recent observations from this source form a major "
            f"source window across {summary['distinct']} normalized shapes"
            f"{': ' + sample_text if sample_text else ''}."
        )
    natural = (
        f"The {source} source is showing a {repetition_mode.replace('_', ' ')}: "
        f"{tendency} This should be represented as a compact source-pattern "
        "baseline, not left as independent low-level events."
    )
    source_tags = [
        "source_digest",
        "discovered_pattern",
        "major_source_window",
        "coverage_source",
        repetition_mode,
        *_source_family_tags(source),
        *_source_activity_tags(source, rows),
    ]
    if repetition_mode == "normalized_repetition":
        source_tags.append("contextual_recurrence")
    prop = {
        "kind": "belief",
        "claim_role": "pattern",
        "abstraction_level": "pattern",
        "time_mode": "recurring",
        "modality": "observed",
        "polarity": "neutral",
        "signature": f"{source} recurring source pattern",
        "observed_tendency": tendency,
        "trigger_conditions": (
            f"T1 event batch from {source} with {summary['total']} observations, "
            f"{summary['distinct']} normalized shapes, and normalized unique ratio "
            f"{summary['unique_ratio']:.2f}."
        ),
        "repetition_mode": repetition_mode,
        "coverage_roles": [
            "source",
            "workstream",
            "temporal",
            "discovered_pattern",
            "contextual_recurrence",
            "epistemic",
            "intervention",
        ],
        "domain_tags": _merge_strings(source_tags),
        "contextual_frame": {
            "source_channels": [source],
            "observation_ids": [
                str(getattr(row, "id"))
                for row in rows[:50]
                if getattr(row, "id", None)
            ],
        },
    }
    return ClaimOp(
        op="insert",
        entry={
            "born_from_event_id": getattr(first, "id"),
            "proposition": prop,
            "natural": natural,
            "confidence": 0.66,
            "scope_actors": actors[:8],
            "scope_entities": entities[:8],
            "scope_temporal": {
                "valid_from": _iso(getattr(first, "occurred_at", None)),
                "valid_until": None,
            },
            "falsifier": {
                "kind": "observation_pattern",
                "pattern": (
                    f"The {source} stream no longer contributes a major recurring "
                    "source window or repeated operational signal shape."
                ),
                "within_window": "P7D",
            },
            "supporting_event_ids": [getattr(row, "id") for row in rows[:50] if getattr(row, "id", None)],
            "domain_tags": prop["domain_tags"],
        },
    )


def _coverage_roles(entry: dict[str, Any], prop: dict[str, Any], frame: dict[str, Any]) -> list[str]:
    roles: list[str] = []
    claim_role = str(prop.get("claim_role") or "")
    abstraction = str(prop.get("abstraction_level") or "")
    time_mode = str(prop.get("time_mode") or "")

    if entry.get("scope_actors") or entry.get("scope_entities") or frame.get("entity_refs"):
        roles.append("entity")
    if _has_workstream_signal(entry, prop, frame):
        roles.append("workstream")
    if claim_role in {"fact", "concern", "capability", "recommendation"}:
        roles.append("state")
    if claim_role == "relation" or abstraction == "relationship":
        roles.append("relationship")
    if time_mode in {"future", "recurring"} or frame.get("time_window"):
        roles.append("temporal")
    if frame.get("source_channels"):
        roles.append("source")
    if claim_role in {"pattern", "situation"} or abstraction in {"pattern", "composite"}:
        roles.append("discovered_pattern")
    if _is_contextual_recurrence(entry, prop):
        roles.append("contextual_recurrence")
    if claim_role in {"concern", "hypothesis", "prediction"} or entry.get("falsifier"):
        roles.append("epistemic")
    if claim_role == "recommendation" or _contains_any(_entry_text(entry, prop), ("block", "risk", "should", "need")):
        roles.append("intervention")
    tags = {
        _tagify(tag)
        for tag in _merge_strings(prop.get("domain_tags"), entry.get("domain_tags"), prop.get("retrieval_tags"))
    }
    if (
        "open_question" in tags
        or "unresolved_unknown" in tags
        or "coverage_curiosity" in tags
        or (
            claim_role == "hypothesis"
            and _contains_any(_entry_text(entry, prop), ("question", "unknown", "resolve", "whether", "which", "what"))
        )
    ):
        roles.append("curiosity")
    return _merge_strings(roles)


def _retrieval_tags(
    entry: dict[str, Any],
    prop: dict[str, Any],
    frame: dict[str, Any],
    coverage_roles: list[str],
) -> list[str]:
    text = _entry_text(entry, prop).casefold()
    tags: list[str] = [f"coverage_{role}" for role in coverage_roles]
    tags.extend(_string_list(prop.get("domain_tags")))
    tags.extend(_string_list(entry.get("domain_tags")))
    claim_role = str(prop.get("claim_role") or "")
    if claim_role:
        tags.append(f"role_{_tagify(claim_role)}")
    tags.extend(_source_family_tags(",".join(_string_list(frame.get("source_channels")))))

    keyword_tags = (
        ("progress_signal", ("started", "picked up", "raised", "opened", "merged", "shipped", "completed")),
        ("review_loop", ("review", "feedback", "comment", "approval", "approved")),
        ("delivery_risk", ("risk", "blocked", "blocker", "stalled", "slip", "delay", "missing")),
        ("coordination_debt", ("handoff", "waiting", "unclear", "owner", "follow up", "follow-up")),
        ("deployment_activity", ("deploy", "release", "rollback", "staging", "production")),
        ("finance_flow", ("invoice", "bill", "payment", "vendor", "runway", "budget", "transaction")),
        ("operational_churn", ("alert", "latency", "error", "aws", "lambda", "incident", "disk", "5xx")),
        ("decision_pressure", ("decision", "revisited", "approved", "rejected", "exception")),
        ("contextual_recurrence", ("repeat", "recurring", "cadence", "again", "same pattern")),
        ("open_question", ("question", "unknown", "resolve", "whether", "which", "what", "who owns")),
        ("success_driver", ("priority", "success", "goal", "commitment", "next action", "risk judgment")),
    )
    for tag, needles in keyword_tags:
        if _contains_any(text, needles):
            tags.append(tag)
    if frame.get("object_refs"):
        tags.append("object_bound")
    if frame.get("work_item_refs"):
        tags.append("work_item_bound")
    if frame.get("repo_refs"):
        tags.append("repo_bound")
    return _merge_strings(tags)


def _domain_tags_for_entry(entry: dict[str, Any], retrieval_tags: list[str]) -> list[str]:
    existing = []
    prop = entry.get("proposition")
    if isinstance(prop, dict):
        existing.extend(_string_list(prop.get("domain_tags")))
    existing.extend(_string_list(entry.get("domain_tags")))
    structural = [
        tag
        for tag in retrieval_tags
        if tag.startswith(("coverage_", "role_"))
        or tag
        in {
            "source_digest",
            "contextual_recurrence",
            "discovered_pattern",
            "progress_signal",
            "delivery_risk",
            "coordination_debt",
            "finance_flow",
            "operational_churn",
            "review_loop",
            "decision_pressure",
            "open_question",
            "operating_question",
            "strategic_question",
            "executive_question",
            "manager_question",
            "operator_question",
            "unresolved_unknown",
            "success_driver",
            "question_policy",
        }
        or tag.startswith(("question_", "unknown_"))
    ]
    return _merge_strings(existing, structural)


def _build_contextual_frame(
    entry: dict[str, Any],
    prop: dict[str, Any],
    trigger: TriggerContext,
    observations: list[Any],
) -> dict[str, Any]:
    text = _entry_text(entry, prop)
    actor_ids = _dedupe_uuid_values([
        *list(entry.get("scope_actors") or []),
        *list(trigger.scope_actors or []),
    ])
    actor_ids.extend(
        uid for uid in _dedupe_uuid_values(getattr(obs, "actor_id", None) for obs in observations) if uid not in actor_ids
    )

    source_channels = _merge_strings(getattr(obs, "source_channel", None) for obs in observations)
    source_actor_refs = _merge_strings(getattr(obs, "source_actor_ref", None) for obs in observations)
    entity_refs = _merge_entity_refs(entry.get("scope_entities"), *(getattr(obs, "entities_mentioned", None) for obs in observations))
    object_refs = _extract_refs("pr", _PR_RE, text)
    work_item_refs = _extract_refs("work_item", _ISSUE_RE, text)
    repo_refs = _extract_refs("repo", _REPO_RE, text)
    urls = _extract_refs("url", _URL_RE, text)
    source_threads = _merge_strings(_source_thread_refs(obs) for obs in observations)

    times = [getattr(obs, "occurred_at", None) for obs in observations if getattr(obs, "occurred_at", None) is not None]
    frame: dict[str, Any] = {
        "actor_ids": [str(uid) for uid in actor_ids[:12]],
        "source_channels": source_channels[:8],
        "source_actor_refs": source_actor_refs[:8],
        "entity_refs": entity_refs[:12],
        "object_refs": object_refs[:12],
        "work_item_refs": work_item_refs[:12],
        "repo_refs": repo_refs[:8],
        "source_threads": source_threads[:8],
        "action": _infer_action(text),
        "observation_ids": [str(getattr(obs, "id")) for obs in observations[:50] if getattr(obs, "id", None)],
    }
    if urls:
        frame["url_refs"] = urls[:8]
    if times:
        frame["time_window"] = {
            "from": _iso(min(times)),
            "to": _iso(max(times)),
        }
    return {key: value for key, value in frame.items() if value not in (None, [], {}, "")}


def _default_source_bound_falsifier(
    entry: dict[str, Any],
    prop: dict[str, Any],
    frame: dict[str, Any],
) -> dict[str, str] | None:
    claim_role = str(prop.get("claim_role") or "")
    if claim_role in {"prediction", "recommendation"}:
        return None
    source_hint = ", ".join(_string_list(frame.get("source_channels"))[:3])
    text = _entry_text(entry, prop)
    if _too_speculative_for_default_falsifier(text):
        return None
    compact = _WS_RE.sub(" ", text).strip()
    if len(compact) > 180:
        compact = compact[:177].rstrip() + "..."
    if not compact:
        compact = str(prop.get("signature") or prop.get("subject") or "the claim")
    source_clause = f" from {source_hint}" if source_hint else ""
    return {
        "kind": "observation_pattern",
        "pattern": (
            f"authoritative observation{source_clause} contradicts or withdraws: "
            f"{compact}"
        ),
        "within_window": "P30D",
    }


def _too_speculative_for_default_falsifier(text: str) -> bool:
    lowered = f" {str(text or '').casefold()} "
    absolute_markers = (
        " definitely ",
        " forever ",
        " everything ",
        " always ",
        " never ",
        " guaranteed ",
        " certainly ",
    )
    future_markers = (
        " will ",
        " would ",
        " should ",
        " must ",
        " going to ",
    )
    return _contains_any(lowered, absolute_markers) or _contains_any(
        lowered,
        future_markers,
    )


def _frame_from_entry(entry: dict[str, Any]) -> dict[str, Any]:
    prop = entry.get("proposition") if isinstance(entry, dict) else None
    if isinstance(prop, dict) and isinstance(prop.get("contextual_frame"), dict):
        return dict(prop["contextual_frame"])
    return _build_contextual_frame(entry, prop if isinstance(prop, dict) else {}, _empty_trigger(), [])


def _frame_from_row(row: dict[str, Any]) -> dict[str, Any]:
    prop = row.get("proposition") if isinstance(row, dict) else None
    if isinstance(prop, (bytes, bytearray)):
        prop = prop.decode()
    if isinstance(prop, str):
        try:
            prop = json.loads(prop)
        except json.JSONDecodeError:
            prop = {}
    if isinstance(prop, dict) and isinstance(prop.get("contextual_frame"), dict):
        return dict(prop["contextual_frame"])
    entry = {
        "natural": row.get("natural") or "",
        "proposition": prop if isinstance(prop, dict) else {},
        "scope_actors": row.get("scope_actors") or [],
        "scope_entities": row.get("scope_entities") or [],
    }
    return _frame_from_entry(entry)


def _empty_trigger() -> TriggerContext:
    return TriggerContext(kind="T1", tenant_id=UUID("00000000-0000-0000-0000-000000000000"))


def _observation_index(bundle: Any) -> dict[UUID, Any]:
    out: dict[UUID, Any] = {}
    for obs in getattr(bundle, "observations", []) or []:
        oid = getattr(obs, "id", None)
        if isinstance(oid, UUID):
            out[oid] = obs
    return out


def _observations_for_entry(
    entry: dict[str, Any],
    trigger: TriggerContext,
    observation_index: dict[UUID, Any],
) -> list[Any]:
    ids = _dedupe_uuid_values(
        [
            entry.get("born_from_event_id"),
            *list(entry.get("supporting_event_ids") or []),
            *list(trigger.observation_ids or []),
            trigger.observation_id,
        ]
    )
    return [observation_index[uid] for uid in ids if uid in observation_index]


def trigger_observations_for_representation(trigger: TriggerContext, bundle: Any) -> list[Any]:
    observations = list(getattr(bundle, "observations", []) or [])
    fragment_observations = _batch_fragment_observations(trigger)
    if fragment_observations:
        by_id = {
            getattr(obs, "id", None): obs
            for obs in observations
            if isinstance(getattr(obs, "id", None), UUID)
        }
        return [by_id.get(getattr(obs, "id", None), obs) for obs in fragment_observations]
    trigger_ids = set(_dedupe_uuid_values([trigger.observation_id, *trigger.observation_ids]))
    if trigger_ids:
        scoped = [obs for obs in observations if getattr(obs, "id", None) in trigger_ids]
        primary_only_event_batch = (
            _is_event_batch_trigger(trigger)
            and not list(trigger.observation_ids or [])
            and len(scoped) <= 1
        )
        if scoped and not primary_only_event_batch:
            return scoped
    return observations


def _batch_fragment_observations(trigger: TriggerContext) -> list[Any]:
    signature = trigger.seed_signature if isinstance(trigger.seed_signature, dict) else {}
    fragments = signature.get("batch_signal_fragments")
    if not isinstance(fragments, list):
        return []
    observations: list[Any] = []
    for fragment in fragments:
        if not isinstance(fragment, dict):
            continue
        oid = _coerce_uuid(fragment.get("observation_id"))
        if oid is None:
            continue
        observations.append(
            SimpleNamespace(
                id=oid,
                source_channel=str(fragment.get("source_channel") or "unknown"),
                source_actor_ref=None,
                actor_id=None,
                content_text=str(fragment.get("text") or ""),
                content={"batch_fragment": True},
                entities_mentioned=[],
                occurred_at=_coerce_datetime(fragment.get("occurred_at")),
            )
        )
    return observations


def _coerce_uuid(value: Any) -> UUID | None:
    if isinstance(value, UUID):
        return value
    try:
        return UUID(str(value))
    except (TypeError, ValueError):
        return None


def _coerce_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return datetime.utcnow()
    return datetime.utcnow()


def _scope_entities_from_observations(rows: list[Any]) -> list[dict[str, str]]:
    return _merge_entity_refs(*(getattr(row, "entities_mentioned", None) for row in rows))


def _merge_entity_refs(*groups: Any) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for group in groups:
        values = _json_list(group)
        for item in values:
            if not isinstance(item, dict):
                continue
            typ = item.get("type")
            ident = item.get("id")
            if not typ or not ident or not _UUID_RE.match(str(ident)):
                continue
            key = (str(typ), str(ident))
            if key in seen:
                continue
            seen.add(key)
            out.append({"type": str(typ), "id": str(ident)})
    return out


def _merge_scope_entities(*groups: Any) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    seen: set[tuple[str, str]] = set()
    for group in groups:
        values = group if isinstance(group, list) else []
        for item in values:
            if not isinstance(item, dict):
                continue
            typ = str(item.get("type") or "").strip()
            ident = str(item.get("id") or "").strip()
            if not typ or not ident:
                continue
            key = (typ, ident)
            if key in seen:
                continue
            seen.add(key)
            out.append({"type": typ, "id": ident})
            if len(out) >= 12:
                return out
    return out


def _source_thread_refs(obs: Any) -> list[str]:
    content = getattr(obs, "content", None)
    if isinstance(content, (bytes, bytearray)):
        try:
            content = json.loads(content.decode())
        except (json.JSONDecodeError, UnicodeDecodeError):
            content = {}
    if isinstance(content, str):
        try:
            content = json.loads(content)
        except json.JSONDecodeError:
            content = {}
    if not isinstance(content, dict):
        return []
    refs: list[str] = []
    for key in ("thread_id", "conversation_id", "channel_id", "channel", "room", "topic", "file_id", "issue_id", "pull_request_id"):
        raw = content.get(key)
        if raw is not None and str(raw).strip():
            refs.append(f"{key}:{str(raw).strip()}")
    return refs


def _extract_refs(kind: str, pattern: re.Pattern[str], text: str) -> list[str]:
    refs: list[str] = []
    for match in pattern.finditer(text or ""):
        value = match.group(1) if match.groups() else match.group(0)
        refs.append(f"{kind}:{value.strip().casefold()}")
    return _merge_strings(refs)


def _infer_action(text: str) -> str | None:
    lowered = (text or "").casefold()
    action_patterns = (
        ("raise_pr", ("raised a pr", "raised pr", "opened pr", "open pr", "pull request")),
        ("merge_pr", ("merged", "merge")),
        ("review", ("review", "approved", "comment")),
        ("deploy", ("deploy", "release", "rollback")),
        ("block", ("blocked", "blocker", "stalled")),
        ("pay", ("payment", "invoice", "bill", "transaction")),
        ("alert", ("alert", "incident", "error", "latency")),
    )
    for action, needles in action_patterns:
        if _contains_any(lowered, needles):
            return action
    return None


def _has_workstream_signal(entry: dict[str, Any], prop: dict[str, Any], frame: dict[str, Any]) -> bool:
    if frame.get("work_item_refs") or frame.get("object_refs") or frame.get("repo_refs"):
        return True
    entities = entry.get("scope_entities") or []
    if any(isinstance(ent, dict) and ent.get("type") in {"commitment", "goal", "decision"} for ent in entities):
        return True
    return _contains_any(_entry_text(entry, prop).casefold(), ("project", "issue", "ticket", "pr", "sprint", "launch"))


def _is_contextual_recurrence(entry: dict[str, Any], prop: dict[str, Any]) -> bool:
    if prop.get("claim_role") == "pattern":
        return True
    tags = _string_list(prop.get("domain_tags")) + _string_list(entry.get("domain_tags"))
    if "contextual_recurrence" in tags or "source_digest" in tags:
        return True
    return _contains_any(_entry_text(entry, prop).casefold(), ("again", "recurring", "repeated", "cadence"))


def _entry_text(entry: dict[str, Any], prop: dict[str, Any] | None = None) -> str:
    prop = prop if isinstance(prop, dict) else entry.get("proposition")
    parts = [str(entry.get("natural") or "")]
    if isinstance(prop, dict):
        for key in (
            "event",
            "assertion",
            "summary",
            "claim",
            "nature",
            "assessment",
            "hypothesis_text",
            "observed_tendency",
            "situation",
            "relationship_summary",
            "expected",
            "signature",
            "trigger_conditions",
        ):
            value = prop.get(key)
            if isinstance(value, str):
                parts.append(value)
    return " ".join(part for part in parts if part)


def _normalize_observation_text(text: str) -> str:
    value = str(text or "").casefold()
    value = _URL_RE.sub("<url>", value)
    value = _HEX_RE.sub("<hex>", value)
    value = _NUMBER_RE.sub("<num>", value)
    value = _WS_RE.sub(" ", value)
    return value.strip()


def _source_family_tags(source_text: str) -> list[str]:
    source = (source_text or "").casefold()
    tags: list[str] = []
    families = (
        ("source_code", ("github", "gitlab", "jira")),
        ("source_chat", ("slack", "telegram", "discord", "signal")),
        ("source_docs", ("notion", "drive", "gmail", "calendar", "fireflies", "miro", "figma")),
        ("source_finance", ("quickbooks", "ramp", "brex", "mercury", "deel", "carta", "gusto")),
        ("source_observability", ("aws", "grafana", "cloudwatch")),
        ("source_people", ("ashby", "hibob", "linkedin")),
    )
    for tag, needles in families:
        if _contains_any(source, needles):
            tags.append(tag)
    return tags


def _source_activity_tags(source: str, rows: list[Any]) -> list[str]:
    text = " ".join(str(getattr(row, "content_text", "") or "") for row in rows).casefold()
    tags: list[str] = []
    activity_tags = (
        ("access_control_activity", ("iam", "access key", "permission", "auth", "login")),
        ("compute_activity", ("lambda", "ec2", "ecs", "compute", "container")),
        ("repo_activity", ("pull request", "pr #", "commit", "merged", "review")),
        ("issue_activity", ("jira", "issue", "ticket", "sprint")),
        ("calendar_activity", ("meeting", "calendar", "1:1", "attendee")),
        ("document_activity", ("document", "pdf", "notion", "drive", "modified")),
        ("finance_activity", ("payment", "invoice", "transaction", "bill", "vendor")),
        ("alert_activity", ("alert", "probe", "error", "latency", "incident")),
        ("people_activity", ("candidate", "hiring", "interview", "employee")),
    )
    for tag, needles in activity_tags:
        if _contains_any(text, needles):
            tags.append(tag)
    tags.extend(_source_family_tags(source))
    return _merge_strings(tags)


def _contains_any(text: str, needles: tuple[str, ...]) -> bool:
    return any(needle in text for needle in needles)


def _tagify(value: str) -> str:
    return re.sub(r"[^a-z0-9_]+", "_", value.strip().casefold()).strip("_")


def _merge_frame(existing: Any, frame: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(existing, dict):
        return frame
    merged = dict(existing)
    for key, value in frame.items():
        if isinstance(value, list):
            merged[key] = _merge_strings(merged.get(key), value)
        elif isinstance(value, dict):
            current = merged.get(key)
            merged[key] = {**current, **value} if isinstance(current, dict) else value
        elif value not in (None, ""):
            merged.setdefault(key, value)
    return {key: value for key, value in merged.items() if value not in (None, [], {}, "")}


def _merge_strings(*groups: Any) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for group in groups:
        for raw in _string_list(group):
            tag = _tagify(raw)
            if not tag or tag in seen:
                continue
            seen.add(tag)
            out.append(tag)
            if len(out) >= _MAX_TAGS:
                return out
    return out


def _string_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [value]
    if isinstance(value, Iterable) and not isinstance(value, (bytes, bytearray, dict)):
        out: list[str] = []
        for item in value:
            if isinstance(item, (list, tuple, set)):
                out.extend(_string_list(item))
            elif item is not None and str(item).strip():
                out.append(str(item))
        return out
    return []


def _json_list(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, (bytes, bytearray)):
        value = value.decode()
    if isinstance(value, str):
        try:
            decoded = json.loads(value)
        except json.JSONDecodeError:
            return []
        return decoded if isinstance(decoded, list) else []
    return []


def _dedupe_uuid_values(values: Any) -> list[UUID]:
    out: list[UUID] = []
    seen: set[UUID] = set()
    if isinstance(values, Iterable) and not isinstance(values, (str, bytes, bytearray, dict)):
        iterable = values
    else:
        iterable = [values]
    for value in iterable:
        if value is None:
            continue
        try:
            uid = value if isinstance(value, UUID) else UUID(str(value))
        except (TypeError, ValueError):
            continue
        if uid not in seen:
            seen.add(uid)
            out.append(uid)
    return out


def _iso(value: Any) -> str:
    if isinstance(value, datetime):
        return value.isoformat()
    return datetime.utcnow().isoformat()


def _append_trace(raw_diff: Any, note: str) -> None:
    trace = getattr(raw_diff, "reasoning_trace", "") or ""
    raw_diff.reasoning_trace = f"{trace}\n{note}".strip() if trace else note


__all__ = [
    "contextual_frames_compatible",
    "enrich_raw_diff_representation",
    "trigger_observations_for_representation",
]
