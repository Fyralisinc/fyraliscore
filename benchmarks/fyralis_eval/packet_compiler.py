"""Context packet compiler for benchmark retrieval outputs."""

from __future__ import annotations

import re
from typing import Any

from benchmarks.adapters.base import BenchmarkQuery
from benchmarks.fyralis_eval.reader import RetrievedEvidence, RetrievalOutput, tokenize


class ContextPacketCompiler:
    """Build compact, serializable context packets from retrieved evidence."""

    def __init__(
        self,
        *,
        max_chars_per_evidence: int = 1800,
        max_snippets_per_evidence: int = 2,
    ) -> None:
        self.max_chars_per_evidence = max_chars_per_evidence
        self.max_snippets_per_evidence = max_snippets_per_evidence

    def compile(
        self,
        query: BenchmarkQuery,
        evidence: list[RetrievedEvidence],
        *,
        latency_ms: int,
        retrieval_calls: int,
    ) -> RetrievalOutput:
        packet_id = f"packet_{query.query_id}"
        evidence_cards = [
            {
                "observation_id": item.observation_id,
                "content": _append_relevant_structured_ui_facts(
                    query.query_text,
                    _focused_content(
                        query.query_text,
                        item.content,
                        max_chars=self.max_chars_per_evidence,
                        max_snippets=self.max_snippets_per_evidence,
                    ),
                    item.metadata,
                    max_chars=self.max_chars_per_evidence,
                ),
                "score": item.score,
                "occurred_at": item.occurred_at,
                "metadata": item.metadata,
            }
            for item in evidence
        ]
        evidence_cards = [
            *_derived_context_evidence_cards(query, evidence_cards),
            *evidence_cards,
        ]
        answer_requirements = _answer_requirements(query)
        sufficiency = _packet_sufficiency(query, evidence_cards, answer_requirements)
        token_estimate = _estimate_tokens(query.query_text, evidence_cards)
        context_packet = {
            "packet_id": packet_id,
            "query": query.to_json(),
            "evidence": evidence_cards,
            "answer_requirements": answer_requirements,
            "sufficiency": sufficiency,
            "budget": {"estimated_tokens_used": token_estimate},
        }
        passthrough_answer = _passthrough_answer_metadata(evidence)
        if passthrough_answer is not None:
            context_packet["passthrough_answer"] = passthrough_answer
        omission_ledger = []
        if not evidence:
            omission_ledger.append({
                "reason": "no_retrieved_evidence",
                "severity": "info",
            })
        if query.metadata.get("requires_external_tool_surface") and not _has_tool_surface_result(
            evidence_cards
        ):
            omission_ledger.append({
                "reason": "external_tool_surface_not_materialized",
                "severity": "warning",
                "required_tool_surfaces": query.metadata.get(
                    "required_tool_surfaces",
                    [],
                ),
            })
        if sufficiency["required_roles"] and sufficiency["missing_roles"]:
            omission_ledger.append({
                "reason": "incomplete_composition_role_coverage",
                "severity": "warning",
                "required_roles": sufficiency["required_roles"],
                "covered_roles": sufficiency["covered_roles"],
                "missing_roles": sufficiency["missing_roles"],
            })
        if sufficiency["requires_finality"] and not sufficiency["has_finality_evidence"]:
            omission_ledger.append({
                "reason": "missing_finality_evidence",
                "severity": "warning",
            })
        return RetrievalOutput(
            query_id=query.query_id,
            packet_id=packet_id,
            retrieved_nodes=[],
            retrieved_evidence=evidence,
            context_packet=context_packet,
            omission_ledger=omission_ledger,
            token_estimate=token_estimate,
            latency_ms=latency_ms,
            retrieval_calls=retrieval_calls,
        )


def _passthrough_answer_metadata(
    evidence: list[RetrievedEvidence],
) -> dict[str, Any] | None:
    for item in evidence:
        metadata = item.metadata if isinstance(item.metadata, dict) else {}
        answer = metadata.get("passthrough_answer")
        if isinstance(answer, dict):
            return dict(answer)
    return None


def _estimate_tokens(query_text: str, evidence_cards: list[dict]) -> int:
    words = len(query_text.split())
    for card in evidence_cards:
        words += len(str(card.get("content", "")).split())
    return max(1, int(words * 1.3))


def _has_tool_surface_result(evidence_cards: list[dict]) -> bool:
    tool_kinds = {
        "filesystem_snapshot",
        "git_history",
        "repository_snapshot",
        "ticket_system_snapshot",
        "tool_result",
    }
    for card in evidence_cards:
        metadata = card.get("metadata")
        if not isinstance(metadata, dict):
            continue
        observation_kind = str(metadata.get("observation_kind") or "")
        if observation_kind in tool_kinds:
            return True
        if metadata.get("tool_result") or metadata.get("external_tool_result"):
            return True
    return False


def _derived_context_evidence_cards(
    query: BenchmarkQuery,
    evidence_cards: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    checkbox_card = _derive_checkbox_comparison_card(query, evidence_cards)
    return [checkbox_card] if checkbox_card is not None else []


def _derive_checkbox_comparison_card(
    query: BenchmarkQuery,
    evidence_cards: list[dict[str, Any]],
) -> dict[str, Any] | None:
    query_text = query.query_text
    if "checkbox" not in query_text.casefold():
        return None
    options = _multiple_choice_options(query_text)
    single_options = {
        letter: label
        for letter, label in options.items()
        if " and " not in label.casefold() and "tied" not in label.casefold()
    }
    if len(single_options) < 2:
        return None

    counts: dict[str, int] = {}
    for card in evidence_cards:
        content = str(card.get("content") or "")
        for count in _checkbox_choice_counts(content):
            for label in single_options.values():
                if _card_mentions_option_page(content, label):
                    counts[label] = max(counts.get(label, 0), count)
    if set(counts) < set(single_options.values()):
        return None

    max_count = max(counts.values())
    winners = sorted(label for label, count in counts.items() if count == max_count)
    answer_letter = _choice_letter_for_winners(options, winners)
    if answer_letter is None:
        return None

    lines = [
        "Derived checkbox comparison from retrieved evidence.",
        *[
            f"- {label}: {counts[label]} checkbox choices"
            for label in single_options.values()
        ],
        "Largest checkbox choice count: "
        + ", ".join(winners)
        + f" ({max_count})",
        f"Matching multiple-choice answer: {answer_letter}",
        f"Final derived answer: \\boxed{{{answer_letter}}}",
    ]
    return {
        "observation_id": f"{query.query_id}:derived:checkbox_comparison",
        "content": "\n".join(lines),
        "score": max(
            (float(card.get("score") or 0.0) for card in evidence_cards),
            default=0.0,
        ),
        "occurred_at": "",
        "metadata": {
            "derived_kind": "checkbox_comparison",
            "source_observation_ids": [
                str(card.get("observation_id"))
                for card in evidence_cards
                if card.get("observation_id")
            ],
        },
    }


def _multiple_choice_options(query_text: str) -> dict[str, str]:
    options: dict[str, str] = {}
    for match in re.finditer(r"(?m)^\s*([A-Z])\.\s+(.+?)\s*$", query_text):
        options[match.group(1)] = " ".join(match.group(2).split())
    return options


def _checkbox_choice_counts(content: str) -> list[int]:
    return [
        int(match.group(1))
        for match in re.finditer(
            r"checkbox choice group visible:\s*count\s*=\s*(\d+)",
            content,
            flags=re.IGNORECASE,
        )
    ]


def _card_mentions_option_page(content: str, label: str) -> bool:
    content_lower = content.casefold()
    label_lower = label.casefold()
    anchors = (
        f"create favorite for {label_lower}",
        f"{label_lower} | servicenow",
        f"order {label_lower}",
    )
    return any(anchor in content_lower for anchor in anchors)


def _choice_letter_for_winners(
    options: dict[str, str],
    winners: list[str],
) -> str | None:
    winner_set = {winner.casefold() for winner in winners}
    for letter, label in options.items():
        label_lower = label.casefold()
        if len(winner_set) == 1 and label_lower in winner_set:
            return letter
        if len(winner_set) > 1 and all(winner in label_lower for winner in winner_set):
            return letter
    return None


def _answer_requirements(query: BenchmarkQuery) -> list[dict[str, Any]]:
    text = f"{query.query_text} {query.query_type}".casefold()
    requirements: list[dict[str, Any]] = []

    def add(kind: str, description: str) -> None:
        if any(item["kind"] == kind for item in requirements):
            return
        requirements.append({"kind": kind, "description": description})

    if any(marker in text for marker in (" and ", " plus ", " along with ", "format:")):
        add("multipart", "Include every requested component, not just the most salient span.")
    if any(marker in text for marker in ("who", "team member", "owner", "lead", "championed")):
        add("actor", "Resolve the exact actor or team requested.")
    if any(marker in text for marker in ("specific", "exact", "metric", "evidence")):
        add("specificity", "Preserve concrete files, metrics, code symbols, artifacts, or evidence sources.")
    if any(marker in text for marker in ("why", "caused", "root cause", "mechanism", "reason")):
        add("causal_mechanism", "Answer with the causal mechanism, not only the symptom.")
    if any(marker in text for marker in ("before", "after", "during", "first", "most recent")):
        add("temporal_scope", "Apply the temporal constraint exactly.")
    if any(marker in text for marker in ("final", "final solution", "resolved", "replacement", "solution")):
        add("finality", "Prefer the final decision or deployed outcome over intermediate assessment events.")
    if query.metadata.get("requires_external_tool_surface"):
        add("external_tool_surface", "Requires a materialized tool result, not only conversational breadcrumbs.")
    return requirements


def _packet_sufficiency(
    query: BenchmarkQuery,
    evidence_cards: list[dict],
    requirements: list[dict[str, Any]],
) -> dict[str, Any]:
    required_roles = _required_roles(query, requirements)
    covered_roles = _covered_roles(evidence_cards)
    missing_roles = sorted(required_roles - covered_roles)
    content = "\n".join(str(card.get("content", "")) for card in evidence_cards).casefold()
    requires_finality = any(item["kind"] == "finality" for item in requirements)
    has_finality_evidence = (
        not requires_finality
        or "final_outcome" in covered_roles
        or any(
            marker in content
            for marker in (
                "completed",
                "deployed",
                "final solution",
                "replacement:",
                "resolution:",
                "resolved",
                "solution shipped",
            )
        )
    )
    return {
        "required_roles": sorted(required_roles),
        "covered_roles": sorted(covered_roles),
        "missing_roles": missing_roles,
        "requires_finality": requires_finality,
        "has_finality_evidence": has_finality_evidence,
        "has_external_tool_result": _has_tool_surface_result(evidence_cards),
    }


def _required_roles(
    query: BenchmarkQuery,
    requirements: list[dict[str, Any]],
) -> set[str]:
    kinds = {str(item.get("kind")) for item in requirements}
    text = f"{query.query_text} {query.query_type}".casefold()
    roles: set[str] = set()
    if {"causal_mechanism", "temporal_scope", "finality"} & kinds:
        roles.update({"temporal_anchor"})
    if "causal_mechanism" in kinds:
        roles.add("cause")
        if any(marker in text for marker in ("what did it cause", "led to", "outcome", "result", "impact")):
            roles.add("outcome")
    if "finality" in kinds:
        roles.update({"decision", "final_outcome"})
    if "actor" in kinds:
        roles.add("owner")
    if any(marker in text for marker in ("what happened", "how did", "led to")):
        roles.add("transition")
    return roles


def _covered_roles(evidence_cards: list[dict]) -> set[str]:
    roles: set[str] = set()
    for card in evidence_cards:
        metadata = card.get("metadata")
        if isinstance(metadata, dict):
            for key in ("covered_roles", "hybrid_roles", "hybrid_packing_roles"):
                raw_roles = metadata.get(key)
                if isinstance(raw_roles, list):
                    roles.update(str(role) for role in raw_roles)
            if metadata.get("derived_kind") == "query_chain":
                roles.add("composed_chain")
            if metadata.get("event_index") is not None or metadata.get("timestamp_raw"):
                roles.add("temporal_anchor")
            if any(metadata.get(key) for key in ("sender", "lead", "team", "author")):
                roles.add("owner")
        content = str(card.get("content", "")).casefold()
        if any(marker in content for marker in ("because", "caused", "root cause", "due to")):
            roles.add("cause")
        if any(marker in content for marker in ("after", "before", "changed", "split", "led to")):
            roles.add("transition")
        if any(marker in content for marker in ("decision:", "decided", "proposed", "replacement:")):
            roles.add("decision")
        if any(marker in content for marker in ("completed", "deployed", "resolved", "solution")):
            roles.add("outcome")
        if any(
            marker in content
            for marker in (
                "completed",
                "deployed",
                "final solution",
                "replacement shipped",
                "resolved",
                "resolution:",
                "solution shipped",
            )
        ):
            roles.add("final_outcome")
    return roles


def _focused_content(
    query_text: str,
    content: str,
    *,
    max_chars: int,
    max_snippets: int,
) -> str:
    if max_chars <= 0 or len(content) <= max_chars:
        return content

    query_terms = tokenize(query_text)
    if not query_terms:
        return _clip(content, max_chars)

    segments = _segments(content)
    scored = []
    for index, segment in enumerate(segments):
        segment_terms = tokenize(segment)
        overlap = query_terms & segment_terms
        if not overlap:
            continue
        score = len(overlap) * 3 + sum(
            min(3, segment.casefold().count(term))
            for term in overlap
        )
        score += _transition_focus_bonus(query_text, segment)
        scored.append((score, index, segment))

    if not scored:
        return _clip(content, max_chars)

    scored.sort(key=lambda item: (-item[0], item[1]))
    selected = sorted(scored[:max(1, max_snippets)], key=lambda item: item[1])
    snippets = [
        _clip_around_terms(segment, query_terms, max_chars=max_chars // len(selected))
        for _score, _index, segment in selected
    ]
    joined = "\n...\n".join(snippets)
    return _clip(joined, max_chars)


def _segments(content: str) -> list[str]:
    lines = [line.strip() for line in content.splitlines() if line.strip()]
    if len(lines) > 1:
        return lines
    rough = []
    current = []
    for sentence in content.replace("?", "?\n").replace("!", "!\n").replace(". ", ".\n").splitlines():
        sentence = sentence.strip()
        if not sentence:
            continue
        current.append(sentence)
        if sum(len(item) for item in current) >= 900:
            rough.append(" ".join(current))
            current = []
    if current:
        rough.append(" ".join(current))
    return rough or [content]


def _transition_focus_bonus(query_text: str, segment: str) -> int:
    query_lower = query_text.casefold()
    segment_lower = segment.casefold()
    dynamic_markers = (
        "after",
        "automatic",
        "automatically",
        "before",
        "change",
        "changed",
        "dropdown",
        "open",
        "option",
        "pipeline",
        "sort",
        "stage",
        "state",
    )
    if not any(marker in query_lower for marker in dynamic_markers):
        return 0
    structured_intents = _structured_ui_intents(query_text)
    if (
        "form controls visible" in segment_lower
        or "after action form controls visible" in segment_lower
        or "field/configuration controls visible" in segment_lower
        or "price option controls visible" in segment_lower
        or "selected/checked controls visible" in segment_lower
        or "unchecked/unselected controls visible" in segment_lower
        or (
            structured_intents
            and (
                "structured ui facts visible" in segment_lower
                or _structured_fact_matches_intents(segment, structured_intents)
            )
        )
        or " option " in segment_lower
    ):
        return 90
    if (
        "remaining_excluding_in_progress_count" in segment_lower
        or "pending_not_started_count" in segment_lower
        or "in_progress_count" in segment_lower
    ):
        return 85
    if (
        "newly visible" in segment_lower
        or "newly showing" in segment_lower
        or "field value has changed" in segment_lower
    ):
        return 70
    if "no longer visible" in segment_lower:
        return 55
    if "after action" in segment_lower or "workflow phase transition" in segment_lower:
        return 45
    if "transition summary" in segment_lower:
        return 30
    if "after state" in segment_lower or "before state" in segment_lower:
        return 10
    return 0


def _append_relevant_structured_ui_facts(
    query_text: str,
    content: str,
    metadata: dict,
    *,
    max_chars: int,
) -> str:
    intents = _structured_ui_intents(query_text)
    if not intents:
        return content
    relevant = _relevant_structured_metadata(metadata, intents)[:8]
    if not relevant:
        return content
    fact_block = "Relevant structured UI facts: " + " | ".join(relevant)
    if not content:
        return _clip(fact_block, max_chars)
    remaining = max_chars - len(fact_block) - 2
    if remaining <= 80:
        return _clip(fact_block, max_chars)
    return _clip(content, remaining).rstrip() + "\n" + fact_block


def _structured_ui_intents(query_text: str) -> set[str]:
    query_lower = query_text.casefold()
    intents: set[str] = set()
    if any(
        marker in query_lower
        for marker in (
            "bottom",
            "last option",
            "option order",
            "selected pane",
            "list columns",
            "left pane",
            "right pane",
        )
    ):
        intents.add("field_list")
    if any(
        marker in query_lower
        for marker in (
            "default sort",
            "initially shown in the sort",
            "sort field",
            "sort row",
            "sorting",
            "target field",
        )
    ):
        intents.add("sort")
    if any(
        marker in query_lower
        for marker in (
            "popup",
            "pop-up",
            "search box",
            "recent selection",
            "recent selections",
            "lookup",
        )
    ) or ("title" in query_lower and ("box" in query_lower or "popup" in query_lower)):
        intents.add("popup")
    if any(marker in query_lower for marker in ("total", "summary row", "subtotal")):
        intents.add("table")
    if any(
        marker in query_lower
        for marker in (
            "checkbox",
            "checked",
            "unchecked",
            "choice",
            "choices",
            "selected options",
        )
    ):
        intents.add("checkbox")
    if any(
        marker in query_lower
        for marker in (
            "automatically change",
            "automatically changes",
            "field automatically",
            "impact",
            "priority",
            "urgency",
            "what value",
        )
    ):
        intents.add("form_value")
    if any(
        marker in query_lower
        for marker in (
            "excluding in-progress",
            "fully complete",
            "how many stages",
            "pipeline",
            "stages remain",
        )
    ):
        intents.add("stage_count")
    if any(
        marker in query_lower
        for marker in (
            "required",
            "read-only",
            "readonly",
            "disabled",
            "editable",
            "duplicate of",
        )
    ):
        intents.add("editable_form")
    return intents


def _relevant_structured_metadata(metadata: dict, intents: set[str]) -> list[str]:
    relevant: list[str] = []
    facts = metadata.get("structured_ui_facts")
    if isinstance(facts, list):
        relevant.extend(
            _readable_structured_fact(str(fact))
            for fact in facts
            if _structured_fact_matches_intents(str(fact), intents)
        )
    if "sort" in intents:
        sort_fields = metadata.get("sort_fields")
        if isinstance(sort_fields, list) and sort_fields:
            relevant.append("sort fields visible: " + "; ".join(str(item) for item in sort_fields[:8]))
    if "stage_count" in intents:
        stage_chains = metadata.get("stage_chains")
        if isinstance(stage_chains, list) and stage_chains:
            relevant.append(
                "pipeline stage chains: "
                + " | ".join(_stage_chain_with_number_words(str(item)) for item in stage_chains[:4])
            )
    return _dedupe_preserve_order(relevant)


def _readable_structured_fact(fact: str) -> str:
    return re.sub(r"\s*=\s*", " = ", fact)


def _structured_fact_matches_intents(fact: str, intents: set[str]) -> bool:
    fact_lower = fact.casefold()
    return (
        ("field_list" in intents and "field list " in fact_lower)
        or ("sort" in intents and ("sort fields visible" in fact_lower or "order results by" in fact_lower))
        or ("popup" in intents and "autocomplete popup title" in fact_lower)
        or ("table" in intents and "table summary row" in fact_lower)
        or ("checkbox" in intents and "checkbox choice group" in fact_lower)
        or (
            "editable_form" in intents
            and (
                "editable form fields" in fact_lower
                or "required editable form fields" in fact_lower
                or "disabled/read-only form fields" in fact_lower
            )
        )
        or (
            "form_value" in intents
            and (
                "editable form fields" in fact_lower
                or "required editable form fields" in fact_lower
                or "disabled/read-only form fields" in fact_lower
            )
        )
    )


def _stage_chain_with_number_words(value: str) -> str:
    def replace_count(match) -> str:
        number = int(match.group(1))
        word = _small_number_word(number)
        if word is None:
            return match.group(0)
        return f"{match.group(0)} ({word})"

    return re.sub(
        r"remaining_excluding_in_progress_count=(\d+)",
        replace_count,
        value,
    )


def _small_number_word(number: int) -> str | None:
    words = {
        0: "zero",
        1: "one",
        2: "two",
        3: "three",
        4: "four",
        5: "five",
        6: "six",
        7: "seven",
        8: "eight",
        9: "nine",
        10: "ten",
        11: "eleven",
        12: "twelve",
    }
    return words.get(number)


def _dedupe_preserve_order(items: list[str]) -> list[str]:
    seen: set[str] = set()
    deduped: list[str] = []
    for item in items:
        normalized = " ".join(str(item).casefold().split())
        if not item or normalized in seen:
            continue
        seen.add(normalized)
        deduped.append(item)
    return deduped


def _clip_around_terms(text: str, query_terms: set[str], *, max_chars: int) -> str:
    if len(text) <= max_chars:
        return text
    lower = text.casefold()
    preferred_markers = (
        "remaining_excluding_in_progress_count",
        "pipeline items",
        "pipeline stages",
        "form controls visible",
        "after action form controls visible",
        "field/configuration controls visible",
        "price option controls visible",
        "selected/checked controls visible",
        "unchecked/unselected controls visible",
        "structured ui facts visible",
        "after action structured ui facts visible",
        "field list ",
        "table summary row",
        "autocomplete popup title",
        "pending_not_started_count",
        "in_progress_count",
        "sort fields visible",
        "newly visible after action",
        "newly visible ui labels",
    )
    preferred_positions = [
        lower.find(marker)
        for marker in preferred_markers
        if lower.find(marker) >= 0
    ]
    if preferred_positions:
        return _bounded_window(text, center=min(preferred_positions), max_chars=max_chars)
    positions = [
        lower.find(term)
        for term in query_terms
        if lower.find(term) >= 0
    ]
    if not positions:
        return _clip(text, max_chars)
    return _bounded_window(text, center=min(positions), max_chars=max_chars)


def _bounded_window(text: str, *, center: int, max_chars: int) -> str:
    if max_chars <= 0 or len(text) <= max_chars:
        return text

    start = max(0, center - max_chars // 3)
    end = min(len(text), start + max_chars)
    for _ in range(3):
        prefix = "[...]" if start > 0 else ""
        suffix = "[...]" if end < len(text) else ""
        available = max(1, max_chars - len(prefix) - len(suffix))
        start = max(0, center - available // 3)
        end = min(len(text), start + available)
        start = max(0, end - available)

    prefix = "[...]" if start > 0 else ""
    suffix = "[...]" if end < len(text) else ""
    return prefix + text[start:end].strip() + suffix


def _clip(text: str, max_chars: int) -> str:
    if max_chars <= 0 or len(text) <= max_chars:
        return text
    suffix = "\n[truncated]"
    if max_chars <= len(suffix):
        return text[:max_chars]
    return text[: max_chars - len(suffix)].rstrip() + suffix
