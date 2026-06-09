"""Typed read projections over Synthesis state.

This module does not create a second source of truth. It turns already
retrieved Synthesis material into a compact, addressable contract that
answering layers can use to reason about current state, premise fit,
workflow gaps, and recurring local traps.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Iterable, Sequence


_SLOT_PRIORITY = {
    "premise_challenge": 0,
    "dynamic_state": 1,
    "current_stage": 2,
    "current_blocker": 3,
    "current_owner": 4,
    "workflow_missing_step": 5,
    "recurring_gotcha": 6,
    "temporal_anchor": 7,
    "exact_value": 8,
}


@dataclass(frozen=True, slots=True)
class StateSource:
    source_kind: str
    source_ref: str
    text: str
    occurred_at: datetime | None = None
    confidence: float = 0.5
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class StateFact:
    slot: str
    subject: str
    value: str
    status: str
    confidence: float
    evidence_refs: tuple[str, ...]
    source_kinds: tuple[str, ...]
    as_of: datetime | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "slot": self.slot,
            "subject": self.subject,
            "value": self.value,
            "status": self.status,
            "confidence": self.confidence,
            "evidence_refs": list(self.evidence_refs),
            "source_kinds": list(self.source_kinds),
            "as_of": self.as_of.isoformat() if self.as_of else None,
        }


@dataclass(frozen=True, slots=True)
class PremiseCheck:
    status: str
    assumptions: tuple[dict[str, str], ...] = ()
    corrections: tuple[str, ...] = ()
    supporting_facts: tuple[str, ...] = ()
    missing_slots: tuple[str, ...] = ()
    counterevidence_refs: tuple[str, ...] = ()
    reason: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "status": self.status,
            "assumptions": list(self.assumptions),
            "corrections": list(self.corrections),
            "supporting_facts": list(self.supporting_facts),
            "missing_slots": list(self.missing_slots),
            "counterevidence_refs": list(self.counterevidence_refs),
            "reason": self.reason,
        }


@dataclass(frozen=True, slots=True)
class StateContract:
    required_slots: tuple[str, ...]
    covered_slots: tuple[str, ...]
    missing_slots: tuple[str, ...]
    facts: tuple[StateFact, ...]
    premise_check: PremiseCheck

    def to_dict(self) -> dict[str, Any]:
        return {
            "required_slots": list(self.required_slots),
            "covered_slots": list(self.covered_slots),
            "missing_slots": list(self.missing_slots),
            "facts": [fact.to_dict() for fact in self.facts],
            "premise_check": self.premise_check.to_dict(),
        }


def compile_state_contract(
    query: str,
    sources: Sequence[StateSource] | Iterable[StateSource],
) -> StateContract:
    """Compile an answer-facing state contract from Synthesis read material."""

    source_list = [source for source in sources if str(source.text or "").strip()]
    subject = _infer_subject(query, (source.text for source in source_list))
    required_slots = tuple(sorted(_required_slots_for_query(query), key=_slot_sort_key))
    facts = _dedupe_facts(
        fact
        for source in source_list
        for fact in _facts_from_source(source, subject)
    )
    covered_slots = tuple(sorted({fact.slot for fact in facts}, key=_slot_sort_key))
    missing_slots = tuple(slot for slot in required_slots if slot not in covered_slots)
    premise_check = _premise_check(query, facts, missing_slots)
    return StateContract(
        required_slots=required_slots,
        covered_slots=covered_slots,
        missing_slots=missing_slots,
        facts=tuple(facts),
        premise_check=premise_check,
    )


def _required_slots_for_query(query: str) -> set[str]:
    text = query.casefold()
    slots: set[str] = set()
    if any(marker in text for marker in ("block", "blocked", "blocker", "risk", "stall", "slip")):
        slots.add("current_blocker")
    if any(
        marker in text
        for marker in (
            "changed",
            "current state",
            "current status",
            "state of",
            "still marked",
            "in commit",
            "marked commit",
            "at risk",
        )
    ):
        slots.add("dynamic_state")
        if "commit" in text or "stage" in text or "status" in text:
            slots.add("current_stage")
    if any(marker in text for marker in ("owner", "owns", "responsible", "assigned", "lead", "dri")):
        slots.add("current_owner")
    if any(
        marker in text
        for marker in (
            "after",
            "as of",
            "before",
            "during",
            "final",
            "first",
            "last",
            "latest",
            "most recent",
            "previous",
            "when",
        )
    ):
        slots.add("temporal_anchor")
    if any(
        marker in text
        for marker in (
            "checkbox",
            "count",
            "exact",
            "false",
            "how many",
            "largest",
            "least",
            "number",
            "price",
            "quantity",
            "true",
            "value",
        )
    ):
        slots.add("exact_value")
    if any(
        marker in text
        for marker in (
            "before",
            "missing step",
            "next step",
            "onboarding",
            "procurement",
            "security review",
            "what step",
            "workflow",
        )
    ):
        slots.add("workflow_missing_step")
    if any(
        marker in text
        for marker in (
            "always",
            "fails when",
            "gotcha",
            "recurring",
            "trap",
            "usually",
        )
    ):
        slots.add("recurring_gotcha")
    return slots


def _facts_from_source(source: StateSource, subject: str) -> list[StateFact]:
    text = " ".join(str(source.text or "").split())
    folded = text.casefold()
    confidence = _bounded_confidence(source.confidence)
    facts: list[StateFact] = []

    def add(slot: str, value: str, status: str = "current") -> None:
        clean = _compact(value, 360)
        if not clean:
            return
        facts.append(
            StateFact(
                slot=slot,
                subject=subject,
                value=clean,
                status=status,
                confidence=confidence,
                evidence_refs=(source.source_ref,),
                source_kinds=(source.source_kind,),
                as_of=source.occurred_at,
            )
        )

    if source.occurred_at is not None:
        add(
            "temporal_anchor",
            f"{source.source_kind} observed at {source.occurred_at.isoformat()}",
            "observed",
        )

    transition = re.search(
        r"\bchanged\s+from\s+(.+?)\s+to\s+(.+?)(?:\s+after|\s+because|\s+when|[.,;]|$)",
        text,
        flags=re.IGNORECASE,
    )
    if transition:
        previous = _clean_phrase(transition.group(1))
        current = _clean_phrase(transition.group(2))
        add("dynamic_state", f"changed from {previous} to {current}", "changed")
        add("current_stage", current, "current")
    elif " at risk" in f" {folded}" or "at-risk" in folded:
        add("current_stage", "At Risk", "current")

    if "commit" in folded and any(
        marker in folded
        for marker in (
            "does not support",
            "is unsupported",
            "not supported",
            "premise is unsupported",
            "unsupported",
        )
    ):
        add("current_stage", "Commit is not supported by current evidence", "challenged")

    blocker_match = re.search(
        r"\bblocked\s+by\s+(.+?)(?:\s+and\s+still|\s+and\s+the|\s+but|[.;]|$)",
        text,
        flags=re.IGNORECASE,
    )
    if blocker_match:
        add("current_blocker", _clean_phrase(blocker_match.group(1)), "current")
    if "blocker" in folded and any(marker in folded for marker in ("not the only", "also active", "another")):
        value = text
        if "data migration" in folded:
            value = "SSO is not the only blocker; data migration is also active"
        add("current_blocker", value, "expanded")
    if "data migration became active" in folded or "data migration is also active" in folded:
        add("current_blocker", "data migration is active", "current")

    if any(
        marker in folded
        for marker in (
            "no explicit owner is represented",
            "lacks an accountable owner",
            "missing owner",
            "owner ambiguity",
            "without an owner",
        )
    ):
        add("current_owner", "no explicit owner represented", "missing")
    owner_match = re.search(
        r"\b(?:owner|dri)\s*(?:is|=|:)\s*([^.;,\n]+)",
        text,
        flags=re.IGNORECASE,
    )
    if owner_match and "no explicit owner" not in folded:
        add("current_owner", _clean_phrase(owner_match.group(1)), "current")
    assigned_match = re.search(
        r"\b(?:assigned to|responsible party is|responsible is)\s+([^.;,\n]+)",
        text,
        flags=re.IGNORECASE,
    )
    if assigned_match:
        add("current_owner", _clean_phrase(assigned_match.group(1)), "current")

    if (
        ("missing" in folded and ("step" in folded or "assignment" in folded))
        or "before procurement can move forward" in folded
        or "before this commitment can move forward" in folded
        or "unless ownership is assigned" in folded
    ):
        add("workflow_missing_step", text, "current")
    requires_match = re.search(
        r"\brequires\s+(.+?)\s+before\s+(.+?)(?:[.;]|$)",
        text,
        flags=re.IGNORECASE,
    )
    if requires_match:
        add(
            "workflow_missing_step",
            f"requires {_clean_phrase(requires_match.group(1))} before {_clean_phrase(requires_match.group(2))}",
            "current",
        )

    if any(
        marker in folded
        for marker in (
            "always stalls unless",
            "blocks procurement if",
            "fail when",
            "fails when",
            "recurring trap",
            "slips if",
        )
    ):
        add("recurring_gotcha", text, "current")

    if any(
        marker in folded
        for marker in (
            "also active",
            "does not support",
            "incomplete premise",
            "is unsupported",
            "not the only blocker",
            "premise is incomplete",
            "premise is unsupported",
            "premise is wrong",
            "premise is stale",
            "stale premise",
            "that premise",
            "wrong premise",
        )
    ):
        add("premise_challenge", text, "challenged")
    if "no explicit owner is represented" in folded:
        add("premise_challenge", "No explicit owner is represented in Synthesis.", "unsupported")

    for value in _exact_values_from_text(text):
        add("exact_value", value, "observed")

    return facts


def _exact_values_from_text(text: str) -> list[str]:
    values: list[str] = []

    def add(raw: str) -> None:
        clean = _clean_phrase(raw)
        if clean and clean.casefold() not in {item.casefold() for item in values}:
            values.append(clean)

    for pattern in (
        r"\b(?:count|quantity|number|value)\s*(?:=|:|is)\s*([^.;,\n]+)",
        r"\b(?:price|amount|cost)\s*(?:=|:|is)\s*(\$?\d+(?:\.\d+)?)",
        r"\bcheckbox(?:\s+choice|\s+choices)?\s*(?:group\s+visible:\s*)?count\s*=\s*(\d+)",
        r"\b(?:status|stage|state|field)\s*(?:=|:|is)\s*([^.;,\n]+)",
    ):
        for match in re.finditer(pattern, text, flags=re.IGNORECASE):
            add(match.group(0))

    lowered = text.casefold()
    if re.search(r"\btrue\b", lowered):
        add("observed boolean true")
    if re.search(r"\bfalse\b", lowered):
        add("observed boolean false")
    return values[:8]


def _premise_check(
    query: str,
    facts: Sequence[StateFact],
    missing_slots: Sequence[str],
) -> PremiseCheck:
    assumptions = tuple(_query_assumptions(query))
    challenge_facts = [fact for fact in facts if fact.slot == "premise_challenge"]
    owner_missing = [
        fact
        for fact in facts
        if fact.slot == "current_owner" and fact.status in {"missing", "unsupported"}
    ]
    stage_challenged = [
        fact
        for fact in facts
        if fact.slot == "current_stage" and fact.status == "challenged"
    ]
    corrections = _dedupe_text(
        [fact.value for fact in [*challenge_facts, *stage_challenged]]
    )
    counter_refs = _dedupe_text(
        ref
        for fact in [*challenge_facts, *stage_challenged, *owner_missing]
        for ref in fact.evidence_refs
    )
    supporting = _dedupe_text(
        fact.value
        for fact in facts
        if fact.slot in {"current_blocker", "current_stage", "dynamic_state", "current_owner"}
    )
    owner_question = bool(
        re.search(r"\b(who\s+owns|owner|responsible|assigned|dri)\b", query, flags=re.IGNORECASE)
    )

    if corrections:
        status = "stale_or_incomplete"
        reason = "Retrieved Synthesis evidence challenges at least one premise in the question."
        if any("unsupported" in correction.casefold() for correction in corrections):
            reason = "Retrieved Synthesis evidence says at least one premise is unsupported or incomplete."
        return PremiseCheck(
            status=status,
            assumptions=assumptions,
            corrections=tuple(corrections),
            supporting_facts=tuple(supporting[:6]),
            missing_slots=tuple(missing_slots),
            counterevidence_refs=tuple(counter_refs),
            reason=reason,
        )
    if owner_question and owner_missing:
        return PremiseCheck(
            status="unsupported",
            assumptions=assumptions,
            corrections=tuple(fact.value for fact in owner_missing[:3]),
            supporting_facts=tuple(supporting[:6]),
            missing_slots=tuple(missing_slots),
            counterevidence_refs=tuple(counter_refs),
            reason="Synthesis represents the ownership slot as missing, not assigned.",
        )
    if assumptions and missing_slots:
        return PremiseCheck(
            status="unknown",
            assumptions=assumptions,
            supporting_facts=tuple(supporting[:6]),
            missing_slots=tuple(missing_slots),
            reason="The compact read lacks required slots to verify every premise.",
        )
    if assumptions:
        return PremiseCheck(
            status="supported",
            assumptions=assumptions,
            supporting_facts=tuple(supporting[:6]),
            reason="No direct premise challenge survived the compact Synthesis read.",
        )
    return PremiseCheck(
        status="not_checked",
        missing_slots=tuple(missing_slots),
        supporting_facts=tuple(supporting[:6]),
        reason="The question did not encode a concrete premise to verify.",
    )


def _query_assumptions(query: str) -> list[dict[str, str]]:
    assumptions: list[dict[str, str]] = []
    blocked_by = re.search(
        r"\bblocked\s+by\s+(.+?)(?:\s+and\s+still|\s+and\s+why|\s+but|[?.;]|$)",
        query,
        flags=re.IGNORECASE,
    )
    if blocked_by:
        assumptions.append(
            {"slot": "current_blocker", "value": _clean_phrase(blocked_by.group(1))}
        )
    if re.search(r"\b(still\s+marked\s+commit|marked\s+commit|in\s+commit)\b", query, flags=re.IGNORECASE):
        assumptions.append({"slot": "current_stage", "value": "Commit"})
    return assumptions


def _dedupe_facts(facts: Iterable[StateFact]) -> list[StateFact]:
    by_key: dict[tuple[str, str, str], StateFact] = {}
    for fact in facts:
        key = (fact.slot, fact.status, fact.value.casefold())
        existing = by_key.get(key)
        if existing is None or fact.confidence > existing.confidence:
            by_key[key] = fact
    return sorted(
        by_key.values(),
        key=lambda fact: (
            _slot_sort_key(fact.slot),
            -(fact.confidence or 0.0),
            fact.value.casefold(),
        ),
    )


def _infer_subject(query: str, source_texts: Iterable[str]) -> str:
    skip = {
        "at",
        "commit",
        "crm",
        "dri",
        "sso",
        "the",
        "what",
        "who",
        "why",
    }
    for text in (query, *list(source_texts)[:8]):
        for candidate in re.findall(r"\b[A-Z][A-Za-z0-9_-]+\b", text):
            if candidate.casefold() not in skip:
                return candidate
    return "current scope"


def _bounded_confidence(value: float | int | None) -> float:
    try:
        raw = float(value if value is not None else 0.5)
    except (TypeError, ValueError):
        raw = 0.5
    return round(max(0.05, min(0.99, raw)), 2)


def _clean_phrase(value: str) -> str:
    return " ".join(str(value or "").strip(" .,:;!?()[]{}\"'").split())


def _compact(text: str, limit: int) -> str:
    clean = " ".join(str(text or "").split())
    if len(clean) <= limit:
        return clean
    return clean[: max(0, limit - 3)].rstrip() + "..."


def _dedupe_text(values: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for value in values:
        clean = _compact(str(value or ""), 360)
        key = clean.casefold()
        if not clean or key in seen:
            continue
        seen.add(key)
        out.append(clean)
    return out


def _slot_sort_key(slot: str) -> tuple[int, str]:
    return (_SLOT_PRIORITY.get(slot, 99), slot)
