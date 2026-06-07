"""Operational memory facets for Fyralis Models.

This module keeps `proposition.kind` small. It does not introduce a
parallel ontology for forms, catalogs, tickets, or benchmarks. Instead,
it annotates ordinary Models with evidence-backed universal facets:
object, property, value, action, sequence, state, delta, count, and
invariant. Retrieval can then ask for the shape of evidence it needs
without learning benchmark-specific shortcuts.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Iterable, Mapping

from services.synthesis.query_understanding import (
    alternative_terms,
    extract_query_alternatives,
)


FACET_SCHEMA_VERSION = "operational_facets_v1"

UNIVERSAL_FACET_ROLES: frozenset[str] = frozenset({
    "object",
    "property",
    "value",
    "action",
    "sequence",
    "state",
    "delta",
    "count",
    "invariant",
})

_CONTROL_PRICE_RE = re.compile(
    r"\b(?P<control>radio|checkbox|option)\s+"
    r"(?P<label>[^;\n|]{1,120}?)\s+"
    r"\[add\s+\$(?P<amount>\d+(?:\.\d+)?)\]\s+"
    r"(?P<state>checked|selected)=(?P<flag>true|false)",
    flags=re.IGNORECASE,
)
_CONTROL_STATE_RE = re.compile(
    r"\b(?P<control>radio|checkbox|option)\s+"
    r"(?P<label>[^;\n|]{1,140}?)\s+"
    r"(?P<state>checked|selected)=(?P<flag>true|false)",
    flags=re.IGNORECASE,
)
_QUOTED_VALUE_RE = re.compile(
    r"['\"](?P<property>[^'\"]{2,100})['\"]\s+value=['\"](?P<value>[^'\"]{0,160})['\"]",
    flags=re.IGNORECASE,
)
_FIELD_ASSIGNMENT_RE = re.compile(
    r"(?P<property>[A-Za-z][A-Za-z0-9 _./()#-]{1,90})\s*=\s*(?P<value>[^;|\n]{1,180})"
)
_FIELD_LIST_RE = re.compile(
    r"field list\s+(?P<subject>[^:;|\n]{2,80})\s+option order:\s*(?P<items>[^|\n]{3,900})",
    flags=re.IGNORECASE,
)
_BOTTOM_OPTION_RE = re.compile(
    r"\bbottom_option\s*=\s*(?P<value>[^;|\n]{1,120})",
    flags=re.IGNORECASE,
)
_PIPELINE_RE = re.compile(
    r"(?:pipeline|stage)\s+(?:stage\s+)?chains?:\s*(?P<chain>[^|\n]{3,1200})",
    flags=re.IGNORECASE,
)
_STAGE_ITEM_RE = re.compile(
    r"(?P<name>[^;()|]{2,120})\s*\((?P<status>[^()]{2,120})\)"
)
_COUNT_RE = re.compile(
    r"\b(?P<name>[A-Za-z][A-Za-z0-9 _./()_-]{0,80}?(?:count|items|records|results|quantity|number))"
    r"\s*(?:=|:|is)?\s*(?P<count>\d+)\b",
    flags=re.IGNORECASE,
)
_RELATED_LINK_RE = re.compile(
    r"related links?\s*(?:visible|include|:)?\s*(?P<labels>[^.\n|]{2,400})",
    flags=re.IGNORECASE,
)
_ABSENCE_RE = re.compile(
    r"\b(?:there\s+is\s+no|there\s+are\s+no|no\s+additional|does\s+not\s+appear|not\s+shown|not\s+available)\b",
    flags=re.IGNORECASE,
)

_QUERY_ROLE_MARKERS: tuple[tuple[tuple[str, ...], tuple[str, ...]], ...] = (
    (("price", "dollar", "amount", "cost", "extra", "add $", "delta"), ("delta", "value", "property")),
    (("field", "form", "mandatory", "read only", "read-only", "prepopulated"), ("property", "state", "value")),
    (("button", "link", "related link", "click", "action"), ("action", "property")),
    (("stage", "pipeline", "remaining", "remain", "complete", "status"), ("sequence", "state", "count")),
    (("how many", "count", "number", "total", "combination", "quantity"), ("count", "value", "property")),
    (("selected", "checked", "checkbox", "radio", "option", "configuration"), ("state", "value", "property")),
    (("workflow", "procedure", "protocol", "typical", "step", "route", "module"), ("sequence", "action", "property")),
    (("column", "list", "sort", "order by", "option order", "bottom", "top-most", "bottom-most"), ("sequence", "property", "state")),
    (("true or false", "false", "not", "no", "does not"), ("invariant", "state")),
)

_TERM_STOPWORDS = {
    "about", "after", "also", "and", "answer", "are", "before",
    "both", "company", "contains", "could", "does", "final", "for",
    "format", "from", "give", "have", "into", "like", "mark", "one",
    "only", "our", "portal", "put", "question", "should", "single",
    "that", "the", "there", "this", "what", "when", "where", "which",
    "with", "work", "working",
}


@dataclass(frozen=True, slots=True)
class OperationalQueryPlan:
    roles: tuple[str, ...]
    terms: tuple[str, ...]


def compile_operational_facets(
    text: str,
    *,
    metadata: Mapping[str, Any] | None = None,
    limit: int = 64,
) -> tuple[dict[str, Any], ...]:
    """Extract conservative universal operational facets from text.

    The extractor intentionally requires explicit structure: controls,
    field assignments, stage chains, counts, related links, or explicit
    absence language. Plain prose usually returns no facets.
    """

    source = _source_text(text, metadata)
    facets: list[dict[str, Any]] = []

    for match in _CONTROL_PRICE_RE.finditer(source):
        label = _clean_label(match.group("label"))
        if not label:
            continue
        facets.append(_facet(
            role="delta",
            subrole="choice_price_delta",
            value=label,
            state=_state_value(match.group("state"), match.group("flag")),
            attributes={
                "control": match.group("control").casefold(),
                "amount": _numeric(match.group("amount")),
                "unit": "USD",
            },
            evidence_text=match.group(0),
        ))

    for match in _CONTROL_STATE_RE.finditer(source):
        label = _clean_control_state_label(match.group("label"))
        if not label:
            continue
        facets.append(_facet(
            role="state",
            subrole="choice_state",
            value=label,
            state=_state_value(match.group("state"), match.group("flag")),
            attributes={"control": match.group("control").casefold()},
            evidence_text=match.group(0),
        ))

    for match in _QUOTED_VALUE_RE.finditer(source):
        prop = _clean_label(match.group("property"))
        value = _clean_label(match.group("value"))
        if prop:
            facets.append(_facet(
                role="property",
                subrole="field_value",
                property=prop,
                value=value,
                evidence_text=match.group(0),
            ))

    for segment in _structured_segments(source):
        lowered = segment.casefold()
        if not any(marker in lowered for marker in ("field", "form", "option", "control", "value")):
            continue
        for match in _FIELD_ASSIGNMENT_RE.finditer(segment):
            prop = _clean_label(match.group("property"))
            value = _clean_label(match.group("value"))
            if not prop or prop.casefold() in {
                "checked",
                "selected",
                "domain",
                "environment",
                "goal",
                "url",
                "action",
                "value",
            }:
                continue
            prop_folded = prop.casefold()
            if prop_folded.endswith(" checked") or prop_folded.endswith(" selected"):
                continue
            facets.append(_facet(
                role="property",
                subrole="field_value",
                property=prop,
                value=value,
                evidence_text=match.group(0),
            ))

    for match in _FIELD_LIST_RE.finditer(source):
        subject = _clean_label(match.group("subject"))
        items = _split_labels(match.group("items"), limit=40)
        bottom = None
        bottom_match = _BOTTOM_OPTION_RE.search(match.group("items"))
        if bottom_match:
            bottom = _clean_label(bottom_match.group("value"))
            items = [item for item in items if "bottom_option" not in item.casefold()]
        if subject and items:
            facets.append(_facet(
                role="sequence",
                subrole="list_option_order",
                subject=subject,
                value="; ".join(items[:24]),
                attributes={"items": items[:40], "bottom_option": bottom},
                evidence_text=match.group(0),
            ))
        if subject and bottom:
            facets.append(_facet(
                role="state",
                subrole="list_bottom_option",
                subject=subject,
                property="bottom_option",
                value=bottom,
                evidence_text=match.group(0),
            ))

    for match in _PIPELINE_RE.finditer(source):
        chain = match.group("chain")
        stages = [
            {
                "name": _clean_label(stage.group("name")),
                "status": _clean_label(stage.group("status")),
            }
            for stage in _STAGE_ITEM_RE.finditer(chain)
        ]
        stages = [stage for stage in stages if stage["name"] and stage["status"]]
        if stages:
            facets.append(_facet(
                role="sequence",
                subrole="stage_chain",
                value="; ".join(f"{s['name']} ({s['status']})" for s in stages[:16]),
                attributes={"stages": stages[:24]},
                evidence_text=match.group(0),
            ))

    for match in _COUNT_RE.finditer(source):
        name = _clean_label(match.group("name"))
        count = int(match.group("count"))
        facets.append(_facet(
            role="count",
            subrole="observed_count",
            property=name,
            value=str(count),
            attributes={"count": count},
            evidence_text=match.group(0),
        ))

    for match in _RELATED_LINK_RE.finditer(source):
        labels = _split_labels(match.group("labels"), limit=12)
        for label in labels:
            facets.append(_facet(
                role="action",
                subrole="related_action",
                value=label,
                evidence_text=match.group(0),
            ))

    if _ABSENCE_RE.search(source):
        facets.append(_facet(
            role="invariant",
            subrole="explicit_absence",
            state="absent",
            evidence_text=_clip(_ABSENCE_RE.search(source).group(0), 180),  # type: ignore[union-attr]
        ))

    return tuple(_dedupe_facets(facets, limit=limit))


def enrich_operational_model_proposition(
    proposition: Mapping[str, Any],
    *,
    natural: str,
    metadata: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Return proposition enriched with operational facets when present."""

    out = dict(proposition)
    if _is_composite_container(out):
        return out

    facets = list(compile_operational_facets(natural, metadata=metadata))
    if not facets:
        return out

    existing = [
        dict(item)
        for item in out.get("operational_facets", [])
        if isinstance(item, Mapping)
    ]
    merged = _dedupe_facets([*existing, *facets], limit=96)
    roles = _dedupe_text(
        role
        for facet in merged
        for role in _facet_roles_for_index(facet)
    )
    tags = _dedupe_text([
        *[str(tag) for tag in out.get("domain_tags", []) if isinstance(tag, str)],
        "operations",
        "systems",
    ])
    out["operational_facet_schema"] = FACET_SCHEMA_VERSION
    out["operational_facets"] = merged
    out["operational_roles"] = roles
    out["domain_tags"] = tags
    return out


def infer_operational_query_plan(query: str) -> OperationalQueryPlan:
    """Infer universal operational roles and literal terms needed by a query."""

    text = str(query or "")
    folded = text.casefold()
    roles: list[str] = []

    def add_role(role: str) -> None:
        if role in UNIVERSAL_FACET_ROLES and role not in roles:
            roles.append(role)

    for markers, marker_roles in _QUERY_ROLE_MARKERS:
        if any(_marker_present(folded, marker) for marker in markers):
            for role in marker_roles:
                add_role(role)

    if not roles:
        return OperationalQueryPlan(roles=(), terms=())

    terms: list[str] = []

    def add_term(raw: str) -> None:
        clean = _term(raw)
        if clean and clean not in terms:
            terms.append(clean)

    for alternative in extract_query_alternatives(text, include_quoted=True):
        for term in alternative_terms(alternative):
            add_term(term)
    for token in re.findall(r"[a-z0-9][a-z0-9_+-]{2,}", folded):
        if token not in _TERM_STOPWORDS and not token.isdigit():
            add_term(token)
    for phrase in _phrase_terms(folded):
        add_term(phrase)

    return OperationalQueryPlan(roles=tuple(roles), terms=tuple(terms[:32]))


def _source_text(text: str, metadata: Mapping[str, Any] | None) -> str:
    parts = [str(text or "")]
    if metadata:
        for key in (
            "form_controls",
            "structured_ui_facts",
            "pipeline_items",
            "stage_chains",
            "ui_labels",
            "ui_labels_added",
            "ui_labels_removed",
        ):
            value = metadata.get(key)
            if value:
                parts.append(_flatten(value))
    return "\n".join(parts)


def _flatten(value: Any) -> str:
    if isinstance(value, Mapping):
        return "; ".join(f"{k}={_flatten(v)}" for k, v in value.items() if v)
    if isinstance(value, (list, tuple)):
        return "; ".join(_flatten(item) for item in value if item)
    return str(value)


def _facet(
    *,
    role: str,
    subrole: str,
    subject: str | None = None,
    property: str | None = None,
    value: str | None = None,
    state: str | None = None,
    attributes: Mapping[str, Any] | None = None,
    evidence_text: str = "",
) -> dict[str, Any]:
    out: dict[str, Any] = {
        "role": role,
        "subrole": subrole,
    }
    if subject:
        out["subject"] = _clip(subject, 160)
    if property:
        out["property"] = _clip(property, 160)
    if value:
        out["value"] = _clip(value, 240)
    if state:
        out["state"] = _clip(state, 80)
    attrs = {k: v for k, v in dict(attributes or {}).items() if v is not None}
    if attrs:
        out["attributes"] = attrs
    if evidence_text:
        out["evidence_text"] = _clip(evidence_text, 360)
    return out


def _dedupe_facets(facets: Iterable[Mapping[str, Any]], *, limit: int) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str, str, str, str]] = set()
    for raw in facets:
        facet = dict(raw)
        role = str(facet.get("role") or "")
        if role not in UNIVERSAL_FACET_ROLES:
            continue
        key = (
            role,
            str(facet.get("subrole") or ""),
            str(facet.get("subject") or "").casefold(),
            str(facet.get("property") or "").casefold(),
            str(facet.get("value") or "").casefold(),
            str(facet.get("state") or "").casefold(),
        )
        if key in seen:
            continue
        seen.add(key)
        out.append(facet)
        if len(out) >= limit:
            break
    return out


def _facet_roles_for_index(facet: Mapping[str, Any]) -> list[str]:
    roles: list[str] = []
    primary = str(facet.get("role") or "")
    if primary:
        roles.append(primary)
    subrole = str(facet.get("subrole") or "")
    if subrole:
        roles.append(subrole)
    if facet.get("subject"):
        roles.append("object")
    if facet.get("property"):
        roles.append("property")
    if facet.get("value"):
        roles.append("value")
    if facet.get("state"):
        roles.append("state")
    attrs = facet.get("attributes")
    if isinstance(attrs, Mapping):
        if "amount" in attrs:
            roles.append("delta")
            roles.append("value")
        if "count" in attrs:
            roles.append("count")
            roles.append("value")
    return roles


def _is_composite_container(proposition: Mapping[str, Any]) -> bool:
    return (
        proposition.get("claim_role") == "situation"
        or proposition.get("legacy_kind") == "situation"
        or proposition.get("abstraction_level") == "composite"
    )


def _structured_segments(text: str) -> list[str]:
    return [
        segment.strip()
        for segment in re.split(r"[|\n]", text)
        if segment and len(segment.strip()) <= 1400
    ]


def _split_labels(raw: str, *, limit: int) -> list[str]:
    labels = []
    for item in re.split(r"\s*;\s*|\s*,\s*", str(raw or "")):
        label = _clean_label(item)
        if label and label not in labels:
            labels.append(label)
        if len(labels) >= limit:
            break
    return labels


def _clean_label(raw: str) -> str:
    clean = re.sub(r"[\uf000-\uffff]", " ", str(raw or ""))
    clean = " ".join(clean.strip().strip("'\"").split())
    clean = clean.strip(" .,:;!?")
    return clean


def _clean_control_state_label(raw: str) -> str:
    without_delta = re.sub(
        r"\s*\[add\s+\$[^\]]+\]\s*$",
        "",
        str(raw or ""),
        flags=re.IGNORECASE,
    )
    return _clean_label(without_delta)


def _state_value(state_word: str, flag: str) -> str:
    base = "checked" if state_word.casefold() == "checked" else "selected"
    return base if flag.casefold() == "true" else f"un{base}"


def _numeric(raw: str) -> int | float:
    value = float(raw)
    if value.is_integer():
        return int(value)
    return value


def _term(raw: str) -> str:
    clean = re.sub(r"[^a-z0-9_+ -]+", " ", str(raw or "").casefold())
    clean = " ".join(clean.split())
    if len(clean) < 3 or clean in _TERM_STOPWORDS:
        return ""
    return clean


def _marker_present(text: str, marker: str) -> bool:
    marker_text = " ".join(str(marker or "").casefold().split())
    if not marker_text:
        return False
    pattern = r"(?<![a-z0-9])" + re.escape(marker_text) + r"(?![a-z0-9])"
    return re.search(pattern, text) is not None


def _phrase_terms(text: str) -> list[str]:
    tokens = [
        token
        for token in re.findall(r"[a-z0-9][a-z0-9_+-]{2,}", text.casefold())
        if token not in _TERM_STOPWORDS and not token.isdigit()
    ]
    out: list[str] = []
    for width in (3, 2):
        for idx in range(0, max(0, len(tokens) - width + 1)):
            phrase = " ".join(tokens[idx : idx + width])
            if phrase not in out:
                out.append(phrase)
    return out[:10]


def _dedupe_text(values: Iterable[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for value in values:
        clean = str(value or "").strip().lower().replace(" ", "_")
        if clean and clean not in seen:
            seen.add(clean)
            out.append(clean)
    return out


def _clip(text: str, limit: int) -> str:
    clean = " ".join(str(text or "").split())
    if len(clean) <= limit:
        return clean
    return clean[: max(0, limit - 3)].rstrip() + "..."


__all__ = [
    "FACET_SCHEMA_VERSION",
    "OperationalQueryPlan",
    "UNIVERSAL_FACET_ROLES",
    "compile_operational_facets",
    "enrich_operational_model_proposition",
    "infer_operational_query_plan",
]
