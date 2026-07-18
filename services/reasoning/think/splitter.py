"""services/reasoning/think/splitter.py — atomic-model splitter.

Decomposes compound LLM-proposed `ClaimOp` entries into a list of
atomic primitives plus (when warranted) a synthesized composing
`situation` ClaimOp. Pure text heuristics — NO LLM call — so the
splitter is cheap, deterministic, and trivially testable.

Why this exists
---------------
The model-layer probe found single-row claims that pack 3-4 distinct
beliefs into one entry, e.g.:

  "HarborRail procurement evidence is delayed, sponsor confidence is
   dropping, ARR is at risk, and security review needs SOC2/data
   residency evidence"

Such compound rows collapse under embedding-based dedupe, prevent
meaningful adjudication, and ruin reconciliation. Splitting them
BEFORE reconciliation runs lets each atomic claim land on its own
Model row and be reconciled / adjudicated independently. The
synthesized `situation` carries the compositional meaning so the
"these things are jointly true" signal isn't lost.

Integration pattern (the integrator wires this; do NOT change applier
in this PR):

    In services/reasoning/think/applier.py:_apply_diff(), BEFORE the per-claim_op loop:
        all_ops = []
        for op in validated.claim_ops:
            all_ops.extend(split_compound_claim_op(op))
        for op in all_ops:
            reconcile + quality_gate + apply
        AFTER inserts: resolve member_model_pending situations using the
        newly inserted atomic model IDs in the same diff.

Heuristics (all text-based, no LLM):

  * Top-level conjunction splits: " and ", ", and ", "; ", ", while ",
    ", with ", ", which means " — each conjunct must contain a
    distinct subject-verb-object pattern (rough check: each piece has
    at least one verb token).
  * Multi-kind signals: text simultaneously expresses STATE (factual
    present) AND CONCERN (worry / risk / blocker) AND/OR PREDICTION
    (future / will / by-date).
  * Compound entity scope: same entry naming >=3 distinct named
    actors / customers / workstreams in scope.

When NONE of these trigger, the splitter returns `[op]` unchanged.
False-negative is safer than over-splitting; under-confident heuristics
yield `[op]`.

Public API:
    split_compound_claim_op(op)         -> list[ClaimOp]
    is_compound(entry)                  -> tuple[bool, list[str]]

The synthesized `situation` ClaimOp uses `member_model_pending=True`
in its `entry` so the integrator can patch member_model_ids after the
atomic inserts complete in the same diff.
"""
from __future__ import annotations

import re
from copy import deepcopy
from typing import Any

from services.reasoning.synthesis.operational_facets import compile_operational_facets

from .diff_schema import ClaimOp


# ---------------------------------------------------------------------
# Tunables
# ---------------------------------------------------------------------

# Conjunction separators evaluated as splits at the top level of the
# claim text. Order matters: longer / more specific separators MUST
# come first so a stray " and " inside ", and " doesn't double-split.
_CONJ_SEPARATORS: tuple[str, ...] = (
    ", which means ",
    ", with ",
    ", while ",
    ", and ",
    "; ",
    " and ",
)

# Rough English verb-ish tokens used as a "this conjunct contains a
# distinct claim" sanity check. We deliberately keep this list short
# and broad — false negative (no split) is better than over-splitting.
_VERB_HINTS: frozenset[str] = frozenset({
    "is", "are", "was", "were", "be", "been", "being",
    "has", "have", "had",
    "needs", "need", "needed",
    "wants", "want",
    "delayed", "dropping", "dropped", "rising", "rose", "falling", "fell",
    "at risk", "at-risk",
    "missing", "missed",
    "blocked", "blocking",
    "approved", "approves", "approve",
    "rejected", "rejects", "reject",
    "shipped", "ships", "ship", "shipping",
    "delivered", "delivers", "deliver", "delivering",
    "will", "would", "should", "must", "may", "might",
    "expected", "expects", "expect",
    "completed", "completes", "complete",
    "review", "reviewed", "reviewing",
    "requires", "require", "required",
    "lacks", "lacking", "lacks",
    "happened", "happens", "happen", "happening",
    "occurred", "occurs", "occur",
    "remains", "unresolved", "moved", "worsening",
    "increased", "decreased", "improved", "degraded",
    "churning", "churned", "churn",
    "concerned", "worried",
    "started", "starts", "start", "starting",
    "stopped", "stops", "stop", "stopping",
    "fails", "fail", "failed", "failing",
    "passes", "pass", "passed", "passing",
})

# Keyword maps for proposition-kind detection. Each entry is a regex
# pattern matched case-insensitively against the full claim text.
_STATE_PATTERNS: tuple[re.Pattern[str], ...] = tuple(
    re.compile(p, re.IGNORECASE) for p in (
        r"\bis\b", r"\bare\b", r"\bhas been\b", r"\bhave been\b",
        r"\bshipped\b", r"\bdelivered\b", r"\bcompleted\b",
        r"\bapproved\b", r"\bmerged\b", r"\bdeployed\b",
    )
)

_CONCERN_PATTERNS: tuple[re.Pattern[str], ...] = tuple(
    re.compile(p, re.IGNORECASE) for p in (
        r"\bat risk\b", r"\bat-risk\b", r"\brisk\b", r"\bworried\b",
        r"\bconcern(ed|ing)?\b", r"\bblock(ed|er|ing)\b",
        r"\bchurn(ing|ed)?\b", r"\bdrop(ping|ped)?\b",
        r"\bmissing\b", r"\bdelayed\b", r"\bslipping\b",
        r"\bdeclin(e|ing|ed)\b", r"\bfailing\b", r"\bbroken\b",
        r"\bovers(told|sold)\b",
    )
)

_PREDICTION_PATTERNS: tuple[re.Pattern[str], ...] = tuple(
    re.compile(p, re.IGNORECASE) for p in (
        r"\bwill\b", r"\bwould\b", r"\bshould\b", r"\bmust\b",
        r"\bby (next|the|tomorrow|monday|tuesday|wednesday|thursday|"
        r"friday|saturday|sunday|q[1-4]|january|february|march|april|"
        r"may|june|july|august|september|october|november|december)\b",
        r"\beta\b", r"\bdue\b", r"\bexpect(ed|s|ing)?\b",
        r"\bplan(ned|s|ning)?\b", r"\btarget(ed|s|ing)?\b",
        r"\bforecast(ed|s|ing)?\b",
    )
)

# Pressure-type keyword heuristics (best-effort). The ordering of
# checks below decides ties; revenue / compliance are checked first
# because they have the most distinctive vocabulary.
_PRESSURE_HEURISTICS: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("revenue", (
        "arr", "mrr", "revenue", "churn", "renewal", "expansion",
        "deal", "pipeline", "quota", "bookings", "upsell",
    )),
    ("compliance", (
        "soc2", "soc 2", "iso27001", "iso 27001", "gdpr", "hipaa",
        "pci", "audit", "data residency", "compliance", "regulatory",
        "policy", "legal review",
    )),
    ("trust", (
        "sponsor", "confidence", "trust", "credibility", "reputation",
        "satisfaction", "nps", "csat", "frustrat", "complain",
    )),
    ("capacity", (
        "overloaded", "overload", "burnout", "burnt out", "stretched",
        "capacity", "bandwidth", "headcount", "understaffed", "backlog",
        "throughput", "swamped", "saturated",
    )),
    ("compliance", (  # second look — narrower forms
        "evidence", "attestation", "questionnaire",
    )),
    ("execution", (
        "delayed", "slipping", "blocked", "blocker", "stalled",
        "behind schedule", "ship", "delivery", "deadline",
        "missed", "rework",
    )),
    ("decision", (
        "decision", "decide", "approval", "approve", "tradeoff",
        "trade-off", "go/no-go", "go no go",
    )),
    ("market", (
        "competitor", "competitive", "market", "incumbent",
        "alternative", "displaced", "rfp",
    )),
    ("resource", (
        "budget", "spend", "cost", "investment", "hire", "hiring",
        "vendor", "license", "quota",
    )),
)

_OPERATIONAL_SPLIT_SUBROLES: frozenset[str] = frozenset({
    "choice_price_delta",
    "choice_state",
    "field_value",
    "list_option_order",
    "list_bottom_option",
    "stage_chain",
    "observed_count",
    "related_action",
    "explicit_absence",
})

_UNSPLITTABLE_CURIOSITY_TAGS: frozenset[str] = frozenset({
    "curiosity",
    "coverage_curiosity",
    "open_question",
    "operating_question",
    "strategic_question",
    "unresolved_unknown",
    "success_driver",
})

_PREDICATE_ROLE_TERMS: tuple[tuple[str, frozenset[str]], ...] = (
    ("absence", frozenset({"missing", "unresolved", "unclear", "unassigned", "unowned", "absent", "lacks", "lacking"})),
    ("movement", frozenset({"moved", "delayed", "slipped", "postponed", "rescheduled", "drifted"})),
    ("incomplete", frozenset({"incomplete", "partial", "stale", "inaccurate", "optimistic"})),
    ("questioned", frozenset({"whether", "questioned", "uncertain", "unverified", "asks"})),
    ("transfer", frozenset({"handoff", "transition", "transfer", "assigned", "received"})),
    ("deterioration", frozenset({"worsening", "dropping", "declining", "degraded", "rising"})),
)
_CAUSAL_MARKERS = (
    " because ", " causes ", " causing ", " caused ", " due to ",
    " leads to ", " resulting in ", " is delaying ", " affects ", " affecting ",
)
_EVIDENCE_MATCH_STOPWORDS = {
    "again", "after", "and", "are", "because", "from", "has", "have",
    "into", "may", "remains", "signal", "signals", "still", "that", "the",
    "their", "these", "this", "update", "while", "with", "workstream",
}


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------


def _claim_text(entry: dict[str, Any]) -> str:
    """Pull the most representative human-text from an entry."""
    text_bits: list[str] = []
    natural = entry.get("natural")
    if isinstance(natural, str) and natural.strip():
        text_bits.append(natural.strip())
    prop = entry.get("proposition")
    if isinstance(prop, dict):
        for key in (
            "assertion", "nature", "summary", "situation",
            "hypothesis_text", "assessment", "observed_tendency",
            "expected",
        ):
            value = prop.get(key)
            if isinstance(value, str) and value.strip():
                text_bits.append(value.strip())
    # Dedupe but keep order.
    seen: set[str] = set()
    out: list[str] = []
    for bit in text_bits:
        if bit not in seen:
            seen.add(bit)
            out.append(bit)
    return " | ".join(out)


def _conjunct_has_verb(piece: str) -> bool:
    """True iff `piece` contains a verb-ish token suggesting a clause."""
    lowered = " " + piece.lower().strip() + " "
    for hint in _VERB_HINTS:
        # Multi-word hints like "at risk" must match with word boundaries.
        token = f" {hint} "
        if token in lowered:
            return True
    return False


def _split_top_level(text: str) -> list[str]:
    """
    Greedy top-level split on conjunction separators. Returns a list of
    >= 1 conjuncts. Each conjunct is stripped; empty conjuncts are
    dropped. Separator order matters (see _CONJ_SEPARATORS).

    Multi-text payloads (the helper joins natural + proposition fields
    with " | ") are first split on the joiner so duplicate restatements
    don't double-count.
    """
    if not text or not text.strip():
        return []
    pieces: list[str] = [p for p in text.split(" | ") if p.strip()]
    # Dedupe restatements across the natural / proposition.assertion
    # text views. The joined " | " input often contains the same claim
    # twice (e.g. natural="x ships", proposition.assertion="ships"),
    # which would otherwise be counted as two conjuncts. Drop any piece
    # that is a substring of a longer piece in the same group.
    if len(pieces) > 1:
        deduped: list[str] = []
        for p in pieces:
            p_norm = p.lower().strip()
            redundant = any(
                q is not p and p_norm in q.lower().strip()
                and len(p_norm) < len(q.lower().strip())
                for q in pieces
            )
            if not redundant:
                deduped.append(p)
        if deduped:
            pieces = deduped
    for sep in _CONJ_SEPARATORS:
        next_pieces: list[str] = []
        for piece in pieces:
            if sep in piece:
                next_pieces.extend(piece.split(sep))
            else:
                next_pieces.append(piece)
        pieces = next_pieces

    # Last-resort: split on bare commas IF each resulting sub-conjunct
    # still contains a verb-hint token. This catches "X is Y, Z is W,
    # ..., and V" style enumerations where the LLM only used "and"
    # before the final element. We deliberately gate by verb-presence
    # to avoid breaking apart noun-phrase lists like "foo, bar, baz".
    expanded: list[str] = []
    for piece in pieces:
        if ", " in piece:
            sub_pieces = [s.strip() for s in piece.split(", ") if s.strip()]
            if (
                len(sub_pieces) >= 2
                and all(_conjunct_has_verb(s) for s in sub_pieces)
            ):
                expanded.extend(sub_pieces)
                continue
        expanded.append(piece)
    return [p.strip() for p in expanded if p.strip()]


def _kind_signals(text: str) -> set[str]:
    """Return the proposition kinds the text appears to express."""
    lowered = text
    found: set[str] = set()
    for pat in _STATE_PATTERNS:
        if pat.search(lowered):
            found.add("state")
            break
    for pat in _CONCERN_PATTERNS:
        if pat.search(lowered):
            found.add("concern")
            break
    for pat in _PREDICTION_PATTERNS:
        if pat.search(lowered):
            found.add("prediction")
            break
    return found


def _distinct_entities(entry: dict[str, Any]) -> set[str]:
    """Approximate set of distinct named entities in scope."""
    out: set[str] = set()
    for actor in entry.get("scope_actors") or []:
        if actor:
            out.add(f"actor:{actor}")
    for ent in entry.get("scope_entities") or []:
        if isinstance(ent, dict):
            eid = ent.get("id")
            if eid:
                out.add(f"entity:{eid}")
    # Also pick up proper-noun-ish words in the claim text, capped to
    # avoid false positives (acronyms / sentence starts).
    text = _claim_text(entry)
    for token in re.findall(r"\b[A-Z][a-zA-Z0-9]{2,}\b", text):
        # Filter common sentence-starter words that look proper-nouny.
        if token.lower() in {
            "the", "and", "but", "however", "additionally",
            "meanwhile", "also", "further",
        }:
            continue
        out.add(f"name:{token}")
    return out


def _infer_pressure_type(text: str) -> str | None:
    """Best-effort pressure_type mapping from claim text."""
    lowered = text.lower()
    for pressure, keywords in _PRESSURE_HEURISTICS:
        for kw in keywords:
            if kw in lowered:
                return pressure
    return None


def _trim(s: str, limit: int) -> str:
    s = s.strip()
    if len(s) <= limit:
        return s
    return s[: limit - 3].rstrip() + "..."


def _normalized_semantic_tokens(text: str) -> set[str]:
    tokens: set[str] = set()
    for raw in re.findall(r"[a-z0-9]+", text.casefold()):
        token = raw
        for suffix in ("ship", "ing", "ed", "s"):
            if len(token) > len(suffix) + 3 and token.endswith(suffix):
                token = token[: -len(suffix)]
                break
        tokens.add(token)
    return tokens


def _predicate_roles(text: str) -> set[str]:
    lowered = " ".join(text.casefold().split())
    tokens = set(re.findall(r"[a-z0-9]+", lowered))
    if "without naming" in lowered or "they have it" in lowered:
        return {"ambiguous_reference"}
    roles = {
        role
        for role, terms in _PREDICATE_ROLE_TERMS
        if tokens & terms
    }
    if "no clearly recorded" in lowered or "no recorded" in lowered:
        roles.add("absence")
    # Prefer the most discriminative state predicate. Words such as
    # "handoff" often name the workstream/object rather than asserting that a
    # transfer occurred, so they must not make every local row compatible.
    for primary in (
        "absence",
        "movement",
        "incomplete",
        "questioned",
        "deterioration",
    ):
        if primary in roles:
            return {primary}
    return roles


def _evidence_material_tokens(text: str) -> set[str]:
    predicate_tokens = _normalized_semantic_tokens(
        " ".join(term for _, terms in _PREDICATE_ROLE_TERMS for term in terms)
    )
    return {
        token
        for token in _normalized_semantic_tokens(text)
        if len(token) >= 4 and token not in _EVIDENCE_MATCH_STOPWORDS
        and token not in predicate_tokens
    }


def _atomic_evidence_matches(claim: str, body: str) -> bool:
    claim_roles = _predicate_roles(claim)
    body_roles = _predicate_roles(body)
    if not claim_roles or not (claim_roles & body_roles):
        return False
    claim_objects = _evidence_material_tokens(claim)
    body_objects = _evidence_material_tokens(body)
    return bool(claim_objects & body_objects)


def _is_causal_atomic(claim: str) -> bool:
    lowered = f" {claim.casefold()} "
    return any(marker in lowered for marker in _CAUSAL_MARKERS)


def _derived_atomic_evidence(
    entry: dict[str, Any],
    claim: str,
    manifest_by_id: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    derivations = entry.get("evidence_derivations")
    if not isinstance(derivations, list):
        return []
    for raw in derivations:
        if not isinstance(raw, dict):
            continue
        claim_pattern = str(raw.get("claim_pattern") or "").casefold().strip()
        if claim_pattern and claim_pattern not in claim.casefold():
            continue
        groups = raw.get("premise_groups")
        if not isinstance(groups, list):
            continue
        roles: set[str] = set()
        ids: list[str] = []
        for group in groups:
            if not isinstance(group, dict):
                continue
            role = str(group.get("role") or "").strip()
            group_ids = [
                str(value) for value in group.get("observation_ids") or []
                if str(value) in manifest_by_id
            ]
            if role and group_ids:
                roles.add(role)
                ids.extend(group_ids)
        if {"cause", "effect"} <= roles:
            return [manifest_by_id[value] for value in dict.fromkeys(ids)]
    return []


def _redistribute_atomic_evidence(entry: dict[str, Any], claim: str) -> bool:
    """Keep only observations that positively support this split atomic.

    Absence of a manifest means this is a legacy/non-compiled claim and retains
    its existing evidence. A present manifest is authoritative: zero matches
    quarantines the atomic instead of widening back to its parent batch.
    """
    manifest = entry.get("evidence_observation_manifest")
    if not isinstance(manifest, list):
        return True
    manifest_by_id = {
        str(row["observation_id"]): dict(row)
        for row in manifest
        if isinstance(row, dict) and row.get("observation_id")
    }
    if _closed_atomic_singleton_manifest(entry, claim, manifest_by_id):
        matched = list(manifest_by_id.values())
    elif _is_causal_atomic(claim):
        matched = _derived_atomic_evidence(entry, claim, manifest_by_id)
    else:
        matched = [
            row for row in manifest_by_id.values()
            if _atomic_evidence_matches(claim, str(row.get("body") or ""))
        ]
    if not matched:
        return False
    matched_ids = list(
        dict.fromkeys(str(row["observation_id"]) for row in matched)
    )
    entry["evidence_observation_manifest"] = matched
    entry["supporting_event_ids"] = matched_ids
    proposition = entry.get("proposition")
    if isinstance(proposition, dict):
        proposition["evidence_event_ids"] = matched_ids
    return True


def _closed_atomic_singleton_manifest(
    entry: dict[str, Any],
    claim: str,
    manifest_by_id: dict[str, dict[str, Any]],
) -> bool:
    """Preserve exact compiler-entailed singleton evidence through splitting.

    The marker alone is insufficient: the claim must remain byte-semantically
    identical after whitespace/case normalization. Split or derived atomics
    therefore cannot borrow the parent's authorization.
    """

    proposition = entry.get("proposition")
    if not isinstance(proposition, dict):
        return False
    contract = proposition.get("closed_atomic_contract")
    if not isinstance(contract, dict) or contract != {
        "version": "v1",
        "compiler_entails_exact_text": True,
        "evidence_cardinality": "singleton",
    }:
        return False
    if len(manifest_by_id) != 1:
        return False
    body = str(next(iter(manifest_by_id.values())).get("body") or "")

    def normalize(value: Any) -> str:
        return " ".join(str(value).casefold().split())

    return bool(normalize(claim)) and normalize(claim) == normalize(body)


def _composite_is_necessary(entry: dict[str, Any]) -> bool:
    """Require an explicit emergent judgment, not merely multiple clauses."""
    proposition = entry.get("proposition")
    if not isinstance(proposition, dict):
        return False
    explicit_situation = proposition.get("claim_role") == "situation"
    mechanism = str(
        proposition.get("shared_mechanism")
        or proposition.get("emergent_mechanism")
        or proposition.get("judgment_change")
        or ""
    ).strip()
    derivation = str(
        proposition.get("composite_derivation")
        or proposition.get("relationship_summary")
        or ""
    ).strip()
    emergent = " ".join((mechanism, derivation)).casefold()
    emergent_markers = (
        "compounds", "feedback", "jointly", "mechanism", "reinforces",
        "tradeoff", "together",
    )
    return bool(
        derivation
        and (explicit_situation or mechanism)
        and any(marker in emergent for marker in emergent_markers)
    )


def _allocate_unsplit_atomic(op: ClaimOp) -> list[ClaimOp]:
    entry = op.entry
    if not isinstance(entry, dict) or not isinstance(
        entry.get("evidence_observation_manifest"), list
    ):
        return [op]
    proposition = entry.get("proposition")
    proposition = proposition if isinstance(proposition, dict) else {}
    if (
        proposition.get("claim_role") in {"pattern", "situation", "recommendation"}
        or proposition.get("abstraction_level") in {"pattern", "composite"}
    ):
        return [op]
    allocated_entry = deepcopy(entry)
    if not _redistribute_atomic_evidence(
        allocated_entry,
        _claim_text(allocated_entry),
    ):
        return []
    return [ClaimOp(op="insert", entry=allocated_entry)]


def _is_unsplittable_proposition(prop: Any) -> bool:
    """Return True for proposition roles the splitter must preserve."""
    if not isinstance(prop, dict):
        return False
    kind = prop.get("kind")
    claim_role = prop.get("claim_role")
    legacy_kind = prop.get("legacy_kind")
    abstraction = prop.get("abstraction_level")
    tags = _normalized_tags(
        prop.get("domain_tags"),
        prop.get("retrieval_tags"),
        prop.get("coverage_roles"),
    )
    if (
        claim_role == "pattern"
        or abstraction == "pattern"
        or tags & {"source_digest", "discovered_pattern", "major_source_window"}
    ):
        return True
    if claim_role == "hypothesis" and tags & _UNSPLITTABLE_CURIOSITY_TAGS:
        return True
    is_situation = (
        kind == "situation"
        or claim_role == "situation"
        or legacy_kind == "situation"
    )
    if is_situation:
        members = prop.get("member_model_ids")
        return isinstance(members, list) and len(members) >= 2
    return (
        kind in {"recommendation", "norm"}
        or claim_role in {"recommendation"}
        or legacy_kind in {"recommendation"}
    )


def _is_compound_reporting_exempt(prop: Any) -> bool:
    """Return True when scoring should preserve a compact pattern as atomic."""
    if not isinstance(prop, dict):
        return False
    claim_role = prop.get("claim_role")
    abstraction = prop.get("abstraction_level")
    tags = _normalized_tags(
        prop.get("domain_tags"),
        prop.get("retrieval_tags"),
        prop.get("coverage_roles"),
    )
    return bool(
        claim_role == "pattern"
        or abstraction == "pattern"
        or tags & {"source_digest", "discovered_pattern", "major_source_window"}
        or (claim_role == "hypothesis" and tags & _UNSPLITTABLE_CURIOSITY_TAGS)
    )


def _normalized_tags(*groups: Any) -> set[str]:
    tags: set[str] = set()
    for group in groups:
        if group is None:
            continue
        values = group if isinstance(group, (list, tuple, set)) else (group,)
        for raw in values:
            tag = re.sub(r"[^a-z0-9_]+", "_", str(raw).strip().casefold()).strip("_")
            if tag:
                tags.add(tag)
    return tags


def _operational_source_text(entry: dict[str, Any]) -> str:
    """Text source for evidence-backed operational splitting."""
    natural = entry.get("natural")
    if isinstance(natural, str) and natural.strip():
        return natural.strip()
    return _claim_text(entry)


def _operational_facet_groups(entry: dict[str, Any]) -> list[list[dict[str, Any]]]:
    """Return independent operational fact groups found in the entry.

    The grouping rule is product-level, not domain-level: each group is
    one explicit evidence span that should be independently confirmable
    and retrievable as a Model. Closely coupled facets from the same
    evidence span, such as an option's price delta and checked state, stay
    together as one atomic belief.
    """
    source = _operational_source_text(entry)
    if not source:
        return []

    facets = [
        dict(facet)
        for facet in compile_operational_facets(source, limit=128)
        if isinstance(facet, dict)
    ]
    if len(facets) < 2:
        return []

    price_evidence = {
        str(facet.get("evidence_text") or "")
        for facet in facets
        if facet.get("subrole") == "choice_price_delta"
        and facet.get("evidence_text")
    }
    list_order_evidence = {
        str(facet.get("evidence_text") or "")
        for facet in facets
        if facet.get("subrole") == "list_option_order"
        and facet.get("evidence_text")
    }

    groups_by_key: dict[tuple[str, str, str, str], list[dict[str, Any]]] = {}
    for facet in facets:
        subrole = str(facet.get("subrole") or "")
        if subrole not in _OPERATIONAL_SPLIT_SUBROLES:
            continue
        if _is_noisy_operational_facet(facet):
            continue
        evidence = str(facet.get("evidence_text") or "")

        # A priced option also compiles into a state facet. Keep that
        # state facet in the same option group instead of creating a
        # second Model for the same evidence.
        if subrole == "choice_state" and evidence in price_evidence:
            key = _operational_group_key({
                **facet,
                "subrole": "choice_price_delta",
            })
        # Bottom-option is a property of the observed list order. Keep
        # it with the list order when the same evidence span is present.
        elif subrole == "list_bottom_option" and evidence in list_order_evidence:
            key = _operational_group_key({
                **facet,
                "subrole": "list_option_order",
            })
        else:
            key = _operational_group_key(facet)
        groups_by_key.setdefault(key, []).append(facet)

    groups = list(groups_by_key.values())
    groups = [group for group in groups if _operational_group_text(group)]
    if len(groups) < 2:
        return []
    return groups


def _is_noisy_operational_facet(facet: dict[str, Any]) -> bool:
    """Filter parser artifacts that are not standalone beliefs."""
    if facet.get("subrole") != "field_value":
        return False
    prop = str(facet.get("property") or "").strip().casefold()
    evidence = str(facet.get("evidence_text") or "").strip().casefold()
    if prop in {"checked", "selected", "value"}:
        return True
    if prop.endswith(" checked") or prop.endswith(" selected"):
        return True
    return evidence.startswith("value=")


def _operational_group_key(facet: dict[str, Any]) -> tuple[str, str, str, str]:
    subrole = str(facet.get("subrole") or "")
    evidence = str(facet.get("evidence_text") or "")
    if subrole in {"choice_price_delta", "choice_state"}:
        identity = evidence.casefold() or str(facet.get("value") or "").casefold()
        return (
            "choice",
            identity,
            "",
            str((facet.get("attributes") or {}).get("control") or "").casefold()
            if isinstance(facet.get("attributes"), dict)
            else "",
        )
    if subrole in {"list_option_order", "list_bottom_option"}:
        return (
            "list",
            evidence.casefold(),
            str(facet.get("subject") or "").casefold(),
            str(facet.get("property") or "").casefold(),
        )
    if subrole == "related_action":
        return (
            subrole,
            str(facet.get("value") or "").casefold(),
            evidence.casefold(),
            "",
        )
    return (
        subrole,
        str(facet.get("subject") or "").casefold(),
        str(facet.get("property") or "").casefold(),
        str(facet.get("value") or evidence).casefold(),
    )


def _operational_group_text(group: list[dict[str, Any]]) -> str:
    for facet in group:
        evidence = str(facet.get("evidence_text") or "").strip()
        if evidence:
            return evidence
    return ""


def _operational_group_subject(
    group: list[dict[str, Any]],
    original_prop: Any,
) -> str:
    for facet in group:
        for key in ("subject", "property", "value"):
            value = facet.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
    if isinstance(original_prop, dict):
        for key in ("subject", "about", "signature"):
            value = original_prop.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
    return "operational fact"


def _operational_atomic_natural(group: list[dict[str, Any]]) -> str:
    """Human-readable natural text for one operational atomic fact."""
    primary = group[0]
    subrole = str(primary.get("subrole") or "")
    evidence = _operational_group_text(group)
    attrs = (
        primary.get("attributes")
        if isinstance(primary.get("attributes"), dict)
        else {}
    )

    if subrole == "choice_price_delta":
        label = str(primary.get("value") or "option").strip()
        amount = attrs.get("amount")
        unit = str(attrs.get("unit") or "USD").strip() or "USD"
        state = str(primary.get("state") or "").strip()
        detail = f"{label} adds {amount} {unit}" if amount is not None else label
        if state:
            detail += f" and is {state}"
        return _operational_sentence(detail, evidence)

    if subrole == "choice_state":
        label = str(primary.get("value") or "option").strip()
        state = str(primary.get("state") or "observed").strip()
        control = str(attrs.get("control") or "option").strip()
        return _operational_sentence(f"{control} {label} is {state}", evidence)

    if subrole == "field_value":
        prop = str(primary.get("property") or "field").strip()
        value = str(primary.get("value") or "").strip()
        return _operational_sentence(f"{prop} value is {value!r}", evidence)

    if subrole == "list_option_order":
        subject = str(primary.get("subject") or "list").strip()
        bottom = ""
        for facet in group:
            if facet.get("subrole") == "list_bottom_option" and facet.get("value"):
                bottom = f"; bottom option is {facet['value']}"
                break
        return _operational_sentence(
            f"{subject} option order is observed{bottom}",
            evidence,
        )

    if subrole == "list_bottom_option":
        subject = str(primary.get("subject") or "list").strip()
        value = str(primary.get("value") or "unknown").strip()
        return _operational_sentence(f"{subject} bottom option is {value}", evidence)

    if subrole == "stage_chain":
        return _operational_sentence("workflow stage chain is observed", evidence)

    if subrole == "observed_count":
        prop = str(primary.get("property") or "count").strip()
        value = str(primary.get("value") or "").strip()
        return _operational_sentence(f"{prop} is {value}", evidence)

    if subrole == "related_action":
        value = str(primary.get("value") or "related action").strip()
        return _operational_sentence(f"related action {value} is visible", evidence)

    if subrole == "explicit_absence":
        return _operational_sentence("explicit absence is observed", evidence)

    return _operational_sentence(evidence or "operational fact is observed", evidence)


def _operational_sentence(summary: str, evidence: str) -> str:
    summary = summary.strip().rstrip(".")
    evidence = evidence.strip().rstrip(".")
    if evidence and evidence.casefold() not in summary.casefold():
        return f"{summary}. Evidence: {evidence}."
    return f"{summary}."


def _operational_atomic_proposition(
    group: list[dict[str, Any]],
    original_prop: Any,
) -> dict[str, Any]:
    natural = _operational_atomic_natural(group)
    subject = _operational_group_subject(group, original_prop)
    return {
        "kind": "belief",
        "claim_role": "fact",
        "abstraction_level": "atomic",
        "time_mode": "current",
        "modality": "observed",
        "polarity": "neutral",
        "subject": subject,
        "assertion": natural.rstrip("."),
        "operational_split_source": "universal_facets",
    }


def _split_operational_claim_op(entry: dict[str, Any]) -> list[ClaimOp]:
    groups = _operational_facet_groups(entry)
    if len(groups) < 2:
        return []

    base_entry = deepcopy(entry)
    base_entry.pop("embedding", None)
    split_ops: list[ClaimOp] = []
    for group in groups:
        atomic_entry = deepcopy(base_entry)
        natural = _operational_atomic_natural(group)
        atomic_entry["natural"] = natural
        atomic_entry["proposition"] = _operational_atomic_proposition(
            group,
            base_entry.get("proposition"),
        )
        atomic_entry.pop("embedding", None)
        if _redistribute_atomic_evidence(atomic_entry, natural):
            split_ops.append(ClaimOp(op="insert", entry=atomic_entry))

    text = _claim_text(entry)
    pressure_type = _infer_pressure_type(text) or "execution"
    trimmed = _trim(text, 200)
    situation_entry = deepcopy(base_entry)
    situation_entry.pop("embedding", None)
    situation_entry["proposition"] = {
        "kind": "belief",
        "claim_role": "situation",
        "abstraction_level": "composite",
        "time_mode": "current",
        "modality": "observed",
        "polarity": "mixed",
        "situation": trimmed,
        "summary": trimmed,
        "member_model_ids": [],
        "relationship_summary": (
            "Atomic operational facts split from one structured Model entry; "
            "they are jointly true in the same observed context."
        ),
        "status": "forming",
        "shared_mechanism": trimmed,
        "pressure_type": pressure_type,
    }
    if base_entry.get("supporting_event_ids"):
        situation_entry["proposition"]["evidence_event_ids"] = list(
            base_entry["supporting_event_ids"]
        )
    situation_entry["natural"] = f"Composite operational situation: {trimmed}"
    situation_entry["member_model_pending"] = True
    situation_entry["split_reasons"] = [
        f"multi_operational_facts:{len(groups)}",
    ]
    if _composite_is_necessary(entry):
        split_ops.append(ClaimOp(op="insert", entry=situation_entry))
    return split_ops


# ---------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------


def is_compound(entry: dict[str, Any]) -> tuple[bool, list[str]]:
    """
    Return (is_compound, reasons).

    `entry` is the insert-payload dict (the same shape as
    `ClaimOp.entry` for `op='insert'`). The function never touches the
    network; it only reads text fields and scope.

    Reasons returned (any combination):
      * "multi_conjunction:<N>" — N >= 2 verb-bearing conjuncts.
      * "multi_kind:<comma-joined-kinds>" — >= 2 distinct
        proposition-kind signals in the text.
      * "multi_entity:<N>" — >= 3 distinct entities in scope.
      * "multi_operational_facts:<N>" — N >= 2 evidence-backed
        universal operational facts in one proposed Model.
    """
    if not isinstance(entry, dict):
        return False, []
    if _is_compound_reporting_exempt(entry.get("proposition")):
        return False, []
    text = _claim_text(entry)
    if not text:
        return False, []

    reasons: list[str] = []

    # Operational-facet bundle heuristic. This catches structured
    # evidence such as UI/form/catalog/table snapshots where the text is
    # not grammatically compound but still contains several independent
    # beliefs that should be addressable as separate Models.
    operational_groups = _operational_facet_groups(entry)
    if len(operational_groups) >= 2:
        reasons.append(f"multi_operational_facts:{len(operational_groups)}")

    # Conjunction heuristic.
    conjuncts = _split_top_level(text)
    verb_conjuncts = [c for c in conjuncts if _conjunct_has_verb(c)]
    if len(verb_conjuncts) >= 2:
        reasons.append(f"multi_conjunction:{len(verb_conjuncts)}")

    # Multi-kind heuristic.
    kinds = _kind_signals(text)
    if len(kinds) >= 2:
        reasons.append("multi_kind:" + ",".join(sorted(kinds)))

    # Compound-entity heuristic. Splitter-produced operational atoms can
    # legitimately contain several capitalized label tokens in one field
    # value; do not reinterpret that as a multi-entity belief.
    prop = entry.get("proposition")
    is_split_operational_atom = (
        isinstance(prop, dict)
        and prop.get("operational_split_source") == "universal_facets"
    )
    if not is_split_operational_atom:
        entities = _distinct_entities(entry)
        if len(entities) >= 3:
            reasons.append(f"multi_entity:{len(entities)}")

    return (bool(reasons), reasons)


def split_compound_claim_op(op: ClaimOp) -> list[ClaimOp]:
    """
    Decompose a compound `ClaimOp` into atomic primitives + optional
    composing `situation`.

    Behavior contract:
      * Returns `[op]` unchanged when:
        - op.op != "insert"
        - entry is missing / malformed
        - heuristics say the op is already atomic
      * Returns N >= 2 atomic ops + 1 synthesized situation op when
        compound.

    Atomic split preserves on EACH split op:
      * `op.tenant_id`-equivalent fields (this module doesn't own
        tenant; tenant lives on the enclosing ValidatedDiff).
      * `born_from_event_id`, `evidence_event_ids`
      * `confidence` / `confidence_at_assertion` (copied verbatim;
        calibration handles per-row tuning later)
      * `scope_actors`, `scope_entities`, `scope_temporal`
      * `falsifier` if present
      * `embedding` is dropped (set to None) so downstream backfills.

    The synthesized situation carries:
      * `proposition.kind = "belief"` and `claim_role = "situation"`
      * `summary` and `shared_mechanism` = trimmed compound text
      * `pressure_type` inferred via keyword heuristic
      * `status = "forming"`
      * `member_model_ids = []` (filled post-insert by the integrator)
      * `member_model_pending = True` flag on the entry itself so the
        integrator can identify which situations need patching.
    """
    if op.op != "insert" or not isinstance(op.entry, dict):
        return [op]

    entry = op.entry
    if _is_unsplittable_proposition(entry.get("proposition")):
        return _allocate_unsplit_atomic(op)

    operational_splits = _split_operational_claim_op(entry)
    if operational_splits:
        return operational_splits

    compound, reasons = is_compound(entry)
    if not compound:
        return _allocate_unsplit_atomic(op)

    text = _claim_text(entry)
    conjuncts = _split_top_level(text)
    verb_conjuncts = [c for c in conjuncts if _conjunct_has_verb(c)]

    # If we can't get >=2 useful conjuncts, do NOT split — under-confident
    # heuristics yield the original (false negative is safer).
    if len(verb_conjuncts) < 2:
        return [op]

    base_entry = deepcopy(entry)
    # Strip embedding from base; each split will recompute downstream.
    base_entry.pop("embedding", None)

    split_ops: list[ClaimOp] = []
    for piece in verb_conjuncts:
        atomic_entry = deepcopy(base_entry)
        atomic_kind = _atomic_kind_for(piece)
        atomic_entry["proposition"] = _atomic_proposition(
            piece, atomic_kind, base_entry.get("proposition"),
        )
        atomic_entry["natural"] = piece.rstrip(".") + "."
        # Embedding stays None — applier / backfill will compute.
        atomic_entry.pop("embedding", None)
        if _redistribute_atomic_evidence(atomic_entry, piece):
            split_ops.append(ClaimOp(op="insert", entry=atomic_entry))

    # Synthesize the composing situation.
    situation_entry = deepcopy(base_entry)
    situation_entry.pop("embedding", None)
    pressure_type = _infer_pressure_type(text) or "execution"
    trimmed = _trim(text, 200)
    sit_prop: dict[str, Any] = {
        "kind": "belief",
        "claim_role": "situation",
        "abstraction_level": "composite",
        "time_mode": "current",
        "modality": "inferred",
        "polarity": "mixed",
        "situation": trimmed,
        "summary": trimmed,
        "member_model_ids": [],
        "relationship_summary": (
            "Atomic claims split from one compound LLM model entry; "
            "they are jointly true and share an operational context."
        ),
        "status": "forming",
        "shared_mechanism": trimmed,
    }
    sit_prop["pressure_type"] = pressure_type
    if base_entry.get("supporting_event_ids"):
        sit_prop["evidence_event_ids"] = list(base_entry["supporting_event_ids"])
    situation_entry["proposition"] = sit_prop
    situation_entry["natural"] = f"Composite situation: {trimmed}"
    # Flag for the integrator: patch member_model_ids after atomic
    # inserts in the same diff.
    situation_entry["member_model_pending"] = True
    # Echo reasons so the integrator / observability can audit why
    # this situation was synthesized.
    situation_entry["split_reasons"] = reasons

    if _composite_is_necessary(entry):
        split_ops.append(ClaimOp(op="insert", entry=situation_entry))
    return split_ops


# ---------------------------------------------------------------------
# Internal helpers for atomic proposition synthesis
# ---------------------------------------------------------------------


def _atomic_kind_for(piece: str) -> str:
    """Pick a single semantic role for an atomic conjunct."""
    kinds = _kind_signals(piece)
    # Priority: concern > prediction > state. Concern is the most
    # information-dense and easiest to misclassify as state.
    if "concern" in kinds:
        return "concern"
    if "prediction" in kinds:
        return "prediction"
    if "state" in kinds:
        return "state"
    # Default to state — safe fallback.
    return "state"


def _atomic_proposition(
    piece: str,
    kind: str,
    original_prop: Any,
) -> dict[str, Any]:
    """Build a minimal valid proposition dict for the atomic conjunct."""
    piece_clean = piece.rstrip(".").strip()
    base_subject: Any = "compound-claim-split"
    base_raised_by: Any = "system:splitter"
    if isinstance(original_prop, dict):
        for src_key in ("subject", "about", "subject_external", "signature"):
            sval = original_prop.get(src_key)
            if isinstance(sval, (str, dict)) and sval:
                base_subject = sval
                break
        rb = original_prop.get("raised_by")
        if isinstance(rb, (str, dict)) and rb:
            base_raised_by = rb

    if kind == "concern":
        return {
            "kind": "belief",
            "claim_role": "concern",
            "polarity": "negative",
            "about": base_subject,
            "nature": piece_clean,
            "raised_by": base_raised_by,
        }
    if kind == "prediction":
        return {
            "kind": "prediction",
            "expected": piece_clean,
            "resolution": "atomic_split_pending_resolution",
        }
    # state default
    return {
        "kind": "belief",
        "claim_role": "fact",
        "subject": base_subject,
        "assertion": piece_clean,
    }


__all__ = [
    "split_compound_claim_op",
    "is_compound",
]
