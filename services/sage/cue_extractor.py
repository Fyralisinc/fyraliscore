"""services.sage.cue_extractor — Phase 2 Structured Cue Extractor.

Implements Stage A ("Structured Cue Extraction") of the SAGE-inspired
self-evolution architecture. See:
  - fyralis-sage-synthesis-self-evolution.md §7.2 (cue fields + example)
  - fyralis-sage-synthesis-self-evolution.md Phase 2 (acceptance)

This component runs BEFORE the LLM question planner in
`services/execution/inquiry.py`. Its job is to lift cheap, deterministic
company-state handles out of raw signal/question text so retrieval and
the planner do not have to rely on raw string similarity.

Design notes:
  * v1 is pure-Python and deterministic. No LLM, no NER model.
  * The single async dependency is loading the tenant's `entity_aliases`
    table (one query, cached per CueExtractor instance). Extraction
    itself is sync.
  * The `alias_loader` constructor argument is purely for testability:
    tests pass a sync stub `lambda: {...}` to avoid touching the DB.
  * The output dataclass is frozen/slotted so it can be safely passed
    across boundaries and re-hashed for cache keys.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any, Awaitable, Callable, Mapping

import asyncpg


# ---------------------------------------------------------------------
# Output dataclass
# ---------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class StructuredCues:
    """Deterministic cues lifted from a signal/question/hypotheses bundle.

    All collection fields are tuples (hashable) and de-duplicated while
    preserving first-seen order. Dict fields use only JSON-safe values
    so the whole object can be serialised for audit logs without extra
    work.
    """

    explicit_entities: tuple[str, ...]
    aliases: tuple[str, ...]
    actor_mentions: tuple[str, ...]
    team_mentions: tuple[str, ...]
    customer_mentions: tuple[str, ...]
    system_mentions: tuple[str, ...]
    goal_mentions: tuple[str, ...]
    commitment_mentions: tuple[str, ...]
    relationship_clues: tuple[str, ...]
    time_constraints: dict[str, Any]
    status_constraints: dict[str, Any]
    source_constraints: dict[str, Any]
    access_constraints: dict[str, Any]
    expected_synthesis_decision_type: tuple[str, ...]


# ---------------------------------------------------------------------
# Built-in dictionaries
# ---------------------------------------------------------------------


# Common system / infra / tool names that appear in B2B SaaS signals.
# Kept intentionally small — the alias table is the primary source of
# truth; this dictionary just catches the cases where a tenant has not
# yet seeded an alias for an obvious common term.
# Subset of systems that should always render as uppercase acronyms
# even if longer than the generic 4-char heuristic.
_FORCE_UPPER_SYSTEMS: frozenset[str] = frozenset(
    {
        "sso", "oauth", "oauth2", "oidc", "saml", "scim", "mfa", "rbac",
        "k8s", "ci", "ci/cd", "api", "rest", "iam", "rds", "s3", "pii", "phi",
    }
)


_BUILTIN_SYSTEMS: frozenset[str] = frozenset(
    {
        "sso",
        "oauth",
        "oauth2",
        "oidc",
        "saml",
        "scim",
        "k8s",
        "kubernetes",
        "postgres",
        "postgresql",
        "redis",
        "kafka",
        "snowflake",
        "datadog",
        "auth0",
        "okta",
        "stripe",
        "twilio",
        "segment",
        "looker",
        "tableau",
        "elasticsearch",
        "opensearch",
        "rabbitmq",
        "nginx",
        "envoy",
        "vault",
        "terraform",
        "ci",
        "ci/cd",
        "api",
        "graphql",
        "rest",
        "mfa",
        "rbac",
        "lambda",
        "s3",
        "rds",
        "iam",
    }
)

# Common channel / source mentions. Used for source_constraints.
_BUILTIN_SOURCES: frozenset[str] = frozenset(
    {
        "slack",
        "email",
        "linear",
        "github",
        "jira",
        "crm",
        "notion",
        "zoom",
        "salesforce",
        "hubspot",
        "gmail",
        "outlook",
        "confluence",
        "drive",
        "asana",
    }
)

# Access-control / sensitivity markers.
_BUILTIN_ACCESS: frozenset[str] = frozenset(
    {
        "confidential",
        "internal",
        "internal-only",
        "legal",
        "hr",
        "restricted",
        "secret",
        "pii",
        "phi",
    }
)

# Words that suggest the mention is a team rather than a customer/actor.
_TEAM_HINTS: frozenset[str] = frozenset(
    {
        "team",
        "squad",
        "guild",
        "pod",
        "platform",
        "engineering",
        "sales",
        "marketing",
        "design",
        "ops",
        "support",
        "success",
        "finance",
        "people",
    }
)

# Words that strongly imply a commitment.
_COMMITMENT_HINTS: frozenset[str] = frozenset(
    {
        "launch",
        "ship",
        "deliver",
        "deadline",
        "milestone",
        "release",
        "rollout",
        "onboarding",
        "go-live",
        "go live",
        "commitment",
        "sla",
    }
)

# Words that strongly imply a goal.
_GOAL_HINTS: frozenset[str] = frozenset(
    {
        "goal",
        "objective",
        "okr",
        "target",
        "north star",
        "kpi",
        "ambition",
    }
)


# Pattern -> relation_clue. Patterns use \b boundaries and are matched
# case-insensitively. Order is irrelevant: every pattern is tried, and
# the resulting clues are de-duplicated downstream.
_RELATION_PATTERNS: tuple[tuple[re.Pattern[str], str], ...] = (
    (re.compile(r"\bdepends?\s+on\b", re.IGNORECASE), "depends_on"),
    (re.compile(r"\bdependency\b|\bdependencies\b", re.IGNORECASE), "depends_on"),
    (re.compile(r"\bblock(?:s|ed|ing)?\b", re.IGNORECASE), "blocks"),
    (re.compile(r"\bblocker\b", re.IGNORECASE), "blocks"),
    (re.compile(r"\bcritical\s*path\b", re.IGNORECASE), "critical_path"),
    (re.compile(r"\bowns?\b|\bownership\b", re.IGNORECASE), "owns"),
    (re.compile(r"\bowner\b", re.IGNORECASE), "owns"),
    (re.compile(r"\bassigned\s+to\b", re.IGNORECASE), "assigned_to"),
    (re.compile(r"\bcontradict(?:s|ed|ing)?\b", re.IGNORECASE), "contradicts"),
    (re.compile(r"\bconflict(?:s|ed|ing)?\b", re.IGNORECASE), "contradicts"),
    (re.compile(r"\bcaused?\s+by\b", re.IGNORECASE), "caused_by"),
    (re.compile(r"\bbecause\s+of\b", re.IGNORECASE), "caused_by"),
    (re.compile(r"\benable(?:s|d|ing)?\b|\bunblock(?:s|ed|ing)?\b", re.IGNORECASE), "enables"),
    (re.compile(r"\bpredicts?\b|\bforecasts?\b", re.IGNORECASE), "predicts"),
    (re.compile(r"\brecurring\b|\brepeated(?:ly)?\b|\bagain\b", re.IGNORECASE), "recurring"),
    (re.compile(r"\bpattern\b", re.IGNORECASE), "recurring"),
    (re.compile(r"\bdelay(?:s|ed|ing)?\b|\bslipping\b|\bslipped\b", re.IGNORECASE), "delays"),
    (re.compile(r"\brisk\b|\bat\s+risk\b", re.IGNORECASE), "risk"),
)


# Time constraint patterns. Each yields a dict fragment merged into
# `time_constraints`. The recency window patterns favour the most
# specific match (explicit N days > "this week" > "today").
_RE_LAST_N_DAYS = re.compile(
    r"\b(?:in\s+the\s+)?last\s+(\d{1,4})\s+(day|days|week|weeks|month|months|quarter|quarters)\b",
    re.IGNORECASE,
)
_RE_PAST_N_DAYS = re.compile(
    r"\bpast\s+(\d{1,4})\s+(day|days|week|weeks|month|months)\b",
    re.IGNORECASE,
)
_RE_SINCE_DATE = re.compile(
    r"\bsince\s+(\d{4})-(\d{1,2})-(\d{1,2})\b",
    re.IGNORECASE,
)
_RE_THIS_WEEK = re.compile(r"\bthis\s+week\b", re.IGNORECASE)
_RE_THIS_MONTH = re.compile(r"\bthis\s+month\b", re.IGNORECASE)
_RE_THIS_QUARTER = re.compile(r"\bthis\s+quarter\b", re.IGNORECASE)
_RE_TODAY = re.compile(r"\btoday\b", re.IGNORECASE)
_RE_YESTERDAY = re.compile(r"\byesterday\b", re.IGNORECASE)


# Status constraint markers — small set of obvious states.
_STATUS_MARKERS: tuple[tuple[re.Pattern[str], str, str], ...] = (
    (re.compile(r"\bopen\b", re.IGNORECASE), "status", "open"),
    (re.compile(r"\bclosed\b|\bresolved\b", re.IGNORECASE), "status", "closed"),
    (re.compile(r"\bactive\b|\bin\s*progress\b", re.IGNORECASE), "status", "active"),
    (re.compile(r"\bblocked\b", re.IGNORECASE), "status", "blocked"),
    (re.compile(r"\bat\s+risk\b", re.IGNORECASE), "health", "at_risk"),
    (re.compile(r"\boverdue\b|\bslipped\b", re.IGNORECASE), "health", "overdue"),
)


# Synthesis-decision-type triggers. Each tuple is (matcher_predicate,
# decision_type). Multiple may fire per question; order of *insertion*
# is preserved in the output tuple.
_DECISION_TRIGGERS: tuple[tuple[re.Pattern[str], str], ...] = (
    (
        re.compile(
            r"\bblock(?:ed|ing|er)?\b|\bdelay(?:s|ed|ing)?\b|\bslipping\b",
            re.IGNORECASE,
        ),
        "update_commitment_risk",
    ),
    (
        re.compile(r"\bpattern\b|\brecurring\b|\bacross\b", re.IGNORECASE),
        "create_emerging_pattern_model",
    ),
    (
        re.compile(r"\bowner\b|\bowns?\b|\bassigned\b|\bownership\b", re.IGNORECASE),
        "create_ownership_relation",
    ),
    (
        re.compile(r"\bdepends?\b|\bdependency\b|\bcritical\s*path\b", re.IGNORECASE),
        "create_dependency_relation",
    ),
    (
        re.compile(r"\bcontradict\b|\bconflict\b", re.IGNORECASE),
        "resolve_contradiction",
    ),
)


# Generic English stop-words used to filter capitalized-noun fallback
# candidates. Intentionally narrow — only words that frequently appear
# capitalized at sentence start. Anything domain-specific belongs in
# the tenant's alias table.
_CAP_STOP_WORDS: frozenset[str] = frozenset(
    {
        "The", "A", "An", "We", "I", "They", "You", "It", "He", "She",
        "This", "That", "These", "Those", "Our", "Their", "My", "Your",
        "Is", "Are", "Was", "Were", "Be", "Been", "Being", "Has", "Have",
        "Had", "Do", "Does", "Did", "Will", "Would", "Should", "Could",
        "May", "Might", "Must", "Can", "Cannot", "Yes", "No", "Not",
        "If", "When", "Where", "Why", "How", "What", "Who", "Which",
        "But", "And", "Or", "So", "Because", "Although", "However",
        "Today", "Tomorrow", "Yesterday", "Monday", "Tuesday",
        "Wednesday", "Thursday", "Friday", "Saturday", "Sunday",
    }
)


# Regex for capitalized multi-word phrases ("Acme Corp", "Project Falcon").
# Allows single-letter caps but skips ALL-CAPS-only-of-length-1.
_RE_CAPITALIZED_PHRASE = re.compile(
    r"\b([A-Z][A-Za-z0-9]+(?:\s+[A-Z][A-Za-z0-9]+){0,3})\b"
)


# ---------------------------------------------------------------------
# Alias loader type
# ---------------------------------------------------------------------


# A loader returns a mapping {alias_text_lower: canonical_entity_str}.
# The canonical_entity_str is whatever printable handle the caller
# wants to surface — typically the resolved entity ref's name field.
AliasLoader = Callable[[], "Awaitable[Mapping[str, str]] | Mapping[str, str]"]


# ---------------------------------------------------------------------
# Extractor
# ---------------------------------------------------------------------


class CueExtractor:
    """Deterministic structured cue extractor.

    Usage:
        extractor = CueExtractor(pool=pool, tenant_id=tid)
        cues = await extractor.extract(
            signal=signal_dict,
            question="Is SSO blocking Acme launch?",
            hypotheses=["H1"],
        )

    Tests inject `alias_loader=lambda: {...}` to avoid DB I/O.
    """

    def __init__(
        self,
        *,
        pool: asyncpg.Pool | None,
        tenant_id: Any,
        alias_loader: AliasLoader | None = None,
    ) -> None:
        self._pool = pool
        self._tenant_id = tenant_id
        self._alias_loader = alias_loader
        self._alias_cache: dict[str, str] | None = None

    # -----------------------------------------------------------------
    # Alias loading
    # -----------------------------------------------------------------

    async def _load_aliases(self) -> Mapping[str, str]:
        if self._alias_cache is not None:
            return self._alias_cache

        if self._alias_loader is not None:
            raw = self._alias_loader()
            if hasattr(raw, "__await__"):
                raw = await raw  # type: ignore[assignment]
            mapping = {k.casefold(): v for k, v in dict(raw).items()}
            self._alias_cache = mapping
            return mapping

        # Default: query entity_aliases for this tenant. Returns a
        # mapping {alias_text_lower: canonical_handle}, where the
        # canonical handle is the resolved_entity_ref's "name" field
        # when present, else a JSON-stringified key.
        if self._pool is None:
            self._alias_cache = {}
            return self._alias_cache

        import json as _json

        rows = await self._pool.fetch(
            """
            SELECT alias_text, resolved_entity_ref
            FROM entity_aliases
            WHERE tenant_id = $1
            """,
            self._tenant_id,
        )
        mapping: dict[str, str] = {}
        for r in rows:
            alias = (r["alias_text"] or "").strip()
            if not alias:
                continue
            ref = r["resolved_entity_ref"]
            if isinstance(ref, str):
                try:
                    ref = _json.loads(ref)
                except ValueError:
                    ref = {"raw": ref}
            handle: str
            if isinstance(ref, dict):
                handle = (
                    ref.get("name")
                    or ref.get("title")
                    or ref.get("id")
                    or _json.dumps(ref, sort_keys=True)
                )
            else:
                handle = str(ref)
            mapping[alias.casefold()] = str(handle)
        self._alias_cache = mapping
        return mapping

    # -----------------------------------------------------------------
    # Main entry point
    # -----------------------------------------------------------------

    async def extract(
        self,
        *,
        signal: dict | None,
        question: str | None,
        hypotheses: list[str] | None,
        evidence_state: dict | None = None,
    ) -> StructuredCues:
        aliases_map = await self._load_aliases()
        text = _compose_text(signal, question, hypotheses, evidence_state)
        return _extract_sync(text, aliases_map)


# ---------------------------------------------------------------------
# Sync extraction core (factored out for direct unit testing)
# ---------------------------------------------------------------------


def _compose_text(
    signal: dict | None,
    question: str | None,
    hypotheses: list[str] | None,
    evidence_state: dict | None,
) -> str:
    parts: list[str] = []
    if signal:
        # Pull the most likely free-text fields. Any others get a JSON
        # tail dump for the regexes to still see.
        for k in ("summary", "signal_summary", "text", "title", "body", "description"):
            v = signal.get(k)
            if isinstance(v, str) and v.strip():
                parts.append(v)
    if question and question.strip():
        parts.append(question)
    for h in hypotheses or ():
        if isinstance(h, str) and h.strip():
            parts.append(h)
    if evidence_state:
        for k in ("summary", "notes", "rationale"):
            v = evidence_state.get(k)
            if isinstance(v, str) and v.strip():
                parts.append(v)
    return "\n".join(parts)


def _extract_sync(text: str, aliases_map: Mapping[str, str]) -> StructuredCues:
    """Run all deterministic passes against an already-composed text blob."""
    if not text or not text.strip():
        return StructuredCues(
            explicit_entities=(),
            aliases=(),
            actor_mentions=(),
            team_mentions=(),
            customer_mentions=(),
            system_mentions=(),
            goal_mentions=(),
            commitment_mentions=(),
            relationship_clues=(),
            time_constraints={},
            status_constraints={},
            source_constraints={},
            access_constraints={},
            expected_synthesis_decision_type=(),
        )

    lower = text.casefold()

    # -------- aliases & explicit entities ----------------------------
    aliases_hit: list[str] = []
    explicit: list[str] = []
    seen_explicit: set[str] = set()
    for alias_text, canonical in aliases_map.items():
        if not alias_text:
            continue
        # Word-boundary match against the lowercased text.
        if re.search(rf"\b{re.escape(alias_text)}\b", lower):
            aliases_hit.append(alias_text)
            if canonical and canonical not in seen_explicit:
                explicit.append(canonical)
                seen_explicit.add(canonical)

    # -------- system mentions (alias-map + builtin dictionary) -------
    systems: list[str] = []
    seen_sys: set[str] = set()
    for sys_name in _BUILTIN_SYSTEMS:
        if re.search(rf"\b{re.escape(sys_name)}\b", lower):
            # Surface in canonical UPPER form for known acronyms or
            # any sys_name <=4 chars (good enough heuristic). Longer
            # technology names render in lower-case canonical form.
            display = (
                sys_name.upper()
                if sys_name in _FORCE_UPPER_SYSTEMS or len(sys_name) <= 4
                else sys_name
            )
            if display not in seen_sys:
                systems.append(display)
                seen_sys.add(display)
                if display not in seen_explicit:
                    explicit.append(display)
                    seen_explicit.add(display)

    # -------- capitalized-noun fallback for unknown entities ---------
    for match in _RE_CAPITALIZED_PHRASE.finditer(text):
        phrase = match.group(1).strip()
        first = phrase.split()[0]
        if first in _CAP_STOP_WORDS:
            continue
        # Skip if this phrase (or any of its tokens) is already covered
        # by an alias hit or a builtin system.
        if phrase.casefold() in aliases_map:
            continue
        if phrase.casefold() in _BUILTIN_SYSTEMS:
            continue
        if phrase in seen_explicit:
            continue
        # Heuristic: drop pure single-word capitalized verbs by
        # requiring either multi-word, all-caps acronym, or trailing
        # context (handled at caller level — we accept it).
        explicit.append(phrase)
        seen_explicit.add(phrase)

    # -------- categorise mentions into team/customer/actor/goal/commit
    teams: list[str] = []
    commitments: list[str] = []
    goals: list[str] = []
    customers: list[str] = []
    actors: list[str] = []

    for ent in explicit:
        ent_lc = ent.casefold()
        if any(hint in ent_lc for hint in _TEAM_HINTS):
            teams.append(ent)
            continue
        if any(hint in ent_lc for hint in _COMMITMENT_HINTS):
            commitments.append(ent)
            continue
        if any(hint in ent_lc for hint in _GOAL_HINTS):
            goals.append(ent)
            continue
        # Light heuristic: words ending with Corp/Inc/Ltd/Co or
        # appearing near "customer" hint -> customer; else default
        # to actor only when looks like a person token (single
        # capitalized word, length>=3, not a known system).
        if re.search(
            r"\b(?:corp|inc|ltd|llc|co|gmbh|sa|plc)\b", ent_lc
        ) or ent_lc in {"acme", "globex", "umbrella", "initech"}:
            customers.append(ent)
            continue
        # Default — if it looks like a single proper noun and not a
        # system, treat as actor candidate. Otherwise leave it in the
        # explicit_entities bucket only.
        if (
            " " not in ent
            and ent not in systems
            and ent_lc not in _BUILTIN_SYSTEMS
            and ent[0].isupper()
            and len(ent) >= 3
        ):
            # Use customer/actor disambiguation by checking nearby
            # cue words.
            if re.search(
                rf"\b{re.escape(ent_lc)}\b[^.\n]{{0,80}}\b(launch|onboarding|customer|account|deal|contract)\b",
                lower,
            ):
                customers.append(ent)
            else:
                actors.append(ent)

    # Promote commitment/goal hints from raw text even if no explicit
    # entity carried the word.
    for hint in _COMMITMENT_HINTS:
        if hint in lower and not any(hint in c.casefold() for c in commitments):
            commitments.append(hint)
    for hint in _GOAL_HINTS:
        if hint in lower and not any(hint in g.casefold() for g in goals):
            goals.append(hint)

    # -------- relationship clues -------------------------------------
    relation_clues: list[str] = []
    seen_clues: set[str] = set()
    for pattern, clue in _RELATION_PATTERNS:
        if pattern.search(text) and clue not in seen_clues:
            relation_clues.append(clue)
            seen_clues.add(clue)

    # -------- time constraints ---------------------------------------
    time_constraints: dict[str, Any] = {}
    m = _RE_LAST_N_DAYS.search(text) or _RE_PAST_N_DAYS.search(text)
    if m:
        n = int(m.group(1))
        unit = m.group(2).lower()
        days = _to_days(n, unit)
        time_constraints["recent_window_days"] = days
        time_constraints["phrase"] = m.group(0)
    elif _RE_THIS_WEEK.search(text):
        time_constraints["recent_window_days"] = 7
        time_constraints["phrase"] = "this week"
    elif _RE_THIS_MONTH.search(text):
        time_constraints["recent_window_days"] = 30
        time_constraints["phrase"] = "this month"
    elif _RE_THIS_QUARTER.search(text):
        time_constraints["recent_window_days"] = 90
        time_constraints["phrase"] = "this quarter"
    elif _RE_TODAY.search(text):
        time_constraints["recent_window_days"] = 1
        time_constraints["phrase"] = "today"
    elif _RE_YESTERDAY.search(text):
        time_constraints["recent_window_days"] = 2
        time_constraints["phrase"] = "yesterday"

    m_since = _RE_SINCE_DATE.search(text)
    if m_since:
        try:
            d = date(int(m_since.group(1)), int(m_since.group(2)), int(m_since.group(3)))
            time_constraints["since"] = d.isoformat()
        except ValueError:
            # Malformed date; leave it out rather than raising.
            pass

    # -------- status constraints -------------------------------------
    status_constraints: dict[str, Any] = {}
    for pattern, key, value in _STATUS_MARKERS:
        if pattern.search(text):
            # First match wins per key.
            status_constraints.setdefault(key, value)

    # -------- source constraints -------------------------------------
    source_constraints: dict[str, Any] = {}
    sources_hit: list[str] = []
    for src in _BUILTIN_SOURCES:
        if re.search(rf"\b{re.escape(src)}\b", lower):
            sources_hit.append(src)
    if sources_hit:
        source_constraints["channels"] = tuple(sorted(set(sources_hit)))

    # -------- access constraints -------------------------------------
    access_constraints: dict[str, Any] = {}
    access_hit: list[str] = []
    for marker in _BUILTIN_ACCESS:
        if re.search(rf"\b{re.escape(marker)}\b", lower):
            access_hit.append(marker)
    if access_hit:
        access_constraints["sensitivity_markers"] = tuple(sorted(set(access_hit)))

    # -------- expected synthesis decision types ----------------------
    decisions: list[str] = []
    seen_dec: set[str] = set()
    for pattern, decision in _DECISION_TRIGGERS:
        if pattern.search(text) and decision not in seen_dec:
            decisions.append(decision)
            seen_dec.add(decision)

    return StructuredCues(
        explicit_entities=tuple(_dedup(explicit)),
        aliases=tuple(_dedup(aliases_hit)),
        actor_mentions=tuple(_dedup(actors)),
        team_mentions=tuple(_dedup(teams)),
        customer_mentions=tuple(_dedup(customers)),
        system_mentions=tuple(_dedup(systems)),
        goal_mentions=tuple(_dedup(goals)),
        commitment_mentions=tuple(_dedup(commitments)),
        relationship_clues=tuple(relation_clues),
        time_constraints=time_constraints,
        status_constraints=status_constraints,
        source_constraints=source_constraints,
        access_constraints=access_constraints,
        expected_synthesis_decision_type=tuple(decisions),
    )


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------


def _dedup(items: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for it in items:
        if it not in seen:
            seen.add(it)
            out.append(it)
    return out


def _to_days(n: int, unit: str) -> int:
    unit = unit.lower().rstrip("s")
    if unit == "day":
        return n
    if unit == "week":
        return n * 7
    if unit == "month":
        return n * 30
    if unit == "quarter":
        return n * 90
    # Fallback — shouldn't happen given the regex character class.
    return n


__all__ = ["StructuredCues", "CueExtractor"]
