"""Batch-level learned mention discovery over persisted source evidence."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Literal, Protocol
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from lib.contracts.entity_mentions import EntityMentionDetectionFate
from lib.observability.metrics import counter, gauge
DISCOVERY_VERSION = "learned-persisted-batch-entity-discovery-v2"
# Below this point the candidate remains a governed non-entity fate. Identity
# resolution still has its own stricter authority/selection gates.
MIN_ACCEPTED_CONFIDENCE = 0.75
CompanyEntityType = Literal[
    "person", "team", "customer", "project", "product", "system",
    "workstream", "goal", "commitment", "decision", "resource", "other",
]

_DISCOVERY_SYSTEM_PROMPT = """\
Extract every explicit named company-entity mention from this persisted signal batch.

Work focal signal by focal signal. Other signals may disambiguate a literal name,
but never copy a name into a focal signal where its characters do not occur.
Complete one signal before advancing: scan its people, organizations, named work,
technical objects, and typed identifiers, then make a final left-to-right pass for
omissions. A signal can contain many mentions; return every distinct occurrence.

BOUNDARIES
Offsets are zero-based Python character offsets into exact content_text and
span_end is exclusive. The surface must equal that exact slice. Return the
smallest *complete written designation*, not merely the shortest unique token:
- include a directly attached semantic designator when it states what the named
  object is (for example a written Project/Initiative, Goal/Objective,
  Decision, Commitment, Contract, Gate, Dataset, or Resource designation);
- include a person's written title when it is attached to their name;
- exclude surrounding punctuation, quoting syntax, verbs, articles, and merely
  descriptive trailing nouns such as "the X project" or "the Y workstream";
- preserve every character inside names and identifiers, including Unicode,
  whitespace, hyphens, slashes, #, @, colons, and code punctuation.
Do not strip a type-bearing prefix from a code, but do not invent one when only
the code occurs.

TYPES -- choose by the referent's role in this signal, not by capitalization:
- person: a named human;
- team: an internal organizational group, squad, guild, or operating unit;
- customer: an explicitly external client, account, buyer, or partner company;
- project: a bounded named project, initiative, or program;
- product: a named customer-facing product or offering;
- system: a technical service, application, API, component, queue, gateway, or
  deployable runtime; service-style identifiers such as svc-* are systems;
- workstream: a named continuing stream of work, rollout, migration, launch, or
  transition, when it is not explicitly presented as a project;
- goal: a named goal or objective;
- commitment: a named promise, obligation, delivery commitment, or milestone;
- decision: a named or coded decision;
- resource: a named ticket, incident, risk, document, dataset, contract, gate,
  tenant, case, or other referenced business artifact/record;
- other: only an explicit named business referent that truly fits none above.
Prefer the stated relationship and explicit designator over name morphology. If
an organization's internal/external role or a product-like name's referent is not
actually established, use other or abstain rather than guessing confidently.

Do not treat conversational or transport coordinates (channel names, thread
numbers, timestamps, message IDs), ordinary language, unnamed roles, pronouns,
dates, generic activities, schema fields, or code snippets as company entities.
A business identifier mentioned inside such context is still an entity. Set
abstain for a visible literal candidate whose entity status or type remains too
uncertain. Never resolve identity, infer a registry ID, or invent absent text.
Before returning, verify every surface against its exact slice and repeat the
per-signal completeness pass.
"""

DISCOVERY_BATCHES = counter(
    "entity_discovery_batches_total",
    "Persisted signal batches processed by learned entity discovery.",
    ("mode", "outcome"),
)
DISCOVERY_READINESS = gauge(
    "entity_discovery_provider_ready",
    "Whether the configured learned entity-discovery provider passed preflight.",
    ("state",),
)


class StructuredDiscoveryProvider(Protocol):
    async def structured(
        self,
        *,
        system: str,
        user: str,
        schema: type[BaseModel],
        temperature: float,
        max_tokens: int,
    ) -> Any: ...


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, str_strip_whitespace=True)


class LearnedMention(_Strict):
    signal_id: UUID
    surface: str = Field(
        min_length=1,
        max_length=240,
        description="Exact smallest complete written designation from content_text.",
    )
    span_start: int = Field(ge=0)
    span_end: int = Field(gt=0)
    entity_type: CompanyEntityType = Field(
        description=(
            "Closed company ontology type chosen from the referent's stated role; "
            "other is a last resort, not a substitute for resource or system."
        )
    )
    confidence: float = Field(ge=0.0, le=1.0)
    abstain: bool

    @field_validator("entity_type", mode="before")
    @classmethod
    def normalize_closed_ontology_synonyms(cls, value: Any) -> Any:
        """Keep one vocabulary miss from invalidating an entire batch.

        ``service`` is a common surface synonym for the canonical company
        ontology's ``system`` type. Identity remains unresolved and the exact
        source span is still verified independently; this only normalizes the
        type vocabulary at the structured boundary.
        """

        if isinstance(value, str) and value.strip().casefold() == "service":
            return "system"
        return value

    @model_validator(mode="after")
    def ordered_span(self) -> "LearnedMention":
        if self.span_end <= self.span_start:
            raise ValueError("span_end must be greater than span_start")
        return self


class LearnedMentionBatch(_Strict):
    mentions: tuple[LearnedMention, ...]


class _DiscoveryPreflightResult(_Strict):
    ready: Literal[True]


@dataclass(frozen=True)
class PersistedSignalText:
    signal_id: UUID
    source_channel: str
    content_text: str


@dataclass(frozen=True)
class VerifiedMentionCandidate:
    signal_id: UUID
    surface: str
    span_start: int
    span_end: int
    entity_type: str
    confidence: float
    fate: EntityMentionDetectionFate
    reason_codes: tuple[str, ...]
    extractor_version: str = DISCOVERY_VERSION


@dataclass(frozen=True)
class LearnedDiscoveryResult:
    candidates: tuple[VerifiedMentionCandidate, ...]
    mode: Literal["learned", "deterministic_fallback"]
    provider_error: str | None = None


class DiscoveryProviderPreflightError(RuntimeError):
    """Startup failure proving that structured discovery is unavailable."""

    def __init__(self, message: str, *, code: str, retryable: bool) -> None:
        super().__init__(message)
        self.code = code
        self.retryable = retryable


_UNSUPPORTED_CONFIGURATION_MARKERS = (
    "unsupported model",
    "unsupported value",
    "model is not supported",
    "model does not support",
    "unknown model",
    "unknown variant",
    "model_not_found",
    "invalid model",
    "upgrade codex",
    "update codex",
    "outdated codex",
    "older version of codex",
    "not supported by this version",
)


async def preflight_structured_discovery(
    provider: StructuredDiscoveryProvider,
) -> None:
    """Prove the selected transport/model can complete a tiny structured turn.

    This intentionally runs before worker health starts. Unsupported models and
    outdated local transports are configuration failures, not retryable batch
    incidents; callers may only change models through an explicit allowlist.
    """

    for state in ("ready", "failed"):
        DISCOVERY_READINESS.set(0, state=state)
    try:
        result = await provider.structured(
            system="Return the requested readiness object exactly.",
            user='Return {"ready": true}.',
            schema=_DiscoveryPreflightResult,
            temperature=0.0,
            max_tokens=32,
        )
        if result.ready is not True:
            raise ValueError("structured readiness response was not true")
    except Exception as exc:
        message = f"{type(exc).__name__}: {exc}"[:500]
        normalized = message.casefold()
        unsupported = any(
            marker in normalized for marker in _UNSUPPORTED_CONFIGURATION_MARKERS
        )
        is_configuration = unsupported or isinstance(exc, (TypeError, ValueError))
        DISCOVERY_READINESS.set(1, state="failed")
        raise DiscoveryProviderPreflightError(
            message,
            code=(
                "unsupported_or_outdated_model"
                if unsupported
                else "provider_configuration"
                if is_configuration
                else "provider_unavailable"
            ),
            retryable=not is_configuration,
        ) from exc
    DISCOVERY_READINESS.set(1, state="ready")


async def discover_batch_mentions(
    *,
    provider: StructuredDiscoveryProvider | None,
    signals: tuple[PersistedSignalText, ...],
) -> LearnedDiscoveryResult:
    """Use exactly one structured call, then distrust and verify its coordinates."""

    if provider is None or not signals:
        if signals:
            DISCOVERY_BATCHES.inc(mode="deterministic_fallback", outcome="disabled")
        return LearnedDiscoveryResult((), "deterministic_fallback")
    payload = [
        {
            "signal_id": str(item.signal_id),
            "source_channel": item.source_channel,
            "content_text": item.content_text,
        }
        for item in signals
    ]
    try:
        learned = await provider.structured(
            system=_DISCOVERY_SYSTEM_PROMPT,
            user=json.dumps({"signals": payload}, ensure_ascii=False),
            schema=LearnedMentionBatch,
            temperature=0.0,
            max_tokens=min(4096, 256 + 160 * len(signals)),
        )
    except Exception as exc:  # provider failure must not block deterministic coverage
        DISCOVERY_BATCHES.inc(
            mode="deterministic_fallback",
            outcome="provider_error",
        )
        return LearnedDiscoveryResult(
            (), "deterministic_fallback", f"{type(exc).__name__}: {exc}"[:500]
        )
    DISCOVERY_BATCHES.inc(mode="learned", outcome="success")
    return LearnedDiscoveryResult(
        _verify_candidates(learned.mentions, signals),
        "learned",
    )


def _verify_candidates(
    mentions: tuple[LearnedMention, ...],
    signals: tuple[PersistedSignalText, ...],
) -> tuple[VerifiedMentionCandidate, ...]:
    by_id = {item.signal_id: item for item in signals}
    candidates: list[VerifiedMentionCandidate] = []
    seen: set[tuple[UUID, int, int, str]] = set()
    for item in mentions:
        signal = by_id.get(item.signal_id)
        if signal is None:
            # There is no governed focal observation on which to persist a fate.
            continue
        in_bounds = item.span_end <= len(signal.content_text)
        exact = (
            in_bounds
            and signal.content_text[item.span_start : item.span_end] == item.surface
        )
        span_start = item.span_start
        span_end = item.span_end
        repaired = False
        if not exact:
            occurrences: list[int] = []
            cursor = 0
            while True:
                found = signal.content_text.find(item.surface, cursor)
                if found < 0:
                    break
                occurrences.append(found)
                cursor = found + max(1, len(item.surface))
            if len(occurrences) == 1:
                span_start = occurrences[0]
                span_end = span_start + len(item.surface)
                exact = True
                repaired = True
        if not exact:
            fate = EntityMentionDetectionFate.REJECTED_NOT_ANCHORED
            reasons = ("learned_span_failed_exact_source_verification",)
        elif item.abstain or item.confidence < MIN_ACCEPTED_CONFIDENCE:
            fate = EntityMentionDetectionFate.REJECTED_NOT_ENTITY
            reasons = (
                "learned_extractor_abstained" if item.abstain
                else "learned_confidence_below_admission_threshold",
                f"learned_type:{item.entity_type}",
            )
        else:
            fate = EntityMentionDetectionFate.DETECTED
            reasons = (
                (
                    "learned_span_repaired_unique_exact_surface"
                    if repaired
                    else "learned_high_confidence_exact_source_span"
                ),
                f"learned_type:{item.entity_type}",
            )
        key = (item.signal_id, span_start, span_end, item.surface.casefold())
        if key in seen:
            continue
        seen.add(key)
        candidates.append(VerifiedMentionCandidate(
            signal_id=item.signal_id,
            surface=item.surface,
            span_start=span_start,
            span_end=span_end,
            entity_type=item.entity_type,
            confidence=item.confidence,
            fate=fate,
            reason_codes=reasons,
        ))
    # Prefer the largest accepted span for nested duplicates; retain rejected fates.
    accepted = [c for c in candidates if c.fate is EntityMentionDetectionFate.DETECTED]
    superseded: set[tuple[UUID, int, int, str]] = set()
    for inner in accepted:
        for outer in accepted:
            if inner is outer or inner.signal_id != outer.signal_id:
                continue
            contains = (
                outer.span_start <= inner.span_start
                and inner.span_end <= outer.span_end
            )
            if contains and (outer.span_end - outer.span_start) > (
                inner.span_end - inner.span_start
            ):
                superseded.add(
                    (
                        inner.signal_id,
                        inner.span_start,
                        inner.span_end,
                        inner.surface.casefold(),
                    )
                )
                break
    return tuple(c for c in candidates if (
        c.signal_id, c.span_start, c.span_end, c.surface.casefold()
    ) not in superseded)


__all__ = [
    "DISCOVERY_VERSION", "DiscoveryProviderPreflightError",
    "LearnedDiscoveryResult", "LearnedMentionBatch",
    "PersistedSignalText", "StructuredDiscoveryProvider",
    "VerifiedMentionCandidate", "discover_batch_mentions",
    "preflight_structured_discovery",
]
