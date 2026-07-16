"""Batch-level learned mention discovery over persisted source evidence."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Literal, Protocol
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, model_validator

from lib.contracts.entity_mentions import EntityMentionDetectionFate
DISCOVERY_VERSION = "learned-persisted-batch-entity-discovery-v1"
MIN_ACCEPTED_CONFIDENCE = 0.80
CompanyEntityType = Literal[
    "person", "team", "customer", "project", "product", "system",
    "workstream", "goal", "commitment", "decision", "resource", "other",
]


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
    surface: str = Field(min_length=1, max_length=240)
    span_start: int = Field(ge=0)
    span_end: int = Field(gt=0)
    entity_type: CompanyEntityType
    confidence: float = Field(ge=0.0, le=1.0)
    abstain: bool

    @model_validator(mode="after")
    def ordered_span(self) -> "LearnedMention":
        if self.span_end <= self.span_start:
            raise ValueError("span_end must be greater than span_start")
        return self


class LearnedMentionBatch(_Strict):
    mentions: tuple[LearnedMention, ...]


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


async def discover_batch_mentions(
    *,
    provider: StructuredDiscoveryProvider | None,
    signals: tuple[PersistedSignalText, ...],
) -> LearnedDiscoveryResult:
    """Use exactly one structured call, then distrust and verify its coordinates."""

    if provider is None or not signals:
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
            system=(
                "Extract explicit company-entity mentions from this persisted "
                "signal batch. Offsets are zero-based Python character offsets "
                "into the exact content_text. Use context across Slack messages, "
                "but return only literal spans from each focal signal. Abstain "
                "on ordinary language, roles without a named referent, "
                "and uncertainty. Never resolve identity or invent registry IDs."
            ),
            user=json.dumps({"signals": payload}, ensure_ascii=False),
            schema=LearnedMentionBatch,
            temperature=0.0,
            max_tokens=min(4096, 256 + 160 * len(signals)),
        )
    except Exception as exc:  # provider failure must not block deterministic coverage
        return LearnedDiscoveryResult(
            (), "deterministic_fallback", f"{type(exc).__name__}: {exc}"[:500]
        )
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
                "learned_high_confidence_exact_source_span",
                f"learned_type:{item.entity_type}",
            )
        key = (item.signal_id, item.span_start, item.span_end, item.surface.casefold())
        if key in seen:
            continue
        seen.add(key)
        candidates.append(VerifiedMentionCandidate(
            signal_id=item.signal_id,
            surface=item.surface,
            span_start=item.span_start,
            span_end=item.span_end,
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
    "DISCOVERY_VERSION", "LearnedDiscoveryResult", "LearnedMentionBatch",
    "PersistedSignalText", "StructuredDiscoveryProvider",
    "VerifiedMentionCandidate", "discover_batch_mentions",
]
