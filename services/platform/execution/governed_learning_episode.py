"""Canonical-scope semantic episodes compiled from persisted batch evidence.

This module is deliberately a thin execution contract.  It does not perform
mention discovery, entity resolution, retrieval, or truth admission; it only
preserves their governed outputs while removing transport-batch identity from
the semantic grouping key.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime
from hashlib import sha256
import json
from typing import Any, Iterable, Literal, Mapping
from uuid import UUID


CoordinateAuthority = Literal["resolved", "provisional", "unresolved"]


@dataclass(frozen=True, slots=True)
class GovernedObservationAssertion:
    tenant_id: UUID
    observation_id: UUID
    occurred_at: datetime
    source_channel: str
    assertion_text: str
    evidence_address: str
    evidence_field_path: str | None
    evidence_span_start: int | None
    evidence_span_end: int | None
    governed_surface: str | None
    canonical_ref: str | None
    coordinate_authority: CoordinateAuthority
    detection_id: UUID | None
    uncertainty: tuple[str, ...] = ()

    def as_payload(self) -> dict[str, Any]:
        payload = asdict(self)
        payload.update({
            "tenant_id": str(self.tenant_id),
            "observation_id": str(self.observation_id),
            "occurred_at": self.occurred_at.isoformat(),
            "detection_id": str(self.detection_id) if self.detection_id else None,
        })
        return payload


@dataclass(frozen=True, slots=True)
class GovernedLearningEpisode:
    episode_id: str
    tenant_id: UUID
    canonical_ref: str | None
    assertions: tuple[GovernedObservationAssertion, ...]
    temporal_start: datetime
    temporal_end: datetime
    uncertainty: tuple[str, ...] = ()

    def as_payload(self) -> dict[str, Any]:
        return {
            "episode_id": self.episode_id,
            "tenant_id": str(self.tenant_id),
            "canonical_ref": self.canonical_ref,
            "assertions": [item.as_payload() for item in self.assertions],
            "temporal_start": self.temporal_start.isoformat(),
            "temporal_end": self.temporal_end.isoformat(),
            "uncertainty": list(self.uncertainty),
        }


def build_governed_learning_episodes(
    *,
    tenant_id: UUID,
    observations: Iterable[Mapping[str, Any]],
    governed_mentions: Mapping[UUID, Iterable[Mapping[str, Any]]],
) -> tuple[GovernedLearningEpisode, ...]:
    """Build deterministic canonical-reference-first semantic episodes.

    Each exact governed mention contributes one assertion to its canonical
    episode. Observations without a usable coordinate remain explicit
    unresolved singleton episodes; they are never coerced into a label group.
    """

    grouped: dict[str, list[GovernedObservationAssertion]] = {}
    for row in observations:
        observation_id = _uuid(row.get("id"))
        occurred_at = row.get("occurred_at")
        if observation_id is None or not isinstance(occurred_at, datetime):
            continue
        source_channel = str(row.get("source_channel") or "")
        text = str(row.get("content_text") or "")
        mentions = sorted(
            (
                mention for mention in governed_mentions.get(observation_id, ())
                if isinstance(mention, Mapping)
            ),
            key=lambda item: (
                str(item.get("canonical_ref") or ""),
                str(item.get("surface") or "").casefold(),
                str(item.get("detection_id") or ""),
            ),
        )
        emitted = False
        for mention in mentions:
            canonical_ref = _canonical_ref(mention.get("canonical_ref"))
            surface = str(mention.get("surface") or "").strip()
            if canonical_ref is None or not surface:
                continue
            authority = _authority(mention.get("authority"))
            assertion = GovernedObservationAssertion(
                tenant_id=tenant_id,
                observation_id=observation_id,
                occurred_at=occurred_at,
                source_channel=source_channel,
                assertion_text=text,
                evidence_address=f"observation:{observation_id}:content_text",
                evidence_field_path=_text_or_none(mention.get("field_path")),
                evidence_span_start=_integer_or_none(mention.get("span_start")),
                evidence_span_end=_integer_or_none(mention.get("span_end")),
                governed_surface=surface,
                canonical_ref=canonical_ref,
                coordinate_authority=authority,
                detection_id=_uuid(mention.get("detection_id")),
                uncertainty=(
                    ("provisional_entity_coordinate",)
                    if authority == "provisional" else ()
                ),
            )
            grouped.setdefault(canonical_ref, []).append(assertion)
            emitted = True
        if not emitted:
            assertion = GovernedObservationAssertion(
                tenant_id=tenant_id,
                observation_id=observation_id,
                occurred_at=occurred_at,
                source_channel=source_channel,
                assertion_text=text,
                evidence_address=f"observation:{observation_id}:content_text",
                evidence_field_path="content_text",
                evidence_span_start=None,
                evidence_span_end=None,
                governed_surface=None,
                canonical_ref=None,
                coordinate_authority="unresolved",
                detection_id=None,
                uncertainty=("missing_governed_entity_coordinate",),
            )
            grouped.setdefault(f"unresolved:{observation_id}", []).append(assertion)

    episodes: list[GovernedLearningEpisode] = []
    for key, members in sorted(grouped.items()):
        assertions = tuple(sorted(
            members,
            key=lambda item: (
                item.occurred_at, str(item.observation_id),
                item.governed_surface or "", str(item.detection_id or ""),
            ),
        ))
        canonical_ref = None if key.startswith("unresolved:") else key
        uncertainty = tuple(sorted({
            value for assertion in assertions for value in assertion.uncertainty
        }))
        episodes.append(GovernedLearningEpisode(
            episode_id=_episode_id(tenant_id, key),
            tenant_id=tenant_id,
            canonical_ref=canonical_ref,
            assertions=assertions,
            temporal_start=min(item.occurred_at for item in assertions),
            temporal_end=max(item.occurred_at for item in assertions),
            uncertainty=uncertainty,
        ))
    return tuple(episodes)


def _episode_id(tenant_id: UUID, coordinate: str) -> str:
    body = json.dumps(
        {"tenant_id": str(tenant_id), "coordinate": coordinate},
        sort_keys=True, separators=(",", ":"),
    )
    return f"GLE_{sha256(body.encode()).hexdigest()[:24]}"


def _authority(raw: Any) -> CoordinateAuthority:
    value = str(raw or "").strip().casefold()
    if value == "resolved_for_consumer":
        return "resolved"
    if value == "provisional_detection":
        return "provisional"
    return "unresolved"


def _canonical_ref(raw: Any) -> str | None:
    value = str(raw or "").strip()
    if ":" not in value:
        return None
    kind, identifier = value.split(":", 1)
    if not kind.strip() or not identifier.strip():
        return None
    return f"{kind.strip().casefold()}:{identifier.strip()}"


def _uuid(raw: Any) -> UUID | None:
    try:
        return raw if isinstance(raw, UUID) else UUID(str(raw))
    except (TypeError, ValueError):
        return None


def _text_or_none(raw: Any) -> str | None:
    value = str(raw or "").strip()
    return value or None


def _integer_or_none(raw: Any) -> int | None:
    return raw if isinstance(raw, int) and not isinstance(raw, bool) else None


__all__ = [
    "CoordinateAuthority",
    "GovernedLearningEpisode",
    "GovernedObservationAssertion",
    "build_governed_learning_episodes",
]
