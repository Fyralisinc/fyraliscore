"""Typed mention-detection command and total-fate contracts.

An unresolved phrase is only a search opportunity.  It becomes an
``EntityMention`` only after the extractor binds it to exact evidence
coordinates (or supplies a separately typed implicit-reference basis).  A
rejected opportunity remains durable control state and never impersonates a
source mention.
"""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Self
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from lib.contracts.semantic_commands import SemanticWriteContext
from lib.contracts.kernel import canonical_sha256
from lib.contracts.perception import EntityMention


class _MentionContract(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        str_strip_whitespace=True,
        use_enum_values=False,
    )


def _aware(value: datetime, *, field_name: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return value


class EntityMentionDetectionFate(StrEnum):
    DETECTED = "detected"
    REJECTED_NOT_ANCHORED = "rejected_not_anchored"
    REJECTED_NOT_ENTITY = "rejected_not_entity"
    UNSUPPORTED_IMPLICIT = "unsupported_implicit"


class EntityMentionDetection(_MentionContract):
    detection_id: UUID
    detection_version: int = Field(ge=1)
    tenant_id: UUID
    source_observation_id: UUID
    source_revision_id: str = Field(min_length=1)
    candidate_surface: str = Field(min_length=1)
    context_snapshot_id: UUID
    context_snapshot_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    source_content_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    fate: EntityMentionDetectionFate
    mention: EntityMention | None = None
    reason_codes: tuple[str, ...] = Field(min_length=1)
    extractor_version: str = Field(min_length=1)
    detected_at: datetime

    @field_validator("detected_at")
    @classmethod
    def detected_at_is_aware(cls, value: datetime) -> datetime:
        return _aware(value, field_name="detected_at")

    @model_validator(mode="after")
    def mention_and_fate_are_exact(self) -> Self:
        detected = self.fate is EntityMentionDetectionFate.DETECTED
        if detected != (self.mention is not None):
            raise ValueError("detected fate requires one mention and rejects require none")
        if self.mention is None:
            return self
        if self.mention.mention_id != str(self.detection_id):
            raise ValueError("current mention ID must equal its detection ID")
        if self.mention.mention_version != self.detection_version:
            raise ValueError("mention and detection versions must match")
        if self.mention.context_snapshot_id != str(self.context_snapshot_id):
            raise ValueError("mention must bind the exact context snapshot")
        anchors = (self.mention.primary_anchor, *self.mention.alternate_anchors)
        coordinates = []
        for anchor in anchors:
            coordinate = anchor.coordinate
            if coordinate.evidence_record_id != (
                f"observation:{self.source_observation_id}"
            ):
                raise ValueError("mention anchor must name the focal observation")
            if coordinate.source_revision != self.source_revision_id:
                raise ValueError("mention anchor must name the focal source revision")
            coordinates.append(
                (
                    coordinate.field_path,
                    coordinate.span_start,
                    coordinate.span_end,
                    coordinate.time_range_start,
                    coordinate.time_range_end,
                )
            )
        if len(coordinates) != len(set(coordinates)):
            raise ValueError("mention anchors must have unique evidence coordinates")
        return self

    @property
    def detection_digest(self) -> str:
        return canonical_sha256(self.model_dump(mode="json"))


class EntityMentionHeadExpectation(_MentionContract):
    expected_detection_version: int = Field(ge=0)
    expected_detection_id: UUID | None = None

    @model_validator(mode="after")
    def creation_and_successor_expectations_are_coherent(self) -> Self:
        if (self.expected_detection_version == 0) != (
            self.expected_detection_id is None
        ):
            raise ValueError(
                "new mention detection expects version zero and no prior detection"
            )
        return self


class CommitEntityMentionDetectionCommand(_MentionContract):
    context: SemanticWriteContext
    detection: EntityMentionDetection
    expected: EntityMentionHeadExpectation
    invalidation_keys: tuple[str, ...] = Field(min_length=1)
    prepared_at: datetime

    @field_validator("prepared_at")
    @classmethod
    def prepared_at_is_aware(cls, value: datetime) -> datetime:
        return _aware(value, field_name="prepared_at")

    @model_validator(mode="after")
    def command_is_authorized_and_writer_scoped(self) -> Self:
        if self.context.tenant_id != self.detection.tenant_id:
            raise ValueError("mention command and detection tenants must match")
        scope = self.context.writer_scope_epoch
        if scope.semantic_responsibility != "entity_mention_detection":
            raise ValueError("mention command requires entity_mention_detection scope")
        if scope.writer_owner != "GroundingAnnotationAppender":
            raise ValueError("GroundingAnnotationAppender is the sole mention writer")
        if not self.context.processing_authority.object_ids.permits(
            self.detection.source_revision_id
        ):
            raise ValueError("focal source revision is outside processing authority")
        if self.prepared_at < self.context.issued_at:
            raise ValueError("mention detection cannot precede command issuance")
        if self.prepared_at >= self.context.expires_at:
            raise ValueError("mention detection was prepared after command expiry")
        return self

    @property
    def detection_key(self) -> str:
        mention = self.detection.mention
        anchors = (
            (
                mention.primary_anchor.coordinate.model_dump(mode="json"),
                *(
                    anchor.coordinate.model_dump(mode="json")
                    for anchor in mention.alternate_anchors
                ),
            )
            if mention is not None
            else ()
        )
        return canonical_sha256(
            {
                "tenant_id": str(self.context.tenant_id),
                "source_observation_id": str(self.detection.source_observation_id),
                "source_revision_id": self.detection.source_revision_id,
                "candidate_surface": " ".join(
                    self.detection.candidate_surface.casefold().split()
                ),
                "anchors": anchors,
                "extractor_version": self.detection.extractor_version,
            }
        )

    @property
    def request_digest(self) -> str:
        return canonical_sha256(self.model_dump(mode="json"))


__all__ = [
    "CommitEntityMentionDetectionCommand",
    "EntityMentionDetection",
    "EntityMentionDetectionFate",
    "EntityMentionHeadExpectation",
]
