"""Minimal deterministic source-semantic constructor for the first vertical."""

from __future__ import annotations

import re

from lib.contracts.kernel import canonical_sha256
from lib.contracts.perception import (
    EvidenceCoordinate,
    MentionAnchorKind,
    Modality,
    SemanticArgument,
    SemanticFrameCandidate,
    SourceAssertion,
    SourceAssertionKind,
    SpeechActCandidate,
    SpeechActKind,
)
from lib.contracts.source_semantics import GroundedSourceSemanticBundle
from services.domain.source_semantics.repo import GroundingTraceContext


_EXTRACTOR_VERSION = "grounded-asserted-report-deterministic-v1"
_QUESTION_PREFIX = re.compile(
    r"^(?:is|are|was|were|do|does|did|can|could|will|would|should|has|have|had|"
    r"who|what|when|where|why|how)\b",
    re.IGNORECASE,
)
_SUPPORTED_REPORT_SUFFIX = re.compile(
    r"^\s+(?:is|are|was|were)\s+(?:not\s+)?"
    r"(?:blocked|approved|ready|delayed|complete)\.?$",
    re.IGNORECASE,
)


def primary_mention_is_supported_subject(
    grounding: GroundingTraceContext,
) -> bool:
    """Require the durable primary span itself to be the report subject."""

    anchor = grounding.mention.primary_anchor
    coordinate = anchor.coordinate
    start = coordinate.span_start
    end = coordinate.span_end
    expected_source_ref = f"observation:{grounding.source_observation_id}"
    if (
        anchor.kind is not MentionAnchorKind.EXPLICIT
        or coordinate.evidence_record_id != expected_source_ref
        or coordinate.source_object_id != expected_source_ref
        or coordinate.field_path != "content_text"
        or start is None
        or end is None
        or not anchor.surface_form
        or start < 0
        or end > len(grounding.content_text)
        or grounding.content_text[start:end] != anchor.surface_form
        or grounding.content_text[:start].strip()
    ):
        return False
    return bool(_SUPPORTED_REPORT_SUFFIX.fullmatch(grounding.content_text[end:]))


class DeterministicSourceSemanticExtractor:
    """Construct only the source semantics needed by the initial belief lane."""

    def extract(self, grounding: GroundingTraceContext) -> GroundedSourceSemanticBundle:
        text = grounding.content_text.strip()
        if not text:
            raise ValueError("source-semantic extraction requires non-empty source text")
        start = grounding.content_text.find(text)
        end = start + len(text)
        identity = canonical_sha256(
            {
                "tenant_id": str(grounding.tenant_id),
                "grounding_trace_id": str(grounding.trace_id),
                "source_observation_id": str(grounding.source_observation_id),
                "content_text": grounding.content_text,
                "extractor_version": _EXTRACTOR_VERSION,
            }
        )
        assertion_id = f"source-assertion:{identity[:32]}"
        is_question = text.endswith("?") or bool(_QUESTION_PREFIX.match(text))
        is_supported_report = (
            not is_question and primary_mention_is_supported_subject(grounding)
        )
        assertion = SourceAssertion(
            assertion_id=assertion_id,
            assertion_version=1,
            context_snapshot_id=str(grounding.context_snapshot_id),
            coordinates=(
                EvidenceCoordinate(
                    evidence_record_id=(
                        f"observation:{grounding.source_observation_id}"
                    ),
                    source_system=grounding.source_channel.split(":", 1)[0],
                    source_object_id=(
                        f"observation:{grounding.source_observation_id}"
                    ),
                    source_revision=(
                        f"observation:{grounding.source_observation_id}:v1"
                    ),
                    field_path="content_text",
                    span_start=start,
                    span_end=end,
                ),
            ),
            current_speaker_or_author=(
                f"source-author:{grounding.source_channel}"
            ),
            kind=(
                SourceAssertionKind.ASKED
                if is_question
                else (
                    SourceAssertionKind.ASSERTED
                    if is_supported_report
                    else SourceAssertionKind.HYPOTHESIZED
                )
            ),
            expressed_content=text,
            source_status="ordinary source expression",
            extractor_version=_EXTRACTOR_VERSION,
            uncertainty=0.1,
        )
        frame = SemanticFrameCandidate(
            frame_id=f"semantic-frame:{identity[:32]}",
            frame_version=1,
            source_assertion_id=assertion_id,
            predicate_or_event_type=self._predicate(text),
            arguments=(
                SemanticArgument(
                    argument_id=f"semantic-argument:{identity[:32]}:subject",
                    role="subject",
                    mention_anchor_refs=(
                        grounding.mention.primary_anchor.anchor_id,
                    ),
                    confidence=grounding.mention.detection_confidence,
                ),
            ),
            negated=self._is_negated(text),
            modality=(
                Modality.POSSIBLE
                if re.search(r"\b(?:may|might|possibly|could)\b", text, re.I)
                else Modality.ACTUAL
            ),
            confidence=0.9,
            extractor_version=_EXTRACTOR_VERSION,
        )
        speech_act = SpeechActCandidate(
            speech_act_id=f"speech-act:{identity[:32]}",
            source_assertion_id=assertion_id,
            distribution={
                (
                    SpeechActKind.QUESTION
                    if is_question
                    else (
                        SpeechActKind.REPORT
                        if is_supported_report
                        else SpeechActKind.HYPOTHETICAL
                    )
                ): 1.0
            },
            authority_cue_refs=(),
            extractor_version=_EXTRACTOR_VERSION,
        )
        return GroundedSourceSemanticBundle(
            tenant_id=grounding.tenant_id,
            grounding_trace_id=grounding.trace_id,
            source_assertion=assertion,
            semantic_frame=frame,
            speech_act=speech_act,
        )

    @staticmethod
    def _predicate(text: str) -> str:
        lowered = text.casefold()
        for predicate in ("blocked", "approved", "ready", "delayed", "complete"):
            if re.search(rf"\b{predicate}\b", lowered):
                return predicate
        return "source_report"

    @staticmethod
    def _is_negated(text: str) -> bool:
        return bool(re.search(r"\b(?:not|never|no longer)\b", text, re.IGNORECASE))


__all__ = [
    "DeterministicSourceSemanticExtractor",
    "primary_mention_is_supported_subject",
]
