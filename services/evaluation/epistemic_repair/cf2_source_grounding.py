"""Gold-blind, source-authenticated grounding fixture for CF2 evaluations."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
import re
from uuid import UUID

import asyncpg

from lib.contracts.kernel import BitemporalInterval
from lib.contracts.perception import SourceIdentityBinding
from services.domain.entity_grounding.episode import (
    GroundingCandidateInput,
    GroundingEpisode,
    build_grounding_episode,
    candidate_id_for_ref,
    prepare_context_selection,
)
from services.domain.entity_grounding.mentions import prepare_entity_mention_detection
from services.domain.entity_grounding.learned_discovery import DISCOVERY_VERSION
from services.domain.entity_grounding.repo import EntityGroundingRepo


_EXPLICIT_SCOPE_RE = re.compile(
    r"^(?P<surface>[A-Z][A-Za-z0-9-]+(?: [A-Za-z0-9-]+){1,3})"
    r"(?:(?:, update \d+:)|(?:\s+(?:is|are|was|were)\b))"
)
_SOURCE_THREAD_SCOPE_RE = re.compile(
    r"^In the (?P<surface>[A-Z][A-Za-z0-9-]+(?: [A-Za-z0-9-]+){1,3})"
    r" thread\b"
)
_SOURCE_TYPES = {
    "release": "workstream",
    "migration": "workstream",
    "handoff": "workstream",
    "renewal": "commitment",
}


@dataclass(frozen=True, slots=True)
class SourceAuthenticatedSignal:
    tenant_id: UUID
    observation_id: UUID
    occurred_at: datetime
    source_channel: str
    source_container_id: str
    content_text: str


def build_source_authenticated_grounding_episode(
    signal: SourceAuthenticatedSignal,
) -> GroundingEpisode | None:
    """Resolve one explicit source subject through production grounding rules.

    Identity is derived only from the authenticated source container and the
    exact anchored source surface.  No evaluator storyline or expected thesis
    is accepted as input.
    """

    match = (
        _EXPLICIT_SCOPE_RE.search(signal.content_text)
        or _SOURCE_THREAD_SCOPE_RE.search(signal.content_text)
    )
    if match is None:
        return None
    surface = match.group("surface")
    entity_type = _SOURCE_TYPES.get(surface.rsplit(" ", 1)[-1].casefold())
    if entity_type is None or not signal.source_container_id.strip():
        return None
    slug = re.sub(r"[^a-z0-9]+", "-", surface.casefold()).strip("-")
    canonical_id = f"{entity_type}:{slug}"
    canonical_ref = {"type": entity_type, "id": canonical_id, "version": 1}
    source_system = signal.source_channel.split(":", 1)[0]
    authority_ref = (
        f"source-identity:{source_system}:{signal.source_container_id}:{slug}"
    )
    binding = SourceIdentityBinding(
        binding_id=f"binding:{source_system}:{signal.source_container_id}:{slug}",
        binding_version=1,
        tenant_id=signal.tenant_id,
        source_system=source_system,
        source_native_identifier=f"{signal.source_container_id}:{surface}",
        source_identity_authority_ref=authority_ref,
        canonical_referent_type=entity_type,
        canonical_referent_id=canonical_id,
        canonical_referent_version=1,
        temporal_scope=BitemporalInterval(
            valid_from=signal.occurred_at,
            transaction_from=signal.occurred_at,
        ),
        evidence_refs=(
            f"observation:{signal.observation_id}:content_text:"
            f"{match.start('surface')}:{match.end('surface')}",
            f"source-container:{source_system}:{signal.source_container_id}",
        ),
    )
    decided_at = signal.occurred_at + timedelta(seconds=1)
    context_command, context_outcome = prepare_context_selection(
        tenant_id=signal.tenant_id,
        observation_id=signal.observation_id,
        phrase=surface,
        occurred_at=signal.occurred_at,
        source_channel=signal.source_channel,
        source_space=signal.source_container_id,
        topology_incomplete=False,
        boundary_hypotheses=(),
        context_observations=(),
        selection_dependency_refs=(),
        now=decided_at,
    )
    mention_command = prepare_entity_mention_detection(
        tenant_id=signal.tenant_id,
        observation_id=signal.observation_id,
        phrase=surface,
        content_text=signal.content_text,
        source_channel=signal.source_channel,
        context_command=context_command,
        context_outcome=context_outcome,
        now=decided_at,
        verified_span=(match.start("surface"), match.end("surface")),
        extractor_version=DISCOVERY_VERSION,
    )
    return build_grounding_episode(
        tenant_id=signal.tenant_id,
        observation_id=signal.observation_id,
        phrase=surface,
        occurred_at=signal.occurred_at,
        source_channel=signal.source_channel,
        source_space=signal.source_container_id,
        topology_incomplete=False,
        boundary_hypotheses=(),
        context_observations=(),
        selection_dependency_refs=(),
        candidates=(GroundingCandidateInput(
            canonical_ref=canonical_ref,
            candidate_source="authenticated_normalized_source",
            positive_evidence_refs=(f"observation:{signal.observation_id}",),
            independent_identity_evidence_refs=(authority_ref,),
            exact_mention_match=True,
            decisive_authority_refs=(authority_ref,),
            genuine_source_binding=binding,
        ),),
        model_candidate_id=candidate_id_for_ref(canonical_ref),
        model_canonical_ref=canonical_ref,
        model_confidence=0.99,
        model_reasoning="one exact source-authenticated candidate",
        decision_source="deterministic_source_fixture",
        decision_metadata={"gold_blind": True, "source_authenticated": True},
        high_confidence=0.8,
        review_min=0.5,
        prepared_context_command=context_command,
        prepared_context_outcome=context_outcome,
        prepared_mention_detection_command=mention_command,
        now=decided_at,
    )


async def persist_source_authenticated_grounding(
    conn: asyncpg.Connection,
    signal: SourceAuthenticatedSignal,
) -> GroundingEpisode | None:
    episode = build_source_authenticated_grounding_episode(signal)
    if episode is None:
        return None
    await EntityGroundingRepo(pool=object()).append_episode(  # type: ignore[arg-type]
        episode=episode,
        tenant_id=signal.tenant_id,
        source_observation_id=signal.observation_id,
        phrase=episode.mention_detection_command.detection.candidate_surface,
        conn=conn,
    )
    return episode


__all__ = [
    "SourceAuthenticatedSignal",
    "build_source_authenticated_grounding_episode",
    "persist_source_authenticated_grounding",
]
