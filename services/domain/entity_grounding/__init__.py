"""Durable entity-grounding assessment and admission sidecars."""

from services.domain.entity_grounding.episode import (
    ContextObservationInput,
    GroundingCandidateInput,
    GroundingEpisode,
    build_grounding_episode,
    candidate_id_for_ref,
)
from services.domain.entity_grounding.mentions import prepare_entity_mention_detection
from services.domain.entity_grounding.mention_fates import (
    MentionFateCoverage,
    ensure_observation_mention_fates,
    ensure_persisted_observation_mention_fates,
)
from services.domain.entity_grounding.repo import EntityGroundingRepo

__all__ = [
    "ContextObservationInput",
    "EntityGroundingRepo",
    "GroundingCandidateInput",
    "GroundingEpisode",
    "MentionFateCoverage",
    "build_grounding_episode",
    "candidate_id_for_ref",
    "prepare_entity_mention_detection",
    "ensure_observation_mention_fates",
    "ensure_persisted_observation_mention_fates",
]
