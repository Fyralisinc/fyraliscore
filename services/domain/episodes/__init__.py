"""Contracts at the perception-to-episode boundary.

The package does not construct episodes. It defines durable intake and the
immutable values that a future constructor must emit.
"""

from .contracts import (
    EpisodeConstitution,
    EpisodeMembershipAssertion,
    EpisodeSnapshot,
    ReasoningEpisodeInput,
    TopicIntent,
)
from .intake import EpisodeIntakeRepository, PerceptionOutboxRow
from .repo import EpisodeMembershipRow, EpisodeRoutingRepository, EpisodeTopicRow
from .construction import EpisodeConstructionService
from .lifecycle import EpisodeLifecycleRepository
from .query import QueryEpisodeResult, QueryEpisodeService
from .routing import RoutingSignal, TopicCandidate, score_membership
from .service import EpisodeRoutingService

__all__ = [
    "EpisodeConstitution",
    "EpisodeIntakeRepository",
    "EpisodeConstructionService",
    "EpisodeLifecycleRepository",
    "EpisodeMembershipRow",
    "EpisodeRoutingRepository",
    "EpisodeRoutingService",
    "EpisodeTopicRow",
    "EpisodeMembershipAssertion",
    "EpisodeSnapshot",
    "PerceptionOutboxRow",
    "QueryEpisodeResult",
    "QueryEpisodeService",
    "RoutingSignal",
    "TopicCandidate",
    "score_membership",
    "ReasoningEpisodeInput",
    "TopicIntent",
]
