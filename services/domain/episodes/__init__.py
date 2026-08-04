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

__all__ = [
    "EpisodeConstitution",
    "EpisodeIntakeRepository",
    "EpisodeMembershipAssertion",
    "EpisodeSnapshot",
    "PerceptionOutboxRow",
    "ReasoningEpisodeInput",
    "TopicIntent",
]
