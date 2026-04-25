"""Internal state machines for simulator: ActorState, CommitmentState, CustomerState.

Each state object owns truth that evolves by tick. The simulator holds references
to each state and advances them deterministically given a seeded random.Random.
"""

from __future__ import annotations

import random
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Literal, Optional

from lsob_contracts import (
    ActorPersona,
    CommitmentTruth,
    CustomerTruth,
)

# Ordered health levels so we can walk up/down the ladder.
HEALTH_ORDER: list[str] = ["healthy", "warning", "degraded", "critical", "churned"]


@dataclass
class ActorState:
    """Tracks an actor's persona plus dynamic workload/mood/signal history."""

    persona: ActorPersona
    # current_workload is the number of open commitments this actor owns.
    current_workload: int = 0
    # mood ∈ [-1.0, 1.0]. Negative = stressed; positive = upbeat.
    mood: float = 0.0
    # recent_signal_ids is bounded to last N (used for prompt context / determinism).
    recent_signal_ids: list[str] = field(default_factory=list)
    # Number of shock events this actor has absorbed.
    shocks_absorbed: int = 0
    # Whether actor is active. Exec_departure / layoff may deactivate actors.
    active: bool = True

    def record_signal(self, signal_id: str, max_history: int = 20) -> None:
        self.recent_signal_ids.append(signal_id)
        if len(self.recent_signal_ids) > max_history:
            self.recent_signal_ids = self.recent_signal_ids[-max_history:]

    def adjust_mood(self, delta: float) -> None:
        self.mood = max(-1.0, min(1.0, self.mood + delta))

    def will_emit_today(self, rng: random.Random) -> bool:
        """Whether this actor emits any signal on this tick."""
        if not self.active:
            return False
        # Base probability from communication_frequency.
        base = self.persona.communication_frequency
        # Stress slightly boosts chatter; extreme negative mood silences.
        if self.mood < -0.6:
            base *= 0.5
        elif self.mood < 0:
            base *= 1.15
        # Workload drives additional updates.
        base += 0.05 * min(self.current_workload, 5)
        return rng.random() < min(base, 0.95)


@dataclass
class CommitmentState:
    """Tracks progress vs perception plus hidden complexity for one commitment."""

    truth: CommitmentTruth
    # Ground-truth progress 0..1.
    true_progress: float = 0.0
    # What the owner/team currently believes (can be wrong).
    perceived_progress: float = 0.0
    # Has the slip been publicly acknowledged yet?
    slip_acknowledged: bool = False
    # Has it been resolved in sim time?
    resolved: bool = False
    # Tick index this commitment was created.
    created_tick: int = 0

    # Derived expectations — do not mutate.
    @property
    def owner_actor_id(self) -> str:
        return self.truth.owner_actor_id

    def daily_true_progress(self) -> float:
        """How much true progress we gain per tick, on average."""
        if self.truth.true_duration_days <= 0:
            return 1.0
        return 1.0 / float(self.truth.true_duration_days)

    def daily_perceived_progress(self, actor: ActorState) -> float:
        """Perceived progress rate reflects optimism/pessimism via estimation_bias."""
        if self.truth.asserted_duration_days <= 0:
            return 1.0
        rate = 1.0 / float(self.truth.asserted_duration_days)
        # estimation_bias > 0 ⇒ optimistic: overstate perceived velocity.
        bias = actor.persona.estimation_bias
        return rate * (1.0 + bias)

    def advance(self, rng: random.Random, actor: ActorState, tick: int) -> None:
        """Advance one tick. Returns nothing; mutates in place."""
        if self.resolved:
            return
        # True progress, with reliability-driven noise.
        reliability = actor.persona.reliability_parameter
        jitter = (rng.random() - 0.5) * (1.0 - reliability) * 0.4
        tp_delta = self.daily_true_progress() * (1.0 + jitter)
        self.true_progress = min(1.0, self.true_progress + max(0.0, tp_delta))
        # Perceived progress — biased toward asserted duration.
        pp_delta = self.daily_perceived_progress(actor)
        self.perceived_progress = min(1.0, self.perceived_progress + max(0.0, pp_delta))
        # Slip acknowledgement once perception cannot keep up with truth window.
        if (
            not self.slip_acknowledged
            and self.perceived_progress >= 0.95
            and self.true_progress < 0.6
        ):
            self.slip_acknowledged = True
            actor.adjust_mood(-0.15)
        # Resolution.
        if self.true_progress >= 1.0:
            self.resolved = True

    def is_at_risk(self) -> bool:
        """Is this commitment likely to slip or fail?"""
        if self.resolved:
            return False
        if self.truth.true_outcome in ("will_slip", "will_be_cancelled", "slipped_but_completed"):
            return True
        # Divergence between perceived and true.
        return (self.perceived_progress - self.true_progress) > 0.25


@dataclass
class CustomerState:
    """Customer health trajectory driven by its served_by commitments."""

    truth: CustomerTruth
    # Current tick-level health (we evolve this, not the trajectory list).
    current_health: str = "healthy"
    # Tick-indexed history of health values for audit / ground truth.
    health_history: list[str] = field(default_factory=list)

    def current_health_index(self) -> int:
        return HEALTH_ORDER.index(self.current_health)

    def advance(
        self,
        rng: random.Random,
        serving_commitments: list[CommitmentState],
        tick: int,
    ) -> None:
        """Evolve health; tends toward healthy when commitments are on track.

        Each at-risk commitment pushes health slightly worse with prob proportional
        to its divergence. Otherwise, customer drifts toward healthy.
        """
        idx = self.current_health_index()
        pressure = 0.0
        for c in serving_commitments:
            if c.resolved:
                continue
            divergence = max(0.0, c.perceived_progress - c.true_progress)
            pressure += divergence
        # Translate pressure into a probability of degrading.
        if pressure > 0.25 and rng.random() < min(0.4, pressure):
            idx = min(len(HEALTH_ORDER) - 1, idx + 1)
        elif pressure < 0.05 and rng.random() < 0.08:
            idx = max(0, idx - 1)
        self.current_health = HEALTH_ORDER[idx]
        self.health_history.append(self.current_health)

    def apply_shock(self, rng: random.Random, magnitude: float) -> None:
        """A turbulence event pushes customers toward unhealthy proportional to magnitude."""
        idx = self.current_health_index()
        if rng.random() < magnitude:
            idx = min(len(HEALTH_ORDER) - 1, idx + 1)
        self.current_health = HEALTH_ORDER[idx]
