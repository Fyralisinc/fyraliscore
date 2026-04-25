"""Simulation-side contracts: configs, actor/commitment/customer truth, turbulence."""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field


class _Base(BaseModel):
    model_config = ConfigDict(extra="forbid")


class PersonalityDistribution(_Base):
    reliable: float = 0.5
    optimistic: float = 0.3
    pessimistic: float = 0.1
    flaky: float = 0.1

    def validate_sum(self) -> None:
        total = self.reliable + self.optimistic + self.pessimistic + self.flaky
        if abs(total - 1.0) > 0.01:
            raise ValueError(f"personality distribution must sum to 1.0, got {total}")


class TurbulenceKind(str, Enum):
    exec_departure = "exec_departure"
    pivot = "pivot"
    layoff = "layoff"
    major_customer_loss = "major_customer_loss"
    reorg = "reorg"


class TurbulenceEvent(_Base):
    event_id: str
    kind: TurbulenceKind
    scheduled_at: datetime
    magnitude: float = Field(ge=0.0, le=1.0, default=0.5)
    payload: dict[str, Any] = Field(default_factory=dict)


class SimulationConfig(_Base):
    company_id: str
    num_actors: int = Field(ge=1)
    actor_personality_distribution: PersonalityDistribution = Field(
        default_factory=PersonalityDistribution
    )
    commitment_generation_rate: float = Field(ge=0.0, default=0.05)
    customer_count: int = Field(ge=0, default=20)
    turbulence_events: list[TurbulenceEvent] = Field(default_factory=list)
    seed: int = 42
    start_date: datetime
    duration_months: int = Field(ge=1, le=36, default=12)


class ActorPersona(_Base):
    actor_id: str
    name: str
    role: str
    reliability_parameter: float = Field(ge=0.0, le=1.0)
    estimation_bias: float = Field(ge=-1.0, le=1.0, default=0.0)
    communication_frequency: float = Field(ge=0.0, le=1.0, default=0.5)
    reactive_to_patterns: list[str] = Field(default_factory=list)


CommitmentOutcome = Literal[
    "will_succeed",
    "will_slip",
    "will_be_cancelled",
    "slipped_but_completed",
    "open",
    "succeeded",
    "cancelled",
]


class CommitmentTruth(_Base):
    commitment_id: str
    owner_actor_id: str
    created_at: datetime
    asserted_duration_days: int
    true_duration_days: int
    true_complexity: Literal["low", "med", "high"]
    true_outcome: CommitmentOutcome
    resolution_event_at: datetime | None = None
    hidden_dependencies: list[str] = Field(default_factory=list)


HealthLevel = Literal["healthy", "warning", "degraded", "critical", "churned"]


class CustomerTruth(_Base):
    customer_id: str
    revenue_value: float
    true_health_trajectory: list[HealthLevel]
    served_by_commitments: list[str] = Field(default_factory=list)


class PatternTruth(_Base):
    pattern_id: str
    description: str
    scope: dict[str, Any] = Field(default_factory=dict)
    emergence_at: datetime
    detection_eligible_after: datetime
    false_detection_should_be_flagged_as: str | None = None
