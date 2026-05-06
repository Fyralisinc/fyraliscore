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


PersonaKind = Literal["reliable", "optimistic", "pessimistic", "flaky"]
ActorRoleFamily = Literal[
    "founder", "exec", "engineering", "product", "design",
    "data_ml", "sales", "customer_success", "marketing",
    "ops", "finance", "legal", "people",
]
CustomerSegment = Literal[
    "enterprise", "mid_market", "smb", "design_partner", "prospect",
]


class ActorProfile(_Base):
    """Rich, named actor profile. When present in SimulationConfig.actor_profiles,
    the simulator uses these instead of generating generic actors from
    PersonalityDistribution + num_actors."""
    actor_id: str
    name: str
    role: str
    role_family: ActorRoleFamily | None = None
    manager_id: str | None = None
    persona_kind: PersonaKind = "reliable"
    reliability_parameter: float | None = Field(default=None, ge=0.0, le=1.0)
    estimation_bias: float | None = Field(default=None, ge=-1.0, le=1.0)
    communication_frequency: float | None = Field(default=None, ge=0.0, le=1.0)
    email: str | None = None
    brief: str = ""


class CustomerProfile(_Base):
    customer_id: str
    company_name: str
    segment: CustomerSegment
    arr_usd: float = Field(ge=0.0)
    initial_health: Literal["healthy", "warning", "degraded", "critical", "churned"] = "healthy"
    primary_contact_actor_ids: list[str] = Field(default_factory=list)


GoalAltitude = Literal["strategic", "operational", "tactical"]


class GoalProfile(_Base):
    goal_id: str
    title: str
    description: str = ""
    owner_actor_id: str
    altitude: GoalAltitude = "operational"
    parent_goal_id: str | None = None
    target_offset_days: int | None = None  # relative to start_date


class DecisionProfile(_Base):
    decision_id: str
    title: str
    decision_text: str
    rationale: str = ""
    decided_offset_days: int = 0


CommitmentOutcome = Literal[
    "will_succeed",
    "will_slip",
    "will_be_cancelled",
    "slipped_but_completed",
    "open",
    "succeeded",
    "cancelled",
]


class CommitmentSeed(_Base):
    """Hand-authored commitment that the simulator materializes at a specific tick."""
    commitment_id: str
    title: str
    owner_actor_id: str
    customer_id: str | None = None
    goal_id: str | None = None
    asserted_duration_days: int = Field(ge=1)
    true_complexity: Literal["low", "med", "high"] = "med"
    intended_outcome: CommitmentOutcome | None = None  # if None, sim samples
    created_offset_days: int = Field(default=0, ge=0)


class CompanyMetadata(_Base):
    company_name: str
    ceo_actor_id: str
    tagline: str = ""
    description: str = ""
    vertical: str = ""


class SignalDensity(_Base):
    """Per-day signal volume target. The simulator scales per-actor emission so
    the total daily signal count lands in [baseline_min, baseline_max] on
    normal days and [crisis_min, crisis_max] inside crisis windows.

    A crisis window covers ±crisis_window_days around any TurbulenceEvent's
    scheduled_at."""
    baseline_min: int = Field(default=100, ge=1)
    baseline_max: int = Field(default=200, ge=1)
    crisis_min: int = Field(default=400, ge=1)
    crisis_max: int = Field(default=500, ge=1)
    crisis_window_days: int = Field(default=14, ge=0)


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

    # Rich-profile extensions (additive; absent → current generic behavior).
    company_metadata: CompanyMetadata | None = None
    actor_profiles: list[ActorProfile] = Field(default_factory=list)
    customer_profiles: list[CustomerProfile] = Field(default_factory=list)
    goals: list[GoalProfile] = Field(default_factory=list)
    decisions: list[DecisionProfile] = Field(default_factory=list)
    commitment_seeds: list[CommitmentSeed] = Field(default_factory=list)
    signal_density: SignalDensity | None = None


class ActorPersona(_Base):
    actor_id: str
    name: str
    role: str
    reliability_parameter: float = Field(ge=0.0, le=1.0)
    estimation_bias: float = Field(ge=-1.0, le=1.0, default=0.0)
    communication_frequency: float = Field(ge=0.0, le=1.0, default=0.5)
    reactive_to_patterns: list[str] = Field(default_factory=list)
    # Rich-profile-derived extras (None when actor was auto-generated).
    role_family: ActorRoleFamily | None = None
    manager_id: str | None = None
    email: str | None = None
    brief: str = ""


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
    # Rich-profile-derived extras (None when commitment was rate-generated).
    title: str | None = None
    customer_id: str | None = None
    goal_id: str | None = None


HealthLevel = Literal["healthy", "warning", "degraded", "critical", "churned"]


class CustomerTruth(_Base):
    customer_id: str
    revenue_value: float
    true_health_trajectory: list[HealthLevel]
    served_by_commitments: list[str] = Field(default_factory=list)
    # Rich-profile-derived extras (None when customer was auto-generated).
    company_name: str | None = None
    segment: CustomerSegment | None = None


class PatternTruth(_Base):
    pattern_id: str
    description: str
    scope: dict[str, Any] = Field(default_factory=dict)
    emergence_at: datetime
    detection_eligible_after: datetime
    false_detection_should_be_flagged_as: str | None = None
