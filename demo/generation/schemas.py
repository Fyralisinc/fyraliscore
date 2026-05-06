"""Pydantic schemas for each LLM call in the demo-generation pipeline.

Wrap-types like `ActorBatch` exist because most providers return
better-shaped JSON when the top-level value is an object with a single
`items` field rather than a bare array.
"""
from __future__ import annotations

from typing import Any, Literal, Optional

from pydantic import BaseModel, Field


# ---------------------------------------------------------------------
# Actors
# ---------------------------------------------------------------------


class GeneratedActor(BaseModel):
    id: str
    name: str
    role: str
    manager_id: Optional[str] = None
    personality_brief: str = ""
    email: Optional[str] = None


class ActorBatch(BaseModel):
    items: list[GeneratedActor]


# ---------------------------------------------------------------------
# Customers (mapped to Resources at SQL emit time)
# ---------------------------------------------------------------------


CustomerSegment = Literal[
    "enterprise", "mid_market", "smb", "design_partner", "prospect",
]
CustomerHealth = Literal["healthy", "watching", "at_risk", "escalating"]


class GeneratedCustomer(BaseModel):
    id: str
    company_name: str
    arr_usd: float
    segment: CustomerSegment
    current_health: CustomerHealth
    primary_contacts: list[str] = Field(default_factory=list)


class CustomerBatch(BaseModel):
    items: list[GeneratedCustomer]


# ---------------------------------------------------------------------
# Goals
# ---------------------------------------------------------------------


GoalAltitude = Literal["strategic", "operational", "tactical"]


class GeneratedGoal(BaseModel):
    id: str
    title: str
    description: str = ""
    owner_id: str
    target_date: Optional[str] = None       # ISO 8601
    parent_goal_id: Optional[str] = None
    altitude: GoalAltitude = "operational"


class GoalBatch(BaseModel):
    items: list[GeneratedGoal]


# ---------------------------------------------------------------------
# Decisions
# ---------------------------------------------------------------------


class GeneratedDecision(BaseModel):
    id: str
    title: str
    decision_text: str
    rationale: str
    scope: dict[str, Any] = Field(default_factory=dict)
    revisit_triggers: list[str] = Field(default_factory=list)


class DecisionBatch(BaseModel):
    items: list[GeneratedDecision]


# ---------------------------------------------------------------------
# Commitments
# ---------------------------------------------------------------------


CommitmentState = Literal[
    "proposed", "active", "at_risk", "blocked", "done", "closed",
]


class GeneratedCommitment(BaseModel):
    id: str
    title: str
    owner_id: str
    contributors: list[str] = Field(default_factory=list)
    state: CommitmentState = "active"
    due_date: Optional[str] = None
    contributes_to_goal_id: Optional[str] = None
    depends_on: list[str] = Field(default_factory=list)
    constrained_by_decision_ids: list[str] = Field(default_factory=list)
    served_by_customer_id: Optional[str] = None


class CommitmentBatch(BaseModel):
    items: list[GeneratedCommitment]


# ---------------------------------------------------------------------
# Signals (Observations)
# ---------------------------------------------------------------------


class EntityMention(BaseModel):
    type: Literal["actor", "commitment", "customer", "decision", "goal"]
    id: str


class GeneratedSignal(BaseModel):
    id: str
    source_channel: str
    source_ref: str
    author_id: str
    occurred_at: str            # ISO 8601 timestamp
    content_text: str
    entities_mentioned: list[EntityMention] = Field(default_factory=list)


class SignalBatch(BaseModel):
    items: list[GeneratedSignal]


# ---------------------------------------------------------------------
# Recommendations (one per LLM call)
# ---------------------------------------------------------------------


class TargetActRef(BaseModel):
    type: Literal["commitment", "goal", "decision", "actor"]
    id: str


class GeneratedRecommendation(BaseModel):
    id: str
    proposition_text: str
    target_act_ref: TargetActRef
    proposed_change: dict[str, Any] = Field(default_factory=dict)
    expected_impact_usd: float
    supporting_observation_ids: list[str] = Field(default_factory=list)
    supporting_model_ids: list[str] = Field(default_factory=list)
    target_actor_id: str


# ---------------------------------------------------------------------
# Models — the substrate's epistemic beliefs (state, relation,
# prediction, pattern, pattern_instance, capability_assessment,
# hypothesis, concern, market_assessment, environmental_trend).
# ---------------------------------------------------------------------


ModelKind = Literal[
    "state", "relation", "prediction", "pattern", "pattern_instance",
    "capability_assessment", "hypothesis", "concern",
    "market_assessment", "environmental_trend",
]


class GeneratedModel(BaseModel):
    id: str
    kind: ModelKind
    natural: str                                    # one-sentence prose
    proposition: dict[str, Any] = Field(default_factory=dict)
                                                    # extra structured fields
    confidence: float = 0.7                         # [0.05, 0.95]
    scope_actor_ids: list[str] = Field(default_factory=list)
    scope_entities: list[dict[str, Any]] = Field(default_factory=list)
                                                    # [{type, id}]
    scope_temporal: dict[str, Any] = Field(default_factory=lambda: {"window": "current"})
    falsifier: Optional[dict[str, Any]] = None      # {condition, threshold, observable_via}
    supporting_observation_ids: list[str] = Field(default_factory=list)
    supporting_model_ids: list[str] = Field(default_factory=list)
    evaluate_at: Optional[str] = None               # ISO 8601 — for predictions


# ---------------------------------------------------------------------
# Resources — capacity primitives (human pods, financial pools, technical
# platforms). Customers are also resources at SQL emit time, but those
# are derived from `GeneratedCustomer`; this type is for the capacity
# class that feeds the Resource view in Structure.
# ---------------------------------------------------------------------


ResourceKind = Literal["human", "financial", "technical", "time"]
ResourceUtilizationState = Literal["available", "deployed", "constrained"]
ResourceControllability = Literal["owned", "shared", "leased"]
ResourceTemporal = Literal["permanent", "time_limited", "ephemeral"]


class GeneratedResource(BaseModel):
    id: str
    kind: ResourceKind
    identity: str               # short canonical name e.g. "pod:engineering"
    label: str                  # display label e.g. "Engineering pod"
    description: str = ""
    capacity: float             # numeric capacity in `unit`
    unit: str                   # "FTE", "USD", "engineer-weeks", "GPU-hours"
    utilization_state: ResourceUtilizationState = "available"
    controllability: ResourceControllability = "owned"
    temporal_character: ResourceTemporal = "permanent"
    metadata: dict[str, Any] = Field(default_factory=dict)


class GeneratedResourceDeployment(BaseModel):
    """Bridge row — a single commitment consuming a slice of a resource."""
    resource_id: str
    commitment_id: str
    deployed_quantity: float    # in the resource's `unit`


# ---------------------------------------------------------------------
# Container — the validated bundle that sql_emit and validate operate on
# ---------------------------------------------------------------------


class GeneratedBundle(BaseModel):
    """All entities for one company. Produced by the orchestrator,
    consumed by validate.py and sql_emit.py."""
    company_id: str
    ceo_actor_id: str
    actors: list[GeneratedActor] = Field(default_factory=list)
    customers: list[GeneratedCustomer] = Field(default_factory=list)
    goals: list[GeneratedGoal] = Field(default_factory=list)
    decisions: list[GeneratedDecision] = Field(default_factory=list)
    commitments: list[GeneratedCommitment] = Field(default_factory=list)
    signals: list[GeneratedSignal] = Field(default_factory=list)
    models: list[GeneratedModel] = Field(default_factory=list)
    recommendations: list[GeneratedRecommendation] = Field(default_factory=list)
    resources: list[GeneratedResource] = Field(default_factory=list)
    resource_deployments: list[GeneratedResourceDeployment] = Field(default_factory=list)


__all__ = [
    "GeneratedActor", "ActorBatch",
    "GeneratedCustomer", "CustomerBatch",
    "GeneratedGoal", "GoalBatch",
    "GeneratedDecision", "DecisionBatch",
    "GeneratedCommitment", "CommitmentBatch",
    "GeneratedSignal", "SignalBatch", "EntityMention",
    "GeneratedModel", "ModelKind",
    "GeneratedRecommendation", "TargetActRef",
    "GeneratedResource", "GeneratedResourceDeployment",
    "GeneratedBundle",
]
