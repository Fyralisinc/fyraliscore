"""
lib/shared/types.py — Pydantic v2 models mirroring SCHEMA-LOCK.md.

Every table in S1-S6 gets a corresponding `*Row` model whose fields
match the SQL column name, type, and nullability exactly. These
types are what asyncpg helpers (`lib/shared/db.py`) hydrate into
when returning query results.

Rules:
- Field names EXACT to column names, including `natural`.
- Enum-like columns use Literal[...] so validation fails fast.
- JSONB columns use dict | list typed by what the spec says.
- UUID columns use pydantic's UUID4 *not allowed* — we use `UUID`
  (v7 would be nice but Pydantic UUID type is version-agnostic).
- Nullable columns are typed Optional[...] with default None.
- Columns with a SQL default but NOT NULL stay required in the
  *Create model and optional in the *Row model (the DB fills them).
"""
from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


# ---------------------------------------------------------------------
# Enum literal types — lock the column's set of legal values.
# ---------------------------------------------------------------------

ObservationKind = Literal[
    "signal",
    "state_change",
    "anomaly_flagged",
    "contestation",
    "prediction_resolution",
    "transaction",
]

TrustTierValue = Literal[
    "authoritative",
    "attested_agent",
    "authoritative_external",
    "reputable",
    "inferential",
    "inferential_external",
    "unvetted",
    # Synthesized observations carry trust derived from their parent
    # signal (demo snapshots, replay tooling). Schema-side this is just
    # text; the literal needs to mirror what the DB actually stores or
    # ObservationRow validation crashes pathway-C retrieval.
    "derived",
]

ModelStatus = Literal["active", "archived", "superseded", "contested_false"]

ModelArchiveReason = Literal[
    "decay",
    "falsifier_triggered",
    "contested_incorrect",
    "contested_reading_incorrect",
    "superseded",
    "manual",
    "resolved_confirmed",
    "resolved_violated",
    "severe_drift",
    "deprecated",   # Post-Wave-0 A3: replaces pseudo-code's deprecated_at
    # Recommendation-kind lifecycle reasons (Stage 1 decision support):
    "acted_upon",          # the recommended change was applied
    "dismissed_by_user",   # the user explicitly rejected the recommendation
    "situation_resolved",  # the underlying condition no longer holds
]

ModelStatusNoteKind = Literal["first_person_override", "manual", "system"]

PropositionKind = Literal[
    "state", "relation", "prediction", "pattern", "pattern_instance",
    "capability_assessment", "hypothesis", "concern",
    "market_assessment", "environmental_trend",
    "recommendation",
]

GoalState = Literal["active", "paused", "achieved", "abandoned"]
GoalAltitude = Literal["strategic", "operational", "tactical"]
GoalCachedHealth = Literal["healthy", "warning", "degraded", "critical"]

CommitmentState = Literal[
    "proposed",
    "active",
    "blocked",
    "paused",
    "doneunverified",
    "doneverified",
    "closed",
]
AmbitionLevel = Literal["base", "stretch", "aspirational"]

DecisionState = Literal["drafted", "active", "revisited", "archived"]

ResourceKind = Literal[
    "financial",
    "ip",
    "relational",
    "capacity",
    "infrastructure",
    "regulatory",
]
ResourceUtilizationState = Literal[
    "available", "deployed", "committed", "depleted", "expired"
]
ResourceControllability = Literal[
    "owned", "joint", "borrowed", "leased", "limited"
]
ResourceTemporalCharacter = Literal[
    "permanent", "time_limited", "renewable", "consumable"
]
ResourceTransactionType = Literal[
    "acquire", "deploy", "release", "spend", "strengthen", "weaken", "expire"
]

ActorType = Literal["human_internal", "human_external", "ai_agent"]
ActorStatus = Literal["active", "inactive", "departed"]


# ---------------------------------------------------------------------
# Model-edge enums (S1 of self-organizing-substrate plan, migration
# 0031_model_edges.sql + lib/shared/edge_registry.py).
# ---------------------------------------------------------------------

# Edge lifecycle. Edges become 'inert' when either endpoint is
# archived (set in the same transaction as the Model archive); they
# stay queryable for audit but don't appear in active-only retrieval.
# 'disputed' is reserved for future contradiction-resolution flows.
EdgeStatus = Literal["active", "inert", "disputed"]

# Provenance: who wrote the edge. New v1 producers populate the first
# four; reconciler / falsifier_overlap / cascade are reserved for
# follow-on stages and the contradicts producer.
EdgeDetectedBy = Literal[
    "llm_explicit",       # LLM emitted the edge in a claim_op
    "precipitation",      # T4 pattern promotion produced an instance_of edge
    "manual",             # operator / debug path
    "backfill",           # one-shot scripts/backfill_model_edges.py
    "reconciler",         # reserved (Stage 4: contradicts producer)
    "falsifier_overlap",  # reserved (future)
    "cascade",            # reserved (future: derived from another edge)
]

# Edge kind discriminator. The registry in lib/shared/edge_registry.py
# is the single source of truth for per-kind semantics (DAG scope,
# cascade callbacks, weight rules, mutually-exclusive-with). v1
# enables writes for the first four; the rest are reserved names for
# follow-on stages and rejected at the repo layer until a producer
# ships.
EdgeKind = Literal[
    "supports",                   # source supports target (was supporting_model_ids)
    "contributes_to_resolution",  # source's state resolves target prediction (was contributing_models)
    "instance_of",                # source is an instance of target pattern (was pattern back-link)
    "superseded_by",              # source was replaced by target (was archive_reason='superseded')
    "contradicts",                # reserved: symmetric, polarity-inverted cascade
    "weakens",                    # reserved: directed, partial counter-evidence
]


# ---------------------------------------------------------------------
# Topology layer (S2, migration 0032). Positional embeddings,
# materialized neighborhoods, propagation queue. The actual geometry
# lives in vectors and join tables; these are just the lifecycle
# enums + dimension constant.
# ---------------------------------------------------------------------

# Topology dimension. Smaller than content embedding (768d) because
# topology is lower-dimensional than semantics — a substrate
# organizes into a handful of dozens of communities; 128 dims has
# more than enough capacity, and the smaller width keeps HNSW
# maintenance cheap as the graph evolves.
TOPO_EMBEDDING_DIM = 128

# Lifecycle of a materialized neighborhood. 'merged' / 'dissolved'
# are emitted by the matching algorithm during re-clustering when a
# previously-active neighborhood is no longer a community in the
# current graph (merged into another, or fragmented away).
NeighborhoodStatus = Literal["active", "dissolved", "merged"]


# ---------------------------------------------------------------------
# Base model with strict v2 config.
# ---------------------------------------------------------------------

class _Strict(BaseModel):
    model_config = ConfigDict(
        strict=False,       # coerce str->UUID, int->float — DB driver returns real types
        extra="forbid",     # reject unknown fields to catch drift at load time
        frozen=False,
        validate_assignment=True,
        str_strip_whitespace=False,
    )


# =====================================================================
# S1 — Observations
# =====================================================================

class ObservationRow(_Strict):
    id: UUID
    tenant_id: UUID
    occurred_at: datetime
    ingested_at: datetime
    kind: ObservationKind
    source_channel: str
    source_actor_ref: str | None = None
    actor_id: UUID | None = None
    content: dict[str, Any]
    content_text: str
    embedding: list[float] | None = None
    embedding_pending: bool = False
    trust_tier: TrustTierValue
    external_id: str | None = None
    cause_id: UUID | None = None
    sequence_num: int
    entities_mentioned: list[dict[str, Any]] = Field(default_factory=list)


class ObservationCreate(_Strict):
    """Payload passed into services/observations/repo.insert()."""
    id: UUID | None = None              # DB/ingestion assigns UUID v7 if None
    tenant_id: UUID
    occurred_at: datetime
    kind: ObservationKind = "signal"
    source_channel: str
    source_actor_ref: str | None = None
    actor_id: UUID | None = None
    content: dict[str, Any]
    content_text: str
    trust_tier: TrustTierValue
    external_id: str | None = None
    cause_id: UUID | None = None
    entities_mentioned: list[dict[str, Any]] = Field(default_factory=list)


# =====================================================================
# S2 — Models
# =====================================================================

class ModelRow(_Strict):
    id: UUID
    tenant_id: UUID
    born_from_event_id: UUID
    proposition: dict[str, Any]
    natural: str
    embedding: list[float]
    scope_actors: list[UUID] = Field(default_factory=list)
    scope_entities: list[dict[str, Any]] = Field(default_factory=list)
    scope_temporal: dict[str, Any]
    confidence: float
    activation: float
    falsifier: dict[str, Any] | None = None
    signal_readings: list[dict[str, Any]] = Field(default_factory=list)
    reading_contestable: bool = True
    supporting_event_ids: list[UUID] = Field(default_factory=list)
    supporting_model_ids: list[UUID] = Field(default_factory=list)
    evidential_weight: float = 0.5
    status: ModelStatus = "active"
    archived_at: datetime | None = None
    archive_reason: ModelArchiveReason | None = None
    created_at: datetime
    last_retrieved_at: datetime | None = None
    retrieval_count: int = 0
    evaluate_at: datetime | None = None
    resolution_criteria: dict[str, Any] | None = None
    contributing_models: list[UUID] = Field(default_factory=list)
    visible_to_subjects: bool = True
    # Post-Wave-0 A1-A2 additions (SCHEMA-LOCK.md amendments)
    proposition_kind: PropositionKind | None = None   # generated stored; hydrated on read
    confirmed_count: int = 0
    contested_count: int = 0
    last_confirmed_at: datetime | None = None
    confidence_at_assertion: float
    resolved_at: datetime | None = None
    resolution_outcome: bool | None = None
    activation_coefficient: float = 1.0
    # Recommendation-kind generated/auxiliary columns (migration 0022).
    # `target_actor_id` is GENERATED from proposition->>'target_actor_id'
    # for recommendation kind, NULL otherwise. `caused_act_change_id`
    # records the resulting Act-layer entity id when a recommendation
    # is acted upon — populated by the act handler, NULL for all other
    # proposition kinds.
    target_actor_id: UUID | None = None
    caused_act_change_id: UUID | None = None


class ModelCreate(_Strict):
    id: UUID | None = None
    tenant_id: UUID
    born_from_event_id: UUID
    proposition: dict[str, Any]
    natural: str
    embedding: list[float]
    scope_actors: list[UUID] = Field(default_factory=list)
    scope_entities: list[dict[str, Any]] = Field(default_factory=list)
    scope_temporal: dict[str, Any]
    confidence: float = Field(ge=0.05, le=0.95)
    # Post-Wave-0 A1: confidence_at_assertion is required at INSERT time.
    # Callers who don't have a distinct pre-calibration value pass the
    # raw confidence here; the DB enforces range via CHECK.
    confidence_at_assertion: float = Field(ge=0.05, le=0.95)
    falsifier: dict[str, Any] | None = None
    signal_readings: list[dict[str, Any]] = Field(default_factory=list)
    reading_contestable: bool = True
    supporting_event_ids: list[UUID] = Field(default_factory=list)
    supporting_model_ids: list[UUID] = Field(default_factory=list)
    evidential_weight: float = 0.5
    activation_coefficient: float = 1.0
    evaluate_at: datetime | None = None
    resolution_criteria: dict[str, Any] | None = None
    contributing_models: list[UUID] = Field(default_factory=list)
    visible_to_subjects: bool = True


# Post-Wave-0 A4 — sidecar table for freeform notes.
class ModelStatusNoteRow(_Strict):
    id: UUID
    model_id: UUID
    note: str
    authored_by: UUID | None = None
    authored_at: datetime
    kind: ModelStatusNoteKind


# =====================================================================
# S3 — Acts (Goals, Commitments, Decisions, edges)
# =====================================================================

class GoalRow(_Strict):
    id: UUID
    tenant_id: UUID
    title: str
    description: str | None = None
    state: GoalState = "active"
    target_date: datetime | None = None
    parent_goal_id: UUID | None = None
    altitude: GoalAltitude = "operational"
    success_criteria: dict[str, Any] | None = None
    cached_health: GoalCachedHealth = "healthy"
    cached_health_computed_at: datetime | None = None
    created_at: datetime
    last_state_change_at: datetime
    created_by_event_id: UUID
    archived_at: datetime | None = None


class CommitmentRow(_Strict):
    id: UUID
    tenant_id: UUID
    title: str
    description: str | None = None
    state: CommitmentState = "proposed"
    owner_id: UUID | None = None
    due_date: datetime | None = None
    ambition_level: AmbitionLevel = "base"
    priority: int = 5
    success_criteria: dict[str, Any] | None = None
    resolved_by_event_ids: list[UUID] = Field(default_factory=list)
    external_counterparty_ref: dict[str, Any] | None = None
    estimated_capacity: dict[str, Any] | None = None
    is_maintenance: bool = False
    created_at: datetime
    last_state_change_at: datetime
    terminal_at: datetime | None = None
    created_by_event_id: UUID
    last_confidence_basis: UUID | None = None


class CommitmentContributorRow(_Strict):
    commitment_id: UUID
    actor_id: UUID
    role: str | None = None


class DecisionRow(_Strict):
    id: UUID
    tenant_id: UUID
    title: str
    decision_text: str
    rationale: str | None = None
    state: DecisionState = "drafted"
    scope: dict[str, Any] | None = None
    revisit_triggers: dict[str, Any] | None = None
    created_at: datetime
    last_state_change_at: datetime
    created_by_event_id: UUID
    archived_at: datetime | None = None


class ContributesToEdge(_Strict):
    commitment_id: UUID
    goal_id: UUID
    is_critical_path: bool = False


class DependsOnEdge(_Strict):
    dependent_commitment_id: UUID
    dependency_commitment_id: UUID


class ConstrainedByEdge(_Strict):
    commitment_id: UUID
    decision_id: UUID


# =====================================================================
# S4 — Resources
# =====================================================================

class ResourceRow(_Strict):
    id: UUID
    tenant_id: UUID
    kind: ResourceKind
    identity: str
    description: str | None = None
    current_value: dict[str, Any]
    valuation_confidence: float = 1.0
    utilization_state: ResourceUtilizationState = "available"
    controllability: ResourceControllability = "owned"
    temporal_character: ResourceTemporalCharacter = "permanent"
    metadata: dict[str, Any] | None = None
    created_at: datetime
    last_updated_at: datetime
    last_updated_by_event_id: UUID | None = None
    archived_at: datetime | None = None


class ResourceTransactionRow(_Strict):
    id: UUID
    resource_id: UUID
    tenant_id: UUID
    transaction_type: ResourceTransactionType
    delta: dict[str, Any]
    occurred_at: datetime
    source_event_id: UUID
    created_at: datetime


class ResourceDeploymentRow(_Strict):
    resource_id: UUID
    commitment_id: UUID
    deployed_quantity: dict[str, Any] | None = None
    deployed_at: datetime
    released_at: datetime | None = None


CustomerCommitmentRelationshipKind = Literal["delivers", "supports", "impacts"]
CustomerCommitmentCriticality = Literal["must_have", "high", "medium", "low"]


class CustomerCommitmentRow(_Strict):
    # Q2 resolved (Option B1): superset shape from spec §27 per
    # migration 0014. The §4 three-column shape is preserved as a
    # subset — `served_description` remains, new columns carry
    # defaults at the DB level so Wave 2-C callers keep working.
    id: UUID
    tenant_id: UUID
    customer_resource_id: UUID
    commitment_id: UUID
    served_description: str | None = None
    relationship_kind: CustomerCommitmentRelationshipKind = "delivers"
    revenue_at_risk_usd: Decimal | None = None
    criticality: CustomerCommitmentCriticality = "medium"
    created_at: datetime


# =====================================================================
# S5 — Actors
# =====================================================================

class ActorRow(_Strict):
    id: UUID
    tenant_id: UUID
    type: ActorType
    display_name: str
    email: str | None = None
    status: ActorStatus = "active"
    metadata: dict[str, Any] | None = None
    specification_id: UUID | None = None
    created_at: datetime
    last_seen_at: datetime | None = None


class ActorIdentityMappingRow(_Strict):
    actor_id: UUID
    source_channel: str
    source_actor_ref: str
    confidence: float = 1.0
    created_at: datetime


# =====================================================================
# S6 — Entity aliases
# =====================================================================

class EntityAliasRow(_Strict):
    id: UUID
    tenant_id: UUID
    alias_text: str
    alias_embedding: list[float] | None = None
    actor_id: UUID | None = None
    resolved_entity_ref: dict[str, Any]
    is_canonical: bool = False
    entity_metadata: dict[str, Any] | None = None
    confidence: float = 0.8
    confirmed_count: int = 0
    contested_count: int = 0
    first_seen_at: datetime
    last_used_at: datetime
    source_event_id: UUID | None = None


# =====================================================================
# Demo infrastructure (migration 0023) — tenants registry, demo configs,
# demo sessions, per-call cost ledger.
# =====================================================================

DemoCompanyId = Literal["truss", "northwind", "meridian", "pelago"]
DemoSessionEndReason = Literal["user_ended", "inactivity", "cost_cap"]


class TenantRow(_Strict):
    id: UUID
    name: str = "unnamed"
    is_demo: bool = False
    demo_config_id: UUID | None = None
    created_at: datetime
    archived_at: datetime | None = None


class DemoConfigRow(_Strict):
    id: UUID
    company_id: DemoCompanyId
    name: str
    description: str
    tagline: str = ""
    snapshot_uri: str
    model_routing: dict[str, Any] = Field(default_factory=dict)
    cost_cap_usd_per_session: Decimal
    notifications_suppressed: bool = True
    determinism_seed: int | None = None
    reset_on_session_end: bool = True
    metadata: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime


class DemoSessionRow(_Strict):
    id: UUID
    tenant_id: UUID
    demo_config_id: UUID
    ceo_actor_id: UUID | None = None
    started_at: datetime
    last_active_at: datetime
    ended_at: datetime | None = None
    end_reason: DemoSessionEndReason | None = None
    total_cost_usd: Decimal
    signals_injected: int = 0
    actions_taken: int = 0
    cost_cap_breached_at: datetime | None = None


class DemoSessionCostRow(_Strict):
    id: UUID
    demo_session_id: UUID
    call_kind: str
    model_name: str
    input_tokens: int = 0
    output_tokens: int = 0
    cost_usd: Decimal
    occurred_at: datetime


__all__ = [
    # enum literals
    "ObservationKind", "TrustTierValue", "ModelStatus", "ModelArchiveReason",
    "ModelStatusNoteKind", "PropositionKind",
    "GoalState", "GoalAltitude", "GoalCachedHealth",
    "CommitmentState", "AmbitionLevel",
    "DecisionState",
    "ResourceKind", "ResourceUtilizationState", "ResourceControllability",
    "ResourceTemporalCharacter", "ResourceTransactionType",
    "ActorType", "ActorStatus",
    "DemoCompanyId", "DemoSessionEndReason",
    # row models
    "ObservationRow", "ObservationCreate",
    "ModelRow", "ModelCreate", "ModelStatusNoteRow",
    "GoalRow",
    "CommitmentRow", "CommitmentContributorRow",
    "DecisionRow",
    "ContributesToEdge", "DependsOnEdge", "ConstrainedByEdge",
    "ResourceRow", "ResourceTransactionRow", "ResourceDeploymentRow",
    "CustomerCommitmentRow", "CustomerCommitmentRelationshipKind", "CustomerCommitmentCriticality",
    "ActorRow", "ActorIdentityMappingRow",
    "EntityAliasRow",
    "TenantRow", "DemoConfigRow", "DemoSessionRow", "DemoSessionCostRow",
]
