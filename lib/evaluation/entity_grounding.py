"""Continuous state evaluation for the company-physics grounding funnel."""

from __future__ import annotations

import json
from collections import Counter, defaultdict
from datetime import datetime
from typing import Any, Mapping, Sequence, Self
from uuid import UUID

import asyncpg
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from lib.architecture_registry import ArchitectureContractRegistry
from lib.contracts.entity_mentions import (
    CommitEntityMentionDetectionCommand,
    EntityMentionDetection,
    EntityMentionDetectionFate,
)
from lib.contracts.kernel import canonical_sha256
from lib.contracts.perception import EntityMention, MentionAnchorKind
from lib.entity_mention_detection import locate_explicit_surface_spans
from lib.evaluation.proof import (
    CANONICAL_COMPONENT_PARTITION_DIMENSION,
    CANONICAL_COMPONENT_PARTITION_PROOF_REF,
    EvidenceTier,
    FateDenominatorRecord,
    IncidentObservation,
    IncidentStatus,
    InvariantRunEvidence,
    MetricObservation,
)
from lib.shared.entity_phrases import phrase_requires_context


_TERMINAL_WORK_FATES = frozenset(
    {"resolved_for_consumer", "review", "unresolved", "abstained", "exhausted", "escalated"}
)
_GOVERNED_ALIAS_REPLAY_SOURCE = "governed_exact_alias_replay"


class _GroundingEvaluationModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, str_strip_whitespace=True)


class GroundingEvaluationScope(_GroundingEvaluationModel):
    tenant_id: UUID
    observation_start: datetime
    observation_end: datetime
    run_id: str = Field(min_length=1)
    observation_ids: tuple[UUID, ...] = ()

    @field_validator("observation_start", "observation_end")
    @classmethod
    def times_are_aware(cls, value: datetime, info) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError(f"{info.field_name} must be timezone-aware")
        return value

    @model_validator(mode="after")
    def interval_is_forward(self) -> Self:
        if self.observation_end <= self.observation_start:
            raise ValueError("observation_end must follow observation_start")
        return self


class EntityGroundingEvaluationState(_GroundingEvaluationModel):
    scope: GroundingEvaluationScope
    eligible_observations: int = Field(ge=0)
    eligible_opportunities: int = Field(ge=0)
    work_head_count: int = Field(ge=0)
    work_fate_counts: dict[str, int]
    work_population_coverage: float | None = Field(default=None, ge=0.0, le=1.0)
    mention_detection_count: int = Field(ge=0)
    mention_detection_fate_counts: dict[str, int]
    mention_detection_population_coverage: float | None = Field(
        default=None, ge=0.0, le=1.0
    )
    detected_mention_count: int = Field(ge=0)
    rejected_mention_count: int = Field(ge=0)
    explicit_anchor_count: int = Field(ge=0)
    reconstructable_explicit_anchor_count: int = Field(ge=0)
    explicit_anchor_reconstructability_rate: float | None = Field(
        default=None, ge=0.0, le=1.0
    )
    mention_source_hash_match_count: int = Field(ge=0)
    mention_source_hash_match_rate: float | None = Field(
        default=None, ge=0.0, le=1.0
    )
    rejected_not_anchored_count: int = Field(ge=0)
    correct_rejected_not_anchored_count: int = Field(ge=0)
    rejected_not_anchored_correctness_rate: float | None = Field(
        default=None, ge=0.0, le=1.0
    )
    mention_context_continuity_count: int = Field(ge=0)
    mention_context_continuity_rate: float | None = Field(
        default=None, ge=0.0, le=1.0
    )
    mention_command_result_count: int = Field(ge=0)
    mention_command_result_coverage: float | None = Field(
        default=None, ge=0.0, le=1.0
    )
    mention_event_count: int = Field(ge=0)
    mention_event_coverage: float | None = Field(
        default=None, ge=0.0, le=1.0
    )
    mention_outbox_count: int = Field(ge=0)
    mention_outbox_coverage: float | None = Field(
        default=None, ge=0.0, le=1.0
    )
    mention_protocol_closure_count: int = Field(ge=0)
    mention_protocol_closure_rate: float | None = Field(
        default=None, ge=0.0, le=1.0
    )
    detected_mention_candidate_count: int = Field(ge=0)
    detected_mention_to_candidate_continuity_rate: float | None = Field(
        default=None, ge=0.0, le=1.0
    )
    rejected_candidate_request_count: int = Field(ge=0)
    terminal_work_count: int = Field(ge=0)
    terminal_trace_required_count: int = Field(ge=0)
    retry_scheduled_count: int = Field(ge=0)
    retry_without_due_time_count: int = Field(ge=0)
    trace_count: int = Field(ge=0)
    traced_terminal_count: int = Field(ge=0)
    terminal_trace_coverage: float | None = Field(default=None, ge=0.0, le=1.0)
    duplicate_trace_count: int = Field(ge=0)
    stage_complete_trace_count: int = Field(ge=0)
    stage_continuity_rate: float | None = Field(default=None, ge=0.0, le=1.0)
    candidate_request_count: int = Field(ge=0)
    immutable_candidate_set_count: int = Field(ge=0)
    candidate_request_fate_coverage: float | None = Field(
        default=None, ge=0.0, le=1.0
    )
    incomplete_lane_fate_count: int = Field(ge=0)
    missing_open_world_option_count: int = Field(ge=0)
    future_context_leak_count: int = Field(ge=0)
    unauthorized_source_space_count: int = Field(ge=0)
    invented_candidate_admission_count: int = Field(ge=0)
    single_referent_without_identity_basis_count: int = Field(ge=0)
    identity_registry_mutation_count: int = Field(ge=0)
    source_observation_mutation_count: int = Field(ge=0)
    resolver_created_alias_count: int = Field(ge=0)
    self_authoritative_observation_count: int = Field(ge=0)
    review_fate_count: int = Field(ge=0)
    review_obligation_count: int = Field(ge=0)
    review_obligation_coverage: float | None = Field(default=None, ge=0.0, le=1.0)
    answered_entity_clarification_count: int = Field(ge=0)
    answered_entity_clarification_lineage_count: int = Field(ge=0)
    answered_entity_clarification_lineage_coverage: float | None = Field(
        default=None, ge=0.0, le=1.0
    )
    adjudicated_alias_count: int = Field(ge=0)
    adjudicated_alias_lineage_count: int = Field(ge=0)
    adjudicated_alias_lineage_coverage: float | None = Field(
        default=None, ge=0.0, le=1.0
    )
    corrective_memory_observed_reuse_count: int = Field(ge=0)
    alias_replay_exposure_count: int = Field(ge=0)
    alias_replay_resolved_count: int = Field(ge=0)
    alias_replay_resolution_rate: float | None = Field(
        default=None, ge=0.0, le=1.0
    )
    alias_replay_llm_avoided_count: int = Field(ge=0)
    unsafe_alias_replay_count: int = Field(ge=0)
    contextual_alias_replay_count: int = Field(ge=0)
    processing_class_counts: dict[str, int]
    incident_counts: dict[str, int]
    incident_refs: dict[str, tuple[str, ...]] = Field(default_factory=dict)
    uncertainty: tuple[str, ...]
    artifact_refs: tuple[str, ...] = Field(min_length=1)

    @property
    def violation_count(self) -> int:
        return sum(self.incident_counts.values())


def _json(value: Any) -> Any:
    if isinstance(value, str):
        return json.loads(value)
    return value


def _phrases(content: Any) -> tuple[str, ...]:
    content = _json(content)
    if not isinstance(content, dict):
        return ()
    values: list[str] = []
    seen: set[str] = set()

    def add(raw: Any) -> None:
        if not isinstance(raw, list):
            return
        for item in raw:
            if not isinstance(item, str):
                continue
            item = item.strip()
            if item and item not in seen:
                seen.add(item)
                values.append(item)

    add(content.get("_unresolved_phrases"))
    for key in ("metadata", "_metadata"):
        nested = content.get(key)
        if isinstance(nested, dict):
            add(nested.get("_unresolved_phrases"))
    return tuple(values)


def _observation_content_text(row: Mapping[str, Any]) -> str:
    direct = row.get("content_text")
    if direct is not None:
        return str(direct)
    content = _json(row.get("content"))
    if not isinstance(content, dict):
        return ""
    for key in ("content_text", "text"):
        value = content.get(key)
        if value is not None:
            return str(value)
    return ""


def _trace_row_id(row: Mapping[str, Any]) -> str | None:
    value = row.get("id")
    if value is None:
        return None
    rendered = str(value).strip()
    return rendered or None


def _superseded_trace_id(row: Mapping[str, Any]) -> str | None:
    value = row.get("supersedes_grounding_trace_id")
    if value is None:
        trace = _json(row.get("trace"))
        if isinstance(trace, dict):
            value = trace.get("supersedes_grounding_trace_id")
    if value is None:
        return None
    rendered = str(value).strip()
    return rendered or None


def _trace_lineage_heads(
    rows: Sequence[Mapping[str, Any]],
) -> tuple[
    tuple[Mapping[str, Any], ...],
    tuple[Mapping[str, Any], ...],
    int,
    tuple[str, ...],
]:
    """Return current lineage heads without treating superseded history as duplicate."""

    rows_by_id: dict[str, Mapping[str, Any]] = {}
    anonymous_rows: list[Mapping[str, Any]] = []
    duplicate_count = 0
    invalid_supersession_refs: list[str] = []
    for row in rows:
        trace_id = _trace_row_id(row)
        if trace_id is None:
            anonymous_rows.append(row)
        elif trace_id in rows_by_id:
            duplicate_count += 1
        else:
            rows_by_id[trace_id] = row

    superseded_ids: set[str] = set()
    for trace_id, row in rows_by_id.items():
        predecessor_id = _superseded_trace_id(row)
        if predecessor_id is None:
            continue
        if predecessor_id == trace_id or predecessor_id not in rows_by_id:
            invalid_supersession_refs.append(trace_id)
            continue
        superseded_ids.add(predecessor_id)

    identified_heads = tuple(
        row
        for trace_id, row in rows_by_id.items()
        if trace_id not in superseded_ids
    )
    if rows_by_id and not identified_heads:
        invalid_supersession_refs.extend(
            trace_id
            for trace_id in rows_by_id
            if trace_id not in invalid_supersession_refs
        )
    heads = identified_heads + tuple(anonymous_rows)
    duplicate_count += max(0, len(heads) - 1)
    generations = tuple(rows_by_id.values()) + tuple(anonymous_rows)
    return heads, generations, duplicate_count, tuple(invalid_supersession_refs)


async def evaluate_entity_grounding_state(
    conn: asyncpg.Connection,
    *,
    scope: GroundingEvaluationScope,
    artifact_refs: tuple[str, ...],
) -> EntityGroundingEvaluationState:
    """Measure the complete scoped opportunity population, not survivors."""

    observations = await conn.fetch(
        """
        SELECT id, occurred_at, content, content_text
        FROM observations
        WHERE tenant_id = $1
          AND occurred_at >= $2
          AND occurred_at < $3
          AND (
            cardinality($4::uuid[]) = 0
            OR id = ANY($4::uuid[])
          )
        ORDER BY occurred_at, id
        """,
        scope.tenant_id,
        scope.observation_start,
        scope.observation_end,
        list(scope.observation_ids),
    )
    observation_ids = [row["id"] for row in observations]
    if observation_ids:
        work_items = await conn.fetch(
            """
            SELECT * FROM entity_grounding_work_items
            WHERE tenant_id = $1
              AND source_observation_id = ANY($2::uuid[])
            ORDER BY updated_at, id
            """,
            scope.tenant_id,
            observation_ids,
        )
        episode_rows = await conn.fetch(
            """
            SELECT
              gt.*,
              ics.snapshot,
              ecgr.request,
              ecs.candidate_set,
              ecs.candidate_set_hash,
              ra.assessment,
              ra.selected_candidate_id,
              ra.model_output,
              gad.decision,
              gt.entity_mention_detection_id AS trace_mention_detection_id,
              gt.entity_mention_id AS trace_mention_id,
              ecgr.entity_mention_detection_id AS candidate_mention_detection_id,
              ecgr.entity_mention_id AS candidate_mention_id,
              EXISTS (
                SELECT 1 FROM clarification_requests clarification
                WHERE clarification.tenant_id = gt.tenant_id
                  AND clarification.kind = 'entity_resolution'
                  AND clarification.source_observation_id =
                      gt.source_observation_id
                  AND clarification.payload ->> 'phrase' = gt.phrase
                  AND clarification.payload
                      -> 'feedback_lineage'
                      ->> 'grounding_trace_id' = gt.id::text
              ) AS has_review_obligation
            FROM grounding_traces gt
            LEFT JOIN interpretation_context_snapshots ics
              ON ics.id = gt.context_snapshot_id
            LEFT JOIN entity_candidate_generation_requests ecgr
              ON ecgr.id = gt.candidate_request_id
            LEFT JOIN entity_candidate_sets ecs
              ON ecs.id = gt.candidate_set_id
            LEFT JOIN resolution_assessments ra
              ON ra.id = gt.resolution_assessment_id
            LEFT JOIN grounding_admission_decisions gad
              ON gad.id = gt.grounding_admission_id
            WHERE gt.tenant_id = $1
              AND gt.source_observation_id = ANY($2::uuid[])
            ORDER BY gt.created_at, gt.id
            """,
            scope.tenant_id,
            observation_ids,
        )
        mention_rows = await conn.fetch(
            """
            SELECT
              emd.*,
              ics.snapshot_content_hash AS committed_context_snapshot_digest
            FROM entity_mention_detection_heads emh
            JOIN entity_mention_detections emd
              ON emd.tenant_id=emh.tenant_id
             AND emd.id=emh.current_detection_id
            LEFT JOIN interpretation_context_snapshots ics
              ON ics.tenant_id=emd.tenant_id
             AND ics.id=emd.context_snapshot_id
            WHERE emh.tenant_id=$1
              AND emh.source_observation_id=ANY($2::uuid[])
            ORDER BY emd.detected_at, emd.id
            """,
            scope.tenant_id,
            observation_ids,
        )
        candidate_request_rows = await conn.fetch(
            """
            SELECT id, source_observation_id, phrase, context_snapshot_id,
                   entity_mention_detection_id, entity_mention_id, request
            FROM entity_candidate_generation_requests
            WHERE tenant_id=$1
              AND source_observation_id=ANY($2::uuid[])
              AND (
                entity_mention_detection_id IS NULL
                OR entity_mention_detection_id=ANY($3::uuid[])
              )
            ORDER BY created_at, id
            """,
            scope.tenant_id,
            observation_ids,
            [row["id"] for row in mention_rows],
        )
        mention_result_ids = [row["command_result_id"] for row in mention_rows]
        if mention_result_ids:
            mention_commands = await conn.fetch(
                """
                SELECT * FROM agency_command_results
                WHERE tenant_id=$1 AND id=ANY($2::uuid[])
                ORDER BY created_at, id
                """,
                scope.tenant_id,
                mention_result_ids,
            )
            mention_events = await conn.fetch(
                """
                SELECT * FROM agency_canonical_events
                WHERE tenant_id=$1 AND command_result_id=ANY($2::uuid[])
                ORDER BY created_at, id
                """,
                scope.tenant_id,
                mention_result_ids,
            )
            mention_event_ids = [row["id"] for row in mention_events]
            mention_outboxes = (
                await conn.fetch(
                    """
                    SELECT * FROM agency_outbox_records
                    WHERE tenant_id=$1 AND event_id=ANY($2::uuid[])
                    ORDER BY created_at, id
                    """,
                    scope.tenant_id,
                    mention_event_ids,
                )
                if mention_event_ids
                else []
            )
        else:
            mention_commands = []
            mention_events = []
            mention_outboxes = []
    else:
        work_items = []
        episode_rows = []
        mention_rows = []
        candidate_request_rows = []
        mention_commands = []
        mention_events = []
        mention_outboxes = []
    resolver_aliases = await conn.fetchval(
        """
        SELECT COUNT(*) FROM entity_aliases
        WHERE tenant_id = $1
          AND first_seen_at >= $2 AND first_seen_at < $3
          AND entity_metadata ->> 'source' = 'resolver_worker'
          AND (
            cardinality($4::uuid[]) = 0
            OR source_event_id = ANY($4::uuid[])
          )
        """,
        scope.tenant_id,
        scope.observation_start,
        scope.observation_end,
        list(scope.observation_ids),
    ) or 0
    self_observations = await conn.fetchval(
        """
        SELECT COUNT(*) FROM observations
        WHERE tenant_id = $1
          AND occurred_at >= $2 AND occurred_at < $3
          AND source_channel = 'internal:state_change'
          AND content ->> '_state_change_kind' = 'entity_late_resolution'
          AND (
            cardinality($4::uuid[]) = 0
            OR content ->> 'source_observation_id' = ANY($5::text[])
          )
        """,
        scope.tenant_id,
        scope.observation_start,
        scope.observation_end,
        list(scope.observation_ids),
        [str(value) for value in scope.observation_ids],
    ) or 0
    corrective_memory = await conn.fetchrow(
        """
        WITH answered AS (
          SELECT id, tenant_id, source_observation_id, payload, answered_at
          FROM clarification_requests
          WHERE tenant_id = $1
            AND kind = 'entity_resolution'
            AND status = 'answered'
            AND answered_at >= $2 AND answered_at < $3
            AND (
              cardinality($4::uuid[]) = 0
              OR source_observation_id = ANY($4::uuid[])
            )
        ),
        adjudicated_aliases AS (
          SELECT a.*, answered.id AS clarification_request_id,
                 answered.source_observation_id AS clarified_observation_id,
                 answered.answered_at
          FROM entity_aliases a
          JOIN answered
            ON a.tenant_id = answered.tenant_id
           AND a.entity_metadata ->> 'clarification_request_id'
               = answered.id::text
          WHERE a.entity_metadata ->> 'identity_basis_class'
                = 'independently_adjudicated'
        )
        SELECT
          (SELECT COUNT(*) FROM answered) AS answered_count,
          (
            SELECT COUNT(*) FROM answered
            WHERE jsonb_typeof(payload -> 'feedback_lineage') = 'object'
              AND COALESCE(
                payload -> 'feedback_lineage' ->> 'grounding_trace_id',
                ''
              ) <> ''
          ) AS answered_lineage_count,
          (SELECT COUNT(*) FROM adjudicated_aliases) AS alias_count,
          (
            SELECT COUNT(*) FROM adjudicated_aliases
            WHERE jsonb_typeof(
                    entity_metadata -> 'grounding_feedback_lineage'
                  ) = 'object'
              AND COALESCE(
                entity_metadata
                  -> 'grounding_feedback_lineage'
                  ->> 'grounding_trace_id',
                ''
              ) <> ''
          ) AS alias_lineage_count,
          (
            SELECT COUNT(*) FROM adjudicated_aliases alias
            WHERE EXISTS (
              SELECT 1
              FROM grounding_traces trace
              WHERE trace.tenant_id = alias.tenant_id
                AND trace.phrase = alias.alias_text
                AND trace.created_at > alias.answered_at
                AND trace.source_observation_id <> alias.clarified_observation_id
                AND (
                  cardinality($4::uuid[]) = 0
                  OR trace.source_observation_id = ANY($4::uuid[])
                )
                AND COALESCE(trace.trace ->> 'adjudication_ref', '')
                    <> 'clarification-request:' || alias.clarification_request_id::text
                AND trace.current_fate = 'resolved_for_consumer'
                AND trace.selected_referent ->> 'type'
                    = alias.resolved_entity_ref ->> 'type'
                AND trace.selected_referent ->> 'id'
                    = alias.resolved_entity_ref ->> 'id'
            )
          ) AS observed_reuse_count
        """,
        scope.tenant_id,
        scope.observation_start,
        scope.observation_end,
        list(scope.observation_ids),
    )
    return analyze_entity_grounding_rows(
        scope=scope,
        observations=observations,
        work_items=work_items,
        episode_rows=episode_rows,
        mention_detection_rows=mention_rows,
        candidate_request_rows=candidate_request_rows,
        mention_commands=mention_commands,
        mention_events=mention_events,
        mention_outboxes=mention_outboxes,
        resolver_created_alias_count=int(resolver_aliases),
        self_authoritative_observation_count=int(self_observations),
        answered_entity_clarification_count=int(
            corrective_memory["answered_count"] if corrective_memory else 0
        ),
        answered_entity_clarification_lineage_count=int(
            corrective_memory["answered_lineage_count"] if corrective_memory else 0
        ),
        adjudicated_alias_count=int(
            corrective_memory["alias_count"] if corrective_memory else 0
        ),
        adjudicated_alias_lineage_count=int(
            corrective_memory["alias_lineage_count"] if corrective_memory else 0
        ),
        corrective_memory_observed_reuse_count=int(
            corrective_memory["observed_reuse_count"] if corrective_memory else 0
        ),
        artifact_refs=artifact_refs,
    )


def analyze_entity_grounding_rows(
    *,
    scope: GroundingEvaluationScope,
    observations: Sequence[Mapping[str, Any]],
    work_items: Sequence[Mapping[str, Any]],
    episode_rows: Sequence[Mapping[str, Any]],
    resolver_created_alias_count: int,
    self_authoritative_observation_count: int,
    artifact_refs: tuple[str, ...],
    answered_entity_clarification_count: int = 0,
    answered_entity_clarification_lineage_count: int = 0,
    adjudicated_alias_count: int = 0,
    adjudicated_alias_lineage_count: int = 0,
    corrective_memory_observed_reuse_count: int = 0,
    mention_detection_rows: Sequence[Mapping[str, Any]] = (),
    candidate_request_rows: Sequence[Mapping[str, Any]] = (),
    mention_commands: Sequence[Mapping[str, Any]] = (),
    mention_events: Sequence[Mapping[str, Any]] = (),
    mention_outboxes: Sequence[Mapping[str, Any]] = (),
) -> EntityGroundingEvaluationState:
    incidents: Counter[str] = Counter()
    incident_refs: dict[str, set[str]] = defaultdict(set)

    def incident(name: str, ref: str) -> None:
        incidents[name] += 1
        incident_refs[name].add(ref)

    eligible: set[tuple[UUID, str]] = set()
    eligible_observations: set[UUID] = set()
    observations_by_id: dict[UUID, Mapping[str, Any]] = {}
    for row in observations:
        observations_by_id[UUID(str(row["id"]))] = row
        for phrase in _phrases(row["content"]):
            observation_id = UUID(str(row["id"]))
            eligible.add((observation_id, phrase))
            eligible_observations.add(observation_id)

    work_by_key: dict[tuple[UUID, str], Mapping[str, Any]] = {}
    for row in work_items:
        key = (row["source_observation_id"], row["phrase"])
        if key in eligible:
            current = work_by_key.get(key)
            row_generation = int(row.get("processing_generation") or 0)
            current_generation = (
                int(current.get("processing_generation") or 0)
                if current is not None
                else -1
            )
            if current is None or row_generation >= current_generation:
                work_by_key[key] = row
    work_fates = Counter(str(row["status"]) for row in work_by_key.values())
    terminal_work = {
        key for key, row in work_by_key.items() if row["status"] in _TERMINAL_WORK_FATES
    }
    retry_without_due = sum(
        row["status"] == "retry_scheduled" and row["next_attempt_at"] is None
        for row in work_by_key.values()
    )

    detection_rows_by_key: dict[
        tuple[UUID, str], list[Mapping[str, Any]]
    ] = defaultdict(list)
    for row in mention_detection_rows:
        try:
            key = (
                UUID(str(row["source_observation_id"])),
                str(row["candidate_surface"]),
            )
        except (KeyError, TypeError, ValueError):
            incident("invalid_mention_detection_identity", "mention-detection:unknown")
            continue
        if key in eligible:
            detection_rows_by_key[key].append(row)
    current_detection_by_key: dict[tuple[UUID, str], Mapping[str, Any]] = {}
    for key, rows in detection_rows_by_key.items():
        ordered = sorted(
            rows,
            key=lambda row: (
                int(row.get("detection_version") or 0),
                str(row.get("id") or ""),
            ),
        )
        current_detection_by_key[key] = ordered[-1]
        if len(ordered) > 1:
            incident(
                "duplicate_current_mention_detection",
                f"mention-opportunity:{key[0]}:{key[1]}",
            )
    for observation_id, phrase in sorted(eligible, key=lambda item: (str(item[0]), item[1])):
        if (observation_id, phrase) not in current_detection_by_key:
            incident(
                "mention_opportunity_without_detection_fate",
                f"mention-opportunity:{observation_id}:{phrase}",
            )

    command_by_id: dict[UUID, Mapping[str, Any]] = {}
    for row in mention_commands:
        try:
            command_by_id[UUID(str(row["id"]))] = row
        except (KeyError, TypeError, ValueError):
            incident("invalid_mention_command_identity", "mention-command:unknown")
    events_by_result: dict[UUID, list[Mapping[str, Any]]] = defaultdict(list)
    for row in mention_events:
        try:
            events_by_result[UUID(str(row["command_result_id"]))].append(row)
        except (KeyError, TypeError, ValueError):
            incident("invalid_mention_event_identity", "mention-event:unknown")
    outboxes_by_event: dict[UUID, list[Mapping[str, Any]]] = defaultdict(list)
    for row in mention_outboxes:
        try:
            outboxes_by_event[UUID(str(row["event_id"]))].append(row)
        except (KeyError, TypeError, ValueError):
            incident("invalid_mention_outbox_identity", "mention-outbox:unknown")

    candidate_rows_by_detection: dict[UUID, list[Mapping[str, Any]]] = defaultdict(list)
    for row in candidate_request_rows:
        raw_detection_id = row.get("entity_mention_detection_id")
        if raw_detection_id is None:
            incident(
                "candidate_request_without_mention_detection",
                f"candidate-request:{row.get('id', 'unknown')}",
            )
            continue
        try:
            candidate_rows_by_detection[UUID(str(raw_detection_id))].append(row)
        except (TypeError, ValueError):
            incident(
                "candidate_request_invalid_mention_detection",
                f"candidate-request:{row.get('id', 'unknown')}",
            )

    detection_fates: Counter[str] = Counter()
    detection_by_id: dict[UUID, Mapping[str, Any]] = {}
    explicit_anchor_count = 0
    reconstructable_anchor_count = 0
    source_hash_matches = 0
    rejected_not_anchored = 0
    correct_rejected_not_anchored = 0
    context_continuity = 0
    command_result_count = 0
    event_count = 0
    outbox_count = 0
    protocol_closure_count = 0
    detected_candidate_count = 0
    rejected_candidate_requests = 0

    for key, row in current_detection_by_key.items():
        observation_id, phrase = key
        raw_id = row.get("id")
        try:
            detection_id = UUID(str(raw_id))
        except (TypeError, ValueError):
            incident("invalid_mention_detection_identity", f"mention-detection:{raw_id}")
            continue
        ref = f"mention-detection:{detection_id}"
        detection_by_id[detection_id] = row
        fate = str(row.get("fate") or "unknown")
        detection_fates[fate] += 1
        observation = observations_by_id.get(observation_id) or {}
        content_text = _observation_content_text(observation)
        expected_spans = locate_explicit_surface_spans(content_text, phrase)
        source_hash_ok = canonical_sha256(content_text) == row.get("source_content_hash")
        source_hash_matches += int(source_hash_ok)
        if not source_hash_ok:
            incident("mention_source_content_hash_mismatch", ref)

        mention_payload = _json(row.get("mention"))
        mention: EntityMention | None = None
        detection: EntityMentionDetection | None = None
        try:
            detection = EntityMentionDetection(
                detection_id=detection_id,
                detection_version=int(row["detection_version"]),
                tenant_id=scope.tenant_id,
                source_observation_id=observation_id,
                source_revision_id=str(row["source_revision_id"]),
                candidate_surface=phrase,
                context_snapshot_id=UUID(str(row["context_snapshot_id"])),
                context_snapshot_digest=str(row["context_snapshot_digest"]),
                source_content_hash=str(row["source_content_hash"]),
                fate=fate,
                mention=mention_payload,
                reason_codes=tuple(row.get("reason_codes") or ()),
                extractor_version=str(row["extractor_version"]),
                detected_at=row["detected_at"],
            )
            mention = detection.mention
            if detection.detection_digest != row.get("detection_digest"):
                incident("mention_detection_digest_mismatch", ref)
        except Exception:
            incident("invalid_mention_detection_contract", ref)

        context_ok = (
            row.get("committed_context_snapshot_digest")
            == row.get("context_snapshot_digest")
            and row.get("committed_context_snapshot_digest") is not None
        )
        if mention is not None:
            context_ok = context_ok and mention.context_snapshot_id == str(
                row.get("context_snapshot_id")
            )
        context_continuity += int(context_ok)
        if not context_ok:
            incident("mention_context_snapshot_discontinuity", ref)

        if fate == EntityMentionDetectionFate.DETECTED.value:
            if mention is None:
                incident("detected_fate_without_valid_mention", ref)
            else:
                anchors = (mention.primary_anchor, *mention.alternate_anchors)
                explicit_anchors = tuple(
                    anchor
                    for anchor in anchors
                    if anchor.kind is MentionAnchorKind.EXPLICIT
                )
                observed_spans: list[tuple[int, int]] = []
                for anchor in explicit_anchors:
                    explicit_anchor_count += 1
                    coordinate = anchor.coordinate
                    start = coordinate.span_start
                    end = coordinate.span_end
                    anchor_ok = (
                        coordinate.evidence_record_id
                        == f"observation:{observation_id}"
                        and coordinate.source_revision == row.get("source_revision_id")
                        and coordinate.field_path == "content_text"
                        and start is not None
                        and end is not None
                        and 0 <= start < end <= len(content_text)
                        and content_text[start:end] == anchor.surface_form
                    )
                    if start is not None and end is not None:
                        observed_spans.append((start, end))
                    reconstructable_anchor_count += int(anchor_ok)
                    if not anchor_ok:
                        incident(
                            "mention_explicit_anchor_not_reconstructable",
                            f"{ref}:anchor:{anchor.anchor_id}",
                        )
                if tuple(observed_spans) != expected_spans:
                    incident("mention_explicit_anchor_population_mismatch", ref)
                if not explicit_anchors:
                    incident("detected_mention_without_explicit_anchor", ref)
        elif fate == EntityMentionDetectionFate.REJECTED_NOT_ANCHORED.value:
            rejected_not_anchored += 1
            if not expected_spans:
                correct_rejected_not_anchored += 1
            else:
                incident("mention_false_rejected_not_anchored", ref)

        result_ok = False
        event_ok = False
        outbox_ok = False
        try:
            command_result_id = UUID(str(row["command_result_id"]))
        except (KeyError, TypeError, ValueError):
            command_result_id = None
        command_row = command_by_id.get(command_result_id) if command_result_id else None
        if command_row is not None and detection is not None:
            try:
                command = CommitEntityMentionDetectionCommand.model_validate(
                    _json(command_row["command"])
                )
                result = _json(command_row["result"])
                result_ok = (
                    command.request_digest == command_row.get("request_digest")
                    and command.request_digest == canonical_sha256(
                        command.model_dump(mode="json")
                    )
                    and command.detection.detection_digest == detection.detection_digest
                    and command_row.get("writer_id") == "GroundingAnnotationAppender"
                    and command_row.get("command_kind")
                    == "commit_entity_mention_detection"
                    and str(command_row.get("object_id")) == str(detection_id)
                    and int(command_row.get("object_version") or 0)
                    == detection.detection_version
                    and str(result.get("detection_id")) == str(detection_id)
                    and result.get("detection_digest") == detection.detection_digest
                    and result.get("fate") == fate
                    and result.get("context_snapshot_digest")
                    == detection.context_snapshot_digest
                    and result.get("source_content_hash")
                    == detection.source_content_hash
                )
            except Exception:
                result_ok = False
        command_result_count += int(result_ok)
        if not result_ok:
            incident("mention_command_result_discontinuity", ref)

        event_rows = events_by_result.get(command_result_id, []) if command_result_id else []
        if len(event_rows) == 1:
            event_row = event_rows[0]
            event_payload = _json(event_row.get("event_payload")) or {}
            event_ok = (
                event_row.get("writer_id") == "GroundingAnnotationAppender"
                and event_row.get("object_type") == "entity_mention_detection"
                and str(event_row.get("object_id")) == str(detection_id)
                and int(event_row.get("object_version") or 0)
                == int(row.get("detection_version") or 0)
                and str(event_payload.get("detection_id")) == str(detection_id)
                and event_payload.get("detection_digest")
                == row.get("detection_digest")
            )
        event_count += int(event_ok)
        if not event_ok:
            incident("mention_command_event_closure", ref)

        if event_ok:
            event_id = UUID(str(event_rows[0]["id"]))
            outbox_rows = outboxes_by_event.get(event_id, [])
            if len(outbox_rows) == 1:
                outbox_row = outbox_rows[0]
                outbox_payload = _json(outbox_row.get("payload")) or {}
                outbox_ok = (
                    outbox_row.get("destination_operation")
                    == "grounding.entity_mention.detected"
                    and canonical_sha256(outbox_payload)
                    == outbox_row.get("payload_hash")
                    and str(outbox_payload.get("detection_id")) == str(detection_id)
                )
        outbox_count += int(outbox_ok)
        if not outbox_ok:
            incident("mention_event_outbox_closure", ref)
        protocol_closure_count += int(result_ok and event_ok and outbox_ok)

        linked_candidates = candidate_rows_by_detection.get(detection_id, [])
        if fate == EntityMentionDetectionFate.DETECTED.value:
            mention_id = row.get("mention_id")
            expected_mention_ref = (
                f"mention:{mention_id}:v{row.get('detection_version')}"
            )
            candidate_ok = bool(linked_candidates)
            for candidate_row in linked_candidates:
                request_payload = _json(candidate_row.get("request")) or {}
                candidate_ok = candidate_ok and (
                    str(candidate_row.get("entity_mention_id")) == str(mention_id)
                    and UUID(str(candidate_row.get("source_observation_id")))
                    == observation_id
                    and candidate_row.get("phrase") == phrase
                    and str(candidate_row.get("context_snapshot_id"))
                    == str(row.get("context_snapshot_id"))
                    and request_payload.get("mention_ref") == expected_mention_ref
                )
            detected_candidate_count += int(candidate_ok)
            if not candidate_ok:
                incident("detected_mention_without_exact_candidate_request", ref)
        elif linked_candidates:
            rejected_candidate_requests += len(linked_candidates)
            incident("rejected_mention_has_candidate_request", ref)

    known_detection_ids = set(detection_by_id)
    for detection_id, rows in candidate_rows_by_detection.items():
        if detection_id not in known_detection_ids:
            for row in rows:
                incident(
                    "candidate_request_unknown_mention_detection",
                    f"candidate-request:{row.get('id', 'unknown')}",
                )

    traces_by_key: dict[tuple[UUID, str], list[Mapping[str, Any]]] = {}
    for row in episode_rows:
        key = (row["source_observation_id"], row["phrase"])
        if key in eligible:
            traces_by_key.setdefault(key, []).append(row)
    current_rows: list[Mapping[str, Any]] = []
    generation_rows: list[Mapping[str, Any]] = []
    duplicate_traces = 0
    for key, rows in traces_by_key.items():
        (
            heads,
            lineage_generations,
            duplicate_count,
            invalid_supersession_refs,
        ) = _trace_lineage_heads(rows)
        generation_rows.extend(lineage_generations)
        duplicate_traces += duplicate_count
        for trace_id in invalid_supersession_refs:
            incident(
                "invalid_grounding_trace_supersession",
                f"grounding-trace:{trace_id}",
            )

        raw_current_trace_id = work_by_key.get(key, {}).get("current_trace_id")
        current_trace_id = (
            str(raw_current_trace_id) if raw_current_trace_id is not None else None
        )
        current_row = next(
            (row for row in rows if _trace_row_id(row) == current_trace_id),
            None,
        )
        if current_trace_id is not None and current_row is None:
            incident(
                "grounding_trace_head_missing",
                f"grounding-work:{key[0]}:{key[1]}",
            )
        elif current_row is not None and all(current_row is not head for head in heads):
            incident(
                "grounding_trace_head_not_current_generation",
                f"grounding-trace:{current_trace_id}",
            )
        if current_row is None:
            current_row = heads[0] if len(heads) == 1 else rows[-1]
        current_rows.append(current_row)

    all_trace_rows = generation_rows

    complete_stage = 0
    lane_incomplete = 0
    open_world_missing = 0
    future_context = 0
    source_space_leaks = 0
    invented_admissions = 0
    ungrounded_single_admissions = 0
    identity_mutations = 0
    source_mutations = 0
    review_fates = 0
    review_obligations = 0
    alias_replay_exposures = 0
    alias_replay_resolved = 0
    alias_replay_llm_avoided = 0
    unsafe_alias_replays = 0
    contextual_alias_replays = 0
    request_ids: set[str] = set()
    set_request_ids: set[str] = set()

    for row in all_trace_rows:
        snapshot = _json(row.get("snapshot"))
        request = _json(row.get("request"))
        candidate_set = _json(row.get("candidate_set"))
        assessment = _json(row.get("assessment"))
        decision = _json(row.get("decision"))
        stages = (snapshot, request, candidate_set, assessment, decision)
        trace_ref = (
            f"grounding-trace:{row.get('source_observation_id')}:{row.get('phrase')}"
        )
        mention_stage_complete = False
        try:
            trace_detection_id = UUID(str(row.get("trace_mention_detection_id")))
        except (TypeError, ValueError):
            trace_detection_id = None
        detection_row = (
            detection_by_id.get(trace_detection_id) if trace_detection_id else None
        )
        if detection_row is not None:
            mention_stage_complete = (
                detection_row.get("fate")
                == EntityMentionDetectionFate.DETECTED.value
                and str(row.get("trace_mention_id"))
                == str(detection_row.get("mention_id"))
                and str(row.get("candidate_mention_detection_id"))
                == str(trace_detection_id)
                and str(row.get("candidate_mention_id"))
                == str(detection_row.get("mention_id"))
                and str(row.get("context_snapshot_id"))
                == str(detection_row.get("context_snapshot_id"))
            )
        if not mention_stage_complete:
            incident("grounding_trace_mention_discontinuity", trace_ref)
        if all(isinstance(stage, dict) for stage in stages) and mention_stage_complete:
            complete_stage += 1
        if isinstance(request, dict):
            request_id = str(request.get("request_id"))
            request_ids.add(request_id)
        else:
            request_id = ""
        if isinstance(candidate_set, dict):
            nested_request = candidate_set.get("request") or {}
            set_request_ids.add(str(nested_request.get("request_id")))
            required_lanes = set(nested_request.get("required_retrieval_lanes") or ())
            lane_ids = {
                item.get("lane_id")
                for item in candidate_set.get("lane_fates") or ()
                if isinstance(item, dict)
            }
            if lane_ids != required_lanes:
                lane_incomplete += 1
            kinds = {
                item.get("kind")
                for item in candidate_set.get("candidates") or ()
                if isinstance(item, dict)
            }
            if not {"none_of_the_above", "novel_referent", "unknown"} <= kinds:
                open_world_missing += 1
        elif request_id:
            lane_incomplete += 1
            open_world_missing += 1

        if isinstance(snapshot, dict):
            snapshot_request = snapshot.get("request") or {}
            cutoff = _parse_datetime(snapshot_request.get("evidence_cutoff"))
            allowed = _restriction_values(snapshot_request.get("allowed_source_spaces"))
            for item in snapshot.get("selected_items") or ():
                if not isinstance(item, dict):
                    continue
                emitted_at = _parse_datetime(item.get("emitted_at"))
                if cutoff and emitted_at and emitted_at > cutoff:
                    future_context += 1
                if allowed is not None and item.get("source_space") not in allowed:
                    source_space_leaks += 1

        model_output = _json(row.get("model_output")) or {}
        selected_candidate_id = row.get("selected_candidate_id")
        selected_referent = _json(row.get("selected_referent"))
        decision_source = str(model_output.get("decision_source") or "")
        if decision_source == _GOVERNED_ALIAS_REPLAY_SOURCE:
            alias_replay_exposures += 1
            llm_was_avoided = model_output.get("llm_invoked") is False
            alias_replay_llm_avoided += int(llm_was_avoided)
            replay_resolved = (
                row.get("current_fate") == "resolved_for_consumer"
                and isinstance(decision, dict)
                and decision.get("disposition") == "single_referent"
                and selected_candidate_id is not None
                and selected_referent is not None
            )
            alias_replay_resolved += int(replay_resolved)
            if (
                model_output.get("resolution_scope") != "tenant_global_exact"
                or phrase_requires_context(str(row.get("phrase") or ""))
            ):
                contextual_alias_replays += 1
            if (
                not llm_was_avoided
                or (
                    replay_resolved
                    and (
                        model_output.get("closed_set_match") is not True
                        or not model_output.get("identity_basis_ref")
                        or not isinstance(assessment, dict)
                        or not assessment.get("identity_evidence_refs")
                    )
                )
            ):
                unsafe_alias_replays += 1
        if (
            model_output.get("closed_set_match") is False
            and (selected_candidate_id is not None or selected_referent is not None)
        ):
            invented_admissions += 1
        if (
            isinstance(decision, dict)
            and decision.get("disposition") == "single_referent"
            and isinstance(assessment, dict)
            and not assessment.get("identity_evidence_refs")
        ):
            ungrounded_single_admissions += 1
        identity_mutations += int(bool(row.get("identity_registry_mutated")))
        source_mutations += int(bool(row.get("source_observation_mutated")))
    for row in current_rows:
        if row.get("current_fate") == "review":
            review_fates += 1
            review_obligations += int(bool(row.get("has_review_obligation")))

    eligible_count = len(eligible)
    work_count = len(work_by_key)
    terminal_count = len(terminal_work)
    rejected_keys = {
        key
        for key, row in current_detection_by_key.items()
        if row.get("fate")
        in {
            EntityMentionDetectionFate.REJECTED_NOT_ANCHORED.value,
            EntityMentionDetectionFate.REJECTED_NOT_ENTITY.value,
            EntityMentionDetectionFate.UNSUPPORTED_IMPLICIT.value,
        }
    }
    trace_required_terminal = terminal_work - rejected_keys
    traced_terminal = len(set(traces_by_key) & trace_required_terminal)
    request_count = len(request_ids - {"None", ""})
    set_count = len((set_request_ids - {"None", ""}) & request_ids)
    structural_incidents = {
        "duplicate_terminal_trace": duplicate_traces,
        "retry_without_due_time": int(retry_without_due),
        "incomplete_grounding_continuity": len(all_trace_rows) - complete_stage,
        "incomplete_candidate_lane_fate": lane_incomplete,
        "missing_open_world_candidate_options": open_world_missing,
        "future_context_leak": future_context,
        "unauthorized_source_space": source_space_leaks,
        "invented_candidate_admitted": invented_admissions,
        "single_referent_without_identity_basis": ungrounded_single_admissions,
        "resolver_mutated_identity_registry": identity_mutations + resolver_created_alias_count,
        "resolver_mutated_source_observation": source_mutations,
        "resolver_created_self_authoritative_observation": self_authoritative_observation_count,
        "review_without_obligation": review_fates - review_obligations,
        "answered_clarification_without_grounding_lineage": (
            answered_entity_clarification_count
            - answered_entity_clarification_lineage_count
        ),
        "adjudicated_alias_without_grounding_lineage": (
            adjudicated_alias_count - adjudicated_alias_lineage_count
        ),
        "unsafe_governed_alias_replay": unsafe_alias_replays,
        "contextual_governed_alias_replay": contextual_alias_replays,
        "terminal_work_without_trace": len(trace_required_terminal) - traced_terminal,
    }
    for name, count in structural_incidents.items():
        if count > 0:
            incidents[name] += count
            incident_refs[name].add(f"grounding-scope:{scope.run_id}")
    mention_count = len(current_detection_by_key)
    detected_count = detection_fates.get(EntityMentionDetectionFate.DETECTED.value, 0)
    rejected_count = sum(
        detection_fates.get(fate.value, 0)
        for fate in EntityMentionDetectionFate
        if fate is not EntityMentionDetectionFate.DETECTED
    )
    incident_counts = dict(sorted(incidents.items()))
    return EntityGroundingEvaluationState(
        scope=scope,
        eligible_observations=len(eligible_observations),
        eligible_opportunities=eligible_count,
        work_head_count=work_count,
        work_fate_counts=dict(sorted(work_fates.items())),
        work_population_coverage=_ratio(work_count, eligible_count),
        mention_detection_count=mention_count,
        mention_detection_fate_counts=dict(sorted(detection_fates.items())),
        mention_detection_population_coverage=_ratio(mention_count, eligible_count),
        detected_mention_count=detected_count,
        rejected_mention_count=rejected_count,
        explicit_anchor_count=explicit_anchor_count,
        reconstructable_explicit_anchor_count=reconstructable_anchor_count,
        explicit_anchor_reconstructability_rate=_ratio(
            reconstructable_anchor_count, explicit_anchor_count
        ),
        mention_source_hash_match_count=source_hash_matches,
        mention_source_hash_match_rate=_ratio(source_hash_matches, mention_count),
        rejected_not_anchored_count=rejected_not_anchored,
        correct_rejected_not_anchored_count=correct_rejected_not_anchored,
        rejected_not_anchored_correctness_rate=_ratio(
            correct_rejected_not_anchored, rejected_not_anchored
        ),
        mention_context_continuity_count=context_continuity,
        mention_context_continuity_rate=_ratio(context_continuity, mention_count),
        mention_command_result_count=command_result_count,
        mention_command_result_coverage=_ratio(command_result_count, mention_count),
        mention_event_count=event_count,
        mention_event_coverage=_ratio(event_count, mention_count),
        mention_outbox_count=outbox_count,
        mention_outbox_coverage=_ratio(outbox_count, mention_count),
        mention_protocol_closure_count=protocol_closure_count,
        mention_protocol_closure_rate=_ratio(protocol_closure_count, mention_count),
        detected_mention_candidate_count=detected_candidate_count,
        detected_mention_to_candidate_continuity_rate=_ratio(
            detected_candidate_count, detected_count
        ),
        rejected_candidate_request_count=rejected_candidate_requests,
        terminal_work_count=terminal_count,
        terminal_trace_required_count=len(trace_required_terminal),
        retry_scheduled_count=work_fates.get("retry_scheduled", 0),
        retry_without_due_time_count=int(retry_without_due),
        trace_count=len(all_trace_rows),
        traced_terminal_count=traced_terminal,
        terminal_trace_coverage=_ratio(traced_terminal, len(trace_required_terminal)),
        duplicate_trace_count=duplicate_traces,
        stage_complete_trace_count=complete_stage,
        stage_continuity_rate=_ratio(complete_stage, len(all_trace_rows)),
        candidate_request_count=request_count,
        immutable_candidate_set_count=set_count,
        candidate_request_fate_coverage=_ratio(set_count, request_count),
        incomplete_lane_fate_count=lane_incomplete,
        missing_open_world_option_count=open_world_missing,
        future_context_leak_count=future_context,
        unauthorized_source_space_count=source_space_leaks,
        invented_candidate_admission_count=invented_admissions,
        single_referent_without_identity_basis_count=ungrounded_single_admissions,
        identity_registry_mutation_count=identity_mutations,
        source_observation_mutation_count=source_mutations,
        resolver_created_alias_count=resolver_created_alias_count,
        self_authoritative_observation_count=self_authoritative_observation_count,
        review_fate_count=review_fates,
        review_obligation_count=review_obligations,
        review_obligation_coverage=_ratio(review_obligations, review_fates),
        answered_entity_clarification_count=answered_entity_clarification_count,
        answered_entity_clarification_lineage_count=(
            answered_entity_clarification_lineage_count
        ),
        answered_entity_clarification_lineage_coverage=_ratio(
            answered_entity_clarification_lineage_count,
            answered_entity_clarification_count,
        ),
        adjudicated_alias_count=adjudicated_alias_count,
        adjudicated_alias_lineage_count=adjudicated_alias_lineage_count,
        adjudicated_alias_lineage_coverage=_ratio(
            adjudicated_alias_lineage_count,
            adjudicated_alias_count,
        ),
        corrective_memory_observed_reuse_count=(
            corrective_memory_observed_reuse_count
        ),
        alias_replay_exposure_count=alias_replay_exposures,
        alias_replay_resolved_count=alias_replay_resolved,
        alias_replay_resolution_rate=_ratio(
            alias_replay_resolved,
            alias_replay_exposures,
        ),
        alias_replay_llm_avoided_count=alias_replay_llm_avoided,
        unsafe_alias_replay_count=unsafe_alias_replays,
        contextual_alias_replay_count=contextual_alias_replays,
        processing_class_counts=dict(
            sorted(Counter(str(row["processing_class"]) for row in work_by_key.values()).items())
        ),
        incident_counts=incident_counts,
        incident_refs={
            key: tuple(sorted(values)) for key, values in sorted(incident_refs.items())
        },
        uncertainty=(
            "Exact explicit anchors are reconstructable for surfaced legacy phrase opportunities, but no gold mention population establishes mention precision or recall.",
            "Implicit, nested, quoted, abbreviated and elided mentions remain outside this deterministic bootstrap extractor.",
            "Legacy entity refs are not yet verified against a versioned CanonicalReferent registry.",
            "Resolution accuracy, calibration, cross-tenant noninterference, correction closure and downstream oracle gap require broader suites.",
            "Observed corrective-memory reuse is an exposure count; no later matching signal is not treated as failed reuse.",
            "Governed alias replay resolution measures exposed persisted decisions only; adaptive-versus-frozen lift still requires a paired experiment.",
            "Component integration evidence is below the E4 full-system simulation floor.",
        ),
        artifact_refs=artifact_refs,
    )


def build_entity_grounding_invariant_evidence(
    state: EntityGroundingEvaluationState,
    *,
    registry: ArchitectureContractRegistry,
    executed_scenario_ids: frozenset[str],
) -> tuple[InvariantRunEvidence, ...]:
    """Project measured state into four non-compensatory invariant rows."""

    by_id = {item.invariant_id: item for item in registry.invariants}
    common_blind_spots = state.uncertainty
    rows: list[InvariantRunEvidence] = []
    definitions = {
        "INV-04": {
            "metric": "inv.perception_reentry",
            "observed": frozenset({"annotation_writer", "reingest_lineage"}),
            "violations": (
                state.source_observation_mutation_count
                + state.resolver_created_alias_count
                + state.self_authoritative_observation_count
            ),
            "successes": max(
                0,
                state.trace_count
                - state.source_observation_mutation_count
                - state.self_authoritative_observation_count,
            ),
            "incident_prefixes": (
                "resolver_mutated_source",
                "resolver_created_self",
                "resolver_mutated_identity",
            ),
        },
        "INV-05": {
            "metric": "inv.entity_stage_separation",
            "observed": frozenset(
                {
                    "mention_detection_fate",
                    "mention_anchor",
                    "mention_context_snapshot",
                    "detection_command_result_event_outbox",
                    "candidate_request_and_set",
                    "resolution_assessment",
                    "registry_command_and_result",
                    "grounding_admission_versions",
                    "governed_alias_replay_decision_source",
                }
            ),
            "violations": (
                state.eligible_opportunities - state.mention_detection_count
                + state.explicit_anchor_count
                - state.reconstructable_explicit_anchor_count
                + state.mention_detection_count
                - state.mention_context_continuity_count
                + state.mention_detection_count
                - state.mention_protocol_closure_count
                + state.detected_mention_count
                - state.detected_mention_candidate_count
                + state.rejected_candidate_request_count
                + state.trace_count
                - state.stage_complete_trace_count
                + state.identity_registry_mutation_count
                + state.resolver_created_alias_count
                + state.single_referent_without_identity_basis_count
                + state.answered_entity_clarification_count
                - state.answered_entity_clarification_lineage_count
                + state.adjudicated_alias_count
                - state.adjudicated_alias_lineage_count
                + state.contextual_alias_replay_count
            ),
            "successes": (
                state.stage_complete_trace_count
                + state.correct_rejected_not_anchored_count
            ),
            "incident_prefixes": (
                "mention_",
                "detected_mention",
                "rejected_mention",
                "grounding_trace_mention",
                "incomplete_grounding",
                "resolver_mutated_identity",
                "single_referent_without_identity_basis",
                "answered_clarification_without_grounding_lineage",
                "adjudicated_alias_without_grounding_lineage",
                "contextual_governed_alias_replay",
            ),
        },
        "INV-06": {
            "metric": "inv.candidate_total_fate",
            "observed": frozenset(
                {
                    "request_digest",
                    "permitted_lanes",
                    "per_lane_fate",
                    "command_result",
                    "set_hash_and_version",
                    "lineage_head",
                    "authority_fingerprint",
                }
            ),
            "violations": (
                state.candidate_request_count - state.immutable_candidate_set_count
                + state.incomplete_lane_fate_count
                + state.duplicate_trace_count
            ),
            "successes": state.immutable_candidate_set_count,
            "incident_prefixes": (
                "incomplete_candidate",
                "duplicate_terminal",
                "future_context",
                "unauthorized_source",
            ),
        },
        "INV-07": {
            "metric": "inv.local_global_separation",
            "observed": frozenset(),
            "violations": (
                state.identity_registry_mutation_count
                + state.resolver_created_alias_count
                + state.single_referent_without_identity_basis_count
                + state.unsafe_alias_replay_count
            ),
            "successes": max(
                0,
                state.trace_count
                - state.identity_registry_mutation_count
                - state.resolver_created_alias_count,
            ),
            "incident_prefixes": (
                "resolver_mutated_identity",
                "single_referent_without_identity_basis",
                "unsafe_governed_alias_replay",
            ),
        },
    }
    for invariant_id, definition in definitions.items():
        invariant = by_id[invariant_id]
        assert invariant.proof is not None
        if invariant_id == "INV-06":
            eligible = state.candidate_request_count
            attempted = state.candidate_request_count
            terminal_fates = {"immutable_set": state.immutable_candidate_set_count}
        else:
            eligible = state.eligible_opportunities
            attempted = state.work_head_count
            terminal_fates = {
                key: value
                for key, value in state.work_fate_counts.items()
                if key in _TERMINAL_WORK_FATES
            }
            nonterminal = state.work_fate_counts.get("retry_scheduled", 0)
        if invariant_id == "INV-06":
            nonterminal = 0
        denominator = FateDenominatorRecord(
            denominator_id=f"{state.scope.run_id}:{invariant_id}:grounding-population",
            denominator_version="entity-grounding-denominator-v1",
            population_definition_version=(
                "accepted-candidate-request-digests-v1"
                if invariant_id == "INV-06"
                else "observation-unresolved-phrase-union-v1"
            ),
            query_or_manifest_hash=canonical_sha256(
                {
                    "scope": state.scope.model_dump(mode="json"),
                    "artifacts": state.artifact_refs,
                    "invariant_id": invariant_id,
                }
            ),
            source_or_oracle_population=eligible,
            production_accepted=eligible,
            eligible=eligible,
            attempted_or_committed=attempted,
            terminal_fates=terminal_fates,
            nonterminal_fates={"retry_scheduled": nonterminal} if nonterminal else {},
            report_cutoff=state.scope.observation_end.isoformat(),
            population_partition_dimension=CANONICAL_COMPONENT_PARTITION_DIMENSION,
            population_partition_value="entity_grounding",
            population_partition_proof_ref=CANONICAL_COMPONENT_PARTITION_PROOF_REF,
        )
        prefixes = definition["incident_prefixes"]
        incidents = tuple(
            IncidentObservation(
                incident_id=f"{state.scope.run_id}:{invariant_id}:{incident_class}",
                incident_class=incident_class,
                status=IncidentStatus.CONFIRMED,
                severity=(
                    5
                    if (
                        "authority" in incident_class
                        or "mutation" in incident_class
                        or "unsafe" in incident_class
                    )
                    else 4
                ),
                summary=f"Observed {count} scoped {incident_class} violations.",
                artifact_refs=state.artifact_refs,
            )
            for incident_class, count in state.incident_counts.items()
            if any(incident_class.startswith(prefix) for prefix in prefixes)
        )
        if invariant_id == "INV-06":
            metric_denominator = state.candidate_request_count
        elif invariant_id == "INV-05":
            metric_denominator = state.eligible_opportunities
        else:
            metric_denominator = state.trace_count
        successes = min(int(definition["successes"]), metric_denominator)
        metric_observations = [
            MetricObservation(
                metric_id=str(definition["metric"]),
                metric_version="entity-grounding-runtime-v1",
                raw_numerator=float(successes),
                raw_denominator=float(metric_denominator),
                point_estimate=(
                    successes / metric_denominator
                    if metric_denominator
                    else None
                ),
                violation_count=int(definition["violations"]),
                severity_mass=float(definition["violations"]),
                artifact_refs=state.artifact_refs,
            )
        ]
        if invariant_id == "INV-05":
            metric_observations.append(
                MetricObservation(
                    metric_id="entity.governed_alias_replay_resolution",
                    metric_version="governed-exact-alias-replay-v1",
                    raw_numerator=float(state.alias_replay_resolved_count),
                    raw_denominator=float(state.alias_replay_exposure_count),
                    point_estimate=state.alias_replay_resolution_rate,
                    violation_count=0,
                    severity_mass=0.0,
                    artifact_refs=state.artifact_refs,
                )
            )
        rows.append(
            InvariantRunEvidence(
                invariant_id=invariant_id,
                applicable_exposures=eligible,
                observed_trace_facts=definition["observed"],
                executed_scenario_ids=frozenset(invariant.proof.suite_and_scenario_ids)
                & executed_scenario_ids,
                metric_observations=tuple(metric_observations),
                incidents=incidents,
                achieved_evidence_tier=EvidenceTier.E3,
                denominator=denominator,
                uncertainty=common_blind_spots,
                blind_spots=common_blind_spots,
                artifact_refs=state.artifact_refs,
            )
        )
    return tuple(rows)


def render_entity_grounding_markdown(state: EntityGroundingEvaluationState) -> str:
    lines = [
        f"# Entity-grounding evaluation: {state.scope.run_id}",
        "",
        f"- Tenant: `{state.scope.tenant_id}`",
        f"- Observation interval: `{state.scope.observation_start.isoformat()}` to `{state.scope.observation_end.isoformat()}`",
        f"- Eligible observations/opportunities: **{state.eligible_observations}/{state.eligible_opportunities}**",
        f"- Durable work coverage: **{state.work_head_count}/{state.eligible_opportunities} ({_format_rate(state.work_population_coverage)})**",
        f"- Mention-fate coverage: **{state.mention_detection_count}/{state.eligible_opportunities} ({_format_rate(state.mention_detection_population_coverage)})**",
        f"- Exact explicit-anchor reconstructability: **{state.reconstructable_explicit_anchor_count}/{state.explicit_anchor_count} ({_format_rate(state.explicit_anchor_reconstructability_rate)})**",
        f"- Mention context continuity: **{state.mention_context_continuity_count}/{state.mention_detection_count} ({_format_rate(state.mention_context_continuity_rate)})**",
        f"- Mention protocol closure: **{state.mention_protocol_closure_count}/{state.mention_detection_count} ({_format_rate(state.mention_protocol_closure_rate)})**",
        f"- Detected mention-to-candidate continuity: **{state.detected_mention_candidate_count}/{state.detected_mention_count} ({_format_rate(state.detected_mention_to_candidate_continuity_rate)})**",
        f"- Correct not-anchored rejections: **{state.correct_rejected_not_anchored_count}/{state.rejected_not_anchored_count} ({_format_rate(state.rejected_not_anchored_correctness_rate)})**",
        f"- Terminal trace coverage where a trace is required: **{state.traced_terminal_count}/{state.terminal_trace_required_count} ({_format_rate(state.terminal_trace_coverage)})**",
        f"- Stage continuity: **{state.stage_complete_trace_count}/{state.trace_count} ({_format_rate(state.stage_continuity_rate)})**",
        f"- Candidate request fate coverage: **{state.immutable_candidate_set_count}/{state.candidate_request_count} ({_format_rate(state.candidate_request_fate_coverage)})**",
        f"- Answered entity-clarification lineage: **{state.answered_entity_clarification_lineage_count}/{state.answered_entity_clarification_count} ({_format_rate(state.answered_entity_clarification_lineage_coverage)})**",
        f"- Adjudicated-alias grounding lineage: **{state.adjudicated_alias_lineage_count}/{state.adjudicated_alias_count} ({_format_rate(state.adjudicated_alias_lineage_coverage)})**",
        f"- Corrective-memory future reuse observed: **{state.corrective_memory_observed_reuse_count}**",
        f"- Governed alias replay resolution: **{state.alias_replay_resolved_count}/{state.alias_replay_exposure_count} ({_format_rate(state.alias_replay_resolution_rate)})**",
        f"- Governed alias replay LLM calls avoided: **{state.alias_replay_llm_avoided_count}**",
        f"- Unsafe/contextual governed alias replays: **{state.unsafe_alias_replay_count}/{state.contextual_alias_replay_count}**",
        "",
        "## Fate distribution",
        "",
        *(f"- {fate}: {count}" for fate, count in state.work_fate_counts.items()),
        "",
        "## Mention-detection fate distribution",
        "",
        *(
            (
                f"- {fate}: {count}"
                for fate, count in state.mention_detection_fate_counts.items()
            )
            if state.mention_detection_fate_counts
            else ("- no mention detections in this scope",)
        ),
        "",
        "## Constitutional and structural incidents",
        "",
        *(
            (f"- {name}: {count}" for name, count in state.incident_counts.items())
            if state.incident_counts
            else ("- none observed in this scope",)
        ),
        "",
        "## Proof limits",
        "",
        *(f"- {item}" for item in state.uncertainty),
        "",
    ]
    return "\n".join(lines)


def _ratio(numerator: int, denominator: int) -> float | None:
    return numerator / denominator if denominator else None


def _format_rate(value: float | None) -> str:
    return "unknown/not exposed" if value is None else f"{value:.1%}"


def _parse_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
    return None


def _restriction_values(value: Any) -> set[str] | None:
    if not isinstance(value, dict):
        return set()
    if value.get("universe") is True:
        return None
    return set(value.get("values") or ())


__all__ = [
    "EntityGroundingEvaluationState",
    "GroundingEvaluationScope",
    "analyze_entity_grounding_rows",
    "build_entity_grounding_invariant_evidence",
    "evaluate_entity_grounding_state",
    "render_entity_grounding_markdown",
]
