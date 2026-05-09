#!/usr/bin/env python3
"""
check_schema_drift.py — verify live Postgres matches SCHEMA-LOCK.md.

Usage:
    python scripts/check_schema_drift.py                 # uses $DATABASE_URL
    python scripts/check_schema_drift.py --dsn postgresql://...

Exits 0 if the live database exactly matches the expected schema,
1 if there is any drift. Prints every drift it finds.

The expected schema below is the hand-authored authoritative copy of
SCHEMA-LOCK.md sections S1-S6 plus required extensions. If you change
a migration, you must also change this file. The whole point of the
drift check is to scream when migrations and the lock file diverge.
"""
from __future__ import annotations

import argparse
import os
import sys
from dataclasses import dataclass, field

import psycopg2
import psycopg2.extras


# ---------------------------------------------------------------------
# Expected schema — mirrors SCHEMA-LOCK.md + db/migrations/0001_foundation.sql
# ---------------------------------------------------------------------

EXPECTED_EXTENSIONS = {"vector", "pg_trgm", "btree_gin"}


@dataclass
class Column:
    name: str
    data_type: str        # information_schema.data_type value (e.g. "uuid", "text", "USER-DEFINED" for vector)
    is_nullable: bool
    has_default: bool = False


@dataclass
class Table:
    columns: dict[str, Column] = field(default_factory=dict)
    indexes: set[str] = field(default_factory=set)
    is_partitioned: bool = False


def _col(name: str, dtype: str, nullable: bool, default: bool = False) -> tuple[str, Column]:
    return name, Column(name=name, data_type=dtype, is_nullable=nullable, has_default=default)


# Data types as reported by information_schema.columns.data_type:
#   uuid            -> "uuid"
#   text            -> "text"
#   boolean         -> "boolean"
#   integer         -> "integer"
#   bigint          -> "bigint"
#   double precision -> "double precision"
#   timestamp with time zone -> "timestamp with time zone"
#   jsonb           -> "jsonb"
#   uuid[]          -> "ARRAY"
#   VECTOR(768)     -> "USER-DEFINED"
#   numeric         -> "numeric"
UUID = "uuid"
TEXT = "text"
BOOL = "boolean"
INT = "integer"
BIGINT = "bigint"
FLOAT = "double precision"
TS = "timestamp with time zone"
JSONB = "jsonb"
ARRAY = "ARRAY"
VECTOR = "USER-DEFINED"


EXPECTED_TABLES: dict[str, Table] = {
    "actors": Table(
        columns=dict([
            _col("id", UUID, False),
            _col("tenant_id", UUID, False),
            _col("type", TEXT, False),
            _col("display_name", TEXT, False),
            _col("email", TEXT, True),
            _col("status", TEXT, True, default=True),
            _col("metadata", JSONB, True),
            _col("specification_id", UUID, True),
            _col("created_at", TS, False, default=True),
            _col("last_seen_at", TS, True),
        ]),
        indexes={"actors_pkey", "actors_email_idx", "actors_type_idx"},
    ),
    "actor_identity_mappings": Table(
        columns=dict([
            _col("actor_id", UUID, False),
            _col("source_channel", TEXT, False),
            _col("source_actor_ref", TEXT, False),
            _col("confidence", FLOAT, True, default=True),
            _col("created_at", TS, True, default=True),
        ]),
        indexes={"actor_identity_mappings_pkey"},
    ),
    "observations": Table(
        columns=dict([
            _col("id", UUID, False),
            _col("tenant_id", UUID, False),
            _col("occurred_at", TS, False),
            _col("ingested_at", TS, False, default=True),
            _col("kind", TEXT, False),
            _col("source_channel", TEXT, False),
            _col("source_actor_ref", TEXT, True),
            _col("actor_id", UUID, True),
            _col("content", JSONB, False),
            _col("content_text", TEXT, False),
            _col("embedding", VECTOR, True),
            _col("embedding_pending", BOOL, True, default=True),
            _col("trust_tier", TEXT, False),
            _col("external_id", TEXT, True),
            _col("cause_id", UUID, True),
            _col("sequence_num", BIGINT, False, default=True),
            _col("entities_mentioned", JSONB, True, default=True),
        ]),
        indexes={
            "observations_pkey",
            "observations_source_channel_external_id_occurred_at_key",
            "obs_embedding_idx",
            "obs_actor_time_idx",
            "obs_channel_time_idx",
            "obs_kind_idx",
            "obs_cause_idx",
            "obs_entities_idx",
            "obs_tenant_time_idx",
        },
        is_partitioned=True,
    ),
    "models": Table(
        columns=dict([
            _col("id", UUID, False),
            _col("tenant_id", UUID, False),
            _col("born_from_event_id", UUID, False),
            _col("proposition", JSONB, False),
            _col("natural", TEXT, False),
            _col("embedding", VECTOR, False),
            _col("scope_actors", ARRAY, True, default=True),
            _col("scope_entities", JSONB, True, default=True),
            _col("scope_temporal", JSONB, False),
            _col("confidence", FLOAT, False),
            _col("activation", FLOAT, False, default=True),
            _col("falsifier", JSONB, True),
            _col("signal_readings", JSONB, True, default=True),
            _col("reading_contestable", BOOL, True, default=True),
            _col("supporting_event_ids", ARRAY, True, default=True),
            _col("supporting_model_ids", ARRAY, True, default=True),
            _col("evidential_weight", FLOAT, True, default=True),
            _col("status", TEXT, False, default=True),
            _col("archived_at", TS, True),
            _col("archive_reason", TEXT, True),
            _col("created_at", TS, False, default=True),
            _col("last_retrieved_at", TS, True),
            _col("retrieval_count", INT, True, default=True),
            _col("evaluate_at", TS, True),
            _col("resolution_criteria", JSONB, True),
            _col("contributing_models", ARRAY, True, default=True),
            _col("visible_to_subjects", BOOL, True, default=True),
            # Post-Wave-0 amendments A1 (SCHEMA-LOCK.md)
            _col("proposition_kind", TEXT, True),   # generated stored -> nullable in info_schema
            _col("confirmed_count", INT, False, default=True),
            _col("contested_count", INT, False, default=True),
            _col("last_confirmed_at", TS, True),
            _col("confidence_at_assertion", FLOAT, False),
            _col("resolved_at", TS, True),
            _col("resolution_outcome", BOOL, True),
            _col("activation_coefficient", FLOAT, False, default=True),
            # Migration 0022 — recommendation proposition support.
            _col("target_actor_id", UUID, True),         # generated stored
            _col("caused_act_change_id", UUID, True),
        ]),
        indexes={
            "models_pkey",
            "models_embedding_idx",
            "models_actors_idx",
            "models_entities_idx",
            "models_evaluate_idx",
            "models_retrieved_idx",
            "models_tenant_status_idx",
            "models_supporting_idx",
            "models_activation_idx",
            "models_proposition_kind_idx",   # A2
            "recommendations_active_idx",    # 0022
        },
    ),
    "model_status_notes": Table(
        # A4 — sidecar freeform annotations.
        columns=dict([
            _col("id", UUID, False),
            _col("model_id", UUID, False),
            _col("note", TEXT, False),
            _col("authored_by", UUID, True),
            _col("authored_at", TS, False, default=True),
            _col("kind", TEXT, False),
        ]),
        indexes={
            "model_status_notes_pkey",
            "model_status_notes_model_idx",
        },
    ),
    "goals": Table(
        columns=dict([
            _col("id", UUID, False),
            _col("tenant_id", UUID, False),
            _col("title", TEXT, False),
            _col("description", TEXT, True),
            _col("state", TEXT, False, default=True),
            _col("target_date", TS, True),
            _col("parent_goal_id", UUID, True),
            _col("altitude", TEXT, True, default=True),
            _col("success_criteria", JSONB, True),
            _col("cached_health", TEXT, True, default=True),
            _col("cached_health_computed_at", TS, True),
            _col("created_at", TS, False, default=True),
            _col("last_state_change_at", TS, False, default=True),
            _col("created_by_event_id", UUID, False),
            _col("archived_at", TS, True),
        ]),
        indexes={"goals_pkey", "goals_state_idx", "goals_parent_idx", "goals_altitude_idx"},
    ),
    "commitments": Table(
        columns=dict([
            _col("id", UUID, False),
            _col("tenant_id", UUID, False),
            _col("title", TEXT, False),
            _col("description", TEXT, True),
            _col("state", TEXT, False, default=True),
            _col("owner_id", UUID, True),
            _col("due_date", TS, True),
            _col("ambition_level", TEXT, True, default=True),
            _col("priority", INT, True, default=True),
            _col("success_criteria", JSONB, True),
            _col("resolved_by_event_ids", ARRAY, True, default=True),
            _col("external_counterparty_ref", JSONB, True),
            _col("estimated_capacity", JSONB, True),
            _col("created_at", TS, False, default=True),
            _col("last_state_change_at", TS, False, default=True),
            _col("terminal_at", TS, True),
            _col("created_by_event_id", UUID, False),
            _col("last_confidence_basis", UUID, True),
        ]),
        indexes={
            "commitments_pkey",
            "commitments_state_idx",
            "commitments_owner_idx",
            "commitments_due_idx",
        },
    ),
    "commitment_contributors": Table(
        columns=dict([
            _col("commitment_id", UUID, False),
            _col("actor_id", UUID, False),
            _col("role", TEXT, True),
        ]),
        indexes={"commitment_contributors_pkey", "commitments_contributors_actor_idx"},
    ),
    "decisions": Table(
        columns=dict([
            _col("id", UUID, False),
            _col("tenant_id", UUID, False),
            _col("title", TEXT, False),
            _col("decision_text", TEXT, False),
            _col("rationale", TEXT, True),
            _col("state", TEXT, False, default=True),
            _col("scope", JSONB, True),
            _col("revisit_triggers", JSONB, True),
            _col("created_at", TS, False, default=True),
            _col("last_state_change_at", TS, False, default=True),
            _col("created_by_event_id", UUID, False),
            _col("archived_at", TS, True),
        ]),
        indexes={"decisions_pkey", "decisions_state_idx"},
    ),
    "contributes_to": Table(
        columns=dict([
            _col("commitment_id", UUID, False),
            _col("goal_id", UUID, False),
            _col("is_critical_path", BOOL, True, default=True),
        ]),
        indexes={"contributes_to_pkey", "contributes_goal_idx", "contributes_critical_idx"},
    ),
    "depends_on": Table(
        columns=dict([
            _col("dependent_commitment_id", UUID, False),
            _col("dependency_commitment_id", UUID, False),
        ]),
        indexes={"depends_on_pkey", "depends_dependency_idx"},
    ),
    "constrained_by": Table(
        columns=dict([
            _col("commitment_id", UUID, False),
            _col("decision_id", UUID, False),
        ]),
        indexes={"constrained_by_pkey", "constrained_decision_idx"},
    ),
    "resources": Table(
        columns=dict([
            _col("id", UUID, False),
            _col("tenant_id", UUID, False),
            _col("kind", TEXT, False),
            _col("identity", TEXT, False),
            _col("description", TEXT, True),
            _col("current_value", JSONB, False),
            _col("valuation_confidence", FLOAT, True, default=True),
            _col("utilization_state", TEXT, False, default=True),
            _col("controllability", TEXT, False, default=True),
            _col("temporal_character", TEXT, False, default=True),
            _col("metadata", JSONB, True),
            _col("created_at", TS, False, default=True),
            _col("last_updated_at", TS, False, default=True),
            _col("last_updated_by_event_id", UUID, True),
            _col("archived_at", TS, True),
        ]),
        indexes={"resources_pkey", "resources_kind_idx", "resources_utilization_idx"},
    ),
    "resource_transactions": Table(
        columns=dict([
            _col("id", UUID, False),
            _col("resource_id", UUID, False),
            _col("tenant_id", UUID, False),
            _col("transaction_type", TEXT, False),
            _col("delta", JSONB, False),
            _col("occurred_at", TS, False),
            _col("source_event_id", UUID, False),
            _col("created_at", TS, False, default=True),
        ]),
        indexes={"resource_transactions_pkey", "resource_tx_resource_idx"},
        is_partitioned=True,
    ),
    "resource_deployments": Table(
        columns=dict([
            _col("resource_id", UUID, False),
            _col("commitment_id", UUID, False),
            _col("deployed_quantity", JSONB, True),
            _col("deployed_at", TS, False, default=True),
            _col("released_at", TS, True),
        ]),
        indexes={"resource_deployments_pkey", "resource_deployments_commitment_idx"},
    ),
    "customer_commitments": Table(
        # Q2 resolved (Option B1): superset shape from spec §27 adopted
        # via migration 0014. Column list reflects the post-migration
        # state. See SCHEMA-LOCK.md "Post-Wave-4→5 amendments" W5.Q2.
        columns=dict([
            _col("id", UUID, False, default=True),
            _col("tenant_id", UUID, False),
            _col("customer_resource_id", UUID, False),
            _col("commitment_id", UUID, False),
            _col("served_description", TEXT, True),
            _col("relationship_kind", TEXT, False, default=True),
            _col("revenue_at_risk_usd", "numeric", True),
            _col("criticality", TEXT, False, default=True),
            _col("created_at", TS, False, default=True),
        ]),
        indexes={
            "customer_commitments_pkey",
            "customer_commitments_customer_commitment_key",
            "customer_commitments_tenant_idx",
            "customer_commitments_criticality_idx",
            "customer_commitments_revenue_idx",
        },
    ),
    "entity_aliases": Table(
        columns=dict([
            _col("id", UUID, False),
            _col("tenant_id", UUID, False),
            _col("alias_text", TEXT, False),
            _col("alias_embedding", VECTOR, True),
            _col("actor_id", UUID, True),
            _col("resolved_entity_ref", JSONB, False),
            _col("is_canonical", BOOL, True, default=True),
            _col("entity_metadata", JSONB, True),
            _col("confidence", FLOAT, False, default=True),
            _col("confirmed_count", INT, True, default=True),
            _col("contested_count", INT, True, default=True),
            _col("first_seen_at", TS, False, default=True),
            _col("last_used_at", TS, False, default=True),
            _col("source_event_id", UUID, True),
        ]),
        indexes={
            "entity_aliases_pkey",
            "entity_aliases_tenant_id_alias_text_actor_id_key",
            "aliases_embedding_idx",
            "aliases_text_idx",
            "aliases_actor_idx",
            "aliases_entity_idx",
            "aliases_canonical_idx",
        },
    ),
    # -----------------------------------------------------------------
    # Wave 2-A additions — Gateway + Ingestion (migrations 0003, 0004)
    # -----------------------------------------------------------------
    "actor_sessions": Table(
        # 0003_actor_sessions.sql — Gateway bearer-token sessions.
        columns=dict([
            _col("id", UUID, False),
            _col("tenant_id", UUID, False),
            _col("actor_id", UUID, False),
            _col("token_hash", TEXT, False),
            _col("expires_at", TS, False),
            _col("created_at", TS, False, default=True),
            _col("revoked_at", TS, True),
        ]),
        indexes={
            "actor_sessions_pkey",
            "actor_sessions_token_hash_key",
            "actor_sessions_actor_idx",
            "actor_sessions_expires_idx",
        },
    ),
    "think_trigger_queue": Table(
        # 0004_think_trigger_queue.sql — T1/T2/T3/T4 Think trigger queue.
        # Partially resolves SCHEMA-QUESTION.md Q4.
        columns=dict([
            _col("id", UUID, False),
            _col("tenant_id", UUID, False),
            _col("trigger_kind", TEXT, False),
            _col("trigger_subkind", TEXT, True),
            _col("observation_id", UUID, True),
            _col("model_id", UUID, True),
            _col("payload", JSONB, False, default=True),
            _col("enqueued_at", TS, False, default=True),
            _col("scheduled_for", TS, False, default=True),
            _col("attempts", INT, False, default=True),
            _col("locked_by", TEXT, True),
            _col("locked_at", TS, True),
            _col("completed_at", TS, True),
        ]),
        indexes={
            "think_trigger_queue_pkey",
            "think_trigger_queue_ready_idx",
        },
    ),
    # -----------------------------------------------------------------
    # Wave 2-B additions — Entity resolver (migration 0005)
    # -----------------------------------------------------------------
    "entity_review_queue": Table(
        # 0005_entity_review_queue.sql — human-review queue for the
        # entity resolver worker (phrases with 0.5-0.8 confidence).
        # Not in SCHEMA-LOCK.md S1-S6; added per BUILD-PLAN §3 Prompt
        # 2.B explicit language "create the schema yourself in a new
        # migration".
        columns=dict([
            _col("id", UUID, False),
            _col("tenant_id", UUID, False),
            _col("phrase", TEXT, False),
            _col("source_observation_id", UUID, False),
            _col("candidates", JSONB, False),
            _col("created_at", TS, False, default=True),
            _col("resolved_at", TS, True),
            _col("resolved_by", UUID, True),
            _col("chosen_ref", JSONB, True),
            _col("dismissed_reason", TEXT, True),
        ]),
        indexes={
            "entity_review_queue_pkey",
            "entity_review_queue_open_idx",
        },
    ),
    "model_reeval_queue": Table(
        # 0007_q4_q8_resolutions.sql — Q8 resolution. Dependent Models
        # waiting for re-evaluation after a supporting Model is
        # archived / deprecated / superseded / contested. Consumed by
        # the cascade engine inside Think (Wave 3-B). Migration 0031
        # dropped the cause_kind CHECK because cause_kinds are now
        # declarative (registry-owned).
        columns=dict([
            _col("id", UUID, False),
            _col("tenant_id", UUID, False),
            _col("model_id", UUID, False),
            _col("cause_model_id", UUID, True),
            _col("cause_kind", TEXT, False),
            _col("enqueued_at", TS, False, default=True),
            _col("processed_at", TS, True),
            _col("attempts", INT, False, default=True),
            _col("last_error", TEXT, True),
        ]),
        indexes={
            "model_reeval_queue_pkey",
            "model_reeval_queue_dedup",
            "model_reeval_queue_pending_idx",
            "model_reeval_queue_model_idx",
        },
    ),
    "model_edges": Table(
        # 0031_model_edges.sql — S1 of the self-organizing-substrate
        # plan. Unified Model-to-Model edge primitive. Replaces the
        # seven ad-hoc connection mechanisms (supporting_model_ids
        # array, contributing_models array, pattern back-link,
        # archive_reason='superseded' lifecycle flag, latent
        # proposition-encoded edges) with a single typed-edge table
        # whose semantics are declared in lib/shared/edge_registry.py.
        # Dual-write phase: arrays remain authoritative; drift
        # detector verifies parity.
        columns=dict([
            _col("id", UUID, False),
            _col("tenant_id", UUID, False),
            _col("source_model_id", UUID, False),
            _col("target_model_id", UUID, False),
            _col("edge_kind", TEXT, False),
            _col("weight", FLOAT, True),
            _col("metadata", JSONB, False, default=True),
            _col("status", TEXT, False, default=True),
            _col("detected_by", TEXT, False),
            _col("created_at", TS, False, default=True),
            _col("created_by_event_id", UUID, True),
            _col("status_changed_at", TS, True),
            _col("status_reason", TEXT, True),
        ]),
        indexes={
            "model_edges_pkey",
            "model_edges_unique",
            "model_edges_source_idx",
            "model_edges_target_idx",
            "model_edges_kind_idx",
        },
    ),
    "think_region_lock_log": Table(
        # 0007_q4_q8_resolutions.sql — Q4 resolution (observability
        # only; enforcement is pg_advisory_xact_lock). Records every
        # region lock acquisition for contention analysis.
        columns=dict([
            _col("id", UUID, False),
            _col("tenant_id", UUID, False),
            _col("think_run_id", UUID, False),
            _col("tenant_hash", INT, False),
            _col("entity_hash", INT, False),
            _col("entity_ids", JSONB, False),
            _col("acquired_at", TS, False),
            _col("released_at", TS, True),
            _col("wait_duration_ms", INT, True),
            _col("hold_duration_ms", INT, True),
        ]),
        indexes={
            "think_region_lock_log_pkey",
            "think_region_lock_log_run_idx",
            "think_region_lock_log_time_idx",
        },
    ),
    # -----------------------------------------------------------------
    # Wave 3-B additions — Think operational tables (0008)
    # -----------------------------------------------------------------
    "applied_triggers": Table(
        # 0008 — idempotency ledger for Think runs. Spec §7.
        columns=dict([
            _col("trigger_id", UUID, False),
            _col("tenant_id", UUID, False),
            _col("applied_at", TS, False, default=True),
            _col("diff_hash", TEXT, False),
            _col("trigger_kind", TEXT, False),
            _col("outcome", TEXT, False),
        ]),
        indexes={
            "applied_triggers_pkey",
            "applied_triggers_tenant_time_idx",
        },
    ),
    "think_runs": Table(
        # 0008 — one row per Think invocation. Observability.
        columns=dict([
            _col("id", UUID, False),
            _col("tenant_id", UUID, False),
            _col("trigger_id", UUID, False),
            _col("trigger_kind", TEXT, False),
            _col("started_at", TS, False, default=True),
            _col("ended_at", TS, True),
            _col("status", TEXT, False, default=True),
            _col("error", TEXT, True),
            _col("retrieval_model_count", INT, True),
            _col("retrieval_observation_count", INT, True),
            _col("llm_latency_ms", INT, True),
            _col("validation_error_count", INT, True),
            _col("ops_applied", JSONB, True),
            _col("cascade_depth", INT, True, default=True),
            _col("region_tenant_hash", INT, True),
            _col("region_entity_hash", INT, True),
        ]),
        indexes={
            "think_runs_pkey",
            "think_runs_tenant_time_idx",
            "think_runs_trigger_idx",
            "think_runs_status_idx",
        },
    ),
    "model_reeval_dead_letter": Table(
        # 0008 — Wave 2→3 W3.Q8 N=5 policy.
        columns=dict([
            _col("id", UUID, False),
            _col("tenant_id", UUID, False),
            _col("original_queue_id", UUID, False),
            _col("model_id", UUID, False),
            _col("cause_model_id", UUID, True),
            _col("cause_kind", TEXT, False),
            _col("attempts", INT, False),
            _col("last_error", TEXT, False),
            _col("enqueued_at", TS, False),
            _col("dead_lettered_at", TS, False, default=True),
        ]),
        indexes={
            "model_reeval_dead_letter_pkey",
            "model_reeval_dead_letter_tenant_idx",
            "model_reeval_dead_letter_model_idx",
        },
    ),
    "think_anomalies_raw": Table(
        # 0008 — durable queue for anomalies detected inside apply.
        columns=dict([
            _col("id", UUID, False),
            _col("tenant_id", UUID, False),
            _col("think_run_id", UUID, False),
            _col("kind", TEXT, False),
            _col("region", JSONB, False),
            _col("significance", FLOAT, False),
            _col("triggering_op", JSONB, False),
            _col("published_at", TS, False, default=True),
            _col("consumed_at", TS, True),
        ]),
        indexes={
            "think_anomalies_raw_pkey",
            "think_anomalies_raw_pending_idx",
            "think_anomalies_raw_run_idx",
        },
    ),
    # -----------------------------------------------------------------
    # Wave 3-A additions — Retrieval background maintenance (0006)
    # -----------------------------------------------------------------
    "relationship_maintenance_log": Table(
        # 0006_relationship_maintenance_log.sql — audit trail for the
        # retrieval `background_relationship_maintenance` worker. Every
        # orphan flag / activation outlier / archival suggestion /
        # percentile snapshot emitted by one invocation shares a
        # `run_id`. Read-only with respect to Models — this log is the
        # only write side effect of the maintenance worker.
        # Not in SCHEMA-LOCK.md S1-S6; added per explicit BUILD-PLAN §4
        # Prompt 3.A "NEW TABLE YOU MUST CREATE" text.
        columns=dict([
            _col("id", UUID, False),
            _col("tenant_id", UUID, False),
            _col("run_id", UUID, False),
            _col("run_started_at", TS, False),
            _col("entry_kind", TEXT, False),
            _col("subject_model_id", UUID, True),
            _col("payload", JSONB, False, default=True),
            _col("created_at", TS, False, default=True),
        ]),
        indexes={
            "relationship_maintenance_log_pkey",
            "relationship_maintenance_log_run_idx",
            "relationship_maintenance_log_tenant_time_idx",
            "relationship_maintenance_log_kind_idx",
        },
    ),
    # -----------------------------------------------------------------
    # Wave 4-B additions — Anomaly processor Memory Fabric (0009)
    # -----------------------------------------------------------------
    "signal_memory_fabric": Table(
        # 0009_signal_memory_fabric.sql — sub-threshold anomaly
        # accumulator per ARCHITECTURE-FINAL.md §18 line 3693. Rows
        # land below SIGNIFICANCE_THRESHOLD; the promote sweep marks
        # promoted_at when a region crosses the count threshold.
        # Partially resolves SCHEMA-QUESTION.md Q4 (pattern_candidates
        # still open).
        columns=dict([
            _col("id", UUID, False),
            _col("tenant_id", UUID, False),
            _col("region_hash", TEXT, False),
            _col("signal_ref", JSONB, False),
            _col("significance", FLOAT, False),
            _col("recorded_at", TS, False, default=True),
            _col("promoted_at", TS, True),
        ]),
        indexes={
            "signal_memory_fabric_pkey",
            "fabric_region",
            "fabric_unpromoted",
        },
    ),
    # -----------------------------------------------------------------
    # Wave 4-C additions — Precipitation + Calibration (0010, 0011)
    # -----------------------------------------------------------------
    "pattern_candidates": Table(
        # 0010_pattern_candidates.sql — CLOSES the last open item in
        # SCHEMA-QUESTION.md Q4. Precipitation worker writes clusters
        # of ≥3 hypothesis/concern Models with density ≥0.5; Think T4
        # consumes via trigger_subkind='pattern_review'; on promote the
        # candidate points at the inserted Pattern Model.
        columns=dict([
            _col("id", UUID, False),
            _col("tenant_id", UUID, False),
            _col("proposed_signature", JSONB, False),
            _col("observed_tendency", JSONB, False),
            _col("constituent_model_ids", ARRAY, False),
            _col("cluster_size", INT, False),
            _col("density", FLOAT, False),
            _col("proposed_at", TS, False, default=True),
            _col("promoted_at", TS, True),
            _col("promoted_pattern_model_id", UUID, True),
            _col("rejected_at", TS, True),
            _col("rejection_reason", TEXT, True),
        ]),
        indexes={
            "pattern_candidates_pkey",
            "pattern_candidates_pending_idx",
            "pattern_candidates_promoted_idx",
        },
    ),
    "calibration_stats": Table(
        # 0011_calibration_tables.sql — ARCHITECTURE-FINAL.md §9 lines
        # 2596-2606 verbatim. Append-only log of every resolved
        # prediction; the weekly Calibration updater reads here and
        # writes calibration_offsets.
        columns=dict([
            _col("id", UUID, False),
            _col("tenant_id", UUID, False),
            _col("actor_id", UUID, False),
            _col("proposition_kind", TEXT, False),
            _col("asserted_confidence", FLOAT, False),
            _col("outcome", BOOL, True),
            _col("resolved_at", TS, False),
            _col("source_model_id", UUID, False),
        ]),
        indexes={
            "calibration_stats_pkey",
            "calibration_stats_actor_kind_idx",
            "calibration_stats_resolved_idx",
        },
    ),
    "calibration_offsets": Table(
        # 0011_calibration_tables.sql — ARCHITECTURE-FINAL.md §9 lines
        # 2612-2622 verbatim. Bucketed offsets per (actor, kind).
        # Consumed by services/models/calibration.py::apply_calibration
        # on every Think-insert path to adjust raw confidence before
        # the [0.05, 0.95] clip.
        columns=dict([
            _col("tenant_id", UUID, False),
            _col("actor_id", UUID, False),
            _col("proposition_kind", TEXT, False),
            _col("bucket_low", FLOAT, False),
            _col("bucket_high", FLOAT, False),
            _col("offset", FLOAT, False),
            _col("sample_size", INT, False),
            _col("last_updated", TS, False, default=True),
        ]),
        indexes={
            "calibration_offsets_pkey",
        },
    ),
    # -----------------------------------------------------------------
    # Wave 4-D additions — Realtime + Maintenance (0012, 0013)
    # -----------------------------------------------------------------
    "realtime_replay_cursors": Table(
        # 0012_realtime_replay_cursors.sql — WS replay bookmarks.
        # PK is composite (tenant_id, actor_id, subscription_id) — no
        # surrogate id column.
        columns=dict([
            _col("tenant_id", UUID, False),
            _col("actor_id", UUID, False),
            _col("subscription_id", UUID, False),
            _col("last_delivered_sequence_num", BIGINT, False),
            _col("last_ack_at", TS, False, default=True),
        ]),
        indexes={
            "realtime_replay_cursors_pkey",
            "realtime_replay_cursors_stale_idx",
        },
    ),
    "orphan_log": Table(
        # 0013_orphan_log.sql — daily orphan-detection log. Write-only
        # investigation data; no Observations are ever deleted.
        columns=dict([
            _col("id", UUID, False),
            _col("tenant_id", UUID, False),
            _col("observation_id", UUID, False),
            _col("detected_at", TS, False, default=True),
            _col("reason", TEXT, False),
        ]),
        indexes={
            "orphan_log_pkey",
            "orphan_log_tenant_idx",
            "orphan_log_obs_idx",
        },
    ),
    # -----------------------------------------------------------------
    # Wave 5-A additions — Access control (0014)
    # -----------------------------------------------------------------
    "actor_roles": Table(
        # 0014_access_control.sql — per-entity role grants with
        # idempotent dedup via UNIQUE NULLS NOT DISTINCT. Spec §26.
        columns=dict([
            _col("tenant_id", UUID, False),
            _col("actor_id", UUID, False),
            _col("entity_type", TEXT, False),
            _col("entity_id", UUID, True),
            _col("role", TEXT, False),
            _col("granted_by", UUID, True),
            _col("granted_at", TS, False, default=True),
            _col("revoked_at", TS, True),
        ]),
        indexes={
            "actor_roles_dedup",
            "actor_roles_actor_idx",
            "actor_roles_entity_idx",
            "actor_roles_role_idx",
        },
    ),
    "shared_channels": Table(
        # 0014_access_control.sql — source_channel → audience_role
        # mapping for Observation Layer-2 visibility.
        columns=dict([
            _col("tenant_id", UUID, False),
            _col("source_channel", TEXT, False),
            _col("audience_role", TEXT, False),
            _col("created_at", TS, False, default=True),
        ]),
        indexes={
            "shared_channels_pkey",
            "shared_channels_tenant_idx",
        },
    ),
    "access_override_log": Table(
        # 0014_access_control.sql — audit trail for admin +
        # first-person overrides. Spec §26 + §11 cross-cut.
        columns=dict([
            _col("id", UUID, False),
            _col("tenant_id", UUID, False),
            _col("actor_id", UUID, False),
            _col("entity_type", TEXT, False),
            _col("entity_id", UUID, True),
            _col("override_kind", TEXT, False),
            _col("reason", TEXT, True),
            _col("occurred_at", TS, False, default=True),
        ]),
        indexes={
            "access_override_log_pkey",
            "access_override_log_tenant_time_idx",
            "access_override_log_actor_idx",
        },
    ),
    # -----------------------------------------------------------------
    # Week-4 Integration additions — CEO view cache + render costs
    # (0017, 0018)
    # -----------------------------------------------------------------
    "view_ceo_cache": Table(
        # 0017_view_ceo_cache.sql — CONTRACTS.md §3. Key/value JSONB
        # store keyed by (tenant_id, cache_key). Agent-GRT writes;
        # /view/ceo/home reads.
        columns=dict([
            _col("tenant_id", UUID, False),
            _col("cache_key", TEXT, False),
            _col("cached_content", JSONB, False),
            _col("cached_at", TS, False, default=True),
            _col("recomputed_reason", TEXT, True),
        ]),
        indexes={
            "view_ceo_cache_pkey",
            "view_ceo_cache_tenant_time",
        },
    ),
    "view_render_costs": Table(
        # 0018_view_render_costs.sql — cost observability for the
        # rendering service. One row per render call; PK is
        # (render_id, computed_at) so retries append rather than clash.
        columns=dict([
            _col("render_id", UUID, False),
            _col("tenant_id", UUID, False),
            _col("render_kind", TEXT, False),
            _col("llm_calls_count", INT, False, default=True),
            _col("llm_input_tokens_total", INT, False, default=True),
            _col("llm_output_tokens_total", INT, False, default=True),
            _col("llm_cost_usd", "numeric", False, default=True),
            _col("latency_total_ms", INT, False, default=True),
            _col("retry_count", INT, False, default=True),
            _col("flagged", BOOL, False, default=True),
            _col("outcome", TEXT, False),
            _col("model_name", TEXT, True),
            _col("computed_at", TS, False, default=True),
        ]),
        indexes={
            "view_render_costs_pkey",
            "render_costs_tenant_time",
            "render_costs_outcome",
            "render_costs_kind",
        },
    ),
}


# ---------------------------------------------------------------------
# Live-DB probing
# ---------------------------------------------------------------------

def fetch_live_extensions(cur) -> set[str]:
    cur.execute("SELECT extname FROM pg_extension")
    return {row[0] for row in cur.fetchall()}


def fetch_live_tables(cur) -> set[str]:
    cur.execute(
        """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
        """
    )
    return {row[0] for row in cur.fetchall()}


def fetch_live_partitioned_parents(cur) -> set[str]:
    cur.execute(
        """
        SELECT c.relname
        FROM pg_class c
        WHERE c.relkind = 'p' AND c.relnamespace = 'public'::regnamespace
        """
    )
    return {row[0] for row in cur.fetchall()}


def fetch_live_columns(cur, table: str) -> dict[str, Column]:
    cur.execute(
        """
        SELECT column_name, data_type, is_nullable, column_default
        FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = %s
        ORDER BY ordinal_position
        """,
        (table,),
    )
    out: dict[str, Column] = {}
    for name, dtype, is_nullable, default in cur.fetchall():
        out[name] = Column(
            name=name,
            data_type=dtype,
            is_nullable=(is_nullable == "YES"),
            has_default=default is not None,
        )
    return out


def fetch_live_indexes(cur, table: str) -> set[str]:
    cur.execute(
        """
        SELECT indexname
        FROM pg_indexes
        WHERE schemaname = 'public' AND tablename = %s
        """,
        (table,),
    )
    return {row[0] for row in cur.fetchall()}


# ---------------------------------------------------------------------
# Comparison
# ---------------------------------------------------------------------

def compare(conn) -> list[str]:
    """Return list of drift messages. Empty list = all green."""
    drifts: list[str] = []
    with conn.cursor() as cur:
        # Extensions
        live_exts = fetch_live_extensions(cur)
        missing_exts = EXPECTED_EXTENSIONS - live_exts
        for ext in sorted(missing_exts):
            drifts.append(f"EXTENSION missing: {ext}")

        # Tables
        live_tables = fetch_live_tables(cur)
        live_partitioned = fetch_live_partitioned_parents(cur)
        # In newer PostgreSQL, partitioned parent tables are relkind='p',
        # and information_schema.tables excludes them; they show up in
        # pg_class instead. We stitch the two views together.
        live_tables_all = live_tables | live_partitioned

        expected_tables = set(EXPECTED_TABLES.keys())
        missing_tables = expected_tables - live_tables_all
        for t in sorted(missing_tables):
            drifts.append(f"TABLE missing: {t}")

        # Extra tables are warnings, not errors (later migrations may
        # add tables legitimately — the three flagged in BUILD-PLAN:
        # memory_fabric, calibration_offsets, actor_sessions, etc.).
        # We only report extras that look like they might be Wave 0
        # typos (prefix match on an expected name).
        # For now we don't fail on extras — only log them at verbose.

        for table_name in sorted(expected_tables & live_tables_all):
            expected = EXPECTED_TABLES[table_name]

            # Partitioning status
            live_is_part = table_name in live_partitioned
            if live_is_part != expected.is_partitioned:
                drifts.append(
                    f"TABLE {table_name}: partitioning mismatch "
                    f"(expected is_partitioned={expected.is_partitioned}, live={live_is_part})"
                )

            # Columns
            live_cols = fetch_live_columns(cur, table_name)
            expected_cols = expected.columns

            missing_cols = set(expected_cols) - set(live_cols)
            for c in sorted(missing_cols):
                drifts.append(f"COLUMN missing: {table_name}.{c}")
            extra_cols = set(live_cols) - set(expected_cols)
            for c in sorted(extra_cols):
                drifts.append(f"COLUMN unexpected: {table_name}.{c}")

            for col in sorted(set(expected_cols) & set(live_cols)):
                e = expected_cols[col]
                l = live_cols[col]
                if e.data_type != l.data_type:
                    drifts.append(
                        f"COLUMN type drift: {table_name}.{col} "
                        f"(expected {e.data_type}, live {l.data_type})"
                    )
                if e.is_nullable != l.is_nullable:
                    drifts.append(
                        f"COLUMN nullability drift: {table_name}.{col} "
                        f"(expected nullable={e.is_nullable}, live nullable={l.is_nullable})"
                    )
                if e.has_default != l.has_default:
                    drifts.append(
                        f"COLUMN default drift: {table_name}.{col} "
                        f"(expected has_default={e.has_default}, live has_default={l.has_default})"
                    )

            # Indexes — every expected index must be present; extras are OK
            # (e.g. partition children inherit and add their own names).
            live_idx = fetch_live_indexes(cur, table_name)
            missing_idx = expected.indexes - live_idx
            for i in sorted(missing_idx):
                drifts.append(f"INDEX missing: {table_name}.{i}")

    return drifts


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dsn",
        default=os.environ.get("DATABASE_URL"),
        help="Postgres DSN (default: $DATABASE_URL)",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Print green-line details even when there is no drift",
    )
    args = parser.parse_args()

    if not args.dsn:
        print(
            "ERROR: no DSN provided. Set $DATABASE_URL or pass --dsn.",
            file=sys.stderr,
        )
        return 2

    try:
        conn = psycopg2.connect(args.dsn)
    except psycopg2.OperationalError as e:
        print(f"ERROR: could not connect to database: {e}", file=sys.stderr)
        return 2

    try:
        drifts = compare(conn)
    finally:
        conn.close()

    if drifts:
        print(f"SCHEMA DRIFT DETECTED — {len(drifts)} finding(s):")
        for d in drifts:
            print(f"  - {d}")
        return 1

    print("Schema lock OK: every expected table, column, and index is present.")
    if args.verbose:
        print(f"  Tables verified : {len(EXPECTED_TABLES)}")
        print(f"  Extensions      : {sorted(EXPECTED_EXTENSIONS)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
