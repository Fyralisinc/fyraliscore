"""Database loader for compact SAGE company learning profiles."""
from __future__ import annotations

from typing import TYPE_CHECKING, Any, Iterable
from uuid import UUID

import asyncpg
from services.domain.models.read_shapes import ACCEPTED_MODEL_ROWS_SQL

from services.reasoning.sage.company_profile.builder import (
    build_company_learning_profile,
)
from services.reasoning.sage.company_profile.types import CompanyLearningProfile

if TYPE_CHECKING:
    from services.reasoning.sage.patterns.types import (
        PatternScoutCandidate,
        StructuralSignature,
    )


async def load_company_learning_profile(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    route_utilities: Iterable[Any] = (),
    question_policy_stats: Iterable[Any] = (),
    structural_signatures: Iterable[StructuralSignature] = (),
    latent_pattern_candidates: Iterable[PatternScoutCandidate] = (),
    limit_per_surface: int = 24,
) -> CompanyLearningProfile:
    """Load a bounded tenant profile from existing SAGE utility surfaces.

    Every table is optional and guarded by `to_regclass` so staged rollouts can
    deploy code before all migrations are present. The returned profile remains
    policy memory only; it contains no canonical facts.
    """

    limit = max(1, int(limit_per_surface))
    negative_memories = await _load_negative_memories(conn, tenant_id, limit=limit)
    shortcuts = await _load_shortcuts(conn, tenant_id, limit=limit)
    affordance_profiles = await _load_affordance_profiles(conn, tenant_id, limit=limit)
    structural_feature_rows = await _load_structural_feature_rows(
        conn,
        tenant_id,
        limit=limit,
    )
    residuals = await _load_open_residuals(conn, tenant_id, limit=limit)
    recent_drift_signals = await _load_recent_drift_signals(
        conn,
        tenant_id,
        limit=limit,
    )
    source_reliability_stats = await _load_source_reliability_stats(
        conn,
        tenant_id,
        limit=limit,
    )
    actor_reliability_stats = await _load_actor_reliability_stats(
        conn,
        tenant_id,
        limit=limit,
    )
    return build_company_learning_profile(
        tenant_id=tenant_id,
        route_utilities=route_utilities,
        question_policy_stats=question_policy_stats,
        negative_memories=negative_memories,
        shortcuts=shortcuts,
        affordance_profiles=affordance_profiles,
        structural_feature_rows=structural_feature_rows,
        structural_signatures=structural_signatures,
        latent_pattern_candidates=latent_pattern_candidates,
        residuals=residuals,
        recent_drift_signals=recent_drift_signals,
        source_reliability_stats=source_reliability_stats,
        actor_reliability_stats=actor_reliability_stats,
    )


async def _load_negative_memories(
    conn: asyncpg.Connection,
    tenant_id: UUID,
    *,
    limit: int,
) -> list[dict[str, Any]]:
    if not await _table_exists(conn, "negative_memory"):
        return []
    rows = await conn.fetch(
        """
        SELECT memory_type,
               COALESCE(NULLIF(reason, ''), memory_type) AS reason,
               signature,
               rejected_path,
               COALESCE(
                 rejected_path->>'path',
                 rejected_path->>'route',
                 signature->>'path',
                 signature->>'route'
               ) AS path,
               COALESCE(
                 signature->>'question_primitive',
                 signature->>'primitive'
               ) AS question_primitive,
               signature->>'signal_type' AS signal_type,
               confidence,
               1 AS count
        FROM negative_memory
        WHERE tenant_id = $1
          AND expires_at > now()
        ORDER BY created_at DESC
        LIMIT $2
        """,
        tenant_id,
        limit,
    )
    return [dict(row) for row in rows]


async def _load_source_reliability_stats(
    conn: asyncpg.Connection,
    tenant_id: UUID,
    *,
    limit: int,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if await _table_exists(conn, "sage_reader_decision_attributions"):
        reader_rows = await conn.fetch(
            """
            SELECT source_key,
                   COUNT(*) AS attempts,
                   COUNT(*) FILTER (WHERE writer_used) AS successes,
                   COALESCE(SUM(writer_credit_score), 0.0) AS total_credit,
                   COALESCE(AVG(activation_score), 0.0) AS avg_activation,
                   'sage_reader_decision_attributions' AS provenance_source
            FROM sage_reader_decision_attributions,
                 LATERAL jsonb_object_keys(source_breakdown)
                   AS source_keys(source_key)
            WHERE tenant_id = $1
              AND source_breakdown <> '{}'::jsonb
            GROUP BY source_key
            ORDER BY successes DESC, attempts DESC, source_key ASC
            LIMIT $2
            """,
            tenant_id,
            max(limit * 2, limit),
        )
        rows.extend(dict(row) for row in reader_rows)

    grounding_tables = (
        "grounding_traces",
        "interpretation_context_snapshots",
        "resolution_assessments",
        "source_semantic_interpretations",
        "source_semantic_admission_decisions",
        "models",
    )
    grounding_tables_exist = True
    for table in grounding_tables:
        if not await _table_exists(conn, table):
            grounding_tables_exist = False
            break
    if grounding_tables_exist:
        grounding_rows = await conn.fetch(
            f"""
            WITH recent_traces AS (
              SELECT trace.*
              FROM grounding_traces trace
              WHERE trace.tenant_id = $1
              ORDER BY trace.created_at DESC, trace.id DESC
              LIMIT $3
            ),
            corrected_predecessors AS (
              SELECT DISTINCT predecessor.id AS grounding_trace_id
              FROM recent_traces predecessor
              JOIN grounding_traces successor
                ON successor.tenant_id = predecessor.tenant_id
               AND successor.trace
                   ->> 'supersedes_grounding_trace_id' = predecessor.id::text
            ),
            measured AS (
              SELECT
                snapshot.source_channel AS source_key,
                1 AS learning_attempt,
                CASE
                  WHEN (
                    model.archive_reason = 'superseded'
                    OR correction.grounding_trace_id IS NOT NULL
                  ) THEN 0
                  WHEN trace.trace ? 'supersedes_grounding_trace_id' THEN 0
                  WHEN admission.disposition = 'belief_applied' THEN 1
                  ELSE 0
                END AS useful_terminal,
                CASE
                  WHEN (
                    model.archive_reason = 'superseded'
                    OR correction.grounding_trace_id IS NOT NULL
                  ) THEN -1.0
                  WHEN trace.trace ? 'supersedes_grounding_trace_id' THEN 0.05
                  WHEN admission.disposition = 'belief_applied' THEN 0.70
                  WHEN admission.disposition = 'no_admission' THEN 0.08
                  WHEN trace.current_fate IN (
                    'review', 'unresolved', 'abstained'
                  ) THEN 0.03
                  ELSE 0.0
                END AS operational_credit,
                0.0::double precision AS activation
              FROM recent_traces trace
              JOIN interpretation_context_snapshots snapshot
                ON snapshot.tenant_id = trace.tenant_id
               AND snapshot.id = trace.context_snapshot_id
              JOIN resolution_assessments assessment
                ON assessment.tenant_id = trace.tenant_id
               AND assessment.id = trace.resolution_assessment_id
              LEFT JOIN source_semantic_interpretations interpretation
                ON interpretation.tenant_id = trace.tenant_id
               AND interpretation.grounding_trace_id = trace.id
              LEFT JOIN source_semantic_admission_decisions admission
                ON admission.tenant_id = interpretation.tenant_id
               AND admission.interpretation_id = interpretation.id
              LEFT JOIN {ACCEPTED_MODEL_ROWS_SQL} model
                ON model.tenant_id = admission.tenant_id
               AND model.id = admission.admitted_model_id
              LEFT JOIN corrected_predecessors correction
                ON correction.grounding_trace_id = trace.id
              WHERE snapshot.source_channel IS NOT NULL
                AND snapshot.source_channel <> ''
                AND (
                  admission.disposition IS NOT NULL
                  OR trace.current_fate IN (
                    'review', 'unresolved', 'abstained'
                  )
                )
            )
            SELECT
              source_key,
              COALESCE(SUM(learning_attempt), 0) AS attempts,
              COALESCE(SUM(useful_terminal), 0) AS successes,
              COALESCE(SUM(operational_credit), 0.0) AS total_credit,
              COALESCE(AVG(activation), 0.0) AS avg_activation,
              'grounding_context_source_semantic_outcomes'
                AS provenance_source
            FROM measured
            GROUP BY source_key
            ORDER BY successes DESC, attempts DESC, source_key ASC
            LIMIT $2
            """,
            tenant_id,
            max(limit * 2, limit),
            max(limit * 32, 128),
        )
        rows.extend(dict(row) for row in grounding_rows)

    return _merge_source_reliability_stats(rows, limit=limit)


def _merge_source_reliability_stats(
    rows: Iterable[dict[str, Any]],
    *,
    limit: int,
) -> list[dict[str, Any]]:
    """Merge reader credit with grounded source operational yield.

    Grounding admission and candidate scores are not factual correctness
    labels. The aggregate only records whether a source repeatedly reached a
    useful terminal processing fate; later correction lineage can negate that
    operational credit before it affects retrieval salience. Abstention,
    review, and no-admission fates remain neutral rather than failures.
    """

    grouped: dict[str, dict[str, Any]] = {}
    for row in rows:
        source_key = str(row.get("source_key") or "").strip()
        if not source_key:
            continue
        attempts = max(0, int(row.get("attempts") or 0))
        if attempts <= 0:
            continue
        bucket = grouped.setdefault(
            source_key,
            {
                "source_key": source_key,
                "attempts": 0,
                "successes": 0,
                "total_credit": 0.0,
                "activation_total": 0.0,
                "provenance_sources": set(),
            },
        )
        bucket["attempts"] += attempts
        bucket["successes"] += max(0, int(row.get("successes") or 0))
        bucket["total_credit"] += float(row.get("total_credit") or 0.0)
        bucket["activation_total"] += (
            float(row.get("avg_activation") or 0.0) * attempts
        )
        provenance = str(row.get("provenance_source") or "").strip()
        if provenance:
            bucket["provenance_sources"].add(provenance)

    merged: list[dict[str, Any]] = []
    for bucket in grouped.values():
        attempts = int(bucket["attempts"])
        if attempts < 2:
            continue
        merged.append(
            {
                "source_key": bucket["source_key"],
                "attempts": attempts,
                "successes": int(bucket["successes"]),
                "total_credit": round(float(bucket["total_credit"]), 6),
                "avg_activation": round(
                    float(bucket["activation_total"]) / attempts,
                    6,
                ),
                "provenance_source": "+".join(
                    sorted(bucket["provenance_sources"])
                ),
            }
        )
    merged.sort(
        key=lambda row: (
            -int(row["attempts"]),
            -float(row["total_credit"]),
            str(row["source_key"]),
        )
    )
    return merged[: max(1, int(limit))]


async def _load_actor_reliability_stats(
    conn: asyncpg.Connection,
    tenant_id: UUID,
    *,
    limit: int,
) -> list[dict[str, Any]]:
    if not await _table_exists(conn, "calibration_stats"):
        return []
    rows = await conn.fetch(
        """
        SELECT actor_id::text AS actor_key,
               proposition_kind,
               COUNT(*) AS attempts,
               COUNT(*) FILTER (WHERE outcome IS TRUE) AS successes,
               COALESCE(AVG(asserted_confidence), 0.0) AS avg_asserted_confidence,
               'calibration_stats' AS provenance_source
        FROM calibration_stats
        WHERE tenant_id = $1
          AND outcome IS NOT NULL
        GROUP BY actor_id, proposition_kind
        HAVING COUNT(*) >= 2
        ORDER BY attempts DESC, successes DESC, actor_key ASC
        LIMIT $2
        """,
        tenant_id,
        limit,
    )
    return [dict(row) for row in rows]


async def _load_shortcuts(
    conn: asyncpg.Connection,
    tenant_id: UUID,
    *,
    limit: int,
) -> list[dict[str, Any]]:
    if not await _table_exists(conn, "discovery_shortcuts"):
        return []
    rows = await conn.fetch(
        """
	        SELECT COALESCE(
	                 to_model_id::text,
	                 to_region_id::text,
	                 to_affordance::text,
	                 md5(from_signature::text)
	               ) AS shortcut_key,
               utility_score,
               GREATEST(success_count + failure_count, 1) AS support_count
        FROM discovery_shortcuts
        WHERE tenant_id = $1
          AND (expires_at IS NULL OR expires_at > now())
        ORDER BY utility_score DESC, updated_at DESC
        LIMIT $2
        """,
        tenant_id,
        limit,
    )
    return [dict(row) for row in rows]


async def _load_affordance_profiles(
    conn: asyncpg.Connection,
    tenant_id: UUID,
    *,
    limit: int,
) -> list[dict[str, Any]]:
    if not await _table_exists(conn, "retrieval_affordance_profiles"):
        return []
    rows = await conn.fetch(
        """
        SELECT model_id, utility_score, 1 AS attempts
        FROM retrieval_affordance_profiles
        WHERE tenant_id = $1
        ORDER BY utility_score DESC, last_updated_at DESC
        LIMIT $2
        """,
        tenant_id,
        limit,
    )
    return [dict(row) for row in rows]


async def _load_structural_feature_rows(
    conn: asyncpg.Connection,
    tenant_id: UUID,
    *,
    limit: int,
) -> list[dict[str, Any]]:
    if not await _table_exists(conn, "model_structural_features"):
        return []
    rows = await conn.fetch(
        """
        SELECT model_id, degree_total, bridge_score, hub_score
        FROM model_structural_features
        WHERE tenant_id = $1
          AND (
            COALESCE(bridge_score, 0) > 0
            OR COALESCE(hub_score, 0) > 0
          )
        ORDER BY GREATEST(
            COALESCE(bridge_score, 0),
            COALESCE(hub_score, 0)
        ) DESC, updated_at DESC
        LIMIT $2
        """,
        tenant_id,
        limit,
    )
    return [dict(row) for row in rows]


async def _load_open_residuals(
    conn: asyncpg.Connection,
    tenant_id: UUID,
    *,
    limit: int,
) -> list[dict[str, Any]]:
    if not await _table_exists(conn, "model_residual_evidence"):
        return []
    rows = await conn.fetch(
        """
        SELECT id, residual_kind, status, model_id, source_observation_id, created_at
        FROM model_residual_evidence
        WHERE tenant_id = $1
          AND status = 'open'
        ORDER BY created_at DESC
        LIMIT $2
        """,
        tenant_id,
        limit,
    )
    return [dict(row) for row in rows]


async def _load_recent_drift_signals(
    conn: asyncpg.Connection,
    tenant_id: UUID,
    *,
    limit: int,
) -> list[dict[str, Any]]:
    if not await _table_exists(conn, "model_residual_evidence"):
        return []
    rows = await conn.fetch(
        """
        SELECT id,
               residual_kind AS drift_kind,
               model_id,
               source_observation_id,
               created_at,
               'model_residual_evidence' AS provenance_source
        FROM model_residual_evidence
        WHERE tenant_id = $1
          AND status = 'open'
          AND created_at >= now() - interval '30 days'
        ORDER BY created_at DESC
        LIMIT $2
        """,
        tenant_id,
        limit,
    )
    return [dict(row) for row in rows]


async def _table_exists(conn: asyncpg.Connection, table_name: str) -> bool:
    return bool(await conn.fetchval("SELECT to_regclass($1)", f"public.{table_name}"))


__all__ = ["load_company_learning_profile"]
