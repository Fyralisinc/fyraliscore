"""Database loader for compact SAGE company learning profiles."""
from __future__ import annotations

from typing import TYPE_CHECKING, Any, Iterable
from uuid import UUID

import asyncpg

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
    if not await _table_exists(conn, "sage_reader_decision_attributions"):
        return []
    rows = await conn.fetch(
        """
        SELECT source_key,
               COUNT(*) AS attempts,
               COUNT(*) FILTER (WHERE writer_used) AS successes,
               COALESCE(SUM(writer_credit_score), 0.0) AS total_credit,
               COALESCE(AVG(activation_score), 0.0) AS avg_activation,
               'sage_reader_decision_attributions' AS provenance_source
        FROM sage_reader_decision_attributions,
             LATERAL jsonb_object_keys(source_breakdown) AS source_keys(source_key)
        WHERE tenant_id = $1
          AND source_breakdown <> '{}'::jsonb
        GROUP BY source_key
        HAVING COUNT(*) >= 2
        ORDER BY successes DESC, attempts DESC, source_key ASC
        LIMIT $2
        """,
        tenant_id,
        limit,
    )
    return [dict(row) for row in rows]


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
