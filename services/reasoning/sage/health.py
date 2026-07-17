"""Operational health report for the SAGE learning loop."""
from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID

import asyncpg


def _iso(value: Any) -> str | None:
    if isinstance(value, datetime):
        return value.isoformat()
    return None


async def _table_exists(conn: asyncpg.Connection, table_name: str) -> bool:
    exists = await conn.fetchval("SELECT to_regclass($1)", f"public.{table_name}")
    return exists is not None


def _age_seconds(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _record_dict(row: asyncpg.Record | None) -> dict[str, Any]:
    return dict(row) if row is not None else {}


async def build_sage_health_report(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    structural_freshness_hours: int = 6,
    optimizer_lag_minutes: int = 30,
) -> dict[str, Any]:
    """Return a read-only health report for SAGE utility-layer freshness."""

    findings: list[dict[str, Any]] = []
    active_models = await _count_active_models(conn, tenant_id)
    checks: dict[str, Any] = {"active_models": active_models}
    checks.update(
        await _collect_structural_feature_checks(
            conn,
            tenant_id=tenant_id,
            active_models=active_models,
            structural_freshness_hours=structural_freshness_hours,
            findings=findings,
        )
    )
    checks["topology_optimizer"] = await _collect_topology_optimizer_check(
        conn,
        tenant_id=tenant_id,
        optimizer_lag_minutes=optimizer_lag_minutes,
        findings=findings,
    )
    relationship_candidates = await _collect_relationship_candidate_check(
        conn, tenant_id=tenant_id
    )
    checks["relationship_candidates"] = relationship_candidates
    checks["relationship_ontology_proposals"] = (
        await _collect_relationship_ontology_proposal_check(
            conn,
            tenant_id=tenant_id,
            relationship_candidates=relationship_candidates,
            findings=findings,
        )
    )

    return {
        "status": "degraded" if findings else "ok",
        "tenant_id": str(tenant_id),
        "checks": checks,
        "findings": findings,
    }


async def _count_active_models(conn: asyncpg.Connection, tenant_id: UUID) -> int:
    value = await conn.fetchval(
        """
        SELECT count(*)
        FROM accepted_current_models
        WHERE tenant_id = $1
          AND status = 'active'
        """,
        tenant_id,
    )
    return int(value or 0)


async def _collect_structural_feature_checks(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    active_models: int,
    structural_freshness_hours: int,
    findings: list[dict[str, Any]],
) -> dict[str, Any]:
    structural_exists = await _table_exists(conn, "model_structural_features")
    edge_structural_exists = await _table_exists(
        conn, "model_edge_structural_features"
    )
    structural_check: dict[str, Any] = {
        "table_present": structural_exists,
        "edge_table_present": edge_structural_exists,
    }
    if structural_exists:
        structural_check.update(
            await _load_structural_feature_stats(
                conn,
                tenant_id=tenant_id,
                active_models=active_models,
                structural_freshness_hours=structural_freshness_hours,
            )
        )
        stale_rows = int(structural_check["stale_or_missing_active_models"])
        if active_models > 0 and stale_rows > 0:
            findings.append(
                {
                    "severity": "degraded",
                    "code": "structural_features_stale_or_missing",
                    "message": (
                        "Some active Models do not have fresh structural features."
                    ),
                    "count": stale_rows,
                }
            )
    elif active_models > 0:
        findings.append(
            {
                "severity": "degraded",
                "code": "structural_features_table_missing",
                "message": "model_structural_features is missing.",
            }
        )

    checks: dict[str, Any] = {"structural_features": structural_check}
    if edge_structural_exists:
        checks["edge_structural_features"] = await _load_edge_structural_stats(
            conn, tenant_id=tenant_id
        )
    return checks


async def _load_structural_feature_stats(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    active_models: int,
    structural_freshness_hours: int,
) -> dict[str, Any]:
    row = await conn.fetchrow(
        """
        SELECT count(*) AS feature_rows,
               max(updated_at) AS newest_updated_at,
               extract(epoch from (now() - max(updated_at))) AS newest_age_seconds
        FROM model_structural_features
        WHERE tenant_id = $1
        """,
        tenant_id,
    )
    stale_rows = await conn.fetchval(
        """
        SELECT count(*)
        FROM accepted_current_models m
        LEFT JOIN model_structural_features f
          ON f.model_id = m.id
         AND f.tenant_id = m.tenant_id
        WHERE m.tenant_id = $1
          AND m.status = 'active'
          AND (
            f.model_id IS NULL
            OR f.updated_at < now() - (($2::text)::interval)
          )
        """,
        tenant_id,
        f"{structural_freshness_hours} hours",
    )
    row_data = _record_dict(row)
    feature_rows = int(row_data.get("feature_rows") or 0)
    coverage = (
        float(feature_rows) / float(active_models) if active_models > 0 else 1.0
    )
    newest_age = _age_seconds(row_data.get("newest_age_seconds"))
    return {
        "feature_rows": feature_rows,
        "active_model_coverage": coverage,
        "newest_updated_at": _iso(row_data.get("newest_updated_at")),
        "newest_age_seconds": newest_age,
        "stale_or_missing_active_models": int(stale_rows or 0),
        "freshness_slo_hours": structural_freshness_hours,
    }


async def _load_edge_structural_stats(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
) -> dict[str, Any]:
    edge_row = await conn.fetchrow(
        """
        SELECT count(*) AS feature_rows,
               max(updated_at) AS newest_updated_at,
               extract(epoch from (now() - max(updated_at))) AS newest_age_seconds
        FROM model_edge_structural_features
        WHERE tenant_id = $1
        """,
        tenant_id,
    )
    edge_data = _record_dict(edge_row)
    return {
        "feature_rows": int(edge_data.get("feature_rows") or 0),
        "newest_updated_at": _iso(edge_data.get("newest_updated_at")),
        "newest_age_seconds": _age_seconds(edge_data.get("newest_age_seconds")),
    }


async def _collect_topology_optimizer_check(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    optimizer_lag_minutes: int,
    findings: list[dict[str, Any]],
) -> dict[str, Any]:
    outcome_events_exists = await _table_exists(conn, "inquiry_outcome_events")
    optimizer_runs_exists = await _table_exists(
        conn, "sage_topology_optimizer_runs"
    )
    check: dict[str, Any] = {
        "outcome_events_table_present": outcome_events_exists,
        "checkpoint_table_present": optimizer_runs_exists,
        "lag_slo_minutes": optimizer_lag_minutes,
    }
    if outcome_events_exists and optimizer_runs_exists:
        check.update(await _load_topology_optimizer_stats(conn, tenant_id))
        pending = int(check["pending_sessions"])
        oldest_running_age = _age_seconds(check["oldest_running_age_seconds"])
        last_completed_age = _age_seconds(check["last_completed_age_seconds"])
        lag_s = optimizer_lag_minutes * 60
        if pending > 0 and (
            last_completed_age is None or last_completed_age > lag_s
        ):
            findings.append(
                {
                    "severity": "degraded",
                    "code": "topology_optimizer_lagging",
                    "message": (
                        "Outcome-event sessions are waiting for topology optimization."
                    ),
                    "pending_sessions": pending,
                }
            )
        if oldest_running_age is not None and oldest_running_age > lag_s:
            findings.append(
                {
                    "severity": "degraded",
                    "code": "topology_optimizer_run_stuck",
                    "message": "A topology optimizer run has been running too long.",
                    "oldest_running_age_seconds": oldest_running_age,
                }
            )
    elif outcome_events_exists and not optimizer_runs_exists:
        findings.append(
            {
                "severity": "degraded",
                "code": "topology_optimizer_checkpoint_missing",
                "message": "sage_topology_optimizer_runs is missing.",
            }
        )

    return check


async def _load_topology_optimizer_stats(
    conn: asyncpg.Connection,
    tenant_id: UUID,
) -> dict[str, Any]:
    pending_sessions = await conn.fetchval(
        """
        SELECT count(DISTINCT e.inquiry_session_id)
        FROM inquiry_outcome_events e
        JOIN inquiry_sessions s
          ON s.id = e.inquiry_session_id
         AND s.tenant_id = e.tenant_id
        LEFT JOIN sage_topology_optimizer_runs r
          ON r.tenant_id = e.tenant_id
         AND r.inquiry_session_id = e.inquiry_session_id
        WHERE e.tenant_id = $1
          AND s.status IN ('completed', 'deferred', 'failed')
          AND r.inquiry_session_id IS NULL
        """,
        tenant_id,
    )
    run_row = await conn.fetchrow(
        """
        SELECT
          count(*) FILTER (WHERE status = 'running') AS running,
          count(*) FILTER (WHERE status = 'completed') AS completed,
          count(*) FILTER (WHERE status = 'failed') AS failed,
          max(completed_at) FILTER (WHERE status = 'completed') AS last_completed_at,
          extract(epoch from (
            now() - max(completed_at) FILTER (WHERE status = 'completed')
          )) AS last_completed_age_seconds,
          extract(epoch from (
            now() - min(started_at) FILTER (WHERE status = 'running')
          )) AS oldest_running_age_seconds
        FROM sage_topology_optimizer_runs
        WHERE tenant_id = $1
        """,
        tenant_id,
    )
    run_data = _record_dict(run_row)
    return {
        "pending_sessions": int(pending_sessions or 0),
        "running": int(run_data.get("running") or 0),
        "completed": int(run_data.get("completed") or 0),
        "failed": int(run_data.get("failed") or 0),
        "last_completed_at": _iso(run_data.get("last_completed_at")),
        "last_completed_age_seconds": _age_seconds(
            run_data.get("last_completed_age_seconds")
        ),
        "oldest_running_age_seconds": _age_seconds(
            run_data.get("oldest_running_age_seconds")
        ),
    }


async def _collect_relationship_candidate_check(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
) -> dict[str, Any]:
    relationship_candidates_exists = await _table_exists(
        conn, "relationship_candidates"
    )
    if relationship_candidates_exists:
        candidate_row = await conn.fetchrow(
            """
            SELECT
              count(*) FILTER (
                WHERE candidate_kind = 'edge_type'
                  AND review_status IN ('candidate', 'needs_review')
              ) AS open_edge_type_candidates,
              extract(epoch from (
                now() - min(created_at) FILTER (
                  WHERE candidate_kind = 'edge_type'
                    AND review_status IN ('candidate', 'needs_review')
                )
              )) AS oldest_open_edge_type_age_seconds
            FROM relationship_candidates
            WHERE tenant_id = $1
            """,
            tenant_id,
        )
        candidate_data = _record_dict(candidate_row)
        return {
            "table_present": True,
            "open_edge_type_candidates": int(
                candidate_data.get("open_edge_type_candidates") or 0
            ),
            "oldest_open_edge_type_age_seconds": _age_seconds(
                candidate_data.get("oldest_open_edge_type_age_seconds")
            ),
        }
    return {"table_present": False}


async def _collect_relationship_ontology_proposal_check(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    relationship_candidates: dict[str, Any],
    findings: list[dict[str, Any]],
) -> dict[str, Any]:
    ontology_proposals_exists = await _table_exists(
        conn, "relationship_ontology_proposals"
    )
    if ontology_proposals_exists:
        check = await _load_relationship_ontology_proposal_stats(
            conn, tenant_id=tenant_id
        )
        review_ready = int(check["review_ready"])
        if review_ready > 0:
            findings.append(
                {
                    "severity": "attention",
                    "code": "ontology_proposals_ready_for_review",
                    "message": "Relationship ontology proposals are ready for review.",
                    "count": review_ready,
                }
            )
        return check

    open_edge_type_candidates = relationship_candidates.get(
        "open_edge_type_candidates", 0
    )
    if open_edge_type_candidates:
        findings.append(
            {
                "severity": "degraded",
                "code": "ontology_proposal_table_missing",
                "message": (
                    "Edge-type candidates exist but the proposal table is missing."
                ),
                "open_edge_type_candidates": open_edge_type_candidates,
            }
        )
    return {"table_present": False}


async def _load_relationship_ontology_proposal_stats(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
) -> dict[str, Any]:
    proposal_rows = await conn.fetch(
        """
        SELECT status, count(*) AS n
        FROM relationship_ontology_proposals
        WHERE tenant_id = $1
        GROUP BY status
        """,
        tenant_id,
    )
    oldest_review_ready = await conn.fetchval(
        """
        SELECT extract(epoch from (now() - min(updated_at)))
        FROM relationship_ontology_proposals
        WHERE tenant_id = $1
          AND status = 'review_ready'
        """,
        tenant_id,
    )
    by_status = {str(row["status"]): int(row["n"]) for row in proposal_rows}
    return {
        "table_present": True,
        "by_status": by_status,
        "review_ready": by_status.get("review_ready", 0),
        "oldest_review_ready_age_seconds": _age_seconds(oldest_review_ready),
    }


__all__ = ["build_sage_health_report"]
