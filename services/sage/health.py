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
    checks: dict[str, Any] = {}

    active_models = await conn.fetchval(
        """
        SELECT count(*)
        FROM models
        WHERE tenant_id = $1
          AND status = 'active'
        """,
        tenant_id,
    )
    active_models = int(active_models or 0)
    checks["active_models"] = active_models

    structural_exists = await _table_exists(conn, "model_structural_features")
    edge_structural_exists = await _table_exists(
        conn, "model_edge_structural_features"
    )
    checks["structural_features"] = {
        "table_present": structural_exists,
        "edge_table_present": edge_structural_exists,
    }
    if structural_exists:
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
            FROM models m
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
            float(feature_rows) / float(active_models)
            if active_models > 0
            else 1.0
        )
        newest_age = _age_seconds(row_data.get("newest_age_seconds"))
        checks["structural_features"].update(
            {
                "feature_rows": feature_rows,
                "active_model_coverage": coverage,
                "newest_updated_at": _iso(row_data.get("newest_updated_at")),
                "newest_age_seconds": newest_age,
                "stale_or_missing_active_models": int(stale_rows or 0),
                "freshness_slo_hours": structural_freshness_hours,
            }
        )
        if active_models > 0 and int(stale_rows or 0) > 0:
            findings.append(
                {
                    "severity": "degraded",
                    "code": "structural_features_stale_or_missing",
                    "message": (
                        "Some active Models do not have fresh structural features."
                    ),
                    "count": int(stale_rows or 0),
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

    if edge_structural_exists:
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
        checks["edge_structural_features"] = {
            "feature_rows": int(edge_data.get("feature_rows") or 0),
            "newest_updated_at": _iso(edge_data.get("newest_updated_at")),
            "newest_age_seconds": _age_seconds(
                edge_data.get("newest_age_seconds")
            ),
        }

    outcome_events_exists = await _table_exists(conn, "inquiry_outcome_events")
    optimizer_runs_exists = await _table_exists(
        conn, "sage_topology_optimizer_runs"
    )
    checks["topology_optimizer"] = {
        "outcome_events_table_present": outcome_events_exists,
        "checkpoint_table_present": optimizer_runs_exists,
        "lag_slo_minutes": optimizer_lag_minutes,
    }
    if outcome_events_exists and optimizer_runs_exists:
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
        pending = int(pending_sessions or 0)
        run_data = _record_dict(run_row)
        oldest_running_age = _age_seconds(
            run_data.get("oldest_running_age_seconds")
        )
        last_completed_age = _age_seconds(
            run_data.get("last_completed_age_seconds")
        )
        checks["topology_optimizer"].update(
            {
                "pending_sessions": pending,
                "running": int(run_data.get("running") or 0),
                "completed": int(run_data.get("completed") or 0),
                "failed": int(run_data.get("failed") or 0),
                "last_completed_at": _iso(
                    run_data.get("last_completed_at")
                ),
                "last_completed_age_seconds": last_completed_age,
                "oldest_running_age_seconds": oldest_running_age,
            }
        )
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

    relationship_candidates_exists = await _table_exists(
        conn, "relationship_candidates"
    )
    ontology_proposals_exists = await _table_exists(
        conn, "relationship_ontology_proposals"
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
        checks["relationship_candidates"] = {
            "table_present": True,
            "open_edge_type_candidates": int(
                candidate_data.get("open_edge_type_candidates") or 0
            ),
            "oldest_open_edge_type_age_seconds": _age_seconds(
                candidate_data.get("oldest_open_edge_type_age_seconds")
            ),
        }
    else:
        checks["relationship_candidates"] = {"table_present": False}

    if ontology_proposals_exists:
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
        checks["relationship_ontology_proposals"] = {
            "table_present": True,
            "by_status": by_status,
            "review_ready": by_status.get("review_ready", 0),
            "oldest_review_ready_age_seconds": _age_seconds(oldest_review_ready),
        }
        if by_status.get("review_ready", 0) > 0:
            findings.append(
                {
                    "severity": "attention",
                    "code": "ontology_proposals_ready_for_review",
                    "message": "Relationship ontology proposals are ready for review.",
                    "count": by_status.get("review_ready", 0),
                }
            )
    else:
        checks["relationship_ontology_proposals"] = {"table_present": False}
        open_edge_type_candidates = (
            checks.get("relationship_candidates", {})
            .get("open_edge_type_candidates", 0)
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

    return {
        "status": "degraded" if findings else "ok",
        "tenant_id": str(tenant_id),
        "checks": checks,
        "findings": findings,
    }


__all__ = ["build_sage_health_report"]
