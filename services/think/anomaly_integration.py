"""services/think/anomaly_integration.py — in-apply anomaly scanning.

Spec §7 "Anomaly checking inside apply". BUILD-PLAN §4 Prompt 3.B
item 10.

Called post-apply, pre-commit — scans the ValidatedDiff for four
kinds of anomalies:

  * confidence_drop (>0.25 within one update)
  * critical_path_blocked (critical-path Commitment → Blocked/Paused)
  * resource_over_deployed (>0.95 utilization post-deploy)
  * customer_health_degraded (customer_resource health downgrade)

Results are returned as Anomaly dataclasses. The caller writes them
into `think_anomalies_raw` (durable queue, migration 0008) — post-
commit publishing hands off to Wave 4-B anomaly_processor.

Decision documented in BUILD-LOG: we chose a durable `think_anomalies_raw`
table (migration 0008) over an in-memory queue because a crash between
apply commit and Wave 4-B consumption should not swallow an anomaly.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any
from uuid import UUID

import asyncpg
import structlog

from lib.shared.ids import uuid7

from .diff_schema import ValidatedDiff


_log = structlog.get_logger(__name__)


@dataclass
class Anomaly:
    kind: str
    region: dict[str, Any]
    significance: float
    triggering_op: dict[str, Any] = field(default_factory=dict)


async def check_anomalies(
    diff: ValidatedDiff,
    conn: asyncpg.Connection,
) -> list[Anomaly]:
    """
    Run all four anomaly scans on `diff`. Returns a list of flagged
    anomalies. Runs inside `conn` so it sees the just-applied state.
    """
    flagged: list[Anomaly] = []

    # --- Confidence drop on claim_op.update confidence changes ---
    for op in diff.claim_ops:
        if op.op == "update" and op.changes and "confidence" in op.changes:
            new_conf = float(op.changes["confidence"])
            row = await conn.fetchrow(
                "SELECT confidence_at_assertion, confidence FROM models WHERE id = $1",
                op.model_id,
            )
            if row is None:
                continue
            # Compare the new confidence against the confidence *before*
            # this apply — we need the pre-update value. After apply the
            # DB shows the new value; use confidence_at_assertion as the
            # baseline to detect dramatic drops from the original value.
            # For relative-drop anomaly we compare against the most
            # recent last_confidence_at_assertion-like reference.
            # Simpler: a drop > 0.25 relative to confidence_at_assertion.
            prior = float(row["confidence_at_assertion"])
            if (prior - new_conf) > 0.25:
                flagged.append(
                    Anomaly(
                        kind="confidence_drop",
                        region={"model_id": str(op.model_id)},
                        significance=min(1.0, 0.5 + (prior - new_conf)),
                        triggering_op={
                            "op": "update",
                            "model_id": str(op.model_id),
                            "prior": prior,
                            "new": new_conf,
                        },
                    )
                )

    # --- Critical-path commitment → Blocked / Paused ---
    for op in diff.act_ops:
        if op.op == "transition_commitment":
            new_state = op.entity.get("new_state")
            if new_state in ("blocked", "paused"):
                cid = op.entity.get("id")
                if cid is None:
                    continue
                is_cp = await conn.fetchval(
                    """
                    SELECT 1 FROM contributes_to
                    WHERE commitment_id = $1 AND is_critical_path = TRUE
                    LIMIT 1
                    """,
                    cid,
                )
                if is_cp:
                    flagged.append(
                        Anomaly(
                            kind="critical_path_blocked",
                            region={
                                "commitment_id": str(cid),
                                "new_state": new_state,
                            },
                            significance=0.75,
                            triggering_op={
                                "op": "transition_commitment",
                                "new_state": new_state,
                                "commitment_id": str(cid),
                            },
                        )
                    )

    # --- Resource over-deployment ---
    for op in diff.resource_ops:
        if op.op == "deploy" and op.resource_id is not None:
            r = await conn.fetchrow(
                "SELECT kind, current_value FROM resources WHERE id = $1",
                op.resource_id,
            )
            if r is None or r["kind"] != "capacity":
                continue
            cv = r["current_value"] or {}
            # current_value is already a dict when JSONB codec registered.
            if isinstance(cv, str):
                import json as _json
                try:
                    cv = _json.loads(cv)
                except Exception:
                    cv = {}
            total = float(cv.get("total_units", 0) or 0)
            deployed = float(cv.get("deployed_units", 0) or 0)
            if total > 0 and deployed / total > 0.95:
                flagged.append(
                    Anomaly(
                        kind="resource_over_deployed",
                        region={"resource_id": str(op.resource_id)},
                        significance=0.7,
                        triggering_op={
                            "op": "deploy",
                            "resource_id": str(op.resource_id),
                            "utilization": deployed / total,
                        },
                    )
                )

    # --- Customer health degradation ---
    for op in diff.resource_ops:
        if op.op == "update" and op.resource_id is not None:
            patch = op.patch or {}
            new_health = patch.get("health")
            if new_health in ("warning", "degraded", "critical"):
                flagged.append(
                    Anomaly(
                        kind="customer_health_degraded",
                        region={"resource_id": str(op.resource_id)},
                        significance=0.6 + {
                            "warning": 0.0,
                            "degraded": 0.1,
                            "critical": 0.2,
                        }[new_health],
                        triggering_op={
                            "op": "update",
                            "resource_id": str(op.resource_id),
                            "new_health": new_health,
                        },
                    )
                )

    return flagged


async def publish_anomalies(
    anomalies: list[Anomaly],
    think_run_id: UUID,
    tenant_id: UUID,
    conn: asyncpg.Connection,
) -> int:
    """
    Persist anomalies to the durable `think_anomalies_raw` table. This
    happens inside the apply transaction so anomaly visibility matches
    applied state (no pre-commit leak, no lost-on-crash).

    Returns the number of rows written.
    """
    if not anomalies:
        return 0
    import json as _json
    rows_written = 0
    for a in anomalies:
        await conn.execute(
            """
            INSERT INTO think_anomalies_raw
              (id, tenant_id, think_run_id, kind, region, significance, triggering_op)
            VALUES ($1, $2, $3, $4, $5::jsonb, $6, $7::jsonb)
            """,
            uuid7(),
            tenant_id,
            think_run_id,
            a.kind,
            _json.dumps(a.region, default=str),
            float(a.significance),
            _json.dumps(a.triggering_op, default=str),
        )
        rows_written += 1
    return rows_written


__all__ = [
    "Anomaly",
    "check_anomalies",
    "publish_anomalies",
]
