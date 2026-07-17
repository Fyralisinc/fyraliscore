"""Adversarial extension of the sealed DB-backed company-physics vertical.

The v1 vertical remains immutable.  This v2 artifact runs it as the positive
substrate, then exercises graph write invariants and correction cascades against
the same tenant.  Every adversarial attempt has an explicit consequence tier
and denominator; rejected writes are evidence only when the durable graph is
unchanged after the attempted mutation.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any
from uuid import UUID

import asyncpg

from lib.shared.edge_registry import EdgeRegistryError
from lib.shared.errors import ValidationError
from lib.shared.types import ModelCreate
from services.company_physics_vertical import run_company_physics_vertical
from services.domain.models.edges_repo import EdgesRepo
from services.domain.models.repo import ModelsRepo
from services.domain.observations.events import notify_scope


async def run_company_physics_adversarial_vertical(
    *,
    pool: asyncpg.Pool,
    tenant_id: UUID,
    output_path: Path | None = None,
    base_output_path: Path | None = None,
) -> dict[str, Any]:
    base = await run_company_physics_vertical(
        pool=pool,
        tenant_id=tenant_id,
        output_path=base_output_path,
    )
    edges = EdgesRepo()
    models = ModelsRepo(pool, run_topology_on_insert=False)

    async with pool.acquire() as conn, conn.transaction():
        base_edge = await conn.fetchrow(
            """SELECT source_model_id, target_model_id, edge_kind
               FROM model_edges WHERE tenant_id=$1 AND status='active'
               ORDER BY created_at LIMIT 1""",
            tenant_id,
        )
        if base_edge is None:
            raise AssertionError("positive vertical did not create its expected edge")
        source = base_edge["source_model_id"]
        target = base_edge["target_model_id"]
        born_from_event = await conn.fetchval(
            "SELECT born_from_event_id FROM models WHERE id=$1", source
        )
        chain_a = await _new_model(
            models, conn=conn, tenant_id=tenant_id,
            born_from_event=born_from_event, label="chain-a",
        )
        chain_b = await _new_model(
            models, conn=conn, tenant_id=tenant_id,
            born_from_event=born_from_event, label="chain-b",
        )
        chain_c = await _new_model(
            models, conn=conn, tenant_id=tenant_id,
            born_from_event=born_from_event, label="chain-c",
        )

        attempts: list[dict[str, Any]] = []
        await _expect_rejection(
            attempts, conn=conn, edges=edges, tenant_id=tenant_id,
            case_id="wrong-direction-reciprocal", consequence_tier="critical",
            source=target, target=source, kind="blocks",
        )
        await _expect_rejection(
            attempts, conn=conn, edges=edges, tenant_id=tenant_id,
            case_id="wrong-type-mutual-exclusion", consequence_tier="critical",
            source=source, target=target, kind="enables",
        )
        await _expect_rejection(
            attempts, conn=conn, edges=edges, tenant_id=tenant_id,
            case_id="wrong-link-self-edge", consequence_tier="high",
            source=source, target=source, kind="supports",
        )

        mention_ids = await conn.fetch(
            """SELECT mention_id FROM entity_mention_detections
               WHERE tenant_id=$1 AND mention_id IS NOT NULL
               ORDER BY detected_at, id LIMIT 3""", tenant_id,
        )
        lineage = {"source_entity_mention_ids": [str(row["mention_id"]) for row in mention_ids]}
        event_ids = [born_from_event]
        await edges.link(
            conn, source=chain_a.id, target=chain_b.id, kind="supports",
            tenant_id=tenant_id, detected_by="think_edge_op", confidence=0.91,
            metadata={**lineage, "sealed_vertical": "company-physics-adversarial-v2"},
            created_by_event_id=born_from_event, evidence_event_ids=event_ids,
        )
        await edges.link(
            conn, source=chain_b.id, target=chain_c.id, kind="supports",
            tenant_id=tenant_id, detected_by="think_edge_op", confidence=0.91,
            metadata={**lineage, "sealed_vertical": "company-physics-adversarial-v2"},
            created_by_event_id=born_from_event, evidence_event_ids=event_ids,
        )
        await _expect_rejection(
            attempts, conn=conn, edges=edges, tenant_id=tenant_id,
            case_id="multi-hop-cycle-closure", consequence_tier="high",
            source=chain_c.id, target=chain_a.id, kind="supports",
        )

        active_chain_before = await _chain_rows(
            conn, tenant_id=tenant_id,
            model_ids=(chain_a.id, chain_b.id, chain_c.id),
        )
        with notify_scope():
            await models.archive(chain_a.id, "contested_incorrect", conn=conn)
        chain_after = await _chain_rows(
            conn, tenant_id=tenant_id,
            model_ids=(chain_a.id, chain_b.id, chain_c.id), all_statuses=True,
        )
        queued = await conn.fetchval(
            """SELECT count(*) FROM model_reeval_queue
               WHERE tenant_id=$1 AND model_id=$2 AND cause_model_id=$3
                 AND cause_kind='contested_cluster'""",
            tenant_id, chain_b.id, chain_a.id,
        )
        active_graph_count = await conn.fetchval(
            "SELECT count(*) FROM model_edges WHERE tenant_id=$1 AND status='active'",
            tenant_id,
        )

    by_tier: dict[str, dict[str, int | float]] = {}
    for tier in ("critical", "high"):
        population = [row for row in attempts if row["consequence_tier"] == tier]
        rejected = sum(bool(row["rejected_without_write"]) for row in population)
        by_tier[tier] = {
            "attempts": len(population), "safe_rejections": rejected,
            "safe_rejection_rate": rejected / len(population) if population else 0.0,
        }
    correction = {
        "pre_correction_active_hops": len(active_chain_before),
        "first_hop_retired": any(
            row["source_model_id"] == str(chain_a.id)
            and row["target_model_id"] == str(chain_b.id)
            and row["status"] == "inert"
            for row in chain_after
        ),
        "downstream_reevaluation_enqueued": int(queued or 0) == 1,
        "second_hop_preserved_pending_reevaluation": any(
            row["source_model_id"] == str(chain_b.id)
            and row["target_model_id"] == str(chain_c.id)
            and row["status"] == "active"
            for row in chain_after
        ),
        "transitive_repair_claimed": False,
    }
    open_world = base["entity_pipeline_v4"]["overall"]
    objective = {
        "schema_version": "sealed-company-physics-adversarial-objective-v2",
        "tenant_id": str(tenant_id),
        "population": {
            "signal_batches": base["population"]["batches"],
            "signals": base["population"]["signals"],
            "adversarial_relation_attempts": len(attempts),
            "multi_hop_chains": 1,
            "correction_events": 1,
        },
        "base_objective_sha256": base["objective_sha256"],
        "adversarial_attempts": attempts,
        "consequence_tier_denominators": by_tier,
        "multi_hop": {
            "expected_hops": 2,
            "observed_active_hops_before_correction": len(active_chain_before),
            "cycle_closure_rejected": next(
                row["rejected_without_write"] for row in attempts
                if row["case_id"] == "multi-hop-cycle-closure"
            ),
            "mention_lineage_count": len(mention_ids),
        },
        "correction_propagation": correction,
        "open_world_abstention": {
            "safe_decision_rate": open_world["safe_decision_rate"],
            "harmful_false_link_rate": open_world["harmful_false_link_rate"],
            "relation_non_admission_safety_rate": open_world[
                "relation_non_admission_safety_rate"
            ],
            "novel_and_homonym_cases": 2,
        },
        "durable_graph": {"active_edges_after_correction": int(active_graph_count or 0)},
        "proof_boundary": (
            "The archive cascade proves immediate dependent re-evaluation and "
            "first-hop edge retirement. It does not claim completed transitive "
            "repair; the second hop remains active until the queued reevaluation runs."
        ),
    }
    canonical = json.dumps(objective, sort_keys=True, separators=(",", ":"))
    objective["objective_sha256"] = hashlib.sha256(canonical.encode()).hexdigest()
    if output_path is not None:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        temporary = output_path.with_suffix(".tmp")
        temporary.write_text(json.dumps(objective, indent=2, sort_keys=True) + "\n")
        temporary.replace(output_path)
    return objective


async def _new_model(
    repo: ModelsRepo, *, conn: asyncpg.Connection, tenant_id: UUID,
    born_from_event: UUID, label: str,
):
    with notify_scope():
        return await repo.insert(
            ModelCreate(
                tenant_id=tenant_id, born_from_event_id=born_from_event,
                proposition={"kind": "state", "subject": label,
                             "assertion": "is active"},
                natural=f"{label} is active",
                embedding=[0.02] * 768, scope_temporal={"type": "now"},
                confidence=0.65, confidence_at_assertion=0.65,
            ),
            conn=conn,
        )


async def _expect_rejection(
    attempts: list[dict[str, Any]], *, conn: asyncpg.Connection,
    edges: EdgesRepo, tenant_id: UUID, case_id: str, consequence_tier: str,
    source: UUID, target: UUID, kind: str,
) -> None:
    before = await conn.fetchval(
        "SELECT count(*) FROM model_edges WHERE tenant_id=$1", tenant_id
    )
    error_type = None
    try:
        await edges.link(
            conn, source=source, target=target, kind=kind, tenant_id=tenant_id,
            detected_by="think_edge_op", confidence=0.99,
            metadata={"sealed_adversarial_attempt": case_id},
        )
    except (EdgeRegistryError, ValidationError) as exc:
        error_type = type(exc).__name__
    after = await conn.fetchval(
        "SELECT count(*) FROM model_edges WHERE tenant_id=$1", tenant_id
    )
    attempts.append({
        "case_id": case_id, "consequence_tier": consequence_tier,
        "relation_type": kind, "error_type": error_type,
        "rejected_without_write": error_type is not None and before == after,
    })


async def _chain_rows(
    conn: asyncpg.Connection, *, tenant_id: UUID,
    model_ids: tuple[UUID, ...], all_statuses: bool = False,
) -> list[dict[str, Any]]:
    rows = await conn.fetch(
        """SELECT source_model_id, target_model_id, status
           FROM model_edges WHERE tenant_id=$1
             AND source_model_id=ANY($2::uuid[]) AND target_model_id=ANY($2::uuid[])
             AND ($3::bool OR status='active') ORDER BY created_at""",
        tenant_id, list(model_ids), all_statuses,
    )
    return [
        {"source_model_id": str(row["source_model_id"]),
         "target_model_id": str(row["target_model_id"]), "status": row["status"]}
        for row in rows
    ]


__all__ = ["run_company_physics_adversarial_vertical"]
