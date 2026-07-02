"""Internal Sage control routes mounted by the gateway."""
from __future__ import annotations

from dataclasses import asdict
from typing import Any
from uuid import UUID

import asyncpg
from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse


def build_sage_internal_router() -> APIRouter:
    router = APIRouter(prefix="/internal", tags=["sage-internal"])

    @router.post("/synthesis-reader/read")
    async def synthesis_reader_read(request: Request) -> JSONResponse:
        pool = _pool(request)
        body = await request.json()
        try:
            tenant_id = UUID(str(body["tenant_id"]))
        except Exception:
            return JSONResponse({"error": "tenant_id required as UUID"}, status_code=400)
        signal_id = body.get("signal_id")
        observation_id = None
        if signal_id:
            try:
                observation_id = UUID(str(signal_id))
            except Exception:
                observation_id = None
        question = str(body.get("question") or "")
        if not question.strip():
            return JSONResponse({"error": "question required"}, status_code=400)
        primitive = str(body.get("question_primitive") or "DEPENDENCY").upper()
        async with pool.acquire() as conn:
            seed_text = await _seed_text(conn, tenant_id, observation_id)
            from services.reasoning.retrieval.primary import TriggerContext
            from services.reasoning.sage.reader import SynthesisReader

            result = await SynthesisReader(pool=pool).read(
                conn=conn,
                tenant_id=tenant_id,
                trigger=TriggerContext(
                    kind="T1",
                    tenant_id=tenant_id,
                    observation_id=observation_id,
                    seed_natural_text=seed_text or question,
                    seed_entity_ids=body.get("known_entities") or [],
                ),
                question_id=str(body.get("question_id") or "Q_API"),
                question=question,
                question_primitive=primitive,
                hypotheses=tuple(body.get("hypotheses") or ()),
            )
        return JSONResponse({
            "activated_nodes": [
                {
                    "model_id": str(t.model_id),
                    "activation_score": t.activation_score,
                    "activation_reasons": list(t.activation_reasons),
                    "selected": t.selected,
                    "selection_rank": t.selection_rank,
                    "source_breakdown": t.source_breakdown,
                }
                for t in result.activations
            ],
            "selected_subgraph": {
                "selected_nodes": [str(mid) for mid in result.selection.selected_nodes],
                "selected_edges": [str(eid) for eid in result.selection.selected_edges],
                "bridge_nodes": [str(mid) for mid in result.selection.bridge_nodes],
                "excluded": [
                    {
                        "model_id": str(e.model_id),
                        "reason": e.reason,
                        "summarized": e.summarized,
                    }
                    for e in result.selection.excluded
                ],
                "coverage_metrics": result.selection.coverage_metrics,
            },
            "projected_evidence": list(result.projected_evidence),
            "omission_candidates": [
                {"evidence_id": eid, "reason": reason}
                for eid, reason in result.omitted_projection
            ],
            "debug": result.debug,
        })

    @router.post("/topology-optimizer/optimize")
    async def topology_optimizer_optimize(request: Request) -> JSONResponse:
        pool = _pool(request)
        body = await request.json()
        try:
            tenant_id = UUID(str(body["tenant_id"]))
            session_id = UUID(str(body["inquiry_session_id"]))
        except Exception:
            return JSONResponse(
                {"error": "tenant_id and inquiry_session_id required as UUID"},
                status_code=400,
            )
        from services.reasoning.sage.topology_optimizer.cadence import (
            OptimizationCadenceRequest,
            run_optimization_pass,
        )

        report = await run_optimization_pass(
            pool=pool,
            request=OptimizationCadenceRequest(
                tenant_id=tenant_id,
                inquiry_session_id=session_id,
                trigger_event=str(body.get("trigger_event") or ""),
                source="sage_internal_route",
            ),
        )
        return JSONResponse({
            "discovery_updates_applied": {
                "affordance_reinforces": report.affordance_reinforces,
                "affordance_decays": report.affordance_decays,
                "shortcut_creates_or_bumps": report.shortcut_creates_or_bumps,
                "shortcut_decays": report.shortcut_decays,
                "negative_memory_inserts": report.negative_memory_inserts,
                "region_refreshes": report.region_refreshes,
                "question_policy_updates": report.question_policy_updates,
            },
            "canonical_update_candidates": {
                "merge": list(report.canonical_merge_candidates),
                "split": list(report.canonical_split_candidates),
                "promote": list(report.canonical_promote_candidates),
                "demote": list(report.canonical_demote_candidates),
            },
            "experience_loop": report.experience_loop,
            "metrics": report.metrics,
        })

    @router.post("/evidence-projector/project")
    async def evidence_projector_project(request: Request) -> JSONResponse:
        pool = _pool(request)
        body = await request.json()
        try:
            tenant_id = UUID(str(body["tenant_id"]))
            node_ids = [UUID(str(v)) for v in body.get("node_ids", [])]
        except Exception:
            return JSONResponse(
                {"error": "tenant_id and node_ids required"},
                status_code=400,
            )
        from services.reasoning.sage.evidence_projection import EvidenceProjector

        result = await EvidenceProjector().project(
            pool=pool,
            tenant_id=tenant_id,
            selected_model_ids=node_ids,
            question_primitive=str(
                body.get("question_primitive") or "DEPENDENCY"
            ).upper(),
        )
        return JSONResponse({
            "projected_evidence": [
                _jsonable(asdict(candidate))
                for candidate in result.projected
            ],
            "omitted": [
                {"evidence_id": str(eid), "reason": reason}
                for eid, reason in result.omitted
            ],
            "coverage": result.coverage,
        })

    return router


async def _seed_text(
    conn: asyncpg.Connection,
    tenant_id: UUID,
    observation_id: UUID | None,
) -> str:
    if observation_id is None:
        return ""
    return await conn.fetchval(
        """
        SELECT content_text
        FROM observations
        WHERE tenant_id = $1 AND id = $2
        """,
        tenant_id,
        observation_id,
    ) or ""


def _pool(request: Request) -> asyncpg.Pool:
    deps = getattr(request.app.state, "deps", None)
    if deps is None:
        raise RuntimeError("Gateway deps not initialised (call lifespan startup)")
    return deps.pool


def _jsonable(value: Any) -> Any:
    if isinstance(value, UUID):
        return str(value)
    if hasattr(value, "isoformat"):
        try:
            return value.isoformat()
        except Exception:
            pass
    if isinstance(value, dict):
        return {str(k): _jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_jsonable(v) for v in value]
    return value
