"""services/gateway/debug_router.py — read-only inspector endpoints.

Powers the /debug UI that shows the full substrate and the end-to-end
processing log for every signal. Scoped by X-Tenant-Id (falls back to
DEFAULT_TENANT_ID in dev). All endpoints are read-only; the single
mutating endpoint is `POST /debug/force-refresh-cache` which just
forwards to GRT.

Fields returned are the raw DB shape — this is a developer tool, not
a user-facing surface. If you want voice or cards, use /view/ceo/*.
"""
from __future__ import annotations

import json
import os
from datetime import datetime
from typing import Any, Optional
from uuid import UUID

import asyncpg
from fastapi import APIRouter, HTTPException, Query, Request


# --------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------

def _resolve_tenant(req: Request) -> UUID:
    hdr = req.headers.get("X-Tenant-Id")
    if hdr:
        try:
            return UUID(hdr)
        except Exception:  # noqa: BLE001
            raise HTTPException(status_code=400, detail="invalid X-Tenant-Id")
    env_tid = os.environ.get("DEFAULT_TENANT_ID") or os.environ.get("COMPANY_OS_TENANT_ID")
    if env_tid:
        try:
            return UUID(env_tid)
        except Exception:  # noqa: BLE001
            pass
    raise HTTPException(status_code=400, detail="tenant_id missing")


def _jsonify(row: asyncpg.Record | None) -> dict | None:
    if row is None:
        return None
    out: dict[str, Any] = {}
    for k, v in dict(row).items():
        if isinstance(v, (datetime,)):
            out[k] = v.isoformat()
        elif isinstance(v, UUID):
            out[k] = str(v)
        elif isinstance(v, (bytes, bytearray, memoryview)):
            out[k] = bytes(v).hex()
        elif isinstance(v, str) and (k.endswith("_json") or k in ("payload", "proposition", "content", "ops_applied")):
            try:
                out[k] = json.loads(v)
            except Exception:  # noqa: BLE001
                out[k] = v
        else:
            out[k] = v
    return out


async def _pool_from_request(req: Request) -> asyncpg.Pool:
    pool = getattr(req.app.state, "deps", None)
    if pool is not None and hasattr(pool, "pool"):
        return pool.pool
    pool = getattr(req.app.state, "pool", None)
    if pool is not None:
        return pool
    raise HTTPException(status_code=500, detail="pool unavailable")


# --------------------------------------------------------------------
# Router
# --------------------------------------------------------------------

def build_debug_router() -> APIRouter:
    router = APIRouter(prefix="/debug", tags=["debug"])

    # ---------- Signals ------------------------------------------
    @router.get("/signals")
    async def list_signals(
        req: Request,
        limit: int = Query(50, ge=1, le=500),
        before: Optional[str] = Query(None),
        channel: Optional[str] = Query(None),
    ):
        tid = _resolve_tenant(req)
        pool = await _pool_from_request(req)
        where = ["tenant_id = $1"]
        args: list[Any] = [tid]
        if channel:
            args.append(channel)
            where.append(f"source_channel = ${len(args)}")
        if before:
            try:
                args.append(UUID(before))
                where.append(f"id < ${len(args)}")
            except Exception:  # noqa: BLE001
                pass
        q = (
            "SELECT id, source_channel, source_actor_ref, kind, actor_id, "
            "       occurred_at, content_text, "
            "       (SELECT count(*) FROM think_runs r "
            "        WHERE r.tenant_id = o.tenant_id "
            "          AND r.trigger_id IN ("
            "             SELECT id FROM think_trigger_queue q "
            "             WHERE q.observation_id = o.id)) as run_count "
            "FROM observations o "
            f"WHERE {' AND '.join(where)} "
            "ORDER BY occurred_at DESC "
            f"LIMIT {limit}"
        )
        async with pool.acquire() as conn:
            rows = await conn.fetch(q, *args)
        return {"signals": [_jsonify(r) for r in rows]}

    @router.get("/signals/{observation_id}")
    async def get_signal(observation_id: str, req: Request):
        tid = _resolve_tenant(req)
        pool = await _pool_from_request(req)
        try:
            oid = UUID(observation_id)
        except Exception:  # noqa: BLE001
            raise HTTPException(status_code=400, detail="bad observation_id")
        async with pool.acquire() as conn:
            obs = await conn.fetchrow(
                "SELECT id, tenant_id, occurred_at, ingested_at, kind, "
                "       source_channel, source_actor_ref, actor_id, "
                "       content, content_text, embedding_pending, trust_tier, "
                "       external_id, cause_id, sequence_num, entities_mentioned "
                "FROM observations WHERE id = $1 AND tenant_id = $2",
                oid, tid,
            )
            if obs is None:
                raise HTTPException(status_code=404, detail="not found")
            triggers = await conn.fetch(
                "SELECT id, trigger_kind, trigger_subkind, enqueued_at, "
                "       scheduled_for, attempts, locked_by, locked_at "
                "FROM think_trigger_queue "
                "WHERE tenant_id = $1 AND observation_id = $2 "
                "ORDER BY enqueued_at",
                tid, oid,
            )
            runs = await conn.fetch(
                "SELECT tr.* FROM think_runs tr "
                "WHERE tr.tenant_id = $1 AND tr.trigger_id IN ("
                "  SELECT id FROM think_trigger_queue "
                "  WHERE observation_id = $2) "
                "ORDER BY started_at",
                tid, oid,
            )
            run_ids = [r["id"] for r in runs]
            artifacts: list[dict] = []
            if run_ids:
                arows = await conn.fetch(
                    "SELECT id, run_id, stage, payload, captured_at "
                    "FROM think_run_artifacts "
                    f"WHERE run_id = ANY($1::uuid[]) "
                    "ORDER BY captured_at",
                    run_ids,
                )
                artifacts = [_jsonify(r) for r in arows]
            models_born = await conn.fetch(
                "SELECT id, proposition_kind, status, confidence, "
                "       proposition, created_at "
                "FROM models "
                "WHERE tenant_id = $1 AND born_from_event_id = $2",
                tid, oid,
            )
        return {
            "observation": _jsonify(obs),
            "triggers": [_jsonify(r) for r in triggers],
            "runs": [_jsonify(r) for r in runs],
            "artifacts": artifacts,
            "models_born": [_jsonify(r) for r in models_born],
        }

    # ---------- Think runs ---------------------------------------
    @router.get("/think-runs")
    async def list_think_runs(
        req: Request,
        limit: int = Query(50, ge=1, le=500),
        status: Optional[str] = Query(None),
        trigger_kind: Optional[str] = Query(None),
    ):
        tid = _resolve_tenant(req)
        pool = await _pool_from_request(req)
        where = ["tenant_id = $1"]
        args: list[Any] = [tid]
        if status:
            args.append(status); where.append(f"status = ${len(args)}")
        if trigger_kind:
            args.append(trigger_kind); where.append(f"trigger_kind = ${len(args)}")
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT id, trigger_id, trigger_kind, started_at, ended_at, "
                "       status, error, retrieval_model_count, "
                "       retrieval_observation_count, llm_latency_ms, "
                "       validation_error_count, ops_applied, cascade_depth "
                f"FROM think_runs WHERE {' AND '.join(where)} "
                f"ORDER BY started_at DESC LIMIT {limit}",
                *args,
            )
        return {"runs": [_jsonify(r) for r in rows]}

    @router.get("/think-runs/{run_id}")
    async def get_think_run(run_id: str, req: Request):
        tid = _resolve_tenant(req)
        pool = await _pool_from_request(req)
        try:
            rid = UUID(run_id)
        except Exception:  # noqa: BLE001
            raise HTTPException(status_code=400, detail="bad run_id")
        async with pool.acquire() as conn:
            run = await conn.fetchrow(
                "SELECT * FROM think_runs WHERE id = $1 AND tenant_id = $2",
                rid, tid,
            )
            if run is None:
                raise HTTPException(status_code=404, detail="not found")
            artifacts = await conn.fetch(
                "SELECT id, stage, payload, captured_at FROM think_run_artifacts "
                "WHERE run_id = $1 ORDER BY captured_at",
                rid,
            )
            trigger = await conn.fetchrow(
                "SELECT * FROM think_trigger_queue WHERE id = $1",
                run["trigger_id"],
            )
            observation = None
            if trigger is not None and trigger["observation_id"] is not None:
                observation = await conn.fetchrow(
                    "SELECT id, source_channel, content_text, occurred_at, "
                    "       actor_id, kind "
                    "FROM observations WHERE id = $1",
                    trigger["observation_id"],
                )
        return {
            "run": _jsonify(run),
            "trigger": _jsonify(trigger),
            "observation": _jsonify(observation),
            "artifacts": [_jsonify(r) for r in artifacts],
        }

    # ---------- Models -------------------------------------------
    @router.get("/models")
    async def list_models(
        req: Request,
        limit: int = Query(100, ge=1, le=500),
        status: Optional[str] = Query(None),
        kind: Optional[str] = Query(None),
        min_confidence: Optional[float] = Query(None),
    ):
        tid = _resolve_tenant(req)
        pool = await _pool_from_request(req)
        where = ["tenant_id = $1"]
        args: list[Any] = [tid]
        if status:
            args.append(status); where.append(f"status = ${len(args)}")
        if kind:
            args.append(kind); where.append(f"proposition_kind = ${len(args)}")
        if min_confidence is not None:
            args.append(min_confidence); where.append(f"confidence >= ${len(args)}")
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT id, proposition_kind, status, confidence, "
                "       confidence_at_assertion, confirmed_count, "
                "       contested_count, proposition, "
                "       born_from_event_id, last_confirmed_at, created_at "
                f"FROM models WHERE {' AND '.join(where)} "
                f"ORDER BY created_at DESC LIMIT {limit}",
                *args,
            )
        return {"models": [_jsonify(r) for r in rows]}

    @router.get("/models/{model_id}")
    async def get_model(model_id: str, req: Request):
        tid = _resolve_tenant(req)
        pool = await _pool_from_request(req)
        try:
            mid = UUID(model_id)
        except Exception:  # noqa: BLE001
            raise HTTPException(status_code=400, detail="bad model_id")
        async with pool.acquire() as conn:
            model = await conn.fetchrow(
                "SELECT id, tenant_id, born_from_event_id, proposition, "
                "       scope_actors, confidence, supporting_event_ids, "
                "       supporting_model_ids, evidential_weight, "
                "       proposition_kind, status, confidence_at_assertion, "
                "       confirmed_count, contested_count, last_confirmed_at, "
                "       activation_coefficient, resolved_at, resolution_outcome, "
                "       archived_at, archive_reason, created_at "
                "FROM models WHERE id = $1 AND tenant_id = $2",
                mid, tid,
            )
            if model is None:
                raise HTTPException(status_code=404, detail="not found")
            status_notes = await conn.fetch(
                "SELECT id, note, authored_by, authored_at, kind "
                "FROM model_status_notes "
                "WHERE model_id = $1 ORDER BY authored_at DESC",
                mid,
            )
            # Supporting events + models
            supporting_events: list[dict] = []
            support_ids = model["supporting_event_ids"] or []
            if support_ids:
                erows = await conn.fetch(
                    "SELECT id, source_channel, kind, content_text, occurred_at "
                    "FROM observations WHERE id = ANY($1::uuid[])",
                    list(support_ids),
                )
                supporting_events = [_jsonify(r) for r in erows]
            supporting_models: list[dict] = []
            sm_ids = model["supporting_model_ids"] or []
            if sm_ids:
                mrows = await conn.fetch(
                    "SELECT id, proposition_kind, status, confidence, proposition "
                    "FROM models WHERE id = ANY($1::uuid[])",
                    list(sm_ids),
                )
                supporting_models = [_jsonify(r) for r in mrows]
        return {
            "model": _jsonify(model),
            "status_notes": [_jsonify(r) for r in status_notes],
            "supporting_events": supporting_events,
            "supporting_models": supporting_models,
        }

    # ---------- Acts ---------------------------------------------
    @router.get("/acts")
    async def list_acts(
        req: Request,
        kind: str = Query(..., regex="^(commitment|goal|decision|resource)$"),
        limit: int = Query(100, ge=1, le=500),
    ):
        tid = _resolve_tenant(req)
        pool = await _pool_from_request(req)
        table = {
            "commitment": "commitments",
            "goal": "goals",
            "decision": "decisions",
            "resource": "resources",
        }[kind]
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                f"SELECT * FROM {table} WHERE tenant_id = $1 "
                f"ORDER BY created_at DESC LIMIT {limit}",
                tid,
            )
        return {"kind": kind, "rows": [_jsonify(r) for r in rows]}

    # ---------- Render ledger ------------------------------------
    @router.get("/renders")
    async def list_renders(
        req: Request,
        limit: int = Query(100, ge=1, le=500),
        render_kind: Optional[str] = Query(None),
    ):
        tid = _resolve_tenant(req)
        pool = await _pool_from_request(req)
        where = ["tenant_id = $1"]
        args: list[Any] = [tid]
        if render_kind:
            args.append(render_kind); where.append(f"render_kind = ${len(args)}")
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT render_id, render_kind, outcome, llm_calls_count, "
                "       llm_input_tokens_total, llm_output_tokens_total, "
                "       llm_cost_usd, latency_total_ms, retry_count, flagged, "
                "       model_name, computed_at "
                f"FROM view_render_costs WHERE {' AND '.join(where)} "
                f"ORDER BY computed_at DESC LIMIT {limit}",
                *args,
            )
            summary = await conn.fetch(
                "SELECT render_kind, count(*) as count, "
                "       sum(llm_cost_usd)::numeric(12,6) as total_usd, "
                "       round(avg(latency_total_ms)) as avg_ms "
                "FROM view_render_costs WHERE tenant_id = $1 "
                "GROUP BY render_kind ORDER BY render_kind",
                tid,
            )
        return {
            "renders": [_jsonify(r) for r in rows],
            "summary": [_jsonify(r) for r in summary],
        }

    # ---------- Cache peek ---------------------------------------
    @router.get("/cache")
    async def list_cache(req: Request):
        tid = _resolve_tenant(req)
        pool = await _pool_from_request(req)
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT cache_key, cached_at, "
                "       extract(epoch from (now() - cached_at))::int as age_seconds, "
                "       cached_content as payload "
                "FROM view_ceo_cache WHERE tenant_id = $1 "
                "ORDER BY cached_at DESC",
                tid,
            )
        return {"cache": [_jsonify(r) for r in rows]}

    # ---------- Stats summary ------------------------------------
    @router.get("/stats")
    async def stats(req: Request):
        tid = _resolve_tenant(req)
        pool = await _pool_from_request(req)
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT "
                "  (SELECT count(*) FROM observations WHERE tenant_id = $1) as observations, "
                "  (SELECT count(*) FROM models WHERE tenant_id = $1 AND status='active') as active_models, "
                "  (SELECT count(*) FROM models WHERE tenant_id = $1 AND status='archived') as archived_models, "
                "  (SELECT count(*) FROM commitments WHERE tenant_id = $1) as commitments, "
                "  (SELECT count(*) FROM goals WHERE tenant_id = $1) as goals, "
                "  (SELECT count(*) FROM decisions WHERE tenant_id = $1) as decisions, "
                "  (SELECT count(*) FROM resources WHERE tenant_id = $1) as resources, "
                "  (SELECT count(*) FROM think_runs WHERE tenant_id = $1) as think_runs, "
                "  (SELECT count(*) FROM think_trigger_queue WHERE tenant_id = $1) as trigger_queue_depth, "
                "  (SELECT count(*) FROM applied_triggers WHERE tenant_id = $1) as applied_triggers, "
                "  (SELECT count(*) FROM think_run_artifacts WHERE tenant_id = $1) as artifacts, "
                "  (SELECT count(*) FROM view_render_costs WHERE tenant_id = $1) as renders ",
                tid,
            )
        return {"stats": _jsonify(row), "tenant_id": str(tid)}

    return router


__all__ = ["build_debug_router"]
