"""services/forecasts/router.py — FastAPI surface for the Forecasts page.

Endpoints (all under /v1/forecasts, BearerAuth via gateway middleware):

  GET  /                  — list (status, category, sort, limit)
  GET  /summary           — strip counters
  GET  /{prediction_id}   — detail (row + signals)
  GET  /accuracy          — bins + recent resolutions + calibration
  GET  /risk_exposure     — weekly time series
  GET  /upcoming          — predictions resolving in next N days
  POST /                  — create scenario

Tenant comes from request.state.auth (set by BearerAuthMiddleware).
The pool comes from `request.app.state.deps.pool`. Both contracts mirror
services/conversations/api.py and services/recommendations (via the
gateway main module).
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Any
from uuid import UUID

from fastapi import APIRouter, HTTPException, Request, status as httpstatus
from fastapi.responses import JSONResponse

from lib.shared.errors import ValidationError
from services.forecasts import accuracy as accuracy_mod
from services.forecasts import page as page_mod
from services.forecasts import repo as repo_mod


log = logging.getLogger(__name__)


def build_router() -> APIRouter:
    router = APIRouter(prefix="/v1/forecasts", tags=["forecasts"])

    @router.get("")
    @router.get("/")
    async def list_endpoint(request: Request) -> JSONResponse:
        auth = _auth(request)
        pool = _pool(request)
        qp = request.query_params

        try:
            status = qp.get("status", "active")
            category = qp.get("category")
            sort = qp.get("sort", "earliest_resolution")
            limit_raw = qp.get("limit", "50")
            try:
                limit = max(1, min(200, int(limit_raw)))
            except (TypeError, ValueError):
                return _bad("invalid_limit")
            async with pool.acquire() as conn:
                rows = await repo_mod.list_predictions(
                    conn, auth.tenant_id,
                    status=status,
                    category=category,
                    sort=sort,
                    limit=limit,
                )
        except ValidationError as e:
            return _bad(e.message, **e.context)
        return JSONResponse({
            "items": [_serialize_prediction(r) for r in rows],
            "count": len(rows),
        })

    @router.get("/summary")
    async def summary_endpoint(request: Request) -> JSONResponse:
        auth = _auth(request)
        pool = _pool(request)
        async with pool.acquire() as conn:
            counters = await repo_mod.summary_counters(conn, auth.tenant_id)
            cal = await accuracy_mod.calibration_summary(conn, auth.tenant_id)
        return JSONResponse({
            "active_count": counters["active_count"],
            "at_risk_arr": counters["at_risk_arr"],
            "high_confidence_count": counters["high_confidence_count"],
            "upcoming_resolutions_count_14d": counters["upcoming_resolutions_count_14d"],
            "model_calibration": cal.value,
            "calibration_delta": cal.delta_vs_last_week,
        })

    @router.get("/accuracy")
    async def accuracy_endpoint(request: Request) -> JSONResponse:
        auth = _auth(request)
        pool = _pool(request)
        qp = request.query_params
        try:
            range_days = int(qp.get("days", "180"))
        except (TypeError, ValueError):
            return _bad("invalid_days")
        try:
            limit = int(qp.get("limit", "20"))
        except (TypeError, ValueError):
            return _bad("invalid_limit")
        async with pool.acquire() as conn:
            bins = await accuracy_mod.accuracy_bins(
                conn, auth.tenant_id, range_days=range_days,
            )
            recent = await accuracy_mod.recent_resolutions(
                conn, auth.tenant_id, limit=limit,
            )
            cal = await accuracy_mod.calibration_summary(conn, auth.tenant_id)
        return JSONResponse({
            "bins": [
                {
                    "bin_label": b.bin_label,
                    "predicted_rate": b.predicted_rate,
                    "observed_hit_rate": b.observed_hit_rate,
                    "n_resolved": b.n_resolved,
                }
                for b in bins
            ],
            "recent_resolutions": [
                {
                    "id": str(r.id),
                    "statement": r.statement,
                    "category": r.category,
                    "confidence": r.confidence,
                    "outcome": r.outcome,
                    "resolution_timeliness": r.resolution_timeliness,
                    "resolved_at": _iso(r.resolved_at),
                    "resolution_at": _iso(r.resolution_at),
                }
                for r in recent
            ],
            "calibration_summary": {
                "value": cal.value,
                "delta_vs_last_week": cal.delta_vs_last_week,
                "n_resolved_total": cal.n_resolved_total,
            },
        })

    @router.get("/risk_exposure")
    async def risk_exposure_endpoint(request: Request) -> JSONResponse:
        auth = _auth(request)
        pool = _pool(request)
        qp = request.query_params
        metric = qp.get("metric", "arr_at_risk")
        try:
            days = int(qp.get("days", "90"))
        except (TypeError, ValueError):
            return _bad("invalid_days")
        async with pool.acquire() as conn:
            series = await repo_mod.risk_exposure_series(
                conn, auth.tenant_id, metric=metric, range_days=days,
            )
        return JSONResponse({
            "metric": metric,
            "range_days": days,
            "buckets": [
                {
                    "bucket_start": _iso(b["bucket_start"]),
                    "bucket_end": _iso(b["bucket_end"]),
                    "value": float(b["value"]),
                }
                for b in series
            ],
        })

    @router.get("/upcoming")
    async def upcoming_endpoint(request: Request) -> JSONResponse:
        auth = _auth(request)
        pool = _pool(request)
        qp = request.query_params
        try:
            days = int(qp.get("days", "14"))
        except (TypeError, ValueError):
            return _bad("invalid_days")
        async with pool.acquire() as conn:
            rows = await repo_mod.upcoming_resolutions(
                conn, auth.tenant_id, days=days,
            )
        return JSONResponse({
            "items": [_serialize_prediction(r) for r in rows],
            "count": len(rows),
            "days": days,
        })

    @router.post("")
    @router.post("/")
    async def create_endpoint(request: Request) -> JSONResponse:
        auth = _auth(request)
        pool = _pool(request)
        try:
            body = await request.json()
        except Exception:
            return _bad("invalid_json")
        if not isinstance(body, dict):
            return _bad("invalid_body")
        # tenant_id comes from auth — overrides any body field so a
        # client can't author across tenants.
        body = dict(body)
        body["tenant_id"] = auth.tenant_id
        try:
            async with pool.acquire() as conn:
                async with conn.transaction():
                    row = await repo_mod.create_prediction(conn, body)
        except ValidationError as e:
            return _bad(e.message, **e.context)
        return JSONResponse(
            _serialize_prediction(row),
            status_code=httpstatus.HTTP_201_CREATED,
        )

    # -----------------------------------------------------------------
    # Spec v1.0 endpoints — Forecasts page rewrite (4 modes, foresight
    # brief, horizon matrix, patterns, ask). Mounted before the
    # `/{prediction_id}` catch-all so the static paths win.
    # -----------------------------------------------------------------

    @router.get("/page")
    async def page_endpoint(request: Request) -> JSONResponse:
        """Initial payload for the Forecasts page (Horizon mode default).

        Composes header, foresight brief, horizon matrix, default
        selection + detail, patterns, and accuracy summary in one
        round trip so the first paint doesn't N+1.
        """
        auth = _auth(request)
        pool = _pool(request)
        qp = request.query_params
        try:
            horizon_days = int(qp.get("horizon_days", "90"))
        except (TypeError, ValueError):
            return _bad("invalid_horizon_days")
        async with pool.acquire() as conn:
            payload = await page_mod.build_page_payload(
                conn, auth.tenant_id, horizon_days=horizon_days,
            )
        return JSONResponse(payload)

    @router.get("/patterns")
    async def patterns_endpoint(request: Request) -> JSONResponse:
        """Pattern Field cards + Patterns mode payload."""
        auth = _auth(request)
        pool = _pool(request)
        async with pool.acquire() as conn:
            patterns = await page_mod.list_patterns(conn, auth.tenant_id)
        return JSONResponse({"patterns": patterns, "count": len(patterns)})

    @router.get("/detail/{forecast_id}")
    async def detail_v2_endpoint(
        forecast_id: str, request: Request,
    ) -> JSONResponse:
        """Spec ForecastDetail — driving patterns, leading indicators,
        falsifiers, intervention levers, related context."""
        auth = _auth(request)
        pool = _pool(request)
        try:
            fid = UUID(forecast_id)
        except (ValueError, TypeError):
            return _bad("invalid_forecast_id")
        async with pool.acquire() as conn:
            detail = await page_mod.build_forecast_detail(
                conn, auth.tenant_id, fid,
            )
        if detail is None:
            return JSONResponse({"error": "not_found"}, status_code=404)
        return JSONResponse(detail)

    @router.post("/ask")
    async def ask_endpoint(request: Request) -> JSONResponse:
        """Ask Fyralis (forecast-scoped). Returns a ForecastAskResponse
        keyed off the prompt intent (explanation/scenario/falsifier/
        intervention/pattern/accuracy).
        """
        auth = _auth(request)
        pool = _pool(request)
        try:
            body = await request.json()
        except Exception:
            return _bad("invalid_json")
        if not isinstance(body, dict):
            return _bad("invalid_body")
        prompt = body.get("prompt")
        if not isinstance(prompt, str) or not prompt.strip():
            return _bad("missing_prompt")
        selected = body.get("selected_forecast_id")
        try:
            selected_uuid = UUID(selected) if selected else None
        except (ValueError, TypeError):
            return _bad("invalid_selected_forecast_id")
        visible_raw = body.get("visible_forecast_ids") or []
        visible: list[UUID] = []
        if isinstance(visible_raw, list):
            for v in visible_raw:
                try:
                    visible.append(UUID(str(v)))
                except (ValueError, TypeError):
                    continue
        req = page_mod.AskRequest(
            page="forecasts",
            mode=str(body.get("mode") or "horizon"),
            selected_forecast_id=selected_uuid,
            selected_pattern_id=(
                str(body.get("selected_pattern_id"))
                if body.get("selected_pattern_id") else None
            ),
            prompt=prompt,
            visible_forecast_ids=visible,
            horizon_days=int(body.get("horizon_days") or 90),
        )
        async with pool.acquire() as conn:
            resp = await page_mod.handle_ask(conn, auth.tenant_id, req)
        return JSONResponse(resp)

    @router.get("/{prediction_id}")
    async def detail_endpoint(
        prediction_id: str, request: Request,
    ) -> JSONResponse:
        auth = _auth(request)
        pool = _pool(request)
        try:
            pid = UUID(prediction_id)
        except (ValueError, TypeError):
            return _bad("invalid_prediction_id")
        async with pool.acquire() as conn:
            detail = await repo_mod.get_prediction(
                conn, auth.tenant_id, pid,
            )
        if detail is None:
            return JSONResponse({"error": "not_found"}, status_code=404)
        return JSONResponse({
            "prediction": _serialize_prediction(detail.prediction),
            "signals": [
                {
                    "id": str(s.id),
                    "source": s.source,
                    "title": s.title,
                    "ts": _iso(s.ts),
                    "trust_tier": s.trust_tier,
                    "weight": s.weight,
                    "ordinal": s.ordinal,
                }
                for s in detail.signals
            ],
        })

    return router


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------


def _auth(request: Request):
    auth = getattr(request.state, "auth", None)
    if auth is None:
        raise HTTPException(status_code=401, detail="unauthorized")
    return auth


def _pool(request: Request):
    deps = getattr(request.app.state, "deps", None)
    if deps is None or getattr(deps, "pool", None) is None:
        raise HTTPException(
            status_code=500, detail="gateway_deps_not_initialised",
        )
    return deps.pool


def _bad(reason: str, **extra: Any) -> JSONResponse:
    payload: dict[str, Any] = {"error": "bad_request", "reason": reason}
    if extra:
        payload["context"] = {k: str(v) for k, v in extra.items()}
    return JSONResponse(payload, status_code=400)


def _serialize_prediction(p: repo_mod.PredictionRow) -> dict[str, Any]:
    return {
        "id": str(p.id),
        "tenant_id": str(p.tenant_id),
        "status": p.status,
        "statement": p.statement,
        "rationale": p.rationale,
        "category": p.category,
        "target_node_kind": p.target_node_kind,
        "target_node_id": str(p.target_node_id) if p.target_node_id else None,
        "target_label": p.target_label,
        "confidence": p.confidence,
        "confidence_basis": p.confidence_basis,
        "falsification_condition": p.falsification_condition,
        "key_drivers": p.key_drivers,
        "impact": p.impact,
        "resolution_at": _iso(p.resolution_at),
        "resolved_at": _iso(p.resolved_at) if p.resolved_at else None,
        "outcome": p.outcome,
        "resolution_timeliness": p.resolution_timeliness,
        "created_at": _iso(p.created_at),
        "updated_at": _iso(p.updated_at),
    }


def _iso(dt: datetime | None) -> str | None:
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.isoformat()


__all__ = ["build_router"]
