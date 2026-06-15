"""services/product/forecasts/router.py — FastAPI surface for the Forecasts page.

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
services/product/conversations/api.py and services/product/recommendations (via the
gateway main module).
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any
from uuid import UUID

from fastapi import APIRouter, HTTPException, Request, status as httpstatus
from fastapi.responses import JSONResponse

from lib.shared.errors import ValidationError
from services.product.forecasts import accuracy as accuracy_mod
from services.product.forecasts import page as page_mod
from services.product.forecasts import repo as repo_mod


log = logging.getLogger(__name__)


def build_router() -> APIRouter:
    router = APIRouter(prefix="/v1/forecasts", tags=["forecasts"])

    router.add_api_route("", list_endpoint, methods=["GET"])
    router.add_api_route("/", list_endpoint, methods=["GET"])
    router.add_api_route("/summary", summary_endpoint, methods=["GET"])
    router.add_api_route("/accuracy", accuracy_endpoint, methods=["GET"])
    router.add_api_route("/risk_exposure", risk_exposure_endpoint, methods=["GET"])
    router.add_api_route("/upcoming", upcoming_endpoint, methods=["GET"])
    router.add_api_route("", create_endpoint, methods=["POST"])
    router.add_api_route("/", create_endpoint, methods=["POST"])
    router.add_api_route("/page", page_endpoint, methods=["GET"])
    router.add_api_route("/patterns", patterns_endpoint, methods=["GET"])
    router.add_api_route("/detail/{forecast_id}", detail_v2_endpoint, methods=["GET"])
    router.add_api_route("/ask", ask_endpoint, methods=["POST"])
    router.add_api_route("/{prediction_id}", detail_endpoint, methods=["GET"])

    return router


async def list_endpoint(request: Request) -> JSONResponse:
    auth = _auth(request)
    qp = request.query_params
    try:
        limit = max(1, min(200, int(qp.get("limit", "50"))))
        async with _pool(request).acquire() as conn:
            rows = await repo_mod.list_predictions(
                conn,
                auth.tenant_id,
                status=qp.get("status", "active"),
                category=qp.get("category"),
                sort=qp.get("sort", "earliest_resolution"),
                limit=limit,
            )
    except (TypeError, ValueError):
        return _bad("invalid_limit")
    except ValidationError as e:
        return _bad(e.message, **e.context)
    return JSONResponse({
        "items": [_serialize_prediction(r) for r in rows],
        "count": len(rows),
    })


async def summary_endpoint(request: Request) -> JSONResponse:
    auth = _auth(request)
    async with _pool(request).acquire() as conn:
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


async def accuracy_endpoint(request: Request) -> JSONResponse:
    auth = _auth(request)
    qp = request.query_params
    try:
        range_days = int(qp.get("days", "180"))
        limit = int(qp.get("limit", "20"))
    except (TypeError, ValueError):
        return _bad("invalid_days")
    async with _pool(request).acquire() as conn:
        bins = await accuracy_mod.accuracy_bins(
            conn, auth.tenant_id, range_days=range_days,
        )
        recent = await accuracy_mod.recent_resolutions(
            conn, auth.tenant_id, limit=limit,
        )
        cal = await accuracy_mod.calibration_summary(conn, auth.tenant_id)
    return JSONResponse({
        "bins": [_accuracy_bin_to_wire(b) for b in bins],
        "recent_resolutions": [_resolution_to_wire(r) for r in recent],
        "calibration_summary": {
            "value": cal.value,
            "delta_vs_last_week": cal.delta_vs_last_week,
            "n_resolved_total": cal.n_resolved_total,
        },
    })


async def risk_exposure_endpoint(request: Request) -> JSONResponse:
    auth = _auth(request)
    qp = request.query_params
    metric = qp.get("metric", "arr_at_risk")
    try:
        days = int(qp.get("days", "90"))
    except (TypeError, ValueError):
        return _bad("invalid_days")
    async with _pool(request).acquire() as conn:
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


async def upcoming_endpoint(request: Request) -> JSONResponse:
    auth = _auth(request)
    try:
        days = int(request.query_params.get("days", "14"))
    except (TypeError, ValueError):
        return _bad("invalid_days")
    async with _pool(request).acquire() as conn:
        rows = await repo_mod.upcoming_resolutions(conn, auth.tenant_id, days=days)
    return JSONResponse({
        "items": [_serialize_prediction(r) for r in rows],
        "count": len(rows),
        "days": days,
    })


async def create_endpoint(request: Request) -> JSONResponse:
    auth = _auth(request)
    body = await _read_body_dict(request)
    if isinstance(body, JSONResponse):
        return body
    body = dict(body)
    body["tenant_id"] = auth.tenant_id
    try:
        async with _pool(request).acquire() as conn:
            async with conn.transaction():
                row = await repo_mod.create_prediction(conn, body)
    except ValidationError as e:
        return _bad(e.message, **e.context)
    return JSONResponse(
        _serialize_prediction(row),
        status_code=httpstatus.HTTP_201_CREATED,
    )


async def page_endpoint(request: Request) -> JSONResponse:
    auth = _auth(request)
    try:
        horizon_days = int(request.query_params.get("horizon_days", "90"))
    except (TypeError, ValueError):
        return _bad("invalid_horizon_days")
    async with _pool(request).acquire() as conn:
        payload = await page_mod.build_page_payload(
            conn, auth.tenant_id, horizon_days=horizon_days,
        )
    return JSONResponse(payload)


async def patterns_endpoint(request: Request) -> JSONResponse:
    auth = _auth(request)
    async with _pool(request).acquire() as conn:
        patterns = await page_mod.list_patterns(conn, auth.tenant_id)
    return JSONResponse({"patterns": patterns, "count": len(patterns)})


async def detail_v2_endpoint(forecast_id: str, request: Request) -> JSONResponse:
    auth = _auth(request)
    try:
        fid = UUID(forecast_id)
    except (ValueError, TypeError):
        return _bad("invalid_forecast_id")
    async with _pool(request).acquire() as conn:
        detail = await page_mod.build_forecast_detail(conn, auth.tenant_id, fid)
    if detail is None:
        return JSONResponse({"error": "not_found"}, status_code=404)
    return JSONResponse(detail)


async def ask_endpoint(request: Request) -> JSONResponse:
    auth = _auth(request)
    body = await _read_body_dict(request)
    if isinstance(body, JSONResponse):
        return body
    prompt = body.get("prompt")
    if not isinstance(prompt, str) or not prompt.strip():
        return _bad("missing_prompt")
    selected_uuid = _parse_optional_uuid(
        body.get("selected_forecast_id"),
        "invalid_selected_forecast_id",
    )
    if isinstance(selected_uuid, JSONResponse):
        return selected_uuid
    req = page_mod.AskRequest(
        page="forecasts",
        mode=str(body.get("mode") or "horizon"),
        selected_forecast_id=selected_uuid,
        selected_pattern_id=(
            str(body.get("selected_pattern_id"))
            if body.get("selected_pattern_id") else None
        ),
        prompt=prompt,
        visible_forecast_ids=_parse_visible_forecast_ids(
            body.get("visible_forecast_ids") or []
        ),
        horizon_days=int(body.get("horizon_days") or 90),
    )
    async with _pool(request).acquire() as conn:
        resp = await page_mod.handle_ask(conn, auth.tenant_id, req)
    return JSONResponse(resp)


async def detail_endpoint(prediction_id: str, request: Request) -> JSONResponse:
    auth = _auth(request)
    try:
        pid = UUID(prediction_id)
    except (ValueError, TypeError):
        return _bad("invalid_prediction_id")
    async with _pool(request).acquire() as conn:
        detail = await repo_mod.get_prediction(conn, auth.tenant_id, pid)
    if detail is None:
        return JSONResponse({"error": "not_found"}, status_code=404)
    return JSONResponse({
        "prediction": _serialize_prediction(detail.prediction),
        "signals": [_signal_to_wire(signal) for signal in detail.signals],
    })


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


async def _read_body_dict(request: Request) -> dict[str, Any] | JSONResponse:
    try:
        body = await request.json()
    except Exception:
        return _bad("invalid_json")
    if not isinstance(body, dict):
        return _bad("invalid_body")
    return body


def _parse_optional_uuid(value: Any, reason: str) -> UUID | None | JSONResponse:
    if not value:
        return None
    try:
        return UUID(str(value))
    except (ValueError, TypeError):
        return _bad(reason)


def _parse_visible_forecast_ids(value: Any) -> list[UUID]:
    visible: list[UUID] = []
    if not isinstance(value, list):
        return visible
    for raw in value:
        try:
            visible.append(UUID(str(raw)))
        except (ValueError, TypeError):
            continue
    return visible


def _accuracy_bin_to_wire(bin_row: Any) -> dict[str, Any]:
    return {
        "bin_label": bin_row.bin_label,
        "predicted_rate": bin_row.predicted_rate,
        "observed_hit_rate": bin_row.observed_hit_rate,
        "n_resolved": bin_row.n_resolved,
    }


def _resolution_to_wire(resolution: Any) -> dict[str, Any]:
    return {
        "id": str(resolution.id),
        "statement": resolution.statement,
        "category": resolution.category,
        "confidence": resolution.confidence,
        "outcome": resolution.outcome,
        "resolution_timeliness": resolution.resolution_timeliness,
        "resolved_at": _iso(resolution.resolved_at),
        "resolution_at": _iso(resolution.resolution_at),
    }


def _signal_to_wire(signal: Any) -> dict[str, Any]:
    return {
        "id": str(signal.id),
        "source": signal.source,
        "title": signal.title,
        "ts": _iso(signal.ts),
        "trust_tier": signal.trust_tier,
        "weight": signal.weight,
        "ordinal": signal.ordinal,
    }


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
