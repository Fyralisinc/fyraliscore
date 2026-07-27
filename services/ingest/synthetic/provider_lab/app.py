"""Loopback-only Provider Lab FastAPI/ASGI application.

Control plane (not written to the provider request ledger):

* ``/_lab/state`` and ``/_lab/sources/{source}/state``
* ``/_lab/clock`` and ``/_lab/clock/advance``
* ``/_lab/quotas``
* ``/_lab/faults``
* ``/_lab/ledger``
* ``/_lab/adapters``

Provider endpoints preserve the existing synthetic URL convention:
``/{source}/...``.  A registered source with no matching adapter route returns
a structured 501.  The lab never invents a generic success response.
"""
from __future__ import annotations

import ipaddress
import json
import os
from dataclasses import replace
from datetime import datetime
from typing import Any, Literal, Mapping

from fastapi import Body, FastAPI, Query, Request, WebSocket, WebSocketDisconnect
from pydantic import BaseModel, ConfigDict, Field
from starlette.responses import Response

from lib.shared.env import is_prod

from .adapters import build_lab_adapter_registry, seed_reference_fixtures
from .protocol import AdapterRegistry, ProviderRequest, ProviderResponse
from .runtime import (
    DEFAULT_CLOCK_START,
    InjectedDisconnect,
    LabRuntime,
    QuotaConfiguration,
    QuotaRequirement,
    body_fingerprint,
    isoformat_z,
    sanitize_headers,
    sanitize_query,
)


_ALL_HTTP_METHODS = [
    "DELETE",
    "GET",
    "HEAD",
    "OPTIONS",
    "PATCH",
    "POST",
    "PUT",
]

# Existing single-host endpoint overrides use compact Google path prefixes.
# They are URL aliases only; ledger/source identity remains canonical.
_SOURCE_PATH_ALIASES: dict[str, tuple[str, str]] = {
    "facebook": ("facebook_pages", ""),
    "gcal": ("google_calendar", "/calendar/v3"),
    "gdrive": ("google_drive", "/drive/v3"),
}


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class ClockSet(_StrictModel):
    now: datetime


class ClockAdvance(_StrictModel):
    seconds: float = Field(default=0.0, ge=0, le=31_536_000)
    milliseconds: int = Field(default=0, ge=0, le=31_536_000_000)


class QuotaConfigure(_StrictModel):
    source: str
    scope: str = Field(min_length=1, max_length=256)
    bucket: str = Field(min_length=1, max_length=128)
    limit_id: str = Field(default="default", min_length=1, max_length=128)
    mode: Literal["disabled", "observe", "enforce"] = "enforce"
    capacity: float = Field(gt=0)
    refill_per_second: float = Field(default=0.0, ge=0)
    initial_tokens: float | None = Field(default=None, ge=0)


class FaultCreate(_StrictModel):
    source: str
    action: Literal["response", "malformed_json", "disconnect"] = "response"
    route_id: str | None = None
    scope: str | None = Field(default=None, min_length=1, max_length=256)
    status_code: int = Field(default=503, ge=400, le=599)
    body: Any = None
    headers: dict[str, str] = Field(default_factory=dict)
    after_requests: int = Field(default=0, ge=0)
    every: int = Field(default=1, gt=0)
    max_hits: int | None = Field(default=None, gt=0)
    latency_ms: int = Field(default=0, ge=0, le=86_400_000)
    enabled: bool = True


def _canonical_json(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def _materialize(response: ProviderResponse) -> tuple[Response, bytes]:
    if response.raw_body is not None:
        content = response.raw_body
    else:
        content = _canonical_json(response.json_body)
    headers = dict(response.headers)
    starlette_response = Response(
        content=content,
        status_code=response.status_code,
        headers=headers,
        media_type=response.media_type,
    )
    return starlette_response, content


def _problem(
    *,
    status_code: int,
    code: str,
    message: str,
    details: Mapping[str, Any] | None = None,
    headers: Mapping[str, str] | None = None,
) -> ProviderResponse:
    error: dict[str, Any] = {"code": code, "message": message}
    if details:
        error["details"] = dict(details)
    return ProviderResponse.json(
        {"error": error},
        status_code=status_code,
        headers=headers,
    )


def _quota_requirements(
    *,
    headers: Mapping[str, str],
    scope: str,
    bucket: str,
    default_cost: float,
) -> tuple[QuotaRequirement, ...]:
    """Decode the test-only atomic quota vector used by certification load."""

    raw = headers.get("x-provider-lab-quota-requirements")
    if raw is None:
        return (
            QuotaRequirement(
                scope=scope,
                bucket=bucket,
                cost=default_cost,
            ),
        )
    try:
        decoded = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError(
            "X-Provider-Lab-Quota-Requirements must be valid JSON"
        ) from exc
    if not isinstance(decoded, list) or not 1 <= len(decoded) <= 32:
        raise ValueError(
            "X-Provider-Lab-Quota-Requirements must contain 1..32 items"
        )
    requirements: list[QuotaRequirement] = []
    fields = {"scope", "bucket", "limit_id", "cost"}
    for index, item in enumerate(decoded):
        if not isinstance(item, Mapping) or set(item) != fields:
            raise ValueError(
                "quota requirement "
                f"{index} fields must equal {sorted(fields)}"
            )
        if item["bucket"] != bucket:
            raise ValueError(
                "quota requirement bucket must match the routed quota bucket"
            )
        requirements.append(
            QuotaRequirement(
                scope=item["scope"],  # type: ignore[arg-type]
                bucket=item["bucket"],  # type: ignore[arg-type]
                limit_id=item["limit_id"],  # type: ignore[arg-type]
                cost=item["cost"],  # type: ignore[arg-type]
            )
        )
    return tuple(requirements)


def _client_is_loopback(request: Request) -> bool:
    client = request.client
    # Direct ASGI invocations may omit a network peer. Starlette's TestClient
    # uses the non-network sentinel "testclient".
    if client is None or client.host == "testclient":
        peer_is_loopback = True
    else:
        try:
            peer_is_loopback = ipaddress.ip_address(client.host).is_loopback
        except ValueError:
            peer_is_loopback = False
    if not peer_is_loopback:
        return False

    # A local reverse proxy must not turn the lab into a remote service.
    forwarded = request.headers.get("x-forwarded-for")
    if forwarded:
        for raw_host in forwarded.split(","):
            host = raw_host.strip()
            try:
                if not ipaddress.ip_address(host).is_loopback:
                    return False
            except ValueError:
                return False
    return True


def _websocket_client_is_loopback(websocket: WebSocket) -> bool:
    client = websocket.client
    if client is None or client.host == "testclient":
        peer_is_loopback = True
    else:
        try:
            peer_is_loopback = ipaddress.ip_address(client.host).is_loopback
        except ValueError:
            peer_is_loopback = False
    if not peer_is_loopback:
        return False
    forwarded = websocket.headers.get("x-forwarded-for")
    if not forwarded:
        return True
    for raw_host in forwarded.split(","):
        try:
            if not ipaddress.ip_address(raw_host.strip()).is_loopback:
                return False
        except ValueError:
            return False
    return True


def _validate_fault_headers(headers: Mapping[str, str]) -> None:
    for key, value in headers.items():
        if not key or "\r" in key or "\n" in key:
            raise ValueError("fault response header names must be non-empty")
        if "\r" in value or "\n" in value:
            raise ValueError("fault response header values may not contain newlines")


def build_provider_lab_app(
    *,
    registry: AdapterRegistry | None = None,
    fixtures: Mapping[str, list[Mapping[str, Any]]] | None = None,
    clock_start: datetime = DEFAULT_CLOCK_START,
) -> FastAPI:
    """Build an isolated in-memory lab.

    Each app instance owns its clock, fixture state, quotas, faults, and
    ledger.  Run one process per lab; multiple worker processes would create
    independent control planes.
    """
    if is_prod():
        raise RuntimeError(
            "Provider Lab is test-only and cannot start in production",
        )

    adapter_registry = registry or build_lab_adapter_registry()
    runtime = LabRuntime(adapter_registry, clock_start=clock_start)
    for source, state in seed_reference_fixtures(fixtures or {}).items():
        runtime.seed_source_state(source, state)

    app = FastAPI(
        title="Fyralis Provider Lab",
        version="0.1.0",
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
    )
    app.state.provider_lab = runtime

    @app.middleware("http")
    async def _loopback_only(request: Request, call_next):  # noqa: ANN001
        if not _client_is_loopback(request):
            response, _content = _materialize(
                _problem(
                    status_code=403,
                    code="loopback_only",
                    message="Provider Lab accepts only loopback ASGI/TCP clients",
                )
            )
            return response
        return await call_next(request)

    @app.get("/healthz")
    async def healthz() -> dict[str, Any]:
        return {
            "ok": True,
            "service": "provider-lab",
            "clock": runtime.clock.snapshot(),
        }

    @app.get("/_lab/state")
    async def state() -> dict[str, Any]:
        return runtime.snapshot()

    @app.post("/_lab/reset")
    async def reset() -> dict[str, Any]:
        return runtime.reset_all()

    @app.get("/_lab/adapters")
    async def adapters() -> dict[str, Any]:
        inventory = runtime.registry.inventory()
        return {
            "count": len(inventory),
            "implemented_count": sum(
                1 for item in inventory if item["implemented"]
            ),
            "adapters": inventory,
        }

    @app.websocket("/discord/gateway")
    async def discord_gateway(websocket: WebSocket) -> None:
        """Emulate the exact Discord Gateway operations Fyralis consumes.

        The lab sends HELLO, accepts IDENTIFY or RESUME, sends READY/RESUMED,
        replays configured dispatches after the acknowledged sequence, and
        answers HEARTBEAT with HEARTBEAT_ACK. It intentionally does not
        implement the rest of Discord's gateway protocol.
        """

        if not _websocket_client_is_loopback(websocket):
            await websocket.close(code=4403, reason="loopback only")
            return
        await websocket.accept()
        state = runtime.get_source_state("discord")
        websocket_host = websocket.headers.get("host", "127.0.0.1:9191")
        resume_gateway_url = (
            f"ws://{websocket_host}/discord/gateway"
        )
        heartbeat_interval = int(
            state.get("gateway_heartbeat_interval_ms", 1_000)
        )
        session_id = str(
            state.get("gateway_session_id", "provider-lab-session")
        )
        raw_events = state.get("gateway_events") or []
        events: list[dict[str, Any]] = []
        for index, raw_event in enumerate(raw_events, start=1):
            if not isinstance(raw_event, Mapping):
                continue
            event = dict(raw_event)
            event.setdefault("op", 0)
            event.setdefault("s", index)
            event.setdefault("t", "MESSAGE_CREATE")
            event.setdefault("d", {})
            events.append(event)
        events.sort(key=lambda event: int(event.get("s") or 0))

        await websocket.send_json(
            {"op": 10, "d": {"heartbeat_interval": heartbeat_interval}}
        )
        last_replayed_sequence = 0
        outcome = "client_disconnected"
        try:
            first = await websocket.receive_json()
            opcode = first.get("op")
            data = first.get("d") or {}
            token = str(data.get("token") or "")
            if not token.startswith(("lab-discord::", "spam-bot::")):
                outcome = "invalid_token"
                await websocket.close(code=4004, reason="authentication failed")
                return

            if opcode == 2:
                ready_sequence = 0
                await websocket.send_json(
                    {
                        "op": 0,
                        "s": ready_sequence,
                        "t": "READY",
                        "d": {
                            "session_id": session_id,
                            "resume_gateway_url": resume_gateway_url,
                            "application": {
                                "id": str(
                                    state.get(
                                        "gateway_application_id",
                                        "provider-lab-application",
                                    )
                                )
                            },
                        },
                    }
                )
            elif opcode == 6:
                if str(data.get("session_id") or "") != session_id:
                    outcome = "invalid_session"
                    await websocket.send_json({"op": 9, "d": False})
                    await websocket.close(code=4009, reason="session timed out")
                    return
                last_replayed_sequence = int(data.get("seq") or 0)
                await websocket.send_json(
                    {
                        "op": 0,
                        "s": last_replayed_sequence,
                        "t": "RESUMED",
                        "d": {},
                    }
                )
            else:
                outcome = "unsupported_opcode"
                await websocket.send_json({"op": 9, "d": False})
                await websocket.close(code=4002, reason="decode error")
                return

            for event in events:
                sequence = int(event.get("s") or 0)
                if sequence > last_replayed_sequence:
                    await websocket.send_json(event)

            while True:
                frame = await websocket.receive_json()
                opcode = frame.get("op")
                if opcode == 1:
                    await websocket.send_json({"op": 11, "d": None})
                elif opcode == 6:
                    data = frame.get("d") or {}
                    if str(data.get("session_id") or "") != session_id:
                        await websocket.send_json({"op": 9, "d": False})
                        outcome = "invalid_session"
                        await websocket.close(
                            code=4009,
                            reason="session timed out",
                        )
                        return
                    await websocket.send_json(
                        {
                            "op": 0,
                            "s": int(data.get("seq") or 0),
                            "t": "RESUMED",
                            "d": {},
                        }
                    )
                else:
                    await websocket.send_json({"op": 9, "d": False})
        except WebSocketDisconnect:
            pass
        finally:
            runtime.ledger.append(
                {
                    "started_at": isoformat_z(runtime.clock.now()),
                    "completed_at": isoformat_z(runtime.clock.now()),
                    "method": "WEBSOCKET",
                    "source": "discord",
                    "route_id": "discord.gateway",
                    "scope": None,
                    "path": "/discord/gateway",
                    "query": {},
                    "headers": {},
                    "request_body": None,
                    "response_body": None,
                    "status_code": 101,
                    "outcome": outcome,
                    "quota": None,
                    "fault_id": None,
                }
            )

    @app.get("/_lab/sources/{source}/state")
    async def source_state(source: str) -> Response:
        if runtime.registry.get(source) is None:
            return _materialize(
                _problem(
                    status_code=404,
                    code="unknown_source",
                    message=f"Source {source!r} is not registered",
                )
            )[0]
        response, _ = _materialize(
            ProviderResponse.json(runtime.source_state_snapshot(source))
        )
        return response

    @app.put("/_lab/sources/{source}/state")
    async def replace_source_state(
        source: str,
        state_body: dict[str, Any] = Body(...),
    ) -> Response:
        if runtime.registry.get(source) is None:
            return _materialize(
                _problem(
                    status_code=404,
                    code="unknown_source",
                    message=f"Source {source!r} is not registered",
                )
            )[0]
        response, _ = _materialize(
            ProviderResponse.json(runtime.set_source_state(source, state_body))
        )
        return response

    @app.delete("/_lab/sources/{source}/state")
    async def reset_source_state(source: str) -> Response:
        if runtime.registry.get(source) is None:
            return _materialize(
                _problem(
                    status_code=404,
                    code="unknown_source",
                    message=f"Source {source!r} is not registered",
                )
            )[0]
        response, _ = _materialize(
            ProviderResponse.json(runtime.reset_source_state(source))
        )
        return response

    @app.get("/_lab/clock")
    async def get_clock() -> dict[str, Any]:
        return runtime.clock.snapshot()

    @app.put("/_lab/clock")
    async def set_clock(command: ClockSet) -> Response:
        try:
            now = runtime.clock.set(command.now)
        except ValueError as exc:
            return _materialize(
                _problem(
                    status_code=409,
                    code="clock_rewind_forbidden",
                    message=str(exc),
                )
            )[0]
        return _materialize(
            ProviderResponse.json({"now": isoformat_z(now)})
        )[0]

    @app.post("/_lab/clock/advance")
    async def advance_clock(command: ClockAdvance) -> dict[str, Any]:
        now = runtime.clock.advance(
            seconds=command.seconds,
            milliseconds=command.milliseconds,
        )
        return {"now": isoformat_z(now)}

    @app.get("/_lab/quotas")
    async def list_quotas() -> dict[str, Any]:
        buckets = runtime.quotas.snapshot()
        return {"count": len(buckets), "buckets": buckets}

    @app.post("/_lab/quotas")
    async def configure_quota(command: QuotaConfigure) -> Response:
        if runtime.registry.get(command.source) is None:
            return _materialize(
                _problem(
                    status_code=404,
                    code="unknown_source",
                    message=f"Source {command.source!r} is not registered",
                )
            )[0]
        try:
            configured = runtime.quotas.configure(
                QuotaConfiguration(
                    source=command.source,
                    scope=command.scope,
                    bucket=command.bucket,
                    limit_id=command.limit_id,
                    mode=command.mode,
                    capacity=command.capacity,
                    refill_per_second=command.refill_per_second,
                    initial_tokens=command.initial_tokens,
                )
            )
        except ValueError as exc:
            return _materialize(
                _problem(
                    status_code=422,
                    code="invalid_quota",
                    message=str(exc),
                )
            )[0]
        return _materialize(
            ProviderResponse.json(configured, status_code=201)
        )[0]

    @app.delete("/_lab/quotas/{source}/{bucket}")
    async def delete_quota(
        source: str,
        bucket: str,
        scope: str = Query(..., min_length=1, max_length=256),
        limit_id: str = Query(default="default", min_length=1, max_length=128),
    ) -> Response:
        removed = runtime.quotas.remove(
            source,
            scope,
            bucket,
            limit_id,
        )
        if not removed:
            return _materialize(
                _problem(
                    status_code=404,
                    code="quota_not_found",
                    message="No matching scoped token bucket",
                )
            )[0]
        return Response(status_code=204)

    @app.get("/_lab/faults")
    async def list_faults() -> dict[str, Any]:
        rules = runtime.faults.snapshot()
        return {"count": len(rules), "rules": rules}

    @app.post("/_lab/faults")
    async def create_fault(command: FaultCreate) -> Response:
        adapter = runtime.registry.get(command.source)
        if adapter is None:
            return _materialize(
                _problem(
                    status_code=404,
                    code="unknown_source",
                    message=f"Source {command.source!r} is not registered",
                )
            )[0]
        if (
            command.route_id is not None
            and command.route_id not in {route.route_id for route in adapter.routes}
        ):
            return _materialize(
                _problem(
                    status_code=422,
                    code="unknown_route_id",
                    message=(
                        f"Route {command.route_id!r} is not declared by "
                        f"{command.source!r}"
                    ),
                )
            )[0]
        try:
            _validate_fault_headers(command.headers)
            rule = runtime.faults.create(**command.model_dump())
        except (TypeError, ValueError) as exc:
            return _materialize(
                _problem(
                    status_code=422,
                    code="invalid_fault",
                    message=str(exc),
                )
            )[0]
        return _materialize(
            ProviderResponse.json(rule, status_code=201)
        )[0]

    @app.delete("/_lab/faults/{rule_id}")
    async def delete_fault(rule_id: str) -> Response:
        if not runtime.faults.remove(rule_id):
            return _materialize(
                _problem(
                    status_code=404,
                    code="fault_not_found",
                    message=f"Fault rule {rule_id!r} does not exist",
                )
            )[0]
        return Response(status_code=204)

    @app.get("/_lab/ledger")
    async def get_ledger(
        source: str | None = None,
        scope: str | None = None,
        route_id: str | None = None,
        outcome: str | None = None,
        after_id: int = Query(default=0, ge=0),
        limit: int = Query(default=100, ge=1, le=1_000),
    ) -> dict[str, Any]:
        entries = runtime.ledger.list(
            source=source,
            scope=scope,
            route_id=route_id,
            outcome=outcome,
            after_id=after_id,
            limit=limit,
        )
        return {
            "count": len(entries),
            "entries": entries,
            "next_after_id": entries[-1]["request_id"] if entries else after_id,
        }

    @app.delete("/_lab/ledger")
    async def clear_ledger() -> Response:
        runtime.ledger.clear()
        return Response(status_code=204)

    async def _dispatch(
        request: Request, source: str, provider_path: str
    ) -> Response:
        started_at = runtime.clock.now()
        body = await request.body()
        path = "/" + provider_path.lstrip("/") if provider_path else "/"
        alias = _SOURCE_PATH_ALIASES.get(source)
        if alias is not None:
            source, api_prefix = alias
            if path.startswith(api_prefix + "/"):
                path = path[len(api_prefix):]
        query_items = tuple(request.query_params.multi_items())
        headers = {key.lower(): value for key, value in request.headers.items()}

        adapter = runtime.registry.get(source)
        if adapter is None:
            provider_response = _problem(
                status_code=404,
                code="unknown_source",
                message=f"Source {source!r} is not registered",
            )
            return _complete_request(
                runtime=runtime,
                request=request,
                source=source,
                route_id=None,
                scope=None,
                path=path,
                query_items=query_items,
                headers=headers,
                request_body=body,
                started_at=started_at,
                provider_response=provider_response,
                outcome="unsupported",
            )

        matched = runtime.registry.match(source, request.method, path)
        if matched is None:
            provider_response = _problem(
                status_code=501,
                code="unsupported_provider_route",
                message=(
                    f"{request.method.upper()} /{source}{path} is not "
                    "implemented by this Provider Lab adapter"
                ),
                details={"source_registered": True},
            )
            return _complete_request(
                runtime=runtime,
                request=request,
                source=source,
                route_id=None,
                scope=None,
                path=path,
                query_items=query_items,
                headers=headers,
                request_body=body,
                started_at=started_at,
                provider_response=provider_response,
                outcome="unsupported",
            )

        matched_adapter, route, path_params = matched
        source_state = runtime.get_source_state(source)
        preliminary = ProviderRequest(
            source=source,
            route=route,
            method=request.method.upper(),
            path=path,
            url=str(request.url),
            path_params=path_params,
            query_items=query_items,
            headers=headers,
            body=body,
            scope="global",
            source_state=source_state,
        )
        try:
            scope = matched_adapter.resolve_scope(preliminary)
            if not isinstance(scope, str) or not 1 <= len(scope) <= 256:
                raise ValueError("adapter scope must contain 1..256 characters")
        except (TypeError, ValueError) as exc:
            provider_response = _problem(
                status_code=500,
                code="adapter_scope_error",
                message=str(exc),
            )
            return _complete_request(
                runtime=runtime,
                request=request,
                source=source,
                route_id=route.route_id,
                scope=None,
                path=path,
                query_items=query_items,
                headers=headers,
                request_body=body,
                started_at=started_at,
                provider_response=provider_response,
                outcome="adapter_error",
            )
        provider_request = replace(preliminary, scope=scope)

        quota = None
        if route.quota_bucket is not None:
            try:
                quota_requirements = _quota_requirements(
                    headers=headers,
                    scope=scope,
                    bucket=route.quota_bucket,
                    default_cost=route.quota_cost,
                )
                quota = runtime.quotas.check_many(
                    source=source,
                    requirements=quota_requirements,
                )
            except (TypeError, ValueError) as exc:
                provider_response = _problem(
                    status_code=400,
                    code="invalid_quota_requirements",
                    message=str(exc),
                )
                return _complete_request(
                    runtime=runtime,
                    request=request,
                    source=source,
                    route_id=route.route_id,
                    scope=scope,
                    path=path,
                    query_items=query_items,
                    headers=headers,
                    request_body=body,
                    started_at=started_at,
                    provider_response=provider_response,
                    outcome="invalid_quota_requirements",
                )
            if not quota.allowed:
                provider_response = _problem(
                    status_code=429,
                    code="quota_exceeded",
                    message="Provider Lab scoped token bucket is exhausted",
                    details={
                        "source": source,
                        "scope": scope,
                        "bucket": route.quota_bucket,
                        "constraint_count": len(quota_requirements),
                    },
                    headers=quota.headers,
                )
                return _complete_request(
                    runtime=runtime,
                    request=request,
                    source=source,
                    route_id=route.route_id,
                    scope=scope,
                    path=path,
                    query_items=query_items,
                    headers=headers,
                    request_body=body,
                    started_at=started_at,
                    provider_response=provider_response,
                    outcome="quota_limited",
                    quota=quota,
                )

        fault = runtime.faults.evaluate(
            source=source,
            route_id=route.route_id,
            scope=scope,
        )
        if fault is not None:
            if fault.latency_ms:
                runtime.clock.advance(milliseconds=fault.latency_ms)
            if fault.action == "disconnect":
                _record_request(
                    runtime=runtime,
                    request=request,
                    source=source,
                    route_id=route.route_id,
                    scope=scope,
                    path=path,
                    query_items=query_items,
                    headers=headers,
                    request_body=body,
                    started_at=started_at,
                    response_body=b"",
                    status_code=None,
                    outcome="fault_disconnect",
                    quota=quota,
                    fault_id=fault.rule_id,
                )
                raise InjectedDisconnect(
                    f"Provider Lab disconnect injected by {fault.rule_id}"
                )
            if fault.action == "malformed_json":
                provider_response = ProviderResponse(
                    status_code=fault.status_code,
                    raw_body=b'{"provider_lab_fault":',
                    headers=fault.headers,
                    media_type="application/json",
                )
            else:
                fault_body = (
                    fault.body
                    if fault.body is not None
                    else {
                        "error": {
                            "code": "injected_fault",
                            "rule_id": fault.rule_id,
                        }
                    }
                )
                provider_response = ProviderResponse.json(
                    fault_body,
                    status_code=fault.status_code,
                    headers=fault.headers,
                )
            return _complete_request(
                runtime=runtime,
                request=request,
                source=source,
                route_id=route.route_id,
                scope=scope,
                path=path,
                query_items=query_items,
                headers=headers,
                request_body=body,
                started_at=started_at,
                provider_response=provider_response,
                outcome="fault_response",
                quota=quota,
                fault_id=fault.rule_id,
            )

        try:
            provider_response = await matched_adapter.handle(provider_request)
        except Exception:  # noqa: BLE001 - provider simulator error boundary
            provider_response = _problem(
                status_code=500,
                code="adapter_error",
                message=(
                    f"Provider Lab adapter {source!r} failed while handling "
                    f"{route.route_id!r}"
                ),
            )
            outcome = "adapter_error"
        else:
            outcome = "provider_response"

        if quota is not None and quota.headers:
            provider_response = replace(
                provider_response,
                headers={**provider_response.headers, **quota.headers},
            )
        return _complete_request(
            runtime=runtime,
            request=request,
            source=source,
            route_id=route.route_id,
            scope=scope,
            path=path,
            query_items=query_items,
            headers=headers,
            request_body=body,
            started_at=started_at,
            provider_response=provider_response,
            outcome=outcome,
            quota=quota,
        )

    @app.api_route("/{source}", methods=_ALL_HTTP_METHODS)
    async def dispatch_source_root(request: Request, source: str) -> Response:
        return await _dispatch(request, source, "")

    @app.api_route("/{source}/{provider_path:path}", methods=_ALL_HTTP_METHODS)
    async def dispatch_provider(
        request: Request, source: str, provider_path: str
    ) -> Response:
        return await _dispatch(request, source, provider_path)

    return app


def _complete_request(
    *,
    runtime: LabRuntime,
    request: Request,
    source: str,
    route_id: str | None,
    scope: str | None,
    path: str,
    query_items: tuple[tuple[str, str], ...],
    headers: Mapping[str, str],
    request_body: bytes,
    started_at: datetime,
    provider_response: ProviderResponse,
    outcome: str,
    quota: Any = None,
    fault_id: str | None = None,
) -> Response:
    response, response_body = _materialize(provider_response)
    _record_request(
        runtime=runtime,
        request=request,
        source=source,
        route_id=route_id,
        scope=scope,
        path=path,
        query_items=query_items,
        headers=headers,
        request_body=request_body,
        started_at=started_at,
        response_body=response_body,
        status_code=provider_response.status_code,
        outcome=outcome,
        quota=quota,
        fault_id=fault_id,
    )
    return response


def _record_request(
    *,
    runtime: LabRuntime,
    request: Request,
    source: str,
    route_id: str | None,
    scope: str | None,
    path: str,
    query_items: tuple[tuple[str, str], ...],
    headers: Mapping[str, str],
    request_body: bytes,
    started_at: datetime,
    response_body: bytes,
    status_code: int | None,
    outcome: str,
    quota: Any = None,
    fault_id: str | None = None,
) -> None:
    quota_entry = None
    if quota is not None:
        quota_entry = {
            "configured": quota.configured,
            "mode": quota.mode,
            "would_limit": quota.would_limit,
            "remaining": quota.remaining,
        }
    runtime.ledger.append(
        {
            "started_at": isoformat_z(started_at),
            "completed_at": isoformat_z(runtime.clock.now()),
            "method": request.method.upper(),
            "source": source,
            "route_id": route_id,
            "scope": scope,
            "path": path,
            "query": sanitize_query(query_items),
            "headers": sanitize_headers(headers),
            "request_body": body_fingerprint(request_body),
            "response_body": body_fingerprint(response_body),
            "status_code": status_code,
            "outcome": outcome,
            "quota": quota_entry,
            "fault_id": fault_id,
        }
    )


def main() -> None:
    """Run one deterministic lab process, bound only to IPv4 loopback."""

    import uvicorn

    port = int(os.environ.get("PROVIDER_LAB_PORT", "9191"))
    uvicorn.run(
        build_provider_lab_app(),
        host="127.0.0.1",
        port=port,
        workers=1,
        log_level=os.environ.get("PROVIDER_LAB_LOG_LEVEL", "warning"),
    )


__all__ = ["build_provider_lab_app", "main"]


if __name__ == "__main__":
    main()
