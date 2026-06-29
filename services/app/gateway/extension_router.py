"""services/app/gateway/extension_router.py — the extension-facing data plane (/ext).

Developer-hosted extensions authenticate here and (in later milestones) read the
filtered stream + post edge observations. ADR-0004 DP1.4 / E3.

This milestone (M1) ships identity:
  - ``POST /ext/oauth/token`` — OAuth2 ``client_credentials`` grant → short-lived
    bearer JWT (HTTP Basic *or* form-body client_id/client_secret).
  - ``require_extension(request)`` — the shared auth dependency every later
    ``/ext`` route uses to resolve the calling :class:`ExtensionPrincipal` from the
    bearer token (401 on missing/invalid/expired).

Read/egress/edge-ingest routes are added to this same router in M2–M4.
"""
from __future__ import annotations

import base64
import binascii
import json
from datetime import datetime
from urllib.parse import parse_qs
from uuid import UUID

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from lib.extensions.host_api.v1 import CapabilityError
from services.app.gateway.deps import get_gateway_deps
from services.platform.extensions.audit import AuditLog
from services.platform.extensions.grants import ExtensionGrantsRepo
from services.platform.extensions.killswitch import KillSwitch
from services.platform.extensions.identity import (
    ExtensionOAuthClientsRepo, ExtensionPrincipal, IdentityError,
    mint_access_token, verify_access_token,
)
from services.platform.extensions.substrate_reader import CapabilityScopedReader


class ExtensionAuthError(Exception):
    """Raised by require_extension; carries the OAuth error code + status."""

    def __init__(self, code: str, status: int = 401) -> None:
        super().__init__(code)
        self.code = code
        self.status = status


def _bearer(request: Request) -> str | None:
    auth = request.headers.get("authorization", "")
    if auth.lower().startswith("bearer "):
        return auth[7:].strip()
    return None


def require_extension(request: Request) -> ExtensionPrincipal:
    """Resolve the calling extension from its bearer token, or raise
    :class:`ExtensionAuthError` (turned into a 401 by the route)."""
    token = _bearer(request)
    if not token:
        raise ExtensionAuthError("missing_bearer_token")
    try:
        return verify_access_token(token)
    except IdentityError as exc:
        raise ExtensionAuthError("invalid_token") from exc


_TENANT_HEADER = "x-fyralis-tenant"


def _tenant_id(request: Request) -> UUID:
    raw = request.headers.get(_TENANT_HEADER, "").strip()
    if not raw:
        raise ExtensionAuthError("missing_tenant_header", status=400)
    try:
        return UUID(raw)
    except ValueError as exc:
        raise ExtensionAuthError("invalid_tenant_header", status=400) from exc


async def _authz_grant(request: Request):
    """Authenticate the extension + resolve the target tenant's ACTIVE grant.
    Returns (principal, tenant_id, grant). Raises ExtensionAuthError on failure."""
    principal = require_extension(request)
    tenant_id = _tenant_id(request)
    pool = get_gateway_deps(request).pool
    if await KillSwitch(pool).is_killed(principal.extension_id):
        raise ExtensionAuthError("extension_disabled", status=403)
    grant = await ExtensionGrantsRepo(pool).get(tenant_id=tenant_id, extension_id=principal.extension_id)
    if grant is None:
        raise ExtensionAuthError("no_active_grant_for_tenant", status=403)
    return principal, tenant_id, grant


async def _reader_for_request(request: Request) -> tuple[ExtensionPrincipal, UUID, CapabilityScopedReader]:
    """Auth + grant, then build a capability-scoped reader from it."""
    principal, tenant_id, grant = await _authz_grant(request)
    pool = get_gateway_deps(request).pool
    reader = CapabilityScopedReader(pool=pool, tenant_id=tenant_id, capabilities=grant.capabilities)
    return principal, tenant_id, reader


def _obs_json(view) -> dict:
    return {
        "id": str(view.id), "tenant_id": str(view.tenant_id),
        "occurred_at": view.occurred_at.isoformat() if isinstance(view.occurred_at, datetime) else view.occurred_at,
        "kind": view.kind, "source_channel": view.source_channel,
        "content": view.content, "content_text": view.content_text,
        "trust_tier": view.trust_tier, "external_id": view.external_id,
        "entities_mentioned": view.entities_mentioned,
    }


def _model_json(view) -> dict:
    out = {}
    for k, v in vars(view).items():
        out[k] = (str(v) if isinstance(v, UUID)
                  else v.isoformat() if isinstance(v, datetime) else v)
    return out


def _basic_credentials(request: Request) -> tuple[str, str] | None:
    auth = request.headers.get("authorization", "")
    if not auth.lower().startswith("basic "):
        return None
    try:
        raw = base64.b64decode(auth[6:].strip()).decode("utf-8")
        cid, _, secret = raw.partition(":")
        return (cid, secret) if cid else None
    except (binascii.Error, UnicodeDecodeError):
        return None


def build_extension_router() -> APIRouter:
    router = APIRouter(prefix="/ext", tags=["extensions"])

    @router.post("/oauth/token")
    async def issue_token(request: Request):
        """OAuth2 client_credentials grant. Accepts HTTP Basic or a form/JSON body.

        Body is parsed manually (urlencoded or JSON) to avoid a python-multipart
        dependency on the host for this one endpoint."""
        form: dict[str, str] = {}
        body = await request.body()
        ctype = request.headers.get("content-type", "")
        if body:
            if "application/json" in ctype:
                try:
                    form = {k: str(v) for k, v in (json.loads(body) or {}).items()}
                except (ValueError, AttributeError):
                    form = {}
            else:
                form = {k: v[0] for k, v in parse_qs(body.decode("utf-8", "replace")).items()}
        grant_type = (form.get("grant_type") or "client_credentials").strip()
        if grant_type != "client_credentials":
            return JSONResponse({"error": "unsupported_grant_type"}, status_code=400)

        creds = _basic_credentials(request)
        client_id = (creds[0] if creds else form.get("client_id") or "").strip()
        client_secret = creds[1] if creds else (form.get("client_secret") or "")
        if not client_id or not client_secret:
            return JSONResponse({"error": "invalid_request"}, status_code=400)

        pool = get_gateway_deps(request).pool
        principal = await ExtensionOAuthClientsRepo(pool).verify_credentials(client_id, client_secret)
        if principal is None:
            return JSONResponse({"error": "invalid_client"}, status_code=401)
        if await KillSwitch(pool).is_killed(principal.extension_id):
            return JSONResponse({"error": "extension_disabled"}, status_code=403)

        return JSONResponse(
            mint_access_token(principal.extension_id, environment=principal.environment)
        )

    @router.get("/whoami")
    async def whoami(request: Request):
        """Echo the authenticated principal — a trivial token-verification probe."""
        try:
            principal = require_extension(request)
        except ExtensionAuthError as exc:
            return JSONResponse({"error": exc.code}, status_code=exc.status)
        return JSONResponse(
            {"extension_id": principal.extension_id, "environment": principal.environment}
        )

    # ---- read-API (M2): SubstrateReader over HTTP --------------------------------
    # All routes require a bearer token (the extension) + X-Fyralis-Tenant (the
    # tenant whose data is requested) + an ACTIVE grant for that pair. Reads run
    # under the fyralis_ext_readonly role + RLS; views are baseline-redacted.

    @router.get("/v1/observations")
    async def list_observations(request: Request):
        try:
            principal, tenant_id, reader = await _reader_for_request(request)
            channel = request.query_params.get("channel") or None
            since_raw = request.query_params.get("since")
            since = datetime.fromisoformat(since_raw) if since_raw else None
            limit = int(request.query_params.get("limit", "100"))
            views = await reader.query_observations(channel=channel, since=since, limit=limit)
        except ExtensionAuthError as exc:
            return JSONResponse({"error": exc.code}, status_code=exc.status)
        except CapabilityError:
            return JSONResponse({"error": "capability_denied"}, status_code=403)
        except ValueError:
            return JSONResponse({"error": "invalid_query"}, status_code=400)
        await AuditLog(get_gateway_deps(request).pool).record(
            extension_id=principal.extension_id, action="read_observations",
            tenant_id=tenant_id, item_count=len(views), detail={"channel": channel})
        return JSONResponse({"observations": [_obs_json(v) for v in views]})

    @router.get("/v1/observations/{observation_id}")
    async def get_observation(request: Request, observation_id: str):
        try:
            _, _, reader = await _reader_for_request(request)
            view = await reader.get_observation(UUID(observation_id))
        except ExtensionAuthError as exc:
            return JSONResponse({"error": exc.code}, status_code=exc.status)
        except CapabilityError:
            return JSONResponse({"error": "capability_denied"}, status_code=403)
        except ValueError:
            return JSONResponse({"error": "invalid_observation_id"}, status_code=400)
        if view is None:
            return JSONResponse({"error": "not_found"}, status_code=404)
        return JSONResponse(_obs_json(view))

    @router.post("/v1/ingest")
    async def ingest(request: Request):
        """Edge-ingest a derived observation (E3.2). Requires bearer +
        X-Fyralis-Tenant + an active grant with write_observations. The stored
        trust tier is capped at the grant ceiling (reject-not-downgrade); the
        channel is host-namespaced ``ext:<id>:<sub>``."""
        from services.platform.extensions.edge_ingest import edge_ingest, EdgeIngestError
        try:
            principal, tenant_id, grant = await _authz_grant(request)
            body = await request.body()
            payload = json.loads(body) if body else {}
            if not isinstance(payload, dict):
                raise EdgeIngestError("invalid_body", 400)
        except ExtensionAuthError as exc:
            return JSONResponse({"error": exc.code}, status_code=exc.status)
        except (ValueError, EdgeIngestError) as exc:
            code = exc.code if isinstance(exc, EdgeIngestError) else "invalid_json"
            status = exc.status if isinstance(exc, EdgeIngestError) else 400
            return JSONResponse({"error": code}, status_code=status)

        deps = get_gateway_deps(request)
        # Per-extension rate limit (best-effort; skipped if no limiter wired).
        limiter = getattr(deps, "rate_limiter", None)
        if limiter is not None:
            try:
                allowed = await limiter.consume(("ext_ingest", principal.extension_id))
                if allowed is False:
                    return JSONResponse({"error": "rate_limited"}, status_code=429)
            except Exception:  # noqa: BLE001 — limiter optional; never block on it
                pass
        try:
            ack = await edge_ingest(
                deps.pool, extension_id=principal.extension_id, tenant_id=tenant_id,
                trust_ceiling=grant.trust_ceiling,
                can_write=grant.capabilities.write_observations,
                sub_channel=payload.get("channel", ""),
                content=payload.get("content", {}),
                content_text=payload.get("content_text", ""),
                external_id=payload.get("external_id"),
                requested_trust_tier=payload.get("trust_tier"),
                occurred_at=payload.get("occurred_at"),
                deps=deps,
            )
        except EdgeIngestError as exc:
            return JSONResponse({"error": exc.code}, status_code=exc.status)
        await AuditLog(deps.pool).record(
            extension_id=principal.extension_id, action="edge_ingest", tenant_id=tenant_id,
            item_count=1, detail={"source_channel": ack["source_channel"],
                                  "trust_tier": ack["trust_tier"], "deduped": ack["deduped"]})
        return JSONResponse(ack)

    @router.get("/v1/stream")
    async def stream(request: Request):
        """Cursor pull of the capability-filtered, redacted egress feed (E3.1).

        Returns items already projected for this (extension, tenant) by the egress
        plane, newest-after-cursor. Pass the returned ``cursor`` back to page
        forward. Requires bearer + X-Fyralis-Tenant + an active grant."""
        from services.platform.extensions.egress.store import EgressStore
        try:
            principal, tenant_id, _ = await _reader_for_request(request)
            cursor = int(request.query_params.get("cursor", "0"))
            limit = int(request.query_params.get("limit", "100"))
        except ExtensionAuthError as exc:
            return JSONResponse({"error": exc.code}, status_code=exc.status)
        except ValueError:
            return JSONResponse({"error": "invalid_cursor"}, status_code=400)
        pool = get_gateway_deps(request).pool
        store = EgressStore(pool)
        items, next_cursor = await store.read(
            extension_id=principal.extension_id, tenant_id=tenant_id,
            after_seq=cursor, limit=limit,
        )
        if items:
            await AuditLog(pool).record(
                extension_id=principal.extension_id, action="stream_pull",
                tenant_id=tenant_id, item_count=len(items), detail={"cursor": next_cursor})
        return JSONResponse({"items": items, "cursor": next_cursor})

    @router.get("/v1/models/{model_id}")
    async def get_model(request: Request, model_id: str):
        try:
            _, _, reader = await _reader_for_request(request)
            view = await reader.get_model(UUID(model_id))
        except ExtensionAuthError as exc:
            return JSONResponse({"error": exc.code}, status_code=exc.status)
        except CapabilityError:
            return JSONResponse({"error": "capability_denied"}, status_code=403)
        except ValueError:
            return JSONResponse({"error": "invalid_model_id"}, status_code=400)
        if view is None:
            return JSONResponse({"error": "not_found"}, status_code=404)
        return JSONResponse(_model_json(view))

    return router


__all__ = ["build_extension_router", "require_extension", "ExtensionAuthError"]
