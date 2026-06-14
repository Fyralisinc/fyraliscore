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
from urllib.parse import parse_qs

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from services.app.gateway.deps import get_gateway_deps
from services.platform.extensions.identity import (
    ExtensionOAuthClientsRepo, ExtensionPrincipal, IdentityError,
    mint_access_token, verify_access_token,
)


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

        repo = ExtensionOAuthClientsRepo(get_gateway_deps(request).pool)
        principal = await repo.verify_credentials(client_id, client_secret)
        if principal is None:
            return JSONResponse({"error": "invalid_client"}, status_code=401)

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

    return router


__all__ = ["build_extension_router", "require_extension", "ExtensionAuthError"]
