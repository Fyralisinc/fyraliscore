"""OAuth-first Figma connection endpoints.

Each customer BYOC deployment owns one Figma OAuth app. The customer journey
is intentionally file-scoped: the browser supplies known Figma file URLs, then
the server performs the OAuth authorization-code exchange, validates those
exact files, persists encrypted token references, and queues the initial sync.
Raw access tokens, refresh tokens, authorization codes, and PKCE verifier
plaintext never leave the customer-cloud process.

Routes:

* ``POST /integrations/figma/oauth/start`` — authenticated; creates a
  single-use state record and returns Figma's top-level authorization URL.
* ``GET /integrations/figma/oauth/callback`` — public callback; consumes the
  state, exchanges the 30-second code, validates selected files and finalizes.
* ``GET /integrations/figma/connect/status`` — authenticated UI status.
* ``POST /integrations/figma/connect/retry`` — requeues the initial sync.
* ``DELETE /integrations/figma/connect`` — disables the installation and
  deletes local credential material.

Legacy PAT preflight/finalize routes remain below as an operator-only fallback.
They are deliberately separate from the OAuth path so the normal onboarding UI
never handles a Figma token directly.
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import re
import secrets
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.parse import parse_qsl, urlencode, unquote, urlsplit, urlunsplit
from uuid import UUID

import asyncpg
import httpx
import structlog
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse, RedirectResponse, Response

from lib.shared.errors import FigmaApiError, SecretStoreError, StateTokenInvalidError
from lib.shared.ids import uuid7
from lib.shared.provider_transport import (
    ProviderRateLimited,
    ProviderTimeoutError,
    ProviderTransientError,
    RequestPolicy,
    parse_retry_after,
)
from lib.shared.secrets import load_app_secret_text_from_env
from lib.shared.tenant_context import tenant_transaction
from services.ingest.integrations.base_url_policy import native_connect_base_url
from services.ingest.integrations.figma.artifact_router import router as artifact_router
from services.ingest.integrations.figma.client import FigmaClient
from services.ingest.integrations.figma.onboarding import (
    finalize_install,
    register_webhook_installation,
)
from services.ingest.integrations.provider_transport import (
    PolicyResolver,
    ProviderExecutor,
    ProviderRequestBinding,
    QuotaResolver,
    explicit_local_transport,
    tenant_preinstall_transport_kwargs,
)
from services.platform.access_control.roles import has_role


log = structlog.get_logger("integrations.figma.oauth")


_FIGMA_AUTHORIZE_URL = "https://www.figma.com/oauth"
_FIGMA_TOKEN_URL = "https://api.figma.com/v1/oauth/token"
_DEFAULT_STATE_TTL_S = 600
_DEFAULT_RETURN_PATH = "/onboarding"
_DEFAULT_TOKEN_TTL_S = 90 * 24 * 60 * 60
_REFRESH_SKEW_S = 5 * 60
_MAX_SELECTED_FILES = 100
_FILE_URL_KINDS = frozenset({"file", "design", "proto", "board", "slides"})
_FILE_KEY_RE = re.compile(r"[A-Za-z0-9_-]{6,256}\Z")
_DEPLOYMENT_SETUP_REQUIRED_STATE = "deployment_setup_required"
_DEPLOYMENT_SETUP_OWNER = "deployment_admin"
_DEPLOYMENT_SETUP_ACTION = (
    "Ask a deployment administrator to configure this deployment's Figma OAuth app."
)
_FIGMA_OAUTH_CALLBACK_PATH = "/integrations/figma/oauth/callback"
_FIGMA_DEVELOPER_APPS_URL = "https://www.figma.com/developers/apps"

# Deliberately keep this equal to the API surface we ship today.  Snapshot
# ingestion reads `/me`, document metadata/content, comments, and versions;
# it does not yet fetch dev resources or provision webhooks.
_DEFAULT_SCOPES = (
    "current_user:read",
    "file_metadata:read",
    "file_content:read",
    "file_comments:read",
    "file_versions:read",
)
_SUPPORTED_SCOPES = frozenset(_DEFAULT_SCOPES)


router = APIRouter(prefix="/integrations/figma", tags=["figma"])
# Artifact retrieval is separate from OAuth state/token code but shares this
# source prefix with the onboarding status API.
router.include_router(artifact_router)
admin_router = APIRouter(
    prefix="/api/admin/integrations/figma/oauth",
    tags=["admin", "figma"],
)


class FigmaOAuthError(RuntimeError):
    """A safe, code-bearing OAuth failure.  Its message never includes a
    provider response body, authorization code, or token."""

    def __init__(self, code: str, message: str) -> None:
        self.code = code
        super().__init__(message)


def _tenant_from_request(request: Request) -> UUID:
    auth = getattr(request.state, "auth", None)
    if auth is None or getattr(auth, "tenant_id", None) is None:
        raise HTTPException(status_code=401, detail="unauthenticated")
    tid = auth.tenant_id
    return tid if isinstance(tid, UUID) else UUID(str(tid))


def _pool_from_request(request: Request) -> asyncpg.Pool:
    pool = getattr(request.app.state, "pool", None)
    if pool is None:
        raise HTTPException(status_code=500, detail="database pool unavailable")
    return pool


def _secret_store_from_request(request: Request) -> Any:
    store = getattr(request.app.state, "secret_store", None)
    if store is None:
        raise HTTPException(status_code=500, detail="secret store unavailable")
    return store


async def _json_body(request: Request) -> dict[str, Any]:
    try:
        body = await request.json()
    except Exception as exc:  # noqa: BLE001 - HTTP boundary only
        raise HTTPException(status_code=400, detail="request body must be JSON") from exc
    if not isinstance(body, dict):
        raise HTTPException(status_code=400, detail="request body must be an object")
    return body


def _oauth_settings() -> tuple[str, str, str]:
    """Return (client_id, client_secret, redirect_uri), failing closed when
    the customer-cloud OAuth app is not configured."""
    client_id = os.environ.get("FIGMA_CLIENT_ID", "").strip()
    client_secret = load_app_secret_text_from_env("FIGMA_CLIENT_SECRET") or ""
    redirect_uri = _figma_redirect_uri()
    missing = [
        name for name, value in {
            "FIGMA_CLIENT_ID": client_id,
            "FIGMA_CLIENT_SECRET": client_secret,
            "FIGMA_REDIRECT_URI": redirect_uri,
        }.items() if not value
    ]
    if missing:
        raise FigmaOAuthError(
            "figma_oauth_unconfigured",
            f"missing runtime configuration: {', '.join(missing)}",
        )
    return client_id, client_secret, redirect_uri


def _figma_oauth_enabled() -> bool:
    """Require explicit enablement in a production deployment.

    A BYOC deployment may be provisioned before its customer-owned Figma app
    exists.  Keep the source dormant until the deployment administrator opts
    in, while retaining a convenient non-production default for isolated
    connector tests and local implementation work.
    """
    configured = os.environ.get("FIGMA_OAUTH_ENABLED", "").strip()
    if configured:
        return configured == "1"
    from lib.shared.env import is_prod

    return not is_prod()


def _figma_redirect_uri() -> str:
    """Read the deployment-owned, exact Figma callback URI.

    Figma only accepts pre-registered redirect URLs.  Enforce the one callback
    owned by this connector rather than allowing a broad URL that could later
    become an OAuth open-redirect or a cross-provider mix-up.
    """
    configured = os.environ.get("FIGMA_REDIRECT_URI", "").strip()
    if not configured:
        return ""
    parts = urlsplit(configured)
    if (
        parts.scheme != "https"
        or not parts.netloc
        or parts.query
        or parts.fragment
        or parts.username
        or parts.password
        or parts.path != _FIGMA_OAUTH_CALLBACK_PATH
    ):
        raise FigmaOAuthError(
            "figma_oauth_redirect_invalid",
            "FIGMA_REDIRECT_URI must be an https Figma OAuth callback URL",
        )
    return urlunsplit((parts.scheme, parts.netloc, parts.path, "", ""))


def _figma_api_base_url() -> str:
    """Resolve one validated API host for OAuth validation and later fetches.

    The persisted installation must use the same host that proved the grant
    during callback handling.  ``endpoint`` also preserves the local source
    mock seam used by connector tests; production only permits HTTPS.
    """
    from lib.integrations.endpoints import endpoint
    from lib.shared.env import is_prod

    configured = endpoint("figma_api").strip().rstrip("/")
    parts = urlsplit(configured)
    if (
        parts.scheme not in {"https", "http"}
        or not parts.netloc
        or parts.query
        or parts.fragment
        or parts.username
        or parts.password
        or (is_prod() and parts.scheme != "https")
    ):
        raise FigmaOAuthError(
            "figma_api_base_url_invalid",
            "FIGMA_API_BASE_URL must be an absolute https URL in production",
        )
    return urlunsplit((parts.scheme, parts.netloc, parts.path.rstrip("/"), "", ""))


def _oauth_scopes() -> list[str]:
    configured = os.environ.get("FIGMA_OAUTH_SCOPES", "").strip()
    scopes = (
        [s for s in re.split(r"[\s,]+", configured) if s]
        if configured else list(_DEFAULT_SCOPES)
    )
    scope_set = set(scopes)
    missing = set(_DEFAULT_SCOPES).difference(scope_set)
    unsupported = scope_set.difference(_SUPPORTED_SCOPES)
    if missing or unsupported:
        raise FigmaOAuthError(
            "figma_oauth_scopes_invalid",
            "FIGMA OAuth scopes do not match the enabled Figma features",
        )
    return list(dict.fromkeys(scopes))


def _authorization_url() -> str:
    return os.environ.get("FIGMA_OAUTH_AUTHORIZE_URL", _FIGMA_AUTHORIZE_URL).rstrip("/")


def _token_url() -> str:
    return os.environ.get("FIGMA_OAUTH_TOKEN_URL", _FIGMA_TOKEN_URL).rstrip("/")


def _ui_base_url() -> str:
    """The allowlisted browser origin to which a public callback may return.

    ``return_path`` is deliberately only a path stored in OAuth state.  Joining
    it to this server-side origin prevents a caller from turning the callback
    into an open redirect and avoids accidentally sending the browser back to
    the customer-cloud gateway origin instead of the onboarding UI.
    """
    configured = os.environ.get("FIGMA_OAUTH_UI_BASE_URL", "").strip().rstrip("/")
    if not configured:
        raise FigmaOAuthError(
            "figma_oauth_ui_unconfigured",
            "FIGMA_OAUTH_UI_BASE_URL is required for the OAuth callback return",
        )
    parts = urlsplit(configured)
    if (
        parts.scheme not in {"https", "http"}
        or not parts.netloc
        or parts.query
        or parts.fragment
        or parts.username
        or parts.password
    ):
        raise FigmaOAuthError(
            "figma_oauth_ui_unconfigured",
            "FIGMA_OAUTH_UI_BASE_URL must be an absolute UI origin",
        )
    from lib.shared.env import is_prod

    loopback_hosts = {"localhost", "127.0.0.1", "::1"}
    allow_http_loopback = (
        os.environ.get("FIGMA_OAUTH_ALLOW_HTTP_LOOPBACK", "").strip() == "1"
        and (parts.hostname or "").lower() in loopback_hosts
    )
    if is_prod() and parts.scheme != "https" and not allow_http_loopback:
        raise FigmaOAuthError(
            "figma_oauth_ui_unconfigured",
            "FIGMA_OAUTH_UI_BASE_URL must use https in production "
            "(or explicitly allow an http loopback UI for local development)",
        )
    return urlunsplit((parts.scheme, parts.netloc, parts.path.rstrip("/"), "", ""))


def _deployment_oauth_ready() -> bool:
    """Whether this customer deployment can safely start Figma OAuth.

    Figma OAuth app registration belongs to the BYOC deployment administrator,
    not a tenant end user.  Deliberately expose only this boolean/state to the
    onboarding UI: client IDs, redirect URLs, secret references, and every
    reason a secret provider could fail remain deployment internals.
    """
    try:
        if not _figma_oauth_enabled():
            return False
        _oauth_settings()
        _ui_base_url()
        _oauth_scopes()
        _state_hmac_key()
    except (FigmaOAuthError, StateTokenInvalidError, SecretStoreError, ValueError):
        return False
    return True


def _deployment_setup_required_payload(*, ok: bool) -> dict[str, Any]:
    """Sanitized admin-gate contract shared by status and mutating routes."""
    return {
        "ok": ok,
        "state": _DEPLOYMENT_SETUP_REQUIRED_STATE,
        "setup_owner": _DEPLOYMENT_SETUP_OWNER,
        "deployment_oauth_ready": False,
        "next_action": _DEPLOYMENT_SETUP_ACTION,
        "message": "Figma OAuth needs deployment administrator setup.",
        "raw_secret_values_included": False,
    }


def _b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def _b64url_decode(value: str) -> bytes:
    return base64.urlsafe_b64decode(value + ("=" * (-len(value) % 4)))


def _state_hmac_key() -> bytes:
    """Match the shared Slack/GitHub state-token protection without coupling
    this connector to a private helper in another source module."""
    raw = load_app_secret_text_from_env("OAUTH_STATE_HMAC_KEY")
    if not raw:
        from lib.shared.env import is_prod

        if is_prod():
            raise StateTokenInvalidError(
                "state_invalid", "OAUTH_STATE_HMAC_KEY not configured in production",
            )
        raw = "dev-only-state-hmac-key-fallback"
    return raw.encode("utf-8")


def _deployment_oauth_admin_readiness() -> dict[str, Any]:
    """Return tenant-admin-safe setup metadata for a BYOC Figma app.

    This is intentionally *not* the ordinary connection-status contract.  It
    names configuration categories and shows the exact non-secret callback and
    scope contract that a deployment administrator must register in Figma; it
    never returns a client secret, a secret reference, an access token, or a
    provider response body.
    """
    client_id_configured = bool(os.environ.get("FIGMA_CLIENT_ID", "").strip())
    client_secret_configured = False
    redirect_uri: str | None = None
    ui_return_origin: str | None = None
    scopes: list[str] = []
    state_hmac_configured = False

    try:
        client_secret_configured = bool(
            load_app_secret_text_from_env("FIGMA_CLIENT_SECRET")
        )
    except SecretStoreError:
        pass
    try:
        redirect_uri = _figma_redirect_uri() or None
    except FigmaOAuthError:
        pass
    try:
        ui_return_origin = _ui_base_url()
    except FigmaOAuthError:
        pass
    try:
        scopes = _oauth_scopes()
    except FigmaOAuthError:
        pass
    try:
        state_hmac_configured = bool(_state_hmac_key())
    except (StateTokenInvalidError, SecretStoreError):
        pass

    checks = {
        "figma_oauth_enabled": _figma_oauth_enabled(),
        "client_id_configured": client_id_configured,
        "client_secret_configured": client_secret_configured,
        "redirect_uri_configured": redirect_uri is not None,
        "ui_return_origin_configured": ui_return_origin is not None,
        "requested_scopes_valid": bool(scopes),
        "oauth_state_hmac_configured": state_hmac_configured,
    }
    missing_configuration = [
        name for name, configured in checks.items() if not configured
    ]
    runtime_ready = not missing_configuration
    return {
        "ok": True,
        "setup_owner": _DEPLOYMENT_SETUP_OWNER,
        "deployment_model": "customer_owned_byoc_oauth_app",
        "runtime_ready": runtime_ready,
        "source_enabled": checks["figma_oauth_enabled"],
        "checks": checks,
        "missing_configuration": missing_configuration,
        "redirect_uri": redirect_uri,
        "ui_return_origin": ui_return_origin,
        "required_scopes": list(_DEFAULT_SCOPES),
        "configured_scopes": scopes,
        "provider_console_url": _FIGMA_DEVELOPER_APPS_URL,
        "recommended_app_mode": "private",
        # Figma does not expose app-registration inspection to this deployment.
        "provider_app_registration_unverified": True,
        "setup_checklist": [
            "Create a private OAuth app owned by the customer Figma team or organization.",
            "Register the exact redirect_uri shown here under OAuth credentials.",
            "Select the required_scopes shown here and publish the private app.",
            "Store the Client Secret only in this deployment's secret manager.",
            "Deploy or restart the customer-cloud gateway, then refresh this readiness check.",
        ],
        "raw_secret_values_included": False,
    }


async def _require_deployment_admin(
    request: Request,
    *,
    tenant_id: UUID,
    pool: asyncpg.Pool,
) -> None:
    """Enforce tenant-admin access for deployment configuration metadata."""
    auth = getattr(request.state, "auth", None)
    actor_id = getattr(auth, "actor_id", None)
    if actor_id is None:
        raise HTTPException(status_code=401, detail="unauthenticated")
    try:
        actor_uuid = actor_id if isinstance(actor_id, UUID) else UUID(str(actor_id))
    except (TypeError, ValueError) as exc:
        raise HTTPException(status_code=401, detail="unauthenticated") from exc
    async with tenant_transaction(tenant_id, pool=pool) as tctx:
        if not await has_role(
            actor_uuid,
            "admin",
            conn=tctx,
            tenant_id=tenant_id,
        ):
            raise HTTPException(status_code=403, detail="admin_role_required")


def _state_token(*, tenant_id: UUID, nonce: str, expires_at: datetime) -> str:
    payload = {
        "tenant_id": str(tenant_id),
        "nonce": nonce,
        "expires_at": expires_at.isoformat(),
    }
    payload_b64 = _b64url(json.dumps(payload, separators=(",", ":")).encode("utf-8"))
    signature = hmac.new(
        _state_hmac_key(), payload_b64.encode("ascii"), hashlib.sha256,
    ).digest()
    return f"{payload_b64}.{_b64url(signature)}"


def _parse_state_token(state: str) -> tuple[UUID, str]:
    if not state or "." not in state:
        raise StateTokenInvalidError("state_invalid", "state token malformed")
    payload_b64, _, signature_b64 = state.partition(".")
    try:
        expected = hmac.new(
            _state_hmac_key(), payload_b64.encode("ascii"), hashlib.sha256,
        ).digest()
        provided = _b64url_decode(signature_b64)
    except (TypeError, ValueError) as exc:
        raise StateTokenInvalidError("state_invalid", "state signature unreadable") from exc
    if not hmac.compare_digest(expected, provided):
        raise StateTokenInvalidError("state_invalid", "state HMAC mismatch")
    try:
        payload = json.loads(_b64url_decode(payload_b64))
        tenant_id = UUID(str(payload["tenant_id"]))
        nonce = str(payload["nonce"])
    except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
        raise StateTokenInvalidError("state_invalid", "state payload unparseable") from exc
    if not nonce:
        raise StateTokenInvalidError("state_invalid", "state nonce missing")
    return tenant_id, nonce


def _safe_return_path(value: Any) -> str:
    if value is None or value == "":
        return _DEFAULT_RETURN_PATH
    if not isinstance(value, str):
        raise HTTPException(status_code=400, detail="return_path must be a relative path")
    parts = urlsplit(value)
    # Do not turn the OAuth callback into an open redirect.  Keep an optional
    # query string because the onboarding UI may preserve a step/source hint.
    if (
        parts.scheme
        or parts.netloc
        or not parts.path.startswith("/")
        or parts.path.startswith("//")
        or "\\" in parts.path
    ):
        raise HTTPException(status_code=400, detail="return_path must be a local path")
    return urlunsplit(("", "", parts.path, parts.query, ""))


def _callback_location(
    return_path: str,
    *,
    state: str,
    error: str | None = None,
    installation_id: UUID | None = None,
    skipped_files: int = 0,
) -> str:
    return_parts = urlsplit(return_path)
    ui_parts = urlsplit(_ui_base_url())
    query = [
        (key, value)
        for key, value in parse_qsl(return_parts.query, keep_blank_values=True)
        if key not in {"figma", "figma_error", "figma_installation_id", "figma_skipped_files"}
    ]
    query.append(("figma", state))
    if error:
        query.append(("figma_error", error))
    if installation_id is not None:
        query.append(("figma_installation_id", str(installation_id)))
    if skipped_files:
        query.append(("figma_skipped_files", str(skipped_files)))
    path = f"{ui_parts.path.rstrip('/')}{return_parts.path}"
    return urlunsplit((ui_parts.scheme, ui_parts.netloc, path, urlencode(query), ""))


def _error_redirect(return_path: str, code: str) -> Response:
    try:
        location = _callback_location(return_path, state="error", error=code)
    except FigmaOAuthError as exc:
        return JSONResponse(
            status_code=503,
            content={"ok": False, "error_code": exc.code, "message": "Figma OAuth UI return is not configured"},
        )
    return RedirectResponse(url=location, status_code=302)


def _file_key_from_url(raw: Any) -> str:
    if not isinstance(raw, str) or not raw.strip():
        raise HTTPException(status_code=400, detail="file_urls must contain non-empty Figma URLs")
    parts = urlsplit(raw.strip())
    host = (parts.hostname or "").lower().rstrip(".")
    if parts.scheme != "https" or not (host == "figma.com" or host.endswith(".figma.com")):
        raise HTTPException(status_code=400, detail="each file URL must be an https://*.figma.com URL")
    segments = [unquote(segment) for segment in parts.path.split("/") if segment]
    # Standard document, prototype, whiteboard and slide links all place the
    # REST file key immediately after their resource-kind path segment.
    for index, segment in enumerate(segments[:-1]):
        if segment.lower() in _FILE_URL_KINDS:
            key = segments[index + 1]
            if _FILE_KEY_RE.fullmatch(key):
                return key
    raise HTTPException(status_code=400, detail="could not find a Figma file key in file URL")


def _selected_file_keys(value: Any) -> list[str]:
    if not isinstance(value, list) or not value:
        raise HTTPException(status_code=400, detail="file_urls must be a non-empty list")
    if len(value) > _MAX_SELECTED_FILES:
        raise HTTPException(status_code=400, detail=f"at most {_MAX_SELECTED_FILES} Figma files may be selected")
    keys: list[str] = []
    seen: set[str] = set()
    for raw in value:
        key = _file_key_from_url(raw)
        if key not in seen:
            seen.add(key)
            keys.append(key)
    if not keys:
        raise HTTPException(status_code=400, detail="at least one Figma file URL is required")
    return keys


async def _issue_figma_state(
    *,
    tenant_id: UUID,
    pool: asyncpg.Pool,
    secret_store: Any,
    file_keys: list[str],
    return_path: str,
) -> tuple[str, str]:
    """Persist a Figma-only, one-use OAuth state record plus an encrypted PKCE
    verifier.  The browser receives only a signed nonce, never verifier data."""
    nonce = secrets.token_urlsafe(32)
    expires_at = datetime.now(timezone.utc) + timedelta(seconds=_DEFAULT_STATE_TTL_S)
    verifier = secrets.token_urlsafe(64)
    verifier_ref = await secret_store.put(
        verifier,
        label=f"figma_pkce_verifier:{nonce[:16]}",
        tenant_id=tenant_id,
    )
    context = {
        "file_keys": file_keys,
        "return_path": return_path,
        "pkce_verifier_ref": verifier_ref,
    }
    try:
        await pool.execute(
            """
            INSERT INTO oauth_install_states
                (id, tenant_id, nonce, provider, expires_at, context)
            VALUES ($1, $2, $3, 'figma', $4, $5::jsonb)
            """,
            uuid7(), tenant_id, nonce, expires_at, json.dumps(context),
        )
    except Exception:
        # A failed state insert must not leave PKCE verifier material around.
        try:
            await secret_store.delete(verifier_ref, tenant_id=tenant_id)
        except Exception:  # noqa: BLE001 - best-effort cleanup only
            pass
        raise
    state = _state_token(tenant_id=tenant_id, nonce=nonce, expires_at=expires_at)
    challenge = _b64url(hashlib.sha256(verifier.encode("ascii")).digest())
    return state, challenge


def _context_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, (str, bytes)):
        try:
            decoded = json.loads(value)
        except (TypeError, ValueError):
            return {}
        return decoded if isinstance(decoded, dict) else {}
    return {}


async def _verify_and_consume_figma_state(
    state: str, pool: asyncpg.Pool,
) -> tuple[UUID, dict[str, Any]]:
    """Verify state HMAC and atomically consume only a Figma state row.

    The provider predicate is important: a correctly signed state issued for a
    different connector must not be consumed by the Figma callback endpoint.
    """
    tenant_id, nonce = _parse_state_token(state)
    row = await pool.fetchrow(
        """
        UPDATE oauth_install_states
           SET consumed_at = now()
         WHERE nonce = $1
           AND provider = 'figma'
           AND consumed_at IS NULL
           AND expires_at > now()
        RETURNING tenant_id, context
        """,
        nonce,
    )
    if row is not None:
        if row["tenant_id"] != tenant_id:
            raise StateTokenInvalidError("state_invalid", "tenant binding mismatch")
        return tenant_id, _context_dict(row["context"])

    existing = await pool.fetchrow(
        "SELECT provider, consumed_at, expires_at FROM oauth_install_states WHERE nonce = $1",
        nonce,
    )
    if existing is None or existing["provider"] != "figma":
        raise StateTokenInvalidError("state_invalid", "nonce was not issued for figma")
    if existing["consumed_at"] is not None:
        raise StateTokenInvalidError("state_consumed", "state token already used")
    raise StateTokenInvalidError("state_expired", "state token expired")


def _oauth_provider_binding(
    *,
    tenant_id: UUID | None,
    installation_id: Any | None,
    http_client: httpx.AsyncClient | None,
    provider_transport: ProviderExecutor | None,
    request_policy: RequestPolicy | PolicyResolver | None,
    quota_resolver: QuotaResolver | None,
    allow_unlimited_local: bool | None,
    require_tenant_installation: bool,
) -> ProviderRequestBinding:
    local_unlimited = explicit_local_transport(
        requested=allow_unlimited_local,
        has_local_injection=http_client is not None,
    )
    return ProviderRequestBinding(
        source="figma",
        tenant_id=str(tenant_id) if tenant_id is not None else None,
        installation_id=(
            str(installation_id) if installation_id is not None else None
        ),
        transport=provider_transport,
        request_policy=request_policy,
        quota_resolver=quota_resolver,
        allow_unlimited_local=local_unlimited,
        require_tenant=True,
        require_installation=require_tenant_installation,
    )


async def _exchange_oauth_code(
    code: str,
    code_verifier: str,
    *,
    tenant_id: UUID | None = None,
    http_client: httpx.AsyncClient | None = None,
    provider_transport: ProviderExecutor | None = None,
    request_policy: RequestPolicy | PolicyResolver | None = None,
    quota_resolver: QuotaResolver | None = None,
    allow_unlimited_local: bool | None = None,
    require_tenant_installation: bool = False,
) -> dict[str, Any]:
    """Exchange Figma's short-lived authorization code using Basic auth +
    PKCE.  Provider response bodies are intentionally not propagated."""
    client_id, client_secret, redirect_uri = _oauth_settings()
    basic = base64.b64encode(f"{client_id}:{client_secret}".encode("utf-8")).decode("ascii")
    provider = _oauth_provider_binding(
        tenant_id=tenant_id,
        installation_id=None,
        http_client=http_client,
        provider_transport=provider_transport,
        request_policy=request_policy,
        quota_resolver=quota_resolver,
        allow_unlimited_local=allow_unlimited_local,
        require_tenant_installation=require_tenant_installation,
    )
    http = http_client or httpx.AsyncClient(timeout=15.0)
    owns_http = http_client is None

    async def _once() -> httpx.Response:
        try:
            response = await http.post(
                _token_url(),
                headers={
                    "Authorization": f"Basic {basic}",
                    "Content-Type": "application/x-www-form-urlencoded",
                    "Accept": "application/json",
                },
                data={
                    "grant_type": "authorization_code",
                    "code": code,
                    "redirect_uri": redirect_uri,
                    "code_verifier": code_verifier,
                },
            )
        except httpx.TimeoutException as exc:
            raise ProviderTimeoutError(
                "Figma OAuth code exchange timed out",
                source="figma",
                operation="oauth.token.exchange",
                error_type=type(exc).__name__,
            ) from exc
        except httpx.TransportError as exc:
            raise ProviderTransientError(
                "Figma OAuth code exchange transport error",
                source="figma",
                operation="oauth.token.exchange",
                error_type=type(exc).__name__,
            ) from exc
        if response.status_code == 429:
            raise ProviderRateLimited(
                "Figma OAuth code exchange rate limit",
                retry_after_seconds=parse_retry_after(
                    response.headers.get("Retry-After"),
                ),
                status_code=429,
                header_parser_id="http.retry_after",
                source="figma",
                operation="oauth.token.exchange",
            )
        if response.status_code >= 500:
            raise ProviderTransientError(
                f"Figma OAuth token endpoint returned HTTP {response.status_code}",
                source="figma",
                operation="oauth.token.exchange",
                http_status=response.status_code,
            )
        return response

    try:
        response = await provider.execute("oauth.token.exchange", _once)
    finally:
        if owns_http:
            await http.aclose()
    if response.status_code // 100 != 2:
        raise FigmaOAuthError("token_exchange_failed", "Figma rejected the authorization code")
    try:
        body = response.json()
    except Exception as exc:  # noqa: BLE001 - untrusted provider body
        raise FigmaOAuthError("token_exchange_failed", "Figma token response was invalid") from exc
    if not isinstance(body, dict):
        raise FigmaOAuthError("token_exchange_failed", "Figma token response was invalid")
    return body


class FigmaOAuthRefreshError(RuntimeError):
    def __init__(self, code: str, *, reauthorization_required: bool = False) -> None:
        self.code = code
        self.reauthorization_required = reauthorization_required
        super().__init__(code)


async def _exchange_oauth_refresh(
    refresh_token: str,
    http: httpx.AsyncClient,
    *,
    tenant_id: UUID | None = None,
    installation_id: Any | None = None,
    provider_binding: ProviderRequestBinding | None = None,
    provider_transport: ProviderExecutor | None = None,
    request_policy: RequestPolicy | PolicyResolver | None = None,
    quota_resolver: QuotaResolver | None = None,
    allow_unlimited_local: bool | None = None,
) -> dict[str, Any]:
    """Refresh an OAuth access token through Figma's current token endpoint.

    Figma moved refreshes to ``/v1/oauth/token`` in 2025; the legacy
    ``/v1/oauth/refresh`` endpoint remains supported but is not used here.
    Refresh tokens are reusable while the prior access token is invalidated, so
    callers must serialize refreshes per installation.
    """
    client_id, client_secret, _ = _oauth_settings()
    basic = base64.b64encode(f"{client_id}:{client_secret}".encode("utf-8")).decode("ascii")
    refresh_url = os.environ.get("FIGMA_OAUTH_REFRESH_URL", _token_url()).rstrip("/")
    provider = provider_binding or _oauth_provider_binding(
        tenant_id=tenant_id,
        installation_id=installation_id,
        http_client=http,
        provider_transport=provider_transport,
        request_policy=request_policy,
        quota_resolver=quota_resolver,
        allow_unlimited_local=allow_unlimited_local,
        require_tenant_installation=True,
    )

    async def _once() -> httpx.Response:
        try:
            response = await http.post(
                refresh_url,
                headers={
                    "Authorization": f"Basic {basic}",
                    "Content-Type": "application/x-www-form-urlencoded",
                    "Accept": "application/json",
                },
                data={
                    "grant_type": "refresh_token",
                    "refresh_token": refresh_token,
                },
            )
        except httpx.TimeoutException as exc:
            raise ProviderTimeoutError(
                "Figma OAuth refresh timed out",
                source="figma",
                operation="oauth.token.refresh",
                error_type=type(exc).__name__,
            ) from exc
        except httpx.TransportError as exc:
            raise ProviderTransientError(
                "Figma OAuth refresh transport error",
                source="figma",
                operation="oauth.token.refresh",
                error_type=type(exc).__name__,
            ) from exc
        if response.status_code == 429:
            raise ProviderRateLimited(
                "Figma OAuth refresh rate limit",
                retry_after_seconds=parse_retry_after(
                    response.headers.get("Retry-After"),
                ),
                status_code=429,
                header_parser_id="http.retry_after",
                source="figma",
                operation="oauth.token.refresh",
            )
        if response.status_code >= 500:
            raise ProviderTransientError(
                f"Figma OAuth token endpoint returned HTTP {response.status_code}",
                source="figma",
                operation="oauth.token.refresh",
                http_status=response.status_code,
            )
        return response

    response = await provider.execute(
        "oauth.token.refresh",
        _once,
    )
    if response.status_code in {400, 401, 403}:
        raise FigmaOAuthRefreshError(
            "reauthorization_required", reauthorization_required=True,
        )
    if response.status_code // 100 != 2:
        raise FigmaOAuthRefreshError("refresh_failed")
    try:
        body = response.json()
    except Exception as exc:  # noqa: BLE001 - provider response boundary
        raise FigmaOAuthRefreshError("refresh_failed") from exc
    if not isinstance(body, dict) or not isinstance(body.get("access_token"), str) or not body["access_token"]:
        raise FigmaOAuthRefreshError("refresh_failed")
    return body


async def _set_oauth_connection_state(
    *, pool: asyncpg.Pool, tenant_id: UUID, installation_id: Any, state: str, error: str | None,
) -> None:
    async with tenant_transaction(tenant_id, pool=pool) as tctx:
        await tctx.execute(
            """
            UPDATE figma_installations
               SET connection_state = $1, last_error = $2
             WHERE id = $3
               AND tenant_id = $4
               AND auth_kind = 'oauth'
               AND disabled_at IS NULL
            """,
            state, error, installation_id, tenant_id,
        )


async def refresh_installation_access_token(
    *,
    pool: asyncpg.Pool | None,
    secret_store: Any | None,
    tenant_id: UUID | None,
    installation_id: Any | None,
    expected_access_ref: str | None,
    force: bool = False,
    http_client: httpx.AsyncClient | None = None,
    provider_binding: ProviderRequestBinding | None = None,
    provider_transport: ProviderExecutor | None = None,
    request_policy: RequestPolicy | PolicyResolver | None = None,
    quota_resolver: QuotaResolver | None = None,
    allow_unlimited_local: bool | None = None,
) -> tuple[str, str, str | None, datetime] | None:
    """Refresh a Figma OAuth token once per tenant/install under a PostgreSQL
    advisory transaction lock.

    A concurrent request that arrives after another worker refreshed simply
    resolves the newer persisted access ref instead of replacing Figma's one
    valid access token again.  Invalid/revoked refresh grants move the install
    to ``reauthorization_required``; transport/server faults become ``degraded``.
    """
    if not (pool is not None and secret_store is not None and tenant_id is not None and installation_id is not None):
        return None
    http = http_client or httpx.AsyncClient(timeout=15.0)
    owns_http = http_client is None
    old_refs: list[str | None] = []
    new_refs: list[str | None] = []
    try:
        async with tenant_transaction(tenant_id, pool=pool) as tctx:
            await tctx.execute(
                "SELECT pg_advisory_xact_lock(hashtext($1))",
                f"figma-oauth-refresh:{installation_id}",
            )
            row = await tctx.fetchrow(
                """
                SELECT secret_ref, refresh_secret_ref, token_expires_at,
                       connection_state
                  FROM figma_installations
                 WHERE id = $1
                   AND tenant_id = $2
                   AND auth_kind = 'oauth'
                   AND disabled_at IS NULL
                 FOR UPDATE
                """,
                installation_id, tenant_id,
            )
            if row is None:
                return None
            current_ref = row["secret_ref"]
            refresh_ref = row["refresh_secret_ref"]
            expires_at = row["token_expires_at"]
            if row["connection_state"] == "reauthorization_required":
                return None
            now = datetime.now(timezone.utc)
            # Another worker won the race after this client observed a 401.
            if force and expected_access_ref and current_ref != expected_access_ref:
                raw_current = await secret_store.get(current_ref, tenant_id=tenant_id)
                token = raw_current.decode("utf-8") if isinstance(raw_current, bytes) else str(raw_current)
                return token, str(current_ref), refresh_ref, expires_at
            if (
                not force
                and expires_at is not None
                and expires_at > now + timedelta(seconds=_REFRESH_SKEW_S)
            ):
                raw_current = await secret_store.get(current_ref, tenant_id=tenant_id)
                token = raw_current.decode("utf-8") if isinstance(raw_current, bytes) else str(raw_current)
                return token, str(current_ref), refresh_ref, expires_at
            if not refresh_ref:
                raise FigmaOAuthRefreshError(
                    "reauthorization_required", reauthorization_required=True,
                )
            raw_refresh = await secret_store.get(refresh_ref, tenant_id=tenant_id)
            refresh_token = raw_refresh.decode("utf-8") if isinstance(raw_refresh, bytes) else str(raw_refresh)
            refreshed = await _exchange_oauth_refresh(
                refresh_token,
                http,
                tenant_id=tenant_id,
                installation_id=installation_id,
                provider_binding=provider_binding,
                provider_transport=provider_transport,
                request_policy=request_policy,
                quota_resolver=quota_resolver,
                allow_unlimited_local=allow_unlimited_local,
            )
            access_token = str(refreshed["access_token"])
            new_access_ref = await secret_store.put(
                access_token,
                label=f"figma_oauth_access_token:{installation_id}",
                tenant_id=tenant_id,
            )
            new_refs.append(new_access_ref)
            returned_refresh = refreshed.get("refresh_token")
            new_refresh_ref = refresh_ref
            if isinstance(returned_refresh, str) and returned_refresh:
                new_refresh_ref = await secret_store.put(
                    returned_refresh,
                    label=f"figma_oauth_refresh_token:{installation_id}",
                    tenant_id=tenant_id,
                )
                new_refs.append(new_refresh_ref)
            expires_at = _token_expiry(refreshed)
            await tctx.execute(
                """
                UPDATE figma_installations
                   SET secret_ref = $1,
                       refresh_secret_ref = $2,
                       token_expires_at = $3,
                       connection_state = 'connected',
                       last_error = NULL
                 WHERE id = $4 AND tenant_id = $5
                """,
                new_access_ref, new_refresh_ref, expires_at, installation_id, tenant_id,
            )
            old_refs = [current_ref, refresh_ref if new_refresh_ref != refresh_ref else None]
        # Delete superseded ciphertext only after the install points at the new
        # refs.  Failure here is harmless orphan cleanup, not a lost credential.
        await _delete_refs(secret_store, tenant_id, old_refs)
        return access_token, new_access_ref, new_refresh_ref, expires_at
    except FigmaOAuthRefreshError as exc:
        await _delete_refs(secret_store, tenant_id, new_refs)
        state = "reauthorization_required" if exc.reauthorization_required else "degraded"
        error = (
            "Figma authorization needs to be reconnected"
            if exc.reauthorization_required else "Figma token refresh failed; retry shortly"
        )
        await _set_oauth_connection_state(
            pool=pool, tenant_id=tenant_id, installation_id=installation_id,
            state=state, error=error,
        )
        return None
    except FigmaOAuthError as exc:
        await _delete_refs(secret_store, tenant_id, new_refs)
        log.warning("figma_oauth_refresh_config_failed", code=exc.code)
        await _set_oauth_connection_state(
            pool=pool, tenant_id=tenant_id, installation_id=installation_id,
            state="degraded", error="Figma OAuth refresh is not configured",
        )
        return None
    except (SecretStoreError, ValueError, asyncpg.PostgresError) as exc:
        await _delete_refs(secret_store, tenant_id, new_refs)
        log.warning("figma_oauth_refresh_persist_failed", error_type=type(exc).__name__)
        await _set_oauth_connection_state(
            pool=pool, tenant_id=tenant_id, installation_id=installation_id,
            state="degraded", error="Figma token refresh could not be persisted",
        )
        return None
    finally:
        if owns_http:
            await http.aclose()


def _token_expiry(token_response: dict[str, Any]) -> datetime:
    try:
        expires_in = int(token_response.get("expires_in"))
    except (TypeError, ValueError):
        expires_in = _DEFAULT_TOKEN_TTL_S
    # Do not persist a non-positive expiry when Figma returns a malformed value.
    expires_in = expires_in if expires_in > 0 else _DEFAULT_TOKEN_TTL_S
    return datetime.now(timezone.utc) + timedelta(seconds=expires_in)


def _grant_scopes(token_response: dict[str, Any]) -> list[str]:
    raw = token_response.get("scope") or token_response.get("scopes")
    if isinstance(raw, str):
        values = [value for value in re.split(r"[\s,]+", raw) if value]
        if values:
            return values
    if isinstance(raw, list):
        values = [str(value).strip() for value in raw if str(value).strip()]
        if values:
            return values
    return _oauth_scopes()


def _oauth_user_id(identity: dict[str, Any], token_response: dict[str, Any]) -> str | None:
    for source, key in (
        (token_response, "user_id_string"),
        (identity, "id"),
        (identity, "user_id_string"),
        (token_response, "user_id"),
    ):
        value = source.get(key)
        if value is not None and str(value).strip():
            return str(value).strip()
    return None


async def _validate_selected_files(
    *,
    access_token: str,
    file_keys: list[str],
    tenant_id: UUID,
) -> tuple[dict[str, Any], list[dict[str, Any]], int]:
    """Verify the OAuth identity and materialize only exactly-selected,
    accessible files.  A partial grant is still useful, but a zero-file grant
    is not finalized as a seemingly-connected source."""
    client = FigmaClient(
        base_url=_figma_api_base_url(),
        api_token=access_token,
        auth_kind="oauth",
        **tenant_preinstall_transport_kwargs(tenant_id),
    )
    accessible: list[dict[str, Any]] = []
    skipped = 0
    try:
        identity = await client.get_current_user()
        for file_key in file_keys:
            try:
                # depth=1 is enough to prove access and get a title without
                # pulling a complete design document during the callback.
                document = await client.get_file(file_key, depth=1)
            except FigmaApiError as exc:
                if exc.code in {"figma_api_unauthorized", "figma_api_not_found"}:
                    skipped += 1
                    continue
                raise
            accessible.append({
                "file_key": file_key,
                "file_name": document.get("name"),
                "project_name": None,
            })
    finally:
        await client.aclose()
    return identity, accessible, skipped


async def _delete_refs(secret_store: Any, tenant_id: UUID, refs: list[str | None]) -> None:
    for ref in {ref for ref in refs if ref}:
        try:
            await secret_store.delete(ref, tenant_id=tenant_id)
        except Exception as exc:  # noqa: BLE001 - disconnected state wins
            log.warning("figma_secret_cleanup_failed", error_type=type(exc).__name__)


@router.post("/oauth/start")
async def oauth_start(request: Request) -> JSONResponse:
    """Start the browser OAuth authorization flow for known Figma file URLs."""
    tenant_id = _tenant_from_request(request)
    pool = _pool_from_request(request)
    secret_store = _secret_store_from_request(request)
    if not _deployment_oauth_ready():
        return JSONResponse(
            status_code=503,
            content={
                **_deployment_setup_required_payload(ok=False),
                "error_code": "deployment_figma_oauth_setup_required",
            },
        )
    body = await _json_body(request)
    file_keys = _selected_file_keys(body.get("file_urls"))
    return_path = _safe_return_path(body.get("return_path"))
    try:
        client_id, _, redirect_uri = _oauth_settings()
        _ui_base_url()  # validate the callback's allowlisted browser origin early
        state, challenge = await _issue_figma_state(
            tenant_id=tenant_id,
            pool=pool,
            secret_store=secret_store,
            file_keys=file_keys,
            return_path=return_path,
        )
    except (FigmaOAuthError, StateTokenInvalidError):
        return JSONResponse(
            status_code=503,
            content={
                **_deployment_setup_required_payload(ok=False),
                "error_code": "deployment_figma_oauth_setup_required",
            },
        )
    except SecretStoreError:
        return JSONResponse(
            status_code=503,
            content={"ok": False, "error_code": "secret_store_unavailable", "message": "Could not prepare Figma OAuth"},
        )

    params = {
        "client_id": client_id,
        "redirect_uri": redirect_uri,
        "scope": ",".join(_oauth_scopes()),
        "state": state,
        "response_type": "code",
        "code_challenge": challenge,
        "code_challenge_method": "S256",
    }
    authorization_url = f"{_authorization_url()}?{urlencode(params)}"
    return JSONResponse(content={
        "ok": True,
        "state": "ready_for_provider_approval",
        "authorization_url": authorization_url,
        # Kept as an alias for generic onboarding clients.
        "install_url": authorization_url,
        "state_expires_in_seconds": _DEFAULT_STATE_TTL_S,
        "requested_file_count": len(file_keys),
        "provider_callback_required": True,
        "raw_secret_values_included": False,
    })


@router.get("/oauth/callback")
async def oauth_callback(request: Request) -> Response:
    """Consume OAuth state, exchange code immediately, and finalize the
    exact selected-file installation before returning the browser to onboarding."""
    pool = _pool_from_request(request)
    secret_store = _secret_store_from_request(request)
    raw_state = request.query_params.get("state", "")
    return_path = _DEFAULT_RETURN_PATH
    verifier_ref: str | None = None
    tenant_id: UUID | None = None
    try:
        tenant_id, context = await _verify_and_consume_figma_state(raw_state, pool)
        return_path = _safe_return_path(context.get("return_path"))
        file_keys = context.get("file_keys")
        verifier_ref = context.get("pkce_verifier_ref")
        if (
            not isinstance(file_keys, list)
            or not file_keys
            or not all(isinstance(key, str) and _FILE_KEY_RE.fullmatch(key) for key in file_keys)
            or not isinstance(verifier_ref, str)
            or not verifier_ref
        ):
            raise FigmaOAuthError("state_invalid", "OAuth state context malformed")
    except StateTokenInvalidError as exc:
        return _error_redirect(return_path, exc.reason)
    except (FigmaOAuthError, HTTPException):
        return _error_redirect(return_path, "state_invalid")

    try:
        provider_error = request.query_params.get("error")
        if provider_error:
            return _error_redirect(return_path, "consent_denied")
        code = request.query_params.get("code", "").strip()
        if not code:
            return _error_redirect(return_path, "missing_authorization_code")

        raw_verifier = await secret_store.get(verifier_ref, tenant_id=tenant_id)
        verifier = raw_verifier.decode("utf-8") if isinstance(raw_verifier, bytes) else str(raw_verifier)
        token_response = await _exchange_oauth_code(
            code,
            verifier,
            **tenant_preinstall_transport_kwargs(tenant_id),
        )
        access_token = token_response.get("access_token")
        refresh_token = token_response.get("refresh_token")
        if not isinstance(access_token, str) or not access_token:
            raise FigmaOAuthError("token_exchange_failed", "Figma did not return an access token")
        if not isinstance(refresh_token, str) or not refresh_token:
            raise FigmaOAuthError("token_exchange_failed", "Figma did not return a refresh token")

        identity, files, skipped_files = await _validate_selected_files(
            access_token=access_token,
            file_keys=file_keys,
            tenant_id=tenant_id,
        )
        oauth_user_id = _oauth_user_id(identity, token_response)
        if not oauth_user_id:
            raise FigmaOAuthError("identity_verification_failed", "Figma identity is unavailable")
        if not files:
            raise FigmaOAuthError("no_accessible_files", "No selected Figma file is accessible")

        # Validate all provider access BEFORE we write token refs.  Once the
        # grant is proven usable, only opaque refs are persisted.
        access_ref = await secret_store.put(
            access_token,
            label=f"figma_oauth_access_token:{oauth_user_id}",
            tenant_id=tenant_id,
        )
        refresh_ref = await secret_store.put(
            refresh_token,
            label=f"figma_oauth_refresh_token:{oauth_user_id}",
            tenant_id=tenant_id,
        )
        try:
            install_id = await finalize_install(
                pool,
                tenant_id=tenant_id,
                base_url=_figma_api_base_url(),
                files=files,
                secret_ref=access_ref,
                # Public OAuth cannot expose a team ID.  Existing event
                # shards require a non-empty stable scope, so this is an
                # explicitly labelled OAuth-user scope rather than a guessed
                # Figma team id.
                team_id=f"oauth-user:{oauth_user_id}",
                auth_kind="oauth",
                refresh_secret_ref=refresh_ref,
                token_expires_at=_token_expiry(token_response),
                oauth_user_id=oauth_user_id,
                granted_scopes=_grant_scopes(token_response),
                connection_state="connected",
            )
        except Exception:
            await _delete_refs(secret_store, tenant_id, [access_ref, refresh_ref])
            raise
    except asyncpg.UniqueViolationError as exc:
        if getattr(exc, "constraint_name", None) == "figma_installations_active_oauth_user_unique":
            log.info("figma_oauth_install_collision")
            return _error_redirect(return_path, "installation_collision")
        log.warning("figma_oauth_finalize_failed", error_type=type(exc).__name__)
        return _error_redirect(return_path, "finalize_failed")
    except FigmaOAuthError as exc:
        log.info("figma_oauth_callback_failed", code=exc.code)
        return _error_redirect(return_path, exc.code)
    except FigmaApiError as exc:
        code = "file_validation_failed" if exc.code != "figma_api_unauthorized" else "authorization_failed"
        log.info("figma_oauth_callback_api_failed", code=code)
        return _error_redirect(return_path, code)
    except (SecretStoreError, ValueError) as exc:
        log.warning("figma_oauth_callback_secret_failed", error_type=type(exc).__name__)
        return _error_redirect(return_path, "secret_store_unavailable")
    except Exception as exc:  # noqa: BLE001 - do not leak OAuth failures
        log.exception("figma_oauth_callback_failed", error_type=type(exc).__name__)
        return _error_redirect(return_path, "finalize_failed")
    finally:
        if tenant_id is not None and verifier_ref:
            await _delete_refs(secret_store, tenant_id, [verifier_ref])

    log.info(
        "figma_oauth_install_finalized",
        installation_id=str(install_id), file_count=len(files), skipped_files=skipped_files,
    )
    try:
        location = _callback_location(
            return_path,
            state="connected",
            installation_id=install_id,
            skipped_files=skipped_files,
        )
    except FigmaOAuthError as exc:
        return JSONResponse(
            status_code=503,
            content={"ok": False, "error_code": exc.code, "message": "Figma OAuth UI return is not configured"},
        )
    return RedirectResponse(url=location, status_code=302)


async def _latest_observation(
    executor: Any,
    tenant_id: UUID,
    installation_id: UUID,
) -> dict[str, Any] | None:
    """Best-effort proof for the onboarding UI.  It intentionally reads only
    safe observation metadata and works before or after snapshot artifacts are
    enabled."""
    try:
        row = await executor.fetchrow(
            """
            SELECT id, occurred_at, ingested_at, source_channel, kind, content_text,
                   content ->> 'file_key' AS file_key,
                   content ->> 'file_name' AS file_name,
                   content -> 'artifacts' -> 0 ->> 'blob_id' AS artifact_id
              FROM observations
             WHERE tenant_id = $1
               AND source_channel LIKE 'figma:%'
               AND content #>> '{source_locator,installation_id}' = $2
             ORDER BY ingested_at DESC
             LIMIT 1
            """,
            tenant_id,
            str(installation_id),
        )
    except Exception as exc:  # noqa: BLE001 - status must remain available
        log.warning("figma_status_observation_lookup_failed", error_type=type(exc).__name__)
        return None
    if row is None:
        return None
    return {
        "id": str(row["id"]),
        "occurred_at": row["occurred_at"].isoformat() if row["occurred_at"] else None,
        "ingested_at": row["ingested_at"].isoformat() if row["ingested_at"] else None,
        "source_channel": row["source_channel"],
        "kind": row["kind"],
        "content_text": row["content_text"],
        "file_key": row["file_key"],
        "file_name": row["file_name"],
        "artifact_id": row["artifact_id"],
    }


async def _figma_observation_count(
    executor: Any,
    tenant_id: UUID,
    installation_id: UUID,
) -> int:
    try:
        value = await executor.fetchval(
            """
            SELECT count(*)
              FROM observations
             WHERE tenant_id = $1
               AND source_channel LIKE 'figma:%'
               AND content #>> '{source_locator,installation_id}' = $2
            """,
            tenant_id,
            str(installation_id),
        )
    except Exception as exc:  # noqa: BLE001 - status must remain available
        log.warning("figma_status_observation_count_failed", error_type=type(exc).__name__)
        return 0
    return int(value or 0)


async def _figma_synced_file_count(
    executor: Any,
    tenant_id: UUID,
    installation_id: UUID,
) -> int:
    try:
        value = await executor.fetchval(
            """
            SELECT count(DISTINCT content ->> 'file_key')
              FROM observations
             WHERE tenant_id = $1
               AND source_channel LIKE 'figma:%'
               AND content #>> '{source_locator,installation_id}' = $2
               AND content ->> 'file_key' IS NOT NULL
            """,
            tenant_id,
            str(installation_id),
        )
    except Exception as exc:  # noqa: BLE001 - status must remain available
        log.warning("figma_status_synced_file_count_failed", error_type=type(exc).__name__)
        return 0
    return int(value or 0)


@admin_router.get("/readiness")
async def deployment_oauth_readiness(request: Request) -> JSONResponse:
    """Show a BYOC deployment admin the safe Figma app setup contract.

    Ordinary Figma onboarding users receive only the generic admin gate from
    `/connect/status`; this endpoint is role-gated because it includes the
    deployment callback URI and detailed configuration categories.
    """
    tenant_id = _tenant_from_request(request)
    pool = _pool_from_request(request)
    await _require_deployment_admin(request, tenant_id=tenant_id, pool=pool)
    return JSONResponse(content=_deployment_oauth_admin_readiness())


@router.get("/connect/status")
async def connect_status(
    request: Request,
    installation_id: UUID | None = None,
) -> JSONResponse:
    tenant_id = _tenant_from_request(request)
    pool = _pool_from_request(request)
    deployment_oauth_ready = _deployment_oauth_ready()
    async with tenant_transaction(tenant_id, pool=pool) as tctx:
        rows = await tctx.fetch(
            """
            SELECT fi.id, fi.auth_kind, fi.connection_state, fi.last_error,
                   fi.token_expires_at, fi.connected_at, fi.disabled_at,
                   COUNT(ff.id) AS file_count,
                   COUNT(ff.id) FILTER (WHERE ff.state = 'active') AS active_file_count
              FROM figma_installations fi
              LEFT JOIN figma_files ff ON ff.figma_installation_id = fi.id
             WHERE fi.tenant_id = $1
               AND ($2::uuid IS NULL OR fi.id = $2)
             GROUP BY fi.id
             ORDER BY fi.connected_at DESC NULLS LAST, fi.created_at DESC
            """,
            tenant_id,
            installation_id,
        )
        installations = [
            await _figma_installation_status_payload(
                tctx,
                tenant_id=tenant_id,
                row=row,
                deployment_oauth_ready=deployment_oauth_ready,
            )
            for row in rows
        ]
    if installation_id is not None and not installations:
        return JSONResponse(
            status_code=404,
            content={
                "ok": False,
                "state": "not_connected",
                "installation_id": str(installation_id),
                "message": "Figma installation was not found for this tenant.",
            },
        )
    if not installations:
        if not deployment_oauth_ready:
            return JSONResponse(
                content={
                    **_deployment_setup_required_payload(ok=True),
                    "installation_id": None,
                    "installations": [],
                    "installation_count": 0,
                    "installation_selection_required": False,
                    "file_count": 0,
                    "selected_file_count": 0,
                    "synced_file_count": 0,
                    "failed_file_count": 0,
                    "observation_count": 0,
                    "latest_observation": None,
                    "files": [],
                },
            )
        return JSONResponse(content={
            "ok": True,
            "state": "not_connected",
            "installation_id": None,
            "installations": [],
            "installation_count": 0,
            "installation_selection_required": False,
            "deployment_oauth_ready": True,
            "setup_owner": None,
        })
    if len(installations) > 1:
        return JSONResponse(
            content={
                "ok": True,
                "state": "multiple_installations",
                "installation_id": None,
                "installations": installations,
                "installation_count": len(installations),
                "installation_selection_required": True,
                "deployment_oauth_ready": deployment_oauth_ready,
                "setup_owner": (
                    None
                    if deployment_oauth_ready
                    else _DEPLOYMENT_SETUP_OWNER
                ),
                "message": (
                    "Select an exact Figma installation before retrying or "
                    "disconnecting."
                ),
            }
        )
    payload = installations[0]
    return JSONResponse(
        content={
            **payload,
            "installations": installations,
            "installation_count": 1,
            "installation_selection_required": False,
        }
    )


async def _figma_installation_status_payload(
    executor: Any,
    *,
    tenant_id: UUID,
    row: Any,
    deployment_oauth_ready: bool,
) -> dict[str, Any]:
    file_rows = await executor.fetch(
        """
        SELECT file_key, file_name, project_name, state, event_cursor,
               last_synced_at, last_error
          FROM figma_files
         WHERE tenant_id = $1
           AND figma_installation_id = $2
         ORDER BY file_key
        """,
        tenant_id,
        row["id"],
    )
    latest_observation = await _latest_observation(
        executor,
        tenant_id,
        row["id"],
    )
    observation_count = await _figma_observation_count(
        executor,
        tenant_id,
        row["id"],
    )
    synced_file_count = await _figma_synced_file_count(
        executor,
        tenant_id,
        row["id"],
    )
    files = [
        {
            "file_key": record["file_key"],
            "file_name": record["file_name"],
            "project_name": record["project_name"],
            "state": record["state"],
            "event_cursor": record["event_cursor"],
            "last_synced_at": (
                record["last_synced_at"].isoformat()
                if record["last_synced_at"] else None
            ),
            "last_error": record["last_error"],
        }
        for record in file_rows
    ]
    state = "disconnected" if row["disabled_at"] is not None else row["connection_state"]
    # A retry re-arms the outbox with `pending`.  Until the worker owns a
    # durable terminal-state update, an observed Figma record is authoritative
    # proof that the connection is usable and should not leave the UI spinning.
    if state == "pending" and latest_observation is not None:
        state = "connected"
    next_action = (
        _DEPLOYMENT_SETUP_ACTION if not deployment_oauth_ready
        else "reauthorize" if state == "reauthorization_required"
        else "view_observation" if latest_observation is not None
        else "wait_for_initial_sync"
    )
    return {
        "ok": True,
        "state": state,
        "installation_id": str(row["id"]),
        "deployment_oauth_ready": deployment_oauth_ready,
        "setup_owner": None if deployment_oauth_ready else _DEPLOYMENT_SETUP_OWNER,
        "auth_kind": row["auth_kind"],
        "file_count": int(row["file_count"] or 0),
        "active_file_count": int(row["active_file_count"] or 0),
        "selected_file_count": int(row["active_file_count"] or 0),
        "synced_file_count": synced_file_count,
        "failed_file_count": sum(1 for file in files if file["state"] == "errored"),
        "connected_at": row["connected_at"].isoformat() if row["connected_at"] else None,
        "token_expires_at": row["token_expires_at"].isoformat() if row["token_expires_at"] else None,
        "last_error": row["last_error"],
        "observation_count": observation_count,
        "latest_observation": latest_observation,
        "files": files,
        "next_action": next_action,
    }


@router.post("/connect/retry")
async def connect_retry(
    request: Request,
    installation_id: UUID,
) -> JSONResponse:
    """Re-arm the existing transactional onboarding trigger without requiring
    the user to reconnect or reveal a credential."""
    tenant_id = _tenant_from_request(request)
    pool = _pool_from_request(request)
    if not _deployment_oauth_ready():
        return JSONResponse(
            status_code=503,
            content={
                **_deployment_setup_required_payload(ok=False),
                "error_code": "deployment_figma_oauth_setup_required",
            },
        )
    async with tenant_transaction(tenant_id, pool=pool) as tctx:
        row = await tctx.fetchrow(
            """
            SELECT id FROM figma_installations
             WHERE tenant_id = $1
               AND id = $2
               AND disabled_at IS NULL
            """,
            tenant_id,
            installation_id,
        )
        if row is None:
            return JSONResponse(
                status_code=404,
                content={"ok": False, "state": "not_connected", "message": "Connect Figma first"},
            )
        install_id = installation_id
        files = await tctx.fetch(
            """
            SELECT file_key FROM figma_files
             WHERE tenant_id = $1 AND figma_installation_id = $2 AND state = 'active'
             ORDER BY file_key
            """,
            tenant_id, install_id,
        )
        file_keys = [str(file["file_key"]) for file in files]
        if not file_keys:
            return JSONResponse(
                status_code=409,
                content={"ok": False, "state": "no_selected_files", "message": "Select at least one Figma file"},
            )
        await tctx.execute(
            """
            INSERT INTO onboarding_triggers (
                id, tenant_id, source, trigger_kind, installation_row_id, payload
            ) VALUES ($1, $2, 'figma', 'manual_replay', $3, $4::jsonb)
            ON CONFLICT (tenant_id, source, installation_row_id)
                WHERE installation_row_id IS NOT NULL
                DO UPDATE SET
                    trigger_kind = 'manual_replay',
                    payload = EXCLUDED.payload,
                    consumed_at = NULL,
                    consumed_by_workflow_id = NULL,
                    consume_attempts = 0,
                    last_attempt_at = NULL,
                    last_error = NULL,
                    created_at = now()
            """,
            uuid7(), tenant_id, install_id,
            json.dumps({"files": file_keys, "retry": True}),
        )
        await tctx.execute(
            """
            UPDATE figma_installations
               SET connection_state = 'pending', last_error = NULL
             WHERE id = $1 AND tenant_id = $2
            """,
            install_id, tenant_id,
        )
    return JSONResponse(content={
        "ok": True,
        "state": "syncing",
        "installation_id": str(install_id),
        "file_count": len(file_keys),
    })


@router.delete("/connect")
async def connect_disconnect(
    request: Request,
    installation_id: UUID,
) -> JSONResponse:
    """Disable Figma, pause all files, and remove local encrypted credentials.
    Figma does not expose a documented OAuth revocation endpoint, so local
    deletion is the deterministic disconnect boundary."""
    tenant_id = _tenant_from_request(request)
    pool = _pool_from_request(request)
    secret_store = _secret_store_from_request(request)
    async with tenant_transaction(tenant_id, pool=pool) as tctx:
        row = await tctx.fetchrow(
            """
            WITH selected AS (
                SELECT id, secret_ref, refresh_secret_ref, webhook_secret_ref
                  FROM figma_installations
                 WHERE tenant_id = $1
                   AND id = $2
                   AND disabled_at IS NULL
                 FOR UPDATE
            ), updated AS (
                UPDATE figma_installations fi
                   SET disabled_at = now(),
                       connection_state = 'disconnected',
                       last_error = NULL,
                       secret_ref = NULL,
                       refresh_secret_ref = NULL,
                       webhook_secret_ref = NULL
                  FROM selected previous
                 WHERE fi.id = previous.id
                RETURNING fi.id,
                          previous.secret_ref AS removed_access_ref,
                          previous.refresh_secret_ref AS removed_refresh_ref,
                          previous.webhook_secret_ref AS removed_webhook_ref
            )
            SELECT * FROM updated
            """,
            tenant_id,
            installation_id,
        )
        if row is None:
            return JSONResponse(content={"ok": True, "state": "disconnected", "already_disconnected": True})
        await tctx.execute(
            "UPDATE figma_files SET state = 'paused' WHERE tenant_id = $1 AND figma_installation_id = $2",
            tenant_id, row["id"],
        )
    await _delete_refs(
        secret_store,
        tenant_id,
        [row["removed_access_ref"], row["removed_refresh_ref"], row["removed_webhook_ref"]],
    )
    return JSONResponse(content={"ok": True, "state": "disconnected", "installation_id": str(row["id"])})


# ---------------------------------------------------------------------
# Legacy PAT fallback — hidden from the OAuth-first UI.
# ---------------------------------------------------------------------

def _require_token(body: dict[str, Any]) -> tuple[str, str]:
    api_token = (body.get("api_token") or "").strip()
    if not api_token:
        raise HTTPException(status_code=400, detail="api_token is required")
    base_url = native_connect_base_url(
        body.get("base_url"),
        endpoint_name="figma_api",
    )
    return api_token, base_url


def _require_team_id(body: dict[str, Any]) -> str:
    team_id = (body.get("team_id") or "").strip()
    if not team_id:
        raise HTTPException(
            status_code=400,
            detail="team_id is required to enumerate Figma projects/files",
        )
    return team_id


def _auth_failure_response(exc: FigmaApiError) -> JSONResponse:
    unauthorized = exc.code == "figma_api_unauthorized"
    return JSONResponse(
        status_code=400,
        content={
            "ok": False,
            "error_code": "figma_auth_failed" if unauthorized else "figma_api_error",
            "message": (
                "Figma rejected the access token or its requested scopes."
                if unauthorized
                else "Could not reach the Figma API. Check the base_url and service connectivity."
            ),
            "underlying_error": str(exc)[:300],
        },
    )


def _normalize_file(file: dict[str, Any]) -> dict[str, Any]:
    return {
        "file_key": str(file.get("file_key") or file.get("key") or ""),
        "file_name": file.get("file_name") or file.get("name"),
        "project_name": file.get("project_name") or file.get("project"),
    }


@router.post("/connect/preflight")
async def connect_preflight(request: Request) -> JSONResponse:
    """Legacy PAT preflight.  OAuth onboarding calls ``/oauth/start``."""
    tenant_id = _tenant_from_request(request)
    body = await _json_body(request)
    api_token, base_url = _require_token(body)
    team_id = _require_team_id(body)
    client = FigmaClient(
        base_url=base_url,
        api_token=api_token,
        team_id=team_id,
        auth_kind="pat",
        **tenant_preinstall_transport_kwargs(tenant_id),
    )
    try:
        files = await client.list_files(team_id)
    except FigmaApiError as exc:
        return _auth_failure_response(exc)
    finally:
        await client.aclose()
    normalized = [_normalize_file(file) for file in files]
    return JSONResponse(content={
        "ok": True,
        "base_url": base_url,
        "team_id": team_id,
        "files": [file for file in normalized if file["file_key"]],
    })


@router.post("/connect/finalize")
async def connect_finalize(request: Request) -> JSONResponse:
    """Legacy PAT finalize retained for constrained private deployments."""
    tenant_id = _tenant_from_request(request)
    pool = _pool_from_request(request)
    store = _secret_store_from_request(request)
    body = await _json_body(request)
    api_token, base_url = _require_token(body)
    team_id = _require_team_id(body)
    requested_keys = body.get("file_keys")
    if requested_keys is not None and not isinstance(requested_keys, list):
        raise HTTPException(status_code=400, detail="file_keys must be a list")
    webhook_id = (body.get("webhook_id") or "").strip() or None
    webhook_secret = (body.get("webhook_secret") or "").strip() or None

    client = FigmaClient(
        base_url=base_url,
        api_token=api_token,
        team_id=team_id,
        auth_kind="pat",
        **tenant_preinstall_transport_kwargs(tenant_id),
    )
    try:
        raw_files = await client.list_files(team_id)
    except FigmaApiError as exc:
        return _auth_failure_response(exc)
    finally:
        await client.aclose()
    files = [file for file in (_normalize_file(file) for file in raw_files) if file["file_key"]]
    if requested_keys:
        wanted = {str(value) for value in requested_keys}
        files = [file for file in files if file["file_key"] in wanted]

    secret_ref = await store.put(api_token, label=f"figma_api_token:{base_url}", tenant_id=tenant_id)
    webhook_secret_ref = None
    if webhook_secret:
        webhook_secret_ref = await store.put(
            webhook_secret, label=f"figma_webhook_secret:{base_url}", tenant_id=tenant_id,
        )
    install_id = await finalize_install(
        pool,
        tenant_id=tenant_id,
        base_url=base_url,
        files=files,
        secret_ref=secret_ref,
        team_id=team_id,
        webhook_secret_ref=webhook_secret_ref,
        auth_kind="pat",
    )
    webhook_registered = False
    if webhook_secret_ref and webhook_id:
        await register_webhook_installation(
            pool,
            tenant_id=tenant_id,
            webhook_id=webhook_id,
            webhook_secret_ref=webhook_secret_ref,
            team_id=team_id,
        )
        webhook_registered = True
    log.info(
        "figma_pat_connect_finalized",
        installation_id=str(install_id), file_count=len(files), webhook_registered=webhook_registered,
    )
    return JSONResponse(content={
        "ok": True,
        "installation_id": str(install_id),
        "file_count": len(files),
        "webhook_registered": webhook_registered,
        "auth_kind": "pat",
    })


__all__ = [
    "router",
    "admin_router",
    "oauth_start",
    "oauth_callback",
    "connect_status",
    "connect_retry",
    "connect_disconnect",
    "deployment_oauth_readiness",
    "_file_key_from_url",
    "_selected_file_keys",
    "_issue_figma_state",
    "_verify_and_consume_figma_state",
    "_exchange_oauth_code",
]
