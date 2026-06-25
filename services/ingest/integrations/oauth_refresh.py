"""services/ingest/integrations/oauth_refresh.py — shared OAuth token-refresh core.

The poll-based finance/recruiting sources authenticate with short-lived OAuth 2.0
**access tokens** (~1 h) plus a longer-lived **refresh token**. This module owns
the *token-endpoint exchange* — the one piece every provider shares — so the
proactive (oauth_poller) and reactive (client 401 re-mint) triggers can both call
one well-tested function.

Provider matrix (VERIFIED against official docs — see each fixture's
`_meta.source`):

  - **QuickBooks (Intuit)** — `grant_type=refresh_token`, HTTP Basic
    (client_id:client_secret), **rotates** the refresh token on every exchange
    (the new `refresh_token` MUST be persisted or the next refresh 400s),
    `expires_in=3600`. Endpoint: `oauth.platform.intuit.com/oauth2/v1/tokens/bearer`.
  - **Ramp** — has NO long-lived refresh credential in our flow: access tokens
    (~1 h) are **RE-MINTED** via `grant_type=client_credentials` at
    `POST https://api.ramp.com/developer/v1/token`, HTTP Basic
    (client_id:client_secret), with the REQUIRED `scope` form field
    (docs.ramp.com authorization — `scope` is mandatory for this grant). No
    refresh token is returned for client_credentials.
  - **Gusto** — `grant_type=refresh_token`, client creds in the **body**,
    rotates, `expires_in` (~7200).
  - **Carta** — has **NO refresh-token grant**. Access tokens expire hourly and
    are **RE-MINTED** via `grant_type=client_credentials` at
    `POST https://login.app.carta.com/o/access_token/` (trailing slash), HTTP
    Basic (client_id:client_secret) + the REQUIRED space-delimited `scope`
    form field (docs.carta.com/carta/docs/client-credentials-flow). The stored
    `refresh_secret_ref` holds the client-credentials *secret*, not an OAuth
    refresh token. No refresh token is returned.
  - **LinkedIn** — `grant_type=refresh_token`, client creds in the **body**,
    programmatic refresh tokens for approved partners only, access tokens
    refreshed at `POST https://www.linkedin.com/oauth/v2/accessToken`.
    LinkedIn returns a refresh token in the refresh response; persist the
    returned value so Fyralis follows the provider payload if the token changes.

Token-endpoint URLs + client credentials are **app-level config** (env), not
per-install secrets:
  - `{PROVIDER}_TOKEN_URL`     — override the doc-default endpoint (staging).
  - `{PROVIDER}_CLIENT_ID`     — the OAuth app client id.
  - `{PROVIDER}_CLIENT_SECRET` — the OAuth app client secret.

A non-2xx / malformed response raises `OAuthRefreshError`; callers translate that
into a *degraded* shard (never a crash, never a silent data drop).
"""
from __future__ import annotations

import base64
import os
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Literal

import httpx
import structlog

from lib.observability import counter, histogram


log = structlog.get_logger("integrations.oauth_refresh")


# Provider is bounded by the refresh-capable source set (QBO/Ramp/Gusto/Carta/
# LinkedIn today); outcome is a closed enum so silent token-rotation failure becomes
# an alertable rate instead of a slow-burn outage.
_ATTEMPTS = counter(
    "oauth_refresh_attempts_total",
    "OAuth token-endpoint exchanges attempted, by provider and grant type.",
    ("provider", "grant_type"),
)
_OUTCOMES = counter(
    "oauth_refresh_outcomes_total",
    "OAuth refresh outcomes (success|transport_error|http_4xx|http_5xx|invalid_response|bad_request_config).",
    ("provider", "outcome"),
)
_DURATION = histogram(
    "oauth_refresh_duration_seconds",
    "Token-endpoint exchange latency by provider.",
    ("provider",),
    buckets=(0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0),
)


class OAuthRefreshError(Exception):
    """A token refresh / re-mint failed. Carries the provider + (optional) HTTP
    status so the caller can mark the shard degraded with a precise reason."""

    def __init__(self, provider: str, message: str, *, status: int | None = None) -> None:
        super().__init__(message)
        self.provider = provider
        self.status = status


GrantType = Literal["refresh_token", "client_credentials"]
AuthStyle = Literal["basic", "body"]


@dataclass(frozen=True)
class RefreshConfig:
    provider: str
    token_url: str
    grant_type: GrantType
    auth_style: AuthStyle          # how client creds are presented
    rotates_refresh_token: bool    # persist the returned refresh_token if True
    install_table: str             # per-provider install table for persistence
    default_expires_in: int = 3600
    # Carta: the per-install `refresh_secret_ref` holds the client_credentials
    # SECRET (not an OAuth refresh token), so the client_secret is resolved from
    # the install rather than the app-level env var.
    client_secret_from_install: bool = False
    # client_credentials grants that REQUIRE a `scope` form field (Ramp —
    # docs.ramp.com; Carta — docs.carta.com client-credentials flow). None →
    # no scope is sent.
    scope: str | None = None


@dataclass(frozen=True)
class RefreshedToken:
    """The result of a successful exchange. `refresh_token` is None for
    client_credentials (Carta) and for providers that did not rotate it."""
    access_token: str
    refresh_token: str | None
    expires_in: int
    obtained_at: datetime

    @property
    def expires_at(self) -> datetime:
        return self.obtained_at + timedelta(seconds=self.expires_in)


def _cfg(
    provider: str, default_url: str, grant: GrantType, auth: AuthStyle,
    rotates: bool, install_table: str, default_expires_in: int = 3600,
    client_secret_from_install: bool = False, scope: str | None = None,
) -> RefreshConfig:
    url = os.environ.get(f"{provider.upper()}_TOKEN_URL", default_url)
    return RefreshConfig(
        provider=provider, token_url=url, grant_type=grant, auth_style=auth,
        rotates_refresh_token=rotates, install_table=install_table,
        default_expires_in=default_expires_in,
        client_secret_from_install=client_secret_from_install,
        scope=scope,
    )


# Doc-derived provider configs. token_url overridable via {PROVIDER}_TOKEN_URL.
REFRESH_CONFIGS: dict[str, RefreshConfig] = {
    "quickbooks": _cfg(
        "quickbooks",
        "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer",
        "refresh_token", "basic", rotates=True,
        install_table="quickbooks_installations", default_expires_in=3600,
    ),
    "ramp": _cfg(
        # Ramp client-credentials RE-MINT (verified docs.ramp.com authorization
        # + OpenAPI /developer/v1/token): HTTP Basic client creds, form body
        # grant_type=client_credentials + REQUIRED scope; access_token
        # expires_in ~3600; NO refresh token for this grant.
        "ramp", "https://api.ramp.com/developer/v1/token",
        "client_credentials", "basic", rotates=False,
        install_table="ramp_installations", default_expires_in=3600,
        scope=os.environ.get(
            "RAMP_OAUTH_SCOPES",
            "transactions:read reimbursements:read cards:read users:read "
            "business:read",
        ),
    ),
    "gusto": _cfg(
        # Gusto refresh (verified docs.gusto.com/app-integrations/docs/oauth2):
        # POST https://api.gusto.com/oauth/token, client creds in the BODY
        # (client_id/client_secret, no Basic), grant_type=refresh_token;
        # access_token expires in 7200 s and the refresh token ROTATES
        # (single-use). TODO(human): the doc example sends the body as
        #   Content-Type application/json; the shared exchange posts RFC-6749
        #   x-www-form-urlencoded. Verify accepted on the first real exchange.
        "gusto", "https://api.gusto.com/oauth/token",
        "refresh_token", "body", rotates=True,
        install_table="gusto_installations", default_expires_in=7200,
    ),
    "carta": _cfg(
        # Carta client-credentials RE-MINT (verified
        # docs.carta.com/carta/docs/client-credentials-flow): the token
        # endpoint is POST https://login.app.carta.com/o/access_token/ (note
        # the trailing slash) with HTTP **Basic** client auth
        # (base64(client_id:client_secret)) + x-www-form-urlencoded body
        # carrying the REQUIRED space-delimited `scope`. Access tokens live
        # ~1 h; NO refresh token is returned — re-mint hourly. Default scopes
        # are the four the /v1alpha1 read surface needs (from the Issuer OAS
        # security entries); override via CARTA_OAUTH_SCOPES.
        # TODO(human): Carta's doc example sends `grant_type=CLIENT_CREDENTIALS`
        #   (uppercase); the shared exchange sends the RFC-6749 lowercase
        #   value. Verify on the first real (partner-gated) exchange.
        "carta", "https://login.app.carta.com/o/access_token/",
        "client_credentials", "basic", rotates=False,
        install_table="carta_installations", default_expires_in=3600,
        client_secret_from_install=True,
        scope=os.environ.get(
            "CARTA_OAUTH_SCOPES",
            "read_issuer_info read_issuer_stakeholders "
            "read_issuer_shareclasses read_issuer_securities",
        ),
    ),
    "linkedin": _cfg(
        # LinkedIn programmatic refresh-token exchange (Microsoft Learn,
        # programmatic-refresh-tokens): form-encoded grant_type=refresh_token
        # with refresh_token + client_id + client_secret in the body. The
        # endpoint returns a refresh_token alongside the new access token; store
        # it so the install row tracks the provider's current credential.
        "linkedin", "https://www.linkedin.com/oauth/v2/accessToken",
        "refresh_token", "body", rotates=True,
        install_table="linkedin_installations", default_expires_in=86400,
    ),
}


# Refresh proactively this many seconds BEFORE expiry so an in-flight poll never
# races the cutover. Overridable via OAUTH_REFRESH_SKEW_SECONDS.
DEFAULT_REFRESH_SKEW_SECONDS = 120


def client_credentials_for(provider: str) -> tuple[str | None, str | None]:
    """App-level OAuth client id/secret from env (never a per-install secret)."""
    return (
        os.environ.get(f"{provider.upper()}_CLIENT_ID"),
        os.environ.get(f"{provider.upper()}_CLIENT_SECRET"),
    )


async def refresh_access_token(
    http: httpx.AsyncClient,
    config: RefreshConfig,
    *,
    client_id: str | None,
    client_secret: str | None,
    refresh_token: str | None = None,
    now: datetime | None = None,
) -> RefreshedToken:
    """Exchange at the provider token endpoint and parse the response.

    `refresh_token` is required for the `refresh_token` grant and ignored for
    `client_credentials` (Carta re-mint). Raises `OAuthRefreshError` on a
    transport error, a non-2xx, or a response missing `access_token`.
    """
    now = now or datetime.now(timezone.utc)
    _ATTEMPTS.inc(provider=config.provider, grant_type=config.grant_type)

    form: dict[str, str] = {"grant_type": config.grant_type}
    if config.grant_type == "refresh_token":
        if not refresh_token:
            _OUTCOMES.inc(provider=config.provider, outcome="bad_request_config")
            raise OAuthRefreshError(
                config.provider, "refresh_token grant requires a refresh_token",
            )
        form["refresh_token"] = refresh_token
    elif config.grant_type == "client_credentials" and config.scope:
        # Ramp (docs.ramp.com) and Carta (docs.carta.com client-credentials
        # flow) both REQUIRE `scope` for this grant; providers without a
        # configured scope send none.
        form["scope"] = config.scope

    headers = {
        "Accept": "application/json",
        "Content-Type": "application/x-www-form-urlencoded",
    }
    if config.auth_style == "basic":
        if not (client_id and client_secret):
            _OUTCOMES.inc(provider=config.provider, outcome="bad_request_config")
            raise OAuthRefreshError(
                config.provider,
                "missing client_id/client_secret for Basic auth "
                f"(set {config.provider.upper()}_CLIENT_ID/_CLIENT_SECRET)",
            )
        basic = base64.b64encode(
            f"{client_id}:{client_secret}".encode("utf-8")
        ).decode("ascii")
        headers["Authorization"] = f"Basic {basic}"
    else:  # body
        # client_credentials (and Gusto's refresh) carry creds in the form body.
        if client_id is not None:
            form["client_id"] = client_id
        if client_secret is not None:
            form["client_secret"] = client_secret

    started = time.monotonic()
    try:
        resp = await http.post(config.token_url, data=form, headers=headers)
    except httpx.TransportError as exc:
        _DURATION.observe(time.monotonic() - started, provider=config.provider)
        _OUTCOMES.inc(provider=config.provider, outcome="transport_error")
        raise OAuthRefreshError(
            config.provider, f"transport error: {type(exc).__name__}",
        ) from exc
    _DURATION.observe(time.monotonic() - started, provider=config.provider)

    if resp.status_code // 100 != 2:
        # 400 invalid_grant (revoked / stale rotated refresh token) and 401
        # (bad client creds) are the auth-degraded signals.
        _OUTCOMES.inc(
            provider=config.provider,
            outcome="http_5xx" if resp.status_code >= 500 else "http_4xx",
        )
        raise OAuthRefreshError(
            config.provider,
            f"token endpoint returned {resp.status_code}",
            status=resp.status_code,
        )

    body = _safe_json(resp)
    if not isinstance(body, dict):
        _OUTCOMES.inc(provider=config.provider, outcome="invalid_response")
        raise OAuthRefreshError(
            config.provider, "token endpoint response was not a JSON object",
        )
    access = body.get("access_token")
    if not isinstance(access, str) or not access:
        _OUTCOMES.inc(provider=config.provider, outcome="invalid_response")
        raise OAuthRefreshError(
            config.provider, "token endpoint response missing access_token",
        )
    _OUTCOMES.inc(provider=config.provider, outcome="success")

    new_refresh: str | None = None
    if config.grant_type == "refresh_token" and config.rotates_refresh_token:
        returned = body.get("refresh_token")
        # If the provider rotates but echoed nothing, keep the prior token.
        new_refresh = returned if isinstance(returned, str) and returned else refresh_token

    expires_in = _coerce_int(body.get("expires_in"), config.default_expires_in)

    return RefreshedToken(
        access_token=access,
        refresh_token=new_refresh,
        expires_in=expires_in,
        obtained_at=now,
    )


def needs_refresh(
    token_expires_at: datetime | None,
    *,
    now: datetime,
    skew_seconds: int = DEFAULT_REFRESH_SKEW_SECONDS,
) -> bool:
    """Proactive trigger: refresh when the access token is missing an expiry
    (unknown → refresh defensively) or is within `skew_seconds` of expiring."""
    if token_expires_at is None:
        return True
    return token_expires_at <= now + timedelta(seconds=skew_seconds)


async def _resolve_secret(secret_store: Any, ref: str | None, *, tenant_id: Any) -> str | None:
    if not ref:
        return None
    raw = await secret_store.get(ref, tenant_id=tenant_id)
    return raw.decode("utf-8") if isinstance(raw, bytes) else str(raw)


async def refresh_and_persist(
    *,
    provider: str,
    pool: Any,
    secret_store: Any,
    http: httpx.AsyncClient,
    tenant_id: Any,
    install_row_id: Any,
    refresh_secret_ref: str | None,
    now: datetime | None = None,
) -> RefreshedToken:
    """Perform the token exchange AND persist the result onto the install row.

    Persistence mirrors the env-encrypted secret-store model: `put` a new
    ciphertext row for the access token (and the rotated refresh token, if any),
    then UPDATE the install's `secret_ref` / `refresh_secret_ref` /
    `token_expires_at` to point at the new refs. The refresh-managed install
    tables share these column names + an `id` PK, so this is generic.

    Raises `OAuthRefreshError` on a failed exchange — the caller marks the shard
    degraded (it never crashes the worker or silently drops data).
    """
    now = now or datetime.now(timezone.utc)
    config = REFRESH_CONFIGS[provider]
    client_id, env_client_secret = client_credentials_for(provider)

    refresh_token = None
    if config.grant_type == "refresh_token":
        refresh_token = await _resolve_secret(
            secret_store, refresh_secret_ref, tenant_id=tenant_id,
        )

    client_secret = env_client_secret
    if config.client_secret_from_install:
        # Carta: the client_credentials secret is the per-install material
        # stored under refresh_secret_ref (not an OAuth refresh token).
        client_secret = await _resolve_secret(
            secret_store, refresh_secret_ref, tenant_id=tenant_id,
        ) or env_client_secret

    refreshed = await refresh_access_token(
        http, config,
        client_id=client_id, client_secret=client_secret,
        refresh_token=refresh_token, now=now,
    )

    new_access_ref = await secret_store.put(
        refreshed.access_token,
        label=f"{provider}_access_token:{install_row_id}",
        tenant_id=tenant_id,
    )
    new_refresh_ref = refresh_secret_ref
    if refreshed.refresh_token:
        new_refresh_ref = await secret_store.put(
            refreshed.refresh_token,
            label=f"{provider}_refresh_token:{install_row_id}",
            tenant_id=tenant_id,
        )

    # Column names are identical across the four install tables; the table name
    # comes from the trusted REFRESH_CONFIGS literal (never user input).
    await pool.execute(
        f"UPDATE {config.install_table} "
        "SET secret_ref = $1, refresh_secret_ref = $2, token_expires_at = $3 "
        "WHERE id = $4 AND tenant_id = $5",
        new_access_ref, new_refresh_ref, refreshed.expires_at,
        install_row_id, tenant_id,
    )
    log.info(
        "oauth_refresh.persisted",
        provider=provider, install_row_id=str(install_row_id),
        rotated=bool(refreshed.refresh_token),
    )
    return refreshed


async def ensure_fresh_access_token(
    *,
    provider: str,
    pool: Any,
    secret_store: Any,
    http: httpx.AsyncClient,
    tenant_id: Any,
    install_row_id: Any,
    current_access_ref: str | None,
    refresh_secret_ref: str | None,
    token_expires_at: datetime | None,
    force: bool = False,
    now: datetime | None = None,
    skew_seconds: int = DEFAULT_REFRESH_SKEW_SECONDS,
) -> str:
    """Return a valid access token, refreshing first if needed.

    The single entry point for BOTH triggers:
      - proactive (oauth poll sweep): `force=False` — refresh only when within
        the expiry skew.
      - reactive (client 401 re-mint): `force=True` — refresh unconditionally.

    On success returns the (possibly refreshed) access-token plaintext. Raises
    `OAuthRefreshError` if a needed refresh fails (→ caller marks degraded).
    """
    now = now or datetime.now(timezone.utc)
    if force or needs_refresh(token_expires_at, now=now, skew_seconds=skew_seconds):
        refreshed = await refresh_and_persist(
            provider=provider, pool=pool, secret_store=secret_store, http=http,
            tenant_id=tenant_id, install_row_id=install_row_id,
            refresh_secret_ref=refresh_secret_ref, now=now,
        )
        return refreshed.access_token
    # Still valid — return the current token plaintext.
    token = await _resolve_secret(secret_store, current_access_ref, tenant_id=tenant_id)
    if token is None:
        # No cached token but not yet expired per the row — refresh to recover.
        refreshed = await refresh_and_persist(
            provider=provider, pool=pool, secret_store=secret_store, http=http,
            tenant_id=tenant_id, install_row_id=install_row_id,
            refresh_secret_ref=refresh_secret_ref, now=now,
        )
        return refreshed.access_token
    return token


async def refresh_on_unauthorized(
    *,
    provider: str,
    pool: Any,
    secret_store: Any,
    http: httpx.AsyncClient,
    tenant_id: Any,
    install_row_id: Any,
    current_access_ref: str | None,
    refresh_secret_ref: str | None,
) -> str | None:
    """Reactive 401 re-mint for the poll/backfill read clients.

    Force a refresh+persist and return the new access-token plaintext so the
    client can retry the request once. Returns None — NEVER raises — when the
    client lacks refresh deps (e.g. spammer mode with a preset token) or the
    refresh itself fails; the caller then surfaces the original 401, which the
    shard_fetch boundary records as a degraded shard (state='failed' +
    last_error) rather than crashing the worker.
    """
    if not (pool is not None and secret_store is not None
            and tenant_id is not None and install_row_id is not None):
        return None
    if provider not in REFRESH_CONFIGS:
        return None
    try:
        return await ensure_fresh_access_token(
            provider=provider, pool=pool, secret_store=secret_store, http=http,
            tenant_id=tenant_id, install_row_id=install_row_id,
            current_access_ref=current_access_ref,
            refresh_secret_ref=refresh_secret_ref,
            token_expires_at=None, force=True,
        )
    except OAuthRefreshError as exc:
        log.warning(
            "oauth_refresh.reactive_failed",
            provider=provider, status=exc.status,
        )
        return None


def _safe_json(resp: httpx.Response) -> Any:
    try:
        return resp.json()
    except Exception:  # noqa: BLE001
        return None


def _coerce_int(value: Any, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


__all__ = [
    "OAuthRefreshError",
    "RefreshConfig",
    "RefreshedToken",
    "REFRESH_CONFIGS",
    "DEFAULT_REFRESH_SKEW_SECONDS",
    "client_credentials_for",
    "refresh_access_token",
    "needs_refresh",
    "refresh_and_persist",
    "ensure_fresh_access_token",
    "refresh_on_unauthorized",
]
