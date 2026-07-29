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

Token-endpoint URLs are app-level config. Most client ids/secrets still have
app-level env fallbacks, but sources with customer/app-specific credentials
can resolve install-scoped secret refs:
  - `{PROVIDER}_TOKEN_URL`     — override the doc-default endpoint (staging).
  - `{PROVIDER}_CLIENT_ID`     — OAuth app client id fallback.
  - `{PROVIDER}_CLIENT_SECRET` — OAuth app client secret fallback.
  - Ramp `refresh_secret_ref`  — optional encrypted JSON
    `client_id`/`client_secret` payload used before env fallback.

A permanent non-2xx / malformed response raises `OAuthRefreshError`; throttles,
timeouts, and retryable upstream failures are owned by `ProviderTransport` and
surface as `RetryLater` when their bounded inline retry budget is exhausted.
"""
from __future__ import annotations

import base64
import contextlib
import json
import os
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Literal

import httpx
import structlog

from lib.observability import counter, histogram
from lib.shared.env import is_prod
from lib.shared.errors import SecretNotFoundError
from lib.shared.provider_transport import (
    ProviderRateLimited,
    ProviderTimeoutError,
    ProviderTransientError,
    RetryLater,
    parse_retry_after,
)
from lib.shared.tenant_context import bind_tenant
from services.ingest.integrations.provider_transport import (
    ProviderRequestBinding,
    explicit_local_transport,
)
from services.ingest.integrations.provider_transport_runtime import (
    get_provider_transport_runtime,
)
from services.ingest.source_contract.catalog import SOURCE_DEFINITIONS
from services.ingest.source_contract.catalog import effective_request_policy
from services.ingest.source_contract.models import CredentialRefreshDefinition


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
    "OAuth refresh outcomes (success|rate_limited|transport_error|http_4xx|"
    "http_5xx|invalid_response|bad_request_config).",
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
    operation_id: str
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
    # Ramp: the per-install `refresh_secret_ref` can hold a JSON payload with
    # both client_id and client_secret. This supports customer-owned Ramp apps
    # without putting app secrets in process env.
    client_credentials_from_install: bool = False
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


def _config_from_contract(
    provider: str,
    declaration: CredentialRefreshDefinition,
) -> RefreshConfig:
    return RefreshConfig(
        provider=provider,
        operation_id=declaration.operation_id,
        token_url=os.environ.get(
            declaration.token_url_env,
            declaration.default_token_url,
        ),
        grant_type=declaration.grant_type,
        auth_style=declaration.auth_style,
        rotates_refresh_token=declaration.rotates_refresh_token,
        install_table=declaration.install_table,
        default_expires_in=declaration.default_expires_in,
        client_secret_from_install=(
            declaration.client_secret_from_install
        ),
        client_credentials_from_install=(
            declaration.client_credentials_from_install
        ),
        scope=(
            os.environ.get(
                declaration.scope_env,
                declaration.default_scope,
            )
            if declaration.scope_env is not None
            else None
        ),
    )


# Runtime view derived from the one source catalog. Environment overrides are
# resolved once at process start, matching the previous refresh-core behavior.
REFRESH_CONFIGS: dict[str, RefreshConfig] = {
    source.source_id: _config_from_contract(
        source.source_id,
        source.credential_refresh,
    )
    for source in SOURCE_DEFINITIONS
    if source.credential_refresh is not None
}


# Refresh proactively this many seconds BEFORE expiry so an in-flight poll never
# races the cutover. Overridable via OAUTH_REFRESH_SKEW_SECONDS.
DEFAULT_REFRESH_SKEW_SECONDS = 120


def client_credentials_for(provider: str) -> tuple[str | None, str | None]:
    """App-level OAuth client id/secret fallback from env."""
    return (
        os.environ.get(f"{provider.upper()}_CLIENT_ID"),
        os.environ.get(f"{provider.upper()}_CLIENT_SECRET"),
    )


def decode_client_credentials_secret(raw: str | None) -> tuple[str | None, str | None]:
    """Decode client-credentials material stored under `refresh_secret_ref`.

    New Ramp installs store JSON (`client_id` + `client_secret`) so refresh can
    re-mint without raw app secrets in env. Existing installs may still store a
    bare secret string; that legacy shape pairs with the app-level client id.
    """
    text = (raw or "").strip()
    if not text:
        return None, None
    if text.startswith("{"):
        try:
            body = json.loads(text)
        except json.JSONDecodeError:
            return None, text
        if isinstance(body, dict):
            client_id = body.get("client_id")
            client_secret = body.get("client_secret") or body.get("secret")
            return (
                str(client_id).strip() if client_id else None,
                str(client_secret).strip() if client_secret else None,
            )
    return None, text


async def refresh_access_token(
    http: Any,
    config: RefreshConfig,
    *,
    client_id: str | None,
    client_secret: str | None,
    refresh_token: str | None = None,
    now: datetime | None = None,
    request_binding: ProviderRequestBinding | None = None,
) -> RefreshedToken:
    """Exchange at the provider token endpoint and parse the response.

    `refresh_token` is required for the `refresh_token` grant and ignored for
    `client_credentials` (Carta re-mint). Permanent HTTP or payload failures
    raise `OAuthRefreshError`; bounded retry exhaustion raises `RetryLater`.
    """
    now = now or datetime.now(timezone.utc)
    binding = request_binding or ProviderRequestBinding(
        source=config.provider,
        tenant_id=None,
        installation_id=None,
        transport=None,
        request_policy=lambda operation: effective_request_policy(
            config.provider,
            operation,
        ),
        quota_resolver=None,
        allow_unlimited_local=explicit_local_transport(
            requested=None,
            has_local_injection=True,
        ),
        require_tenant_installation=False,
    )

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

    async def _once() -> httpx.Response:
        _ATTEMPTS.inc(
            provider=config.provider,
            grant_type=config.grant_type,
        )
        try:
            response = await http.post(
                config.token_url,
                data=form,
                headers=headers,
            )
        except httpx.TimeoutException as exc:
            _OUTCOMES.inc(
                provider=config.provider,
                outcome="transport_error",
            )
            raise ProviderTimeoutError(
                f"{config.provider} token request timed out",
                source=config.provider,
                operation=config.operation_id,
                error_type=type(exc).__name__,
            ) from exc
        except httpx.TransportError as exc:
            _OUTCOMES.inc(
                provider=config.provider,
                outcome="transport_error",
            )
            raise ProviderTransientError(
                f"{config.provider} token transport error",
                source=config.provider,
                operation=config.operation_id,
                error_type=type(exc).__name__,
            ) from exc
        if response.status_code == 429:
            _OUTCOMES.inc(
                provider=config.provider,
                outcome="rate_limited",
            )
            raise ProviderRateLimited(
                f"{config.provider} token endpoint rate limit",
                retry_after_seconds=parse_retry_after(
                    response.headers.get("Retry-After"),
                ),
                status_code=429,
                header_parser_id="http.retry_after",
                source=config.provider,
                operation=config.operation_id,
            )
        if response.status_code >= 500:
            _OUTCOMES.inc(provider=config.provider, outcome="http_5xx")
            raise ProviderTransientError(
                f"{config.provider} token endpoint returned "
                f"{response.status_code}",
                source=config.provider,
                operation=config.operation_id,
                http_status=response.status_code,
            )
        return response

    try:
        resp = await binding.execute(config.operation_id, _once)
    except RetryLater:
        _DURATION.observe(
            time.monotonic() - started,
            provider=config.provider,
        )
        raise
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

    try:
        expires_in = _coerce_int(body.get("expires_in"), config.default_expires_in)
    except ValueError as exc:
        _OUTCOMES.inc(provider=config.provider, outcome="invalid_response")
        raise OAuthRefreshError(
            config.provider,
            "token endpoint response has an invalid expires_in",
            status=422,
        ) from exc

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
    request_binding: ProviderRequestBinding | None = None,
    renewal_lease: Any | None = None,
    minimum_expires_in_seconds: int | None = None,
) -> RefreshedToken:
    """Perform the token exchange AND persist the result onto the install row.

    Persistence mirrors the env-encrypted secret-store model: `put` a new
    ciphertext row for the access token (and the rotated refresh token, if any),
    then UPDATE the install's `secret_ref` / `refresh_secret_ref` /
    `token_expires_at` to point at the new refs. The refresh-managed install
    tables share these column names + an `id` PK, so this is generic.

    Permanent refresh failures raise `OAuthRefreshError`; retryable failures
    raise `RetryLater` so the workflow can persist `next_attempt_at`.

    A bounded lifecycle caller supplies ``renewal_lease``.  In that mode the
    exact active installation is checked before secret creation and the final
    installation mutation is fenced by the same owner/version lease.  A stale
    worker therefore cannot overwrite a rotated credential; any newly-created
    opaque refs are best-effort deleted if the fenced update loses ownership.
    """
    now = now or datetime.now(timezone.utc)
    if is_prod() and renewal_lease is None:
        raise RuntimeError(
            "production credential refresh must hold an exact renewal lease"
        )
    if minimum_expires_in_seconds is not None and (
        isinstance(minimum_expires_in_seconds, bool)
        or not isinstance(minimum_expires_in_seconds, int)
        or minimum_expires_in_seconds <= 0
    ):
        raise ValueError("minimum_expires_in_seconds must be a positive integer")
    config = REFRESH_CONFIGS[provider]
    if renewal_lease is not None:
        await _assert_active_renewal_lease(
            pool=pool,
            provider=provider,
            config=config,
            tenant_id=tenant_id,
            install_row_id=install_row_id,
            renewal_lease=renewal_lease,
        )
    client_id, env_client_secret = client_credentials_for(provider)

    refresh_token = None
    if config.grant_type == "refresh_token":
        try:
            refresh_token = await _resolve_secret(
                secret_store,
                refresh_secret_ref,
                tenant_id=tenant_id,
            )
        except SecretNotFoundError as exc:
            raise OAuthRefreshError(
                provider,
                "refresh credential is unavailable for this installation",
                status=401,
            ) from exc
        if not refresh_token:
            raise OAuthRefreshError(
                provider,
                "refresh credential is unavailable for this installation",
                status=401,
            )

    client_secret = env_client_secret
    if config.client_credentials_from_install:
        try:
            raw_install_secret = await _resolve_secret(
                secret_store,
                refresh_secret_ref,
                tenant_id=tenant_id,
            )
        except SecretNotFoundError as exc:
            raise OAuthRefreshError(
                provider,
                "client credential is unavailable for this installation",
                status=401,
            ) from exc
        install_client_id, install_client_secret = decode_client_credentials_secret(
            raw_install_secret,
        )
        client_id = install_client_id or client_id
        client_secret = install_client_secret or env_client_secret
    elif config.client_secret_from_install:
        # Carta: the client_credentials secret is the per-install material
        # stored under refresh_secret_ref (not an OAuth refresh token).
        try:
            client_secret = await _resolve_secret(
                secret_store,
                refresh_secret_ref,
                tenant_id=tenant_id,
            ) or env_client_secret
        except SecretNotFoundError as exc:
            raise OAuthRefreshError(
                provider,
                "client credential is unavailable for this installation",
                status=401,
            ) from exc

    if request_binding is None:
        runtime = get_provider_transport_runtime()
        request_binding = ProviderRequestBinding(
            source=provider,
            tenant_id=str(tenant_id),
            installation_id=str(install_row_id),
            transport=runtime.transport if runtime is not None else None,
            request_policy=lambda operation: effective_request_policy(
                provider,
                operation,
            ),
            quota_resolver=(
                runtime.quota_resolver if runtime is not None else None
            ),
            allow_unlimited_local=explicit_local_transport(
                requested=None,
                has_local_injection=http is not None,
            ),
        )
    refreshed = await refresh_access_token(
        http,
        config,
        client_id=client_id,
        client_secret=client_secret,
        refresh_token=refresh_token,
        now=now,
        request_binding=request_binding,
    )
    if (
        minimum_expires_in_seconds is not None
        and refreshed.expires_at
        <= now + timedelta(seconds=minimum_expires_in_seconds)
    ):
        raise OAuthRefreshError(
            provider,
            "token endpoint response expires before the renewal safety window",
            status=422,
        )

    created_refs: list[str] = []
    try:
        new_access_ref = await secret_store.put(
            refreshed.access_token,
            label=f"{provider}_access_token:{install_row_id}",
            tenant_id=tenant_id,
        )
        created_refs.append(new_access_ref)
        new_refresh_ref = refresh_secret_ref
        if refreshed.refresh_token:
            new_refresh_ref = await secret_store.put(
                refreshed.refresh_token,
                label=f"{provider}_refresh_token:{install_row_id}",
                tenant_id=tenant_id,
            )
            created_refs.append(new_refresh_ref)

        if renewal_lease is None:
            # Column names are identical across the refresh-managed install
            # tables; the table name comes from REFRESH_CONFIGS, which is
            # derived from the immutable catalog rather than request data.
            update_result = await pool.execute(
                f"UPDATE {config.install_table} "
                "SET secret_ref = $1, refresh_secret_ref = $2, token_expires_at = $3 "
                "WHERE id = $4 AND tenant_id = $5 AND disabled_at IS NULL",
                new_access_ref, new_refresh_ref, refreshed.expires_at,
                install_row_id, tenant_id,
            )
            if update_result == "UPDATE 0":
                raise OAuthRefreshError(
                    provider,
                    "installation unavailable while persisting refreshed credentials",
                    status=401,
                )
        else:
            await _persist_with_renewal_lease(
                pool=pool,
                provider=provider,
                config=config,
                tenant_id=tenant_id,
                install_row_id=install_row_id,
                access_ref=new_access_ref,
                refresh_ref=new_refresh_ref,
                expires_at=refreshed.expires_at,
                renewal_lease=renewal_lease,
            )
    except Exception:
        # A fenced update can legitimately lose ownership after the provider
        # exchange.  Do not leave newly-created secret rows orphaned.  Existing
        # refs are never put into ``created_refs`` and are therefore untouched.
        delete = getattr(secret_store, "delete", None)
        if callable(delete):
            for ref in created_refs:
                with contextlib.suppress(Exception):  # cleanup must not mask root cause
                    await delete(ref, tenant_id=tenant_id)
        raise
    log.info(
        "oauth_refresh.persisted",
        provider=provider, install_row_id=str(install_row_id),
        rotated=bool(refreshed.refresh_token),
    )
    return refreshed


def _lease_identity(
    renewal_lease: Any,
    *,
    provider: str,
    tenant_id: Any,
    install_row_id: Any,
) -> tuple[str, int]:
    """Validate the generic job lease before it fences an install mutation."""

    key = getattr(renewal_lease, "key", None)
    owner = getattr(renewal_lease, "owner", None)
    version = getattr(renewal_lease, "version", None)
    if (
        key is None
        or getattr(key, "source_id", None) != provider
        or str(getattr(key, "tenant_id", "")) != str(tenant_id)
        or str(getattr(key, "installation_id", "")) != str(install_row_id)
        or getattr(key, "target_key", None) != "installation"
        or not isinstance(owner, str)
        or not owner
        or isinstance(version, bool)
        or not isinstance(version, int)
        or version < 1
    ):
        raise OAuthRefreshError(
            provider,
            "credential renewal lease does not match the exact installation",
            status=401,
        )
    return owner, version


async def _assert_active_renewal_lease(
    *,
    pool: Any,
    provider: str,
    config: RefreshConfig,
    tenant_id: Any,
    install_row_id: Any,
    renewal_lease: Any,
) -> None:
    """Perform a short, pre-secret active-install/fence validation."""

    owner, version = _lease_identity(
        renewal_lease,
        provider=provider,
        tenant_id=tenant_id,
        install_row_id=install_row_id,
    )
    async with pool.acquire() as conn:
        async with conn.transaction():
            async with bind_tenant(conn, tenant_id) as tctx:
                row = await tctx.fetchrow(
                    f"""
                    SELECT install.id
                      FROM {config.install_table} install
                      JOIN source_renewal_jobs job
                        ON job.source_id = $3
                       AND job.tenant_id = install.tenant_id
                       AND job.installation_id = install.id
                       AND job.target_key = 'installation'
                       AND job.lease_owner = $4
                       AND job.lease_version = $5
                       AND job.lease_expires_at > now()
                     WHERE install.id = $1
                       AND install.tenant_id = $2
                       AND install.disabled_at IS NULL
                    """,
                    install_row_id,
                    tenant_id,
                    provider,
                    owner,
                    version,
                )
    if row is None:
        raise OAuthRefreshError(
            provider,
            "installation unavailable before credential renewal",
            status=401,
        )


async def _persist_with_renewal_lease(
    *,
    pool: Any,
    provider: str,
    config: RefreshConfig,
    tenant_id: Any,
    install_row_id: Any,
    access_ref: str,
    refresh_ref: str | None,
    expires_at: datetime,
    renewal_lease: Any,
) -> None:
    """Fence the final token-ref mutation with the claimed renewal lease."""

    owner, version = _lease_identity(
        renewal_lease,
        provider=provider,
        tenant_id=tenant_id,
        install_row_id=install_row_id,
    )
    async with pool.acquire() as conn:
        async with conn.transaction():
            async with bind_tenant(conn, tenant_id) as tctx:
                row = await tctx.fetchrow(
                    f"""
                    WITH held_lease AS MATERIALIZED (
                        SELECT source_id
                          FROM source_renewal_jobs
                         WHERE source_id = $6
                           AND tenant_id = $5
                           AND installation_id = $4
                           AND target_key = 'installation'
                           AND lease_owner = $7
                           AND lease_version = $8
                           AND lease_expires_at > now()
                         FOR UPDATE
                    )
                    UPDATE {config.install_table}
                       SET secret_ref = $1,
                           refresh_secret_ref = $2,
                           token_expires_at = $3
                     WHERE id = $4
                       AND tenant_id = $5
                       AND disabled_at IS NULL
                       AND EXISTS (SELECT 1 FROM held_lease)
                    RETURNING id
                    """,
                    access_ref,
                    refresh_ref,
                    expires_at,
                    install_row_id,
                    tenant_id,
                    provider,
                    owner,
                    version,
                )
    if row is None:
        raise OAuthRefreshError(
            provider,
            "credential renewal lease lost before installation persistence",
            status=401,
        )


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
    request_binding: ProviderRequestBinding | None = None,
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
        return await _refresh_through_renewal_job(
            provider=provider,
            pool=pool,
            secret_store=secret_store,
            http=http,
            tenant_id=tenant_id,
            install_row_id=install_row_id,
            now=now,
            force=force,
            request_binding=request_binding,
        )
    # Still valid — return the current token plaintext.
    token = await _resolve_secret(secret_store, current_access_ref, tenant_id=tenant_id)
    if token is None:
        # No cached token but not yet expired per the row — refresh to recover.
        return await _refresh_through_renewal_job(
            provider=provider,
            pool=pool,
            secret_store=secret_store,
            http=http,
            tenant_id=tenant_id,
            install_row_id=install_row_id,
            now=now,
            force=True,
            request_binding=request_binding,
        )
    return token


async def _refresh_through_renewal_job(
    *,
    provider: str,
    pool: Any,
    secret_store: Any,
    http: httpx.AsyncClient,
    tenant_id: Any,
    install_row_id: Any,
    now: datetime,
    force: bool,
    request_binding: ProviderRequestBinding | None,
) -> str:
    """Use the same exact durable lease for scheduled and reactive refresh.

    A 401-triggered refresh is not an exception to the single-writer rule.
    It can race the periodic renewal precisely when a rotating refresh token is
    most vulnerable. The bounded lifecycle owns the provider exchange and
    fenced persistence; this helper reads the resulting opaque secret only
    after a successful/no-longer-due settlement.
    """

    # Local import avoids a module cycle: bounded renewal delegates its actual
    # token exchange to ``refresh_and_persist`` above.
    from services.ingest.integrations.bounded_renewal import (
        RenewalInvocation,
        run_credential_renewal,
    )

    outcome = await run_credential_renewal(
        RenewalInvocation(
            pool=pool,
            tenant_id=tenant_id,
            installation_id=install_row_id,
            target_key="installation",
            secret_store=secret_store,
            http=http,
            request_binding=request_binding,
            now=now,
            force=force,
        ),
        source_id=provider,
    )
    if outcome.state == "reauthorization_required":
        raise OAuthRefreshError(
            provider,
            "credential renewal requires reauthorization",
            status=401,
        )
    if outcome.state == "manual_reconciliation_required":
        raise OAuthRefreshError(
            provider,
            "credential renewal requires operator reconciliation",
            status=409,
        )
    if outcome.state in {"retry_scheduled", "lease_unavailable"}:
        raise OAuthRefreshError(
            provider,
            "credential renewal is durably pending",
            status=503,
        )

    config = REFRESH_CONFIGS[provider]
    async with pool.acquire() as conn:
        async with conn.transaction():
            async with bind_tenant(conn, tenant_id) as tctx:
                row = await tctx.fetchrow(
                    f"""
                    SELECT secret_ref
                      FROM {config.install_table}
                     WHERE id = $1
                       AND tenant_id = $2
                       AND disabled_at IS NULL
                    """,
                    install_row_id,
                    tenant_id,
                )
    if row is None:
        raise OAuthRefreshError(
            provider,
            "installation unavailable after credential renewal",
            status=401,
        )
    token = await _resolve_secret(
        secret_store,
        row["secret_ref"],
        tenant_id=tenant_id,
    )
    if token is None:
        raise OAuthRefreshError(
            provider,
            "credential renewal did not persist an access token",
            status=422,
        )
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
    request_binding: ProviderRequestBinding | None = None,
) -> str | None:
    """Reactive 401 re-mint for the poll/backfill read clients.

    Force a refresh+persist and return the new access-token plaintext so the
    client can retry the request once. Returns None when the client lacks
    refresh deps or a permanent refresh error occurs. `RetryLater` deliberately
    propagates so a provider cooldown is durably scheduled instead of being
    mistaken for an authentication failure.
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
            request_binding=request_binding,
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
    if value is None:
        return default
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        raise ValueError("expires_in must be a positive integer") from None
    if parsed <= 0:
        raise ValueError("expires_in must be a positive integer")
    return parsed


__all__ = [
    "OAuthRefreshError",
    "RefreshConfig",
    "RefreshedToken",
    "REFRESH_CONFIGS",
    "DEFAULT_REFRESH_SKEW_SECONDS",
    "client_credentials_for",
    "decode_client_credentials_secret",
    "refresh_access_token",
    "needs_refresh",
    "refresh_and_persist",
    "ensure_fresh_access_token",
    "refresh_on_unauthorized",
]
