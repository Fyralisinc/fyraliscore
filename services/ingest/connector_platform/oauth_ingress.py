"""Connector-authoritative OAuth ingress with legacy-compatible persistence."""

from __future__ import annotations

import json
from collections.abc import Awaitable, Callable
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import NAMESPACE_URL, UUID, uuid4, uuid5

import httpx
from fastapi import Request
from fastapi.responses import JSONResponse, RedirectResponse

from lib.shared.errors import (
    InstallationCollisionError,
    SecretStoreError,
    StateTokenInvalidError,
)
from services.ingest.connector_platform.authority_store import (
    PostgresAuthorityRepository,
)
from services.ingest.connector_platform.production_host_services import (
    ProductionHostBackends,
    build_production_host_services_factory,
)
from services.ingest.connector_runtime.policy import ExecutionMode, RouteRequest
from services.ingest.connector_runtime.authority import scope_authority
from services.ingest.source_contract.capabilities import OAUTH2_V1
from services.ingest.source_contract.capabilities.installation import (
    OAuthBeginRequest,
    OAuthCompleteRequest,
    OAuthResult,
)
from services.ingest.source_contract.connector import BindingContext, OperationContext
from services.ingest.source_contract.errors import ConnectorError
from services.ingest.source_contract.host_services import SecretCandidate
from services.ingest.source_contract.models import InstallationRef


LegacyHandler = Callable[[], Awaitable[Any]]


def _composition(request: Request, provider: str):
    composition = getattr(request.app.state, "source_connector_runtime", None)
    if composition is None:
        return None
    try:
        registration = composition.registry.for_source(provider)
    except Exception:
        return None
    if OAUTH2_V1.ref not in {key.ref for key in registration.capability_keys}:
        return None
    return composition, registration


def _connector_mode(request: Request, provider: str, tenant_id: UUID) -> bool:
    resolved = _composition(request, provider)
    if resolved is None:
        return False
    composition, registration = resolved
    decision = composition.routing.resolve(
        RouteRequest(
            tenant_id=tenant_id,
            connector_id=registration.connector_id,
            source=provider,
            capability=OAUTH2_V1.ref.id,
        )
    )
    return decision.mode is ExecutionMode.CONNECTOR


async def _ensure_bootstrap_authority(
    pool: Any,
    *,
    tenant_id: UUID,
    connector_id: str,
    secret_slots: tuple[str, ...],
    scopes: tuple[str, ...],
    outbound_hosts: tuple[str, ...],
) -> UUID:
    installation_id = uuid5(
        NAMESPACE_URL,
        f"fyralis:oauth-bootstrap:{tenant_id}:{connector_id}",
    )
    await pool.execute(
        """
        INSERT INTO source_connector_installations (
          id, tenant_id, connector_id, external_installation_id,
          desired_state, observed_phase, provenance
        ) VALUES ($1, $2, $3, $4, 'Ready', 'Authorizing', $5::jsonb)
        ON CONFLICT (id) DO UPDATE
          SET observed_phase = CASE
                WHEN source_connector_installations.observed_phase = 'Removed'
                THEN 'Authorizing'
                ELSE source_connector_installations.observed_phase
              END,
              updated_at = now()
        """,
        installation_id,
        tenant_id,
        connector_id,
        f"oauth-bootstrap:{tenant_id}",
        json.dumps({"origin": "connector_oauth_ingress"}),
    )
    await pool.execute(
        """
        INSERT INTO source_connector_authority_grants (
          installation_id, tenant_id, connector_id, credential_owner,
          granted_slot_names, granted_scopes, granted_outbound_hosts,
          maximum_trust_tier, provenance
        ) VALUES ($1, $2, $3, 'connector_oauth_bootstrap', $4, $5, $6,
                  'attested_agent', $7::jsonb)
        ON CONFLICT (installation_id) DO UPDATE
          SET granted_slot_names = EXCLUDED.granted_slot_names,
              granted_scopes = EXCLUDED.granted_scopes,
              granted_outbound_hosts = EXCLUDED.granted_outbound_hosts,
              revoked_at = NULL,
              authority_generation =
                source_connector_authority_grants.authority_generation + 1,
              updated_at = now()
        """,
        installation_id,
        tenant_id,
        connector_id,
        list(secret_slots),
        list(scopes),
        list(outbound_hosts),
        json.dumps({"purpose": "authorization_handshake"}),
    )
    return installation_id


async def _bound_oauth(
    request: Request,
    *,
    provider: str,
    tenant_id: UUID,
) -> tuple[Any, OperationContext, httpx.AsyncClient]:
    composition, registration = _composition(request, provider)  # type: ignore[misc]
    pool = request.app.state.pool
    manifest = registration.manifest
    installation_id = await _ensure_bootstrap_authority(
        pool,
        tenant_id=tenant_id,
        connector_id=registration.connector_id,
        secret_slots=manifest.spec.permissions.secret_slots,
        scopes=manifest.spec.permissions.requested_scopes,
        outbound_hosts=manifest.spec.permissions.outbound_hosts,
    )
    installation = InstallationRef(
        id=installation_id,
        tenant_id=tenant_id,
        connector_id=registration.connector_id,
        generation=1,
    )
    durable = await PostgresAuthorityRepository(pool).load(installation_id)
    if durable is None:
        raise RuntimeError("OAuth bootstrap authority was not persisted")
    authority = scope_authority(manifest, durable.validate_for(installation))
    client = httpx.AsyncClient(follow_redirects=False, timeout=15.0)
    services = build_production_host_services_factory(
        ProductionHostBackends(
            pool=pool,
            secret_store=request.app.state.secret_store,
            http_client=client,
        )
    ).build(
        installation.id,
        authority,
        connector_id=installation.connector_id,
    )
    binding = composition.registry.resolve_for_install(
        BindingContext(installation, authority, services)
    )
    operation = OperationContext(
        invocation_id=uuid4(),
        deadline=datetime.now(timezone.utc) + timedelta(seconds=30),
        services=services,
    )
    return binding.require(OAUTH2_V1), operation, client


def _configuration_error(provider: str, exc: Exception) -> JSONResponse:
    return JSONResponse(
        {
            "code": f"{provider}_client_unconfigured",
            "message": "OAuth connector configuration is incomplete",
            "context": {"provider": provider, "error_type": type(exc).__name__},
        },
        status_code=500,
    )


async def execute_oauth_install(
    request: Request,
    *,
    provider: str,
    legacy_handler: LegacyHandler,
) -> Any:
    auth = getattr(request.state, "auth", None)
    tenant_id = getattr(auth, "tenant_id", None)
    if not isinstance(tenant_id, UUID) or not _connector_mode(
        request, provider, tenant_id
    ):
        return await legacy_handler()
    pool = getattr(request.app.state, "pool", None)
    secret_store = getattr(request.app.state, "secret_store", None)
    if pool is None or secret_store is None:
        return await legacy_handler()
    from services.ingest.integrations.slack.oauth import issue_state_token

    state = await issue_state_token(tenant_id, pool, provider=provider)
    try:
        capability, operation, client = await _bound_oauth(
            request,
            provider=provider,
            tenant_id=tenant_id,
        )
        try:
            redirect = await capability.begin(
                OAuthBeginRequest(
                    redirect_uri=_redirect_uri(provider),
                    state=state,
                ),
                operation,
            )
        finally:
            await client.aclose()
    except ConnectorError as exc:
        return _configuration_error(provider, exc)
    _record_install(provider, "initiated")
    return RedirectResponse(redirect.url, status_code=302)


def _redirect_uri(provider: str) -> str:
    import os

    return os.environ.get(f"{provider.upper()}_REDIRECT_URI", "")


def _record_install(provider: str, outcome: str) -> None:
    if provider == "slack":
        from services.ingest.integrations.slack import metrics
    else:
        from services.ingest.integrations.notion import metrics
    metrics.record_install_outcome(outcome)


def _error_redirect(provider: str, reason: str) -> RedirectResponse:
    _record_install(provider, reason)
    return RedirectResponse(
        f"/integrations/{provider}/install-error?reason={reason}",
        status_code=302,
        headers={"X-Install-Error-Reason": reason},
    )


def _candidate(
    candidates: tuple[SecretCandidate, ...], slot: str
) -> SecretCandidate | None:
    return next((item for item in candidates if str(item.slot) == slot), None)


async def _persist_control_plane(
    conn: Any,
    *,
    installation_id: UUID,
    tenant_id: UUID,
    connector_id: str,
    external_installation_id: str,
    connector_version: str,
    scopes: tuple[str, ...],
    secret_refs: dict[str, str],
    outbound_hosts: tuple[str, ...],
) -> None:
    await conn.execute(
        """
        INSERT INTO source_connector_installations (
          id, tenant_id, connector_id, external_installation_id,
          desired_state, observed_phase, observed_generation,
          bound_connector_version, enabled_capabilities, provenance
        ) VALUES ($1, $2, $3, $4, 'Ready', 'Ready', 1, $5, $6, $7::jsonb)
        ON CONFLICT (id) DO UPDATE
          SET external_installation_id = EXCLUDED.external_installation_id,
              desired_state = 'Ready', observed_phase = 'Ready',
              bound_connector_version = EXCLUDED.bound_connector_version,
              enabled_capabilities = EXCLUDED.enabled_capabilities,
              provenance = EXCLUDED.provenance,
              updated_at = now()
        """,
        installation_id,
        tenant_id,
        connector_id,
        external_installation_id,
        connector_version,
        [OAUTH2_V1.ref.id],
        json.dumps({"origin": "connector_oauth", "verified": True}),
    )
    await conn.execute(
        """
        INSERT INTO source_connector_authority_grants (
          installation_id, tenant_id, connector_id, credential_owner,
          granted_slot_names, granted_scopes, granted_outbound_hosts,
          maximum_trust_tier, provenance
        ) VALUES ($1, $2, $3, 'connector_oauth', $4, $5, $6,
                  'attested_agent', $7::jsonb)
        ON CONFLICT (installation_id) DO UPDATE
          SET credential_owner = EXCLUDED.credential_owner,
              granted_slot_names = EXCLUDED.granted_slot_names,
              granted_scopes = EXCLUDED.granted_scopes,
              granted_outbound_hosts = EXCLUDED.granted_outbound_hosts,
              provenance = EXCLUDED.provenance, revoked_at = NULL,
              authority_generation =
                source_connector_authority_grants.authority_generation + 1,
              updated_at = now()
        """,
        installation_id,
        tenant_id,
        connector_id,
        list(secret_refs),
        list(scopes),
        list(outbound_hosts),
        json.dumps({"verified_by": "oauth_callback"}),
    )
    await conn.execute(
        """
        UPDATE source_connector_credentials
           SET state = 'retired', retired_at = now()
         WHERE installation_id = $1 AND state = 'current'
        """,
        installation_id,
    )
    for slot, secret_ref in secret_refs.items():
        await conn.execute(
            """
            INSERT INTO source_connector_credentials (
              installation_id, tenant_id, slot, secret_ref, state,
              owner, provenance, verified_at
            ) VALUES ($1, $2, $3, $4, 'current', 'connector_oauth',
                      $5::jsonb, now())
            """,
            installation_id,
            tenant_id,
            slot,
            secret_ref,
            json.dumps({"rotation": "oauth_callback"}),
        )


def _validate_result(
    registration: Any,
    result: OAuthResult,
    candidates: tuple[SecretCandidate, ...],
) -> None:
    requested = set(registration.manifest.spec.permissions.requested_scopes)
    granted = set(result.granted_scopes)
    unexpected = granted - requested
    missing = requested - granted
    if unexpected or missing:
        raise ValueError(
            "provider scope grant does not match the connector manifest: "
            f"missing={sorted(missing)}, unexpected={sorted(unexpected)}"
        )
    returned_slots = {str(candidate.slot) for candidate in candidates}
    requested_slots = set(registration.manifest.spec.permissions.secret_slots)
    missing_slots = requested_slots - returned_slots
    if missing_slots:
        raise SecretStoreError(
            f"OAuth capability omitted required secret slots: {sorted(missing_slots)}"
        )


async def _persist_result(
    request: Request,
    *,
    provider: str,
    tenant_id: UUID,
    result: OAuthResult,
    candidates: tuple[SecretCandidate, ...],
) -> str:
    from services.ingest.integrations.notion import oauth as notion_oauth
    from services.ingest.integrations.slack import oauth as slack_oauth

    pool = request.app.state.pool
    secret_store = request.app.state.secret_store
    composition, registration = _composition(request, provider)  # type: ignore[misc]
    _validate_result(registration, result, candidates)
    token = _candidate(candidates, "oauth_access_token")
    if token is None:
        raise SecretStoreError("OAuth capability returned no access token")
    external_id = result.external_installation_id
    secret_refs: dict[str, str] = {}
    if provider == "slack":
        signing = _candidate(candidates, "webhook_signing_secret")
        if signing is None:
            raise SecretStoreError("Slack capability returned no signing secret")
        bot_ref = await secret_store.put(
            token.value.reveal_text(),
            label=f"slack_bot_token:{external_id}",
            tenant_id=tenant_id,
        )
        signing_ref = await secret_store.put(
            signing.value.reveal_text(),
            label="slack_signing_secret:app",
            tenant_id=tenant_id,
        )
        secret_refs.update(
            oauth_access_token=str(bot_ref),
            webhook_signing_secret=str(signing_ref),
        )
        user = _candidate(candidates, "oauth_user_access_token")
        user_id = result.metadata.get("authed_user_id")
        user_ref = None
        if user is not None and isinstance(user_id, str):
            user_ref = await secret_store.put(
                user.value.reveal_text(),
                label=f"slack_user_token:{external_id}:{user_id}",
                tenant_id=tenant_id,
            )
            secret_refs["oauth_user_access_token"] = str(user_ref)
        provider_secret_ref = signing_ref
    else:
        provider_secret_ref = await secret_store.put(
            token.value.reveal_text(),
            label=f"notion_token:{external_id}",
            tenant_id=tenant_id,
        )
        secret_refs["oauth_access_token"] = str(provider_secret_ref)
        user_ref = None
        user_id = None

    async with pool.acquire() as conn:
        async with conn.transaction():
            if provider == "slack":
                installation_id, inserted = await slack_oauth._upsert_installation(
                    conn, tenant_id, external_id, provider_secret_ref
                )
                await slack_oauth._emit_onboarding_trigger(
                    conn,
                    tenant_id=tenant_id,
                    installation_row_id=installation_id,
                    trigger_kind="install" if inserted else "reinstall",
                    payload={"team_id": external_id},
                )
                if user_ref is not None and isinstance(user_id, str):
                    await slack_oauth._upsert_dm_installation(
                        conn,
                        tenant_id=tenant_id,
                        team_id=external_id,
                        user_id=user_id,
                        user_token_secret_ref=user_ref,
                        granted_user_scopes=result.metadata.get("granted_user_scopes"),
                    )
            else:
                installation_id, inserted = await notion_oauth._upsert_installation(
                    conn, tenant_id, external_id, provider_secret_ref
                )
                await notion_oauth._emit_onboarding_trigger(
                    conn,
                    tenant_id=tenant_id,
                    installation_row_id=installation_id,
                    trigger_kind="install" if inserted else "reinstall",
                    payload={"workspace_id": external_id},
                )
            await _persist_control_plane(
                conn,
                installation_id=installation_id,
                tenant_id=tenant_id,
                connector_id=registration.connector_id,
                external_installation_id=external_id,
                connector_version=registration.manifest.metadata.version,
                scopes=result.granted_scopes,
                secret_refs=secret_refs,
                outbound_hosts=registration.manifest.spec.permissions.outbound_hosts,
            )

    if provider == "slack":
        await slack_oauth._write_audit(
            pool,
            tenant_id,
            installation_id,
            "install",
            "ok",
            {"connector_runtime": True, "was_reinstall": not inserted},
        )
        slack_oauth._invalidate_resolver_cache(request, external_id)
        return f"/integrations/slack/installed?team={slack_oauth.short_team_hash(external_id)}"
    await notion_oauth._write_audit(
        pool,
        tenant_id,
        installation_id,
        "install",
        "ok",
        {"connector_runtime": True, "was_reinstall": not inserted},
    )
    notion_oauth._invalidate_resolver_cache(request, external_id)
    from services.ingest.integrations.notion.client import short_workspace_hash

    return (
        f"/integrations/notion/installed?workspace={short_workspace_hash(external_id)}"
    )


async def execute_oauth_callback(
    request: Request,
    *,
    provider: str,
    legacy_handler: LegacyHandler,
) -> Any:
    state = request.query_params.get("state", "")
    code = request.query_params.get("code", "")
    if not state or not code:
        return await legacy_handler()
    pool = getattr(request.app.state, "pool", None)
    if pool is None or _composition(request, provider) is None:
        return await legacy_handler()
    from services.ingest.integrations.slack.oauth import verify_and_consume_state

    try:
        tenant_id, _ = await verify_and_consume_state(state, pool)
    except StateTokenInvalidError as exc:
        return _error_redirect(provider, exc.reason)
    if not _connector_mode(request, provider, tenant_id):
        # A state is consumed only after the durable route is known. This
        # branch can happen only when an operator changes the route between
        # authorization and callback, so fail closed instead of exchanging
        # the code through two implementations.
        return _error_redirect(provider, "routing_revision_changed")
    try:
        capability, operation, client = await _bound_oauth(
            request,
            provider=provider,
            tenant_id=tenant_id,
        )
        try:
            result, candidates = await capability.complete(
                OAuthCompleteRequest(code=code, redirect_uri=_redirect_uri(provider)),
                operation,
            )
        finally:
            await client.aclose()
        location = await _persist_result(
            request,
            provider=provider,
            tenant_id=tenant_id,
            result=result,
            candidates=candidates,
        )
    except InstallationCollisionError:
        return _error_redirect(provider, "installation_collision")
    except (ConnectorError, SecretStoreError, ValueError) as exc:
        reason = (
            "secret_store_unavailable"
            if isinstance(exc, SecretStoreError)
            else f"{provider}_oauth_error"
        )
        return _error_redirect(provider, reason)
    _record_install(provider, "success")
    return RedirectResponse(location, status_code=302)


__all__ = ["execute_oauth_callback", "execute_oauth_install"]
