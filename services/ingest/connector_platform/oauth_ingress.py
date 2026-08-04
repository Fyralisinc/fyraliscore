"""Connector-authoritative OAuth ingress and installation persistence."""

from __future__ import annotations

import json
import hashlib
import secrets
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
from lib.shared.ids import uuid7
from services.ingest.connector_platform.authority_store import (
    PostgresAuthorityRepository,
)
from services.ingest.connector_platform.oauth_state import (
    issue_state_token,
    verify_and_consume_state,
)
from services.ingest.connector_platform.production_host_services import (
    ProductionHostBackends,
    build_production_host_services_factory,
)
from services.ingest.connector_runtime.authority import scope_authority
from services.ingest.source_contract.capabilities import (
    CONFIGURATION_V1,
    OAUTH2_V1,
    WEBHOOK_V1,
)
from services.ingest.source_contract.capabilities.installation import (
    OAuthBeginRequest,
    OAuthCompleteRequest,
    OAuthResult,
)
from services.ingest.source_contract.connector import BindingContext, OperationContext
from services.ingest.source_contract.errors import ConnectorError
from services.ingest.source_contract.host_services import SecretCandidate
from services.ingest.source_contract.models import InstallationRef


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


async def _ensure_bootstrap_authority(
    pool: Any,
    *,
    tenant_id: UUID,
    connector_id: str,
    secret_slots: tuple[str, ...],
    scopes: tuple[str, ...],
    outbound_hosts: tuple[str, ...],
    maximum_trust_tier: str,
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
          SET desired_state = 'Ready',
              observed_phase = 'Authorizing',
              removed_at = NULL,
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
                  $7, $8::jsonb)
        ON CONFLICT (installation_id) DO UPDATE
          SET granted_slot_names = EXCLUDED.granted_slot_names,
              granted_scopes = EXCLUDED.granted_scopes,
              granted_outbound_hosts = EXCLUDED.granted_outbound_hosts,
              maximum_trust_tier = EXCLUDED.maximum_trust_tier,
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
        maximum_trust_tier,
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
        secret_slots=(),
        scopes=manifest.spec.permissions.requested_scopes,
        outbound_hosts=manifest.spec.permissions.outbound_hosts,
        maximum_trust_tier=manifest.spec.trust.maximum_tier,
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


async def _bound_configuration(
    request: Request,
    *,
    provider: str,
    tenant_id: UUID,
) -> tuple[Any, Any, OperationContext, httpx.AsyncClient]:
    composition = getattr(request.app.state, "source_connector_runtime", None)
    if composition is None:
        raise RuntimeError("source connector runtime is unavailable")
    registration = composition.registry.for_source(provider)
    if CONFIGURATION_V1.ref not in {
        key.ref for key in registration.capability_keys
    }:
        raise RuntimeError("connector has no configuration capability")
    manifest = registration.manifest
    installation_id = await _ensure_bootstrap_authority(
        request.app.state.pool,
        tenant_id=tenant_id,
        connector_id=registration.connector_id,
        secret_slots=(),
        scopes=manifest.spec.permissions.requested_scopes,
        outbound_hosts=manifest.spec.permissions.outbound_hosts,
        maximum_trust_tier=manifest.spec.trust.maximum_tier,
    )
    installation = InstallationRef(
        id=installation_id,
        tenant_id=tenant_id,
        connector_id=registration.connector_id,
        generation=1,
    )
    durable = await PostgresAuthorityRepository(request.app.state.pool).load(
        installation_id
    )
    if durable is None:
        raise RuntimeError("configuration bootstrap authority was not persisted")
    authority = scope_authority(manifest, durable.validate_for(installation))
    client = httpx.AsyncClient(follow_redirects=False, timeout=15.0)
    services = build_production_host_services_factory(
        ProductionHostBackends(
            pool=request.app.state.pool,
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
    return registration, binding.require(CONFIGURATION_V1), operation, client


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
) -> Any:
    auth = getattr(request.state, "auth", None)
    tenant_id = getattr(auth, "tenant_id", None)
    if not isinstance(tenant_id, UUID):
        return JSONResponse({"code": "authentication_required"}, status_code=401)
    if _composition(request, provider) is None:
        return JSONResponse(
            {"code": "connector_oauth_unavailable", "provider": provider},
            status_code=503,
        )
    pool = getattr(request.app.state, "pool", None)
    secret_store = getattr(request.app.state, "secret_store", None)
    if pool is None or secret_store is None:
        return JSONResponse(
            {"code": "connector_runtime_unavailable", "provider": provider},
            status_code=503,
        )
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
    # The router records provider-neutral onboarding workflow metrics. Keeping
    # this layer source-agnostic prevents OAuth from owning legacy integrations.
    del provider, outcome


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
    granted_slots: tuple[str, ...],
    outbound_hosts: tuple[str, ...],
    enabled_capabilities: tuple[str, ...],
    maximum_trust_tier: str,
    owner: str,
    provenance: dict[str, Any],
    installation_data: dict[str, dict[str, Any]] | None = None,
    phase: str = "Ready",
) -> None:
    await conn.execute(
        """
        INSERT INTO source_connector_installations (
          id, tenant_id, connector_id, external_installation_id,
          desired_state, observed_phase, observed_generation,
          bound_connector_version, enabled_capabilities, provenance
        ) VALUES ($1, $2, $3, $4, $8, $8, 1, $5, $6, $7::jsonb)
        ON CONFLICT (id) DO UPDATE
          SET external_installation_id = EXCLUDED.external_installation_id,
              desired_state = EXCLUDED.desired_state,
              observed_phase = EXCLUDED.observed_phase,
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
        list(enabled_capabilities),
        json.dumps(provenance),
        phase,
    )
    await conn.execute(
        """
        INSERT INTO source_connector_authority_grants (
          installation_id, tenant_id, connector_id, credential_owner,
          granted_slot_names, granted_scopes, granted_outbound_hosts,
          maximum_trust_tier, provenance
        ) VALUES ($1, $2, $3, $4, $5, $6, $7,
                  $8, $9::jsonb)
        ON CONFLICT (installation_id) DO UPDATE
          SET credential_owner = EXCLUDED.credential_owner,
              granted_slot_names = EXCLUDED.granted_slot_names,
              granted_scopes = EXCLUDED.granted_scopes,
              granted_outbound_hosts = EXCLUDED.granted_outbound_hosts,
              maximum_trust_tier = EXCLUDED.maximum_trust_tier,
              provenance = EXCLUDED.provenance, revoked_at = NULL,
              authority_generation =
                source_connector_authority_grants.authority_generation + 1,
              updated_at = now()
        """,
        installation_id,
        tenant_id,
        connector_id,
        owner,
        list(granted_slots),
        list(scopes),
        list(outbound_hosts),
        maximum_trust_tier,
        json.dumps(provenance),
    )
    await conn.execute(
        """
        UPDATE source_connector_credentials
           SET state = 'retired', retired_at = now()
         WHERE installation_id = $1
           AND slot = ANY($2::text[])
           AND state = 'current'
        """,
        installation_id,
        list(secret_refs),
    )
    for slot, secret_ref in secret_refs.items():
        await conn.execute(
            """
            INSERT INTO source_connector_credentials (
              installation_id, tenant_id, slot, secret_ref, state,
              owner, provenance, verified_at
            ) VALUES ($1, $2, $3, $4, 'current', $5,
                      $6::jsonb, now())
            """,
            installation_id,
            tenant_id,
            slot,
            secret_ref,
            owner,
            json.dumps(provenance),
        )
    for namespace, values in (installation_data or {}).items():
        await conn.execute(
            """
            INSERT INTO source_connector_installation_data (
              installation_id, tenant_id, namespace, generation, values
            ) VALUES ($1, $2, $3, 1, $4::jsonb)
            ON CONFLICT (installation_id, namespace) DO UPDATE
              SET generation = source_connector_installation_data.generation + 1,
                  values = EXCLUDED.values,
                  updated_at = now()
            """,
            installation_id,
            tenant_id,
            namespace,
            json.dumps(values),
        )


async def _retire_bootstrap(
    conn: Any,
    *,
    tenant_id: UUID,
    connector_id: str,
) -> None:
    installation_id = uuid5(
        NAMESPACE_URL,
        f"fyralis:oauth-bootstrap:{tenant_id}:{connector_id}",
    )
    await conn.execute(
        """
        UPDATE source_connector_credentials
           SET state = 'retired', retired_at = now()
         WHERE installation_id = $1
           AND state IN ('pending', 'current')
        """,
        installation_id,
    )
    await conn.execute(
        """
        UPDATE source_connector_authority_grants
           SET revoked_at = now(), updated_at = now()
         WHERE installation_id = $1
        """,
        installation_id,
    )
    await conn.execute(
        """
        UPDATE source_connector_installations
           SET desired_state = 'Removed', observed_phase = 'Removed',
               removed_at = now(), updated_at = now()
         WHERE id = $1
        """,
        installation_id,
    )


async def _persist_webhook_callback(
    conn: Any,
    *,
    installation_id: UUID,
    tenant_id: UUID,
    nonce_secret_ref: str,
) -> UUID:
    endpoint_id = uuid5(
        NAMESPACE_URL,
        f"fyralis:connector-webhook:{installation_id}",
    )
    await conn.execute(
        """
        INSERT INTO source_connector_callbacks (
          endpoint_id, installation_id, tenant_id, purpose,
          nonce_secret_ref, status
        ) VALUES ($1, $2, $3, 'webhook', $4, 'active')
        ON CONFLICT (endpoint_id) DO UPDATE
          SET nonce_secret_ref = EXCLUDED.nonce_secret_ref,
              status = 'active'
        """,
        endpoint_id,
        installation_id,
        tenant_id,
        nonce_secret_ref,
    )
    return endpoint_id


async def _webhook_callback_secret(
    request: Request,
    *,
    tenant_id: UUID,
    installation_id: UUID,
    enabled_capabilities: tuple[str, ...],
) -> str | None:
    if str(WEBHOOK_V1.ref.id) not in enabled_capabilities:
        return None
    nonce = secrets.token_urlsafe(32)
    ref = await request.app.state.secret_store.put(
        nonce,
        label=f"source_connector:webhook_callback:{installation_id}",
        tenant_id=tenant_id,
    )
    return str(ref)


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
    undeclared_slots = returned_slots - requested_slots
    if undeclared_slots:
        raise SecretStoreError(
            f"OAuth capability returned undeclared slots: {sorted(undeclared_slots)}"
        )
    if not returned_slots:
        raise SecretStoreError("OAuth capability returned no credentials")


async def _persist_result(
    request: Request,
    *,
    provider: str,
    tenant_id: UUID,
    result: OAuthResult,
    candidates: tuple[SecretCandidate, ...],
) -> str:
    pool = request.app.state.pool
    secret_store = request.app.state.secret_store
    composition, registration = _composition(request, provider)  # type: ignore[misc]
    _validate_result(registration, result, candidates)
    external_id = result.external_installation_id
    secret_refs: dict[str, str] = {}
    for candidate in candidates:
        slot = str(candidate.slot)
        secret_ref = await secret_store.put(
            candidate.value.reveal_bytes(),
            label=f"source_connector:{provider}:{slot}",
            tenant_id=tenant_id,
        )
        secret_refs[slot] = str(secret_ref)

    installation_id = uuid5(
        NAMESPACE_URL,
        f"fyralis:connector-install:{tenant_id}:{registration.connector_id}:{external_id}",
    )
    collision = await pool.fetchrow(
        """
        SELECT tenant_id
          FROM source_connector_installations
         WHERE connector_id = $1 AND external_installation_id = $2
        """,
        registration.connector_id,
        external_id,
    )
    if collision is not None and collision["tenant_id"] != tenant_id:
        raise InstallationCollisionError("connector installation belongs to another tenant")
    existing_slots = {
        str(row["slot"])
        for row in await pool.fetch(
            """
            SELECT slot FROM source_connector_credentials
             WHERE installation_id = $1 AND state = 'current'
            """,
            installation_id,
        )
    }
    configured_slots = tuple(sorted(existing_slots | set(secret_refs)))
    # The manifest is the permission ceiling. The durable grant contains only
    # slots backed by current credential references so configuredBy remains an
    # executable capability assertion rather than a declaration-only hint.
    granted_slots = configured_slots
    required_slots = {
        str(slot)
        for declaration in registration.manifest.spec.capabilities
        if declaration.available and declaration.required
        for slot in declaration.configured_by
    }
    missing_slots = tuple(sorted(required_slots - set(configured_slots)))
    phase = "Maintenance" if missing_slots else "Ready"
    enabled_capabilities = tuple(
        ref.id
        for ref in registration.manifest.configured_capability_refs(
            frozenset(configured_slots)
        )
    )
    webhook_nonce_ref = await _webhook_callback_secret(
        request,
        tenant_id=tenant_id,
        installation_id=installation_id,
        enabled_capabilities=enabled_capabilities,
    )

    webhook_endpoint_id: UUID | None = None
    async with pool.acquire() as conn:
        async with conn.transaction():
            await _persist_control_plane(
                conn,
                installation_id=installation_id,
                tenant_id=tenant_id,
                connector_id=registration.connector_id,
                external_installation_id=external_id,
                connector_version=registration.manifest.metadata.version,
                scopes=result.granted_scopes,
                secret_refs=secret_refs,
                granted_slots=granted_slots,
                outbound_hosts=registration.manifest.spec.permissions.outbound_hosts,
                enabled_capabilities=enabled_capabilities,
                maximum_trust_tier=registration.manifest.spec.trust.maximum_tier,
                owner="connector_oauth",
                provenance={"origin": "connector_oauth", "verified": True},
                installation_data={"configuration": result.metadata},
                phase=phase,
            )
            if webhook_nonce_ref is not None:
                webhook_endpoint_id = await _persist_webhook_callback(
                    conn,
                    installation_id=installation_id,
                    tenant_id=tenant_id,
                    nonce_secret_ref=webhook_nonce_ref,
                )
            if not missing_slots:
                await conn.execute(
                    """
                    INSERT INTO onboarding_triggers (
                      id, tenant_id, source, trigger_kind,
                      connector_installation_id, payload
                    ) VALUES ($1, $2, $3, 'install', $4, $5::jsonb)
                    ON CONFLICT DO NOTHING
                    """,
                    uuid7(),
                    tenant_id,
                    provider,
                    installation_id,
                    json.dumps({"external_installation_id": external_id}),
                )
            await _retire_bootstrap(
                conn,
                tenant_id=tenant_id,
                connector_id=registration.connector_id,
            )

    runtime = getattr(request.app.state, "integration_runtime", None)
    resolver = getattr(runtime, "tenant_resolver", None)
    cache = getattr(resolver, "_cache", None)
    if cache is not None:
        cache.invalidate((provider, external_id))
    short = hashlib.blake2b(external_id.encode(), digest_size=8).hexdigest()
    location = f"/integrations/{provider}/installed?installation={short}"
    if missing_slots:
        location += "&phase=Maintenance&missing_slots=" + ",".join(missing_slots)
    if webhook_endpoint_id is not None:
        location += f"&webhook_endpoint={webhook_endpoint_id}"
    return location


async def execute_configuration_install(
    request: Request,
    *,
    provider: str,
) -> JSONResponse:
    """Validate and persist a non-redirect connector installation.

    This is the single install surface for API keys, service accounts, AWS
    credentials, and gateway/session tokens. Credentials are accepted only for
    manifest-declared slots and are written directly to the host secret store.
    """

    auth = getattr(request.state, "auth", None)
    tenant_id = getattr(auth, "tenant_id", None)
    if not isinstance(tenant_id, UUID):
        return JSONResponse({"code": "authentication_required"}, status_code=401)
    if (
        getattr(request.app.state, "pool", None) is None
        or getattr(request.app.state, "secret_store", None) is None
    ):
        return JSONResponse({"code": "connector_runtime_unavailable"}, status_code=503)
    try:
        payload = await request.json()
    except Exception:
        return JSONResponse({"code": "invalid_json"}, status_code=400)
    if not isinstance(payload, dict):
        return JSONResponse({"code": "invalid_configuration"}, status_code=400)
    external_id = payload.get("external_installation_id")
    credentials = payload.get("credentials", {})
    configuration = payload.get("configuration", {})
    installation_data = payload.get("installation_data", {})
    if not isinstance(external_id, str) or not external_id.strip():
        return JSONResponse(
            {"code": "invalid_configuration", "field": "external_installation_id"},
            status_code=422,
        )
    if (
        not isinstance(credentials, dict)
        or not isinstance(configuration, dict)
        or not isinstance(installation_data, dict)
    ):
        return JSONResponse({"code": "invalid_configuration"}, status_code=422)
    malformed_namespaces = {
        str(namespace)
        for namespace, values in installation_data.items()
        if not isinstance(namespace, str) or not isinstance(values, dict)
    }
    if malformed_namespaces:
        return JSONResponse(
            {
                "code": "invalid_installation_data",
                "namespaces": sorted(malformed_namespaces),
            },
            status_code=422,
        )
    configuration = dict(configuration)
    configuration["external_installation_id"] = external_id.strip()
    configuration.setdefault("selected_resources", [])
    try:
        registration, capability, operation, client = await _bound_configuration(
            request,
            provider=provider,
            tenant_id=tenant_id,
        )
    except Exception:
        return JSONResponse(
            {"code": "connector_configuration_unavailable", "provider": provider},
            status_code=503,
        )
    try:
        validation = await capability.validate_configuration(configuration, operation)
    except ConnectorError:
        return JSONResponse({"code": "configuration_rejected"}, status_code=422)
    finally:
        await client.aclose()
    if not validation.valid:
        return JSONResponse(
            {
                "code": "configuration_rejected",
                "issues": [issue.model_dump() for issue in validation.issues],
            },
            status_code=422,
        )

    manifest = registration.manifest
    pool = request.app.state.pool
    connector_id = registration.connector_id
    installation_id = uuid5(
        NAMESPACE_URL,
        f"fyralis:connector-install:{tenant_id}:{connector_id}:{external_id.strip()}",
    )
    existing_slots = {
        str(row["slot"])
        for row in await pool.fetch(
            """
            SELECT slot FROM source_connector_credentials
             WHERE installation_id = $1 AND state = 'current'
            """,
            installation_id,
        )
    }
    declared_namespaces = set(manifest.spec.installation_data_namespaces)
    undeclared_namespaces = set(installation_data) - declared_namespaces
    if undeclared_namespaces:
        return JSONResponse(
            {
                "code": "undeclared_installation_data",
                "namespaces": sorted(undeclared_namespaces),
            },
            status_code=422,
        )
    declared_slots = {str(slot) for slot in manifest.spec.permissions.secret_slots}
    supplied_slots = {str(slot) for slot in credentials}
    undeclared = supplied_slots - declared_slots
    required_slots = {
        str(slot)
        for declaration in manifest.spec.capabilities
        if declaration.available and declaration.required
        for slot in declaration.configured_by
    }
    missing = required_slots - supplied_slots - existing_slots
    invalid_values = {
        str(slot)
        for slot, value in credentials.items()
        if not isinstance(value, str) or not value
    }
    if undeclared or missing or invalid_values:
        return JSONResponse(
            {
                "code": "invalid_credentials",
                "undeclared_slots": sorted(undeclared),
                "missing_slots": sorted(missing),
                "invalid_slots": sorted(invalid_values),
            },
            status_code=422,
        )

    collision = await pool.fetchrow(
        """
        SELECT tenant_id FROM source_connector_installations
         WHERE connector_id = $1 AND external_installation_id = $2
        """,
        connector_id,
        external_id.strip(),
    )
    if collision is not None and collision["tenant_id"] != tenant_id:
        return JSONResponse({"code": "installation_collision"}, status_code=409)
    secret_refs: dict[str, str] = {}
    try:
        for slot, value in credentials.items():
            secret_ref = await request.app.state.secret_store.put(
                value.encode(),
                label=f"source_connector:{provider}:{slot}",
                tenant_id=tenant_id,
            )
            secret_refs[str(slot)] = str(secret_ref)
    except Exception:
        return JSONResponse({"code": "secret_store_unavailable"}, status_code=503)

    configured_slots = tuple(sorted(existing_slots | set(secret_refs)))
    granted_slots = configured_slots
    enabled_capabilities = tuple(
        ref.id for ref in manifest.configured_capability_refs(frozenset(configured_slots))
    )
    webhook_nonce_ref = await _webhook_callback_secret(
        request,
        tenant_id=tenant_id,
        installation_id=installation_id,
        enabled_capabilities=enabled_capabilities,
    )
    webhook_endpoint_id: UUID | None = None
    async with pool.acquire() as conn:
        async with conn.transaction():
            await _persist_control_plane(
                conn,
                installation_id=installation_id,
                tenant_id=tenant_id,
                connector_id=connector_id,
                external_installation_id=external_id.strip(),
                connector_version=manifest.metadata.version,
                scopes=manifest.spec.permissions.requested_scopes,
                secret_refs=secret_refs,
                granted_slots=granted_slots,
                outbound_hosts=manifest.spec.permissions.outbound_hosts,
                enabled_capabilities=enabled_capabilities,
                maximum_trust_tier=manifest.spec.trust.maximum_tier,
                owner="connector_configuration",
                provenance={"origin": "connector_configuration", "verified": True},
                installation_data={
                    "configuration": configuration,
                    **{
                        str(namespace): dict(values)
                        for namespace, values in installation_data.items()
                    },
                },
            )
            if webhook_nonce_ref is not None:
                webhook_endpoint_id = await _persist_webhook_callback(
                    conn,
                    installation_id=installation_id,
                    tenant_id=tenant_id,
                    nonce_secret_ref=webhook_nonce_ref,
                )
            await conn.execute(
                """
                INSERT INTO onboarding_triggers (
                  id, tenant_id, source, trigger_kind,
                  connector_installation_id, payload
                ) VALUES ($1, $2, $3, 'install', $4, $5::jsonb)
                ON CONFLICT DO NOTHING
                """,
                uuid7(),
                tenant_id,
                provider,
                installation_id,
                json.dumps({"external_installation_id": external_id.strip()}),
            )
            await _retire_bootstrap(
                conn,
                tenant_id=tenant_id,
                connector_id=connector_id,
            )
    return JSONResponse(
        {
            "installation_id": str(installation_id),
            "source": provider,
            "phase": "Ready",
            "webhook_path": (
                f"/webhooks/{provider}/callback/{webhook_endpoint_id}"
                if webhook_endpoint_id is not None
                else None
            ),
        },
        status_code=201,
    )


async def execute_oauth_callback(
    request: Request,
    *,
    provider: str,
) -> Any:
    state = request.query_params.get("state", "")
    code = request.query_params.get("code", "")
    if not state or not code:
        return _error_redirect(provider, "missing_state_or_code")
    pool = getattr(request.app.state, "pool", None)
    if pool is None or _composition(request, provider) is None:
        return _error_redirect(provider, "connector_oauth_unavailable")
    try:
        tenant_id, _ = await verify_and_consume_state(
            state, pool, provider=provider
        )
    except StateTokenInvalidError as exc:
        return _error_redirect(provider, exc.reason)
    try:
        capability, operation, client = await _bound_oauth(
            request,
            provider=provider,
            tenant_id=tenant_id,
        )
        try:
            result, candidates = await capability.complete(
                OAuthCompleteRequest(
                    code=code,
                    redirect_uri=_redirect_uri(provider),
                    callback_parameters={
                        str(key): str(value)
                        for key, value in request.query_params.items()
                    },
                ),
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


__all__ = [
    "execute_configuration_install",
    "execute_oauth_callback",
    "execute_oauth_install",
]
