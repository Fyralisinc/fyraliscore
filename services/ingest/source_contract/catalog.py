"""Deterministic catalog for Fyralis ingestion providers and sources.

This module is intentionally declarative.  It does not import any planner,
fetcher, handler, provider client, workflow, or database model.  Executable
bindings are validated ``module:callable`` references that the runtime resolves
explicitly and lazily.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import replace
from types import MappingProxyType
from typing import cast

from lib.shared.provider_transport import RetrySafety
from services.ingest.source_contract.models import (
    AcknowledgementPolicy,
    AllowedObservationKind,
    AllowedTrustTier,
    BrowserAgentDefinition,
    Certification,
    CredentialAuthStyle,
    CredentialGrantType,
    CredentialRefreshDefinition,
    DedicatedAcknowledgementPolicy,
    DedicatedIngressDefinition,
    DedicatedIngressMethod,
    DedicatedKafkaMode,
    DeliveryPolicy,
    HistoryKind,
    IngressKind,
    IngressRoute,
    InstallationAdapter,
    InstallationManagementDefinition,
    LiveTransportKind,
    LocalRehearsalDefinition,
    NativeConnectDefinition,
    NonSourceChannelDefinition,
    OnboardingDefinition,
    OAuthIngressDefinition,
    OAuthIngressMountMode,
    OperationPolicyDefinition,
    ProviderDefinition,
    RequestPolicy,
    SourceCategory,
    SourceConnectionMethod,
    SourceDefinition,
    SourceDisplayDefinition,
    SourceSyncMode,
    WebhookAcknowledgementPolicy,
    WebhookHandlerMode,
    WebhookIngressDefinition,
    WebhookKafkaMode,
    WebhookTenantBinding,
    normalize_catalog_name,
)


_STANDARD_RETRYABLE_HTTP_STATUSES: tuple[int, ...] = (
    408,
    425,
    429,
    500,
    502,
    503,
    504,
)
_STANDARD_RETRYABLE_ERROR_CODES: tuple[str, ...] = (
    "provider_rate_limited",
    "provider_timeout",
    "provider_transient_error",
)
_INTERNAL_IDEMPOTENT_MAX_CONCURRENCY = 4


def _request_policy(
    *,
    retry_safety: RetrySafety,
    retryable_status_codes: tuple[int, ...],
    rate_limit_header_parser_id: str | None,
) -> RequestPolicy:
    """Build a fully specified operation policy without quota guesses."""

    automatically_retryable = retry_safety is not RetrySafety.UNSAFE
    return RequestPolicy(
        max_attempts=3 if automatically_retryable else 1,
        timeout_seconds=30.0,
        max_elapsed_seconds=60.0,
        base_backoff_seconds=0.5,
        max_backoff_seconds=30.0,
        max_inline_retry_after_seconds=30.0,
        max_quota_wait_seconds=0.0,
        default_retry_later_seconds=60.0,
        # This is a local safety ceiling, not a claim about provider quota.
        # Distributed provider limits remain evidence-backed quota rules.
        max_concurrency=(
            _INTERNAL_IDEMPOTENT_MAX_CONCURRENCY if automatically_retryable else 1
        ),
        retry_safety=retry_safety,
        retryable_status_codes=(
            retryable_status_codes if automatically_retryable else ()
        ),
        retryable_error_codes=(
            _STANDARD_RETRYABLE_ERROR_CODES if automatically_retryable else ()
        ),
        rate_limit_header_parser_id=rate_limit_header_parser_id,
    )


def _operation_policies(
    *,
    idempotent: tuple[str, ...] = (),
    idempotency_key: tuple[str, ...] = (),
    unsafe: tuple[str, ...] = (),
    retryable_status_codes: tuple[int, ...] = (_STANDARD_RETRYABLE_HTTP_STATUSES),
    rate_limit_header_parser_id: str | None = None,
) -> tuple[OperationPolicyDefinition, ...]:
    """Materialize exact, disjoint operation declarations."""

    declared = (*idempotent, *idempotency_key, *unsafe)
    if len(declared) != len(set(declared)):
        raise ValueError("operation policy safety groups must contain exact unique IDs")
    rows: list[OperationPolicyDefinition] = []
    for operation_ids, safety in (
        (idempotent, RetrySafety.IDEMPOTENT),
        (idempotency_key, RetrySafety.IDEMPOTENCY_KEY),
        (unsafe, RetrySafety.UNSAFE),
    ):
        rows.extend(
            OperationPolicyDefinition(
                operation_id=operation_id,
                request_policy=_request_policy(
                    retry_safety=safety,
                    retryable_status_codes=retryable_status_codes,
                    rate_limit_header_parser_id=(rate_limit_header_parser_id),
                ),
            )
            for operation_id in operation_ids
        )
    return tuple(rows)


def _webhook_ingress(
    route_id: str,
    source_id: str | None,
    channel: str,
    *,
    route_path: str | None = None,
    tenant_binding: WebhookTenantBinding = "payload",
    handler_mode: WebhookHandlerMode = "generic",
    acknowledgement_policy: WebhookAcknowledgementPolicy = ("observation_response"),
    kafka_mode: WebhookKafkaMode = ("flagged_kafka_first_with_inline_fallback"),
    ingress_metadata_binding: str = (
        "services.app.webhooks.ingress_metadata:build_generic_metadata"
    ),
    normalizer_header_projection: tuple[tuple[str, str], ...] = (),
    verification_handshake_binding: str | None = None,
    verification_handshake_handler_binding: str | None = None,
    dedicated_handler_binding: str | None = None,
) -> WebhookIngressDefinition:
    """Build one dependency-light provider-edge webhook declaration."""

    return WebhookIngressDefinition(
        route_id=route_id,
        source_id=source_id,
        route_path=route_path or f"/webhooks/{route_id}",
        channel=channel,
        verifier_binding=(
            f"services.app.webhooks.signatures.{route_id}:verifier.verify"
        ),
        tenant_extractor_binding=(
            f"services.app.webhooks.tenant_resolver:_extract_{route_id}"
        ),
        ingress_metadata_binding=ingress_metadata_binding,
        normalizer_header_projection=normalizer_header_projection,
        tenant_binding=tenant_binding,
        handler_mode=handler_mode,
        acknowledgement_policy=acknowledgement_policy,
        kafka_mode=kafka_mode,
        verification_handshake_binding=verification_handshake_binding,
        verification_handshake_handler_binding=(verification_handshake_handler_binding),
        dedicated_handler_binding=dedicated_handler_binding,
    )


def _oauth_ingress(
    source_id: str,
    *,
    install_path: str | None = None,
    callback_path: str | None = None,
    install_handler: str = "install_handler",
    callback_handler: str = "callback_handler",
    mount_mode: OAuthIngressMountMode = "shared_router",
    public_result_paths: tuple[str, ...] | None = None,
) -> OAuthIngressDefinition:
    """Declare one exact browser/provider OAuth callback boundary."""

    module = f"services.ingest.integrations.{source_id}.oauth"
    return OAuthIngressDefinition(
        source_id=source_id,
        install_path=install_path or f"/integrations/{source_id}/install",
        callback_path=(callback_path or f"/integrations/{source_id}/callback"),
        install_handler_binding=f"{module}:{install_handler}",
        callback_handler_binding=f"{module}:{callback_handler}",
        mount_mode=mount_mode,
        public_result_paths=(
            public_result_paths
            if public_result_paths is not None
            else (
                f"/integrations/{source_id}/installed",
                f"/integrations/{source_id}/install-error",
            )
        ),
    )


def _credential_refresh(
    source_id: str,
    default_token_url: str,
    grant_type: CredentialGrantType,
    auth_style: CredentialAuthStyle,
    *,
    rotates_refresh_token: bool,
    install_table: str,
    operation_id: str,
    default_expires_in: int = 3600,
    client_secret_from_install: bool = False,
    client_credentials_from_install: bool = False,
    scope_env: str | None = None,
    default_scope: str | None = None,
) -> CredentialRefreshDefinition:
    return CredentialRefreshDefinition(
        operation_id=operation_id,
        default_token_url=default_token_url,
        token_url_env=f"{source_id.upper()}_TOKEN_URL",
        grant_type=grant_type,
        auth_style=auth_style,
        rotates_refresh_token=rotates_refresh_token,
        install_table=install_table,
        default_expires_in=default_expires_in,
        client_secret_from_install=client_secret_from_install,
        client_credentials_from_install=client_credentials_from_install,
        scope_env=scope_env,
        default_scope=default_scope,
    )


def _dedicated_ingress(
    ingress_id: str,
    source_id: str,
    route_path: str,
    methods: tuple[DedicatedIngressMethod, ...],
    ingress_kind: IngressKind,
    channel: str | None,
    *,
    verification_policy: str,
    verification_bindings: tuple[str, ...],
    tenant_binding_policy: str,
    tenant_resolver_binding: str,
    acknowledgement_policy: DedicatedAcknowledgementPolicy,
    kafka_mode: DedicatedKafkaMode,
    dispatcher_binding: str,
    router_factory_binding: str,
    router_factory_accepts_debug_endpoints: bool = False,
) -> DedicatedIngressDefinition:
    return DedicatedIngressDefinition(
        ingress_id=ingress_id,
        source_id=source_id,
        route_path=route_path,
        methods=methods,
        ingress_kind=ingress_kind,
        channel=channel,
        verification_policy=verification_policy,
        verification_bindings=verification_bindings,
        tenant_binding_policy=tenant_binding_policy,
        tenant_resolver_binding=tenant_resolver_binding,
        acknowledgement_policy=acknowledgement_policy,
        kafka_mode=kafka_mode,
        dispatcher_binding=dispatcher_binding,
        router_factory_binding=router_factory_binding,
        router_factory_accepts_debug_endpoints=(router_factory_accepts_debug_endpoints),
    )


class CatalogValidationError(ValueError):
    """Raised when provider/source declarations are incomplete or inconsistent."""


_PROVIDER_BASE_DEFINITIONS: tuple[ProviderDefinition, ...] = (
    ProviderDefinition(
        "slack",
        "Slack",
        (),
        ("slack",),
        ("oauth2",),
        oauth_ingresses=(_oauth_ingress("slack"),),
        webhook_ingresses=(
            _webhook_ingress(
                "slack",
                "slack",
                "slack:message",
                route_path="/webhooks/slack/events",
                ingress_metadata_binding=(
                    "services.app.webhooks.ingress_metadata:" "build_slack_metadata"
                ),
            ),
        ),
    ),
    ProviderDefinition(
        "github",
        "GitHub",
        ("git_hub",),
        ("github",),
        ("github_app",),
        oauth_ingresses=(_oauth_ingress("github"),),
        webhook_ingresses=(
            _webhook_ingress(
                "github",
                "github",
                "github:webhook",
                ingress_metadata_binding=(
                    "services.app.webhooks.ingress_metadata:" "build_github_metadata"
                ),
                normalizer_header_projection=(("event_type", "X-GitHub-Event"),),
            ),
        ),
    ),
    ProviderDefinition(
        "discord",
        "Discord",
        (),
        ("discord",),
        ("oauth2_bot",),
        oauth_ingresses=(_oauth_ingress("discord"),),
        webhook_ingresses=(
            _webhook_ingress(
                "discord",
                "discord",
                "discord:interaction",
                acknowledgement_policy="synchronous_provider_response",
                kafka_mode="inline_then_shadow",
                ingress_metadata_binding=(
                    "services.app.webhooks.ingress_metadata:" "build_discord_metadata"
                ),
            ),
        ),
    ),
    ProviderDefinition(
        "google",
        "Google",
        ("google_workspace", "workspace"),
        ("gmail", "google_calendar", "google_drive"),
        ("domain_wide_delegation",),
        dedicated_ingresses=(
            _dedicated_ingress(
                "gmail_pubsub",
                "gmail",
                "/webhooks/gmail/pubsub",
                ("POST",),
                "pubsub",
                None,
                verification_policy="google_oidc",
                verification_bindings=(
                    "services.app.webhooks.signatures.google_oidc:"
                    "verify_pubsub_oidc_token",
                ),
                tenant_binding_policy="subscription_installation",
                tenant_resolver_binding=(
                    "services.ingest.integrations.gmail.push_handler:" "handle_push"
                ),
                acknowledgement_policy="ack_and_reconcile_on_failure",
                kafka_mode="hydrated_messages_handler_managed",
                dispatcher_binding=(
                    "services.app.webhooks.gmail_pubsub:gmail_pubsub_push"
                ),
                router_factory_binding=(
                    "services.app.webhooks.gmail_pubsub:" "build_gmail_pubsub_router"
                ),
            ),
            _dedicated_ingress(
                "google_calendar_push",
                "google_calendar",
                "/webhooks/google_calendar/push",
                ("POST",),
                "webhook",
                "google_calendar:event",
                verification_policy="google_watch_channel_token",
                verification_bindings=(
                    "services.ingest.integrations._google_watch:resolve_push",
                ),
                tenant_binding_policy="watch_channel_installation",
                tenant_resolver_binding=(
                    "services.ingest.integrations._google_watch:resolve_push"
                ),
                acknowledgement_policy="ack_and_reconcile_on_failure",
                kafka_mode="reconciled_delta_drain",
                dispatcher_binding=(
                    "services.app.webhooks.google_push:" "google_calendar_push"
                ),
                router_factory_binding=(
                    "services.app.webhooks.google_push:" "build_google_push_router"
                ),
            ),
            _dedicated_ingress(
                "google_drive_push",
                "google_drive",
                "/webhooks/google_drive/push",
                ("POST",),
                "webhook",
                "google_drive:file",
                verification_policy="google_watch_channel_token",
                verification_bindings=(
                    "services.ingest.integrations._google_watch:resolve_push",
                ),
                tenant_binding_policy="watch_channel_installation",
                tenant_resolver_binding=(
                    "services.ingest.integrations._google_watch:resolve_push"
                ),
                acknowledgement_policy="ack_and_reconcile_on_failure",
                kafka_mode="reconciled_delta_drain",
                dispatcher_binding=(
                    "services.app.webhooks.google_push:google_drive_push"
                ),
                router_factory_binding=(
                    "services.app.webhooks.google_push:" "build_google_push_router"
                ),
            ),
        ),
    ),
    ProviderDefinition(
        "notion",
        "Notion",
        (),
        ("notion",),
        ("oauth2",),
        oauth_ingresses=(_oauth_ingress("notion"),),
        webhook_ingresses=(
            _webhook_ingress(
                "notion",
                "notion",
                "notion:object",
                route_path="/webhooks/notion/events",
                handler_mode="dedicated",
                acknowledgement_policy="dedicated_handler",
                kafka_mode="dedicated_shadow_then_ack",
                verification_handshake_binding=(
                    "services.ingest.integrations.notion.webhook:"
                    "is_verification_handshake"
                ),
                verification_handshake_handler_binding=(
                    "services.ingest.integrations.notion.webhook:"
                    "handle_verification_handshake"
                ),
                dedicated_handler_binding=(
                    "services.ingest.integrations.notion.webhook:" "handle_notion_event"
                ),
            ),
        ),
    ),
    ProviderDefinition(
        "atlassian",
        "Atlassian",
        ("jira_cloud",),
        ("jira",),
        ("api_token_basic",),
        webhook_ingresses=(
            _webhook_ingress(
                "jira",
                "jira",
                "jira:issue",
                route_path="/webhooks/jira/events",
            ),
        ),
    ),
    ProviderDefinition(
        "mercury",
        "Mercury",
        (),
        ("mercury",),
        ("api_token",),
        webhook_ingresses=(
            _webhook_ingress(
                "mercury",
                "mercury",
                "mercury:transaction",
            ),
        ),
    ),
    ProviderDefinition(
        "intuit",
        "Intuit",
        ("quickbooks", "quickbooks_online"),
        ("quickbooks",),
        ("oauth2",),
        webhook_ingresses=(
            _webhook_ingress(
                "quickbooks",
                "quickbooks",
                "quickbooks:object",
            ),
        ),
    ),
    ProviderDefinition(
        "grafana",
        "Grafana",
        (),
        ("grafana",),
        ("service_account_token",),
        webhook_ingresses=(
            _webhook_ingress(
                "grafana",
                "grafana",
                "grafana:alert",
                route_path="/webhooks/grafana/events",
            ),
        ),
    ),
    ProviderDefinition(
        "telegram",
        "Telegram",
        (),
        ("telegram",),
        ("mtproto_session",),
    ),
    ProviderDefinition(
        "brex",
        "Brex",
        (),
        ("brex",),
        ("api_token",),
        webhook_ingresses=(_webhook_ingress("brex", "brex", "brex:transaction"),),
    ),
    ProviderDefinition(
        "ramp",
        "Ramp",
        (),
        ("ramp",),
        ("oauth2_client_credentials",),
        webhook_ingresses=(_webhook_ingress("ramp", "ramp", "ramp:transaction"),),
    ),
    ProviderDefinition(
        "gusto",
        "Gusto",
        (),
        ("gusto",),
        ("oauth2",),
        webhook_ingresses=(_webhook_ingress("gusto", "gusto", "gusto:object"),),
    ),
    ProviderDefinition(
        "deel",
        "Deel",
        (),
        ("deel",),
        ("api_token",),
        webhook_ingresses=(_webhook_ingress("deel", "deel", "deel:payment"),),
    ),
    ProviderDefinition(
        "fireflies",
        "Fireflies.ai",
        ("fireflies_ai",),
        ("fireflies",),
        ("api_key",),
        webhook_ingresses=(
            _webhook_ingress(
                "fireflies",
                "fireflies",
                "fireflies:transcript",
            ),
        ),
    ),
    ProviderDefinition(
        "signal",
        "Signal",
        ("signal_messenger",),
        ("signal",),
        ("linked_device_session",),
    ),
    ProviderDefinition(
        "aws",
        "Amazon Web Services",
        ("amazon_web_services",),
        ("aws",),
        ("sigv4_assume_role", "sigv4_static_credentials"),
    ),
    ProviderDefinition(
        "miro",
        "Miro",
        (),
        ("miro",),
        ("api_token",),
        webhook_ingresses=(_webhook_ingress("miro", "miro", "miro:item"),),
    ),
    ProviderDefinition(
        "figma",
        "Figma",
        (),
        ("figma",),
        ("oauth2_pkce", "personal_access_token"),
        oauth_ingresses=(
            _oauth_ingress(
                "figma",
                install_path="/integrations/figma/oauth/start",
                callback_path="/integrations/figma/oauth/callback",
                install_handler="oauth_start",
                callback_handler="oauth_callback",
                mount_mode="native_router",
                public_result_paths=(),
            ),
        ),
        webhook_ingresses=(_webhook_ingress("figma", "figma", "figma:event"),),
    ),
    ProviderDefinition(
        "carta",
        "Carta",
        (),
        ("carta",),
        ("oauth2_client_credentials",),
    ),
    ProviderDefinition(
        "hibob",
        "HiBob",
        ("bob",),
        ("hibob",),
        ("service_user_basic",),
        webhook_ingresses=(_webhook_ingress("hibob", "hibob", "hibob:object"),),
    ),
    ProviderDefinition(
        "ashby",
        "Ashby",
        (),
        ("ashby",),
        ("api_key_basic",),
        webhook_ingresses=(
            _webhook_ingress(
                "ashby",
                "ashby",
                "ashby:object",
                route_path="/webhooks/ashby/{installation_id}",
                tenant_binding="path_then_payload",
            ),
        ),
    ),
    ProviderDefinition(
        "linkedin",
        "LinkedIn",
        ("linked_in",),
        ("linkedin",),
        ("oauth2",),
    ),
    ProviderDefinition(
        "meta",
        "Meta",
        ("facebook", "meta_platforms"),
        ("whatsapp", "facebook_pages"),
        ("oauth2", "app_secret_webhook"),
        oauth_ingresses=(_oauth_ingress("facebook_pages"),),
        dedicated_ingresses=(
            _dedicated_ingress(
                "whatsapp_webhook",
                "whatsapp",
                "/integrations/whatsapp/webhook",
                ("GET", "POST"),
                "webhook",
                "whatsapp:message",
                verification_policy="meta_verify_token_hmac",
                verification_bindings=(
                    "services.app.gateway.whatsapp_router:"
                    "_verify_token_matches_installation",
                    "services.ingest.integrations.whatsapp.signature:"
                    "verify_signature",
                ),
                tenant_binding_policy="phone_number_installation",
                tenant_resolver_binding=(
                    "services.app.gateway.whatsapp_router:" "_lookup_installation"
                ),
                acknowledgement_policy="durable_or_inline_before_ack",
                kafka_mode="flagged_kafka_first_with_inline_fallback",
                dispatcher_binding=(
                    "services.app.gateway.whatsapp_router:" "build_whatsapp_router"
                ),
                router_factory_binding=(
                    "services.app.gateway.whatsapp_router:" "build_whatsapp_router"
                ),
                router_factory_accepts_debug_endpoints=True,
            ),
            _dedicated_ingress(
                "facebook_pages_webhook",
                "facebook_pages",
                "/integrations/facebook_pages/webhook",
                ("GET", "POST"),
                "webhook",
                "facebook_pages:message",
                verification_policy="meta_verify_token_hmac",
                verification_bindings=(
                    "services.app.gateway.facebook_pages_router:"
                    "_verify_token_matches_installation",
                    "services.ingest.integrations.whatsapp.signature:"
                    "verify_signature",
                ),
                tenant_binding_policy="page_installation",
                tenant_resolver_binding=(
                    "services.app.gateway.facebook_pages_router:" "_lookup_installation"
                ),
                acknowledgement_policy="durable_or_inline_before_ack",
                kafka_mode="flagged_kafka_first_with_inline_fallback",
                dispatcher_binding=(
                    "services.app.gateway.facebook_pages_router:"
                    "build_facebook_pages_router"
                ),
                router_factory_binding=(
                    "services.app.gateway.facebook_pages_router:"
                    "build_facebook_pages_router"
                ),
            ),
        ),
    ),
    ProviderDefinition(
        provider_id="linear",
        display_name="Linear",
        aliases=(),
        source_ids=(),
        auth_strategies=("webhook_hmac",),
        webhook_ingresses=(
            _webhook_ingress(
                "linear",
                None,
                "linear:webhook",
                kafka_mode="inline_only",
            ),
        ),
        data_plane=False,
        operation_policy_ids=(),
    ),
    ProviderDefinition(
        provider_id="stripe",
        display_name="Stripe",
        aliases=(),
        source_ids=(),
        auth_strategies=("webhook_hmac",),
        webhook_ingresses=(
            _webhook_ingress(
                "stripe",
                None,
                "stripe:webhook",
                kafka_mode="inline_only",
            ),
        ),
        data_plane=False,
        operation_policy_ids=(),
    ),
)


_ACK_BY_TRANSPORT: Mapping[LiveTransportKind, AcknowledgementPolicy] = {
    "webhook": "durable_before_ack",
    "pubsub": "durable_before_ack",
    "websocket": "checkpoint_after_durable",
    "mtproto": "checkpoint_after_durable",
    "json_rpc": "checkpoint_after_durable",
    "api_poll": "cursor_after_durable",
    "queue_poll": "cursor_after_durable",
}
_DELIVERY_BY_TRANSPORT: Mapping[LiveTransportKind, DeliveryPolicy] = {
    "webhook": "at_least_once",
    "pubsub": "at_least_once",
    "websocket": "replayable_stream",
    "mtproto": "replayable_stream",
    "json_rpc": "replayable_stream",
    "api_poll": "replayable_pull",
    "queue_poll": "replayable_pull",
}


def _certification(
    source_id: str,
    *,
    notes: tuple[str, ...] = (),
) -> Certification:
    return Certification(
        status="unverified",
        require_test_kit=True,
        require_evidence=True,
        require_canary=True,
        test_kit_id=f"ingest.test_kit.{source_id}",
        evidence_id=f"ingest.evidence.{source_id}",
        canary_id=f"ingest.canary.{source_id}",
        notes=notes,
    )


_HANDLER_PACKAGE = "services.ingest.ingestion.handlers"


def _native_connect(
    source_id: str,
    kind: str,
    *,
    payload_fields: tuple[str, ...],
    preflight_payload_fields: tuple[str, ...] = (),
    scope_aliases: tuple[str, ...] = (),
) -> NativeConnectDefinition:
    """Build the standard preflight/finalize native-connect route family."""

    return NativeConnectDefinition(
        kind=kind,
        preflight_path=f"/integrations/{source_id}/connect/preflight",
        finalize_path=f"/integrations/{source_id}/connect/finalize",
        preflight_payload_fields=preflight_payload_fields,
        payload_fields=payload_fields,
        scope_aliases=scope_aliases,
    )


_COMMON_BROWSER_COMPLETION_CHECKS = (
    "provider handoff prepared",
    "customer-cloud secret refs created or confirmed",
    "source install status is pollable",
    "onboarding trigger or source-native install row is present",
    "sanitized connection proof can be read from observations",
)
_OAUTH_BROWSER_GATES = (
    "admin signs in and completes MFA when prompted",
    "admin approves provider app scopes",
)
_TOKEN_BROWSER_GATES = (
    "admin signs in and completes MFA when prompted",
    "admin creates or approves a least-privilege service credential",
)
_LOCAL_SESSION_BROWSER_GATES = (
    "admin signs in and completes MFA when prompted",
    "admin authorizes the local customer-cloud session or device link",
)
_DWD_BROWSER_GATES = (
    "Google Workspace admin signs in and completes MFA when prompted",
    (
        "Google Workspace admin authorizes the Fyralis service account "
        "client ID and scopes"
    ),
    "admin approves workspace inclusion scope",
)


def _browser_agent(
    settings_targets: tuple[str, ...],
    agent_collects: tuple[str, ...],
    agent_generates: tuple[str, ...],
    human_gates: tuple[str, ...],
) -> BrowserAgentDefinition:
    """Build one immutable source-owned browser automation declaration."""

    return BrowserAgentDefinition(
        settings_targets=settings_targets,
        agent_collects=agent_collects,
        agent_generates=agent_generates,
        human_gates=human_gates,
        completion_checks=_COMMON_BROWSER_COMPLETION_CHECKS,
    )


def _local_rehearsal(
    kind: str,
    *,
    needs_public_url: bool,
    env: tuple[str, ...],
    required_env: tuple[str, ...],
    manual_gate_names: tuple[str, ...],
    install_endpoint: str | None = None,
    callback_path: str | None = None,
    preflight_endpoint: str | None = None,
    finalize_endpoint: str | None = None,
    webhook_path: str | None = None,
    derived_env: tuple[tuple[str, str], ...] = (),
    default_env: tuple[tuple[str, str], ...] = (),
    runtime_components: tuple[str, ...] = (),
) -> LocalRehearsalDefinition:
    """Build one immutable customer-local provider rehearsal declaration."""

    return LocalRehearsalDefinition(
        kind=kind,
        needs_public_url=needs_public_url,
        env=env,
        required_env=required_env,
        manual_gate_names=manual_gate_names,
        install_endpoint=install_endpoint,
        callback_path=callback_path,
        preflight_endpoint=preflight_endpoint,
        finalize_endpoint=finalize_endpoint,
        webhook_path=webhook_path,
        derived_env=derived_env,
        default_env=default_env,
        runtime_components=runtime_components,
    )


def _onboarding(
    method: str,
    discovery_target: str,
    native_connect: NativeConnectDefinition,
    *,
    browser_agent: BrowserAgentDefinition,
    default_scopes: tuple[str, ...] = (),
    provider_permissions: tuple[str, ...] = (),
    ingress_paths: tuple[str, ...] = (),
    required_refs: tuple[str, ...] = (),
    no_ingress_reason: str | None = None,
    local_rehearsal: LocalRehearsalDefinition | None = None,
    required_inputs: tuple[str, ...] | None = None,
    optional_inputs: tuple[str, ...] | None = None,
    provider_console_url: str | None = None,
    generic_authorization_mode: str | None = None,
) -> OnboardingDefinition:
    """Build one immutable source-owned BYOC onboarding declaration."""

    return OnboardingDefinition(
        required_inputs=required_inputs,
        optional_inputs=optional_inputs,
        provider_console_url=provider_console_url,
        generic_authorization_mode=generic_authorization_mode,
        method=method,
        discovery_target=discovery_target,
        native_connect=native_connect,
        browser_agent=browser_agent,
        default_scopes=default_scopes,
        provider_permissions=provider_permissions,
        ingress_paths=ingress_paths,
        required_refs=required_refs,
        no_ingress_reason=no_ingress_reason,
        local_rehearsal=local_rehearsal,
    )


def _source(
    source_id: str,
    provider_id: str,
    display_name: str,
    *,
    display: SourceDisplayDefinition,
    ui_slug: str | None = None,
    aliases: tuple[str, ...] = (),
    data_objects: tuple[str, ...],
    default_scopes: tuple[str, ...] = (),
    provider_permissions: tuple[str, ...] = (),
    cli_ingress_paths: tuple[str, ...] = (),
    required_refs: tuple[str, ...] = (),
    history: HistoryKind | None,
    installation_identifiers: tuple[str, ...],
    runtime_identifiers: tuple[str, ...],
    ingress_routes: tuple[tuple[IngressKind, str], ...],
    normalization_inputs: tuple[str, ...],
    normalizer_bindings: tuple[str, ...],
    idempotency_builder_bindings: tuple[str, ...],
    allowed_observation_kinds: tuple[AllowedObservationKind, ...],
    trust_tiers: tuple[AllowedTrustTier, ...],
    live_transports: tuple[LiveTransportKind, ...],
    onboarding: OnboardingDefinition,
    planner_client_builder_binding: str | None = None,
    onboarding_failure_binding: str | None = None,
    installation_management: InstallationManagementDefinition | None = None,
    installation_status_loader_binding: str | None = None,
    certification_notes: tuple[str, ...] = (),
    credential_refresh: CredentialRefreshDefinition | None = None,
    capability_flags: tuple[str, ...] = (),
    idempotent_operation_ids: tuple[str, ...] = (),
    idempotency_key_operation_ids: tuple[str, ...] = (),
    unsafe_operation_ids: tuple[str, ...] = (),
    retryable_status_codes: tuple[int, ...] = (_STANDARD_RETRYABLE_HTTP_STATUSES),
    rate_limit_header_parser_id: str | None = None,
    provider_transport_enforced: bool = False,
    operator_live_ingress: str | None = None,
    no_ingress_reason: str | None = None,
    local_rehearsal: LocalRehearsalDefinition | None = None,
) -> SourceDefinition:
    acknowledgement_policies = tuple(
        _ACK_BY_TRANSPORT[transport] for transport in live_transports
    )
    delivery_policies = tuple(
        _DELIVERY_BY_TRANSPORT[transport] for transport in live_transports
    )
    live_bindings = tuple(
        f"ingest.live.{source_id}.{transport}" for transport in live_transports
    )
    history_package = "services.ingest.ingestion"
    installation_package = "services.ingest.ingestion.installations"
    return SourceDefinition(
        source_id=source_id,
        ui_slug=ui_slug or source_id,
        provider_id=provider_id,
        display_name=display_name,
        display=display,
        aliases=aliases,
        data_objects=data_objects,
        history=history,
        installation_identifiers=installation_identifiers,
        runtime_identifiers=runtime_identifiers,
        ingress_routes=tuple(
            IngressRoute(ingress_kind, channel)
            for ingress_kind, channel in ingress_routes
        ),
        normalization_inputs=normalization_inputs,
        normalizer_bindings=normalizer_bindings,
        idempotency_builder_bindings=idempotency_builder_bindings,
        allowed_observation_kinds=allowed_observation_kinds,
        trust_tiers=trust_tiers,
        live_transports=live_transports,
        acknowledgement_policies=cast(
            tuple[AcknowledgementPolicy, ...],
            acknowledgement_policies,
        ),
        delivery_policies=cast(
            tuple[DeliveryPolicy, ...],
            delivery_policies,
        ),
        live_bindings=live_bindings,
        planner_binding=(
            f"{history_package}.planners.{source_id}:plan_shards_{source_id}"
            if history is not None
            else None
        ),
        fetcher_binding=(
            f"{history_package}.fetchers.{source_id}:fetch_page_{source_id}"
            if history is not None
            else None
        ),
        reconciler_binding=(
            f"{history_package}.reconcilers.{source_id}:reconcile_{source_id}"
            if history is not None
            else None
        ),
        installation_adapter=(
            InstallationAdapter(
                loader_binding=(
                    f"{installation_package}:load_{source_id}_installation"
                    if history is not None
                    else None
                ),
                status_loader_binding=(
                    installation_status_loader_binding
                    or (
                        f"{history_package}.installation_status:"
                        "load_managed_installation_status_rows"
                        if installation_management is not None
                        else (
                            f"{history_package}.installation_status:"
                            "load_provider_installation_status_rows"
                        )
                    )
                ),
                planner_client_builder_binding=planner_client_builder_binding,
                onboarding_failure_binding=onboarding_failure_binding,
                management=installation_management,
            )
            if history is not None or installation_management is not None
            else None
        ),
        connect_router_binding=(
            f"services.ingest.integrations.{source_id}.oauth:router"
        ),
        onboarding=replace(
            onboarding,
            default_scopes=default_scopes,
            provider_permissions=provider_permissions,
            ingress_paths=cli_ingress_paths,
            required_refs=required_refs,
            no_ingress_reason=no_ingress_reason,
            local_rehearsal=local_rehearsal,
        ),
        certification=_certification(
            source_id,
            notes=certification_notes,
        ),
        credential_refresh=credential_refresh,
        capability_flags=capability_flags,
        operation_policies=_operation_policies(
            idempotent=idempotent_operation_ids,
            idempotency_key=idempotency_key_operation_ids,
            unsafe=unsafe_operation_ids,
            retryable_status_codes=retryable_status_codes,
            rate_limit_header_parser_id=rate_limit_header_parser_id,
        ),
        provider_transport_enforced=provider_transport_enforced,
        operator_live_ingress=operator_live_ingress,
    )


def _display(
    order: int,
    category: SourceCategory,
    description: str,
    connection_method: SourceConnectionMethod,
    setup_requirements: str,
    supported_sync_modes: tuple[SourceSyncMode, ...],
    *,
    display_name_override: str | None = None,
    notice: str | None = None,
) -> SourceDisplayDefinition:
    """Build source-owned onboarding marketplace metadata."""

    return SourceDisplayDefinition(
        order=order,
        category=category,
        description=description,
        connection_method=connection_method,
        setup_requirements=setup_requirements,
        supported_sync_modes=supported_sync_modes,
        display_name_override=display_name_override,
        notice=notice,
    )


SOURCE_DEFINITIONS: tuple[SourceDefinition, ...] = (
    _source(
        "slack",
        "slack",
        "Slack",
        display=_display(
            0,
            "Communication",
            "Channels, events, and consented DMs.",
            "OAuth",
            (
                "Slack workspace admin approval, app/OAuth install, signing "
                "secret, channel allowlist, optional DM consent."
            ),
            (
                "Dry run",
                "Limited backfill",
                "Live events",
                "Backfill plus live",
            ),
        ),
        default_scopes=("#leadership", "#finance-ops", "#customer-success"),
        provider_permissions=(
            "channels:history",
            "groups:history",
            "users:read",
            "team:read",
        ),
        cli_ingress_paths=(
            "/integrations/slack/callback",
            "/webhooks/slack/events",
        ),
        required_refs=("oauth_client", "bot_token", "signing_secret"),
        local_rehearsal=_local_rehearsal(
            "oauth_app",
            needs_public_url=True,
            install_endpoint="/integrations/slack/install",
            callback_path="/integrations/slack/callback",
            webhook_path="/webhooks/slack/events",
            env=(
                "SLACK_CLIENT_ID",
                "SLACK_CLIENT_SECRET",
                "SLACK_SIGNING_SECRET",
                "SLACK_REDIRECT_URI",
                "OAUTH_STATE_HMAC_KEY",
            ),
            required_env=(
                "SLACK_CLIENT_ID",
                "SLACK_CLIENT_SECRET",
                "SLACK_SIGNING_SECRET",
                "SLACK_REDIRECT_URI",
                "OAUTH_STATE_HMAC_KEY",
            ),
            derived_env=(("WEBHOOK_SECRET_SLACK", "SLACK_SIGNING_SECRET"),),
            manual_gate_names=(
                "slack_app_creation_or_admin_approval",
                "slack_oauth_consent",
            ),
        ),
        data_objects=("channel_message", "message_edit", "direct_message"),
        history="api",
        installation_identifiers=("team_id",),
        runtime_identifiers=("team_id",),
        ingress_routes=(
            ("webhook", "slack:message"),
            ("backfill", "slack:message"),
        ),
        normalization_inputs=("slack:message",),
        idempotency_builder_bindings=(
            "services.ingest.ingestion.idempotency:slack_message",
        ),
        normalizer_bindings=(
            "services.ingest.ingestion.handlers.slack:handle_slack_message",
        ),
        allowed_observation_kinds=("signal",),
        trust_tiers=("attested_agent",),
        live_transports=("webhook",),
        onboarding=_onboarding(
            "oauth",
            (
                "workspace, public/private channels the app can access, "
                "users, and events"
            ),
            _native_connect(
                "slack",
                "oauth_callback_native_connect",
                payload_fields=(
                    "workspace_id",
                    "approved_channel_ids",
                    "oauth_redirect_url",
                    "events_request_url",
                    "installation_id",
                ),
            ),
            browser_agent=_browser_agent(
                ("Slack app settings", "OAuth scopes", "event subscriptions"),
                ("workspace id", "channel scope", "event callback URL"),
                ("signing secret ref", "Slack event scope contract"),
                _OAUTH_BROWSER_GATES,
            ),
            provider_console_url="https://api.slack.com/apps",
        ),
        idempotent_operation_ids=(
            "users.info",
            "conversations.info",
            "conversations.list",
            "conversations.history",
        ),
        unsafe_operation_ids=(
            "chat.postMessage",
            "oauth.v2.access",
        ),
        rate_limit_header_parser_id="http.retry_after",
        provider_transport_enforced=True,
        planner_client_builder_binding=(
            "services.ingest.ingestion.installations:" "build_slack_planner_client"
        ),
    ),
    _source(
        "github",
        "github",
        "GitHub",
        installation_status_loader_binding=(
            "services.ingest.ingestion.installation_status:"
            "load_github_installation_status_rows"
        ),
        display=_display(
            4,
            "Engineering",
            "Repositories, pull requests, issues, and code intelligence.",
            "OAuth",
            (
                "GitHub App installation, repository selection, webhook "
                "secret, org admin approval, installation ID mapping."
            ),
            (
                "Dry run",
                "Limited backfill",
                "Live events",
                "Backfill plus live",
            ),
        ),
        default_scopes=("selected-repositories", "pull-requests", "issues"),
        provider_permissions=(
            "repository metadata",
            "pull requests",
            "issues",
            "webhooks",
        ),
        cli_ingress_paths=(
            "/integrations/github/callback",
            "/webhooks/github",
        ),
        required_refs=("github_app_private_key", "webhook_secret"),
        local_rehearsal=_local_rehearsal(
            "github_app",
            needs_public_url=True,
            install_endpoint="/integrations/github/install",
            callback_path="/integrations/github/callback",
            webhook_path="/webhooks/github",
            env=(
                "GITHUB_APP_SLUG",
                "GITHUB_APP_ID",
                "GITHUB_APP_PRIVATE_KEY",
                "WEBHOOK_SECRET_GITHUB",
                "OAUTH_STATE_HMAC_KEY",
            ),
            required_env=(
                "GITHUB_APP_SLUG",
                "GITHUB_APP_ID",
                "GITHUB_APP_PRIVATE_KEY",
                "WEBHOOK_SECRET_GITHUB",
                "OAUTH_STATE_HMAC_KEY",
            ),
            manual_gate_names=(
                "github_provider_admin_approval",
                "github_app_installation_approval",
            ),
        ),
        aliases=("git_hub",),
        data_objects=(
            "issue",
            "pull_request",
            "issue_comment",
            "commit",
            "pull_request_review",
            "check_run",
        ),
        history="api",
        installation_identifiers=("installation_id",),
        runtime_identifiers=("installation_id", "repository_id"),
        ingress_routes=(
            ("webhook", "github:webhook"),
            ("backfill", "github:webhook"),
        ),
        normalization_inputs=("github:webhook",),
        idempotency_builder_bindings=(
            "services.ingest.ingestion.idempotency:github_push",
            "services.ingest.ingestion.idempotency:github_object",
        ),
        normalizer_bindings=(
            "services.ingest.ingestion.handlers.github:handle_github_webhook",
        ),
        allowed_observation_kinds=("signal", "state_change"),
        trust_tiers=("authoritative", "inferential"),
        live_transports=("webhook",),
        onboarding=_onboarding(
            "oauth",
            ("installations, repositories, pull requests, issues, and " "webhooks"),
            _native_connect(
                "github",
                "github_app_native_connect",
                payload_fields=(
                    "installation_id",
                    "organization",
                    "repository_selection",
                    "oauth_redirect_url",
                    "events_request_url",
                ),
            ),
            browser_agent=_browser_agent(
                (
                    "GitHub App settings",
                    "organization installations",
                    "webhook settings",
                ),
                (
                    "installation id",
                    "repository scope",
                    "webhook delivery URL",
                ),
                ("webhook secret ref", "repository scope contract"),
                (
                    "org admin signs in and completes MFA when prompted",
                    (
                        "org admin approves the GitHub App installation "
                        "and repositories"
                    ),
                ),
            ),
            provider_console_url="https://github.com/settings/apps",
        ),
        idempotent_operation_ids=(
            "installation_repositories.list",
            "repo_events.issues.list",
            "repo_events.issues.head",
            "repo_events.pull_requests.list",
            "repo_events.pull_requests.head",
            "repo_events.issue_comments.list",
            "repo_events.issue_comments.head",
            "repo_events.commits.list",
            "repo_events.commits.head",
            "pull_reviews.list",
            "check_runs.list",
        ),
        unsafe_operation_ids=("installation_token.mint",),
        retryable_status_codes=(
            403,
            *_STANDARD_RETRYABLE_HTTP_STATUSES,
        ),
        rate_limit_header_parser_id="github.rate_limit_headers",
        provider_transport_enforced=True,
        planner_client_builder_binding=(
            "services.ingest.ingestion.installations:" "build_github_planner_client"
        ),
    ),
    _source(
        "discord",
        "discord",
        "Discord",
        installation_status_loader_binding=(
            "services.ingest.ingestion.installation_status:"
            "load_discord_installation_status_rows"
        ),
        display=_display(
            7,
            "Communication",
            "Community and team message streams.",
            "Gateway",
            (
                "Discord app or bot token, guild/channel allowlist, gateway "
                "intents, single-worker lease readiness."
            ),
            ("Dry run", "Limited backfill", "Live events"),
        ),
        default_scopes=("pilot-guilds", "approved-channels"),
        provider_permissions=(
            "bot token",
            "message content intent",
            "guild read",
        ),
        cli_ingress_paths=(
            "/integrations/discord/callback",
            "/webhooks/discord",
        ),
        required_refs=("bot_token", "signing_secret"),
        local_rehearsal=_local_rehearsal(
            "oauth_app",
            needs_public_url=True,
            install_endpoint="/integrations/discord/install",
            callback_path="/integrations/discord/callback",
            webhook_path="/webhooks/discord",
            env=(
                "DISCORD_CLIENT_ID",
                "DISCORD_CLIENT_SECRET",
                "DISCORD_REDIRECT_URI",
                "DISCORD_APPLICATION_ID",
                "DISCORD_BOT_TOKEN",
                "WEBHOOK_SECRET_DISCORD",
                "OAUTH_STATE_HMAC_KEY",
            ),
            required_env=(
                "DISCORD_CLIENT_ID",
                "DISCORD_CLIENT_SECRET",
                "DISCORD_REDIRECT_URI",
                "DISCORD_APPLICATION_ID",
                "DISCORD_BOT_TOKEN",
                "WEBHOOK_SECRET_DISCORD",
                "OAUTH_STATE_HMAC_KEY",
            ),
            manual_gate_names=(
                "discord_application_creation_or_update",
                "discord_oauth_consent",
            ),
        ),
        data_objects=("guild_message", "thread_message", "interaction"),
        history="api",
        installation_identifiers=("guild_id",),
        runtime_identifiers=("guild_id", "application_id"),
        ingress_routes=(
            ("gateway", "discord:message"),
            ("webhook", "discord:interaction"),
            ("backfill", "discord:message"),
        ),
        normalization_inputs=("discord:message", "discord:interaction"),
        idempotency_builder_bindings=(
            "services.ingest.ingestion.idempotency:discord_event",
        ),
        normalizer_bindings=(
            "services.ingest.ingestion.handlers.discord:handle_discord_message",
            "services.ingest.ingestion.handlers.discord:handle_discord_webhook",
        ),
        allowed_observation_kinds=("signal",),
        trust_tiers=("attested_agent",),
        live_transports=("websocket", "webhook"),
        onboarding=_onboarding(
            "oauth_plus_gateway",
            (
                "guilds, message channels, private channels, forum/media "
                "posts, and threads"
            ),
            _native_connect(
                "discord",
                "oauth_gateway_native_connect",
                payload_fields=(
                    "guild_id",
                    "application_id",
                    "approved_channel_ids",
                    "oauth_redirect_url",
                    "events_request_url",
                ),
            ),
            browser_agent=_browser_agent(
                (
                    "application OAuth2 settings",
                    "bot settings",
                    "guild settings",
                ),
                (
                    "application id",
                    "guild ids",
                    "channel ids",
                    "enabled intents",
                ),
                ("bot/gateway session contract", "webhook verifier ref"),
                (
                    "server admin signs in and completes MFA when prompted",
                    ("server admin approves bot install and gateway " "intents"),
                ),
            ),
            provider_console_url=("https://discord.com/developers/applications"),
        ),
        idempotent_operation_ids=(
            "/guilds/{guild_id}/members/{user_id}",
            "/channels/{channel_id}",
            "/applications/{application_id}/commands",
            "/users/@me/guilds",
            "/guilds/{guild_id}/channels",
            "/guilds/{guild_id}/threads/active",
            "/channels/{channel_id}/threads/archived/public",
            "/channels/{channel_id}/threads/archived/private",
            "/channels/{channel_id}/messages",
            "/gateway/bot",
        ),
        unsafe_operation_ids=(
            "/webhooks/{application_id}/{interaction_token}",
            "/oauth2/token",
        ),
        rate_limit_header_parser_id="discord.rate_limit_headers",
        provider_transport_enforced=True,
        planner_client_builder_binding=(
            "services.ingest.ingestion.installations:" "build_discord_planner_client"
        ),
    ),
    _source(
        "gmail",
        "google",
        "Gmail",
        display=_display(
            1,
            "Productivity",
            "Workspace email with watch and history polling.",
            "Workspace DWD",
            (
                "Google Workspace admin approval, domain-wide delegation "
                "setup, mailbox scope, Pub/Sub watch topic, history-poller "
                "access."
            ),
            ("Dry run", "Limited backfill", "Live events"),
        ),
        default_scopes=("gmail.metadata", "approved-mailboxes"),
        provider_permissions=(
            "Domain-Wide Delegation",
            "gmail.metadata",
            "directory users/groups read",
            "pubsub.topics.attachSubscription",
        ),
        cli_ingress_paths=("/webhooks/gmail/pubsub",),
        required_refs=(
            "workspace_domain",
            "admin_email",
            "dwd_grant_receipt",
        ),
        aliases=("google_mail",),
        data_objects=("mail_message", "thread_link"),
        history="api",
        installation_management=InstallationManagementDefinition(
            source="gmail",
            table="gmail_installations",
            scope_column="workspace_domain",
            ref_columns=(),
            entity_table="gmail_mailbox_watches",
            entity_install_column="gmail_installation_id",
            base_url_column=None,
            status_detail_columns=(
                "service_account_email",
                "scope",
                "resolved_user_count",
                "resolved_at",
            ),
            status_credential_column_groups=(("service_account_email",),),
        ),
        installation_identifiers=("workspace_customer_id", "mailbox_email"),
        runtime_identifiers=("gmail_installation_id", "email_address"),
        ingress_routes=(
            ("backfill", "gmail:"),
            ("poll", "gmail:"),
        ),
        normalization_inputs=("gmail:",),
        idempotency_builder_bindings=(
            "services.ingest.ingestion.idempotency:gmail_message",
        ),
        normalizer_bindings=("services.ingest.ingestion.handlers.gmail:handle_gmail",),
        allowed_observation_kinds=("signal",),
        trust_tiers=("attested_agent",),
        live_transports=("pubsub", "api_poll"),
        onboarding=_onboarding(
            "dwd",
            ("mailboxes, labels, watch channels, and Pub/Sub topic " "readiness"),
            _native_connect(
                "gmail",
                "google_workspace_dwd",
                preflight_payload_fields=(
                    "workspace_domain",
                    "admin_email",
                    "scope",
                ),
                payload_fields=(
                    "workspace_domain",
                    "admin_email",
                    "scope",
                    "inclusion_spec",
                ),
                scope_aliases=("gmail.metadata",),
            ),
            browser_agent=_browser_agent(
                (
                    "Domain-wide delegation",
                    "API controls",
                    "mailbox inclusion scope",
                    "Pub/Sub topic",
                ),
                (
                    "workspace domain",
                    "admin email",
                    "mailbox/user/group/org-unit scope",
                ),
                (
                    "DWD preflight payload",
                    "mailbox inclusion contract",
                    "watch verifier ref",
                ),
                _DWD_BROWSER_GATES,
            ),
            required_inputs=(
                "workspace_domain",
                "admin_email",
                "dwd_grant",
            ),
            optional_inputs=(
                "scope",
                "inclusion_spec",
                "pubsub_topic",
                "watch_channel_id",
            ),
            provider_console_url=(
                "https://admin.google.com/ac/owl/domainwidedelegation"
            ),
        ),
        idempotent_operation_ids=(
            "watch.stop",
            "messages.list",
            "history.list",
            "messages.get",
            "profile.get",
            "directory.users.list",
            "directory.groups.list",
            "directory.group_members.list",
            "directory.org_units.list",
            "directory.users_by_org_unit.list",
            "pubsub.topic.create",
            "pubsub.topic.delete",
            "pubsub.subscription.create",
            "pubsub.subscription.delete",
            "pubsub.iam.get",
            "pubsub.iam.set",
        ),
        unsafe_operation_ids=(
            "watch.create",
            "dwd.token.exchange",
        ),
        retryable_status_codes=(
            403,
            *_STANDARD_RETRYABLE_HTTP_STATUSES,
        ),
        rate_limit_header_parser_id="http.retry_after",
        provider_transport_enforced=True,
    ),
    _source(
        "notion",
        "notion",
        "Notion",
        display=_display(
            6,
            "Knowledge",
            "Pages, databases, and workspace knowledge.",
            "OAuth",
            (
                "Workspace integration token, pages and databases shared to "
                "integration, workspace owner approval, selector scope."
            ),
            ("Dry run", "Limited backfill"),
        ),
        default_scopes=("shared-pages", "shared-databases"),
        provider_permissions=(
            "read content",
            "read users",
            "database access",
        ),
        cli_ingress_paths=(
            "/integrations/notion/callback",
            "/webhooks/notion/events",
        ),
        required_refs=("oauth_client", "token_ref"),
        local_rehearsal=_local_rehearsal(
            "oauth_app",
            needs_public_url=True,
            install_endpoint="/integrations/notion/install",
            callback_path="/integrations/notion/callback",
            webhook_path="/webhooks/notion/events",
            env=(
                "NOTION_CLIENT_ID",
                "NOTION_CLIENT_SECRET",
                "NOTION_REDIRECT_URI",
                "OAUTH_STATE_HMAC_KEY",
                "NOTION_WEBHOOK_VERIFICATION_TOKEN",
            ),
            required_env=(
                "NOTION_CLIENT_ID",
                "NOTION_CLIENT_SECRET",
                "NOTION_REDIRECT_URI",
                "OAUTH_STATE_HMAC_KEY",
            ),
            manual_gate_names=(
                "notion_integration_creation_or_update",
                "notion_oauth_consent",
                "notion_webhook_verification_token_copy",
            ),
        ),
        data_objects=("database", "page", "block", "comment"),
        history="api",
        installation_identifiers=("workspace_id",),
        runtime_identifiers=("workspace_id", "entity_id"),
        ingress_routes=(
            ("webhook", "notion:object"),
            ("backfill", "notion:object"),
            ("poll", "notion:object"),
        ),
        normalization_inputs=("notion:object",),
        idempotency_builder_bindings=(
            "services.ingest.ingestion.idempotency:notion_object",
        ),
        normalizer_bindings=(
            "services.ingest.ingestion.handlers.notion:handle_notion_object",
        ),
        allowed_observation_kinds=("signal", "state_change"),
        trust_tiers=("attested_agent",),
        live_transports=("webhook", "api_poll"),
        onboarding=_onboarding(
            "oauth",
            "shared pages, databases, users, and webhook eligibility",
            _native_connect(
                "notion",
                "oauth_callback_native_connect",
                payload_fields=(
                    "workspace_id",
                    "shared_page_ids",
                    "shared_database_ids",
                    "oauth_redirect_url",
                    "events_request_url",
                    "installation_id",
                ),
            ),
            browser_agent=_browser_agent(
                (
                    "integration settings",
                    "workspace sharing",
                    "database/page settings",
                ),
                ("workspace id", "shared pages", "shared databases"),
                (
                    "workspace/page scope contract",
                    "webhook eligibility contract",
                ),
                _OAUTH_BROWSER_GATES,
            ),
            provider_console_url="https://www.notion.so/my-integrations",
        ),
        planner_client_builder_binding=(
            "services.ingest.ingestion.installations:" "build_notion_planner_client"
        ),
        idempotent_operation_ids=(
            "search",
            "databases.query",
            "blocks.children.list",
            "comments.list",
            "pages.retrieve",
            "users.me",
        ),
        unsafe_operation_ids=("oauth.token.exchange",),
        rate_limit_header_parser_id="http.retry_after",
        provider_transport_enforced=True,
    ),
    _source(
        "google_calendar",
        "google",
        "Google Calendar",
        display=_display(
            2,
            "Productivity",
            "Calendar events and shared calendars.",
            "Workspace DWD",
            (
                "Google Workspace admin approval, domain-wide delegation "
                "setup, calendar scope, allowlist, and local polling access."
            ),
            ("Dry run", "Limited backfill"),
        ),
        ui_slug="google-calendar",
        default_scopes=("calendar.readonly", "pilot-calendars"),
        provider_permissions=(
            "Domain-Wide Delegation",
            "calendar.readonly",
            "directory users/groups read",
        ),
        cli_ingress_paths=(),
        required_refs=(
            "workspace_domain",
            "admin_email",
            "dwd_grant_receipt",
        ),
        no_ingress_reason=(
            "Google Calendar DWD install is poll-only; no webhook or push "
            "watch is configured."
        ),
        aliases=("gcal", "calendar"),
        data_objects=("calendar_event",),
        history="api",
        installation_management=InstallationManagementDefinition(
            source="google_calendar",
            table="google_calendar_installations",
            scope_column="workspace_domain",
            ref_columns=(),
            entity_table="google_calendar_calendars",
            entity_install_column="google_calendar_installation_id",
            base_url_column=None,
            native_google_watch_table=True,
            status_detail_columns=(
                "service_account_email",
                "scope",
                "resolved_calendar_count",
                "resolved_at",
            ),
            status_credential_column_groups=(("service_account_email",),),
        ),
        installation_identifiers=(
            "workspace_customer_id",
            "owner_email",
            "calendar_id",
        ),
        runtime_identifiers=("channel_id", "resource_id", "calendar_id"),
        ingress_routes=(
            ("backfill", "google_calendar:event"),
            ("poll", "google_calendar:event"),
        ),
        normalization_inputs=("google_calendar:event",),
        idempotency_builder_bindings=(
            "services.ingest.ingestion.idempotency:google_calendar_event",
        ),
        normalizer_bindings=(
            "services.ingest.ingestion.handlers.google_calendar:"
            "handle_google_calendar_event",
        ),
        allowed_observation_kinds=("signal", "state_change"),
        trust_tiers=("authoritative",),
        live_transports=("webhook", "api_poll"),
        onboarding=_onboarding(
            "dwd",
            "calendars and shared calendar inclusion scope",
            _native_connect(
                "google_calendar",
                "google_workspace_dwd",
                preflight_payload_fields=(
                    "workspace_domain",
                    "admin_email",
                    "scope",
                ),
                payload_fields=(
                    "workspace_domain",
                    "admin_email",
                    "scope",
                    "inclusion_spec",
                ),
                scope_aliases=("calendar.readonly",),
            ),
            browser_agent=_browser_agent(
                (
                    "Domain-wide delegation",
                    "Calendar API controls",
                    "calendar inclusion scope",
                ),
                (
                    "workspace domain",
                    "admin email",
                    "calendar/user/group/org-unit scope",
                ),
                ("DWD preflight payload", "calendar inclusion contract"),
                _DWD_BROWSER_GATES,
            ),
            required_inputs=(
                "workspace_domain",
                "admin_email",
                "dwd_grant",
            ),
            optional_inputs=("scope", "inclusion_spec"),
            provider_console_url=(
                "https://admin.google.com/ac/owl/domainwidedelegation"
            ),
        ),
        idempotent_operation_ids=(
            "directory.users.list",
            "directory.groups.list",
            "directory.group_members.list",
            "directory.org_units.list",
            "directory.users_by_org_unit.list",
            "calendarList.list",
            "events.list",
            "channels.stop",
        ),
        unsafe_operation_ids=(
            "dwd.token.exchange",
            "events.watch",
        ),
        retryable_status_codes=(
            403,
            *_STANDARD_RETRYABLE_HTTP_STATUSES,
        ),
        rate_limit_header_parser_id="http.retry_after",
        provider_transport_enforced=True,
    ),
    _source(
        "google_drive",
        "google",
        "Google Drive",
        display=_display(
            3,
            "Knowledge",
            "Files, metadata, and Drive watches.",
            "Workspace DWD",
            (
                "Google Workspace admin approval, domain-wide delegation "
                "setup, Drive scopes, shared-drive scope, change watch setup, "
                "large-file policy."
            ),
            ("Dry run", "Limited backfill", "Live events"),
        ),
        ui_slug="google-drive",
        default_scopes=(
            "drive.readonly",
            "shared-drives",
            "approved-folders",
        ),
        provider_permissions=(
            "Domain-Wide Delegation",
            "drive.readonly",
            "directory users/groups read",
        ),
        cli_ingress_paths=("/webhooks/google_drive/push",),
        required_refs=(
            "workspace_domain",
            "admin_email",
            "dwd_grant_receipt",
        ),
        aliases=("gdrive", "drive"),
        data_objects=(
            "file",
            "comment",
            "reply",
            "revision",
            "permission",
            "extracted_text",
        ),
        history="api",
        installation_management=InstallationManagementDefinition(
            source="google_drive",
            table="google_drive_installations",
            scope_column="workspace_domain",
            ref_columns=(),
            entity_table="google_drive_targets",
            entity_install_column="google_drive_installation_id",
            base_url_column=None,
            native_google_watch_table=True,
            status_detail_columns=(
                "service_account_email",
                "scope",
                "include_shared_drives",
                "resolved_target_count",
                "resolved_at",
            ),
            status_credential_column_groups=(("service_account_email",),),
        ),
        installation_identifiers=(
            "workspace_customer_id",
            "owner_email",
            "drive_id",
        ),
        runtime_identifiers=("channel_id", "resource_id", "drive_id"),
        ingress_routes=(
            ("backfill", "google_drive:file"),
            ("poll", "google_drive:file"),
        ),
        normalization_inputs=("google_drive:file",),
        idempotency_builder_bindings=(
            "services.ingest.ingestion.idempotency:google_drive_file",
            "services.ingest.ingestion.idempotency:google_drive_comment",
            "services.ingest.ingestion.idempotency:google_drive_revision",
        ),
        normalizer_bindings=(
            "services.ingest.ingestion.handlers.google_drive:"
            "handle_google_drive_file",
        ),
        allowed_observation_kinds=("signal", "state_change"),
        trust_tiers=("authoritative",),
        live_transports=("webhook", "api_poll"),
        onboarding=_onboarding(
            "dwd",
            "shared drives, folders, files, and change tokens",
            _native_connect(
                "google_drive",
                "google_workspace_dwd",
                preflight_payload_fields=(
                    "workspace_domain",
                    "admin_email",
                    "scope",
                ),
                payload_fields=(
                    "workspace_domain",
                    "admin_email",
                    "scope",
                    "inclusion_spec",
                    "include_shared_drives",
                ),
                scope_aliases=("drive.readonly",),
            ),
            browser_agent=_browser_agent(
                (
                    "Domain-wide delegation",
                    "Drive API controls",
                    "Drive inclusion scope",
                ),
                (
                    "workspace domain",
                    "admin email",
                    "shared-drive/user/group/org-unit scope",
                ),
                (
                    "DWD preflight payload",
                    "Drive inclusion contract",
                    "drive watch verifier ref",
                ),
                _DWD_BROWSER_GATES,
            ),
            required_inputs=(
                "workspace_domain",
                "admin_email",
                "dwd_grant",
            ),
            optional_inputs=(
                "scope",
                "inclusion_spec",
                "include_shared_drives",
                "watch_channel_id",
            ),
            provider_console_url=(
                "https://admin.google.com/ac/owl/domainwidedelegation"
            ),
        ),
        idempotent_operation_ids=(
            "directory.users.list",
            "directory.groups.list",
            "directory.group_members.list",
            "directory.org_units.list",
            "directory.users_by_org_unit.list",
            "drives.list",
            "changes.getStartPageToken",
            "files.list",
            "changes.list",
            "files.export",
            "files.get",
            "comments.list",
            "revisions.list",
            "channels.stop",
        ),
        unsafe_operation_ids=(
            "dwd.token.exchange",
            "changes.watch",
        ),
        retryable_status_codes=(
            403,
            *_STANDARD_RETRYABLE_HTTP_STATUSES,
        ),
        rate_limit_header_parser_id="http.retry_after",
        provider_transport_enforced=True,
    ),
    _source(
        "jira",
        "atlassian",
        "Jira",
        display=_display(
            5,
            "Engineering",
            "Issues, projects, and work tracking signals.",
            "API token",
            (
                "Jira site URL, project scope, API token or OAuth app, "
                "webhook callback approval, issue/comment permissions."
            ),
            ("Dry run", "Limited backfill", "Live events"),
        ),
        default_scopes=("pilot-projects", "issues", "comments"),
        provider_permissions=(
            "read:jira-work",
            "read:jira-user",
            "webhook registration",
        ),
        cli_ingress_paths=("/webhooks/jira/events",),
        required_refs=("api_token", "site_url"),
        local_rehearsal=_local_rehearsal(
            "api_token_connect",
            needs_public_url=True,
            preflight_endpoint="/integrations/jira/connect/preflight",
            finalize_endpoint="/integrations/jira/connect/finalize",
            webhook_path="/webhooks/jira/events",
            env=(
                "JIRA_BASE_URL",
                "JIRA_ACCOUNT_EMAIL",
                "JIRA_API_TOKEN_REF",
                "JIRA_PROJECT_KEYS",
                "JIRA_WEBHOOK_SECRET_REF",
            ),
            required_env=(
                "JIRA_BASE_URL",
                "JIRA_ACCOUNT_EMAIL",
                "JIRA_API_TOKEN_REF",
            ),
            manual_gate_names=(
                "jira_api_token_creation",
                "jira_project_scope_admin_approval",
                "jira_webhook_admin_approval",
            ),
        ),
        aliases=("jira_cloud",),
        data_objects=("issue", "comment", "status_transition"),
        history="api",
        installation_management=InstallationManagementDefinition(
            source="jira",
            table="jira_installations",
            scope_column="base_url",
            ref_columns=("secret_ref", "webhook_secret_ref"),
            entity_table="jira_projects",
            entity_install_column="jira_installation_id",
            webhook_installation_id_column="base_url",
            webhook_installation_id_transform="host",
            status_detail_columns=("account_email", "cloud_id"),
            status_presence_columns=(("webhook_registered", "webhook_secret_ref"),),
        ),
        installation_identifiers=("site_id",),
        runtime_identifiers=("installation_id", "project_id"),
        ingress_routes=(
            ("backfill", "jira:issue"),
            ("poll", "jira:issue"),
            ("webhook", "jira:issue"),
        ),
        normalization_inputs=("jira:issue",),
        idempotency_builder_bindings=(
            "services.ingest.ingestion.idempotency:jira_issue",
            "services.ingest.ingestion.idempotency:jira_transition",
            "services.ingest.ingestion.idempotency:jira_comment",
        ),
        normalizer_bindings=(
            "services.ingest.ingestion.handlers.jira:handle_jira_issue",
        ),
        allowed_observation_kinds=("signal", "state_change"),
        trust_tiers=("authoritative",),
        live_transports=("webhook", "api_poll"),
        onboarding=_onboarding(
            "api_token",
            "projects, issue types, comments, and webhook registration",
            _native_connect(
                "jira",
                "jira_api_token_native_connect",
                preflight_payload_fields=(
                    "base_url",
                    "account_email",
                    "api_token",
                ),
                payload_fields=(
                    "base_url",
                    "account_email",
                    "api_token",
                    "project_keys",
                    "webhook_secret",
                ),
            ),
            browser_agent=_browser_agent(
                (
                    "API token page",
                    "Jira project settings",
                    "webhook settings",
                ),
                ("site URL", "project keys", "issue/comment scope"),
                ("webhook secret ref", "Jira project scope contract"),
                _TOKEN_BROWSER_GATES,
            ),
            required_inputs=("base_url", "account_email", "api_token"),
            optional_inputs=("project_keys", "webhook_secret"),
            provider_console_url="https://admin.atlassian.com/",
        ),
        idempotent_operation_ids=(
            "issues.search",
            "projects.list",
            "issues.approximate_count",
            "users.myself.get",
        ),
        rate_limit_header_parser_id="http.retry_after",
        provider_transport_enforced=True,
    ),
    _source(
        "mercury",
        "mercury",
        "Mercury",
        display=_display(
            17,
            "Finance",
            "Banking, cash accounts, and transactions.",
            "API token",
            (
                "Mercury organization ID, account IDs, API token, webhook "
                "secret if live events are enabled, account scope."
            ),
            ("Dry run", "Limited backfill", "Live events"),
        ),
        default_scopes=("organization", "accounts", "transactions"),
        provider_permissions=("accounts read", "transactions read"),
        cli_ingress_paths=("/webhooks/mercury/events",),
        required_refs=("api_token", "webhook_secret"),
        data_objects=("account", "balance_snapshot", "transaction"),
        history="api",
        installation_management=InstallationManagementDefinition(
            source="mercury",
            table="mercury_installations",
            scope_column="organization_id",
            ref_columns=("secret_ref", "webhook_secret_ref"),
            entity_table="mercury_accounts",
            entity_install_column="mercury_installation_id",
            webhook_installation_id_column="organization_id",
            entity_status_columns=("account_id", "account_name", "state"),
        ),
        installation_identifiers=("organization_id",),
        runtime_identifiers=("installation_id", "account_id"),
        ingress_routes=(
            ("backfill", "mercury:transaction"),
            ("poll", "mercury:transaction"),
            ("webhook", "mercury:transaction"),
        ),
        normalization_inputs=("mercury:transaction",),
        idempotency_builder_bindings=(
            "services.ingest.ingestion.idempotency:mercury_transaction",
            "services.ingest.ingestion.idempotency:mercury_balance",
        ),
        normalizer_bindings=(
            "services.ingest.ingestion.handlers.mercury:" "handle_mercury_transaction",
        ),
        allowed_observation_kinds=("signal", "state_change"),
        trust_tiers=("authoritative",),
        live_transports=("webhook", "api_poll"),
        onboarding=_onboarding(
            "api_token",
            "organization, accounts, and transaction scopes",
            _native_connect(
                "mercury",
                "api_token_native_connect",
                preflight_payload_fields=("api_token", "base_url"),
                payload_fields=(
                    "api_token",
                    "base_url",
                    "account_ids",
                    "organization_id",
                    "webhook_secret",
                ),
            ),
            browser_agent=_browser_agent(
                (
                    "API token settings",
                    "account settings",
                    "webhook settings",
                ),
                ("organization id", "account ids", "transaction scope"),
                ("webhook secret ref", "bank account scope contract"),
                _TOKEN_BROWSER_GATES,
            ),
            required_inputs=("api_token",),
            optional_inputs=(
                "organization_id",
                "base_url",
                "account_ids",
                "webhook_secret",
            ),
            provider_console_url=("https://app.mercury.com/settings/tokens"),
        ),
        capability_flags=("finance_testing",),
        idempotent_operation_ids=(
            "accounts.list",
            "accounts.get",
            "transactions.list",
        ),
        rate_limit_header_parser_id="http.retry_after",
        provider_transport_enforced=True,
    ),
    _source(
        "quickbooks",
        "intuit",
        "QuickBooks Online",
        display=_display(
            18,
            "Finance",
            "Accounting, company, and ledger signals.",
            "OAuth",
            (
                "QuickBooks company realm ID, OAuth token path, sandbox or "
                "production base URL, webhook verifier."
            ),
            ("Dry run", "Limited backfill", "Live events"),
            display_name_override="QuickBooks",
        ),
        default_scopes=("company", "accounting", "webhooks"),
        provider_permissions=(
            "accounting read",
            "company info",
            "webhooks",
        ),
        cli_ingress_paths=("/webhooks/quickbooks/events",),
        required_refs=("oauth_client", "token_ref", "realm_id"),
        aliases=("qbo", "quickbooks_online"),
        data_objects=("invoice", "bill", "bill_payment", "payment"),
        history="api",
        installation_management=InstallationManagementDefinition(
            source="quickbooks",
            table="quickbooks_installations",
            scope_column="realm_id",
            ref_columns=(
                "secret_ref",
                "refresh_secret_ref",
                "webhook_secret_ref",
            ),
            entity_table="quickbooks_entities",
            entity_install_column="quickbooks_installation_id",
            webhook_installation_id_column="realm_id",
            entity_status_columns=("entity_type", "state"),
        ),
        installation_identifiers=("realm_id",),
        runtime_identifiers=("realm_id", "entity_id"),
        ingress_routes=(
            ("backfill", "quickbooks:object"),
            ("poll", "quickbooks:object"),
            ("webhook", "quickbooks:object"),
        ),
        normalization_inputs=("quickbooks:object",),
        idempotency_builder_bindings=(
            "services.ingest.ingestion.idempotency:quickbooks_entity",
            "services.ingest.ingestion.idempotency:quickbooks_change",
        ),
        normalizer_bindings=(
            "services.ingest.ingestion.handlers.quickbooks:" "handle_quickbooks_object",
        ),
        allowed_observation_kinds=("signal", "state_change"),
        trust_tiers=("authoritative",),
        live_transports=("webhook", "api_poll"),
        onboarding=_onboarding(
            "oauth",
            "company realm, accounting entities, and webhook verifier",
            _native_connect(
                "quickbooks",
                "access_token_native_connect",
                preflight_payload_fields=(
                    "realm_id",
                    "access_token",
                    "base_url",
                ),
                payload_fields=(
                    "realm_id",
                    "access_token",
                    "base_url",
                    "refresh_token",
                    "webhook_verifier_token",
                    "entities",
                ),
            ),
            browser_agent=_browser_agent(
                (
                    "developer app settings",
                    "company realm",
                    "webhook settings",
                ),
                (
                    "realm id",
                    "accounting entity scope",
                    "webhook verifier status",
                ),
                ("webhook verifier ref", "realm scope contract"),
                _OAUTH_BROWSER_GATES,
            ),
            required_inputs=("realm_id", "token_ref"),
            optional_inputs=(
                "oauth_client",
                "base_url",
                "refresh_token_ref",
                "webhook_verifier_token",
            ),
            provider_console_url=("https://developer.intuit.com/app/developer/myapps"),
        ),
        capability_flags=("finance_testing",),
        credential_refresh=_credential_refresh(
            "quickbooks",
            "https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer",
            "refresh_token",
            "basic",
            rotates_refresh_token=True,
            install_table="quickbooks_installations",
            operation_id="oauth.token.refresh",
        ),
        idempotent_operation_ids=(
            "entities.query",
            "company_info.get",
        ),
        unsafe_operation_ids=("oauth.token.refresh",),
        rate_limit_header_parser_id="http.retry_after",
        provider_transport_enforced=True,
    ),
    _source(
        "grafana",
        "grafana",
        "Grafana",
        display=_display(
            15,
            "Operations",
            "Dashboards, alerts, and operations signals.",
            "API token",
            (
                "Grafana instance URL, service account token, "
                "dashboard/folder scope, alert scope, network reachability "
                "from BYOC."
            ),
            ("Dry run", "Limited backfill"),
        ),
        default_scopes=("dashboards", "alerts", "folders"),
        provider_permissions=(
            "dashboards read",
            "alerts read",
            "folders read",
        ),
        cli_ingress_paths=("/webhooks/grafana/events",),
        required_refs=(
            "service_account_token",
            "base_url",
            "webhook_secret",
        ),
        data_objects=("annotation", "alert_group"),
        history="api",
        installation_management=InstallationManagementDefinition(
            source="grafana",
            table="grafana_installations",
            scope_column="base_url",
            ref_columns=("secret_ref", "webhook_secret_ref"),
            entity_table=None,
            entity_install_column=None,
            webhook_installation_id_column="base_url",
            webhook_installation_id_transform="host",
        ),
        installation_identifiers=("base_url", "org_id"),
        runtime_identifiers=("installation_id", "org_id"),
        ingress_routes=(
            ("backfill", "grafana:annotation"),
            ("poll", "grafana:annotation"),
            ("webhook", "grafana:alert"),
        ),
        normalization_inputs=("grafana:annotation", "grafana:alert"),
        idempotency_builder_bindings=(
            "services.ingest.ingestion.idempotency:grafana_annotation",
            "services.ingest.ingestion.idempotency:grafana_alert",
        ),
        normalizer_bindings=(
            "services.ingest.ingestion.handlers.grafana:" "handle_grafana_annotation",
            "services.ingest.ingestion.handlers.grafana:handle_grafana_alert",
        ),
        allowed_observation_kinds=("signal", "state_change"),
        trust_tiers=("authoritative",),
        live_transports=("webhook", "api_poll"),
        onboarding=_onboarding(
            "api_token",
            "folders, dashboards, alert rules, and org metadata",
            _native_connect(
                "grafana",
                "api_token_native_connect",
                preflight_payload_fields=(
                    "base_url",
                    "service_account_token",
                    "org_id",
                ),
                payload_fields=(
                    "base_url",
                    "service_account_token",
                    "org_id",
                    "webhook_secret",
                ),
            ),
            browser_agent=_browser_agent(
                (
                    "service account settings",
                    "folder settings",
                    "alerting settings",
                ),
                (
                    "instance URL",
                    "folder ids",
                    "dashboard ids",
                    "alert scope",
                ),
                (
                    "service account token ref",
                    "webhook secret ref",
                    "dashboard/alert scope contract",
                ),
                _TOKEN_BROWSER_GATES,
            ),
            required_inputs=("base_url", "service_account_token"),
            optional_inputs=("webhook_secret",),
            provider_console_url="https://grafana.com/auth/sign-in/",
        ),
        idempotent_operation_ids=(
            "annotations.list",
            "org.get",
        ),
        rate_limit_header_parser_id="http.retry_after",
        provider_transport_enforced=True,
    ),
    _source(
        "telegram",
        "telegram",
        "Telegram",
        display=_display(
            9,
            "Communication",
            "MTProto user-account backfill and live updates.",
            "Gateway",
            (
                "Telegram API ID/hash, authorized user session, chats "
                "allowlist, backfill approval, gateway worker readiness."
            ),
            ("Dry run", "Limited backfill", "Live events"),
        ),
        default_scopes=("approved-chats",),
        provider_permissions=("api id", "api hash", "approved chats"),
        cli_ingress_paths=(),
        required_refs=("api_id", "api_hash", "live_session"),
        no_ingress_reason=(
            "Local MTProto gateway session runs from the customer cloud."
        ),
        local_rehearsal=_local_rehearsal(
            "local_gateway_session",
            needs_public_url=False,
            env=(
                "TELEGRAM_ACCOUNT_LABEL",
                "TELEGRAM_API_ID",
                "TELEGRAM_API_HASH",
                "TELEGRAM_SESSION",
                "TELEGRAM_BACKFILL_SESSION",
                "TELEGRAM_DIALOGS_JSON",
            ),
            required_env=(
                "TELEGRAM_ACCOUNT_LABEL",
                "TELEGRAM_API_ID",
                "TELEGRAM_API_HASH",
                "TELEGRAM_SESSION",
            ),
            manual_gate_names=(
                "telegram_api_id_creation",
                "telegram_mtproto_login_code",
                "telegram_dialog_scope_selection",
            ),
        ),
        data_objects=("dialog_message", "message_edit"),
        history="session",
        installation_management=InstallationManagementDefinition(
            source="telegram",
            table="telegram_installations",
            scope_column="account_label",
            ref_columns=(
                "api_hash_secret_ref",
                "session_secret_ref",
                "backfill_session_secret_ref",
            ),
            entity_table="telegram_dialogs",
            entity_install_column="telegram_installation_id",
            base_url_column=None,
            status_detail_columns=("api_id",),
            status_presence_columns=(
                (
                    "backfill_session_configured",
                    "backfill_session_secret_ref",
                ),
            ),
            status_credential_column_groups=(
                ("api_hash_secret_ref", "session_secret_ref"),
            ),
        ),
        installation_identifiers=("account_id", "session_id"),
        runtime_identifiers=("installation_id", "dialog_id"),
        ingress_routes=(
            ("gateway", "telegram:message"),
            ("backfill", "telegram:message"),
        ),
        normalization_inputs=("telegram:message",),
        idempotency_builder_bindings=(
            "services.ingest.ingestion.idempotency:telegram_message",
        ),
        normalizer_bindings=(
            "services.ingest.ingestion.handlers.telegram:handle_telegram",
        ),
        allowed_observation_kinds=("signal",),
        trust_tiers=("attested_agent",),
        live_transports=("mtproto",),
        onboarding=_onboarding(
            "gateway",
            "account, dialogs, channels, groups, and live update cursor",
            _native_connect(
                "telegram",
                "local_session_native_connect",
                payload_fields=(
                    "account_label",
                    "api_id",
                    "api_hash",
                    "live_session",
                    "backfill_session",
                    "dialogs",
                ),
            ),
            browser_agent=_browser_agent(
                (
                    "Telegram API app",
                    "local MTProto session",
                    "dialog discovery",
                ),
                (
                    "api id",
                    "account label",
                    "dialogs",
                    "channel/group scope",
                ),
                (
                    "api hash ref",
                    "MTProto session refs",
                    "Telegram gateway runner contract",
                ),
                _LOCAL_SESSION_BROWSER_GATES,
            ),
            required_inputs=(
                "account_label",
                "api_id",
                "api_hash",
                "live_session",
            ),
            optional_inputs=("backfill_session", "dialogs"),
            provider_console_url="https://my.telegram.org/apps",
            generic_authorization_mode="customer_mtproto_session",
        ),
        idempotent_operation_ids=(
            "session.connect",
            "session.is_user_authorized",
            "get_history",
            "iter_dialogs",
            "has_history_since",
            "me",
            "gateway.connect",
            "gateway.is_user_authorized",
            "updates.catch_up",
            "updates.get_state",
        ),
        rate_limit_header_parser_id="telegram.flood_wait",
        provider_transport_enforced=True,
        operator_live_ingress="customer-cloud MTProto gateway worker",
    ),
    _source(
        "brex",
        "brex",
        "Brex",
        display=_display(
            19,
            "Finance",
            "Corporate cards, cash, and transactions.",
            "API token",
            (
                "Brex organization ID, account IDs, API token, webhook secret "
                "if live events are enabled, transaction scope."
            ),
            ("Dry run", "Limited backfill", "Live events"),
        ),
        default_scopes=("accounts", "transactions", "cards"),
        provider_permissions=(
            "accounts read",
            "transactions read",
            "cards read",
        ),
        cli_ingress_paths=("/webhooks/brex",),
        required_refs=("api_token", "webhook_secret"),
        data_objects=("account", "balance_snapshot", "transaction"),
        history="api",
        installation_management=InstallationManagementDefinition(
            source="brex",
            table="brex_installations",
            scope_column="organization_id",
            ref_columns=("secret_ref", "webhook_secret_ref"),
            entity_table="brex_accounts",
            entity_install_column="brex_installation_id",
            webhook_installation_id_column="organization_id",
            entity_status_columns=("account_id", "account_name", "state"),
        ),
        installation_identifiers=("organization_id",),
        runtime_identifiers=("installation_id", "account_id"),
        ingress_routes=(
            ("backfill", "brex:transaction"),
            ("poll", "brex:transaction"),
            ("webhook", "brex:transaction"),
        ),
        normalization_inputs=("brex:transaction",),
        idempotency_builder_bindings=(
            "services.ingest.ingestion.idempotency:brex_transaction",
            "services.ingest.ingestion.idempotency:brex_balance",
        ),
        normalizer_bindings=(
            "services.ingest.ingestion.handlers.brex:handle_brex_transaction",
        ),
        allowed_observation_kinds=("signal", "state_change"),
        trust_tiers=("authoritative",),
        live_transports=("webhook", "api_poll"),
        onboarding=_onboarding(
            "api_token",
            "cash accounts, cards, and transaction scopes",
            _native_connect(
                "brex",
                "api_token_native_connect",
                preflight_payload_fields=("api_token", "base_url"),
                payload_fields=(
                    "api_token",
                    "base_url",
                    "account_ids",
                    "organization_id",
                    "webhook_secret",
                ),
            ),
            browser_agent=_browser_agent(
                (
                    "developer API settings",
                    "webhook settings",
                    "organization settings",
                ),
                (
                    "organization id",
                    "account ids",
                    "supported card and transaction scopes",
                ),
                ("webhook verifier ref", "finance entity scope contract"),
                _TOKEN_BROWSER_GATES,
            ),
            required_inputs=("api_token",),
            optional_inputs=(
                "organization_id",
                "base_url",
                "account_ids",
                "webhook_secret",
            ),
            provider_console_url="https://developer.brex.com/",
        ),
        capability_flags=("finance_testing",),
        idempotent_operation_ids=(
            "accounts.cash.list",
            "accounts.card.list",
            "transactions.cash.list",
            "transactions.card.list",
        ),
        rate_limit_header_parser_id="http.retry_after",
        provider_transport_enforced=True,
    ),
    _source(
        "ramp",
        "ramp",
        "Ramp",
        display=_display(
            20,
            "Finance",
            "Spend management and finance events.",
            "OAuth",
            (
                "Ramp business scope, access token or OAuth client "
                "credentials, entity allowlist, webhook verifier, and poll "
                "schedule approval."
            ),
            ("Dry run", "Limited backfill", "Live events"),
        ),
        default_scopes=(
            "transactions",
            "reimbursements",
            "cards",
            "users",
            "business",
        ),
        provider_permissions=(
            "transactions read",
            "reimbursements read",
            "cards read",
            "users read",
            "business read",
        ),
        cli_ingress_paths=("/webhooks/ramp",),
        required_refs=("access_token_or_client_credentials",),
        data_objects=("transaction", "reimbursement", "card", "user"),
        history="api",
        installation_management=InstallationManagementDefinition(
            source="ramp",
            table="ramp_installations",
            scope_column="business_id",
            ref_columns=(
                "secret_ref",
                "refresh_secret_ref",
                "webhook_secret_ref",
            ),
            entity_table="ramp_entities",
            entity_install_column="ramp_installation_id",
            webhook_installation_id_column="business_id",
            status_detail_columns=("token_expires_at",),
            status_presence_columns=(("webhook_registered", "webhook_secret_ref"),),
            status_credential_column_groups=(
                ("secret_ref",),
                ("refresh_secret_ref",),
            ),
            entity_status_columns=("entity_type", "state"),
        ),
        installation_identifiers=("business_id",),
        runtime_identifiers=("business_id",),
        ingress_routes=(
            ("backfill", "ramp:transaction"),
            ("poll", "ramp:transaction"),
            ("webhook", "ramp:transaction"),
        ),
        normalization_inputs=("ramp:transaction",),
        idempotency_builder_bindings=(
            "services.ingest.ingestion.idempotency:ramp_transaction",
            "services.ingest.ingestion.idempotency:ramp_entity",
            "services.ingest.ingestion.idempotency:ramp_change",
        ),
        normalizer_bindings=(
            "services.ingest.ingestion.handlers.ramp:handle_ramp_transaction",
        ),
        allowed_observation_kinds=("signal", "state_change"),
        trust_tiers=("authoritative",),
        live_transports=("webhook", "api_poll"),
        onboarding=_onboarding(
            "oauth_client_credentials",
            ("business scope, transactions, reimbursements, cards, and " "users"),
            _native_connect(
                "ramp",
                "ramp_native_connect",
                preflight_payload_fields=(
                    "access_token",
                    "client_id",
                    "client_secret",
                    "scopes",
                    "base_url",
                ),
                payload_fields=(
                    "access_token",
                    "client_id",
                    "client_secret",
                    "base_url",
                    "business_id",
                    "entities",
                    "webhook_verifier_token",
                ),
            ),
            browser_agent=_browser_agent(
                (
                    "OAuth app settings",
                    "business settings",
                    "webhook settings",
                ),
                (
                    "business id",
                    "transaction/reimbursement/card/user streams",
                    "API base URL",
                    "webhook target",
                ),
                (
                    "webhook verifier token ref",
                    "Ramp spend scope contract",
                ),
                _TOKEN_BROWSER_GATES,
            ),
            required_inputs=("access_token_or_client_credentials",),
            optional_inputs=(
                "business_id",
                "base_url",
                "entity_scope",
                "webhook_verifier_token",
            ),
            provider_console_url="https://developers.ramp.com/",
        ),
        capability_flags=("finance_testing",),
        credential_refresh=_credential_refresh(
            "ramp",
            "https://api.ramp.com/developer/v1/token",
            "client_credentials",
            "basic",
            rotates_refresh_token=False,
            install_table="ramp_installations",
            operation_id="oauth.token.mint",
            client_credentials_from_install=True,
            scope_env="RAMP_OAUTH_SCOPES",
            default_scope=(
                "transactions:read reimbursements:read cards:read users:read "
                "business:read"
            ),
        ),
        idempotent_operation_ids=(
            "transactions.list",
            "reimbursements.list",
            "cards.list",
            "users.list",
            "business.get",
        ),
        unsafe_operation_ids=("oauth.token.mint",),
        rate_limit_header_parser_id="http.retry_after",
        provider_transport_enforced=True,
    ),
    _source(
        "gusto",
        "gusto",
        "Gusto",
        display=_display(
            22,
            "People",
            "Payroll and company HR finance records.",
            "OAuth",
            (
                "Gusto company UUID, OAuth app approval, access/refresh token "
                "path, payroll and employee scopes."
            ),
            ("Dry run", "Limited backfill", "Live events"),
        ),
        default_scopes=("company", "employees", "payroll"),
        provider_permissions=(
            "company read",
            "employee read",
            "payroll read",
        ),
        cli_ingress_paths=("/webhooks/gusto",),
        required_refs=("oauth_client", "token_ref"),
        data_objects=("employee", "payroll"),
        history="api",
        installation_management=InstallationManagementDefinition(
            source="gusto",
            table="gusto_installations",
            scope_column="company_uuid",
            ref_columns=(
                "secret_ref",
                "refresh_secret_ref",
                "webhook_secret_ref",
            ),
            entity_table="gusto_entities",
            entity_install_column="gusto_installation_id",
            webhook_installation_id_column="company_uuid",
            entity_status_columns=("entity_type", "state"),
        ),
        installation_identifiers=("company_uuid",),
        runtime_identifiers=("company_uuid", "resource_uuid"),
        ingress_routes=(
            ("backfill", "gusto:object"),
            ("poll", "gusto:object"),
            ("webhook", "gusto:object"),
        ),
        normalization_inputs=("gusto:object",),
        idempotency_builder_bindings=(
            "services.ingest.ingestion.idempotency:gusto_entity",
            "services.ingest.ingestion.idempotency:gusto_change",
        ),
        normalizer_bindings=(
            "services.ingest.ingestion.handlers.gusto:handle_gusto_object",
        ),
        allowed_observation_kinds=("signal", "state_change"),
        trust_tiers=("authoritative",),
        live_transports=("webhook", "api_poll"),
        onboarding=_onboarding(
            "oauth",
            "company, employees, and payroll scopes",
            _native_connect(
                "gusto",
                "access_token_native_connect",
                payload_fields=(
                    "company_uuid",
                    "access_token",
                    "base_url",
                    "refresh_token",
                    "webhook_verifier_token",
                    "entities",
                ),
            ),
            browser_agent=_browser_agent(
                (
                    "developer app settings",
                    "company settings",
                    "webhook settings",
                ),
                (
                    "company uuid",
                    "employee/payroll scope",
                    "webhook availability",
                ),
                ("webhook verifier token ref", "company scope contract"),
                _OAUTH_BROWSER_GATES,
            ),
            required_inputs=("token_ref",),
            optional_inputs=(
                "company_uuid",
                "oauth_client",
                "base_url",
                "refresh_token_ref",
                "webhook_verifier_token",
            ),
            provider_console_url="https://dev.gusto.com/",
        ),
        capability_flags=("finance_testing",),
        credential_refresh=_credential_refresh(
            "gusto",
            "https://api.gusto.com/oauth/token",
            "refresh_token",
            "body",
            rotates_refresh_token=True,
            install_table="gusto_installations",
            operation_id="oauth.token.refresh",
            default_expires_in=7200,
        ),
        idempotent_operation_ids=(
            "employees.list",
            "payrolls.list",
            "companies.get",
        ),
        unsafe_operation_ids=("oauth.token.refresh",),
        rate_limit_header_parser_id="http.retry_after",
        provider_transport_enforced=True,
    ),
    _source(
        "deel",
        "deel",
        "Deel",
        display=_display(
            25,
            "People",
            "Contractor, payroll, and workforce records.",
            "API token",
            (
                "Deel organization access, API token, worker/contract scope, "
                "payroll or contractor data approval."
            ),
            ("Dry run", "Limited backfill", "Live events"),
        ),
        default_scopes=("workers", "contracts", "payments"),
        provider_permissions=(
            "workers read",
            "contracts read",
            "payments read",
        ),
        cli_ingress_paths=("/webhooks/deel",),
        required_refs=("api_token",),
        data_objects=("contract", "organization_invoice"),
        history="api",
        installation_management=InstallationManagementDefinition(
            source="deel",
            table="deel_installations",
            scope_column="organization_id",
            ref_columns=("secret_ref", "webhook_secret_ref"),
            entity_table="deel_contracts",
            entity_install_column="deel_installation_id",
            webhook_installation_id_column="organization_id",
            entity_status_columns=("contract_id", "contract_name", "state"),
        ),
        installation_identifiers=("organization_id",),
        runtime_identifiers=("organization_id", "contract_id"),
        ingress_routes=(
            ("backfill", "deel:payment"),
            ("poll", "deel:payment"),
            ("webhook", "deel:payment"),
        ),
        normalization_inputs=("deel:payment",),
        idempotency_builder_bindings=(
            "services.ingest.ingestion.idempotency:deel_payment",
            "services.ingest.ingestion.idempotency:deel_contract",
        ),
        normalizer_bindings=(
            "services.ingest.ingestion.handlers.deel:handle_deel_payment",
        ),
        allowed_observation_kinds=("signal", "state_change"),
        trust_tiers=("authoritative",),
        live_transports=("webhook", "api_poll"),
        onboarding=_onboarding(
            "api_token",
            "contracts, workers, and payment scopes",
            _native_connect(
                "deel",
                "api_token_native_connect",
                preflight_payload_fields=("api_token", "base_url"),
                payload_fields=(
                    "api_token",
                    "base_url",
                    "contract_ids",
                    "organization_id",
                    "webhook_secret",
                ),
            ),
            browser_agent=_browser_agent(
                (
                    "developer API settings",
                    "webhook settings",
                    "organization settings",
                ),
                (
                    "organization id",
                    "contract ids",
                    "worker/payment scope availability",
                ),
                ("webhook signing secret ref", "workforce scope contract"),
                _TOKEN_BROWSER_GATES,
            ),
            required_inputs=("api_token",),
            optional_inputs=(
                "organization_id",
                "base_url",
                "contract_ids",
                "webhook_secret",
            ),
            provider_console_url="https://app.deel.com/",
        ),
        capability_flags=("finance_testing",),
        idempotent_operation_ids=(
            "contracts.list",
            "contracts.get",
            "invoices.list",
        ),
        rate_limit_header_parser_id="http.retry_after",
        provider_transport_enforced=True,
    ),
    _source(
        "fireflies",
        "fireflies",
        "Fireflies.ai",
        display=_display(
            12,
            "Meetings",
            "Meeting transcripts and conversation records.",
            "API token",
            (
                "Workspace approval, API token, transcript scope, meeting "
                "history window, webhook or poll setup."
            ),
            ("Dry run", "Limited backfill", "Live events"),
            display_name_override="Fireflies",
        ),
        default_scopes=("transcripts", "meetings"),
        provider_permissions=("transcripts read", "meetings read"),
        cli_ingress_paths=("/webhooks/fireflies",),
        required_refs=("api_token", "webhook_secret"),
        aliases=("fireflies_ai",),
        data_objects=("meeting", "summary", "action_item"),
        history="api",
        installation_management=InstallationManagementDefinition(
            source="fireflies",
            table="fireflies_installations",
            scope_column="workspace_id",
            ref_columns=("secret_ref", "webhook_secret_ref"),
            entity_table=None,
            entity_install_column=None,
            webhook_installation_id_column="workspace_id",
        ),
        installation_identifiers=("workspace_id",),
        runtime_identifiers=("workspace_id", "transcript_id"),
        ingress_routes=(
            ("backfill", "fireflies:transcript"),
            ("poll", "fireflies:transcript"),
            ("webhook", "fireflies:transcript"),
        ),
        normalization_inputs=("fireflies:transcript",),
        idempotency_builder_bindings=(
            "services.ingest.ingestion.idempotency:fireflies_transcript",
        ),
        normalizer_bindings=(
            "services.ingest.ingestion.handlers.fireflies:"
            "handle_fireflies_transcript",
        ),
        allowed_observation_kinds=("signal",),
        trust_tiers=("attested_agent",),
        live_transports=("webhook", "api_poll"),
        onboarding=_onboarding(
            "api_token",
            "workspace, meetings, and transcripts",
            _native_connect(
                "fireflies",
                "api_token_native_connect",
                preflight_payload_fields=("api_token", "base_url"),
                payload_fields=(
                    "api_token",
                    "base_url",
                    "workspace_id",
                    "webhook_secret",
                ),
            ),
            browser_agent=_browser_agent(
                (
                    "integration settings",
                    "workspace settings",
                    "webhook settings",
                ),
                (
                    "workspace id",
                    "transcript scope",
                    "meeting history availability",
                ),
                (
                    "webhook signing secret ref",
                    "meeting transcript scope contract",
                ),
                _TOKEN_BROWSER_GATES,
            ),
            required_inputs=("api_token",),
            optional_inputs=("workspace_id", "base_url", "webhook_secret"),
            provider_console_url=("https://app.fireflies.ai/integrations"),
        ),
        idempotent_operation_ids=(
            "user.get",
            "transcript.get",
            "transcripts.list",
        ),
        rate_limit_header_parser_id="http.retry_after",
        provider_transport_enforced=True,
    ),
    _source(
        "signal",
        "signal",
        "Signal",
        display=_display(
            10,
            "Communication",
            "Linked-device message ingestion.",
            "Gateway",
            (
                "Linked-device session, account approval, contact or group "
                "scope, gateway worker readiness."
            ),
            ("Dry run", "Live events"),
        ),
        default_scopes=("approved-contacts", "approved-groups"),
        provider_permissions=(
            "linked device session",
            "approved contacts",
            "approved groups",
        ),
        cli_ingress_paths=(),
        required_refs=("linked_device_session",),
        no_ingress_reason=(
            "Linked-device gateway session runs from the customer cloud."
        ),
        aliases=("signal_messenger",),
        data_objects=("thread_message", "message_edit"),
        history="session",
        installation_management=InstallationManagementDefinition(
            source="signal",
            table="signal_installations",
            scope_column="account_label",
            ref_columns=(
                "session_secret_ref",
                "backfill_session_secret_ref",
            ),
            entity_table="signal_threads",
            entity_install_column="signal_installation_id",
            base_url_column=None,
        ),
        installation_identifiers=("account_id", "device_id"),
        runtime_identifiers=("installation_id", "thread_id"),
        ingress_routes=(
            ("gateway", "signal:message"),
            ("backfill", "signal:message"),
        ),
        normalization_inputs=("signal:message",),
        idempotency_builder_bindings=(
            "services.ingest.ingestion.idempotency:signal_message",
        ),
        normalizer_bindings=(
            "services.ingest.ingestion.handlers.signal:handle_signal",
        ),
        allowed_observation_kinds=("signal",),
        trust_tiers=("attested_agent",),
        live_transports=("json_rpc",),
        onboarding=_onboarding(
            "gateway",
            "linked account, approved contacts, groups, and threads",
            _native_connect(
                "signal",
                "local_session_native_connect",
                payload_fields=(
                    "account_label",
                    "linked_device_session",
                    "backfill_session",
                    "threads",
                ),
            ),
            browser_agent=_browser_agent(
                (
                    "local device-link session",
                    "contact/group scope",
                    "gateway runner settings",
                ),
                (
                    "account label",
                    "approved contacts",
                    "approved groups",
                    "thread scope",
                ),
                (
                    "linked-device session ref",
                    "Signal gateway runner contract",
                ),
                _LOCAL_SESSION_BROWSER_GATES,
            ),
            required_inputs=("linked_device_session",),
            optional_inputs=(
                "account_label",
                "backfill_session",
                "thread_scope",
            ),
            provider_console_url="https://signal.org/download/",
            generic_authorization_mode=("customer_linked_device_session"),
        ),
        idempotent_operation_ids=(
            "list_groups",
            "receive_poll",
            "subscribe_receive",
            "unsubscribe_receive",
            "events_stream",
        ),
        rate_limit_header_parser_id="signal.retry_after",
        provider_transport_enforced=True,
        operator_live_ingress=(
            "customer-cloud signal-cli HTTP JSON-RPC/SSE gateway worker"
        ),
        certification_notes=(
            "Uses the unofficial pinned signal-cli 0.14.4.1 HTTP JSON-RPC/SSE "
            "boundary; a linked-device real-provider canary remains mandatory.",
        ),
    ),
    _source(
        "aws",
        "aws",
        "AWS",
        display=_display(
            16,
            "Cloud",
            "Cloud inventory and operational events.",
            "IAM role",
            (
                "AWS account and region, customer IAM role or access ref, "
                "inventory scope, CloudTrail/EventBridge scope if enabled."
            ),
            ("Dry run", "Limited backfill", "Live events"),
            notice=(
                "Fyralis polls customer-authorized AWS APIs from the local "
                "data plane."
            ),
        ),
        default_scopes=("inventory", "cloudtrail", "eventbridge"),
        provider_permissions=(
            "inventory read",
            "cloudtrail read",
            "eventbridge read",
        ),
        cli_ingress_paths=(),
        required_refs=("role_arn",),
        no_ingress_reason=("Fyralis polls customer-authorized AWS APIs locally."),
        aliases=("amazon_web_services", "cloudtrail"),
        data_objects=("cloudtrail_event", "cloudwatch_alarm_state"),
        history="api",
        installation_management=InstallationManagementDefinition(
            source="aws",
            table="aws_installations",
            scope_column="account_id",
            ref_columns=("secret_ref",),
            entity_table=None,
            entity_install_column=None,
            base_url_column=None,
            extra_output_columns=("region", "credential_kind"),
            status_detail_columns=("backfill_window_days",),
        ),
        installation_identifiers=("account_id", "role_arn"),
        runtime_identifiers=("account_id", "region"),
        ingress_routes=(
            ("backfill", "aws:event"),
            ("poll", "aws:event"),
        ),
        normalization_inputs=("aws:event",),
        idempotency_builder_bindings=(
            "services.ingest.ingestion.idempotency:aws_event",
        ),
        normalizer_bindings=(
            "services.ingest.ingestion.handlers.aws:handle_aws_event",
        ),
        allowed_observation_kinds=("signal", "state_change"),
        trust_tiers=("authoritative",),
        live_transports=("queue_poll", "api_poll"),
        onboarding=_onboarding(
            "iam_role",
            "account, region, CloudTrail, and inventory scope",
            _native_connect(
                "aws",
                "aws_iam_native_connect",
                payload_fields=(
                    "account_id",
                    "region",
                    "credential_kind",
                    "role_arn",
                    "external_id",
                    "backfill_window_days",
                ),
            ),
            browser_agent=_browser_agent(
                (
                    "IAM roles",
                    "CloudTrail",
                    "EventBridge",
                    "AWS Organizations",
                ),
                (
                    "account id",
                    "region list",
                    "role ARN",
                    "event source availability",
                ),
                (
                    "read-only IAM policy template",
                    "external id",
                    "role trust contract",
                ),
                (
                    "cloud admin signs in and completes MFA when prompted",
                    ("cloud admin creates or approves the read-only " "Fyralis role"),
                ),
            ),
            required_inputs=("role_arn",),
            optional_inputs=("account_id", "region"),
            provider_console_url=(
                "https://console.aws.amazon.com/cloudformation/home"
                "#/stacks/create/template"
            ),
            generic_authorization_mode="customer_iam_role_ref",
        ),
        idempotent_operation_ids=(
            "sts.get_caller_identity",
            "cloudtrail.lookup_events",
        ),
        unsafe_operation_ids=("sts.assume_role",),
        retryable_status_codes=(
            400,
            *_STANDARD_RETRYABLE_HTTP_STATUSES,
        ),
        rate_limit_header_parser_id="aws.sdk_throttle_headers",
        provider_transport_enforced=True,
        operator_live_ingress=(
            "customer-cloud SQS/EventBridge and API polling workers"
        ),
        certification_notes=("The queue live adapter requires a deployed poll loop.",),
    ),
    _source(
        "miro",
        "miro",
        "Miro",
        display=_display(
            14,
            "Design",
            "Boards, items, and collaboration artifacts.",
            "API token",
            (
                "Workspace admin approval, bearer token, board allowlist, "
                "polling setup, API base confirmation."
            ),
            ("Dry run", "Limited backfill"),
        ),
        default_scopes=("team", "approved-boards"),
        provider_permissions=("boards read", "team read"),
        cli_ingress_paths=(),
        required_refs=("api_token",),
        no_ingress_reason=(
            "Miro is poll-only from the customer data plane; provider "
            "webhooks are not configured."
        ),
        data_objects=("board", "board_item"),
        history="api",
        installation_management=InstallationManagementDefinition(
            source="miro",
            table="miro_installations",
            scope_column="org_id",
            ref_columns=("secret_ref", "webhook_secret_ref"),
            entity_table="miro_boards",
            entity_install_column="miro_installation_id",
            webhook_installation_id_column="org_id",
        ),
        installation_identifiers=("organization_id", "board_id"),
        runtime_identifiers=("organization_id", "board_id"),
        ingress_routes=(
            ("backfill", "miro:item"),
            ("poll", "miro:item"),
            ("webhook", "miro:item"),
        ),
        normalization_inputs=("miro:item",),
        idempotency_builder_bindings=(
            "services.ingest.ingestion.idempotency:miro_item",
        ),
        normalizer_bindings=(
            "services.ingest.ingestion.handlers.miro:handle_miro_item",
        ),
        allowed_observation_kinds=("signal", "state_change"),
        trust_tiers=("authoritative",),
        live_transports=("api_poll",),
        onboarding=_onboarding(
            "api_token",
            "teams, boards, and board items",
            _native_connect(
                "miro",
                "api_token_native_connect",
                preflight_payload_fields=("api_token", "base_url"),
                payload_fields=("api_token", "base_url", "board_ids"),
            ),
            browser_agent=_browser_agent(
                (
                    "developer app settings",
                    "team settings",
                    "board settings",
                ),
                ("team id", "board ids", "polling scope"),
                ("board scope contract", "polling cadence guard"),
                _TOKEN_BROWSER_GATES,
            ),
            required_inputs=("api_token",),
            optional_inputs=("base_url", "board_ids"),
            provider_console_url="https://developers.miro.com/",
        ),
        certification_notes=(
            "API polling is the dependable transport; webhook assumptions are "
            "not certified.",
        ),
        idempotent_operation_ids=(
            "boards.list",
            "boards.get",
            "board_items.list",
        ),
        rate_limit_header_parser_id="http.retry_after",
        provider_transport_enforced=True,
    ),
    _source(
        "figma",
        "figma",
        "Figma",
        display=_display(
            13,
            "Design",
            ("Selected design files, durable snapshots, comments, and " "versions."),
            "OAuth",
            (
                "A deployment administrator configures one customer-owned "
                "Figma OAuth app, then users paste the Figma file URLs to "
                "approve. The file allowlist is kept current by scheduled "
                "polling."
            ),
            ("Dry run", "Limited backfill", "Backfill plus polling"),
            notice=(
                "Figma OAuth starts with a durable snapshot and scheduled "
                "polling. Provider webhooks are an optional future "
                "acceleration, not required for correctness."
            ),
        ),
        default_scopes=(
            "current_user:read",
            "file_metadata:read",
            "file_content:read",
            "file_comments:read",
            "file_versions:read",
        ),
        provider_permissions=(
            "current user identity read",
            "file metadata read",
            "file document content read",
            "file comments read",
            "file version history read",
        ),
        cli_ingress_paths=("/integrations/figma/oauth/callback",),
        required_refs=("figma_client_secret", "oauth_state_hmac_key"),
        local_rehearsal=_local_rehearsal(
            "oauth_app",
            needs_public_url=True,
            callback_path="/integrations/figma/oauth/callback",
            env=(
                "FIGMA_OAUTH_ENABLED",
                "FIGMA_CLIENT_ID",
                "FIGMA_CLIENT_SECRET_SECRET_REF",
                "FIGMA_REDIRECT_URI",
                "FIGMA_OAUTH_UI_BASE_URL",
                "FIGMA_OAUTH_SCOPES",
                "OAUTH_STATE_HMAC_KEY_SECRET_REF",
            ),
            required_env=(
                "FIGMA_OAUTH_ENABLED",
                "FIGMA_CLIENT_ID",
                "FIGMA_CLIENT_SECRET_SECRET_REF",
                "FIGMA_REDIRECT_URI",
                "FIGMA_OAUTH_UI_BASE_URL",
                "FIGMA_OAUTH_SCOPES",
                "OAUTH_STATE_HMAC_KEY_SECRET_REF",
            ),
            default_env=(
                ("FIGMA_OAUTH_ENABLED", "1"),
                (
                    "FIGMA_OAUTH_SCOPES",
                    "current_user:read,file_metadata:read,file_content:read,"
                    "file_comments:read,file_versions:read",
                ),
            ),
            runtime_components=(
                "gateway",
                "shard-fetch",
                "reconciler",
                "periodic-reconciler",
            ),
            manual_gate_names=(
                "figma_private_oauth_app_creation_or_update",
                "figma_redirect_uri_registration",
                "figma_deployment_secret_storage",
                "figma_file_scoped_oauth_consent",
            ),
        ),
        data_objects=("file_snapshot", "version", "comment"),
        history="api",
        installation_management=InstallationManagementDefinition(
            source="figma",
            table="figma_installations",
            scope_column="team_id",
            ref_columns=("secret_ref", "webhook_secret_ref"),
            entity_table="figma_files",
            entity_install_column="figma_installation_id",
            webhook_installation_id_column="team_id",
        ),
        installation_identifiers=("installation_id", "file_key"),
        runtime_identifiers=("webhook_id", "file_key"),
        ingress_routes=(
            ("backfill", "figma:event"),
            ("poll", "figma:event"),
            ("webhook", "figma:event"),
        ),
        normalization_inputs=("figma:event", "figma:file_snapshot"),
        idempotency_builder_bindings=(
            "services.ingest.ingestion.idempotency:figma_event",
            "services.ingest.ingestion.idempotency:figma_file_snapshot",
        ),
        normalizer_bindings=(
            "services.ingest.ingestion.handlers.figma:handle_figma_event",
            "services.ingest.ingestion.handlers.figma:handle_figma_event",
        ),
        allowed_observation_kinds=("signal", "state_change"),
        trust_tiers=("authoritative",),
        live_transports=("webhook", "api_poll"),
        onboarding=_onboarding(
            "oauth",
            (
                "explicitly selected Figma design files, document structure, "
                "comments, and version history"
            ),
            NativeConnectDefinition(
                kind="figma_oauth_file_scoped_connect",
                start_path="/integrations/figma/oauth/start",
                status_path="/integrations/figma/connect/status",
                retry_path="/integrations/figma/connect/retry",
                disconnect_path="/integrations/figma/connect",
                payload_fields=("file_urls", "return_path"),
            ),
            browser_agent=_browser_agent(
                (
                    "private OAuth app settings",
                    "OAuth redirect URL",
                    "read-only Figma document scopes",
                ),
                (
                    "OAuth client ID",
                    "registered redirect URL",
                    "approved design file scope",
                ),
                (
                    "Figma OAuth client secret deployment ref",
                    "OAuth state HMAC key deployment ref",
                    "design file scope contract",
                ),
                (
                    ("deployment admin signs in and completes MFA when " "prompted"),
                    (
                        "deployment admin creates or updates the private "
                        "Figma OAuth app"
                    ),
                    (
                        "deployment admin stores the Client Secret only in "
                        "the customer-cloud secret manager"
                    ),
                    (
                        "each user later approves Figma consent for "
                        "explicitly selected design files"
                    ),
                ),
            ),
            required_inputs=("file_urls",),
            optional_inputs=(),
            provider_console_url=("https://www.figma.com/developers/apps"),
        ),
        onboarding_failure_binding=(
            "services.ingest.ingestion.installations:" "record_figma_onboarding_failure"
        ),
        idempotent_operation_ids=(
            "teams.projects.list",
            "projects.files.list",
            "users.me.get",
            "files.get",
            "file_versions.list",
            "file_comments.list",
        ),
        unsafe_operation_ids=(
            "oauth.token.exchange",
            "oauth.token.refresh",
        ),
        rate_limit_header_parser_id="http.retry_after",
        provider_transport_enforced=True,
    ),
    _source(
        "carta",
        "carta",
        "Carta",
        display=_display(
            21,
            "Finance",
            "Cap table, grants, and equity-management signals.",
            "OAuth",
            (
                "Carta firm or issuer ID, OAuth access token or "
                "client-credentials path, entity scope, token re-mint "
                "process."
            ),
            ("Dry run", "Limited backfill"),
        ),
        default_scopes=("issuer", "securities", "stakeholders"),
        provider_permissions=(
            "issuer read",
            "securities read",
            "stakeholders read",
        ),
        cli_ingress_paths=(),
        required_refs=("oauth_client", "token_ref"),
        no_ingress_reason=("Carta is poll-only from the customer data plane."),
        data_objects=(
            "stakeholder",
            "share_class",
            "option_grant",
            "convertible_note",
        ),
        history="api",
        installation_management=InstallationManagementDefinition(
            source="carta",
            table="carta_installations",
            scope_column="firm_id",
            ref_columns=("secret_ref", "refresh_secret_ref"),
            entity_table="carta_entities",
            entity_install_column="carta_installation_id",
        ),
        installation_identifiers=("firm_id",),
        runtime_identifiers=("firm_id",),
        ingress_routes=(
            ("backfill", "carta:object"),
            ("poll", "carta:object"),
        ),
        normalization_inputs=("carta:object",),
        idempotency_builder_bindings=(
            "services.ingest.ingestion.idempotency:carta_entity",
        ),
        normalizer_bindings=(
            "services.ingest.ingestion.handlers.carta:handle_carta_object",
        ),
        allowed_observation_kinds=("signal", "state_change"),
        trust_tiers=("authoritative",),
        live_transports=("api_poll",),
        onboarding=_onboarding(
            "oauth",
            "issuer, securities, and stakeholder scopes",
            _native_connect(
                "carta",
                "access_token_native_connect",
                payload_fields=(
                    "access_token",
                    "base_url",
                    "issuer_id",
                    "firm_id",
                    "client_secret",
                    "refresh_token",
                    "entities",
                ),
            ),
            browser_agent=_browser_agent(
                (
                    "developer app settings",
                    "issuer or firm settings",
                    "token settings",
                ),
                (
                    "issuer id",
                    "firm id",
                    "OAuth/client-credentials availability",
                ),
                (
                    "equity entity scope contract",
                    "token refresh ref contract",
                ),
                _OAUTH_BROWSER_GATES,
            ),
            required_inputs=("token_ref",),
            optional_inputs=(
                "firm_id",
                "oauth_client",
                "base_url",
                "refresh_token_ref",
            ),
            provider_console_url="https://developers.app.carta.com/",
        ),
        idempotent_operation_ids=(
            "issuers.list",
            "issuers.get",
            "stakeholders.list",
            "share_classes.list",
            "option_grants.list",
            "convertible_notes.list",
        ),
        unsafe_operation_ids=("oauth.token.mint",),
        rate_limit_header_parser_id="http.retry_after",
        credential_refresh=_credential_refresh(
            "carta",
            "https://login.app.carta.com/o/access_token/",
            "client_credentials",
            "basic",
            rotates_refresh_token=False,
            install_table="carta_installations",
            operation_id="oauth.token.mint",
            client_secret_from_install=True,
            scope_env="CARTA_OAUTH_SCOPES",
            default_scope=(
                "read_issuer_info read_issuer_stakeholders "
                "read_issuer_shareclasses read_issuer_securities"
            ),
        ),
        provider_transport_enforced=True,
    ),
    _source(
        "hibob",
        "hibob",
        "HiBob",
        display=_display(
            23,
            "People",
            "People directory and HRIS signals.",
            "API token",
            (
                "HiBob company ID, service user/API credentials, people-field "
                "scope, employee directory approval."
            ),
            ("Dry run", "Limited backfill", "Live events"),
        ),
        default_scopes=("people", "fields", "reports"),
        provider_permissions=("people read", "fields read", "reports read"),
        cli_ingress_paths=("/webhooks/hibob",),
        required_refs=("service_user_id", "service_user_token"),
        aliases=("bob",),
        data_objects=("employee", "lifecycle", "time_off", "payroll"),
        history="api",
        installation_management=InstallationManagementDefinition(
            source="hibob",
            table="hibob_installations",
            scope_column="company_id",
            ref_columns=("secret_ref", "webhook_secret_ref"),
            entity_table="hibob_entities",
            entity_install_column="hibob_installation_id",
            webhook_installation_id_column="company_id",
        ),
        installation_identifiers=("company_id",),
        runtime_identifiers=("company_id",),
        ingress_routes=(
            ("backfill", "hibob:object"),
            ("poll", "hibob:object"),
            ("webhook", "hibob:object"),
        ),
        normalization_inputs=("hibob:object",),
        idempotency_builder_bindings=(
            "services.ingest.ingestion.idempotency:hibob_entity",
            "services.ingest.ingestion.idempotency:hibob_change",
        ),
        normalizer_bindings=(
            "services.ingest.ingestion.handlers.hibob:handle_hibob_object",
        ),
        allowed_observation_kinds=("signal", "state_change"),
        trust_tiers=("authoritative",),
        live_transports=("webhook", "api_poll"),
        onboarding=_onboarding(
            "api_token",
            "people fields, reports, and company metadata",
            _native_connect(
                "hibob",
                "api_token_native_connect",
                preflight_payload_fields=(
                    "company_id",
                    "service_user_id",
                    "service_user_token",
                    "base_url",
                ),
                payload_fields=(
                    "company_id",
                    "service_user_id",
                    "service_user_token",
                    "base_url",
                    "entities",
                    "webhook_secret",
                ),
            ),
            browser_agent=_browser_agent(
                (
                    "service user settings",
                    "reports",
                    "people fields",
                ),
                (
                    "company id",
                    "service user id",
                    "people fields",
                    "report ids",
                    "directory scope",
                ),
                (
                    "service user token ref",
                    "people field scope contract",
                ),
                _TOKEN_BROWSER_GATES,
            ),
            required_inputs=("service_user_id", "service_user_token"),
            optional_inputs=("company_id", "base_url", "webhook_secret"),
            provider_console_url="https://app.hibob.com/",
        ),
        idempotent_operation_ids=(
            "people.search",
            "timeoff.changes.list",
            "people.salaries.list",
            "people.work.list",
        ),
        rate_limit_header_parser_id="http.retry_after",
        provider_transport_enforced=True,
    ),
    _source(
        "ashby",
        "ashby",
        "Ashby",
        display=_display(
            24,
            "People",
            "Recruiting pipeline and hiring signals.",
            "API token",
            (
                "Ashby organization access, API token, jobs/candidates scope, "
                "recruiting data approval."
            ),
            ("Dry run", "Limited backfill", "Live events"),
        ),
        default_scopes=("jobs", "candidates", "interviews"),
        provider_permissions=(
            "jobs read",
            "candidates read",
            "interviews read",
        ),
        cli_ingress_paths=("/webhooks/ashby/{install-id}",),
        required_refs=("api_token",),
        data_objects=(
            "candidate",
            "application",
            "job",
            "interview",
            "offer",
            "feedback",
        ),
        history="api",
        installation_management=InstallationManagementDefinition(
            source="ashby",
            table="ashby_installations",
            scope_column="org_id",
            ref_columns=("secret_ref", "webhook_secret_ref"),
            entity_table="ashby_entities",
            entity_install_column="ashby_installation_id",
            webhook_installation_id_column="org_id",
        ),
        installation_identifiers=("organization_id",),
        runtime_identifiers=("installation_id", "organization_id"),
        ingress_routes=(
            ("backfill", "ashby:object"),
            ("poll", "ashby:object"),
            ("webhook", "ashby:object"),
        ),
        normalization_inputs=("ashby:object",),
        idempotency_builder_bindings=(
            "services.ingest.ingestion.idempotency:ashby_entity",
        ),
        normalizer_bindings=(
            "services.ingest.ingestion.handlers.ashby:handle_ashby_object",
        ),
        allowed_observation_kinds=("signal", "state_change"),
        trust_tiers=("authoritative",),
        live_transports=("webhook", "api_poll"),
        onboarding=_onboarding(
            "api_token",
            ("jobs, candidates, interviews, and organization metadata"),
            _native_connect(
                "ashby",
                "api_token_native_connect",
                preflight_payload_fields=(
                    "api_token",
                    "base_url",
                    "org_id",
                ),
                payload_fields=(
                    "api_token",
                    "base_url",
                    "org_id",
                    "entities",
                    "webhook_secret",
                ),
            ),
            browser_agent=_browser_agent(
                (
                    "API settings",
                    "webhook settings",
                    "organization settings",
                ),
                (
                    "organization id",
                    "API base URL",
                    "available recruiting scopes",
                ),
                (
                    "webhook signing secret ref",
                    "jobs/candidates/interviews scope contract",
                ),
                _TOKEN_BROWSER_GATES,
            ),
            required_inputs=("api_token",),
            optional_inputs=("org_id", "base_url", "webhook_secret"),
            provider_console_url="https://app.ashbyhq.com/admin/api",
        ),
        idempotent_operation_ids=(
            "entities.list",
            "entities.info",
        ),
        rate_limit_header_parser_id="http.retry_after",
        provider_transport_enforced=True,
    ),
    _source(
        "linkedin",
        "linkedin",
        "LinkedIn",
        display=_display(
            26,
            "CRM",
            "Company and professional-network signals.",
            "Poll",
            (
                "LinkedIn organization/page access, OAuth app access token, "
                "optional refresh token, company/profile scope, rate-limit "
                "approval."
            ),
            ("Dry run", "Limited backfill"),
        ),
        default_scopes=("organization", "company-page"),
        provider_permissions=(
            "organization read",
            "profile read",
            "rate-limit approval",
        ),
        cli_ingress_paths=(),
        required_refs=("oauth_client", "token_ref"),
        no_ingress_reason=("LinkedIn is poll-only from the customer data plane."),
        aliases=("linked_in",),
        data_objects=(
            "organization_post",
            "share_statistics",
            "follower_statistics",
        ),
        history="api",
        installation_management=InstallationManagementDefinition(
            source="linkedin",
            table="linkedin_installations",
            scope_column="organization_urn",
            ref_columns=("secret_ref", "refresh_secret_ref"),
            entity_table="linkedin_entities",
            entity_install_column="linkedin_installation_id",
        ),
        installation_identifiers=("organization_urn",),
        runtime_identifiers=("organization_urn",),
        ingress_routes=(
            ("backfill", "linkedin:object"),
            ("poll", "linkedin:object"),
        ),
        normalization_inputs=("linkedin:object",),
        idempotency_builder_bindings=(
            "services.ingest.ingestion.idempotency:linkedin_entity",
        ),
        normalizer_bindings=(
            "services.ingest.ingestion.handlers.linkedin:" "handle_linkedin_object",
        ),
        allowed_observation_kinds=("signal", "state_change"),
        trust_tiers=("authoritative",),
        live_transports=("api_poll",),
        onboarding=_onboarding(
            "poll",
            "organization/page scope and polling windows",
            _native_connect(
                "linkedin",
                "access_token_native_connect",
                preflight_payload_fields=(
                    "organization_urn",
                    "access_token",
                    "base_url",
                ),
                payload_fields=(
                    "organization_urn",
                    "access_token",
                    "base_url",
                    "refresh_token",
                    "entities",
                ),
            ),
            browser_agent=_browser_agent(
                (
                    "developer app settings",
                    "organization/page settings",
                    "rate limit posture",
                ),
                ("organization URN", "page scope", "polling window"),
                ("polling contract", "rate-limit guard"),
                _OAUTH_BROWSER_GATES,
            ),
            required_inputs=("token_ref",),
            optional_inputs=(
                "organization_urn",
                "oauth_client",
                "base_url",
                "refresh_token_ref",
            ),
            provider_console_url=("https://www.linkedin.com/developers/apps"),
        ),
        credential_refresh=_credential_refresh(
            "linkedin",
            "https://www.linkedin.com/oauth/v2/accessToken",
            "refresh_token",
            "body",
            rotates_refresh_token=True,
            install_table="linkedin_installations",
            operation_id="oauth.token.refresh",
            default_expires_in=86400,
        ),
        idempotent_operation_ids=(
            "posts.list",
            "share_statistics.list",
            "follower_statistics.list",
            "organizations.get",
        ),
        unsafe_operation_ids=("oauth.token.refresh",),
        rate_limit_header_parser_id="http.retry_after",
        provider_transport_enforced=True,
    ),
    _source(
        "whatsapp",
        "meta",
        "WhatsApp Business",
        display=_display(
            11,
            "Communication",
            "Cloud API live webhook ingestion.",
            "Webhook",
            (
                "WhatsApp Cloud API app, business account and phone IDs, "
                "generated verify token, app secret, and optional "
                "customer-local access token."
            ),
            ("Live events",),
            display_name_override="WhatsApp",
        ),
        default_scopes=("business-account", "approved-phone-ids"),
        provider_permissions=(
            "business account read",
            "messages webhook",
            "phone id",
        ),
        cli_ingress_paths=("/integrations/whatsapp/webhook",),
        required_refs=("phone_number_id", "app_secret", "verify_token"),
        aliases=("whatsapp_business", "whatsapp_cloud"),
        data_objects=("inbound_message", "delivery_status"),
        history=None,
        installation_management=InstallationManagementDefinition(
            source="whatsapp",
            table="whatsapp_installations",
            scope_column="phone_number_id",
            ref_columns=(
                "app_secret_ref",
                "verify_token_ref",
                "access_token_ref",
            ),
            entity_table=None,
            entity_install_column=None,
            base_url_column=None,
            enabled_column="enabled",
            updated_at_column="updated_at",
            status_detail_columns=("waba_id", "display_phone_number"),
            status_presence_columns=(("access_token_configured", "access_token_ref"),),
            status_credential_column_groups=(("app_secret_ref", "verify_token_ref"),),
        ),
        installation_identifiers=("phone_number_id",),
        runtime_identifiers=("phone_number_id",),
        ingress_routes=(("webhook", "whatsapp:message"),),
        normalization_inputs=("whatsapp:message",),
        idempotency_builder_bindings=(
            "services.ingest.ingestion.idempotency:whatsapp_message",
            "services.ingest.ingestion.idempotency:whatsapp_status",
        ),
        normalizer_bindings=(
            "services.ingest.ingestion.handlers.whatsapp:handle_whatsapp",
        ),
        allowed_observation_kinds=("signal", "state_change"),
        trust_tiers=("attested_agent", "authoritative"),
        live_transports=("webhook",),
        onboarding=_onboarding(
            "webhook",
            (
                "business account, phone numbers, webhook verification, and "
                "message events"
            ),
            _native_connect(
                "whatsapp",
                "whatsapp_native_connect",
                preflight_payload_fields=(
                    "phone_number_id",
                    "app_secret",
                    "verify_token",
                ),
                payload_fields=(
                    "phone_number_id",
                    "business_account_id",
                    "display_phone_number",
                    "app_secret",
                    "verify_token",
                    "access_token",
                ),
            ),
            browser_agent=_browser_agent(
                (
                    "Meta app settings",
                    "WhatsApp business settings",
                    "webhook settings",
                ),
                (
                    "business account id",
                    "phone number id",
                    "webhook verify target",
                ),
                (
                    "verify token ref",
                    "app secret ref",
                    "WhatsApp webhook contract",
                ),
                (
                    "Meta admin signs in and completes MFA when prompted",
                    ("Meta admin approves business phone and webhook " "subscriptions"),
                ),
            ),
            required_inputs=(
                "phone_number_id",
                "verify_token",
                "app_secret",
            ),
            optional_inputs=(
                "business_account_id",
                "display_phone_number",
                "access_token",
            ),
            provider_console_url=("https://developers.facebook.com/apps/"),
            generic_authorization_mode="customer_webhook_app",
        ),
        capability_flags=("no_outbound_provider_requests",),
        provider_transport_enforced=True,
        certification_notes=(
            "Live webhook only; missed and pre-install history is unrecoverable.",
        ),
    ),
    _source(
        "facebook_pages",
        "meta",
        "Facebook Pages / Messenger",
        installation_status_loader_binding=(
            "services.ingest.ingestion.installation_status:"
            "load_facebook_pages_installation_status_rows"
        ),
        display=_display(
            8,
            "Communication",
            (
                "All available Page message history via Graph API, plus live "
                "webhooks."
            ),
            "OAuth",
            (
                "Meta app review, Page selection, OAuth approval, Page "
                "message permissions, app secret, verify token, and messages "
                "webhook subscription."
            ),
            (
                "Dry run",
                "Limited backfill",
                "Live events",
                "Backfill plus live",
            ),
            display_name_override="Facebook Page Messages",
        ),
        default_scopes=(
            "pages_show_list",
            "pages_messaging",
            "pages_manage_metadata",
            "pages_read_engagement",
        ),
        provider_permissions=(
            "pages_show_list",
            "pages_messaging",
            "pages_manage_metadata",
            "pages_read_engagement",
        ),
        cli_ingress_paths=(
            "/integrations/facebook_pages/callback",
            "/integrations/facebook_pages/webhook",
        ),
        required_refs=(
            "oauth_client",
            "page_access_token",
            "app_secret",
            "verify_token",
        ),
        local_rehearsal=_local_rehearsal(
            "oauth_app",
            needs_public_url=True,
            install_endpoint="/integrations/facebook_pages/install",
            callback_path="/integrations/facebook_pages/callback",
            webhook_path="/integrations/facebook_pages/webhook",
            env=(
                "FACEBOOK_APP_ID",
                "FACEBOOK_APP_SECRET",
                "FACEBOOK_REDIRECT_URI",
                "FACEBOOK_WEBHOOK_VERIFY_TOKEN",
                "FACEBOOK_PAGE_ID",
                "FACEBOOK_GRAPH_API_VERSION",
                "OAUTH_STATE_HMAC_KEY",
            ),
            required_env=(
                "FACEBOOK_APP_ID",
                "FACEBOOK_APP_SECRET",
                "FACEBOOK_REDIRECT_URI",
                "FACEBOOK_WEBHOOK_VERIFY_TOKEN",
                "OAUTH_STATE_HMAC_KEY",
            ),
            manual_gate_names=(
                "facebook_pages_app_creation_or_update",
                "facebook_pages_oauth_consent",
                "facebook_pages_webhook_subscription_approval",
            ),
        ),
        aliases=("facebook", "facebook_messenger", "messenger"),
        data_objects=("conversation", "message", "attachment"),
        history="api",
        installation_identifiers=("page_id",),
        runtime_identifiers=("page_id",),
        ingress_routes=(
            ("webhook", "facebook_pages:message"),
            ("backfill", "facebook_pages:message"),
        ),
        normalization_inputs=("facebook_pages:message",),
        idempotency_builder_bindings=(
            "services.ingest.ingestion.idempotency:facebook_page_message",
        ),
        normalizer_bindings=(
            "services.ingest.ingestion.handlers.facebook_pages:"
            "handle_facebook_pages",
        ),
        allowed_observation_kinds=("signal",),
        trust_tiers=("attested_agent",),
        live_transports=("webhook", "api_poll"),
        onboarding=_onboarding(
            "oauth",
            (
                "Facebook Pages, Messenger conversations, Page message "
                "history, and webhooks"
            ),
            _native_connect(
                "facebook_pages",
                "oauth_native_connect",
                preflight_payload_fields=(
                    "page_id",
                    "oauth_redirect_url",
                    "events_request_url",
                ),
                payload_fields=(
                    "page_id",
                    "installation_id",
                    "oauth_redirect_url",
                    "events_request_url",
                ),
            ),
            browser_agent=_browser_agent(
                (
                    "Meta app settings",
                    "Messenger API settings",
                    "Page access settings",
                    "webhook settings",
                ),
                (
                    "page id",
                    "Page message scope",
                    "webhook callback URL",
                ),
                (
                    "verify token ref",
                    "app secret ref",
                    "Facebook Page message scope contract",
                ),
                (
                    "Meta admin signs in and completes MFA when prompted",
                    (
                        "Meta admin approves Page messaging scopes and "
                        "webhook subscription"
                    ),
                ),
            ),
            required_inputs=("page_id",),
            optional_inputs=("oauth_redirect_url", "events_request_url"),
            provider_console_url=("https://developers.facebook.com/apps/"),
        ),
        idempotent_operation_ids=(
            "pages.list",
            "pages.subscribe",
            "conversations.list",
            "messages.list",
        ),
        unsafe_operation_ids=("oauth.token.exchange",),
        rate_limit_header_parser_id="http.retry_after",
        provider_transport_enforced=True,
    ),
)


def _provider_operation_ids(provider_id: str) -> tuple[str, ...]:
    """Derive a provider's operation union from its owned sources."""

    ordered: dict[str, None] = {}
    for source in SOURCE_DEFINITIONS:
        if source.provider_id != provider_id:
            continue
        for operation_id in source.operation_policy_ids:
            ordered.setdefault(operation_id, None)
    return tuple(ordered)


PROVIDER_DEFINITIONS: tuple[ProviderDefinition, ...] = tuple(
    replace(
        provider,
        operation_policy_ids=_provider_operation_ids(provider.provider_id),
    )
    for provider in _PROVIDER_BASE_DEFINITIONS
)

# Identity is derived from the definition catalogs.  There is no leading
# literal tuple for a new source or provider to update independently.
CANONICAL_SOURCE_IDS: tuple[str, ...] = tuple(
    source.source_id for source in SOURCE_DEFINITIONS
)
CANONICAL_PROVIDER_IDS: tuple[str, ...] = tuple(
    provider.provider_id for provider in PROVIDER_DEFINITIONS
)


NON_SOURCE_CHANNEL_DEFINITIONS: tuple[NonSourceChannelDefinition, ...] = (
    NonSourceChannelDefinition(
        channel="email:inbound",
        owner_kind="provider",
        owner_id="email",
        normalizer_binding=(f"{_HANDLER_PACKAGE}.email:handle_email_webhook"),
        allowed_observation_kinds=("signal",),
        trust_tiers=("attested_agent", "inferential"),
    ),
    NonSourceChannelDefinition(
        channel="calendar:sync",
        owner_kind="provider",
        owner_id="calendar",
        normalizer_binding=(f"{_HANDLER_PACKAGE}.calendar:handle_calendar_webhook"),
        allowed_observation_kinds=("signal", "state_change"),
        trust_tiers=("authoritative",),
    ),
    NonSourceChannelDefinition(
        channel="linear:webhook",
        owner_kind="provider",
        owner_id="linear",
        normalizer_binding=(f"{_HANDLER_PACKAGE}.linear:handle_linear_webhook"),
        allowed_observation_kinds=("signal", "state_change"),
        trust_tiers=("authoritative",),
    ),
    NonSourceChannelDefinition(
        channel="stripe:webhook",
        owner_kind="provider",
        owner_id="stripe",
        normalizer_binding=(f"{_HANDLER_PACKAGE}.stripe:handle_stripe_webhook"),
        allowed_observation_kinds=("signal", "state_change"),
        trust_tiers=("authoritative",),
    ),
    NonSourceChannelDefinition(
        channel="internal:state_change",
        owner_kind="platform",
        owner_id="internal",
        normalizer_binding=(f"{_HANDLER_PACKAGE}.system:handle_state_change"),
        allowed_observation_kinds=("state_change",),
        trust_tiers=("authoritative",),
    ),
    NonSourceChannelDefinition(
        channel="internal:anomaly",
        owner_kind="platform",
        owner_id="internal",
        normalizer_binding=f"{_HANDLER_PACKAGE}.system:handle_anomaly",
        allowed_observation_kinds=("anomaly_flagged",),
        trust_tiers=("authoritative",),
    ),
    NonSourceChannelDefinition(
        channel="internal:prediction_resolution",
        owner_kind="platform",
        owner_id="internal",
        normalizer_binding=(f"{_HANDLER_PACKAGE}.system:handle_prediction_resolution"),
        allowed_observation_kinds=("prediction_resolution",),
        trust_tiers=("authoritative",),
    ),
    # Trust-classified channels that bypass the in-process normalizer router.
    NonSourceChannelDefinition(
        channel="discord:webhook",
        owner_kind="provider",
        owner_id="discord",
        normalizer_binding=None,
        allowed_observation_kinds=("signal",),
        trust_tiers=("attested_agent",),
    ),
    NonSourceChannelDefinition(
        channel="journal:ui",
        owner_kind="platform",
        owner_id="journal",
        normalizer_binding=None,
        allowed_observation_kinds=("signal",),
        trust_tiers=("authoritative",),
    ),
    NonSourceChannelDefinition(
        channel="agent:attested",
        owner_kind="platform",
        owner_id="agent",
        normalizer_binding=None,
        allowed_observation_kinds=("signal",),
        trust_tiers=("attested_agent",),
    ),
    NonSourceChannelDefinition(
        channel="news:rss",
        owner_kind="provider",
        owner_id="news",
        normalizer_binding=None,
        allowed_observation_kinds=("signal",),
        trust_tiers=("reputable",),
    ),
    NonSourceChannelDefinition(
        channel="news:web",
        owner_kind="provider",
        owner_id="news",
        normalizer_binding=None,
        allowed_observation_kinds=("signal",),
        trust_tiers=("inferential_external",),
    ),
    NonSourceChannelDefinition(
        channel="social:twitter",
        owner_kind="provider",
        owner_id="social",
        normalizer_binding=None,
        allowed_observation_kinds=("signal",),
        trust_tiers=("unvetted",),
    ),
    NonSourceChannelDefinition(
        channel="social:linkedin",
        owner_kind="provider",
        owner_id="social",
        normalizer_binding=None,
        allowed_observation_kinds=("signal",),
        trust_tiers=("reputable",),
    ),
    NonSourceChannelDefinition(
        channel="market:api",
        owner_kind="provider",
        owner_id="market",
        normalizer_binding=None,
        allowed_observation_kinds=("signal",),
        trust_tiers=("authoritative_external",),
    ),
    NonSourceChannelDefinition(
        channel="regulatory:api",
        owner_kind="provider",
        owner_id="regulatory",
        normalizer_binding=None,
        allowed_observation_kinds=("signal",),
        trust_tiers=("authoritative_external",),
    ),
    NonSourceChannelDefinition(
        channel="analyst:report",
        owner_kind="provider",
        owner_id="analyst",
        normalizer_binding=None,
        allowed_observation_kinds=("signal",),
        trust_tiers=("reputable",),
    ),
    NonSourceChannelDefinition(
        channel="ui:contestation",
        owner_kind="platform",
        owner_id="ui",
        normalizer_binding=None,
        allowed_observation_kinds=("contestation",),
        trust_tiers=("authoritative",),
    ),
)


def _build_name_index(
    definitions: Iterable[ProviderDefinition | SourceDefinition],
    *,
    id_attribute: str,
) -> dict[str, str]:
    index: dict[str, str] = {}
    for definition in definitions:
        canonical_id = cast(str, getattr(definition, id_attribute))
        names = (canonical_id, definition.display_name, *definition.aliases)
        for name in names:
            normalized = normalize_catalog_name(name)
            if not normalized:
                raise CatalogValidationError(
                    f"{id_attribute}={canonical_id!r} has a name that "
                    f"normalizes to empty: {name!r}"
                )
            existing = index.get(normalized)
            if existing is not None and existing != canonical_id:
                raise CatalogValidationError(
                    f"normalized name {normalized!r} is ambiguous between "
                    f"{existing!r} and {canonical_id!r}"
                )
            index[normalized] = canonical_id
    return index


def validate_catalog(
    providers: Sequence[ProviderDefinition],
    sources: Sequence[SourceDefinition],
    *,
    expected_provider_ids: Sequence[str] | None = None,
    expected_source_ids: Sequence[str] | None = None,
) -> None:
    """Validate deterministic identity, wiring, and certification invariants."""

    provider_ids = tuple(provider.provider_id for provider in providers)
    source_ids = tuple(source.source_id for source in sources)
    ui_slugs = tuple(source.ui_slug for source in sources)
    display_orders = tuple(source.display.order for source in sources)
    if len(provider_ids) != len(set(provider_ids)):
        raise CatalogValidationError("provider ids must be unique")
    if len(source_ids) != len(set(source_ids)):
        raise CatalogValidationError("source ids must be unique")
    if len(ui_slugs) != len(set(ui_slugs)):
        raise CatalogValidationError("source ui slugs must be unique")
    if set(display_orders) != set(range(len(sources))):
        raise CatalogValidationError(
            "source display order must contain every integer from zero to "
            f"{len(sources) - 1}; got {display_orders!r}"
        )
    if expected_provider_ids is not None and provider_ids != tuple(
        expected_provider_ids
    ):
        raise CatalogValidationError(
            "provider order/coverage differs from the canonical ids: "
            f"expected {tuple(expected_provider_ids)!r}, got {provider_ids!r}"
        )
    if expected_source_ids is not None and source_ids != tuple(expected_source_ids):
        raise CatalogValidationError(
            "source order/coverage differs from the canonical ids: "
            f"expected {tuple(expected_source_ids)!r}, got {source_ids!r}"
        )

    provider_set = set(provider_ids)
    for source in sources:
        if source.provider_id not in provider_set:
            raise CatalogValidationError(
                f"source {source.source_id!r} references unknown provider "
                f"{source.provider_id!r}"
            )
        onboarding = source.onboarding
        for field_name in (
            "default_scopes",
            "provider_permissions",
            "required_refs",
        ):
            if not getattr(onboarding, field_name):
                raise CatalogValidationError(
                    f"source {source.source_id!r} has no onboarding " f"{field_name}"
                )
        missing = source.certification.missing_required_declarations()
        if missing:
            raise CatalogValidationError(
                f"source {source.source_id!r} is missing required certification "
                f"declarations: {', '.join(missing)}"
            )

    for provider in providers:
        derived_source_ids = tuple(
            source.source_id
            for source in sources
            if source.provider_id == provider.provider_id
        )
        if provider.source_ids != derived_source_ids:
            raise CatalogValidationError(
                f"provider {provider.provider_id!r} declares source_ids "
                f"{provider.source_ids!r}, but source definitions resolve to "
                f"{derived_source_ids!r}"
            )
        derived_operation_ids = tuple(
            dict.fromkeys(
                operation_id
                for source in sources
                if source.provider_id == provider.provider_id
                for operation_id in source.operation_policy_ids
            )
        )
        if provider.operation_policy_ids != derived_operation_ids:
            raise CatalogValidationError(
                f"provider {provider.provider_id!r} operation policies "
                f"{provider.operation_policy_ids!r} differ from the source "
                f"contract union {derived_operation_ids!r}"
            )
        provider_policies: dict[str, RequestPolicy] = {}
        for source in sources:
            if source.provider_id != provider.provider_id:
                continue
            for definition in source.operation_policies:
                existing = provider_policies.get(definition.operation_id)
                if existing is not None and existing != definition.request_policy:
                    raise CatalogValidationError(
                        f"provider {provider.provider_id!r} has conflicting "
                        "source-owned policies for operation "
                        f"{definition.operation_id!r}"
                    )
                provider_policies[definition.operation_id] = definition.request_policy

    _build_name_index(providers, id_attribute="provider_id")
    _build_name_index(sources, id_attribute="source_id")

    normalization_inputs: dict[str, str] = {}
    onboarding_route_paths: dict[str, str] = {}
    binding_ids: dict[str, str] = {}
    certification_ids: dict[str, str] = {}
    installation_management_tables: dict[str, str] = {}
    for source in sources:
        route_prefix = f"/integrations/{source.source_id}/"
        for route_path in source.onboarding.native_connect.route_paths():
            if not route_path.startswith(route_prefix):
                raise CatalogValidationError(
                    f"source {source.source_id!r} owns native-connect route "
                    f"{route_path!r} outside {route_prefix!r}"
                )
            existing = onboarding_route_paths.get(route_path)
            if existing is not None and existing != source.source_id:
                raise CatalogValidationError(
                    f"native-connect route {route_path!r} is owned by both "
                    f"{existing!r} and {source.source_id!r}"
                )
            onboarding_route_paths[route_path] = source.source_id

        for channel in source.normalization_inputs:
            existing = normalization_inputs.get(channel)
            if existing is not None and existing != source.source_id:
                raise CatalogValidationError(
                    f"normalization input {channel!r} is owned by both "
                    f"{existing!r} and {source.source_id!r}"
                )
            normalization_inputs[channel] = source.source_id

        installation_bindings = (
            tuple(
                binding
                for binding in (
                    source.installation_adapter.loader_binding,
                    source.installation_adapter.planner_client_builder_binding,
                    source.installation_adapter.onboarding_failure_binding,
                )
                if binding is not None
            )
            if source.installation_adapter is not None
            else ()
        )
        if (
            source.installation_adapter is not None
            and source.installation_adapter.management is not None
        ):
            management = source.installation_adapter.management
            existing = installation_management_tables.get(management.table)
            if existing is not None and existing != source.source_id:
                raise CatalogValidationError(
                    f"installation management table {management.table!r} is "
                    f"owned by both {existing!r} and {source.source_id!r}"
                )
            installation_management_tables[management.table] = source.source_id
        source_bindings = (
            *source.normalizer_bindings,
            *source.live_bindings,
            source.connect_router_binding,
            *(
                binding
                for binding in (
                    source.planner_binding,
                    source.fetcher_binding,
                    source.reconciler_binding,
                )
                if binding is not None
            ),
            *installation_bindings,
        )
        for binding in source_bindings:
            existing = binding_ids.get(binding)
            if existing is not None and existing != source.source_id:
                raise CatalogValidationError(
                    f"binding {binding!r} is declared by both {existing!r} "
                    f"and {source.source_id!r}"
                )
            binding_ids[binding] = source.source_id

        cert = source.certification
        for declaration in (cert.test_kit_id, cert.evidence_id, cert.canary_id):
            if declaration is None:
                continue
            existing = certification_ids.get(declaration)
            if existing is not None and existing != source.source_id:
                raise CatalogValidationError(
                    f"certification declaration {declaration!r} is shared by "
                    f"{existing!r} and {source.source_id!r}"
                )
            certification_ids[declaration] = source.source_id


def validate_channel_catalog(
    sources: Sequence[SourceDefinition],
    non_source_channels: Sequence[NonSourceChannelDefinition],
) -> None:
    """Validate channel ownership across source and supplemental contracts."""

    owners: dict[str, str] = {}
    canonical_bindings: dict[str, str] = {}
    for source in sources:
        for channel, binding in source.normalization_contracts():
            owner = f"source:{source.source_id}"
            existing = owners.get(channel)
            if existing is not None:
                raise CatalogValidationError(
                    f"normalization channel {channel!r} is owned by both "
                    f"{existing!r} and {owner!r}"
                )
            owners[channel] = owner
            canonical_bindings[channel] = binding

    for definition in non_source_channels:
        owner = f"{definition.owner_kind}:{definition.owner_id}"
        existing = owners.get(definition.channel)
        if existing is not None:
            raise CatalogValidationError(
                f"normalization channel {definition.channel!r} is owned by "
                f"both {existing!r} and {owner!r}"
            )
        owners[definition.channel] = owner


def validate_provider_ingress_catalog(
    providers: Sequence[ProviderDefinition],
    sources: Sequence[SourceDefinition],
    non_source_channels: Sequence[NonSourceChannelDefinition],
) -> None:
    """Validate provider-edge routes against canonical channel ownership."""

    source_by_id = {source.source_id: source for source in sources}
    non_source_by_channel = {
        definition.channel: definition for definition in non_source_channels
    }
    route_owners: dict[str, str] = {}
    oauth_source_owners: dict[str, str] = {}
    public_route_owners: dict[tuple[str, str], str] = {}
    dedicated_ingress_owners: dict[str, str] = {}
    dedicated_route_owners: dict[tuple[str, str], str] = {}
    router_debug_modes: dict[str, bool] = {}
    verifier_owners: dict[str, str] = {}
    extractor_owners: dict[str, str] = {}

    for provider in providers:
        for ingress in provider.oauth_ingresses:
            existing = oauth_source_owners.get(ingress.source_id)
            if existing is not None:
                raise CatalogValidationError(
                    f"OAuth ingress for source {ingress.source_id!r} is "
                    f"declared by both {existing!r} and "
                    f"{provider.provider_id!r}"
                )
            oauth_source_owners[ingress.source_id] = provider.provider_id
            source = source_by_id.get(ingress.source_id)
            if source is None:
                raise CatalogValidationError(
                    f"OAuth ingress references unknown source " f"{ingress.source_id!r}"
                )
            if source.provider_id != provider.provider_id:
                raise CatalogValidationError(
                    f"OAuth ingress for source {source.source_id!r} is owned "
                    f"by provider {provider.provider_id!r}, but the source "
                    f"belongs to {source.provider_id!r}"
                )
            for purpose, path in (
                ("install", ingress.install_path),
                ("callback", ingress.callback_path),
                *(
                    ("result", result_path)
                    for result_path in ingress.public_result_paths
                ),
            ):
                route_key = ("GET", path)
                owner = f"oauth:{ingress.source_id}:{purpose}"
                existing = public_route_owners.get(route_key)
                if existing is not None:
                    raise CatalogValidationError(
                        f"public route GET {path!r} is declared by both "
                        f"{existing!r} and {owner!r}"
                    )
                public_route_owners[route_key] = owner

        for ingress in provider.webhook_ingresses:
            existing = route_owners.get(ingress.route_id)
            if existing is not None:
                raise CatalogValidationError(
                    f"webhook route {ingress.route_id!r} is declared by both "
                    f"{existing!r} and {provider.provider_id!r}"
                )
            route_owners[ingress.route_id] = provider.provider_id
            route_key = ("POST", ingress.route_path)
            existing_path_owner = public_route_owners.get(route_key)
            if existing_path_owner is not None:
                raise CatalogValidationError(
                    f"public route POST {ingress.route_path!r} is declared by "
                    f"both {existing_path_owner!r} and "
                    f"webhook:{ingress.route_id!r}"
                )
            public_route_owners[route_key] = f"webhook:{ingress.route_id}"

            for binding, owners, label in (
                (ingress.verifier_binding, verifier_owners, "verifier"),
                (
                    ingress.tenant_extractor_binding,
                    extractor_owners,
                    "tenant extractor",
                ),
            ):
                existing = owners.get(binding)
                if existing is not None:
                    raise CatalogValidationError(
                        f"webhook {label} binding {binding!r} is shared by "
                        f"routes {existing!r} and {ingress.route_id!r}"
                    )
                owners[binding] = ingress.route_id

            if ingress.source_id is not None:
                source = source_by_id.get(ingress.source_id)
                if source is None:
                    raise CatalogValidationError(
                        f"webhook route {ingress.route_id!r} references "
                        f"unknown source {ingress.source_id!r}"
                    )
                if source.provider_id != provider.provider_id:
                    raise CatalogValidationError(
                        f"webhook route {ingress.route_id!r} is owned by "
                        f"provider {provider.provider_id!r}, but source "
                        f"{source.source_id!r} belongs to "
                        f"{source.provider_id!r}"
                    )
                expected_channel = source.channel_for_ingress("webhook")
                if ingress.channel != expected_channel:
                    raise CatalogValidationError(
                        f"webhook route {ingress.route_id!r} declares channel "
                        f"{ingress.channel!r}; source {source.source_id!r} "
                        f"declares {expected_channel!r}"
                    )
                continue

            channel = non_source_by_channel.get(ingress.channel)
            if channel is None:
                raise CatalogValidationError(
                    f"ingress-only webhook route {ingress.route_id!r} "
                    f"references undeclared channel {ingress.channel!r}"
                )
            if (
                channel.owner_kind != "provider"
                or channel.owner_id != provider.provider_id
            ):
                raise CatalogValidationError(
                    f"ingress-only webhook route {ingress.route_id!r} channel "
                    f"{ingress.channel!r} is not owned by provider "
                    f"{provider.provider_id!r}"
                )

        for ingress in provider.dedicated_ingresses:
            existing = dedicated_ingress_owners.get(ingress.ingress_id)
            if existing is not None:
                raise CatalogValidationError(
                    f"dedicated ingress {ingress.ingress_id!r} is declared by "
                    f"both {existing!r} and {provider.provider_id!r}"
                )
            dedicated_ingress_owners[ingress.ingress_id] = provider.provider_id

            for method in ingress.methods:
                route_key = (ingress.route_path, method)
                existing = dedicated_route_owners.get(route_key)
                if existing is not None:
                    raise CatalogValidationError(
                        f"dedicated ingress route {method} "
                        f"{ingress.route_path!r} is declared by both "
                        f"{existing!r} and {ingress.ingress_id!r}"
                    )
                dedicated_route_owners[route_key] = ingress.ingress_id
                public_route_key = (method, ingress.route_path)
                existing_path_owner = public_route_owners.get(public_route_key)
                if existing_path_owner is not None:
                    raise CatalogValidationError(
                        f"public route {method} {ingress.route_path!r} is "
                        f"declared by both {existing_path_owner!r} and "
                        f"dedicated:{ingress.ingress_id!r}"
                    )
                public_route_owners[public_route_key] = (
                    f"dedicated:{ingress.ingress_id}"
                )

            source = source_by_id.get(ingress.source_id)
            if source is None:
                raise CatalogValidationError(
                    f"dedicated ingress {ingress.ingress_id!r} references "
                    f"unknown source {ingress.source_id!r}"
                )
            if source.provider_id != provider.provider_id:
                raise CatalogValidationError(
                    f"dedicated ingress {ingress.ingress_id!r} is owned by "
                    f"provider {provider.provider_id!r}, but source "
                    f"{source.source_id!r} belongs to {source.provider_id!r}"
                )
            if ingress.channel is not None and (
                ingress.channel not in source.normalization_inputs
            ):
                raise CatalogValidationError(
                    f"dedicated ingress {ingress.ingress_id!r} channel "
                    f"{ingress.channel!r} is not owned by source "
                    f"{source.source_id!r}"
                )
            if (
                ingress.channel is None
                and ingress.kafka_mode != "hydrated_messages_handler_managed"
            ):
                raise CatalogValidationError(
                    f"dedicated ingress {ingress.ingress_id!r} may omit its "
                    "channel only when its handler hydrates source records"
                )
            expected_transport = (
                "pubsub" if ingress.ingress_kind == "pubsub" else "webhook"
            )
            if expected_transport not in source.live_transports:
                raise CatalogValidationError(
                    f"dedicated ingress {ingress.ingress_id!r} requires live "
                    f"transport {expected_transport!r} on source "
                    f"{source.source_id!r}"
                )

            existing_debug_mode = router_debug_modes.get(ingress.router_factory_binding)
            if (
                existing_debug_mode is not None
                and existing_debug_mode
                != ingress.router_factory_accepts_debug_endpoints
            ):
                raise CatalogValidationError(
                    f"dedicated router factory "
                    f"{ingress.router_factory_binding!r} has inconsistent "
                    "debug-endpoint configuration"
                )
            router_debug_modes[ingress.router_factory_binding] = (
                ingress.router_factory_accepts_debug_endpoints
            )


validate_catalog(
    PROVIDER_DEFINITIONS,
    SOURCE_DEFINITIONS,
    expected_provider_ids=CANONICAL_PROVIDER_IDS,
    expected_source_ids=CANONICAL_SOURCE_IDS,
)
validate_channel_catalog(
    SOURCE_DEFINITIONS,
    NON_SOURCE_CHANNEL_DEFINITIONS,
)
validate_provider_ingress_catalog(
    PROVIDER_DEFINITIONS,
    SOURCE_DEFINITIONS,
    NON_SOURCE_CHANNEL_DEFINITIONS,
)


_PROVIDER_BY_ID = MappingProxyType(
    {provider.provider_id: provider for provider in PROVIDER_DEFINITIONS}
)
_SOURCE_BY_ID = MappingProxyType(
    {source.source_id: source for source in SOURCE_DEFINITIONS}
)
_SOURCE_BY_UI_SLUG = MappingProxyType(
    {
        source.ui_slug: source
        for source in sorted(SOURCE_DEFINITIONS, key=lambda item: item.ui_slug)
    }
)
_NON_SOURCE_CHANNEL_BY_NAME = MappingProxyType(
    {definition.channel: definition for definition in NON_SOURCE_CHANNEL_DEFINITIONS}
)
_PROVIDER_NAME_INDEX = MappingProxyType(
    _build_name_index(PROVIDER_DEFINITIONS, id_attribute="provider_id")
)
_SOURCE_NAME_INDEX = MappingProxyType(
    _build_name_index(SOURCE_DEFINITIONS, id_attribute="source_id")
)
_WEBHOOK_INGRESS_BY_ROUTE = MappingProxyType(
    {
        ingress.route_id: ingress
        for provider in PROVIDER_DEFINITIONS
        for ingress in provider.webhook_ingresses
    }
)
_PROVIDER_BY_WEBHOOK_ROUTE = MappingProxyType(
    {
        ingress.route_id: provider
        for provider in PROVIDER_DEFINITIONS
        for ingress in provider.webhook_ingresses
    }
)
_OAUTH_INGRESS_BY_SOURCE = MappingProxyType(
    {
        ingress.source_id: ingress
        for provider in PROVIDER_DEFINITIONS
        for ingress in provider.oauth_ingresses
    }
)
_DEDICATED_INGRESS_BY_ID = MappingProxyType(
    {
        ingress.ingress_id: ingress
        for provider in PROVIDER_DEFINITIONS
        for ingress in provider.dedicated_ingresses
    }
)
_PROVIDER_BY_DEDICATED_INGRESS = MappingProxyType(
    {
        ingress.ingress_id: provider
        for provider in PROVIDER_DEFINITIONS
        for ingress in provider.dedicated_ingresses
    }
)

PROVIDER_CATALOG: Mapping[str, ProviderDefinition] = _PROVIDER_BY_ID
SOURCE_CATALOG: Mapping[str, SourceDefinition] = _SOURCE_BY_ID
SOURCE_CONNECTION_CATALOG: Mapping[str, SourceDefinition] = _SOURCE_BY_UI_SLUG
SOURCE_CONNECTION_SLUGS: tuple[str, ...] = tuple(SOURCE_CONNECTION_CATALOG)
INSTALLATION_MANAGEMENT_CATALOG: Mapping[
    str,
    InstallationManagementDefinition,
] = MappingProxyType(
    {
        source.source_id: source.installation_adapter.management
        for source in SOURCE_DEFINITIONS
        if source.installation_adapter is not None
        and source.installation_adapter.management is not None
    }
)
OAUTH_INGRESS_CATALOG: Mapping[str, OAuthIngressDefinition] = _OAUTH_INGRESS_BY_SOURCE
SOURCE_OPERATION_POLICY_CATALOG: Mapping[
    str,
    Mapping[str, RequestPolicy],
] = MappingProxyType(
    {
        source.source_id: MappingProxyType(
            {
                definition.operation_id: definition.request_policy
                for definition in source.operation_policies
            }
        )
        for source in SOURCE_DEFINITIONS
    }
)
PROVIDER_TRANSPORT_OPERATION_CATALOG: Mapping[str, frozenset[str]] = MappingProxyType(
    {
        source.source_id: frozenset(SOURCE_OPERATION_POLICY_CATALOG[source.source_id])
        for source in SOURCE_DEFINITIONS
        if source.operation_policy_ids
    }
)
DEDICATED_INGRESS_CATALOG: Mapping[str, DedicatedIngressDefinition] = (
    _DEDICATED_INGRESS_BY_ID
)
DEDICATED_INGRESS_DEFINITIONS: tuple[DedicatedIngressDefinition, ...] = tuple(
    _DEDICATED_INGRESS_BY_ID.values()
)
WEBHOOK_INGRESS_CATALOG: Mapping[str, WebhookIngressDefinition] = (
    _WEBHOOK_INGRESS_BY_ROUTE
)
WEBHOOK_INGRESS_ROUTE_IDS: tuple[str, ...] = tuple(_WEBHOOK_INGRESS_BY_ROUTE)
_source_live_ingress_paths: dict[str, str] = {
    ingress.source_id: ingress.route_path
    for provider in PROVIDER_DEFINITIONS
    for ingress in provider.webhook_ingresses
    if ingress.source_id is not None
}
for provider in PROVIDER_DEFINITIONS:
    for ingress in provider.dedicated_ingresses:
        existing = _source_live_ingress_paths.get(ingress.source_id)
        if existing is not None and existing != ingress.route_path:
            raise CatalogValidationError(
                f"source {ingress.source_id!r} has ambiguous live ingress "
                f"paths {existing!r} and {ingress.route_path!r}"
            )
        _source_live_ingress_paths[ingress.source_id] = ingress.route_path
for source in SOURCE_DEFINITIONS:
    if (
        source.operator_live_ingress is not None
        and source.source_id not in _source_live_ingress_paths
    ):
        _source_live_ingress_paths[source.source_id] = source.operator_live_ingress
SOURCE_LIVE_INGRESS_CATALOG: Mapping[str, str] = MappingProxyType(
    _source_live_ingress_paths
)
NON_SOURCE_CHANNEL_CATALOG: Mapping[str, NonSourceChannelDefinition] = (
    _NON_SOURCE_CHANNEL_BY_NAME
)

_normalizer_binding_index: dict[str, str] = {
    channel: binding
    for source in SOURCE_DEFINITIONS
    for channel, binding in source.normalization_contracts()
}
_normalizer_binding_index.update(
    {
        definition.channel: definition.normalizer_binding
        for definition in NON_SOURCE_CHANNEL_DEFINITIONS
        if definition.normalizer_binding is not None
    }
)
NORMALIZER_BINDING_CATALOG: Mapping[str, str] = MappingProxyType(
    _normalizer_binding_index
)

_channel_trust_index: dict[str, AllowedTrustTier] = {
    channel: source.default_trust_tier
    for source in SOURCE_DEFINITIONS
    for channel in source.normalization_inputs
}
_channel_trust_index.update(
    {
        definition.channel: definition.default_trust_tier
        for definition in NON_SOURCE_CHANNEL_DEFINITIONS
    }
)
CHANNEL_TRUST_CATALOG: Mapping[str, AllowedTrustTier] = MappingProxyType(
    _channel_trust_index
)


def resolve_source_id(name: str) -> str:
    """Resolve a canonical source name, display name, or retained alias."""

    normalized = normalize_catalog_name(name)
    try:
        return _SOURCE_NAME_INDEX[normalized]
    except KeyError as exc:
        raise KeyError(f"unknown ingestion source name {name!r}") from exc


def resolve_provider_id(name: str) -> str:
    """Resolve a canonical provider name, display name, or retained alias."""

    normalized = normalize_catalog_name(name)
    try:
        return _PROVIDER_NAME_INDEX[normalized]
    except KeyError as exc:
        raise KeyError(f"unknown ingestion provider name {name!r}") from exc


def source_definition(name: str) -> SourceDefinition:
    """Return a source definition by canonical ID, display name, or alias."""

    return _SOURCE_BY_ID[resolve_source_id(name)]


def source_connection_definition(ui_slug: str) -> SourceDefinition:
    """Return the immutable source contract for an exact UI/CLI slug."""

    try:
        return SOURCE_CONNECTION_CATALOG[ui_slug]
    except KeyError as exc:
        raise KeyError(f"unknown source connection slug {ui_slug!r}") from exc


def source_connection_profile(ui_slug: str) -> dict[str, object]:
    """Build a fresh legacy-compatible source connection profile."""

    return source_connection_definition(ui_slug).onboarding.as_cli_source_profile()


def source_local_rehearsal_profile(
    ui_slug: str,
) -> dict[str, object] | None:
    """Build a fresh explicit rehearsal override, if one is declared."""

    rehearsal = source_connection_definition(ui_slug).onboarding.local_rehearsal
    return rehearsal.as_payload() if rehearsal is not None else None


def provider_definition(name: str) -> ProviderDefinition:
    """Return a provider definition by canonical ID, display name, or alias."""

    return _PROVIDER_BY_ID[resolve_provider_id(name)]


def oauth_ingress_definition(source_name: str) -> OAuthIngressDefinition:
    """Return the exact OAuth/App-install ingress for a source."""

    source_id = resolve_source_id(source_name)
    try:
        return _OAUTH_INGRESS_BY_SOURCE[source_id]
    except KeyError as exc:
        raise KeyError(f"source {source_id!r} has no OAuth callback ingress") from exc


def live_ingress_endpoint(source_name: str) -> str | None:
    """Return a public route or operator runtime used for live ingress."""

    source_id = resolve_source_id(source_name)
    return SOURCE_LIVE_INGRESS_CATALOG.get(source_id)


def webhook_ingress_definition(route_id: str) -> WebhookIngressDefinition:
    """Return the exact public webhook-route contract."""

    try:
        return _WEBHOOK_INGRESS_BY_ROUTE[route_id]
    except KeyError as exc:
        raise KeyError(f"unknown provider webhook route {route_id!r}") from exc


def provider_for_webhook_route(route_id: str) -> ProviderDefinition:
    """Return the canonical provider owning a public webhook route."""

    try:
        return _PROVIDER_BY_WEBHOOK_ROUTE[route_id]
    except KeyError as exc:
        raise KeyError(f"unknown provider webhook route {route_id!r}") from exc


def dedicated_ingress_definition(
    ingress_id: str,
) -> DedicatedIngressDefinition:
    """Return one provider-specific public ingress contract."""

    try:
        return _DEDICATED_INGRESS_BY_ID[ingress_id]
    except KeyError as exc:
        raise KeyError(f"unknown dedicated ingress {ingress_id!r}") from exc


def provider_for_dedicated_ingress(
    ingress_id: str,
) -> ProviderDefinition:
    """Return the canonical provider owning a dedicated ingress."""

    try:
        return _PROVIDER_BY_DEDICATED_INGRESS[ingress_id]
    except KeyError as exc:
        raise KeyError(f"unknown dedicated ingress {ingress_id!r}") from exc


def sources_for_provider(name: str) -> tuple[SourceDefinition, ...]:
    """Return a provider's sources in canonical deterministic source order."""

    provider = provider_definition(name)
    return tuple(_SOURCE_BY_ID[source_id] for source_id in provider.source_ids)


def request_policy_for_operation(
    provider_name: str,
    operation_id: str,
) -> RequestPolicy:
    """Resolve the policy explicitly assigned to a provider operation."""

    provider = provider_definition(provider_name)
    policies = tuple(
        source.request_policy_for_operation(operation_id)
        for source in sources_for_provider(provider.provider_id)
        if operation_id in source.operation_policy_ids
    )
    if not policies:
        raise KeyError(
            f"provider {provider.provider_id!r} has no request policy for "
            f"operation {operation_id!r}"
        )
    if any(policy != policies[0] for policy in policies[1:]):
        raise CatalogValidationError(
            f"provider {provider.provider_id!r} has conflicting source-owned "
            f"policies for operation {operation_id!r}"
        )
    return policies[0]


def effective_request_policy(
    source_name: str,
    operation_id: str,
) -> RequestPolicy:
    """Resolve one exact source-owned operation policy."""

    source = source_definition(source_name)
    return source.request_policy_for_operation(operation_id)


def normalizer_binding_for_channel(channel: str) -> str:
    """Return the immutable normalizer binding declared for ``channel``."""

    try:
        return NORMALIZER_BINDING_CATALOG[channel]
    except KeyError as exc:
        raise KeyError(f"unknown normalization channel {channel!r}") from exc


def normalizer_channels() -> tuple[str, ...]:
    """Return every routable channel in deterministic lexical order."""

    return tuple(sorted(NORMALIZER_BINDING_CATALOG))


def channel_trust_tier(channel: str) -> AllowedTrustTier:
    """Return the contract-declared default trust tier for ``channel``."""

    try:
        return CHANNEL_TRUST_CATALOG[channel]
    except KeyError as exc:
        raise KeyError(f"unknown trust-classified channel {channel!r}") from exc


__all__ = [
    "CANONICAL_PROVIDER_IDS",
    "CANONICAL_SOURCE_IDS",
    "CHANNEL_TRUST_CATALOG",
    "CatalogValidationError",
    "DEDICATED_INGRESS_CATALOG",
    "DEDICATED_INGRESS_DEFINITIONS",
    "INSTALLATION_MANAGEMENT_CATALOG",
    "NON_SOURCE_CHANNEL_CATALOG",
    "NON_SOURCE_CHANNEL_DEFINITIONS",
    "NORMALIZER_BINDING_CATALOG",
    "OAUTH_INGRESS_CATALOG",
    "PROVIDER_CATALOG",
    "PROVIDER_DEFINITIONS",
    "PROVIDER_TRANSPORT_OPERATION_CATALOG",
    "SOURCE_CATALOG",
    "SOURCE_CONNECTION_CATALOG",
    "SOURCE_CONNECTION_SLUGS",
    "SOURCE_DEFINITIONS",
    "SOURCE_OPERATION_POLICY_CATALOG",
    "SOURCE_LIVE_INGRESS_CATALOG",
    "WEBHOOK_INGRESS_CATALOG",
    "WEBHOOK_INGRESS_ROUTE_IDS",
    "channel_trust_tier",
    "dedicated_ingress_definition",
    "effective_request_policy",
    "normalizer_binding_for_channel",
    "normalizer_channels",
    "oauth_ingress_definition",
    "live_ingress_endpoint",
    "provider_definition",
    "provider_for_dedicated_ingress",
    "provider_for_webhook_route",
    "request_policy_for_operation",
    "resolve_provider_id",
    "resolve_source_id",
    "source_definition",
    "source_connection_definition",
    "source_connection_profile",
    "source_local_rehearsal_profile",
    "sources_for_provider",
    "validate_channel_catalog",
    "validate_catalog",
    "validate_provider_ingress_catalog",
    "webhook_ingress_definition",
]
