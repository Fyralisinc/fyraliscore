"""Provider-edge contract parity and legacy-registry ratchets."""

from __future__ import annotations

import inspect
from dataclasses import FrozenInstanceError, replace

import pytest

from services.app.webhooks import router, tenant_resolver
from services.app.webhooks import signatures
from services.app.webhooks.signatures import verifier_for_provider
from services.app.webhooks.tenant_resolver import (
    tenant_extractor_for_provider,
)
from services.ingest.source_contract import (
    DEDICATED_INGRESS_CATALOG,
    PROVIDER_DEFINITIONS,
    SOURCE_CATALOG,
    WEBHOOK_INGRESS_CATALOG,
    provider_for_webhook_route,
    provider_for_dedicated_ingress,
    resolve_callable_reference,
    resolve_webhook_ingress_metadata_builder,
    resolve_webhook_secret_loader,
    resolve_webhook_verified_pre_tenant_handler,
    resolve_webhook_verified_tenant_handler,
    validate_provider_ingress_catalog,
)
from services.ingest.source_contract.catalog import (
    NON_SOURCE_CHANNEL_DEFINITIONS,
)


_EXPECTED_PROVIDER_EDGE = {
    "slack": (
        "slack",
        "slack",
        "slack:message",
        "flagged_kafka_first_with_inline_fallback",
    ),
    "github": (
        "github",
        "github",
        "github:webhook",
        "flagged_kafka_first_with_inline_fallback",
    ),
    "discord": (
        "discord",
        "discord",
        "discord:interaction",
        "inline_then_shadow",
    ),
    "notion": (
        "notion",
        "notion",
        "notion:object",
        "dedicated_kafka_first_with_inline_fallback",
    ),
    "jira": (
        "atlassian",
        "jira",
        "jira:issue",
        "flagged_kafka_first_with_inline_fallback",
    ),
    "mercury": (
        "mercury",
        "mercury",
        "mercury:transaction",
        "flagged_kafka_first_with_inline_fallback",
    ),
    "quickbooks": (
        "intuit",
        "quickbooks",
        "quickbooks:object",
        "flagged_kafka_first_with_inline_fallback",
    ),
    "grafana": (
        "grafana",
        "grafana",
        "grafana:alert",
        "flagged_kafka_first_with_inline_fallback",
    ),
    "brex": (
        "brex",
        "brex",
        "brex:transaction",
        "flagged_kafka_first_with_inline_fallback",
    ),
    "ramp": (
        "ramp",
        "ramp",
        "ramp:transaction",
        "flagged_kafka_first_with_inline_fallback",
    ),
    "gusto": (
        "gusto",
        "gusto",
        "gusto:object",
        "flagged_kafka_first_with_inline_fallback",
    ),
    "deel": (
        "deel",
        "deel",
        "deel:payment",
        "flagged_kafka_first_with_inline_fallback",
    ),
    "fireflies": (
        "fireflies",
        "fireflies",
        "fireflies:transcript",
        "flagged_kafka_first_with_inline_fallback",
    ),
    "figma": (
        "figma",
        "figma",
        "figma:event",
        "flagged_kafka_first_with_inline_fallback",
    ),
    "hibob": (
        "hibob",
        "hibob",
        "hibob:object",
        "flagged_kafka_first_with_inline_fallback",
    ),
    "ashby": (
        "ashby",
        "ashby",
        "ashby:object",
        "flagged_kafka_first_with_inline_fallback",
    ),
    "linear": ("linear", None, "linear:webhook", "inline_only"),
    "stripe": ("stripe", None, "stripe:webhook", "inline_only"),
}

_EXPECTED_DEDICATED_INGRESS = {
    "gmail_pubsub": (
        "google",
        "gmail",
        "/webhooks/gmail/pubsub",
        ("POST",),
        None,
        "google_oidc",
        "subscription_installation",
        "ack_and_reconcile_on_failure",
        "hydrated_messages_handler_managed",
    ),
    "google_calendar_push": (
        "google",
        "google_calendar",
        "/webhooks/google_calendar/push",
        ("POST",),
        "google_calendar:event",
        "google_watch_channel_token",
        "watch_channel_installation",
        "ack_and_reconcile_on_failure",
        "reconciled_delta_drain",
    ),
    "google_drive_push": (
        "google",
        "google_drive",
        "/webhooks/google_drive/push",
        ("POST",),
        "google_drive:file",
        "google_watch_channel_token",
        "watch_channel_installation",
        "ack_and_reconcile_on_failure",
        "reconciled_delta_drain",
    ),
    "whatsapp_webhook": (
        "meta",
        "whatsapp",
        "/integrations/whatsapp/webhook",
        ("GET", "POST"),
        "whatsapp:message",
        "meta_verify_token_hmac",
        "phone_number_installation",
        "durable_or_inline_before_ack",
        "flagged_kafka_first_with_inline_fallback",
    ),
    "facebook_pages_webhook": (
        "meta",
        "facebook_pages",
        "/integrations/facebook_pages/webhook",
        ("GET", "POST"),
        "facebook_pages:message",
        "meta_verify_token_hmac",
        "page_installation",
        "durable_or_inline_before_ack",
        "flagged_kafka_first_with_inline_fallback",
    ),
}


def test_provider_edge_contract_preserves_all_current_routes() -> None:
    actual = {
        route_id: (
            provider_for_webhook_route(route_id).provider_id,
            ingress.source_id,
            ingress.channel,
            ingress.kafka_mode,
        )
        for route_id, ingress in WEBHOOK_INGRESS_CATALOG.items()
    }
    assert actual == _EXPECTED_PROVIDER_EDGE


def test_every_webhook_binding_resolves_from_the_contract() -> None:
    for route_id in WEBHOOK_INGRESS_CATALOG:
        assert verifier_for_provider(route_id) is not None
        assert tenant_extractor_for_provider(route_id) is not None
        assert callable(resolve_webhook_ingress_metadata_builder(route_id))
        assert callable(resolve_webhook_secret_loader(route_id))

    assert verifier_for_provider("unknown") is None
    assert tenant_extractor_for_provider("unknown") is None


def test_dedicated_ingress_contract_preserves_all_current_routes() -> None:
    actual = {
        ingress_id: (
            provider_for_dedicated_ingress(ingress_id).provider_id,
            ingress.source_id,
            ingress.route_path,
            ingress.methods,
            ingress.channel,
            ingress.verification_policy,
            ingress.tenant_binding_policy,
            ingress.acknowledgement_policy,
            ingress.kafka_mode,
        )
        for ingress_id, ingress in DEDICATED_INGRESS_CATALOG.items()
    }
    assert actual == _EXPECTED_DEDICATED_INGRESS


def test_every_dedicated_binding_resolves_from_the_contract() -> None:
    for ingress in DEDICATED_INGRESS_CATALOG.values():
        references = (
            *ingress.verification_bindings,
            ingress.tenant_resolver_binding,
            ingress.dispatcher_binding,
            ingress.router_factory_binding,
        )
        assert all(callable(resolve_callable_reference(ref)) for ref in references)


def test_dedicated_routes_and_methods_are_globally_unique() -> None:
    route_methods = [
        (ingress.route_path, method)
        for ingress in DEDICATED_INGRESS_CATALOG.values()
        for method in ingress.methods
    ]
    assert len(route_methods) == len(set(route_methods))


def test_provider_source_relationships_and_channels_are_exact() -> None:
    for route_id, ingress in WEBHOOK_INGRESS_CATALOG.items():
        provider = provider_for_webhook_route(route_id)
        if ingress.source_id is None:
            assert provider.data_plane is False
            assert route_id in {"linear", "stripe"}
            continue

        source = SOURCE_CATALOG[ingress.source_id]
        assert source.provider_id == provider.provider_id
        assert ingress.source_id in provider.source_ids
        assert ingress.channel == source.channel_for_ingress("webhook")

    for ingress_id, ingress in DEDICATED_INGRESS_CATALOG.items():
        provider = provider_for_dedicated_ingress(ingress_id)
        source = SOURCE_CATALOG[ingress.source_id]
        assert source.provider_id == provider.provider_id
        assert ingress.source_id in provider.source_ids
        if ingress.channel is not None:
            assert ingress.channel in source.normalization_inputs


def test_discord_declares_synchronous_ack_and_forbids_202_cutover() -> None:
    discord = WEBHOOK_INGRESS_CATALOG["discord"]
    assert discord.acknowledgement_policy == "synchronous_provider_response"
    assert discord.kafka_mode == "inline_then_shadow"
    assert discord.kafka_cutover_enabled is False
    assert discord.shadow_write_enabled is True


def test_github_owns_the_only_webhook_handler_header_projection() -> None:
    projected = {
        route_id: ingress.normalizer_header_projection
        for route_id, ingress in WEBHOOK_INGRESS_CATALOG.items()
        if ingress.normalizer_header_projection
    }

    assert projected == {
        "github": (("event_type", "X-GitHub-Event"),),
    }


def test_verified_webhook_policies_are_contract_owned() -> None:
    declared = {
        route_id: (
            ingress.verified_pre_tenant_handler_binding,
            ingress.verified_tenant_handler_binding,
        )
        for route_id, ingress in WEBHOOK_INGRESS_CATALOG.items()
        if ingress.verified_pre_tenant_handler_binding is not None
        or ingress.verified_tenant_handler_binding is not None
    }

    assert declared == {
        "slack": (
            "services.ingest.integrations.slack.webhook_ingress:"
            "handle_verified_pre_tenant",
            "services.ingest.integrations.slack.webhook_ingress:"
            "handle_verified_tenant",
        ),
        "github": (
            "services.ingest.integrations.github.webhook_ingress:"
            "handle_verified_pre_tenant",
            "services.ingest.integrations.github.webhook_ingress:"
            "handle_verified_tenant",
        ),
        "quickbooks": (
            None,
            "services.ingest.integrations.quickbooks.webhook_ingress:"
            "handle_verified_tenant",
        ),
    }
    assert callable(resolve_webhook_verified_pre_tenant_handler("github"))
    assert callable(resolve_webhook_verified_tenant_handler("github"))
    assert callable(resolve_webhook_verified_pre_tenant_handler("slack"))
    assert callable(resolve_webhook_verified_tenant_handler("slack"))
    assert callable(resolve_webhook_verified_tenant_handler("quickbooks"))
    assert resolve_webhook_verified_pre_tenant_handler("discord") is None
    assert resolve_webhook_verified_tenant_handler("discord") is None


def test_webhook_secret_loader_scope_is_contract_owned() -> None:
    app_scoped = {
        route_id: ingress.secret_loader_binding
        for route_id, ingress in WEBHOOK_INGRESS_CATALOG.items()
        if ingress.secret_loader_binding
        != "services.app.webhooks.secrets:load_installation_secrets"
    }

    assert app_scoped == {
        "github": (
            "services.ingest.integrations.github.webhook_secrets:"
            "load_app_webhook_secrets"
        ),
        "notion": (
            "services.ingest.integrations.notion.webhook_secrets:"
            "load_app_webhook_secrets"
        ),
    }


def test_webhook_verified_policy_rejects_malformed_or_dedicated_bindings() -> None:
    github = WEBHOOK_INGRESS_CATALOG["github"]
    with pytest.raises(ValueError, match="module:callable"):
        replace(
            github,
            verified_pre_tenant_handler_binding="github.pre_tenant",
        )
    with pytest.raises(ValueError, match="module:callable"):
        replace(
            github,
            secret_loader_binding="github.webhook_secret_loader",
        )

    notion = WEBHOOK_INGRESS_CATALOG["notion"]
    with pytest.raises(ValueError, match="dedicated webhook ingress"):
        replace(
            notion,
            verified_tenant_handler_binding=(
                "services.ingest.integrations.github.webhook_ingress:"
                "handle_verified_tenant"
            ),
        )


@pytest.mark.parametrize(
    "projection",
    (
        (("event_type", ""),),
        (("Event Type", "X-GitHub-Event"),),
        (("event_type", "bad header"),),
        (("event_type", "X-GitHub-Event"), ("event_type", "X-Other")),
        (("event_type", "X-GitHub-Event"), ("delivery_id", "x-github-event")),
    ),
)
def test_webhook_handler_header_projection_rejects_malformed_contracts(
    projection: tuple[tuple[str, str], ...],
) -> None:
    with pytest.raises((TypeError, ValueError)):
        replace(
            WEBHOOK_INGRESS_CATALOG["github"],
            normalizer_header_projection=projection,
        )


def test_dedicated_notion_and_ingress_only_provider_semantics() -> None:
    notion = WEBHOOK_INGRESS_CATALOG["notion"]
    assert notion.handler_mode == "dedicated"
    assert notion.acknowledgement_policy == "dedicated_handler"
    assert notion.kafka_mode == "dedicated_kafka_first_with_inline_fallback"
    assert notion.shadow_write_enabled is True
    assert notion.kafka_cutover_enabled is False
    assert notion.inline_fallback_enabled is True
    assert notion.verification_handshake_binding is not None
    assert notion.verification_handshake_handler_binding is not None
    assert notion.dedicated_handler_binding is not None

    for route_id in ("linear", "stripe"):
        ingress = WEBHOOK_INGRESS_CATALOG[route_id]
        assert ingress.source_id is None
        assert ingress.kafka_mode == "inline_only"
        assert ingress.inline_fallback_enabled is False


def test_dedicated_kafka_fallback_mode_cannot_be_used_by_generic_webhooks() -> None:
    github = WEBHOOK_INGRESS_CATALOG["github"]
    with pytest.raises(ValueError, match="dedicated Kafka mode"):
        replace(
            github,
            kafka_mode="dedicated_kafka_first_with_inline_fallback",
        )

    notion = WEBHOOK_INGRESS_CATALOG["notion"]
    with pytest.raises(ValueError, match="Kafka-first inline-fallback"):
        replace(notion, kafka_mode="inline_only")


def test_provider_edge_catalog_and_entries_are_immutable() -> None:
    with pytest.raises(TypeError):
        WEBHOOK_INGRESS_CATALOG["other"] = WEBHOOK_INGRESS_CATALOG["slack"]  # type: ignore[index]
    with pytest.raises(FrozenInstanceError):
        WEBHOOK_INGRESS_CATALOG["slack"].channel = "other:channel"  # type: ignore[misc]
    with pytest.raises(TypeError):
        DEDICATED_INGRESS_CATALOG["other"] = DEDICATED_INGRESS_CATALOG["gmail_pubsub"]  # type: ignore[index]
    with pytest.raises(FrozenInstanceError):
        DEDICATED_INGRESS_CATALOG["gmail_pubsub"].route_path = "/other"  # type: ignore[misc]


def test_webhook_route_parameters_must_be_declared_single_segments() -> None:
    ashby = WEBHOOK_INGRESS_CATALOG["ashby"]

    with pytest.raises(ValueError, match="single-segment"):
        replace(
            ashby,
            route_path="/webhooks/ashby/{installation_id:path}",
        )
    with pytest.raises(ValueError, match="require tenant_binding"):
        replace(ashby, tenant_binding="payload")
    with pytest.raises(ValueError, match="requires a declared"):
        replace(
            ashby,
            route_path="/webhooks/ashby",
        )


def test_validator_rejects_duplicate_public_routes() -> None:
    github = PROVIDER_DEFINITIONS[1]
    conflicting_ingress = replace(
        github.webhook_ingresses[0],
        route_id="slack",
        route_path="/webhooks/slack",
    )
    conflicting_provider = replace(
        github,
        webhook_ingresses=(conflicting_ingress,),
    )
    providers = (
        PROVIDER_DEFINITIONS[0],
        conflicting_provider,
        *PROVIDER_DEFINITIONS[2:],
    )

    with pytest.raises(ValueError, match="declared by both"):
        validate_provider_ingress_catalog(
            providers,
            tuple(SOURCE_CATALOG.values()),
            NON_SOURCE_CHANNEL_DEFINITIONS,
        )


def test_validator_rejects_duplicate_dedicated_route_methods() -> None:
    google = PROVIDER_DEFINITIONS[3]
    gmail, calendar, drive = google.dedicated_ingresses
    conflicting_drive = replace(
        drive,
        route_path=calendar.route_path,
    )
    conflicting_google = replace(
        google,
        dedicated_ingresses=(gmail, calendar, conflicting_drive),
    )
    providers = (
        *PROVIDER_DEFINITIONS[:3],
        conflicting_google,
        *PROVIDER_DEFINITIONS[4:],
    )

    with pytest.raises(ValueError, match="route POST.*declared by both"):
        validate_provider_ingress_catalog(
            providers,
            tuple(SOURCE_CATALOG.values()),
            NON_SOURCE_CHANNEL_DEFINITIONS,
        )


def test_legacy_provider_edge_registries_are_absent() -> None:
    for name in (
        "_PROVIDER_TO_SHADOW_SOURCE",
        "_CUTOVER_ENABLED_PROVIDERS",
        "_PROVIDER_CHANNEL",
    ):
        assert not hasattr(router, name)
    assert not hasattr(signatures, "VERIFIERS")
    assert not hasattr(tenant_resolver, "PROVIDER_EXTRACTORS")
    assert not hasattr(tenant_resolver, "_PATH_RESOLVED_PROVIDERS")


def test_shared_router_has_no_github_verified_policy_switch() -> None:
    source = inspect.getsource(router)
    assert 'provider == "github"' not in source
    assert 'provider != "github"' not in source
    for helper_name in (
        "_is_github_ping",
        "_handle_github_lifecycle",
        "_load_github_selected_repositories",
    ):
        assert not hasattr(router, helper_name)


def test_shared_router_has_no_slack_verified_policy_switch() -> None:
    source = inspect.getsource(router)
    assert "slack" not in source.casefold()
    assert 'provider == "slack"' not in source
    assert 'provider != "slack"' not in source
    assert "slack_url_verification" not in source
    for helper_name in (
        "_is_slack_url_verification",
        "_slack_lifecycle_event",
        "_handle_slack_lifecycle",
    ):
        assert not hasattr(router, helper_name)
