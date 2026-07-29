from __future__ import annotations

from dataclasses import replace
from typing import get_args

import pytest

from lib.shared.provider_transport import RetrySafety
from services.ingest.ingestion.raw_tier.envelope import SourceLiteral
from services.ingest.source_contract.catalog import (
    CANONICAL_PROVIDER_IDS,
    CANONICAL_SOURCE_IDS,
    CHANNEL_TRUST_CATALOG,
    CatalogValidationError,
    NON_SOURCE_CHANNEL_CATALOG,
    NON_SOURCE_CHANNEL_DEFINITIONS,
    NORMALIZER_BINDING_CATALOG,
    PROVIDER_CATALOG,
    PROVIDER_DEFINITIONS,
    PROVIDER_ENDPOINT_CATALOG,
    PROVIDER_ENDPOINT_OPERATION_CATALOG,
    PROVIDER_TRANSPORT_OPERATION_CATALOG,
    SOURCE_CATALOG,
    SOURCE_DEFINITIONS,
    SOURCE_OPERATION_ENDPOINT_CATALOG,
    SOURCE_OPERATION_POLICY_CATALOG,
    effective_request_policy,
    operation_endpoint_owner,
    provider_definition,
    request_policy_for_operation,
    resolve_provider_id,
    resolve_source_id,
    source_definition,
    sources_for_provider,
    validate_channel_catalog,
    validate_catalog,
)
from services.ingest.source_contract.models import (
    Certification,
    IngressRoute,
    OutboundEndpointBinding,
)


def test_catalog_covers_the_27_legacy_canonical_sources_in_wire_order() -> None:
    assert CANONICAL_SOURCE_IDS == tuple(get_args(SourceLiteral))
    assert tuple(SOURCE_CATALOG) == CANONICAL_SOURCE_IDS
    assert tuple(source.source_id for source in SOURCE_DEFINITIONS) == (
        CANONICAL_SOURCE_IDS
    )
    assert len(SOURCE_DEFINITIONS) == 27


def test_catalog_requires_one_contiguous_source_display_order() -> None:
    slack = SOURCE_DEFINITIONS[0]
    github = SOURCE_DEFINITIONS[1]
    duplicate_order = replace(
        github,
        display=replace(github.display, order=slack.display.order),
    )

    with pytest.raises(CatalogValidationError, match="source display order"):
        validate_catalog(
            PROVIDER_DEFINITIONS,
            (slack, duplicate_order, *SOURCE_DEFINITIONS[2:]),
        )


def test_provider_catalog_is_deterministic_and_relationally_complete() -> None:
    assert tuple(PROVIDER_CATALOG) == CANONICAL_PROVIDER_IDS
    assert tuple(provider.provider_id for provider in PROVIDER_DEFINITIONS) == (
        CANONICAL_PROVIDER_IDS
    )
    assert tuple(
        source.source_id for source in sources_for_provider("google workspace")
    ) == ("gmail", "google_calendar", "google_drive")
    assert tuple(source.source_id for source in sources_for_provider("Meta")) == (
        "whatsapp",
        "facebook_pages",
    )
    assert sources_for_provider("Linear") == ()
    assert sources_for_provider("Stripe") == ()


def test_ingress_only_providers_do_not_invent_data_plane_sources() -> None:
    for provider_id in ("linear", "stripe"):
        provider = provider_definition(provider_id)
        assert provider.data_plane is False
        assert provider.source_ids == ()
        assert provider.operation_policy_ids == ()


@pytest.mark.parametrize(
    ("name", "expected"),
    [
        ("GitHub", "github"),
        ("git-hub", "github"),
        ("Google Calendar", "google_calendar"),
        ("gcal", "google_calendar"),
        ("Google Drive", "google_drive"),
        ("gdrive", "google_drive"),
        ("QuickBooks Online", "quickbooks"),
        ("qbo", "quickbooks"),
        ("WhatsApp Cloud", "whatsapp"),
        ("Facebook Messenger", "facebook_pages"),
        ("Linked-In", "linkedin"),
    ],
)
def test_source_aliases_resolve_to_canonical_ids(name: str, expected: str) -> None:
    assert resolve_source_id(name) == expected
    assert source_definition(name).source_id == expected


def test_every_declared_source_and_provider_alias_is_retained() -> None:
    for source in SOURCE_DEFINITIONS:
        assert resolve_source_id(source.source_id) == source.source_id
        assert resolve_source_id(source.display_name) == source.source_id
        for alias in source.aliases:
            assert resolve_source_id(alias) == source.source_id
    for provider in PROVIDER_DEFINITIONS:
        assert resolve_provider_id(provider.provider_id) == provider.provider_id
        assert resolve_provider_id(provider.display_name) == provider.provider_id
        for alias in provider.aliases:
            assert resolve_provider_id(alias) == provider.provider_id


def test_whatsapp_explicitly_has_no_history_contract() -> None:
    whatsapp = source_definition("whatsapp")
    assert whatsapp.history is None
    assert whatsapp.planner_binding is None
    assert whatsapp.fetcher_binding is None
    assert whatsapp.reconciler_binding is None
    assert whatsapp.provider_transport_enforced is True
    assert whatsapp.operation_policy_ids == ()
    assert whatsapp.capability_flags == ("no_outbound_provider_requests",)
    assert tuple(
        source.source_id for source in SOURCE_DEFINITIONS if source.history is None
    ) == ("whatsapp",)


def test_every_historical_source_has_all_three_history_bindings() -> None:
    for source in SOURCE_DEFINITIONS:
        if source.history is None:
            continue
        assert source.planner_binding
        assert source.fetcher_binding
        assert source.reconciler_binding


def test_normalization_and_live_contracts_are_explicit_for_every_source() -> None:
    for source in SOURCE_DEFINITIONS:
        assert source.normalization_contracts()
        assert source.allowed_observation_kinds
        assert source.trust_tiers
        assert source.installation_identifiers
        assert source.runtime_identifiers
        assert source.live_contracts()
        transport_owners = source.live_runtime.transport_owners()
        assert len(transport_owners) == len(source.live_transports)
        for transport, ack, delivery in source.live_contracts():
            assert sum(
                owner.transport == transport for owner in transport_owners
            ) == 1
            assert ack.endswith(("ack", "durable"))
            assert delivery in {
                "at_least_once",
                "replayable_stream",
                "replayable_pull",
            }


def test_live_runtime_declares_real_deployment_and_managed_boundaries() -> None:
    from services.platform.runtime.process_manifest import production_processes

    production_units = {process.name for process in production_processes()}
    managed_workers = []
    for source in SOURCE_DEFINITIONS:
        for boundary in source.live_runtime.ingress_boundaries:
            assert boundary.deployment_unit == "gateway"
            assert boundary.deployment_unit in production_units
        for worker in source.live_runtime.workers:
            if worker.deployment_owner == "customer_deployment":
                managed_workers.append((source.source_id, worker.component_id))
                assert worker.launcher_binding is None
                assert worker.dispatch_binding
                assert worker.managed_reason
                continue
            assert worker.launcher_binding
            assert worker.deployment_unit in production_units
            if worker.role in {
                "incremental_poll",
                "watch_renewal",
                "credential_renewal",
                "periodic_reconciliation",
            }:
                assert worker.cadence_seconds
            assert worker.lease_scope != "none"

    assert managed_workers == [("aws", "aws_queue_consumer")]


def test_every_historical_source_declares_periodic_reconciliation_runtime() -> None:
    for source in SOURCE_DEFINITIONS:
        periodic = tuple(
            worker
            for worker in source.live_runtime.workers
            if worker.role == "periodic_reconciliation"
        )
        assert len(periodic) == (0 if source.history is None else 1)
        if periodic:
            assert periodic[0].cadence_seconds == 6 * 60 * 60
            assert periodic[0].deployment_unit == "periodic_reconciler"


def test_google_push_sources_declare_watch_renewal_workers() -> None:
    expected = {
        "gmail": ("gmail_watch_scheduler", 15 * 60),
        "google_calendar": ("google_calendar_watch_scheduler", 15 * 60),
        "google_drive": ("google_drive_watch_scheduler", 15 * 60),
    }
    expected_supporting_operations = {
        "gmail": ("dwd.token.exchange",),
        "google_calendar": ("dwd.token.exchange", "channels.stop"),
        "google_drive": ("dwd.token.exchange", "channels.stop"),
    }
    for source_id, (component_id, cadence) in expected.items():
        source = source_definition(source_id)
        worker = source.live_runtime.worker(component_id)
        assert worker.role == "watch_renewal"
        assert worker.transport is None
        assert worker.cadence_seconds == cadence
        assert worker.lease_scope == "resource"
        assert source.renewal is not None
        assert source.renewal.kind == "watch"
        # A cold DWD cache mints a credential before the watch request. The
        # bounded renewal contract must declare that conditional child call so
        # load/certification accounting cannot undercount provider quota.
        assert (
            source.renewal.supporting_operation_ids
            == expected_supporting_operations[source_id]
        )


def test_credential_renewal_sources_declare_the_shared_fyralis_worker() -> None:
    credential_sources = tuple(
        source
        for source in SOURCE_DEFINITIONS
        if source.renewal is not None and source.renewal.kind == "credential"
    )

    assert len(credential_sources) == 5
    for source in credential_sources:
        assert source.credential_refresh is not None
        worker = source.live_runtime.worker("credential_renewal")
        assert worker.role == "credential_renewal"
        assert worker.transport is None
        assert worker.lease_scope == "installation"
        assert worker.cadence_seconds == source.renewal.cadence_seconds
        assert worker.deployment_unit == "credential_renewal_scheduler"


def test_onboarding_contracts_cover_all_sources_and_routes() -> None:
    route_paths: list[str] = []
    for source in SOURCE_DEFINITIONS:
        onboarding = source.onboarding
        assert onboarding.method
        assert onboarding.discovery_target
        assert onboarding.native_connect.payload_fields
        assert onboarding.native_connect.as_payload()["kind"]
        paths = onboarding.native_connect.route_paths()
        assert paths
        assert all(
            path.startswith(f"/integrations/{source.source_id}/") for path in paths
        )
        route_paths.extend(paths)

    assert len(route_paths) == len(set(route_paths))


def test_catalog_rejects_native_connect_route_owned_by_another_source() -> None:
    github = source_definition("github")
    invalid_native_connect = replace(
        github.onboarding.native_connect,
        preflight_path="/integrations/slack/connect/preflight",
    )
    sources = tuple(
        replace(
            source,
            onboarding=replace(
                source.onboarding,
                native_connect=invalid_native_connect,
            ),
        )
        if source is github
        else source
        for source in SOURCE_DEFINITIONS
    )

    with pytest.raises(CatalogValidationError, match="outside"):
        validate_catalog(PROVIDER_DEFINITIONS, sources)


def test_all_67_ingress_routes_are_owned_by_the_source_contract() -> None:
    routes = {
        (source.source_id, route.ingress_kind): route.channel
        for source in SOURCE_DEFINITIONS
        for route in source.ingress_routes
    }

    assert len(routes) == 67
    assert routes[("discord", "gateway")] == "discord:message"
    assert routes[("discord", "webhook")] == "discord:interaction"
    assert routes[("grafana", "backfill")] == "grafana:annotation"
    assert routes[("grafana", "webhook")] == "grafana:alert"
    assert routes[("figma", "backfill")] == "figma:event"
    assert ("gmail", "pubsub") not in routes

    for source in SOURCE_DEFINITIONS:
        assert all(
            route.channel in source.normalization_inputs
            for route in source.ingress_routes
        )
        assert source.channel_for_ingress("not-supported") is None
        if source.history is None:
            assert source.channel_for_ingress("backfill") is None
        else:
            assert source.channel_for_ingress("backfill") is not None


def test_miro_is_poll_only_and_google_calendar_owns_watch_push_metadata() -> None:
    miro = source_definition("miro")
    google_calendar = source_definition("google_calendar")

    assert provider_definition("miro").webhook_ingresses == ()
    assert tuple(route.ingress_kind for route in miro.ingress_routes) == (
        "backfill",
        "poll",
    )
    assert miro.live_transports == ("api_poll",)
    assert miro.allowed_observation_kinds == ("signal",)
    assert miro.installation_identifiers == ("org_id", "board_id")
    assert miro.runtime_identifiers == ("org_id", "board_id")
    assert miro.onboarding.native_connect is not None
    assert "org_id" in miro.onboarding.native_connect.payload_fields
    assert "webhook_secret" not in miro.onboarding.native_connect.payload_fields
    assert miro.onboarding.ingress_paths == ()
    assert miro.onboarding.no_ingress_reason is not None
    assert miro.installation_adapter is not None
    assert miro.installation_adapter.management is not None
    assert miro.installation_adapter.management.ref_columns == ("secret_ref",)
    assert (
        miro.installation_adapter.management.webhook_installation_id_column
        is None
    )

    assert google_calendar.onboarding.ingress_paths == (
        "/webhooks/google_calendar/push",
    )
    assert google_calendar.onboarding.no_ingress_reason is None
    assert google_calendar.live_transports == ("webhook", "api_poll")
    assert "events.watch" in google_calendar.operation_policy_ids
    assert "Live events" in google_calendar.display.supported_sync_modes


def test_source_definition_rejects_duplicate_ingress_routes() -> None:
    slack = source_definition("slack")
    with pytest.raises(ValueError, match="duplicate ingress kind"):
        replace(
            slack,
            ingress_routes=(
                IngressRoute("webhook", "slack:message"),
                IngressRoute("webhook", "slack:message"),
                IngressRoute("backfill", "slack:message"),
            ),
        )


def test_source_definition_rejects_ingress_route_to_foreign_channel() -> None:
    slack = source_definition("slack")
    with pytest.raises(ValueError, match="not a normalization input"):
        replace(
            slack,
            ingress_routes=(
                IngressRoute("webhook", "github:webhook"),
                IngressRoute("backfill", "slack:message"),
            ),
        )


def test_source_definition_requires_history_backfill_consistency() -> None:
    slack = source_definition("slack")
    with pytest.raises(ValueError, match="must declare a backfill"):
        replace(
            slack,
            ingress_routes=(IngressRoute("webhook", "slack:message"),),
        )

    whatsapp = source_definition("whatsapp")
    with pytest.raises(ValueError, match="cannot declare a backfill"):
        replace(
            whatsapp,
            ingress_routes=(
                *whatsapp.ingress_routes,
                IngressRoute("backfill", "whatsapp:message"),
            ),
        )


def test_all_normalizers_and_trust_defaults_are_contract_derived() -> None:
    canonical_channels = {
        channel
        for source in SOURCE_DEFINITIONS
        for channel in source.normalization_inputs
    }
    assert canonical_channels <= NORMALIZER_BINDING_CATALOG.keys()
    assert set(NORMALIZER_BINDING_CATALOG) == canonical_channels | {
        "email:inbound",
        "calendar:sync",
        "linear:webhook",
        "stripe:webhook",
        "internal:state_change",
        "internal:anomaly",
        "internal:prediction_resolution",
    }
    assert set(CHANNEL_TRUST_CATALOG) == (
        canonical_channels | set(NON_SOURCE_CHANNEL_CATALOG)
    )
    assert CHANNEL_TRUST_CATALOG["github:webhook"] == "authoritative"
    assert CHANNEL_TRUST_CATALOG["email:inbound"] == "attested_agent"


def test_non_source_channels_have_validated_provider_or_platform_owners() -> None:
    definitions = {
        definition.channel: definition for definition in NON_SOURCE_CHANNEL_DEFINITIONS
    }
    assert definitions["linear:webhook"].owner_kind == "provider"
    assert definitions["stripe:webhook"].owner_kind == "provider"
    assert definitions["internal:state_change"].owner_kind == "platform"
    assert definitions["email:inbound"].normalizer_binding
    assert definitions["calendar:sync"].normalizer_binding


def test_certification_is_required_but_never_self_certifies() -> None:
    declaration_ids: set[str] = set()
    for source in SOURCE_DEFINITIONS:
        certification = source.certification
        assert certification.status == "unverified"
        assert certification.require_test_kit is True
        assert certification.require_evidence is True
        assert certification.require_canary is True
        assert certification.missing_required_declarations() == ()
        for declaration in (
            certification.test_kit_id,
            certification.evidence_id,
            certification.canary_id,
        ):
            assert declaration is not None
            assert declaration not in declaration_ids
            declaration_ids.add(declaration)
    assert len(declaration_ids) == len(SOURCE_DEFINITIONS) * 3


def test_catalog_mappings_are_read_only() -> None:
    with pytest.raises(TypeError):
        SOURCE_CATALOG["other"] = SOURCE_DEFINITIONS[0]  # type: ignore[index]
    with pytest.raises(TypeError):
        PROVIDER_CATALOG["other"] = PROVIDER_DEFINITIONS[0]  # type: ignore[index]
    with pytest.raises(TypeError):
        NORMALIZER_BINDING_CATALOG["other"] = "x:y"  # type: ignore[index]
    with pytest.raises(TypeError):
        CHANNEL_TRUST_CATALOG["other"] = "unvetted"  # type: ignore[index]


def test_effective_request_policy_is_owned_by_the_exact_source_operation() -> None:
    gmail = source_definition("gmail")
    policy = effective_request_policy("gmail", "messages.get")
    assert request_policy_for_operation("google", "messages.get") == policy
    assert policy is gmail.request_policy_for_operation("messages.get")
    assert policy.retry_safety is RetrySafety.IDEMPOTENT
    assert policy.retryable_status_codes == (
        403,
        408,
        425,
        429,
        500,
        502,
        503,
        504,
    )
    assert policy.retryable_error_codes == (
        "provider_rate_limited",
        "provider_timeout",
        "provider_transient_error",
    )
    assert policy.rate_limit_header_parser_id == "http.retry_after"
    with pytest.raises(KeyError, match="no request policy"):
        request_policy_for_operation("stripe", "fetch_page")


def test_every_operation_has_a_fully_declared_immutable_policy() -> None:
    declared_count = 0
    policy_object_ids: set[int] = set()
    for source in SOURCE_DEFINITIONS:
        policies = SOURCE_OPERATION_POLICY_CATALOG[source.source_id]
        assert tuple(policies) == source.operation_policy_ids
        for operation_id, policy in policies.items():
            declared_count += 1
            policy_object_ids.add(id(policy))
            assert operation_id
            assert policy.max_attempts >= 1
            assert policy.timeout_seconds > 0
            assert policy.max_elapsed_seconds > 0
            assert policy.max_concurrency is not None
            assert policy.max_concurrency >= 1
            assert policy.retryable_status_codes is not None
            assert policy.retryable_error_codes is not None
            assert policy.rate_limit_header_parser_id is not None
            if policy.retry_safety is RetrySafety.UNSAFE:
                assert policy.max_attempts == 1
                assert policy.retryable_status_codes == ()
                assert policy.retryable_error_codes == ()
                assert policy.max_concurrency == 1
            else:
                assert policy.max_attempts > 1
        if policies:
            with pytest.raises(TypeError):
                policies["not.real"] = policies[next(iter(policies))]  # type: ignore[index]

    assert declared_count == 166
    assert len(policy_object_ids) == declared_count


@pytest.mark.parametrize(
    ("source_id", "operation_id"),
    [
        ("slack", "chat.postMessage"),
        ("slack", "oauth.v2.access"),
        ("github", "installation_token.mint"),
        ("discord", "/webhooks/{application_id}/{interaction_token}"),
        ("discord", "/oauth2/token"),
        ("gmail", "watch.create"),
        ("google_calendar", "events.watch"),
        ("google_drive", "changes.watch"),
        ("quickbooks", "oauth.token.refresh"),
        ("figma", "oauth.token.refresh"),
        ("facebook_pages", "oauth.token.exchange"),
        ("facebook_pages", "oauth.user_token.extend"),
    ],
)
def test_unproven_side_effects_are_one_attempt_unsafe(
    source_id: str,
    operation_id: str,
) -> None:
    policy = effective_request_policy(source_id, operation_id)
    assert policy.retry_safety is RetrySafety.UNSAFE
    assert policy.max_attempts == 1


def test_transport_operation_catalog_is_derived_from_source_contracts() -> None:
    assert tuple(PROVIDER_TRANSPORT_OPERATION_CATALOG) == (
        "slack",
        "github",
        "discord",
        "gmail",
        "notion",
        "google_calendar",
        "google_drive",
        "jira",
        "mercury",
        "quickbooks",
        "grafana",
        "telegram",
        "brex",
        "ramp",
        "gusto",
        "deel",
        "fireflies",
        "signal",
        "aws",
        "miro",
        "figma",
        "carta",
        "hibob",
        "ashby",
        "linkedin",
        "facebook_pages",
    )
    for source_id, operations in PROVIDER_TRANSPORT_OPERATION_CATALOG.items():
        source = source_definition(source_id)
        assert operations == frozenset(source.operation_policy_ids)
    assert all(source.provider_transport_enforced for source in SOURCE_DEFINITIONS)
    assert "whatsapp" not in PROVIDER_TRANSPORT_OPERATION_CATALOG


def test_every_operation_has_exact_endpoint_ownership() -> None:
    assert len(PROVIDER_ENDPOINT_CATALOG) == 27
    assert all(PROVIDER_ENDPOINT_OPERATION_CATALOG.values())
    assert (
        sum(len(owners) for owners in SOURCE_OPERATION_ENDPOINT_CATALOG.values()) == 166
    )
    assert (
        sum(len(owners) for owners in PROVIDER_ENDPOINT_OPERATION_CATALOG.values())
        == 143
    )

    for source in SOURCE_DEFINITIONS:
        owners = SOURCE_OPERATION_ENDPOINT_CATALOG[source.source_id]
        assert tuple(owners) == source.operation_policy_ids

    assert operation_endpoint_owner("gmail", "messages.get") == "gmail_api"
    assert (
        operation_endpoint_owner("google_calendar", "directory.users.list")
        == "google_directory"
    )
    assert operation_endpoint_owner("discord", "/gateway/bot") == "discord_gateway_bot"
    assert (
        operation_endpoint_owner("quickbooks", "oauth.token.refresh")
        == "credential_refresh"
    )
    assert (
        operation_endpoint_owner("telegram", "get_history") == "non_resolver:protocol"
    )


def test_source_definition_rejects_missing_or_duplicate_operation_ownership() -> None:
    slack = source_definition("slack")
    with pytest.raises(ValueError, match="missing endpoint ownership"):
        replace(slack, outbound_endpoint_bindings=())

    first_operation = slack.operation_policy_ids[0]
    with pytest.raises(ValueError, match="duplicate endpoint ownership"):
        replace(
            slack,
            outbound_endpoint_bindings=(
                *slack.outbound_endpoint_bindings,
                OutboundEndpointBinding(
                    "github_api",
                    (first_operation,),
                ),
            ),
        )


def test_provider_definition_rejects_endpoint_reverse_index_drift() -> None:
    slack = provider_definition("slack")
    providers = tuple(
        replace(provider, outbound_endpoint_names=()) if provider is slack else provider
        for provider in PROVIDER_DEFINITIONS
    )
    with pytest.raises(CatalogValidationError, match="outbound endpoints"):
        validate_catalog(providers, SOURCE_DEFINITIONS)


def test_finance_testing_surface_is_a_source_capability() -> None:
    finance_sources = tuple(
        source
        for source in SOURCE_DEFINITIONS
        if "finance_testing" in source.capability_flags
    )
    assert tuple(source.source_id for source in finance_sources) == (
        "mercury",
        "quickbooks",
        "brex",
        "ramp",
        "gusto",
        "deel",
    )
    assert all(source.finance_testing_binding for source in finance_sources)
    assert all(
        source.finance_testing_binding is None
        for source in SOURCE_DEFINITIONS
        if source not in finance_sources
    )


def test_validation_rejects_source_alias_collisions() -> None:
    facebook = source_definition("facebook_pages")
    sources = tuple(
        replace(source, aliases=("qbo",)) if source is facebook else source
        for source in SOURCE_DEFINITIONS
    )
    with pytest.raises(CatalogValidationError, match="ambiguous"):
        validate_catalog(
            PROVIDER_DEFINITIONS,
            sources,
            expected_provider_ids=CANONICAL_PROVIDER_IDS,
            expected_source_ids=CANONICAL_SOURCE_IDS,
        )


def test_validation_rejects_missing_certification_declaration() -> None:
    slack = source_definition("slack")
    incomplete = Certification(
        test_kit_id="ingest.test_kit.slack",
        evidence_id=None,
        canary_id="ingest.canary.slack",
    )
    sources = tuple(
        replace(source, certification=incomplete) if source is slack else source
        for source in SOURCE_DEFINITIONS
    )
    with pytest.raises(
        CatalogValidationError,
        match=r"slack.*evidence_id",
    ):
        validate_catalog(PROVIDER_DEFINITIONS, sources)


def test_validation_rejects_missing_validation_runtime() -> None:
    slack = source_definition("slack")
    incomplete = replace(slack.certification, validation_runtime=None)
    sources = tuple(
        replace(source, certification=incomplete) if source is slack else source
        for source in SOURCE_DEFINITIONS
    )
    with pytest.raises(
        CatalogValidationError,
        match=r"slack.*no certification validation runtime",
    ):
        validate_catalog(PROVIDER_DEFINITIONS, sources)


def test_validation_rejects_canonical_coverage_or_order_drift() -> None:
    with pytest.raises(CatalogValidationError, match="order/coverage"):
        validate_catalog(
            PROVIDER_DEFINITIONS,
            tuple(reversed(SOURCE_DEFINITIONS)),
            expected_source_ids=CANONICAL_SOURCE_IDS,
        )


def test_validation_rejects_provider_reverse_index_drift() -> None:
    google = provider_definition("google")
    providers = tuple(
        replace(
            provider,
            source_ids=("gmail",),
            dedicated_ingresses=(),
        )
        if provider is google
        else provider
        for provider in PROVIDER_DEFINITIONS
    )
    with pytest.raises(CatalogValidationError, match="declares source_ids"):
        validate_catalog(providers, SOURCE_DEFINITIONS)


def test_validation_rejects_non_source_channel_collision() -> None:
    collision = replace(
        NON_SOURCE_CHANNEL_DEFINITIONS[0],
        channel="slack:message",
    )
    with pytest.raises(CatalogValidationError, match="owned by both"):
        validate_channel_catalog(
            SOURCE_DEFINITIONS,
            (collision, *NON_SOURCE_CHANNEL_DEFINITIONS[1:]),
        )


def test_unknown_names_fail_loudly() -> None:
    with pytest.raises(KeyError, match="unknown ingestion source"):
        resolve_source_id("not-a-real-source")
    with pytest.raises(KeyError, match="unknown ingestion provider"):
        resolve_provider_id("not-a-real-provider")
