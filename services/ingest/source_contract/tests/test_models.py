from __future__ import annotations

from dataclasses import FrozenInstanceError, replace

import pytest

from services.ingest.source_contract import (
    Certification,
    CredentialRefreshDefinition,
    LiveRuntimeDefinition,
    LiveWorkerDefinition,
    OperationPolicyDefinition,
    RequestPolicy,
    source_definition,
)
from services.ingest.source_contract.models import normalize_catalog_name


def test_contract_values_are_deeply_immutable_sequences() -> None:
    source = source_definition("slack")
    with pytest.raises(FrozenInstanceError):
        source.display_name = "Other"  # type: ignore[misc]
    with pytest.raises(FrozenInstanceError):
        source.aliases += ("other",)  # type: ignore[misc]
    assert isinstance(source.data_objects, tuple)
    assert isinstance(source.live_contracts(), tuple)
    with pytest.raises(FrozenInstanceError):
        source.display.order = 99  # type: ignore[misc]


def test_source_display_contract_rejects_invalid_order_and_sync_mode() -> None:
    display = source_definition("slack").display
    with pytest.raises(ValueError, match="integer >= 0"):
        replace(display, order=-1)
    with pytest.raises(ValueError, match="unknown source display sync modes"):
        replace(
            display,
            supported_sync_modes=("Unbounded replay",),  # type: ignore[arg-type]
        )


def test_onboarding_contract_rejects_overlapping_input_fields() -> None:
    onboarding = source_definition("slack").onboarding
    with pytest.raises(ValueError, match="overlap"):
        replace(
            onboarding,
            required_inputs=("workspace_id",),
            optional_inputs=("workspace_id",),
        )


def test_onboarding_contract_requires_valid_runtime_handoff_and_finalize_mode() -> None:
    onboarding = source_definition("slack").onboarding
    with pytest.raises(ValueError, match="module:callable"):
        replace(onboarding, provider_handoff_binding="slack.provider_handoff")
    with pytest.raises(ValueError, match="module:callable"):
        replace(onboarding, access_status_binding="discord.access")
    with pytest.raises(ValueError, match="module:callable"):
        replace(onboarding, provider_setup_builder_binding="slack.setup")
    with pytest.raises(ValueError, match="module:callable"):
        replace(onboarding, rehearsal_artifact_builder_binding="slack.artifacts")
    with pytest.raises(ValueError, match="rehearsal_finalize_mode"):
        replace(
            onboarding,
            rehearsal_finalize_mode="fallback",  # type: ignore[arg-type]
        )


def test_google_dwd_onboarding_requires_exact_https_consent_scopes() -> None:
    onboarding = source_definition("gmail").onboarding

    with pytest.raises(ValueError, match="exact consent_scopes"):
        replace(onboarding, consent_scopes=())
    with pytest.raises(ValueError, match="must use HTTPS"):
        replace(onboarding, consent_scopes=("gmail.metadata",))


def test_native_connect_contract_rejects_duplicate_fields_and_partial_routes() -> None:
    native_connect = source_definition("slack").onboarding.native_connect
    with pytest.raises(ValueError, match="duplicate"):
        replace(
            native_connect,
            payload_fields=("workspace_id", "workspace_id"),
        )
    with pytest.raises(ValueError, match="declared together"):
        replace(native_connect, finalize_path=None)


@pytest.mark.parametrize(
    "value",
    [
        RequestPolicy(),
        RequestPolicy(base_backoff_seconds=0, max_backoff_seconds=0),
        RequestPolicy(max_attempts=1),
    ],
)
def test_request_policy_valid_examples_are_immutable(value: RequestPolicy) -> None:
    with pytest.raises(FrozenInstanceError):
        value.max_attempts = 99  # type: ignore[misc]


@pytest.mark.parametrize(
    "kwargs",
    [
        {"timeout_seconds": 0},
        {"max_attempts": 0},
        {"base_backoff_seconds": -0.1},
        {"base_backoff_seconds": 2.0, "max_backoff_seconds": 1.0},
        {"max_elapsed_seconds": 0},
        {"max_quota_wait_seconds": -0.1},
        {"max_concurrency": 0},
    ],
)
def test_request_policy_rejects_invalid_budgets(kwargs: dict[str, object]) -> None:
    with pytest.raises((TypeError, ValueError)):
        RequestPolicy(**kwargs)  # type: ignore[arg-type]


def test_unverified_certification_can_describe_pending_work() -> None:
    certification = Certification()
    assert certification.status == "unverified"
    assert certification.missing_required_declarations() == (
        "test_kit_id",
        "evidence_id",
        "canary_id",
    )


def test_verified_certification_cannot_omit_required_declarations() -> None:
    with pytest.raises(ValueError, match="verified certification"):
        Certification(status="verified")


def test_history_none_forbids_executable_history_bindings() -> None:
    whatsapp = source_definition("whatsapp")
    with pytest.raises(ValueError, match="history=None"):
        replace(
            whatsapp,
            planner_binding=(
                "services.ingest.ingestion.planners.slack:"
                "plan_shards_slack"
            ),
        )


def test_history_support_requires_all_executable_history_bindings() -> None:
    slack = source_definition("slack")
    with pytest.raises(ValueError, match="must declare planner"):
        replace(slack, fetcher_binding=None)


def test_live_policy_vectors_must_have_equal_length() -> None:
    slack = source_definition("slack")
    with pytest.raises(ValueError, match="must have equal length"):
        replace(
            slack,
            delivery_policies=("at_least_once", "replayable_pull"),
        )


def test_live_runtime_must_cover_each_declared_transport_exactly_once() -> None:
    slack = source_definition("slack")
    with pytest.raises(ValueError, match="exactly once"):
        replace(
            slack,
            live_runtime=LiveRuntimeDefinition(
                workers=(
                    LiveWorkerDefinition(
                        component_id="orphan_poller",
                        role="incremental_poll",
                        transport="api_poll",
                        deployment_owner="fyralis_worker",
                        lease_scope="resource",
                        launcher_binding=(
                            "services.ingest.ingestion.workflows."
                            "periodic_reconciler:run_forever"
                        ),
                        cadence_seconds=60,
                        deployment_unit="periodic_reconciler",
                    ),
                ),
            ),
        )


def test_customer_managed_live_runtime_cannot_claim_a_launcher() -> None:
    with pytest.raises(ValueError, match="cannot claim a Fyralis launcher"):
        LiveWorkerDefinition(
            component_id="external_queue",
            role="managed_dispatch",
            transport="queue_poll",
            deployment_owner="customer_deployment",
            lease_scope="installation",
            launcher_binding=(
                "services.ingest.integrations.aws.live_poll:"
                "handle_polled_event"
            ),
            dispatch_binding=(
                "services.ingest.integrations.aws.live_poll:"
                "handle_polled_event"
            ),
            managed_reason="Customer deploys the queue consumer.",
        )


def test_poll_and_watch_workers_require_explicit_cadence() -> None:
    with pytest.raises(ValueError, match="requires cadence_seconds"):
        LiveWorkerDefinition(
            component_id="poller",
            role="incremental_poll",
            transport="api_poll",
            deployment_owner="fyralis_worker",
            lease_scope="resource",
            launcher_binding=(
                "services.ingest.ingestion.workflows.periodic_reconciler:"
                "run_forever"
            ),
            deployment_unit="periodic_reconciler",
        )


def test_normalizer_bindings_require_real_callable_references() -> None:
    slack = source_definition("slack")
    with pytest.raises(ValueError, match="module:callable"):
        replace(
            slack,
            normalizer_bindings=("ingest.normalizer.slack.message",),
        )


def test_one_callable_may_intentionally_normalize_multiple_channels() -> None:
    figma = source_definition("figma")
    assert figma.normalizer_bindings == (
        "services.ingest.ingestion.handlers.figma:handle_figma_event",
        "services.ingest.ingestion.handlers.figma:handle_figma_event",
    )


def test_source_contract_owns_external_id_builders() -> None:
    drive = source_definition("google_drive")
    assert drive.idempotency_builder_bindings == (
        "services.ingest.ingestion.idempotency:google_drive_file",
        "services.ingest.ingestion.idempotency:google_drive_comment",
        "services.ingest.ingestion.idempotency:google_drive_revision",
    )
    with pytest.raises(ValueError, match="module:callable"):
        replace(
            drive,
            idempotency_builder_bindings=("google_drive_file",),
        )
    with pytest.raises(ValueError, match="must not be empty"):
        replace(drive, idempotency_builder_bindings=())


def test_metrics_export_bindings_require_real_callable_references() -> None:
    slack = source_definition("slack")
    assert slack.metrics_export_bindings == (
        "services.ingest.integrations.slack.metrics:export_metrics",
    )
    with pytest.raises(ValueError, match="module:callable"):
        replace(slack, metrics_export_bindings=("slack.metrics.export",))
    with pytest.raises(ValueError, match="duplicate"):
        replace(
            slack,
            metrics_export_bindings=(
                "services.ingest.integrations.slack.metrics:export_metrics",
                "services.ingest.integrations.slack.metrics:export_metrics",
            ),
        )


def test_request_policy_is_the_shared_provider_transport_contract() -> None:
    from lib.shared.provider_transport import RequestPolicy as SharedRequestPolicy

    assert RequestPolicy is SharedRequestPolicy


def test_transport_enforcement_requires_finite_semantic_operations() -> None:
    slack = source_definition("slack")
    with pytest.raises(ValueError, match="operation_policies"):
        replace(slack, operation_policies=())
    with pytest.raises(ValueError, match="semantic operation"):
        replace(
            slack,
            operation_policies=(
                OperationPolicyDefinition(
                    "GET https://slack.test",
                    RequestPolicy(),
                ),
            ),
        )


def test_operation_policy_ids_are_exact_and_unique() -> None:
    slack = source_definition("slack")
    duplicate = slack.operation_policies[0]
    with pytest.raises(ValueError, match="duplicate operation IDs"):
        replace(
            slack,
            operation_policies=(
                duplicate,
                duplicate,
            ),
        )


def test_operation_policy_requires_the_shared_request_policy() -> None:
    with pytest.raises(TypeError, match="RequestPolicy"):
        OperationPolicyDefinition(
            "messages.get",
            object(),  # type: ignore[arg-type]
        )


def test_operation_policy_requires_explicit_concurrency_and_retry_allowlists() -> None:
    with pytest.raises(ValueError, match="max_concurrency"):
        OperationPolicyDefinition(
            "messages.get",
            RequestPolicy(),
        )
    with pytest.raises(ValueError, match="retryable_status_codes"):
        OperationPolicyDefinition(
            "messages.get",
            RequestPolicy(
                max_concurrency=1,
            ),
        )
    with pytest.raises(ValueError, match="retryable_error_codes"):
        OperationPolicyDefinition(
            "messages.get",
            RequestPolicy(
                max_concurrency=1,
                retryable_status_codes=(429,),
            ),
        )


def test_live_only_source_can_certify_that_it_has_no_outbound_requests() -> None:
    whatsapp = source_definition("whatsapp")
    assert whatsapp.provider_transport_enforced is True
    assert whatsapp.operation_policy_ids == ()
    assert "no_outbound_provider_requests" in whatsapp.capability_flags

    with pytest.raises(ValueError, match="requires history=None"):
        replace(
            source_definition("slack"),
            capability_flags=("no_outbound_provider_requests",),
        )


def test_credential_refresh_requires_a_declared_transport_operation() -> None:
    quickbooks = source_definition("quickbooks")
    assert quickbooks.credential_refresh is not None
    undeclared = replace(
        quickbooks.credential_refresh,
        operation_id="oauth.token.undeclared",
    )
    with pytest.raises(
        ValueError,
        match="credential refresh operation must be declared",
    ):
        replace(quickbooks, credential_refresh=undeclared)


@pytest.mark.parametrize(
    "kwargs",
    [
        {"default_token_url": "http://provider.invalid/token"},
        {"token_url_env": "not-an-env-key"},
        {"default_expires_in": 0},
        {
            "client_secret_from_install": True,
            "client_credentials_from_install": True,
        },
    ],
)
def test_credential_refresh_rejects_ambiguous_or_unsafe_contracts(
    kwargs: dict[str, object],
) -> None:
    base = dict(
        operation_id="oauth.token.refresh",
        default_token_url="https://provider.test/token",
        token_url_env="PROVIDER_TOKEN_URL",
        grant_type="refresh_token",
        auth_style="basic",
        rotates_refresh_token=True,
        install_table="provider_installations",
    )
    with pytest.raises((TypeError, ValueError)):
        CredentialRefreshDefinition(**(base | kwargs))  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("  Google Calendar ", "google_calendar"),
        ("QuickBooks.Online", "quickbooks_online"),
        ("FACEBOOK---PAGES", "facebook_pages"),
    ],
)
def test_catalog_name_normalization_is_deterministic(
    value: str,
    expected: str,
) -> None:
    assert normalize_catalog_name(value) == expected
