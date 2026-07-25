"""One certification specification for every canonical source.

These declarations are deliberately not marked verified.  Official references
identify what must be pinned; ``verified_at`` and schema checksums are populated
only by the evidence-lock workflow after a human/live review.  The evaluator
therefore blocks release until the promised proof actually exists.
"""
from __future__ import annotations

from types import MappingProxyType
from typing import Mapping

from services.ingest.source_certification.models import (
    CanaryDefinition,
    CertificationBindingRole,
    CertificationCallableBinding,
    EvidenceReference,
    LoadSuite,
    SourceCertificationSpec,
)
from services.ingest.source_contract.catalog import (
    CANONICAL_SOURCE_IDS,
    source_definition,
)


# Primary official documentation for the API surface Fyralis uses.  These are
# evidence *inputs*, not claims that the current implementation was verified.
_DOCS: Mapping[str, tuple[str, str, str]] = MappingProxyType(
    {
        "slack": (
            "Web API / Events API",
            "https://docs.slack.dev/apis/web-api/",
            "https://docs.slack.dev/apis/web-api/rate-limits/",
        ),
        "github": (
            "REST API 2022-11-28",
            "https://docs.github.com/en/rest",
            "https://docs.github.com/en/rest/using-the-rest-api/rate-limits-for-the-rest-api",
        ),
        "discord": (
            "HTTP v10 / Gateway v10",
            "https://docs.discord.com/developers/reference",
            "https://docs.discord.com/developers/topics/rate-limits",
        ),
        "gmail": (
            "Gmail API v1",
            "https://developers.google.com/workspace/gmail/api/reference/rest",
            "https://developers.google.com/workspace/gmail/api/reference/quota",
        ),
        "notion": (
            "Notion API 2022-06-28",
            "https://developers.notion.com/reference/intro",
            "https://developers.notion.com/reference/request-limits",
        ),
        "google_calendar": (
            "Calendar API v3",
            "https://developers.google.com/workspace/calendar/api/v3/reference",
            "https://developers.google.com/calendar/api/guides/quota",
        ),
        "google_drive": (
            "Drive API v3",
            "https://developers.google.com/workspace/drive/api/reference/rest/v3",
            "https://developers.google.com/drive/api/guides/limits",
        ),
        "jira": (
            "Jira Cloud REST API v3",
            "https://developer.atlassian.com/cloud/jira/platform/rest/v3/intro/",
            "https://developer.atlassian.com/cloud/jira/platform/rate-limiting/",
        ),
        "mercury": (
            "Mercury API",
            "https://docs.mercury.com/reference/welcome-to-mercurys-api",
            "https://docs.mercury.com/reference/welcome-to-mercurys-api",
        ),
        "quickbooks": (
            "QuickBooks Online Accounting API v3",
            "https://developer.intuit.com/app/developer/qbo/docs/api/accounting/all-entities/account",
            "https://developer.intuit.com/app/developer/qbo/docs/develop/rest-api-features",
        ),
        "grafana": (
            "Grafana HTTP API",
            "https://grafana.com/docs/grafana/latest/developers/http_api/",
            "https://grafana.com/docs/grafana/latest/developers/http_api/",
        ),
        "telegram": (
            "Telegram MTProto layer (Telethon boundary)",
            "https://core.telegram.org/api",
            "https://docs.telethon.dev/en/stable/concepts/errors.html",
        ),
        "brex": (
            "Brex API",
            "https://developer.brex.com/openapi/",
            "https://developer.brex.com/openapi/",
        ),
        "ramp": (
            "Ramp Developer API",
            "https://docs.ramp.com/developer-api/v1/guides/getting-started",
            "https://docs.ramp.com/developer-api/v1/guides/rate-limits",
        ),
        "gusto": (
            "Gusto Embedded API",
            "https://docs.gusto.com/embedded-payroll/reference/introduction",
            "https://docs.gusto.com/embedded-payroll/docs/rate-limits",
        ),
        "deel": (
            "Deel API",
            "https://developer.deel.com/docs/welcome",
            "https://developer.deel.com/docs/welcome",
        ),
        "fireflies": (
            "Fireflies GraphQL API",
            "https://docs.fireflies.ai/graphql-api/query/",
            "https://docs.fireflies.ai/graphql-api/query/",
        ),
        "signal": (
            "signal-cli JSON-RPC",
            "https://github.com/AsamK/signal-cli/blob/master/man/signal-cli-jsonrpc.5.adoc",
            "https://github.com/AsamK/signal-cli",
        ),
        "aws": (
            "AWS CloudTrail / STS / SQS APIs",
            "https://docs.aws.amazon.com/awscloudtrail/latest/APIReference/Welcome.html",
            "https://docs.aws.amazon.com/general/latest/gr/ct.html",
        ),
        "miro": (
            "Miro REST API v2",
            "https://developers.miro.com/reference/api-reference",
            "https://developers.miro.com/docs/miro-rest-api-introduction#rate-limiting",
        ),
        "figma": (
            "Figma REST API v1",
            "https://developers.figma.com/docs/rest-api/",
            "https://developers.figma.com/docs/rest-api/rate-limits/",
        ),
        "carta": (
            "Carta API v1alpha",
            "https://docs.carta.com/",
            "https://docs.carta.com/",
        ),
        "hibob": (
            "HiBob API",
            "https://apidocs.hibob.com/",
            "https://apidocs.hibob.com/",
        ),
        "ashby": (
            "Ashby API",
            "https://developers.ashbyhq.com/reference/introduction",
            "https://developers.ashbyhq.com/reference/rate-limit",
        ),
        "linkedin": (
            "LinkedIn Rest.li",
            "https://learn.microsoft.com/en-us/linkedin/shared/api-guide/concepts/protocol-version",
            "https://learn.microsoft.com/en-us/linkedin/shared/api-guide/concepts/rate-limits",
        ),
        "whatsapp": (
            "WhatsApp Cloud API",
            "https://developers.facebook.com/docs/whatsapp/cloud-api/",
            "https://developers.facebook.com/docs/whatsapp/cloud-api/overview/",
        ),
        "facebook_pages": (
            "Meta Graph API v23.0",
            "https://developers.facebook.com/docs/messenger-platform/",
            "https://developers.facebook.com/docs/graph-api/overview/rate-limiting/",
        ),
    }
)


_BASE_SCENARIOS = (
    "auth_success_and_expiry",
    "exact_tenant_and_installation_resolution",
    "pagination_or_stream_resume",
    "create_update_delete_or_declared_absence",
    "duplicate_delivery_and_idempotency",
    "out_of_order_delivery",
    "provider_429_shared_cooldown",
    "provider_5xx_timeout_and_recovery",
    "no_cursor_advance_past_required_hydration_failure",
    "raw_evidence_and_normalized_topic",
    "observation_persistence_and_t1_trigger",
    "two_replica_cross_tenant_isolation",
)
_BASE_SIMULATOR_CAPABILITIES = (
    "strict_request_schema",
    "auth_validation",
    "stateful_pagination",
    "deterministic_virtual_clock",
    "scoped_weighted_quotas",
    "429_reset_headers",
    "5xx_timeout_disconnect_faults",
    "replay_and_out_of_order_delivery",
    "request_ledger",
    "loopback_only",
)

_FIXTURE_BINDING_MODULE = "services.ingest.synthetic.fixtures.certification"
_INSTALLATION_BINDING_MODULE = (
    "services.ingest.synthetic.backfill_harness.harness"
)

_SPECIAL_SCENARIOS: Mapping[str, tuple[str, ...]] = MappingProxyType(
    {
        "slack": ("thread_refetch_and_full_thread_upsert", "kafka_inline_parity"),
        "github": (
            "app_jwt_installation_token_rotation",
            "link_etag_and_historical_external_id_parity",
        ),
        "discord": (
            "gateway_resume_and_session_limits",
            "synchronous_interaction_response",
            "route_and_global_bucket_feedback",
        ),
        "gmail": ("history_id_expiry", "pubsub_thin_event_hydration"),
        "notion": ("recursive_block_hydration",),
        "google_calendar": ("watch_renewal", "expired_sync_token_full_resync"),
        "google_drive": (
            "watch_renewal",
            "expired_change_token_full_resync",
            "export_comments_revisions_permissions",
        ),
        "jira": ("simultaneous_quota_windows", "live_canary_never_load_tests"),
        "quickbooks": ("multi_realm_notification_fanout", "oauth_refresh"),
        "grafana": ("deployment_configured_quota_policy",),
        "telegram": ("floodwait_and_catchup", "installation_scoped_worker"),
        "signal": ("pinned_signal_cli_version", "installation_scoped_worker"),
        "aws": ("sigv4_and_sts_identity", "sqs_eventbridge_path"),
        "figma": ("event_to_file_snapshot_multi_output", "plan_tier_quota"),
        "linkedin": ("developer_entitlement_required", "restli_version_headers"),
        "whatsapp": ("history_explicitly_unsupported", "verification_challenge"),
        "facebook_pages": ("historical_live_overlap", "page_token_rotation"),
    }
)


def _callable_binding(
    source_id: str,
    role: CertificationBindingRole,
) -> CertificationCallableBinding:
    if role == "fixture_factory":
        reference = (
            f"{_FIXTURE_BINDING_MODULE}:build_{source_id}_fixture"
        )
    else:
        reference = (
            f"{_INSTALLATION_BINDING_MODULE}:"
            f"_write_{source_id}_install_and_trigger"
        )
    return CertificationCallableBinding(
        source_id=source_id,
        role=role,
        reference=reference,
    )


def _suite(kind: str, source_id: str, history_supported: bool) -> LoadSuite:
    if kind == "historical":
        mix = (
            ("plan", "fetch_page", "reconcile")
            if history_supported
            else ("assert_history_unsupported",)
        )
    elif kind == "live":
        mix = ("receive", "verify", "hydrate", "normalize", "persist")
    else:
        mix = (
            "live_receive",
            "historical_fetch" if history_supported else "history_rejection",
            "reconcile",
            "token_or_watch_renewal",
        )
    return LoadSuite(kind=kind, operation_mix=tuple(f"{source_id}.{x}" for x in mix))


def _canary_operations(source) -> tuple[str, ...]:  # noqa: ANN001
    """Derive the exact low-rate real-provider surface from the source contract."""

    return (
        "auth.conformance",
        *(
            f"provider_request.{operation_id}"
            for operation_id in source.operation_policy_ids
        ),
        *(
            f"live_transport.{transport}"
            for transport in source.live_transports
        ),
    )


def _spec(source_id: str) -> SourceCertificationSpec:
    source = source_definition(source_id)
    api_version, docs_uri, quota_uri = _DOCS[source_id]
    history_supported = source.history is not None
    test_kit_id = source.certification.test_kit_id
    if test_kit_id is None:
        raise RuntimeError(
            f"source {source_id!r} has no certification test kit"
        )
    return SourceCertificationSpec(
        source_id=source_id,
        spec_version="2.0.0",
        provider_api_version=api_version,
        test_kit_id=test_kit_id,
        evidence=(
            EvidenceReference(
                behavior_id="used_api_surface",
                kind="documented",
                uri=docs_uri,
                api_version=api_version,
                # Intentionally empty until P0 evidence locking runs.
                schema_sha256=None,
                verified_at=None,
            ),
            EvidenceReference(
                behavior_id="quota_policy",
                kind="documented",
                uri=quota_uri,
                quota_uri=quota_uri,
                verified_at=None,
            ),
            EvidenceReference(
                behavior_id="fyralis_runtime_contract",
                kind="fyralis_specific",
                uri=f"repo://services/ingest/source_contract/catalog.py#{source_id}",
                api_version="source-contract-v1",
                verified_at=None,
            ),
        ),
        required_scenarios=_BASE_SCENARIOS + _SPECIAL_SCENARIOS.get(source_id, ()),
        simulator_capabilities=_BASE_SIMULATOR_CAPABILITIES,
        load_suites=(
            _suite("historical", source_id, history_supported),
            _suite("live", source_id, history_supported),
            _suite("combined", source_id, history_supported),
        ),
        canary=CanaryDefinition(
            canary_id=f"ingest.canary.{source_id}",
            credential_env_prefix=f"FYRALIS_CANARY_{source_id.upper()}",
            account_type=f"dedicated disposable {source.display_name} test account",
            required_operations=_canary_operations(source),
            read_only_by_default=True,
            max_requests=25,
        ),
        fixture_factory_binding=(
            _callable_binding(source_id, "fixture_factory")
            if history_supported
            else None
        ),
        installation_seeder_binding=(
            _callable_binding(source_id, "installation_seeder")
            if history_supported
            else None
        ),
    )


def _validate_source_linkage(spec: SourceCertificationSpec) -> None:
    source = source_definition(spec.source_id)
    if source.source_id != spec.source_id:
        raise RuntimeError(
            f"certification source mismatch: {spec.source_id!r} resolved "
            f"to {source.source_id!r}"
        )
    if source.certification.test_kit_id != spec.test_kit_id:
        raise RuntimeError(
            f"certification kit mismatch for {spec.source_id!r}: "
            f"{spec.test_kit_id!r} != "
            f"{source.certification.test_kit_id!r}"
        )
    if source.certification.canary_id != spec.canary.canary_id:
        raise RuntimeError(
            f"certification canary mismatch for {spec.source_id!r}"
        )
    has_bindings = (
        spec.fixture_factory_binding is not None
        and spec.installation_seeder_binding is not None
    )
    if has_bindings != (source.history is not None):
        support = "supports" if source.history is not None else "does not support"
        raise RuntimeError(
            f"source {spec.source_id!r} {support} history but its "
            "certification callable bindings disagree"
        )


SOURCE_CERTIFICATION_SPECS: tuple[SourceCertificationSpec, ...] = tuple(
    _spec(source_id) for source_id in CANONICAL_SOURCE_IDS
)
SOURCE_CERTIFICATION_CATALOG: Mapping[str, SourceCertificationSpec] = (
    MappingProxyType({spec.source_id: spec for spec in SOURCE_CERTIFICATION_SPECS})
)

if tuple(SOURCE_CERTIFICATION_CATALOG) != CANONICAL_SOURCE_IDS:
    raise RuntimeError("certification catalog coverage/order differs from source catalog")
for _declared_spec in SOURCE_CERTIFICATION_SPECS:
    _validate_source_linkage(_declared_spec)


def source_certification_spec(source_name: str) -> SourceCertificationSpec:
    """Return the source's structurally validated certification kit."""

    source = source_definition(source_name)
    try:
        spec = SOURCE_CERTIFICATION_CATALOG[source.source_id]
    except KeyError as exc:
        raise KeyError(
            f"source {source.source_id!r} has no certification spec"
        ) from exc
    _validate_source_linkage(spec)
    return spec


__all__ = [
    "SOURCE_CERTIFICATION_CATALOG",
    "SOURCE_CERTIFICATION_SPECS",
    "source_certification_spec",
]
