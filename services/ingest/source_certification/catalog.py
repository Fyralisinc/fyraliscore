"""One certification specification for every canonical source.

These declarations are deliberately not marked verified.  Official references
identify what must be pinned; ``verified_at`` and schema checksums are populated
only by the evidence-lock workflow after a human/live review.  The evaluator
therefore blocks release until the promised proof actually exists.
"""

from __future__ import annotations

import json
from types import MappingProxyType
from typing import Mapping

from lib.shared.provider_transport import RetrySafety
from services.ingest.source_certification.evidence import (
    SURFACE_ARTIFACT_DIRECTORY,
    load_evidence_catalog,
    load_surface_operation_methods,
)
from services.ingest.source_certification.models import (
    CanaryDefinition,
    CanaryOperationContract,
    CertificationBindingRole,
    CertificationCallableBinding,
    ExecutableLoadOperation,
    LoadOperationContractAbsence,
    LoadQuotaMapping,
    LoadSuite,
    LoadSuiteNonApplicability,
    SourceCertificationSpec,
    SuiteKind,
)
from services.ingest.source_contract.catalog import (
    CANONICAL_SOURCE_IDS,
    effective_request_policy,
    source_definition,
)


_EVIDENCE_PACKS = load_evidence_catalog()
_PROVIDER_OPERATION_METHODS = MappingProxyType(
    {
        source_id: load_surface_operation_methods(source_id)
        for source_id in CANONICAL_SOURCE_IDS
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
_INSTALLATION_BINDING_MODULE = "services.ingest.synthetic.backfill_harness.harness"

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
        reference = f"{_FIXTURE_BINDING_MODULE}:build_{source_id}_fixture"
    elif role == "live_fixture_factory":
        reference = f"{_FIXTURE_BINDING_MODULE}:build_{source_id}_live_fixture"
    elif role == "fixture_count_oracle":
        reference = (
            f"{_FIXTURE_BINDING_MODULE}:" f"count_{source_id}_fixture_observations"
        )
    else:
        reference = (
            f"{_INSTALLATION_BINDING_MODULE}:" f"_write_{source_id}_install_and_trigger"
        )
    return CertificationCallableBinding(
        source_id=source_id,
        role=role,
        reference=reference,
    )


def _suite(
    kind: SuiteKind,
    source_id: str,
    history_supported: bool,
) -> LoadSuite:
    source = source_definition(source_id)
    evidence_id = (
        f"{source.certification.evidence_id}.fyralis_runtime_contract"
    )
    quota_mappings = _load_quota_mappings(source_id)
    control_proofs = (
        ("binding_invocation", "quota_mapping")
        if quota_mappings
        else ("binding_invocation",)
    )
    data_proofs = (
        "binding_invocation",
        "raw_s3",
        "raw_kafka",
        "normalized_kafka",
        "observation",
        "t1",
        "replica_processing",
        "quota_mapping",
    )
    live_data_proofs = tuple(
        proof_id
        for proof_id in data_proofs
        if proof_id != "quota_mapping"
    )

    def control(
        operation_name: str,
        binding: str,
        *,
        provider_calls: bool,
        trial_position: str,
    ) -> ExecutableLoadOperation:
        return ExecutableLoadOperation(
            operation_id=f"{source_id}.{operation_name}",
            executable_binding=binding,
            evidence_id=evidence_id,
            kind="control",
            execution_frequency="once_per_trial",
            trial_position=trial_position,  # type: ignore[arg-type]
            raw_cardinality="none",
            normalized_cardinality="none",
            observation_cardinality="none",
            cursor_applicability="not_applicable",
            quota_mappings=quota_mappings if provider_calls else (),
            receipt_proof_requirements=(
                control_proofs
                if provider_calls
                else ("binding_invocation",)
            ),
        )

    def historical_fetch(
        operation_name: str,
    ) -> ExecutableLoadOperation:
        if source.fetcher_binding is None:
            raise RuntimeError(
                f"source {source_id!r} has no historical fetch binding"
            )
        return ExecutableLoadOperation(
            operation_id=f"{source_id}.{operation_name}",
            executable_binding=source.fetcher_binding,
            evidence_id=evidence_id,
            kind="data",
            execution_frequency="per_item",
            selection_weight=1,
            raw_cardinality="one_or_more",
            normalized_cardinality="one_or_more",
            observation_cardinality="one_or_more",
            cursor_applicability="required",
            quota_mappings=quota_mappings,
            receipt_proof_requirements=(
                *data_proofs,
                "cursor_consistency",
            ),
        )

    validation_runtime = source.certification.validation_runtime
    if validation_runtime is None:
        raise RuntimeError(
            f"source {source_id!r} has no validation runtime"
        )
    live_receive = ExecutableLoadOperation(
        operation_id=(
            f"{source_id}.receive"
            if kind == "live"
            else f"{source_id}.live_receive"
        ),
        executable_binding=validation_runtime.live_event_binding,
        evidence_id=evidence_id,
        kind="data",
        execution_frequency="per_item",
        selection_weight=1,
        raw_cardinality="exactly_one",
        normalized_cardinality="one_or_more",
        observation_cardinality="one_or_more",
        cursor_applicability="optional",
        receipt_proof_requirements=live_data_proofs,
    )

    if kind == "historical":
        mix = (
            ("plan", "fetch_page", "reconcile")
            if history_supported
            else ("assert_history_unsupported",)
        )
        if history_supported:
            if (
                source.planner_binding is None
                or source.reconciler_binding is None
            ):
                raise RuntimeError(
                    f"source {source_id!r} has incomplete history bindings"
                )
            executable_operations = (
                control(
                    "plan",
                    source.planner_binding,
                    provider_calls=True,
                    trial_position="before_load",
                ),
                historical_fetch("fetch_page"),
                control(
                    "reconcile",
                    source.reconciler_binding,
                    provider_calls=True,
                    trial_position="after_load",
                ),
            )
        else:
            executable_operations = ()
    elif kind == "live":
        mix = ("receive", "verify", "hydrate", "normalize", "persist")
        executable_operations = (live_receive,)
    else:
        mix = (
            "live_receive",
            "historical_fetch" if history_supported else "history_rejection",
            "reconcile",
            "token_or_watch_renewal",
        )
        if history_supported:
            if (
                source.planner_binding is None
                or source.reconciler_binding is None
            ):
                raise RuntimeError(
                    f"source {source_id!r} has incomplete history bindings"
                )
            executable_operations = (
                control(
                    "plan",
                    source.planner_binding,
                    provider_calls=True,
                    trial_position="before_load",
                ),
                live_receive,
                historical_fetch("historical_fetch"),
                control(
                    "reconcile",
                    source.reconciler_binding,
                    provider_calls=True,
                    trial_position="after_load",
                ),
            )
        else:
            executable_operations = (live_receive,)
    requires_renewal = (
        source.credential_refresh is not None
        or any(
            worker.role == "watch_renewal"
            for worker in source.live_runtime.workers
        )
    )
    absence_assertions: tuple[LoadOperationContractAbsence, ...] = ()
    if kind == "combined":
        absence_assertions = (
            LoadOperationContractAbsence(
                operation_id=f"{source_id}.token_or_watch_renewal",
                evidence_id=(
                    f"{source.certification.evidence_id}."
                    "fyralis_runtime_contract"
                ),
                missing_contract="bounded_callable",
                reason=(
                    (
                        "the source requires renewal but exposes no bounded "
                        "one-shot callable; long-running launchers and policy "
                        "metadata are not offerable load operations"
                    )
                    if requires_renewal
                    else (
                        "the source contract declares no token or watch "
                        "renewal semantic"
                    )
                ),
                blocks_execution=requires_renewal,
            ),
        )
        if not history_supported:
            absence_assertions = (
                LoadOperationContractAbsence(
                    operation_id=f"{source_id}.history_rejection",
                    evidence_id=evidence_id,
                    missing_contract="bounded_callable",
                    reason=(
                        "historical ingestion is genuinely not applicable; "
                        "there is no data-plane history operation to offer"
                    ),
                    blocks_execution=False,
                ),
                *absence_assertions,
            )
    return LoadSuite(
        kind=kind,
        operation_mix=tuple(f"{source_id}.{x}" for x in mix),
        executable_operations=executable_operations,
        contract_absence_assertions=absence_assertions,
        non_applicability=(
            LoadSuiteNonApplicability(
                evidence_id=evidence_id,
                reason=(
                    "the source contract explicitly declares historical "
                    "ingestion unsupported"
                ),
            )
            if kind == "historical" and not history_supported
            else None
        ),
    )


def _load_quota_mappings(source_id: str) -> tuple[LoadQuotaMapping, ...]:
    """Load the hash-pinned operation/bucket/cost allowlist for receipts."""

    path = SURFACE_ARTIFACT_DIRECTORY / f"{source_id}.json"
    payload = json.loads(path.read_text(encoding="utf-8"))
    routes = payload["provider_lab"]["routes"]
    mappings: dict[tuple[str, str | None], LoadQuotaMapping] = {}
    for route in routes:
        bucket = route["quota_bucket"]
        units = route["quota_cost"]
        for operation in route["operation_bindings"]:
            mapping = LoadQuotaMapping(
                operation_id=operation["operation_id"],
                quota_bucket=bucket,
                units_per_request=units,
            )
            identity = (mapping.operation_id, mapping.quota_bucket)
            existing = mappings.get(identity)
            if existing is not None and existing != mapping:
                raise RuntimeError(
                    f"source {source_id!r} has conflicting quota mapping "
                    f"{identity!r}"
                )
            mappings[identity] = mapping
    return tuple(
        mappings[identity]
        for identity in sorted(
            mappings,
            key=lambda item: (item[0], item[1] or ""),
        )
    )


def _canary_operations(source) -> tuple[str, ...]:  # noqa: ANN001
    """Derive the exact low-rate real-provider surface from the source contract."""

    return (
        "auth.conformance",
        *(
            f"provider_request.{operation_id}"
            for operation_id in source.operation_policy_ids
        ),
        *(f"live_transport.{transport}" for transport in source.live_transports),
    )


def _canary_operation_contracts(source) -> tuple[CanaryOperationContract, ...]:  # noqa: ANN001
    """Classify only exact, unambiguously read-only canary operations.

    Safe HTTP methods are insufficient by themselves: a source-contract
    operation marked ``UNSAFE`` remains unclassified even when a provider
    happens to encode it as GET. Non-safe HTTP methods, protocol-only
    operations, and live subscription/gateway lifecycles also remain
    unclassified until a source-specific canary declares their mutation and
    cleanup semantics. An unclassified operation makes release promotion
    structurally impossible.
    """

    contracts = [
        CanaryOperationContract(
            operation_id="auth.conformance",
            mutability="read",
            cleanup_action=None,
            classification_basis=(
                "credential conformance performs no provider resource mutation"
            ),
        )
    ]
    for provider_operation_id in source.operation_policy_ids:
        operation_id = f"provider_request.{provider_operation_id}"
        method = _PROVIDER_OPERATION_METHODS[source.source_id].get(
            provider_operation_id
        )
        request_policy = effective_request_policy(
            source.source_id,
            provider_operation_id,
        )
        if (
            method in {"GET", "HEAD", "OPTIONS"}
            and request_policy.retry_safety is not RetrySafety.UNSAFE
        ):
            contracts.append(
                CanaryOperationContract(
                    operation_id=operation_id,
                    mutability="read",
                    cleanup_action=None,
                    classification_basis=(
                        f"hash-pinned exact Provider Lab binding uses safe "
                        f"HTTP {method} and the source request policy is "
                        f"{request_policy.retry_safety.value}"
                    ),
                )
            )
            continue
        binding_description = (
            f"hash-pinned exact HTTP {method}"
            if method is not None
            else "operation without a hash-pinned HTTP binding"
        )
        contracts.append(
            CanaryOperationContract(
                operation_id=operation_id,
                mutability="unclassified",
                cleanup_action=None,
                classification_basis=(
                    f"{binding_description} with "
                    f"{request_policy.retry_safety.value} retry safety has no "
                    "source-specific read/mutation and cleanup declaration"
                ),
            )
        )
    for transport in source.live_transports:
        operation_id = f"live_transport.{transport}"
        if transport == "api_poll":
            contracts.append(
                CanaryOperationContract(
                    operation_id=operation_id,
                    mutability="read",
                    cleanup_action=None,
                    classification_basis=(
                        "source contract declares a provider-data poll with no "
                        "subscription lifecycle"
                    ),
                )
            )
        else:
            contracts.append(
                CanaryOperationContract(
                    operation_id=operation_id,
                    mutability="unclassified",
                    cleanup_action=None,
                    classification_basis=(
                        "live transport session/subscription behavior and "
                        "cleanup are not explicitly classified"
                    ),
                )
            )
    return tuple(contracts)


def _spec(source_id: str) -> SourceCertificationSpec:
    source = source_definition(source_id)
    evidence_pack = _EVIDENCE_PACKS[source_id]
    api_version = evidence_pack.provider_api_version
    history_supported = source.history is not None
    test_kit_id = source.certification.test_kit_id
    evidence_pack_id = source.certification.evidence_id
    if test_kit_id is None:
        raise RuntimeError(f"source {source_id!r} has no certification test kit")
    if evidence_pack_id is None:
        raise RuntimeError(f"source {source_id!r} has no certification evidence pack")
    return SourceCertificationSpec(
        source_id=source_id,
        spec_version="2.0.0",
        provider_api_version=api_version,
        test_kit_id=test_kit_id,
        evidence_pack_id=evidence_pack_id,
        evidence_pack_version=evidence_pack.pack_version,
        evidence_pack_sha256=evidence_pack.content_sha256,
        evidence=evidence_pack.evidence,
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
            operation_contracts=_canary_operation_contracts(source),
            read_only_by_default=True,
            max_requests=25,
        ),
        fixture_factory_binding=(
            _callable_binding(source_id, "fixture_factory")
            if history_supported
            else None
        ),
        live_fixture_factory_binding=(
            _callable_binding(source_id, "live_fixture_factory")
            if not history_supported
            else None
        ),
        fixture_count_oracle_binding=(
            _callable_binding(source_id, "fixture_count_oracle")
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
    if source.certification.evidence_id != spec.evidence_pack_id:
        raise RuntimeError(
            f"certification evidence pack mismatch for {spec.source_id!r}"
        )
    if source.certification.canary_id != spec.canary.canary_id:
        raise RuntimeError(f"certification canary mismatch for {spec.source_id!r}")
    has_bindings = (
        spec.fixture_factory_binding is not None
        and spec.fixture_count_oracle_binding is not None
        and spec.installation_seeder_binding is not None
    )
    if has_bindings != (source.history is not None):
        support = "supports" if source.history is not None else "does not support"
        raise RuntimeError(
            f"source {spec.source_id!r} {support} history but its "
            "certification callable bindings disagree"
        )
    if (spec.live_fixture_factory_binding is not None) != (
        source.history is None
    ):
        support = (
            "requires a live fixture"
            if source.history is None
            else "must not declare a live-only fixture"
        )
        raise RuntimeError(f"source {spec.source_id!r} {support}")


SOURCE_CERTIFICATION_SPECS: tuple[SourceCertificationSpec, ...] = tuple(
    _spec(source_id) for source_id in CANONICAL_SOURCE_IDS
)
SOURCE_CERTIFICATION_CATALOG: Mapping[str, SourceCertificationSpec] = MappingProxyType(
    {spec.source_id: spec for spec in SOURCE_CERTIFICATION_SPECS}
)

if tuple(SOURCE_CERTIFICATION_CATALOG) != CANONICAL_SOURCE_IDS:
    raise RuntimeError(
        "certification catalog coverage/order differs from source catalog"
    )
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
