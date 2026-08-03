from __future__ import annotations

from typing import Protocol, runtime_checkable

import pytest

from services.ingest.connector_conformance.fakes import make_binding_context
from services.ingest.connector_runtime.diagnostics import (
    DiagnosticSeverity,
    RegistryDiagnostic,
)
from services.ingest.connector_runtime.registry import (
    ConnectorCandidate,
    ConnectorRegistryBuilder,
    HostCompatibility,
    RegistryBuildResult,
    RegistryStatus,
)
from services.ingest.connector_runtime.tests.helpers import (
    ExampleConnector,
    ExampleIdentity,
    make_candidate,
    make_manifest,
)
from services.ingest.source_contract.capabilities import IDENTITY_V1
from services.ingest.source_contract.capabilities.semantic import (
    IdentityCapability,
)
from services.ingest.source_contract.connector import (
    CapabilityKey,
    GrantedAuthority,
    StaticBoundConnector,
)
from services.ingest.source_contract.errors import (
    BindingError,
    ConnectorNotFoundError,
    RegistryBuildError,
)
from services.ingest.source_contract.manifest import (
    CapabilityRef,
    ConnectorManifest,
)
from services.ingest.source_contract.versioning import SemanticVersion


def _error_codes(result: RegistryBuildResult) -> set[str]:
    return {
        diagnostic.code
        for diagnostic in result.diagnostics
        if diagnostic.severity is DiagnosticSeverity.ERROR
    }


def test_builds_queryable_immutable_snapshot() -> None:
    manifest = make_manifest(aliases=("example_legacy",))
    candidate, _ = make_candidate(manifest)

    registry = ConnectorRegistryBuilder().add(candidate).build()

    assert registry.connector_ids() == ("fyralis/example",)
    assert registry.require("fyralis/example").manifest == manifest
    assert registry.for_source("example").connector_id == "fyralis/example"
    assert registry.for_source("example_legacy").connector_id == "fyralis/example"
    assert registry.list_by_capability(IDENTITY_V1.ref) == (
        registry.require("fyralis/example"),
    )
    description = registry.describe("fyralis/example")
    assert description.negotiated_contract_version == "1.0.0"
    assert description.capabilities == (IDENTITY_V1.ref,)
    with pytest.raises(AttributeError):
        description.source = "changed"  # type: ignore[misc]


def test_health_has_deterministic_snapshot_fingerprint() -> None:
    candidate, _ = make_candidate()
    first = ConnectorRegistryBuilder().add(candidate).build().health()
    second = ConnectorRegistryBuilder().add(candidate).build().health()

    assert first.status is RegistryStatus.READY
    assert first.healthy
    assert first.connector_count == 1
    assert first.fingerprint == second.fingerprint
    assert len(first.fingerprint) == 64


def test_unknown_connector_and_source_are_typed_errors() -> None:
    candidate, _ = make_candidate()
    registry = ConnectorRegistryBuilder().add(candidate).build()

    with pytest.raises(ConnectorNotFoundError):
        registry.require("fyralis/missing")
    with pytest.raises(ConnectorNotFoundError):
        registry.for_source("missing")


@pytest.mark.parametrize(
    ("second_manifest", "expected_code"),
    [
        (
            make_manifest(connector_id="fyralis/example", source="other"),
            "duplicate_connector_id",
        ),
        (
            make_manifest(connector_id="fyralis/other", source="example"),
            "duplicate_source",
        ),
        (
            make_manifest(
                connector_id="fyralis/other",
                source="other",
                aliases=("example",),
            ),
            "duplicate_source",
        ),
    ],
)
def test_duplicate_identity_rejects_without_factory_activation(
    second_manifest: ConnectorManifest,
    expected_code: str,
) -> None:
    calls: list[str] = []
    first = make_manifest()

    def candidate_for(manifest: ConnectorManifest) -> ConnectorCandidate:
        def unexpected_factory() -> object:
            calls.append("called")
            return object()

        return ConnectorCandidate(
            manifest=manifest,
            factory=unexpected_factory,  # type: ignore[arg-type]
            capability_keys=(IDENTITY_V1,),
        )

    result = (
        ConnectorRegistryBuilder()
        .add(candidate_for(first))
        .add(candidate_for(second_manifest))
        .build_result()
    )

    assert result.registry is None
    assert expected_code in _error_codes(result)
    assert calls == []


def test_contract_incompatibility_does_not_activate_factory() -> None:
    manifest = make_manifest(contract=">=2.0,<3.0")
    calls = 0

    def factory() -> ExampleConnector:
        nonlocal calls
        calls += 1
        return ExampleConnector(manifest)

    candidate = ConnectorCandidate(manifest, factory, (IDENTITY_V1,))
    result = ConnectorRegistryBuilder().add(candidate).build_result()

    assert result.registry is None
    assert _error_codes(result) == {"contract_incompatible"}
    assert calls == 0


def test_required_unsupported_capability_rejects_candidate() -> None:
    manifest = make_manifest(
        capabilities=(("experimental.unknown", 1, True),)
    )

    @runtime_checkable
    class UnknownCapability(Protocol):
        def invoke(self) -> None: ...

    key: CapabilityKey[UnknownCapability] = CapabilityKey(
        CapabilityRef(id="experimental.unknown", version=1),
        UnknownCapability,
    )
    candidate = ConnectorCandidate(
        manifest,
        lambda: ExampleConnector(manifest),
        (key,),
    )

    result = ConnectorRegistryBuilder().add(candidate).build_result()

    assert result.registry is None
    assert "required_capability_unsupported" in _error_codes(result)


def test_optional_unsupported_capability_is_omitted_with_warning() -> None:
    manifest = make_manifest(
        capabilities=(("experimental.unknown", 1, False),)
    )

    @runtime_checkable
    class UnknownCapability(Protocol):
        def invoke(self) -> None: ...

    key: CapabilityKey[UnknownCapability] = CapabilityKey(
        CapabilityRef(id="experimental.unknown", version=1),
        UnknownCapability,
    )
    candidate = ConnectorCandidate(
        manifest,
        lambda: ExampleConnector(manifest),
        (key,),
    )

    registry = ConnectorRegistryBuilder().add(candidate).build()

    assert registry.describe(manifest.connector_id).capabilities == ()
    health = registry.health()
    assert health.status is RegistryStatus.DEGRADED
    assert not health.healthy
    assert any(
        item.code == "optional_capability_omitted"
        for item in health.diagnostics
    )


@pytest.mark.parametrize(
    ("manifest_capabilities", "candidate_keys", "expected_code"),
    [
        (
            (("semantic.identity", 1, True),),
            (),
            "declared_capability_missing",
        ),
        (
            (),
            (IDENTITY_V1,),
            "undeclared_capability",
        ),
    ],
)
def test_manifest_and_implementation_capabilities_must_match(
    manifest_capabilities: tuple[tuple[str, int, bool], ...],
    candidate_keys: tuple[CapabilityKey[object], ...],
    expected_code: str,
) -> None:
    manifest = make_manifest(capabilities=manifest_capabilities)
    candidate = ConnectorCandidate(
        manifest,
        lambda: ExampleConnector(manifest),
        candidate_keys,
    )

    result = ConnectorRegistryBuilder().add(candidate).build_result()

    assert result.registry is None
    assert expected_code in _error_codes(result)


def test_host_and_candidate_interface_identity_must_match() -> None:
    @runtime_checkable
    class DifferentIdentity(Protocol):
        def external_id(self, value: object) -> str: ...

    mismatched_key: CapabilityKey[DifferentIdentity] = CapabilityKey(
        IDENTITY_V1.ref,
        DifferentIdentity,
    )
    manifest = make_manifest()
    candidate = ConnectorCandidate(
        manifest,
        lambda: ExampleConnector(manifest),
        (mismatched_key,),
    )

    result = ConnectorRegistryBuilder().add(candidate).build_result()

    assert result.registry is None
    assert _error_codes(result) == {"capability_interface_mismatch"}


def test_custom_manifest_policy_can_reject_before_activation() -> None:
    manifest = make_manifest()
    activated = False

    def factory() -> ExampleConnector:
        nonlocal activated
        activated = True
        return ExampleConnector(manifest)

    def reject_policy(
        _manifest: ConnectorManifest,
    ) -> tuple[RegistryDiagnostic, ...]:
        return (
            RegistryDiagnostic(
                severity=DiagnosticSeverity.ERROR,
                code="permission_policy_denied",
                message="requested host is not allowed in this environment",
                connector_id=manifest.connector_id,
            ),
        )

    result = ConnectorRegistryBuilder(validators=(reject_policy,)).add(
        ConnectorCandidate(manifest, factory, (IDENTITY_V1,))
    ).build_result()

    assert result.registry is None
    assert _error_codes(result) == {"permission_policy_denied"}
    assert not activated


def test_manifest_policy_exception_is_redacted_diagnostic() -> None:
    manifest = make_manifest()

    def broken_policy(_manifest: ConnectorManifest) -> tuple[()]:
        raise RuntimeError("sensitive policy details")

    result = ConnectorRegistryBuilder(validators=(broken_policy,)).add(
        ConnectorCandidate(
            manifest,
            lambda: ExampleConnector(manifest),
            (IDENTITY_V1,),
        )
    ).build_result()

    assert _error_codes(result) == {"manifest_validator_failed"}
    diagnostic = next(
        item
        for item in result.diagnostics
        if item.code == "manifest_validator_failed"
    )
    assert "RuntimeError" in diagnostic.message
    assert "sensitive" not in diagnostic.message


def test_factory_failures_and_manifest_mismatch_are_diagnostics() -> None:
    manifest = make_manifest()

    def fail_factory() -> ExampleConnector:
        raise RuntimeError("broken factory")

    failing = ConnectorCandidate(manifest, fail_factory, (IDENTITY_V1,))
    failed_result = ConnectorRegistryBuilder().add(failing).build_result()
    assert failed_result.registry is None
    assert _error_codes(failed_result) == {"factory_failed"}

    other_manifest = make_manifest(
        connector_id="fyralis/other",
        source="other",
    )
    mismatch = ConnectorCandidate(
        manifest,
        lambda: ExampleConnector(other_manifest),
        (IDENTITY_V1,),
    )
    mismatch_result = ConnectorRegistryBuilder().add(mismatch).build_result()
    assert mismatch_result.registry is None
    assert _error_codes(mismatch_result) == {
        "manifest_implementation_mismatch"
    }


def test_invalid_factory_object_is_diagnostic_and_build_raises() -> None:
    manifest = make_manifest()
    candidate = ConnectorCandidate(
        manifest,
        lambda: object(),  # type: ignore[arg-type,return-value]
        (IDENTITY_V1,),
    )
    result = ConnectorRegistryBuilder().add(candidate).build_result()

    assert _error_codes(result) == {"invalid_connector_object"}
    with pytest.raises(RegistryBuildError):
        result.require_registry()


def test_factory_activation_is_deterministic() -> None:
    activation_order: list[str] = []

    def candidate(connector_id: str, source: str) -> ConnectorCandidate:
        manifest = make_manifest(connector_id=connector_id, source=source)

        def factory() -> ExampleConnector:
            activation_order.append(connector_id)
            return ExampleConnector(manifest)

        return ConnectorCandidate(manifest, factory, (IDENTITY_V1,))

    ConnectorRegistryBuilder().extend(
        (
            candidate("fyralis/zeta", "zeta"),
            candidate("fyralis/alpha", "alpha"),
        )
    ).build()

    assert activation_order == ["fyralis/alpha", "fyralis/zeta"]


def test_binding_validates_authority_before_connector_activation() -> None:
    candidate, connector = make_candidate()
    registry = ConnectorRegistryBuilder().add(candidate).build()
    context = make_binding_context(
        candidate.manifest,
        authority=GrantedAuthority(),
    )

    with pytest.raises(BindingError) as captured:
        registry.resolve_for_install(context)

    assert connector.bind_calls == 0
    assert captured.value.details["missing_secret_slots"] == ("api_token",)


def test_binding_validates_installation_and_capability_shape() -> None:
    manifest = make_manifest()
    context = make_binding_context(manifest)
    wrong_installation = context.installation.model_copy(
        update={"id": context.installation.tenant_id}
    )
    wrong_binding_connector = ExampleConnector(
        manifest,
        binding_factory=lambda _context: StaticBoundConnector(
            wrong_installation,
            {IDENTITY_V1.ref: ExampleIdentity()},
        ),
    )
    wrong_candidate, _ = make_candidate(
        manifest,
        connector=wrong_binding_connector,
    )
    wrong_registry = ConnectorRegistryBuilder().add(wrong_candidate).build()
    with pytest.raises(BindingError, match="different installation"):
        wrong_registry.resolve_for_install(context)

    invalid_capability_connector = ExampleConnector(
        manifest,
        capabilities={IDENTITY_V1.ref: object()},
    )
    invalid_candidate, _ = make_candidate(
        manifest,
        connector=invalid_capability_connector,
    )
    invalid_registry = ConnectorRegistryBuilder().add(invalid_candidate).build()
    with pytest.raises(BindingError, match="invalid implementation"):
        invalid_registry.resolve_for_install(context)


def test_successful_binding_resolves_typed_capability() -> None:
    candidate, connector = make_candidate()
    registry = ConnectorRegistryBuilder().add(candidate).build()
    context = make_binding_context(candidate.manifest)

    binding = registry.resolve_for_install(context)

    implementation = binding.capability(IDENTITY_V1)
    assert implementation is not None
    assert isinstance(implementation, IdentityCapability)
    assert connector.bind_calls == 1


def test_host_selects_highest_mutually_supported_contract_version() -> None:
    host = HostCompatibility(
        contract_versions=(
            SemanticVersion.parse("1.0.0"),
            SemanticVersion.parse("1.5.0"),
        )
    )
    candidate, _ = make_candidate()

    registry = ConnectorRegistryBuilder(host).add(candidate).build()

    assert (
        registry.describe(candidate.manifest.connector_id)
        .negotiated_contract_version
        == "1.5.0"
    )


def test_host_can_require_approved_conformance_evidence() -> None:
    candidate, connector = make_candidate()
    strict_without_evidence = HostCompatibility(
        contract_versions=(SemanticVersion.parse("1.0.0"),),
        require_conformance_fingerprint=True,
    )
    missing = (
        ConnectorRegistryBuilder(strict_without_evidence)
        .add(candidate)
        .build_result()
    )
    assert _error_codes(missing) == {"conformance_evidence_missing"}

    fingerprint = "a" * 64
    attested = ConnectorCandidate(
        manifest=candidate.manifest,
        factory=lambda: connector,
        capability_keys=candidate.capability_keys,
        conformance_fingerprint=fingerprint,
    )
    strict_with_approval = HostCompatibility(
        contract_versions=(SemanticVersion.parse("1.0.0"),),
        require_conformance_fingerprint=True,
        approved_conformance_fingerprints=frozenset({fingerprint}),
    )
    registry = (
        ConnectorRegistryBuilder(strict_with_approval).add(attested).build()
    )
    assert (
        registry.describe(candidate.manifest.connector_id)
        .conformance_fingerprint
        == fingerprint
    )


def test_conformance_fingerprint_is_a_sha256_value() -> None:
    candidate, connector = make_candidate()
    with pytest.raises(ValueError, match="lowercase SHA-256"):
        ConnectorCandidate(
            manifest=candidate.manifest,
            factory=lambda: connector,
            capability_keys=candidate.capability_keys,
            conformance_fingerprint="not-a-fingerprint",
        )
