"""Fail-closed certification contracts for ingestion sources.

The production source catalog declares *which* kit/evidence/canary belongs to
each source.  This package declares what those artifacts must contain and
evaluates whether they are strong enough for a release.  Local simulations are
valuable evidence, but can never silently stand in for a real-provider canary.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any, Literal


EvidenceKind = Literal["documented", "observed_live", "fyralis_specific"]
SuiteKind = Literal["historical", "live", "combined"]
CertificationState = Literal["unverified", "passed", "failed", "blocked"]
CanaryCleanupState = Literal["not_required", "passed", "failed", "blocked"]
CanaryOperationMutability = Literal["read", "mutation", "unclassified"]
CertificationBindingRole = Literal[
    "fixture_factory",
    "live_fixture_factory",
    "fixture_count_oracle",
    "installation_seeder",
]
_CERTIFICATION_STATES = frozenset({"unverified", "passed", "failed", "blocked"})
_SUITE_KINDS = frozenset({"historical", "live", "combined"})
_CALLABLE_REFERENCE_RE = re.compile(
    r"^[a-z_][a-z0-9_]*(?:\.[a-z_][a-z0-9_]*)*:"
    r"[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)*$"
)


class CertificationInvariantError(ValueError):
    """A certification declaration or result is internally inconsistent."""


def _nonempty(value: str, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise CertificationInvariantError(f"{field} must be non-empty")
    return value.strip()


def _aware(value: datetime | None, field: str) -> None:
    if value is not None and (value.tzinfo is None or value.utcoffset() is None):
        raise CertificationInvariantError(f"{field} must be timezone-aware")


def _state(value: str, field: str) -> None:
    if value not in _CERTIFICATION_STATES:
        raise CertificationInvariantError(
            f"{field} must be one of {sorted(_CERTIFICATION_STATES)}"
        )


def _strings(values: tuple[str, ...], field: str) -> None:
    if not isinstance(values, tuple):
        raise CertificationInvariantError(f"{field} must be a tuple")
    for value in values:
        _nonempty(value, f"{field} item")


@dataclass(frozen=True, slots=True)
class EvidenceReference:
    """One official or observed fact used by a source contract."""

    behavior_id: str
    kind: EvidenceKind
    uri: str
    api_version: str | None = None
    schema_sha256: str | None = None
    quota_uri: str | None = None
    verified_at: datetime | None = None
    notes: str = ""

    def __post_init__(self) -> None:
        _nonempty(self.behavior_id, "behavior_id")
        if self.kind not in {"documented", "observed_live", "fyralis_specific"}:
            raise CertificationInvariantError(f"invalid evidence kind {self.kind!r}")
        _nonempty(self.uri, "uri")
        _aware(self.verified_at, "verified_at")
        if self.schema_sha256 is not None:
            digest = self.schema_sha256.lower()
            if len(digest) != 64 or any(c not in "0123456789abcdef" for c in digest):
                raise CertificationInvariantError(
                    "schema_sha256 must be a lowercase SHA-256 digest"
                )

    @property
    def verified(self) -> bool:
        return self.verified_at is not None


@dataclass(frozen=True, slots=True)
class LoadSuite:
    """Deterministic local workload required for one source."""

    kind: SuiteKind
    operation_mix: tuple[str, ...]
    tenants: int = 2
    installations_per_tenant: int = 2
    replicas: int = 2
    warmup_seconds: int = 120
    stable_seconds: int = 900
    weekly_soak_seconds: int = 3600
    step_percent: int = 25
    search_tolerance_percent: int = 5

    def __post_init__(self) -> None:
        if self.kind not in {"historical", "live", "combined"}:
            raise CertificationInvariantError(f"invalid suite kind {self.kind!r}")
        if not self.operation_mix:
            raise CertificationInvariantError("operation_mix must not be empty")
        for operation in self.operation_mix:
            _nonempty(operation, "operation_mix item")
        for name in (
            "tenants",
            "installations_per_tenant",
            "replicas",
            "warmup_seconds",
            "stable_seconds",
            "weekly_soak_seconds",
            "step_percent",
            "search_tolerance_percent",
        ):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                raise CertificationInvariantError(f"{name} must be a positive integer")


@dataclass(frozen=True, slots=True)
class CanaryOperationContract:
    """Operation-bound mutability and cleanup policy for a live canary."""

    operation_id: str
    mutability: CanaryOperationMutability
    cleanup_action: str | None
    classification_basis: str

    def __post_init__(self) -> None:
        _nonempty(self.operation_id, "canary operation_id")
        if self.mutability not in {"read", "mutation", "unclassified"}:
            raise CertificationInvariantError(
                "canary operation mutability must be read, mutation, or "
                "unclassified"
            )
        _nonempty(self.classification_basis, "classification_basis")
        if self.mutability == "mutation":
            if self.cleanup_action is None:
                raise CertificationInvariantError(
                    "mutating canary operation requires a cleanup_action"
                )
            _nonempty(self.cleanup_action, "cleanup_action")
        elif self.cleanup_action is not None:
            raise CertificationInvariantError(
                "only mutating canary operations may declare cleanup_action"
            )


@dataclass(frozen=True, slots=True)
class CanaryDefinition:
    """Low-rate real-provider proof; never a saturation test."""

    canary_id: str
    credential_env_prefix: str
    account_type: str
    required_operations: tuple[str, ...]
    operation_contracts: tuple[CanaryOperationContract, ...]
    read_only_by_default: bool = True
    max_requests: int = 25

    def __post_init__(self) -> None:
        _nonempty(self.canary_id, "canary_id")
        _nonempty(self.credential_env_prefix, "credential_env_prefix")
        _nonempty(self.account_type, "account_type")
        _strings(self.required_operations, "required_operations")
        if not self.required_operations:
            raise CertificationInvariantError("required_operations must not be empty")
        if len(self.required_operations) != len(set(self.required_operations)):
            raise CertificationInvariantError(
                "required_operations must not contain duplicates"
            )
        if not isinstance(self.operation_contracts, tuple) or not all(
            isinstance(contract, CanaryOperationContract)
            for contract in self.operation_contracts
        ):
            raise CertificationInvariantError(
                "operation_contracts must be a tuple of "
                "CanaryOperationContract values"
            )
        operation_ids = tuple(
            contract.operation_id for contract in self.operation_contracts
        )
        if operation_ids != self.required_operations:
            raise CertificationInvariantError(
                "operation_contracts must cover required_operations exactly "
                "once and in order"
            )
        if (
            isinstance(self.max_requests, bool)
            or not isinstance(self.max_requests, int)
            or self.max_requests <= 0
        ):
            raise CertificationInvariantError("max_requests must be positive")
        if len(self.required_operations) > self.max_requests:
            raise CertificationInvariantError(
                "max_requests cannot be lower than required operation count"
            )
        if self.read_only_by_default and self.mutating_actions:
            # Declaring isolated writes is allowed, but must be obvious in the
            # account description rather than looking like a read-only canary.
            if "disposable" not in self.account_type.casefold():
                raise CertificationInvariantError(
                    "mutating canaries must name a disposable account type"
                )

    @property
    def mutating_actions(self) -> tuple[str, ...]:
        return tuple(
            contract.cleanup_action
            for contract in self.operation_contracts
            if contract.cleanup_action is not None
        )

    @property
    def unclassified_operations(self) -> tuple[str, ...]:
        return tuple(
            contract.operation_id
            for contract in self.operation_contracts
            if contract.mutability == "unclassified"
        )

    def operation_contract_for(
        self,
        operation_id: str,
    ) -> CanaryOperationContract:
        for contract in self.operation_contracts:
            if contract.operation_id == operation_id:
                return contract
        raise KeyError(operation_id)


@dataclass(frozen=True, slots=True)
class CertificationCallableBinding:
    """One source-owned callable used by its deterministic certification kit."""

    source_id: str
    role: CertificationBindingRole
    reference: str

    def __post_init__(self) -> None:
        _nonempty(self.source_id, "binding source_id")
        if self.role not in {
            "fixture_factory",
            "live_fixture_factory",
            "fixture_count_oracle",
            "installation_seeder",
        }:
            raise CertificationInvariantError(
                f"invalid certification binding role {self.role!r}"
            )
        reference = _nonempty(self.reference, "binding reference")
        if _CALLABLE_REFERENCE_RE.fullmatch(reference) is None:
            raise CertificationInvariantError(
                "binding reference must be 'module.path:callable'; "
                f"got {reference!r}"
            )


@dataclass(frozen=True, slots=True)
class SourceCertificationSpec:
    """The complete, immutable release gate for one canonical source."""

    source_id: str
    spec_version: str
    provider_api_version: str
    test_kit_id: str
    evidence_pack_id: str
    evidence_pack_version: str
    evidence_pack_sha256: str
    evidence: tuple[EvidenceReference, ...]
    required_scenarios: tuple[str, ...]
    simulator_capabilities: tuple[str, ...]
    load_suites: tuple[LoadSuite, ...]
    canary: CanaryDefinition
    fixture_factory_binding: CertificationCallableBinding | None = None
    live_fixture_factory_binding: CertificationCallableBinding | None = None
    fixture_count_oracle_binding: CertificationCallableBinding | None = None
    installation_seeder_binding: CertificationCallableBinding | None = None

    def __post_init__(self) -> None:
        _nonempty(self.source_id, "source_id")
        _nonempty(self.spec_version, "spec_version")
        _nonempty(self.provider_api_version, "provider_api_version")
        _nonempty(self.test_kit_id, "test_kit_id")
        _nonempty(self.evidence_pack_id, "evidence_pack_id")
        _nonempty(self.evidence_pack_version, "evidence_pack_version")
        digest = self.evidence_pack_sha256.lower()
        if len(digest) != 64 or any(
            character not in "0123456789abcdef" for character in digest
        ):
            raise CertificationInvariantError(
                "evidence_pack_sha256 must be a lowercase SHA-256 digest"
            )
        for field in ("evidence", "required_scenarios", "simulator_capabilities"):
            if not getattr(self, field):
                raise CertificationInvariantError(f"{field} must not be empty")
        if {suite.kind for suite in self.load_suites} != {
            "historical",
            "live",
            "combined",
        }:
            raise CertificationInvariantError(
                "load_suites must contain historical, live, and combined exactly once"
            )
        ids = [item.behavior_id for item in self.evidence]
        if len(ids) != len(set(ids)):
            raise CertificationInvariantError("evidence behavior_ids must be unique")
        _strings(self.required_scenarios, "required_scenarios")
        if len(self.required_scenarios) != len(set(self.required_scenarios)):
            raise CertificationInvariantError(
                "required_scenarios must not contain duplicates"
            )
        bindings = (
            ("fixture_factory", self.fixture_factory_binding),
            ("live_fixture_factory", self.live_fixture_factory_binding),
            ("fixture_count_oracle", self.fixture_count_oracle_binding),
            ("installation_seeder", self.installation_seeder_binding),
        )
        for expected_role, binding in bindings:
            if binding is None:
                continue
            if not isinstance(binding, CertificationCallableBinding):
                raise CertificationInvariantError(
                    f"{expected_role}_binding must be a " "CertificationCallableBinding"
                )
            if binding.source_id != self.source_id:
                raise CertificationInvariantError(
                    f"{expected_role} binding belongs to "
                    f"{binding.source_id!r}, not {self.source_id!r}"
                )
            if binding.role != expected_role:
                raise CertificationInvariantError(
                    f"{expected_role} binding declares role " f"{binding.role!r}"
                )
        historical_bindings = (
            self.fixture_factory_binding,
            self.fixture_count_oracle_binding,
            self.installation_seeder_binding,
        )
        declared_bindings = tuple(
            binding is not None for binding in historical_bindings
        )
        if len(set(declared_bindings)) != 1:
            raise CertificationInvariantError(
                "fixture_factory_binding, fixture_count_oracle_binding, and "
                "installation_seeder_binding must be declared together"
            )

    def declaration_hash(self) -> str:
        payload = json.dumps(
            asdict(self),
            sort_keys=True,
            separators=(",", ":"),
            default=lambda value: (
                value.astimezone(timezone.utc).isoformat()
                if isinstance(value, datetime)
                else str(value)
            ),
        ).encode("utf-8")
        return hashlib.sha256(payload).hexdigest()


@dataclass(frozen=True, slots=True)
class SuiteResult:
    kind: SuiteKind
    state: CertificationState
    artifact_uri: str | None = None
    started_at: datetime | None = None
    completed_at: datetime | None = None
    metrics: tuple[tuple[str, float], ...] = ()
    limiting_component: str | None = None
    failures: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if self.kind not in _SUITE_KINDS:
            raise CertificationInvariantError(f"invalid suite kind {self.kind!r}")
        _state(self.state, "suite state")
        _aware(self.started_at, "started_at")
        _aware(self.completed_at, "completed_at")
        if (
            self.started_at is not None
            and self.completed_at is not None
            and self.completed_at < self.started_at
        ):
            raise CertificationInvariantError("completed_at cannot precede started_at")
        if self.artifact_uri is not None:
            _nonempty(self.artifact_uri, "artifact_uri")
        if self.limiting_component is not None:
            _nonempty(self.limiting_component, "limiting_component")
        _strings(self.failures, "failures")
        names: set[str] = set()
        for name, value in self.metrics:
            _nonempty(name, "metric name")
            if name in names:
                raise CertificationInvariantError(
                    f"metrics contains duplicate {name!r}"
                )
            names.add(name)
            if (
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or not math.isfinite(float(value))
            ):
                raise CertificationInvariantError(
                    f"metric {name!r} must be a finite number"
                )
        if self.state == "passed" and (
            self.failures or not self.artifact_uri or not self.limiting_component
        ):
            raise CertificationInvariantError(
                "a passed suite requires an artifact, limiting component, "
                "and no failures"
            )


@dataclass(frozen=True, slots=True)
class ScenarioResult:
    """Artifact-backed outcome for one required local correctness scenario."""

    scenario_id: str
    state: CertificationState
    artifact_uri: str
    failures: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        _nonempty(self.scenario_id, "scenario_id")
        _state(self.state, "scenario state")
        _nonempty(self.artifact_uri, "scenario artifact_uri")
        _strings(self.failures, "scenario failures")
        if self.state == "passed" and self.failures:
            raise CertificationInvariantError(
                "a passed scenario result cannot contain failures"
            )


@dataclass(frozen=True, slots=True)
class CanaryOperationResult:
    """Artifact-backed outcome for one declared real-provider operation."""

    operation_id: str
    state: CertificationState
    artifact_uri: str
    failures: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        _nonempty(self.operation_id, "operation_id")
        _state(self.state, "canary operation state")
        _nonempty(self.artifact_uri, "canary operation artifact_uri")
        _strings(self.failures, "canary operation failures")
        if self.state == "passed" and self.failures:
            raise CertificationInvariantError(
                "a passed canary operation cannot contain failures"
            )


@dataclass(frozen=True, slots=True)
class CanaryResult:
    state: CertificationState
    operation_results: tuple[CanaryOperationResult, ...]
    tested_at: datetime | None = None
    account_type: str | None = None
    api_version: str | None = None
    artifact_uri: str | None = None
    request_count: int = 0
    account_identity_sha256: str | None = None
    mutation_actions: tuple[str, ...] = ()
    cleanup_state: CanaryCleanupState = "not_required"
    failures: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        _state(self.state, "canary state")
        if not isinstance(self.operation_results, tuple) or not all(
            isinstance(result, CanaryOperationResult)
            for result in self.operation_results
        ):
            raise CertificationInvariantError(
                "operation_results must be a tuple of CanaryOperationResult values"
            )
        operation_ids = [result.operation_id for result in self.operation_results]
        if len(operation_ids) != len(set(operation_ids)):
            raise CertificationInvariantError(
                "operation_results must not contain duplicate operation IDs"
            )
        _aware(self.tested_at, "tested_at")
        for name in ("account_type", "api_version", "artifact_uri"):
            value = getattr(self, name)
            if value is not None:
                _nonempty(value, name)
        if (
            isinstance(self.request_count, bool)
            or not isinstance(self.request_count, int)
            or self.request_count < 0
        ):
            raise CertificationInvariantError(
                "request_count must be a non-negative integer"
            )
        if self.account_identity_sha256 is not None:
            digest = self.account_identity_sha256.lower()
            if len(digest) != 64 or any(
                character not in "0123456789abcdef" for character in digest
            ):
                raise CertificationInvariantError(
                    "account_identity_sha256 must be a lowercase SHA-256 digest"
                )
        _strings(self.mutation_actions, "mutation_actions")
        if len(self.mutation_actions) != len(set(self.mutation_actions)):
            raise CertificationInvariantError(
                "mutation_actions must not contain duplicates"
            )
        if self.cleanup_state not in {
            "not_required",
            "passed",
            "failed",
            "blocked",
        }:
            raise CertificationInvariantError(
                "cleanup_state must be not_required, passed, failed, or blocked"
            )
        _strings(self.failures, "failures")
        if self.state == "passed":
            required = (
                self.tested_at,
                self.account_type,
                self.api_version,
                self.artifact_uri,
                self.account_identity_sha256,
            )
            if (
                any(value is None for value in required)
                or self.request_count <= 0
                or self.failures
            ):
                raise CertificationInvariantError(
                    "a passed canary requires timestamp, account/API metadata, "
                    "account identity, positive request count, artifact, and "
                    "no failures"
                )
            if self.mutation_actions and self.cleanup_state != "passed":
                raise CertificationInvariantError(
                    "a mutating passed canary requires successful cleanup"
                )
            if not self.mutation_actions and self.cleanup_state != "not_required":
                raise CertificationInvariantError(
                    "a read-only passed canary must declare cleanup not_required"
                )


@dataclass(frozen=True, slots=True)
class CertificationInput:
    """Evidence supplied to the fail-closed evaluator."""

    spec_hash: str
    local_correctness: CertificationState
    local_correctness_artifact: str | None
    scenario_results: tuple[ScenarioResult, ...]
    provider_safe_suites: tuple[SuiteResult, ...]
    fyralis_ceiling_suites: tuple[SuiteResult, ...]
    fault_recovery_suites: tuple[SuiteResult, ...]
    canary: CanaryResult
    legacy_reference_count: int
    skipped_tests: tuple[str, ...] = ()
    todos: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        digest = self.spec_hash.lower()
        if len(digest) != 64 or any(
            character not in "0123456789abcdef" for character in digest
        ):
            raise CertificationInvariantError(
                "spec_hash must be a lowercase SHA-256 digest"
            )
        _state(self.local_correctness, "local_correctness")
        if self.local_correctness_artifact is not None:
            _nonempty(
                self.local_correctness_artifact,
                "local_correctness_artifact",
            )
        if not isinstance(self.scenario_results, tuple) or not all(
            isinstance(result, ScenarioResult) for result in self.scenario_results
        ):
            raise CertificationInvariantError(
                "scenario_results must be a tuple of ScenarioResult values"
            )
        scenario_ids = [result.scenario_id for result in self.scenario_results]
        if len(scenario_ids) != len(set(scenario_ids)):
            raise CertificationInvariantError(
                "scenario_results must not contain duplicate scenario IDs"
            )
        for field in (
            "provider_safe_suites",
            "fyralis_ceiling_suites",
            "fault_recovery_suites",
        ):
            suites = getattr(self, field)
            if not isinstance(suites, tuple) or not all(
                isinstance(suite, SuiteResult) for suite in suites
            ):
                raise CertificationInvariantError(
                    f"{field} must be a tuple of SuiteResult values"
                )
        if not isinstance(self.canary, CanaryResult):
            raise CertificationInvariantError("canary must be a CanaryResult")
        if (
            isinstance(self.legacy_reference_count, bool)
            or not isinstance(self.legacy_reference_count, int)
            or self.legacy_reference_count < 0
        ):
            raise CertificationInvariantError(
                "legacy_reference_count must be a non-negative integer"
            )
        _strings(self.skipped_tests, "skipped_tests")
        _strings(self.todos, "todos")


@dataclass(frozen=True, slots=True)
class CertificationDecision:
    source_id: str
    state: CertificationState
    spec_hash: str
    evaluated_at: datetime
    failures: tuple[str, ...]
    artifact: dict[str, Any]
