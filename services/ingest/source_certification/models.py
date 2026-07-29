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
LoadOperationKind = Literal["data", "control"]
LoadExecutionFrequency = Literal["per_item", "once_per_trial", "periodic"]
LoadCardinality = Literal["none", "exactly_one", "zero_or_more", "one_or_more"]
LoadCursorApplicability = Literal["required", "optional", "not_applicable"]
CertificationState = Literal[
    "unverified",
    "passed",
    "failed",
    "blocked",
    "not_applicable",
]
CanaryCleanupState = Literal["not_required", "passed", "failed", "blocked"]
CanaryOperationMutability = Literal["read", "mutation", "unclassified"]
CertificationBindingRole = Literal[
    "fixture_factory",
    "live_fixture_factory",
    "fixture_count_oracle",
    "installation_seeder",
]
_CERTIFICATION_STATES = frozenset(
    {"unverified", "passed", "failed", "blocked", "not_applicable"}
)
_SUITE_KINDS = frozenset({"historical", "live", "combined"})
_CALLABLE_REFERENCE_RE = re.compile(
    r"^[a-z_][a-z0-9_]*(?:\.[a-z_][a-z0-9_]*)*:"
    r"[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)*$"
)
_LOAD_ID_RE = re.compile(r"^[a-z][a-z0-9_.-]*$")
_PROVIDER_OPERATION_ID_RE = re.compile(r"^[A-Za-z0-9_./@{}:-]+$")


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
class LoadQuotaMapping:
    """One provider operation/bucket charge an executable load operation may use."""

    operation_id: str
    quota_bucket: str | None
    units_per_request: float

    def __post_init__(self) -> None:
        operation_id = _nonempty(self.operation_id, "quota operation_id")
        if _PROVIDER_OPERATION_ID_RE.fullmatch(operation_id) is None:
            raise CertificationInvariantError(
                f"invalid quota operation_id {operation_id!r}"
            )
        if self.quota_bucket is not None:
            bucket = _nonempty(self.quota_bucket, "quota_bucket")
            if _LOAD_ID_RE.fullmatch(bucket) is None:
                raise CertificationInvariantError(
                    f"invalid quota_bucket {bucket!r}"
                )
        if (
            isinstance(self.units_per_request, bool)
            or not isinstance(self.units_per_request, (int, float))
            or not math.isfinite(float(self.units_per_request))
            or self.units_per_request <= 0
        ):
            raise CertificationInvariantError(
                "units_per_request must be a finite positive number"
            )

    def to_dict(self) -> dict[str, object]:
        return {
            "operation_id": self.operation_id,
            "quota_bucket": self.quota_bucket,
            "units_per_request": float(self.units_per_request),
        }


@dataclass(frozen=True, slots=True)
class ExecutableLoadOperation:
    """One bounded callable and its operation-aware receipt contract."""

    operation_id: str
    executable_binding: str
    evidence_id: str
    kind: LoadOperationKind
    execution_frequency: LoadExecutionFrequency
    raw_cardinality: LoadCardinality
    normalized_cardinality: LoadCardinality
    observation_cardinality: LoadCardinality
    cursor_applicability: LoadCursorApplicability
    receipt_proof_requirements: tuple[str, ...]
    quota_mappings: tuple[LoadQuotaMapping, ...] = ()
    selection_weight: int | None = None
    cadence_seconds: float | None = None
    trial_position: Literal["before_load", "after_load"] | None = None

    def __post_init__(self) -> None:
        operation_id = _nonempty(self.operation_id, "load operation_id")
        if _LOAD_ID_RE.fullmatch(operation_id) is None:
            raise CertificationInvariantError(
                f"invalid load operation_id {operation_id!r}"
            )
        binding = _nonempty(self.executable_binding, "executable_binding")
        if _CALLABLE_REFERENCE_RE.fullmatch(binding) is None:
            raise CertificationInvariantError(
                "executable_binding must be 'module.path:callable'; "
                f"got {binding!r}"
            )
        evidence_id = _nonempty(self.evidence_id, "load evidence_id")
        if _LOAD_ID_RE.fullmatch(evidence_id) is None:
            raise CertificationInvariantError(
                f"invalid load evidence_id {evidence_id!r}"
            )
        if self.kind not in {"data", "control"}:
            raise CertificationInvariantError(
                f"invalid load operation kind {self.kind!r}"
            )
        if self.execution_frequency not in {
            "per_item",
            "once_per_trial",
            "periodic",
        }:
            raise CertificationInvariantError(
                "invalid load execution_frequency "
                f"{self.execution_frequency!r}"
            )
        if self.raw_cardinality not in {
            "none",
            "exactly_one",
            "zero_or_more",
            "one_or_more",
        }:
            raise CertificationInvariantError(
                f"invalid raw_cardinality {self.raw_cardinality!r}"
            )
        if self.observation_cardinality not in {
            "none",
            "exactly_one",
            "zero_or_more",
            "one_or_more",
        }:
            raise CertificationInvariantError(
                "invalid observation_cardinality "
                f"{self.observation_cardinality!r}"
            )
        if self.normalized_cardinality not in {
            "none",
            "exactly_one",
            "zero_or_more",
            "one_or_more",
        }:
            raise CertificationInvariantError(
                "invalid normalized_cardinality "
                f"{self.normalized_cardinality!r}"
            )
        if self.cursor_applicability not in {
            "required",
            "optional",
            "not_applicable",
        }:
            raise CertificationInvariantError(
                "invalid cursor_applicability "
                f"{self.cursor_applicability!r}"
            )
        _strings(
            self.receipt_proof_requirements,
            "receipt_proof_requirements",
        )
        if (
            not self.receipt_proof_requirements
            or len(self.receipt_proof_requirements)
            != len(set(self.receipt_proof_requirements))
            or any(
                _LOAD_ID_RE.fullmatch(proof_id) is None
                for proof_id in self.receipt_proof_requirements
            )
        ):
            raise CertificationInvariantError(
                "receipt_proof_requirements must contain unique valid IDs"
            )
        if "binding_invocation" not in self.receipt_proof_requirements:
            raise CertificationInvariantError(
                "receipt proof must require binding_invocation"
            )
        if not isinstance(self.quota_mappings, tuple) or not all(
            isinstance(mapping, LoadQuotaMapping)
            for mapping in self.quota_mappings
        ):
            raise CertificationInvariantError(
                "quota_mappings must be a tuple of LoadQuotaMapping values"
            )
        quota_ids = tuple(
            (mapping.operation_id, mapping.quota_bucket)
            for mapping in self.quota_mappings
        )
        if len(quota_ids) != len(set(quota_ids)):
            raise CertificationInvariantError(
                "quota_mappings must not contain duplicate operation/bucket pairs"
            )
        if bool(self.quota_mappings) != (
            "quota_mapping" in self.receipt_proof_requirements
        ):
            raise CertificationInvariantError(
                "quota_mapping receipt proof must match quota_mappings presence"
            )

        if self.execution_frequency == "per_item":
            if self.kind != "data":
                raise CertificationInvariantError(
                    "per_item load operations must be data operations"
                )
            if (
                isinstance(self.selection_weight, bool)
                or not isinstance(self.selection_weight, int)
                or self.selection_weight <= 0
            ):
                raise CertificationInvariantError(
                    "per_item operations require a positive selection_weight"
                )
            if self.cadence_seconds is not None:
                raise CertificationInvariantError(
                    "per_item operations cannot declare cadence_seconds"
                )
            if self.trial_position is not None:
                raise CertificationInvariantError(
                    "per_item operations cannot declare trial_position"
                )
        else:
            if self.kind != "control":
                raise CertificationInvariantError(
                    "once_per_trial/periodic operations must be control operations"
                )
            if self.selection_weight is not None:
                raise CertificationInvariantError(
                    "control operations cannot declare selection_weight"
                )
            if self.execution_frequency == "once_per_trial":
                if self.cadence_seconds is not None:
                    raise CertificationInvariantError(
                        "once_per_trial operations cannot declare cadence_seconds"
                    )
                if self.trial_position not in {
                    "before_load",
                    "after_load",
                }:
                    raise CertificationInvariantError(
                        "once_per_trial operations require a trial_position"
                    )
            elif (
                isinstance(self.cadence_seconds, bool)
                or not isinstance(self.cadence_seconds, (int, float))
                or not math.isfinite(float(self.cadence_seconds))
                or self.cadence_seconds <= 0
            ):
                raise CertificationInvariantError(
                    "periodic operations require finite positive cadence_seconds"
                )
            elif self.trial_position is not None:
                raise CertificationInvariantError(
                    "periodic operations cannot declare trial_position"
                )

        data_proofs = {
            "raw_s3",
            "raw_kafka",
            "normalized_kafka",
            "observation",
            "t1",
            "replica_processing",
        }
        if self.kind == "control":
            if (
                self.raw_cardinality != "none"
                or self.normalized_cardinality != "none"
                or self.observation_cardinality != "none"
                or self.cursor_applicability != "not_applicable"
                or data_proofs.intersection(self.receipt_proof_requirements)
            ):
                raise CertificationInvariantError(
                    "control operations cannot claim data/cursor output proofs"
                )
        else:
            if (
                self.raw_cardinality == "none"
                or self.normalized_cardinality == "none"
                or self.observation_cardinality == "none"
                or not data_proofs.issubset(self.receipt_proof_requirements)
            ):
                raise CertificationInvariantError(
                    "data operations require non-none cardinalities and all "
                    "exact pipeline receipt proofs"
                )
            has_cursor_proof = (
                "cursor_consistency" in self.receipt_proof_requirements
            )
            if (self.cursor_applicability == "required") != has_cursor_proof:
                raise CertificationInvariantError(
                    "cursor_consistency proof is required exactly when cursor "
                    "applicability is required"
                )

    @property
    def declaration_sha256(self) -> str:
        payload = json.dumps(
            self.to_dict(),
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        return hashlib.sha256(payload).hexdigest()

    def to_dict(self) -> dict[str, object]:
        return {
            "operation_id": self.operation_id,
            "executable_binding": self.executable_binding,
            "evidence_id": self.evidence_id,
            "kind": self.kind,
            "execution_frequency": self.execution_frequency,
            "selection_weight": self.selection_weight,
            "cadence_seconds": (
                float(self.cadence_seconds)
                if self.cadence_seconds is not None
                else None
            ),
            "trial_position": self.trial_position,
            "raw_cardinality": self.raw_cardinality,
            "normalized_cardinality": self.normalized_cardinality,
            "observation_cardinality": self.observation_cardinality,
            "cursor_applicability": self.cursor_applicability,
            "quota_mappings": [
                mapping.to_dict() for mapping in self.quota_mappings
            ],
            "receipt_proof_requirements": list(
                self.receipt_proof_requirements
            ),
        }


@dataclass(frozen=True, slots=True)
class LoadOperationContractAbsence:
    """Explicit proof that a legacy semantic has no bounded load callable."""

    operation_id: str
    evidence_id: str
    missing_contract: Literal["bounded_callable"]
    reason: str
    blocks_execution: bool

    def __post_init__(self) -> None:
        for field_name in ("operation_id", "evidence_id"):
            value = _nonempty(getattr(self, field_name), field_name)
            if _LOAD_ID_RE.fullmatch(value) is None:
                raise CertificationInvariantError(
                    f"invalid load contract absence {field_name} {value!r}"
                )
        if self.missing_contract != "bounded_callable":
            raise CertificationInvariantError(
                "load contract absence must identify bounded_callable"
            )
        _nonempty(self.reason, "load contract absence reason")
        if not isinstance(self.blocks_execution, bool):
            raise CertificationInvariantError(
                "load contract absence blocks_execution must be a boolean"
            )

    def to_dict(self) -> dict[str, object]:
        return {
            "operation_id": self.operation_id,
            "evidence_id": self.evidence_id,
            "missing_contract": self.missing_contract,
            "reason": self.reason,
            "blocks_execution": self.blocks_execution,
        }


@dataclass(frozen=True, slots=True)
class LoadSuiteNonApplicability:
    """Evidence that a suite shape does not exist for this source."""

    evidence_id: str
    reason: str

    def __post_init__(self) -> None:
        evidence_id = _nonempty(self.evidence_id, "non-applicable evidence_id")
        if _LOAD_ID_RE.fullmatch(evidence_id) is None:
            raise CertificationInvariantError(
                f"invalid non-applicable evidence_id {evidence_id!r}"
            )
        _nonempty(self.reason, "non-applicable reason")

    def to_dict(self) -> dict[str, str]:
        return {"evidence_id": self.evidence_id, "reason": self.reason}


@dataclass(frozen=True, slots=True)
class LoadSuite:
    """Deterministic local workload required for one source.

    ``operation_mix`` is an optional, temporary read-only compatibility
    projection for
    historical diagnostic readers.  Active execution must use
    ``executable_operations`` and :meth:`execution_workload_dict`; internal
    semantic stages are never treated as offerable work merely because they
    appear in the compatibility projection.
    """

    kind: SuiteKind
    operation_mix: tuple[str, ...] = ()
    executable_operations: tuple[ExecutableLoadOperation, ...] = ()
    contract_absence_assertions: tuple[LoadOperationContractAbsence, ...] = ()
    non_applicability: LoadSuiteNonApplicability | None = None
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
        for operation in self.operation_mix:
            _nonempty(operation, "operation_mix item")
        if (
            not isinstance(self.executable_operations, tuple)
            or not all(
                isinstance(operation, ExecutableLoadOperation)
                for operation in self.executable_operations
            )
        ):
            raise CertificationInvariantError(
                "executable_operations must be a tuple of "
                "ExecutableLoadOperation values"
            )
        if self.non_applicability is not None and not isinstance(
            self.non_applicability,
            LoadSuiteNonApplicability,
        ):
            raise CertificationInvariantError(
                "non_applicability must be a LoadSuiteNonApplicability"
            )
        if self.executable_operations and self.non_applicability is not None:
            raise CertificationInvariantError(
                "a load suite cannot be executable and not applicable"
            )
        if not self.executable_operations and self.non_applicability is None:
            raise CertificationInvariantError(
                "an applicable load suite requires executable operations"
            )
        executable_ids = tuple(
            operation.operation_id for operation in self.executable_operations
        )
        if len(executable_ids) != len(set(executable_ids)):
            raise CertificationInvariantError(
                "executable_operations must have unique operation IDs"
            )
        if not isinstance(self.contract_absence_assertions, tuple) or not all(
            isinstance(assertion, LoadOperationContractAbsence)
            for assertion in self.contract_absence_assertions
        ):
            raise CertificationInvariantError(
                "contract_absence_assertions must be a tuple of "
                "LoadOperationContractAbsence values"
            )
        absence_ids = tuple(
            assertion.operation_id
            for assertion in self.contract_absence_assertions
        )
        if (
            len(absence_ids) != len(set(absence_ids))
            or set(absence_ids).intersection(executable_ids)
        ):
            raise CertificationInvariantError(
                "contract absence IDs must be unique and not executable"
            )
        if self.non_applicability is not None and self.contract_absence_assertions:
            raise CertificationInvariantError(
                "a non-applicable load suite cannot declare executable "
                "contract absences"
            )
        if self.executable_operations and not any(
            operation.kind == "data"
            and operation.execution_frequency == "per_item"
            for operation in self.executable_operations
        ):
            raise CertificationInvariantError(
                "an executable load suite requires at least one per-item "
                "data operation"
            )
        if self.kind == "historical" and self.executable_operations:
            historical_data_operations = self.data_operations
            if not historical_data_operations or any(
                cardinality == "zero_or_more"
                for operation in historical_data_operations
                for cardinality in (
                    operation.raw_cardinality,
                    operation.normalized_cardinality,
                    operation.observation_cardinality,
                )
            ):
                raise CertificationInvariantError(
                    "historical data operations require positive raw, "
                    "normalized, and observation cardinalities"
                )
            if any(
                not operation.quota_mappings
                for operation in historical_data_operations
            ):
                raise CertificationInvariantError(
                    "historical data operations require quota mappings"
                )
            if any(
                operation.cursor_applicability != "required"
                for operation in historical_data_operations
            ):
                raise CertificationInvariantError(
                    "historical data operations require cursor consistency"
                )
        if self.kind == "combined" and self.executable_operations:
            if self.non_applicability is not None:
                raise CertificationInvariantError(
                    "combined load suite cannot be non-applicable"
                )
            renewal_ids = tuple(
                operation_id
                for operation_id in (*executable_ids, *absence_ids)
                if operation_id.endswith(".token_or_watch_renewal")
            )
            if len(renewal_ids) != 1:
                raise CertificationInvariantError(
                    "combined typed workload must declare one "
                    "token_or_watch_renewal semantic"
                )
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

    @property
    def data_operations(self) -> tuple[ExecutableLoadOperation, ...]:
        """The typed operations whose selection weight drives offered rate."""

        return tuple(
            operation
            for operation in self.executable_operations
            if operation.kind == "data"
            and operation.execution_frequency == "per_item"
        )

    @property
    def control_operations(self) -> tuple[ExecutableLoadOperation, ...]:
        """Typed once-per-trial and periodic control operations."""

        return tuple(
            operation
            for operation in self.executable_operations
            if operation.kind == "control"
        )

    def execution_workload_dict(self) -> dict[str, object]:
        """Return the hash-pinned typed workload consumed by load runners.

        The compatibility ``operation_mix`` deliberately does not participate
        in this payload.  It remains available for historical readers but
        cannot alter callable selection, cardinality, cursor, quota, or
        receipt requirements.
        """

        payload = {
            "kind": self.kind,
            "executable_operations": [
                operation.to_dict() for operation in self.executable_operations
            ],
            "contract_absence_assertions": [
                assertion.to_dict()
                for assertion in self.contract_absence_assertions
            ],
            "non_applicability": (
                self.non_applicability.to_dict()
                if self.non_applicability is not None
                else None
            ),
        }
        rendered = json.dumps(
            payload,
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        ).encode("utf-8") + b"\n"
        return {
            **payload,
            "declaration_sha256": hashlib.sha256(rendered).hexdigest(),
        }

    @property
    def execution_workload_sha256(self) -> str:
        """Stable identity of the active typed workload declaration."""

        return str(self.execution_workload_dict()["declaration_sha256"])


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
        for suite in self.load_suites:
            if (
                not suite.executable_operations
                and suite.non_applicability is None
            ):
                raise CertificationInvariantError(
                    "canonical load suites must be executable or explicitly "
                    "not applicable"
                )
            operation_ids = (
                *(operation.operation_id for operation in suite.executable_operations),
                *(
                    assertion.operation_id
                    for assertion in suite.contract_absence_assertions
                ),
            )
            if any(
                not operation_id.startswith(f"{self.source_id}.")
                for operation_id in operation_ids
            ):
                raise CertificationInvariantError(
                    "load operation/absence belongs to a different source"
                )
            evidence_ids = (
                *(
                    operation.evidence_id
                    for operation in suite.executable_operations
                ),
                *(
                    assertion.evidence_id
                    for assertion in suite.contract_absence_assertions
                ),
                *(
                    (suite.non_applicability.evidence_id,)
                    if suite.non_applicability is not None
                    else ()
                ),
            )
            if any(
                not evidence_id.startswith(f"{self.evidence_pack_id}.")
                for evidence_id in evidence_ids
            ):
                raise CertificationInvariantError(
                    "load operation evidence identity differs from the "
                    "source evidence pack"
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
        if self.state == "not_applicable" and (
            self.failures or not self.artifact_uri
        ):
            raise CertificationInvariantError(
                "a not-applicable suite requires an artifact and no failures"
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
