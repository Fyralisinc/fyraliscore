"""Loader and validator for the build-time ArchitectureContractRegistry."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Self

import yaml
from pydantic import BaseModel, ConfigDict, Field, model_validator

from lib.contracts import ArchitectureCommitmentClass


class ArchitectureRegistryError(ValueError):
    """Raised when the canonical architecture registry is structurally invalid."""


class _RegistryModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, str_strip_whitespace=True)


class RegistryDocument(_RegistryModel):
    path: str = Field(min_length=1)
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    role: str = Field(min_length=1)


class RegistryMeta(_RegistryModel):
    registry_id: str = Field(min_length=1)
    registry_version: str = Field(min_length=1)
    scope: str = Field(min_length=1)
    maturity: str = Field(pattern=r"^(experimental|candidate|stable)$")
    completeness_target: str = Field(min_length=1)
    documents: tuple[RegistryDocument, ...] = Field(min_length=1)


class RegistryWriter(_RegistryModel):
    writer_id: str = Field(min_length=1)
    owner: str = Field(min_length=1)
    package: str = Field(min_length=1)
    implementation_status: str = Field(pattern=r"^(planned|partial|implemented)$")


class RegistryComponent(_RegistryModel):
    """One logical latest-system component and its current physical boundary.

    ``owned_paths`` are paths for which this component is already the sole
    architectural owner. ``shared_legacy_paths`` are deliberately unresolved
    mixed-architecture hotspots; listing them is an inventory, not an ownership
    claim. Target-only packages belong in prose until they exist, so the checked
    registry cannot make absent implementation look complete.
    """

    component_id: str = Field(pattern=r"^(C0|E0|P(?:[1-9]|10))$")
    name: str = Field(min_length=1)
    semantic_plane: str = Field(min_length=1)
    architecture_role: str = Field(
        pattern=(
            r"^(contract_kernel|canonical_plane|temporary_workspace|"
            r"derived_projection|runtime_control|authority_control|evaluation_only)$"
        )
    )
    implementation_status: str = Field(pattern=r"^(planned|partial|implemented)$")
    separation_status: str = Field(pattern=r"^(separated|mixed|contract_only)$")
    owned_paths: tuple[str, ...] = ()
    shared_legacy_paths: tuple[str, ...] = ()
    test_paths: tuple[str, ...] = Field(min_length=1)
    writer_ids: tuple[str, ...] = ()
    contract_ids: tuple[str, ...] = ()
    depends_on: tuple[str, ...] = ()
    forbidden_responsibilities: tuple[str, ...] = Field(min_length=1)
    next_component_gate: str = Field(min_length=1)

    @model_validator(mode="after")
    def paths_and_references_are_locally_unique(self) -> Self:
        for field in (
            "owned_paths",
            "shared_legacy_paths",
            "test_paths",
            "writer_ids",
            "contract_ids",
            "depends_on",
        ):
            values = getattr(self, field)
            if len(values) != len(set(values)):
                raise ValueError(
                    f"component {self.component_id} has duplicate values in {field}"
                )
        overlap = set(self.owned_paths) & set(self.shared_legacy_paths)
        if overlap:
            raise ValueError(
                f"component {self.component_id} cannot both own and share paths "
                f"{sorted(overlap)}"
            )
        return self


class RegistryContract(_RegistryModel):
    contract_id: str = Field(min_length=1)
    contract_kind: str = Field(min_length=1)
    commitment_class: ArchitectureCommitmentClass
    maturity: str = Field(pattern=r"^(experimental|candidate|stable)$")
    implementation_status: str = Field(pattern=r"^(planned|partial|implemented)$")
    semantic_plane: str = Field(min_length=1)
    durability: str = Field(
        pattern=r"^(canonical|derived|temporary|embedded|build_time)$"
    )
    writer_id: str | None = None
    writer_ids: tuple[str, ...] = ()
    traits: tuple[str, ...] = ()
    invariant_ids: tuple[str, ...] = Field(min_length=1)
    dependency_ids: tuple[str, ...] = ()

    @model_validator(mode="after")
    def durable_contract_has_writer(self) -> Self:
        if self.writer_id and self.writer_ids:
            raise ValueError("contract must use writer_id or writer_ids, not both")
        if len(self.writer_ids) != len(set(self.writer_ids)):
            raise ValueError("contract writer_ids must be unique")
        if self.durability in {"canonical", "derived"} and not (
            self.writer_id or self.writer_ids
        ):
            raise ValueError("canonical and derived contracts require a writer")
        return self

    @property
    def all_writer_ids(self) -> tuple[str, ...]:
        return (self.writer_id,) if self.writer_id else self.writer_ids


class RegistryInvariantProof(_RegistryModel):
    """Executable proof identity owned by the architecture registry.

    A missing proof definition is permitted while the candidate registry is
    being built, but it is reported as a coverage gap by the proof compiler and
    prevents production-freeze completeness.
    """

    invariant_version: str = Field(min_length=1)
    object_and_transition_scope: str = Field(min_length=1)
    eligibility_predicate_version: str = Field(min_length=1)
    exposure_unit: str = Field(min_length=1)
    fate_vocabulary_version: str = Field(min_length=1)
    mutually_exclusive_fates: tuple[str, ...] = Field(min_length=1)
    mandatory_trace_facts: tuple[str, ...] = Field(min_length=1)
    oracle_or_metamorphic_relation: str = Field(min_length=1)
    suite_and_scenario_ids: tuple[str, ...] = Field(min_length=1)
    continuous_metric_ids: tuple[str, ...] = Field(min_length=1)
    incident_class: str = Field(min_length=1)
    known_blind_spots: tuple[str, ...] = Field(min_length=1)


class RegistryInvariant(_RegistryModel):
    invariant_id: str = Field(pattern=r"^INV-[0-9]{2}$")
    title: str = Field(min_length=1)
    evidence_floor: str = Field(pattern=r"^E[0-6]$")
    owner: str = Field(min_length=1)
    implementation_status: str = Field(pattern=r"^(planned|partial|implemented)$")
    contract_ids: tuple[str, ...] = ()
    proof: RegistryInvariantProof | None = None


class RegistryProjection(_RegistryModel):
    projection_id: str = Field(min_length=1)
    path: str = Field(min_length=1)
    verification: str = Field(pattern=r"^(digest|generated|checked)$")
    required_for_complete: bool = True


class ArchitectureContractRegistry(_RegistryModel):
    meta: RegistryMeta
    writers: tuple[RegistryWriter, ...]
    components: tuple[RegistryComponent, ...]
    contracts: tuple[RegistryContract, ...]
    invariants: tuple[RegistryInvariant, ...]
    projections: tuple[RegistryProjection, ...]

    @model_validator(mode="after")
    def references_are_closed(self) -> Self:
        _require_unique(self.writers, "writer_id")
        _require_unique(self.components, "component_id")
        _require_unique(self.contracts, "contract_id")
        _require_unique(self.invariants, "invariant_id")
        _require_unique(self.projections, "projection_id")

        writer_ids = {item.writer_id for item in self.writers}
        component_ids = {item.component_id for item in self.components}
        contract_ids = {item.contract_id for item in self.contracts}
        invariant_ids = {item.invariant_id for item in self.invariants}
        owned_paths: dict[str, str] = {}
        for component in self.components:
            missing_writers = set(component.writer_ids) - writer_ids
            if missing_writers:
                raise ValueError(
                    f"component {component.component_id} references unknown writers "
                    f"{sorted(missing_writers)}"
                )
            missing_contracts = set(component.contract_ids) - contract_ids
            if missing_contracts:
                raise ValueError(
                    f"component {component.component_id} references unknown contracts "
                    f"{sorted(missing_contracts)}"
                )
            missing_components = set(component.depends_on) - component_ids
            if missing_components:
                raise ValueError(
                    f"component {component.component_id} references unknown components "
                    f"{sorted(missing_components)}"
                )
            if component.component_id in component.depends_on:
                raise ValueError(
                    f"component {component.component_id} cannot depend on itself"
                )
            for path in component.owned_paths:
                previous = owned_paths.get(path)
                if previous is not None:
                    raise ValueError(
                        f"owned path {path!r} belongs to both {previous} and "
                        f"{component.component_id}"
                    )
                owned_paths[path] = component.component_id
        for contract in self.contracts:
            missing_writers = set(contract.all_writer_ids) - writer_ids
            if missing_writers:
                raise ValueError(
                    f"contract {contract.contract_id} references unknown writers "
                    f"{sorted(missing_writers)}"
                )
            missing_invariants = set(contract.invariant_ids) - invariant_ids
            if missing_invariants:
                raise ValueError(
                    f"contract {contract.contract_id} references unknown invariants "
                    f"{sorted(missing_invariants)}"
                )
            missing_dependencies = set(contract.dependency_ids) - contract_ids
            if missing_dependencies:
                raise ValueError(
                    f"contract {contract.contract_id} references unknown contracts "
                    f"{sorted(missing_dependencies)}"
                )
        for invariant in self.invariants:
            missing_contracts = set(invariant.contract_ids) - contract_ids
            if missing_contracts:
                raise ValueError(
                    f"invariant {invariant.invariant_id} references unknown contracts "
                    f"{sorted(missing_contracts)}"
                )
        return self

    @property
    def digest(self) -> str:
        payload = json.dumps(
            self.model_dump(mode="json"),
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
        return hashlib.sha256(payload).hexdigest()

    def coverage(self) -> RegistryCoverage:
        expected = {f"INV-{number:02d}" for number in range(1, 43)}
        registered = {item.invariant_id for item in self.invariants}
        mapped = {item.invariant_id for item in self.invariants if item.contract_ids}
        executable_proofs = {
            item.invariant_id for item in self.invariants if item.proof is not None
        }
        implemented_contracts = {
            item.contract_id
            for item in self.contracts
            if item.implementation_status == "implemented"
        }
        return RegistryCoverage(
            expected_invariant_count=len(expected),
            registered_invariant_count=len(registered & expected),
            missing_invariant_ids=tuple(sorted(expected - registered)),
            extra_invariant_ids=tuple(sorted(registered - expected)),
            mapped_invariant_count=len(mapped & expected),
            unmapped_invariant_ids=tuple(sorted((registered & expected) - mapped)),
            executable_proof_count=len(executable_proofs & expected),
            missing_proof_invariant_ids=tuple(
                sorted((registered & expected) - executable_proofs)
            ),
            contract_count=len(self.contracts),
            implemented_contract_count=len(implemented_contracts),
            planned_or_partial_contract_ids=tuple(
                sorted(
                    item.contract_id
                    for item in self.contracts
                    if item.implementation_status != "implemented"
                )
            ),
        )


class RegistryCoverage(_RegistryModel):
    expected_invariant_count: int
    registered_invariant_count: int
    missing_invariant_ids: tuple[str, ...]
    extra_invariant_ids: tuple[str, ...]
    mapped_invariant_count: int
    unmapped_invariant_ids: tuple[str, ...]
    executable_proof_count: int
    missing_proof_invariant_ids: tuple[str, ...]
    contract_count: int
    implemented_contract_count: int
    planned_or_partial_contract_ids: tuple[str, ...]

    @property
    def complete(self) -> bool:
        return not (
            self.missing_invariant_ids
            or self.extra_invariant_ids
            or self.unmapped_invariant_ids
            or self.missing_proof_invariant_ids
            or self.planned_or_partial_contract_ids
        )


class RegistryValidationReport(_RegistryModel):
    registry_digest: str
    coverage: RegistryCoverage
    projection_digest_mismatches: tuple[str, ...] = ()
    missing_projection_paths: tuple[str, ...] = ()
    missing_component_paths: tuple[str, ...] = ()
    missing_component_test_paths: tuple[str, ...] = ()
    missing_implemented_writer_packages: tuple[str, ...] = ()

    @property
    def internally_valid(self) -> bool:
        return (
            not self.projection_digest_mismatches
            and not self.missing_projection_paths
            and not self.missing_component_paths
            and not self.missing_component_test_paths
            and not self.missing_implemented_writer_packages
        )

    @property
    def production_freeze_ready(self) -> bool:
        return self.internally_valid and self.coverage.complete


class _UniqueKeyLoader(yaml.SafeLoader):
    pass


def _construct_unique_mapping(
    loader: _UniqueKeyLoader,
    node: yaml.MappingNode,
    deep: bool = False,
) -> dict[Any, Any]:
    mapping: dict[Any, Any] = {}
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node, deep=deep)
        if key in mapping:
            raise ArchitectureRegistryError(f"duplicate YAML key: {key!r}")
        mapping[key] = loader.construct_object(value_node, deep=deep)
    return mapping


_UniqueKeyLoader.add_constructor(
    yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG,
    _construct_unique_mapping,
)


def load_architecture_registry(path: Path) -> ArchitectureContractRegistry:
    try:
        raw = yaml.load(path.read_text(), Loader=_UniqueKeyLoader)
        return ArchitectureContractRegistry.model_validate(raw)
    except (OSError, yaml.YAMLError, ValueError) as exc:
        if isinstance(exc, ArchitectureRegistryError):
            raise
        raise ArchitectureRegistryError(str(exc)) from exc


def validate_architecture_registry(
    registry: ArchitectureContractRegistry,
    *,
    root: Path,
) -> RegistryValidationReport:
    digest_mismatches: list[str] = []
    missing_paths: list[str] = []
    for document in registry.meta.documents:
        path = root / document.path
        if not path.is_file():
            missing_paths.append(document.path)
            continue
        actual = hashlib.sha256(path.read_bytes()).hexdigest()
        if actual != document.sha256:
            digest_mismatches.append(document.path)
    for projection in registry.projections:
        if projection.required_for_complete and not (root / projection.path).exists():
            missing_paths.append(projection.path)
    missing_component_paths: list[str] = []
    missing_component_test_paths: list[str] = []
    missing_implemented_writer_packages: list[str] = []
    for writer in registry.writers:
        if writer.implementation_status != "implemented":
            continue
        package_path = root / writer.package.replace(".", "/")
        if not package_path.exists() and not package_path.with_suffix(".py").exists():
            missing_implemented_writer_packages.append(
                f"{writer.writer_id}:{writer.package}"
            )
    for component in registry.components:
        for component_path in (*component.owned_paths, *component.shared_legacy_paths):
            if not (root / component_path).exists():
                missing_component_paths.append(
                    f"{component.component_id}:{component_path}"
                )
        for test_path in component.test_paths:
            if not (root / test_path).exists():
                missing_component_test_paths.append(
                    f"{component.component_id}:{test_path}"
                )
    return RegistryValidationReport(
        registry_digest=registry.digest,
        coverage=registry.coverage(),
        projection_digest_mismatches=tuple(sorted(set(digest_mismatches))),
        missing_projection_paths=tuple(sorted(set(missing_paths))),
        missing_component_paths=tuple(sorted(set(missing_component_paths))),
        missing_component_test_paths=tuple(
            sorted(set(missing_component_test_paths))
        ),
        missing_implemented_writer_packages=tuple(
            sorted(set(missing_implemented_writer_packages))
        ),
    )


def _require_unique(items: tuple[_RegistryModel, ...], field: str) -> None:
    values = [getattr(item, field) for item in items]
    duplicates = sorted({value for value in values if values.count(value) > 1})
    if duplicates:
        raise ValueError(f"duplicate {field}: {duplicates}")


__all__ = [
    "ArchitectureContractRegistry",
    "ArchitectureRegistryError",
    "RegistryContract",
    "RegistryComponent",
    "RegistryCoverage",
    "RegistryDocument",
    "RegistryInvariant",
    "RegistryInvariantProof",
    "RegistryMeta",
    "RegistryProjection",
    "RegistryValidationReport",
    "RegistryWriter",
    "load_architecture_registry",
    "validate_architecture_registry",
]
