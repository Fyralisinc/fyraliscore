from __future__ import annotations

from pathlib import Path

import pytest

from lib.architecture_registry import (
    ArchitectureContractRegistry,
    ArchitectureRegistryError,
    load_architecture_registry,
    validate_architecture_registry,
)


ROOT = Path(__file__).resolve().parents[3]
REGISTRY_PATH = ROOT / "architecture" / "registry.yaml"


def test_registry_is_internally_valid_and_honest_about_incomplete_scope() -> None:
    registry = load_architecture_registry(REGISTRY_PATH)
    report = validate_architecture_registry(registry, root=ROOT)

    assert report.internally_valid
    assert report.coverage.registered_invariant_count == 42
    assert report.coverage.missing_invariant_ids == ()
    assert report.coverage.unmapped_invariant_ids == ()
    assert report.coverage.missing_proof_invariant_ids == ()
    assert report.coverage.planned_or_partial_contract_ids
    assert not report.production_freeze_ready


def test_registry_digest_is_deterministic() -> None:
    first = load_architecture_registry(REGISTRY_PATH)
    second = load_architecture_registry(REGISTRY_PATH)
    assert first.digest == second.digest
    assert len(first.digest) == 64


def test_registry_rejects_duplicate_yaml_keys(tmp_path: Path) -> None:
    path = tmp_path / "registry.yaml"
    path.write_text("meta:\n  registry_id: first\n  registry_id: second\n")

    with pytest.raises(ArchitectureRegistryError, match="duplicate YAML key"):
        load_architecture_registry(path)


def test_registry_rejects_unknown_writer_reference() -> None:
    registry = load_architecture_registry(REGISTRY_PATH)
    payload = registry.model_dump(mode="json")
    payload["contracts"][0]["writer_id"] = "CompetingUnknownWriter"

    with pytest.raises(ValueError, match="unknown writer"):
        ArchitectureContractRegistry.model_validate(payload)


def test_registry_supports_multiwriter_boundary_without_collapsing_owners() -> None:
    registry = load_architecture_registry(REGISTRY_PATH)
    boundary = next(
        item
        for item in registry.contracts
        if item.contract_id == "PredictionInterventionSettlementBoundary"
    )
    assert boundary.writer_id is None
    assert set(boundary.writer_ids) == {
        "ProposalAppender",
        "EpisodeCoordinator",
        "PredictionWriter",
        "AuthorizationApplier",
        "OutcomeRecorder",
        "SettlementApplier",
    }


def test_registry_rejects_unknown_multiwriter_reference() -> None:
    registry = load_architecture_registry(REGISTRY_PATH)
    payload = registry.model_dump(mode="json")
    contract = next(
        item
        for item in payload["contracts"]
        if item["contract_id"] == "PredictionInterventionSettlementBoundary"
    )
    contract["writer_ids"].append("CompetingUnknownWriter")

    with pytest.raises(ValueError, match="unknown writers"):
        ArchitectureContractRegistry.model_validate(payload)


def test_registry_rejects_unknown_invariant_reference() -> None:
    registry = load_architecture_registry(REGISTRY_PATH)
    payload = registry.model_dump(mode="json")
    payload["contracts"][0]["invariant_ids"] = ["INV-99"]

    with pytest.raises(ValueError, match="unknown invariants"):
        ArchitectureContractRegistry.model_validate(payload)


def test_registry_rejects_canonical_contract_without_writer() -> None:
    registry = load_architecture_registry(REGISTRY_PATH)
    payload = registry.model_dump(mode="json")
    payload["contracts"][0]["writer_id"] = None

    with pytest.raises(ValueError, match="require a writer"):
        ArchitectureContractRegistry.model_validate(payload)


def test_projection_digest_drift_is_visible() -> None:
    registry = load_architecture_registry(REGISTRY_PATH)
    payload = registry.model_dump(mode="json")
    payload["meta"]["documents"][0]["sha256"] = "0" * 64
    drifted = ArchitectureContractRegistry.model_validate(payload)

    report = validate_architecture_registry(drifted, root=ROOT)
    assert not report.internally_valid
    assert report.projection_digest_mismatches == (
        "docs/plans/revised-reality-belief-intent-system-implementation.md",
    )
