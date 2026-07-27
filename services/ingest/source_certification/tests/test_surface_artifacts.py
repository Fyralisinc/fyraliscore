from __future__ import annotations

import hashlib
import shutil
from pathlib import Path

from scripts.generate_source_certification_surfaces import (
    build_surface_artifacts,
    main,
    render_surface_artifact,
)
from services.ingest.source_certification.evidence import (
    SURFACE_ARTIFACT_DIRECTORY,
    load_evidence_catalog,
    load_surface_operation_methods,
)
from services.ingest.source_contract.catalog import CANONICAL_SOURCE_IDS


def test_checked_in_surfaces_match_contract_lab_and_golden_fixtures() -> None:
    artifacts = build_surface_artifacts()

    assert tuple(artifacts) == CANONICAL_SOURCE_IDS
    assert {
        path.stem for path in SURFACE_ARTIFACT_DIRECTORY.glob("*.json")
    } == set(CANONICAL_SOURCE_IDS)
    for source_id, artifact in artifacts.items():
        expected = render_surface_artifact(artifact)
        path = SURFACE_ARTIFACT_DIRECTORY / f"{source_id}.json"
        assert path.read_text(encoding="utf-8") == expected


def test_used_surface_checksums_pin_exact_checked_in_bytes() -> None:
    packs = load_evidence_catalog()

    for source_id, pack in packs.items():
        surface = next(
            item
            for item in pack.evidence
            if item.behavior_id == "used_api_surface"
        )
        artifact = SURFACE_ARTIFACT_DIRECTORY / f"{source_id}.json"
        assert surface.schema_sha256 == hashlib.sha256(
            artifact.read_bytes(),
        ).hexdigest()


def test_surface_pins_exact_operation_methods_for_canary_classification() -> None:
    artifacts = build_surface_artifacts()

    for source_id, artifact in artifacts.items():
        expected = {
            binding["operation_id"]: binding["method"]
            for route in artifact["provider_lab"]["routes"]
            for binding in route["operation_bindings"]
        }
        assert dict(load_surface_operation_methods(source_id)) == expected


def test_surface_generator_check_mode_rejects_stale_bundle(
    tmp_path: Path,
) -> None:
    output = tmp_path / "surfaces"
    evidence = tmp_path / "evidence"
    shutil.copytree(
        Path(__file__).resolve().parents[1] / "evidence",
        evidence,
    )

    args = [
        "--output-dir",
        str(output),
        "--evidence-dir",
        str(evidence),
    ]
    assert main([*args, "--check"]) == 1
    assert main(args) == 0
    assert main([*args, "--check"]) == 0
    (output / "slack.json").write_text("{}\n", encoding="utf-8")
    assert main([*args, "--check"]) == 1
