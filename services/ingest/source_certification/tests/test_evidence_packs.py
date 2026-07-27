from __future__ import annotations

import json
import shutil

import pytest

from services.ingest.source_certification.evidence import (
    EVIDENCE_PACK_DIRECTORY,
    SURFACE_ARTIFACT_DIRECTORY,
    EvidencePackError,
    load_evidence_catalog,
    load_evidence_pack,
)
from services.ingest.source_certification.catalog import (
    SOURCE_CERTIFICATION_CATALOG,
)
from services.ingest.source_contract.catalog import CANONICAL_SOURCE_IDS


def test_exactly_one_versioned_evidence_pack_exists_for_every_source() -> None:
    catalog = load_evidence_catalog()

    assert tuple(catalog) == CANONICAL_SOURCE_IDS
    assert len(catalog) == 27
    for source_id, pack in catalog.items():
        assert pack.source_id == source_id
        assert pack.pack_version == "1.0.0"
        assert len(pack.content_sha256) == 64
        assert pack.path.name == f"{source_id}.json"
        assert {reference.behavior_id for reference in pack.evidence} == {
            "used_api_surface",
            "quota_policy",
            "fyralis_runtime_contract",
        }


def test_checked_in_evidence_packs_remain_fail_closed_until_locked() -> None:
    catalog = load_evidence_catalog()

    assert sum(pack.ready for pack in catalog.values()) == 0
    for pack in catalog.values():
        surface = next(
            reference
            for reference in pack.evidence
            if reference.behavior_id == "used_api_surface"
        )
        assert surface.schema_sha256 is not None
        assert len(surface.schema_sha256) == 64
        assert all(reference.verified_at is None for reference in pack.evidence)


def test_certification_specs_are_derived_from_versioned_evidence_packs() -> None:
    packs = load_evidence_catalog()

    for source_id, pack in packs.items():
        spec = SOURCE_CERTIFICATION_CATALOG[source_id]
        assert spec.evidence_pack_id == f"ingest.evidence.{source_id}"
        assert spec.evidence_pack_version == pack.pack_version
        assert spec.evidence_pack_sha256 == pack.content_sha256
        assert spec.provider_api_version == pack.provider_api_version
        assert spec.evidence == pack.evidence


def test_evidence_catalog_rejects_missing_or_parallel_pack(tmp_path) -> None:  # noqa: ANN001
    for path in EVIDENCE_PACK_DIRECTORY.glob("*.json"):
        shutil.copyfile(path, tmp_path / path.name)

    (tmp_path / "slack.json").unlink()
    with pytest.raises(EvidencePackError, match="missing slack.json"):
        load_evidence_catalog(directory=tmp_path)

    shutil.copyfile(
        EVIDENCE_PACK_DIRECTORY / "slack.json",
        tmp_path / "slack.json",
    )
    (tmp_path / "legacy_source.json").write_text("{}\n", encoding="utf-8")
    with pytest.raises(EvidencePackError, match="unknown legacy_source.json"):
        load_evidence_catalog(directory=tmp_path)


def test_evidence_pack_rejects_filename_source_mismatch(tmp_path) -> None:  # noqa: ANN001
    raw = json.loads(
        (EVIDENCE_PACK_DIRECTORY / "slack.json").read_text(encoding="utf-8")
    )
    raw["source_id"] = "github"
    (tmp_path / "slack.json").write_text(
        json.dumps(raw),
        encoding="utf-8",
    )

    with pytest.raises(EvidencePackError, match="declares source 'github'"):
        load_evidence_pack("slack", directory=tmp_path)


def test_evidence_pack_rejects_surface_checksum_drift(tmp_path) -> None:  # noqa: ANN001
    evidence_dir = tmp_path / "evidence"
    surface_dir = tmp_path / "surfaces"
    evidence_dir.mkdir()
    surface_dir.mkdir()
    shutil.copyfile(
        EVIDENCE_PACK_DIRECTORY / "slack.json",
        evidence_dir / "slack.json",
    )
    shutil.copyfile(
        SURFACE_ARTIFACT_DIRECTORY / "slack.json",
        surface_dir / "slack.json",
    )
    with (surface_dir / "slack.json").open("a", encoding="utf-8") as handle:
        handle.write(" ")

    with pytest.raises(EvidencePackError, match="checksum differs"):
        load_evidence_pack(
            "slack",
            directory=evidence_dir,
            surface_directory=surface_dir,
        )


def test_evidence_pack_rejects_unpinned_behavior_shape(tmp_path) -> None:  # noqa: ANN001
    raw = json.loads(
        (EVIDENCE_PACK_DIRECTORY / "slack.json").read_text(encoding="utf-8")
    )
    raw["evidence"][0]["unexpected"] = True
    (tmp_path / "slack.json").write_text(
        json.dumps(raw),
        encoding="utf-8",
    )

    with pytest.raises(EvidencePackError, match="unknown unexpected"):
        load_evidence_pack("slack", directory=tmp_path)
