from __future__ import annotations

import json
from pathlib import Path

from scripts.verify_byoc_bootstrap_bundle import main


ROOT = Path(__file__).resolve().parents[2]
BUNDLE = ROOT / "deploy/byoc/bootstrap-bundle.example.yaml"
DATAPLANE = ROOT / "deploy/byoc/dataplane.example.yaml"
PERMISSIONS = ROOT / "deploy/byoc/permissions.example.yaml"


def test_verify_byoc_bootstrap_bundle_passes_checked_in_sample(capsys) -> None:
    code = main([
        str(BUNDLE),
        "--dataplane-manifest",
        str(DATAPLANE),
        "--permissions-manifest",
        str(PERMISSIONS),
        "--verify-local-files",
        "--repo-root",
        str(ROOT),
    ])

    captured = capsys.readouterr()
    assert code == 0
    assert captured.out == "BYOC bootstrap bundle passed.\n"


def test_verify_byoc_bootstrap_bundle_json_output(capsys) -> None:
    code = main([
        "--json",
        str(BUNDLE),
        "--dataplane-manifest",
        str(DATAPLANE),
        "--permissions-manifest",
        str(PERMISSIONS),
        "--verify-local-files",
        "--repo-root",
        str(ROOT),
    ])

    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert code == 0
    assert payload["valid"] is True
    assert payload["schema_errors"] == []
    assert "gateway_image" in payload["artifacts"]


def test_verify_byoc_bootstrap_bundle_prints_schema(capsys) -> None:
    code = main(["--schema"])

    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert code == 0
    assert payload["properties"]["schema_version"]["const"] == (
        "fyralis.byoc.bootstrap_bundle.v1"
    )


def test_verify_byoc_bootstrap_bundle_prints_cosign_commands(capsys) -> None:
    code = main([
        str(BUNDLE),
        "--print-cosign-commands",
    ])

    captured = capsys.readouterr()
    assert code == 0
    assert "cosign verify " in captured.out
    assert "cosign verify-blob " in captured.out
    assert "iam.bootstrap.template.yaml" in captured.out


def test_verify_byoc_bootstrap_bundle_reports_contract_errors(
    tmp_path: Path,
    capsys,
) -> None:
    manifest = BUNDLE.read_text(encoding="utf-8")
    manifest = manifest.replace(
        "@sha256:1111111111111111111111111111111111111111111111111111111111111111",
        ":latest",
        1,
    )
    path = tmp_path / "unsafe.yaml"
    path.write_text(manifest, encoding="utf-8")

    code = main([str(path)])

    captured = capsys.readouterr()
    assert code == 1
    assert "mutable_artifact_ref" in captured.err


def test_verify_byoc_bootstrap_bundle_reports_local_digest_errors(
    tmp_path: Path,
    capsys,
) -> None:
    manifest = BUNDLE.read_text(encoding="utf-8")
    manifest = manifest.replace(
        "sha256:85472394a36b43bb0273b43c292be2ce052422c09986512b77c6f77a56ad8fff",
        "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        1,
    )
    path = tmp_path / "bad-digest.yaml"
    path.write_text(manifest, encoding="utf-8")

    code = main([
        str(path),
        "--verify-local-files",
        "--repo-root",
        str(ROOT),
    ])

    captured = capsys.readouterr()
    assert code == 1
    assert "local_artifact_digest_mismatch" in captured.err
