from __future__ import annotations

import json
from pathlib import Path

from scripts.validate_byoc_permissions_manifest import main


ROOT = Path(__file__).resolve().parents[2]
PERMISSIONS = ROOT / "deploy/byoc/permissions.example.yaml"
DATAPLANE = ROOT / "deploy/byoc/dataplane.example.yaml"
AWS_TEMPLATE = ROOT / "deploy/byoc/aws/iam.bootstrap.template.yaml"


def test_validate_byoc_permissions_manifest_passes_checked_in_sample(
    capsys,
) -> None:
    code = main([
        str(PERMISSIONS),
        "--dataplane-manifest",
        str(DATAPLANE),
        "--aws-template",
        str(AWS_TEMPLATE),
    ])

    captured = capsys.readouterr()
    assert code == 0
    assert captured.out == "BYOC permissions manifest passed.\n"


def test_validate_byoc_permissions_manifest_json_output(capsys) -> None:
    code = main([
        "--json",
        str(PERMISSIONS),
        "--dataplane-manifest",
        str(DATAPLANE),
        "--aws-template",
        str(AWS_TEMPLATE),
    ])

    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert code == 0
    assert payload["valid"] is True
    assert payload["schema_errors"] == []
    assert "data_plane_agent" in payload["roles"]
    assert "data_plane_agent" in payload["aws_template_roles"]


def test_validate_byoc_permissions_manifest_prints_schema(capsys) -> None:
    code = main(["--schema"])

    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert code == 0
    assert payload["properties"]["schema_version"]["const"] == (
        "fyralis.byoc.permissions.v1"
    )


def test_validate_byoc_permissions_manifest_prints_aws_template_schema(
    capsys,
) -> None:
    code = main(["--aws-template-schema"])

    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert code == 0
    assert payload["properties"]["schema_version"]["const"] == (
        "fyralis.byoc.aws.iam_skeleton.v1"
    )


def test_validate_byoc_permissions_manifest_reports_contract_errors(
    tmp_path: Path,
    capsys,
) -> None:
    manifest = PERMISSIONS.read_text(encoding="utf-8")
    manifest = manifest.replace("sts:GetCallerIdentity", "iam:*")
    path = tmp_path / "unsafe.yaml"
    path.write_text(manifest, encoding="utf-8")

    code = main([str(path)])

    captured = capsys.readouterr()
    assert code == 1
    assert "wildcard_action_forbidden" in captured.err


def test_validate_byoc_permissions_manifest_reports_template_errors(
    tmp_path: Path,
    capsys,
) -> None:
    template = AWS_TEMPLATE.read_text(encoding="utf-8")
    template = template.replace(
        "ReadAccountAndRegionPreflight",
        "ReadSomethingElse",
        1,
    )
    path = tmp_path / "template.yaml"
    path.write_text(template, encoding="utf-8")

    code = main([str(PERMISSIONS), "--aws-template", str(path)])

    captured = capsys.readouterr()
    assert code == 1
    assert "unknown_policy_grant_sid" in captured.err
