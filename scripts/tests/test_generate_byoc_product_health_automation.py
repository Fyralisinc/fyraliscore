from __future__ import annotations

import json
from pathlib import Path

import yaml

from scripts.generate_byoc_product_health_automation import main


ROOT = Path(__file__).resolve().parents[2]
AUTOMATION = ROOT / "deploy/byoc/product-health-automation.example.yaml"
DATAPLANE = ROOT / "deploy/byoc/dataplane.example.yaml"


def test_generate_byoc_product_health_automation_yaml_output(capsys) -> None:
    code = main([])

    captured = capsys.readouterr()
    payload = yaml.safe_load(captured.out)
    assert code == 0
    assert payload["schema_version"] == (
        "fyralis.byoc.product_health_automation.v1"
    )
    assert payload["deployment_id"] == "dep_example01"
    assert payload["runtime"]["no_inbound_ports"] is True
    assert payload["control_plane"]["submit_path"] == (
        "/byoc/control-plane/product-health-snapshots"
    )
    assert captured.err == ""


def test_generate_byoc_product_health_automation_json_output(capsys) -> None:
    code = main(["--json"])

    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert code == 0
    assert payload["schema_version"] == (
        "fyralis.byoc.product_health_automation.v1"
    )
    assert payload["privacy_boundary"]["raw_secret_values_included"] is False


def test_generate_byoc_product_health_automation_check_passes(capsys) -> None:
    code = main(["--check-automation", str(AUTOMATION)])

    captured = capsys.readouterr()
    assert code == 0
    assert "passed" in captured.out
    assert captured.err == ""


def test_generate_byoc_product_health_automation_prints_schema(capsys) -> None:
    code = main(["--schema"])

    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert code == 0
    assert payload["properties"]["schema_version"]["const"] == (
        "fyralis.byoc.product_health_automation.v1"
    )


def test_generate_byoc_product_health_automation_reports_drift(
    tmp_path: Path,
    capsys,
) -> None:
    _copy_automation_tree(tmp_path)
    automation_path = tmp_path / "deploy/byoc/product-health-automation.example.yaml"
    data = yaml.safe_load(automation_path.read_text(encoding="utf-8"))
    data["schedule"]["timeout_seconds"] = 45
    automation_path.write_text(
        yaml.safe_dump(data, sort_keys=False, width=1_000_000),
        encoding="utf-8",
    )

    code = main(
        [
            "--check-automation",
            str(automation_path),
            "--dataplane-manifest",
            str(tmp_path / "deploy/byoc/dataplane.example.yaml"),
            "--repo-root",
            str(tmp_path),
        ]
    )

    captured = capsys.readouterr()
    assert code == 1
    assert "generated_product_health_automation_drift" in captured.err
    assert captured.out == ""


def test_generate_byoc_product_health_automation_write_outputs(
    tmp_path: Path,
    capsys,
) -> None:
    dataplane_path = tmp_path / "deploy/byoc/dataplane.example.yaml"
    dataplane_path.parent.mkdir(parents=True, exist_ok=True)
    dataplane_path.write_text(DATAPLANE.read_text(encoding="utf-8"), encoding="utf-8")

    code = main(
        [
            "--write",
            "--dataplane-manifest",
            str(dataplane_path),
            "--automation-output",
            str(tmp_path / "deploy/byoc/product-health-automation.example.yaml"),
            "--repo-root",
            str(tmp_path),
        ]
    )

    captured = capsys.readouterr()
    assert code == 0
    assert (tmp_path / "deploy/byoc/product-health-automation.example.yaml").exists()
    assert (
        tmp_path / "deploy/byoc/kubernetes/product-health-collector.cronjob.example.yaml"
    ).exists()
    assert (
        tmp_path / "deploy/byoc/systemd/product-health-collector.service.example"
    ).exists()
    assert "written" in captured.out


def _copy_automation_tree(tmp_path: Path) -> None:
    for source in (
        DATAPLANE,
        AUTOMATION,
        ROOT / "scripts/run_byoc_product_health_collector.py",
        ROOT / "deploy/byoc/kubernetes/product-health-collector.cronjob.example.yaml",
        ROOT / "deploy/byoc/systemd/product-health-collector.service.example",
        ROOT / "deploy/byoc/systemd/product-health-collector.timer.example",
    ):
        rel = source.relative_to(ROOT)
        target = tmp_path / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(source.read_text(encoding="utf-8"), encoding="utf-8")
