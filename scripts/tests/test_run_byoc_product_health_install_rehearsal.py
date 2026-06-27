from __future__ import annotations

import json
import shutil
from pathlib import Path

from scripts.run_byoc_product_health_install_rehearsal import main


ROOT = Path(__file__).resolve().parents[2]
PLAN = ROOT / "deploy/byoc/product-health-install-rehearsal.example.yaml"


def test_run_byoc_product_health_install_rehearsal_json_output(capsys) -> None:
    code = main(["--json", "--repo-root", str(ROOT), "--install-plan", str(PLAN)])

    payload = json.loads(capsys.readouterr().out)
    rendered = json.dumps(payload, sort_keys=True)
    assert code == 0
    assert payload["schema_version"] == (
        "fyralis.byoc.product_health_install_rehearsal_report.v1"
    )
    assert payload["status"] == "pass"
    assert payload["failed_check_count"] == 0
    assert "https://" not in rendered
    assert "postgresql://" not in rendered
    assert "token=" not in rendered
    assert "arn:aws" not in rendered


def test_run_byoc_product_health_install_rehearsal_prints_schema(capsys) -> None:
    code = main(["--schema"])

    payload = json.loads(capsys.readouterr().out)
    assert code == 0
    assert payload["plan"]["properties"]["schema_version"]["const"] == (
        "fyralis.byoc.product_health_install_rehearsal.v1"
    )
    assert payload["report"]["properties"]["schema_version"]["const"] == (
        "fyralis.byoc.product_health_install_rehearsal_report.v1"
    )


def test_run_byoc_product_health_install_rehearsal_writes_output(
    tmp_path: Path,
    capsys,
) -> None:
    output = tmp_path / "install-rehearsal-report.json"

    code = main(
        [
            "--json",
            "--repo-root",
            str(ROOT),
            "--install-plan",
            str(PLAN),
            "--output",
            str(output),
        ]
    )

    captured = capsys.readouterr()
    assert code == 0
    assert json.loads(output.read_text(encoding="utf-8")) == json.loads(captured.out)


def test_run_byoc_product_health_install_rehearsal_returns_nonzero_on_failure(
    tmp_path: Path,
    capsys,
) -> None:
    _copy_rehearsal_tree(tmp_path)
    cronjob = tmp_path / "deploy/byoc/kubernetes/product-health-collector.cronjob.example.yaml"
    cronjob.write_text(
        cronjob.read_text(encoding="utf-8")
        + "\nports:\n  - containerPort: 8080\n",
        encoding="utf-8",
    )

    code = main(
        [
            "--json",
            "--repo-root",
            str(tmp_path),
            "--install-plan",
            str(tmp_path / "deploy/byoc/product-health-install-rehearsal.example.yaml"),
        ]
    )

    payload = json.loads(capsys.readouterr().out)
    rendered = json.dumps(payload, sort_keys=True)
    assert code == 1
    assert payload["status"] == "fail"
    assert "remove_inbound_listener_from_install_artifact" in payload["next_actions"]
    assert "containerPort" not in rendered


def _copy_rehearsal_tree(tmp_path: Path) -> None:
    for source in (
        PLAN,
        ROOT / "deploy/byoc/product-health-automation.example.yaml",
        ROOT / "deploy/byoc/dataplane.example.yaml",
        ROOT / "scripts/run_byoc_product_health_collector.py",
        ROOT / "deploy/byoc/kubernetes/product-health-collector.cronjob.example.yaml",
        ROOT / "deploy/byoc/systemd/product-health-collector.service.example",
        ROOT / "deploy/byoc/systemd/product-health-collector.timer.example",
    ):
        target = tmp_path / source.relative_to(ROOT)
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source, target)
