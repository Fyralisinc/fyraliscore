from __future__ import annotations

import json
import shutil
from datetime import UTC, datetime
from pathlib import Path

import pytest
import yaml
from pydantic import ValidationError

from services.platform.runtime.byoc_product_health_install_rehearsal import (
    ByocProductHealthInstallRehearsalInputs,
    ByocProductHealthInstallRehearsalPlan,
    load_product_health_install_rehearsal_plan,
    product_health_install_rehearsal_json_schema,
    render_product_health_install_rehearsal_json,
    run_product_health_install_rehearsal,
)


ROOT = Path(__file__).resolve().parents[4]
PLAN = ROOT / "deploy/byoc/product-health-install-rehearsal.example.yaml"
GENERATED_AT = datetime(2026, 6, 27, 12, 0, tzinfo=UTC)


def test_checked_in_product_health_install_rehearsal_passes() -> None:
    plan = load_product_health_install_rehearsal_plan(PLAN)
    report = run_product_health_install_rehearsal(
        ByocProductHealthInstallRehearsalInputs(
            install_plan_path=PLAN,
            repo_root=ROOT,
            generated_at=GENERATED_AT,
        )
    )
    rendered = render_product_health_install_rehearsal_json(report)

    assert plan.schema_version == "fyralis.byoc.product_health_install_rehearsal.v1"
    assert report.status == "pass"
    assert report.failed_check_count == 0
    assert report.check_count == 14
    assert report.stored_scope == (
        "customer_side_product_health_install_rehearsal_metadata_only"
    )
    assert {check.status for check in report.checks} == {"pass"}
    assert "kubernetes_no_inbound_ports" in {check.name for check in report.checks}
    assert "systemd_no_inbound_sockets" in {check.name for check in report.checks}
    assert "https://" not in rendered
    assert "postgresql://" not in rendered
    assert "arn:aws" not in rendered
    assert "token=" not in rendered
    assert "bearer " not in rendered.lower()


def test_product_health_install_rehearsal_schema_is_exportable() -> None:
    schema = product_health_install_rehearsal_json_schema()

    assert schema["plan"]["properties"]["schema_version"]["const"] == (
        "fyralis.byoc.product_health_install_rehearsal.v1"
    )
    assert schema["report"]["properties"]["schema_version"]["const"] == (
        "fyralis.byoc.product_health_install_rehearsal_report.v1"
    )
    assert schema["stored_scope"] == (
        "customer_side_product_health_install_rehearsal_metadata_only"
    )


def test_product_health_install_rehearsal_rejects_raw_endpoint_key() -> None:
    data = yaml.safe_load(PLAN.read_text(encoding="utf-8"))
    data["kubernetes"]["config_map_keys"]["control_plane_url_key"] = (
        "https://customer.example"
    )

    with pytest.raises(ValidationError):
        ByocProductHealthInstallRehearsalPlan.model_validate(data)


def test_product_health_install_rehearsal_flags_inbound_kubernetes_artifact(
    tmp_path: Path,
) -> None:
    _copy_rehearsal_tree(tmp_path)
    cronjob = tmp_path / "deploy/byoc/kubernetes/product-health-collector.cronjob.example.yaml"
    cronjob.write_text(
        cronjob.read_text(encoding="utf-8")
        + "\nports:\n  - containerPort: 8080\n",
        encoding="utf-8",
    )

    report = run_product_health_install_rehearsal(
        ByocProductHealthInstallRehearsalInputs(
            install_plan_path=(
                tmp_path / "deploy/byoc/product-health-install-rehearsal.example.yaml"
            ),
            repo_root=tmp_path,
            generated_at=GENERATED_AT,
        )
    )
    rendered = json.loads(render_product_health_install_rehearsal_json(report))

    assert report.status == "fail"
    assert "kubernetes_no_inbound_ports" in {check.name for check in report.checks}
    assert "remove_inbound_listener_from_install_artifact" in report.next_actions
    assert "containerPort" not in json.dumps(rendered)


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
