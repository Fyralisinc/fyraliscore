from __future__ import annotations

from pathlib import Path

import pytest

from services.platform.runtime import byoc_validation
from services.platform.runtime.byoc_validation import (
    ByocValidationInputs,
    parse_worker_health_args,
    render_report_json,
    run_byoc_post_deploy_validation,
)
from services.platform.runtime.process_manifest import production_processes


ROOT = Path(__file__).resolve().parents[4]
MANIFEST = ROOT / "deploy/byoc/dataplane.example.yaml"
ENV_TEMPLATE = ROOT / ".env.production.example"


def test_byoc_post_deploy_validation_passes_offline_contract() -> None:
    report = run_byoc_post_deploy_validation(
        ByocValidationInputs(manifest_path=MANIFEST, env_path=ENV_TEMPLATE)
    )

    assert report.status == "pass"
    by_name = {check.name: check for check in report.checks}
    assert by_name["manifest_contract"].status == "pass"
    assert by_name["env_contract"].status == "pass"
    assert by_name["runtime_process_contract"].status == "pass"
    assert by_name["gateway_health"].status == "skipped"
    assert by_name["database_rls_safety"].status == "skipped"


def test_byoc_post_deploy_validation_fails_env_manifest_mismatch(
    tmp_path: Path,
) -> None:
    env_path = tmp_path / ".env.production"
    env_path.write_text(
        ENV_TEMPLATE.read_text(encoding="utf-8").replace(
            "FYRALIS_BYOC_REGION=us-east-1",
            "FYRALIS_BYOC_REGION=eu-west-1",
        ),
        encoding="utf-8",
    )

    report = run_byoc_post_deploy_validation(
        ByocValidationInputs(manifest_path=MANIFEST, env_path=env_path)
    )

    assert report.status == "fail"
    env_check = {check.name: check for check in report.checks}["env_contract"]
    assert env_check.status == "fail"
    assert "FYRALIS_BYOC_REGION" in env_check.details


def test_byoc_post_deploy_validation_require_live_fails_missing_inputs() -> None:
    report = run_byoc_post_deploy_validation(
        ByocValidationInputs(
            manifest_path=MANIFEST,
            env_path=ENV_TEMPLATE,
            require_live=True,
        )
    )

    assert report.status == "fail"
    by_name = {check.name: check for check in report.checks}
    assert by_name["gateway_health"].status == "fail"
    assert by_name["worker_health_coverage"].status == "fail"
    assert by_name["database_rls_safety"].status == "fail"
    assert by_name["broker_reachability"].status == "fail"
    assert by_name["object_store_reachability"].status == "fail"


def test_byoc_post_deploy_validation_live_probes_can_pass_with_all_inputs(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    disabled = "\n".join(
        (
            f"    - name: {process.name}\n"
            "      reason: disabled in focused validation test."
        )
        for process in production_processes()
        if process.name != "gateway"
    )
    manifest_path = tmp_path / "dataplane.yaml"
    manifest_path.write_text(
        MANIFEST.read_text(encoding="utf-8").replace(
            "    - name: discord_gateway_worker\n"
            "      reason: First customer profile does not enable Discord.\n"
            "    - name: telegram_gateway_worker\n"
            "      reason: First customer profile does not enable Telegram.\n"
            "    - name: signal_gateway_worker\n"
            "      reason: First customer profile does not enable Signal.\n",
            disabled + "\n",
        ),
        encoding="utf-8",
    )

    calls = []

    def fake_http_get_status(url: str, *, timeout_s: float) -> int:
        calls.append(("http", url, timeout_s))
        return 200

    async def fake_db_safety(database_url: str) -> None:
        calls.append(("db", database_url))

    def fake_tcp_connect(host: str, port: int, *, timeout_s: float) -> None:
        calls.append(("tcp", host, port, timeout_s))

    monkeypatch.setattr(byoc_validation, "_http_get_status", fake_http_get_status)
    monkeypatch.setattr(byoc_validation, "_assert_db_safety", fake_db_safety)
    monkeypatch.setattr(byoc_validation, "_tcp_connect", fake_tcp_connect)

    report = run_byoc_post_deploy_validation(
        ByocValidationInputs(
            manifest_path=manifest_path,
            env_path=ENV_TEMPLATE,
            gateway_url="http://gateway.local",
            database_url="postgresql://app@postgres/fyralis",
            kafka_bootstrap_servers="broker.local:9092",
            object_store_url="https://object-store.local",
            require_live=True,
        )
    )

    assert report.status == "pass"
    by_name = {check.name: check for check in report.checks}
    assert by_name["gateway_health"].status == "pass"
    assert by_name["gateway_readiness"].status == "pass"
    assert by_name["worker_health_coverage"].status == "pass"
    assert by_name["database_rls_safety"].status == "pass"
    assert by_name["broker_reachability"].status == "pass"
    assert by_name["object_store_reachability"].status == "pass"
    assert ("db", "postgresql://app@postgres/fyralis") in calls


def test_byoc_post_deploy_validation_rejects_unknown_worker_health() -> None:
    report = run_byoc_post_deploy_validation(
        ByocValidationInputs(
            manifest_path=MANIFEST,
            env_path=ENV_TEMPLATE,
            worker_health_urls={"unknown_worker": "http://worker.local"},
        )
    )

    assert report.status == "fail"
    assert any(
        check.name == "worker_health.unknown_worker" and check.status == "fail"
        for check in report.checks
    )


def test_byoc_post_deploy_report_renders_json() -> None:
    report = run_byoc_post_deploy_validation(
        ByocValidationInputs(manifest_path=MANIFEST, env_path=ENV_TEMPLATE)
    )

    rendered = render_report_json(report)

    assert '"required_checks_passed": true' in rendered
    assert '"manifest_contract"' in rendered


def test_parse_worker_health_args_requires_name_url_pairs() -> None:
    assert parse_worker_health_args(
        ["think_worker=http://127.0.0.1:9300"]
    ) == {"think_worker": "http://127.0.0.1:9300"}
    with pytest.raises(ValueError, match="NAME=URL"):
        parse_worker_health_args(["not-a-pair"])
