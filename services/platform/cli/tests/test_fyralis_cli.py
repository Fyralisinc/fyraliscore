from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import services.platform.cli.fyralis as fyralis_cli
from services.ingest.source_contract import SOURCE_DEFINITIONS
from services.platform.cli.fyralis import main


ROLE_ARN = "arn:aws:iam::123456789012:role/fyralis-dep-example01-bootstrap"


def test_source_display_names_are_contract_derived() -> None:
    assert {
        source.source_id: fyralis_cli._source_display_name(source.source_id)
        for source in SOURCE_DEFINITIONS
    } == {
        source.source_id: (
            source.display.display_name_override or source.display_name
        )
        for source in SOURCE_DEFINITIONS
    }


def test_byoc_agent_cli_runs_customer_cloud_artifact_flow(
    tmp_path: Path,
    capsys,
) -> None:
    workdir = tmp_path / "agent"

    code = main(
        [
            "byoc",
            "agent",
            "role-template",
            "--cloud",
            "aws",
            "--region",
            "us-east-1",
            "--external-id",
            "fyralis-acme-finance-pilot",
            "--workdir",
            str(workdir),
            "--json",
        ]
    )
    role_template = json.loads(capsys.readouterr().out)
    assert code == 0
    assert role_template["schema_version"] == "fyralis.byoc.agent.role_template.v1"
    assert (workdir / "templates" / "setup-role-template.json").is_file()
    assert "fyralis-acme-finance-pilot" not in json.dumps(role_template)

    code = main(
        [
            "byoc",
            "agent",
            "install",
            "--bundle",
            "fyralis-byoc-acme-finance.zip",
            "--region",
            "us-east-1",
            "--workdir",
            str(workdir),
            "--json",
        ]
    )
    registration = json.loads(capsys.readouterr().out)
    assert code == 0
    assert registration["access_mode"] == "customer_cloud_agent"
    assert (workdir / "state" / "registration.json").is_file()

    code = main(
        [
            "byoc",
            "agent",
            "discover",
            "--region",
            "us-east-1",
            "--capabilities",
            "kubernetes,network,secrets,postgres,s3,kafka",
            "--skip-live-aws",
            "--emit-plan",
            "--workdir",
            str(workdir),
            "--json",
        ]
    )
    plan = json.loads(capsys.readouterr().out)
    assert code == 0
    assert plan["schema_version"] == "fyralis.byoc.agent.discovery_plan.v1"
    assert plan["status"] == "ready_for_approval"
    assert len(plan["capabilities"]) == 6
    assert (workdir / "plans" / "latest.json").is_file()
    assert (workdir / "reports" / "aws-live-preflight.json").is_file()

    code = main(
        [
            "byoc",
            "agent",
            "plan",
            "--no-apply",
            "--emit-review-bundle",
            "--workdir",
            str(workdir),
            "--json",
        ]
    )
    review = json.loads(capsys.readouterr().out)
    assert code == 0
    assert review["schema_version"] == "fyralis.byoc.agent.review_bundle.v1"
    assert review["capability_count"] == 6
    assert (workdir / "review" / "latest-review-bundle.json").is_file()

    code = main(
        [
            "byoc",
            "agent",
            "apply",
            "--requires-approval",
            "--plan",
            "latest",
            "--workdir",
            str(workdir),
            "--json",
        ]
    )
    receipt = json.loads(capsys.readouterr().out)
    assert code == 0
    assert receipt["status"] == "approved_for_customer_cloud_execution"
    assert receipt["cloud_mutations_executed"] is False
    assert (workdir / "receipts" / "latest-apply.json").is_file()

    code = main(
        [
            "byoc",
            "agent",
            "validate",
            "--json",
            "--emit-sanitized-readiness-report",
            "--workdir",
            str(workdir),
        ]
    )
    report = json.loads(capsys.readouterr().out)
    assert code == 0
    assert report["schema_version"] == "fyralis.byoc.agent.readiness_report.v1"
    assert report["required_checks_passed"] is True
    assert report["deployment_state"] == "approved_for_customer_cloud_execution"
    assert (workdir / "reports" / "readiness-report.json").is_file()


def test_byoc_agent_register_role_redacts_role_arn(
    tmp_path: Path,
    capsys,
) -> None:
    code = main(
        [
            "byoc",
            "agent",
            "register-role",
            "--role-arn",
            ROLE_ARN,
            "--external-id",
            "external-id",
            "--workdir",
            str(tmp_path),
            "--json",
        ]
    )
    output = capsys.readouterr().out
    payload = json.loads(output)

    assert code == 0
    assert payload["access_mode"] == "customer_cloud_setup_role"
    assert "role_arn_sha256" in payload["sanitized_summary"]
    assert ROLE_ARN not in output
    assert ROLE_ARN in (tmp_path / "state" / "registration.json").read_text(
        encoding="utf-8"
    )


def test_byoc_agent_autopilot_runs_setup_flow(
    tmp_path: Path,
    capsys,
) -> None:
    workdir = tmp_path / "autopilot"

    code = main(
        [
            "byoc",
            "agent",
            "autopilot",
            "--cloud",
            "aws",
            "--region",
            "us-east-1",
            "--external-id",
            "fyralis-acme-finance-pilot",
            "--bundle",
            "fyralis-byoc-acme-finance.zip",
            "--skip-live-aws",
            "--auto-approve",
            "--workdir",
            str(workdir),
            "--json",
        ]
    )
    payload = json.loads(capsys.readouterr().out)

    assert code == 0
    assert payload["schema_version"] == "fyralis.byoc.agent.autopilot_run.v1"
    assert payload["status"] == "ready"
    assert payload["auto_approve"] is True
    assert (workdir / "templates" / "setup-role-template.json").is_file()
    assert (workdir / "plans" / "latest.json").is_file()
    assert (workdir / "receipts" / "latest-apply.json").is_file()
    assert (workdir / "reports" / "readiness-report.json").is_file()


def test_byoc_agent_provider_executor_renders_real_aws_package(
    tmp_path: Path,
    capsys,
) -> None:
    code = main(
        [
            "byoc",
            "agent",
            "provider-executor",
            "--cloud",
            "aws",
            "--region",
            "us-east-1",
            "--stack-name",
            "fyralis-byoc-test",
            "--workdir",
            str(tmp_path),
            "--json",
        ]
    )
    payload = json.loads(capsys.readouterr().out)

    assert code == 0
    assert payload["schema_version"] == "fyralis.byoc.aws_provider_executor.v1"
    assert payload["status"] == "pass"
    assert payload["cloud_api_mutations_executed"] is False
    assert payload["resource_mutations_executed"] is False
    assert (
        tmp_path / "provider" / "aws-cloudformation" / "fyralis-byoc-template.json"
    ).is_file()
    assert (
        tmp_path / "provider" / "aws-cloudformation" / "provider-executor-report.json"
    ).is_file()


def test_byoc_agent_local_rehearsal_generates_zero_spend_runbook(
    tmp_path: Path,
    capsys,
) -> None:
    code = main(
        [
            "byoc",
            "agent",
            "local-rehearsal",
            "--region",
            "us-east-1",
            "--workdir",
            str(tmp_path),
            "--json",
        ]
    )
    payload = json.loads(capsys.readouterr().out)

    assert code == 0
    assert payload["schema_version"] == "fyralis.byoc.local_rehearsal.v1"
    assert payload["status"] == "ready"
    assert payload["zero_cloud_spend"] is True
    assert payload["cloud_mutations_executed"] is False
    assert payload["helm_chart"] == "./deploy/helm/fyralis"
    assert (
        tmp_path / "provider" / "aws-cloudformation" / "provider-executor-report.json"
    ).is_file()
    assert (tmp_path / "local-rehearsal-runbook.json").is_file()
    assert (tmp_path / "customer-source-refs.example.json").is_file()
    assert any(
        "helm template fyralis ./deploy/helm/fyralis" in item["command"]
        for item in payload["commands"]
    )
    assert any(
        "kind create cluster --name fyralis-byoc" in item["command"]
        for item in payload["commands"]
    )


def test_byoc_source_rehearse_slack_generates_real_setup_files(
    tmp_path: Path,
    capsys,
) -> None:
    setup_dir = tmp_path / "slack"

    code = main(
        [
            "byoc",
            "source",
            "rehearse-slack",
            "--setup-dir",
            str(setup_dir),
            "--public-url",
            "https://fyralis-slack.example",
            "--no-start-tunnel",
            "--json",
        ]
    )
    output = capsys.readouterr().out
    payload = json.loads(output)

    assert code == 0
    assert payload["schema_version"] == "fyralis.byoc.source.rehearsal.v1"
    assert payload["status"] == "ready_for_provider_setup"
    assert payload["public_url"] == "https://fyralis-slack.example"
    assert payload["raw_secret_values_exported"] is False
    assert (setup_dir / "fyralis-slack-app-manifest.yaml").is_file()
    assert (setup_dir / "fyralis-slack-app-events-manifest.yaml").is_file()
    assert (setup_dir / "slack-app.env.example").is_file()
    assert (setup_dir / "rehearsal-status.json").is_file()
    manifest = (setup_dir / "fyralis-slack-app-manifest.yaml").read_text(
        encoding="utf-8"
    )
    events_manifest = (setup_dir / "fyralis-slack-app-events-manifest.yaml").read_text(
        encoding="utf-8"
    )
    env_example = (setup_dir / "slack-app.env.example").read_text(encoding="utf-8")
    assert "https://fyralis-slack.example/integrations/slack/callback" in manifest
    assert "channels:history" in manifest
    assert "im:history" in manifest
    assert "https://fyralis-slack.example/webhooks/slack/events" in events_manifest
    assert "message.channels" in events_manifest
    assert "SLACK_CLIENT_SECRET=" in env_example
    assert "OAUTH_STATE_HMAC_KEY=replace-with" not in env_example
    assert (
        "OAUTH_STATE_HMAC_KEY=customer-cloud-secret-ref://oauth-state-hmac-key"
        in env_example
    )


def test_byoc_source_rehearse_slack_reports_missing_env_when_apply_requested(
    tmp_path: Path,
    capsys,
) -> None:
    setup_dir = tmp_path / "slack"

    code = main(
        [
            "byoc",
            "source",
            "rehearse-slack",
            "--setup-dir",
            str(setup_dir),
            "--public-url",
            "https://fyralis-slack.example",
            "--no-start-tunnel",
            "--apply-env",
            "--json",
        ]
    )
    payload = json.loads(capsys.readouterr().out)

    assert code == 1
    assert payload["status"] == "blocked"
    assert payload["provider_env"]["error"] == "provider_env_missing"
    assert "SLACK_CLIENT_SECRET" not in json.dumps(payload)


def test_byoc_source_rehearse_supports_core_provider_set(
    tmp_path: Path,
    capsys,
) -> None:
    expected = {
        "ashby": ("ashby-connection-checklist.json", "ashby.env.example"),
        "jira": ("jira-connect-payload.example.json", "jira.env.example"),
        "github": ("fyralis-github-app-manifest.json", "github.env.example"),
        "discord": ("fyralis-discord-app-setup.json", "discord.env.example"),
        "notion": ("fyralis-notion-app-setup.json", "notion.env.example"),
        "telegram": ("telegram-session-plan.json", "telegram.env.example"),
    }

    for source, (artifact, env_file) in expected.items():
        setup_dir = tmp_path / source
        args = [
            "byoc",
            "source",
            "rehearse",
            "--source",
            source,
            "--setup-dir",
            str(setup_dir),
            "--no-start-tunnel",
            "--json",
        ]
        if source != "telegram":
            args.extend(["--public-url", f"https://{source}.fyralis.example"])
        code = main(args)
        payload = json.loads(capsys.readouterr().out)

        assert code == 0
        assert payload["schema_version"] == "fyralis.byoc.source.rehearsal.v1"
        assert payload["source"] == source
        assert payload["raw_secret_values_exported"] is False
        assert (setup_dir / artifact).is_file()
        assert (setup_dir / env_file).is_file()
        assert (setup_dir / "rehearsal-status.json").is_file()
        assert "CLIENT_SECRET_VALUE" not in json.dumps(payload)


def test_byoc_source_rehearse_figma_builds_private_deployment_oauth_contract(
    tmp_path: Path,
    capsys,
) -> None:
    setup_dir = tmp_path / "figma"

    code = main(
        [
            "byoc",
            "source",
            "rehearse",
            "--source",
            "figma",
            "--setup-dir",
            str(setup_dir),
            "--public-url",
            "https://figma.fyralis.example",
            "--no-start-tunnel",
            "--json",
        ]
    )
    payload = json.loads(capsys.readouterr().out)

    assert code == 0
    assert payload["provider_kind"] == "oauth_app"
    assert payload["status"] == "ready_for_provider_setup"
    assert any(
        gate["name"] == "figma_private_oauth_app_creation_or_update"
        for gate in payload["manual_gates"]
    )
    assert payload["browser_agent_run"]["native_connect"]["kind"] == (
        "figma_oauth_file_scoped_connect"
    )
    assert not any(
        action["id"] in {"run_native_preflight", "run_native_finalize"}
        for action in payload["browser_agent_run"]["action_queue"]
    )

    checklist = json.loads(
        (setup_dir / "figma-connection-checklist.json").read_text(encoding="utf-8")
    )
    env_example = (setup_dir / "figma.env.example").read_text(encoding="utf-8")
    assert checklist["callback_url"] == (
        "https://figma.fyralis.example/integrations/figma/oauth/callback"
    )
    assert checklist["webhook_url"] is None
    assert checklist["default_scopes"] == [
        "current_user:read",
        "file_metadata:read",
        "file_content:read",
        "file_comments:read",
        "file_versions:read",
    ]
    assert checklist["browser_agent_run"]["provider_setup_bundle"]["kind"] == (
        "figma_deployment_oauth_app_setup"
    )
    assert "FIGMA_OAUTH_ENABLED=1" in env_example
    assert (
        "FIGMA_REDIRECT_URI=https://figma.fyralis.example/"
        "integrations/figma/oauth/callback" in env_example
    )
    assert "FIGMA_CLIENT_SECRET_SECRET_REF=" in env_example
    assert "FIGMA_CLIENT_SECRET=" not in env_example
    assert "api_token" not in json.dumps(payload)


def test_byoc_source_rehearse_figma_apply_env_rolls_out_to_oauth_ingestion_and_reconcilers(
    tmp_path: Path,
    capsys,
    monkeypatch,
) -> None:
    env_path = tmp_path / "figma.env"
    env_path.write_text(
        "\n".join(
            (
                "FIGMA_OAUTH_ENABLED=1",
                "FIGMA_CLIENT_ID=figma-client-id",
                "FIGMA_CLIENT_SECRET_SECRET_REF=prod/fyralis/figma-client-secret",
                "FIGMA_REDIRECT_URI=https://figma.fyralis.example/integrations/figma/oauth/callback",
                "FIGMA_OAUTH_UI_BASE_URL=https://app.fyralis.example",
                "FIGMA_OAUTH_SCOPES=current_user:read,file_metadata:read,file_content:read,file_comments:read,file_versions:read",
                "OAUTH_STATE_HMAC_KEY_SECRET_REF=prod/fyralis/oauth-state-hmac-key",
                # A local fallback must never be copied into the Kubernetes
                # integration secret by the BYOC rollout command.
                "FIGMA_CLIENT_SECRET=development-only-plaintext",
            )
        )
        + "\n",
        encoding="utf-8",
    )
    calls: list[tuple[list[str], dict[str, object]]] = []
    rendered_env_files: list[str] = []

    def fake_run(command, **kwargs):  # noqa: ANN001, ANN003 - subprocess seam
        command_list = [str(value) for value in command]
        calls.append((command_list, kwargs))
        if command_list[3:5] == ["get", "deployment"]:
            return SimpleNamespace(
                stdout=json.dumps(
                    {
                        "items": [
                            {
                                "metadata": {
                                    "name": "fyralis-gateway",
                                    "labels": {
                                        "app.kubernetes.io/component": "gateway",
                                    },
                                }
                            },
                            {
                                "metadata": {
                                    "name": "fyralis-shard-fetch",
                                    "labels": {
                                        "app.kubernetes.io/component": "shard-fetch",
                                    },
                                }
                            },
                            {
                                "metadata": {
                                    "name": "fyralis-reconciler",
                                    "labels": {
                                        "app.kubernetes.io/component": "reconciler",
                                    },
                                }
                            },
                            {
                                "metadata": {
                                    "name": "fyralis-periodic-reconciler",
                                    "labels": {
                                        "app.kubernetes.io/component": "periodic-reconciler",
                                    },
                                }
                            },
                        ]
                    }
                ).encode("utf-8")
            )
        env_file_argument = next(
            (value for value in command_list if value.startswith("--from-env-file=")),
            None,
        )
        if env_file_argument:
            rendered_env_files.append(
                Path(env_file_argument.removeprefix("--from-env-file=")).read_text(
                    encoding="utf-8"
                )
            )
        return SimpleNamespace(stdout=b"apiVersion: v1\n")

    monkeypatch.setattr(fyralis_cli.subprocess, "run", fake_run)

    code = main(
        [
            "byoc",
            "source",
            "rehearse",
            "--source",
            "figma",
            "--setup-dir",
            str(tmp_path / "figma"),
            "--provider-env",
            str(env_path),
            "--apply-env",
            "--public-url",
            "https://figma.fyralis.example",
            "--no-start-tunnel",
            "--kubectl",
            "kubectl-test",
            "--namespace",
            "customer-fyralis",
            "--json",
        ]
    )
    payload = json.loads(capsys.readouterr().out)

    expected_targets = [
        "fyralis-gateway",
        "fyralis-shard-fetch",
        "fyralis-reconciler",
        "fyralis-periodic-reconciler",
    ]
    assert code == 0
    assert payload["provider_env"]["applied"] is True
    assert payload["provider_env"]["mode"] == "figma_runtime_env_applied"
    assert payload["provider_env"]["runtime_deployments"] == expected_targets

    set_env_targets = [
        command[5].removeprefix("deployment/")
        for command, _kwargs in calls
        if command[3:5] == ["set", "env"]
    ]
    rollout_targets = [
        command[5].removeprefix("deployment/")
        for command, _kwargs in calls
        if command[3:5] == ["rollout", "status"]
    ]
    assert set_env_targets == expected_targets
    assert rollout_targets == expected_targets
    assert len(rendered_env_files) == 1
    assert "FIGMA_CLIENT_SECRET_SECRET_REF=prod/fyralis/figma-client-secret" in rendered_env_files[0]
    assert "FIGMA_CLIENT_SECRET=development-only-plaintext" not in rendered_env_files[0]


def test_figma_runtime_deployment_discovery_skips_absent_optional_reconcilers(
    monkeypatch,
) -> None:
    def fake_run(command, **kwargs):  # noqa: ANN001, ANN003 - subprocess seam
        del kwargs
        assert command[3:5] == ["get", "deployment"]
        return SimpleNamespace(
            stdout=json.dumps(
                {
                    "items": [
                        {
                            "metadata": {
                                "name": "customer-a-gateway",
                                "labels": {
                                    "app.kubernetes.io/component": "gateway",
                                },
                            }
                        },
                        {
                            "metadata": {
                                "name": "customer-a-shard-fetch",
                                "labels": {
                                    "app.kubernetes.io/component": "shard-fetch",
                                },
                            }
                        },
                    ]
                }
            ).encode("utf-8")
        )

    monkeypatch.setattr(fyralis_cli.subprocess, "run", fake_run)

    targets = fyralis_cli._provider_runtime_deployments(
        SimpleNamespace(kubectl="kubectl-test", namespace="customer-fyralis"),
        fyralis_cli._source_rehearsal_profile("figma"),
    )

    assert targets == ("customer-a-gateway", "customer-a-shard-fetch")


def test_figma_runtime_deployment_discovery_requires_gateway_and_shard_fetch(
    monkeypatch,
) -> None:
    def fake_run(command, **kwargs):  # noqa: ANN001, ANN003 - subprocess seam
        del command, kwargs
        return SimpleNamespace(
            stdout=json.dumps(
                {
                    "items": [
                        {
                            "metadata": {
                                "name": "customer-a-gateway",
                                "labels": {
                                    "app.kubernetes.io/component": "gateway",
                                },
                            }
                        }
                    ]
                }
            ).encode("utf-8")
        )

    monkeypatch.setattr(fyralis_cli.subprocess, "run", fake_run)

    try:
        fyralis_cli._provider_runtime_deployments(
            SimpleNamespace(kubectl="kubectl-test", namespace="customer-fyralis"),
            fyralis_cli._source_rehearsal_profile("figma"),
        )
    except ValueError as exc:
        assert "shard-fetch" in str(exc)
    else:
        raise AssertionError("Figma rollout must require the shard-fetch deployment")


def test_byoc_source_rehearse_figma_apply_env_fails_before_kubectl_when_a_required_ref_is_missing(
    tmp_path: Path,
    capsys,
    monkeypatch,
) -> None:
    env_path = tmp_path / "figma.env"
    env_path.write_text(
        "\n".join(
            (
                "FIGMA_OAUTH_ENABLED=1",
                "FIGMA_CLIENT_ID=figma-client-id",
                "FIGMA_CLIENT_SECRET_SECRET_REF=prod/fyralis/figma-client-secret",
                "FIGMA_REDIRECT_URI=https://figma.fyralis.example/integrations/figma/oauth/callback",
                "FIGMA_OAUTH_UI_BASE_URL=https://app.fyralis.example",
                "FIGMA_OAUTH_SCOPES=current_user:read,file_metadata:read,file_content:read,file_comments:read,file_versions:read",
            )
        )
        + "\n",
        encoding="utf-8",
    )

    def unexpected_kubectl(*_args, **_kwargs):  # noqa: ANN002, ANN003 - subprocess seam
        raise AssertionError("kubectl must not run for incomplete Figma configuration")

    monkeypatch.setattr(fyralis_cli.subprocess, "run", unexpected_kubectl)

    code = main(
        [
            "byoc",
            "source",
            "rehearse",
            "--source",
            "figma",
            "--setup-dir",
            str(tmp_path / "figma"),
            "--provider-env",
            str(env_path),
            "--apply-env",
            "--public-url",
            "https://figma.fyralis.example",
            "--no-start-tunnel",
            "--json",
        ]
    )
    payload = json.loads(capsys.readouterr().out)

    assert code == 1
    assert payload["status"] == "blocked"
    assert payload["provider_env"]["error"] == "missing_env_values"
    assert payload["provider_env"]["missing"] == ["OAUTH_STATE_HMAC_KEY_SECRET_REF"]


def test_byoc_agent_autopilot_can_run_provider_executor_render_path(
    tmp_path: Path,
    capsys,
) -> None:
    code = main(
        [
            "byoc",
            "agent",
            "autopilot",
            "--cloud",
            "aws",
            "--region",
            "us-east-1",
            "--external-id",
            "fyralis-acme-finance-pilot",
            "--skip-live-aws",
            "--auto-approve",
            "--run-provider-executor",
            "--workdir",
            str(tmp_path),
            "--json",
        ]
    )
    payload = json.loads(capsys.readouterr().out)

    assert code == 0
    assert payload["status"] == "ready"
    assert payload["artifacts"]["provider_executor_report"] is not None
    assert payload["cloud_mutations_executed"] is False
    assert (
        tmp_path / "provider" / "aws-cloudformation" / "provider-executor-report.json"
    ).is_file()


def test_byoc_source_autopilot_redacts_credential_ref(
    tmp_path: Path,
    capsys,
) -> None:
    code = main(
        [
            "byoc",
            "source",
            "autopilot",
            "--source",
            "slack",
            "--credential-ref",
            "aws-secretsmanager:/fyralis/sources/slack/oauth",
            "--scopes",
            "#leadership,#finance-ops",
            "--sync-mode",
            "limited-backfill",
            "--auto-activate",
            "--workdir",
            str(tmp_path),
            "--json",
        ]
    )
    output = capsys.readouterr().out
    payload = json.loads(output)

    assert code == 0
    assert payload["schema_version"] == "fyralis.byoc.source.autopilot_run.v1"
    assert payload["status"] == "active"
    assert payload["source"] == "slack"
    assert payload["source_count"] == 1
    assert "credential_ref_sha256" in payload["sources"][0]
    assert payload["sources"][0]["browser_agent"]["source"] == "slack"
    assert payload["sources"][0]["browser_agent_run"]["source"] == "slack"
    assert "aws-secretsmanager:/fyralis/sources/slack/oauth" not in output
    connection_path = tmp_path / "sources" / "slack" / "connection.json"
    assert connection_path.is_file()
    connection = json.loads(connection_path.read_text(encoding="utf-8"))
    secret_refs_path = tmp_path / "sources" / "slack" / "secret-refs.json"
    secret_refs_text = secret_refs_path.read_text(encoding="utf-8")
    secret_refs = json.loads(secret_refs_text)
    connection_text = connection_path.read_text(encoding="utf-8")
    assert "credential_ref" not in connection
    assert "aws-secretsmanager:/fyralis/sources/slack/oauth" not in connection_text
    assert "aws-secretsmanager:/fyralis/sources/slack/oauth" not in secret_refs_text
    assert secret_refs["credential_ref_hint"] == (
        "aws-secretsmanager:/fyralis/sources/slack/[provided]"
    )
    assert secret_refs["required_refs"]["bot_token"]["ref_hint"].endswith("/[provided]")
    assert connection["browser_agent"]["source"] == "slack"
    assert connection["browser_agent_run"]["source"] == "slack"
    assert (
        connection["browser_agent_run"]["handoff_url"] == "https://api.slack.com/apps"
    )
    assert connection["browser_agent_run"]["native_connect"]["kind"] == (
        "oauth_callback_native_connect"
    )
    assert any(
        action["id"] == "run_native_preflight"
        for action in connection["browser_agent_run"]["action_queue"]
    )
    setup_bundle = connection["browser_agent_run"]["provider_setup_bundle"]
    assert setup_bundle["kind"] == "slack_app_manifest"
    assert setup_bundle["native_connect"]["preflight_path"] == (
        "/integrations/slack/connect/preflight"
    )
    assert setup_bundle["oauth_redirect_url"] == (
        "https://fyralis-ingress.customer.example/integrations/slack/callback"
    )
    assert setup_bundle["events_request_url"] == (
        "https://fyralis-ingress.customer.example/webhooks/slack/events"
    )
    assert setup_bundle["browser_dom_plan"]["schema_version"] == (
        "fyralis.byoc.source.browser_dom_plan.v1"
    )
    assert setup_bundle["browser_dom_plan"]["steps"]
    assert any(
        action["id"] == "generate_slack_app_manifest"
        for action in connection["browser_agent_run"]["action_queue"]
    )
    assert (
        connection["browser_agent"]["provider_console_url"]
        == "https://api.slack.com/apps"
    )
    assert (tmp_path / "sources" / "slack" / "provider-setup.json").is_file()
    assert (tmp_path / "sources" / "slack" / "secret-refs.json").is_file()
    assert (tmp_path / "sources" / "slack" / "readiness-receipt.json").is_file()
    assert (tmp_path / "sources" / "slack" / "activation.json").is_file()

    code = main(
        [
            "byoc",
            "source",
            "browser-agent",
            "--source",
            "slack",
            "--workdir",
            str(tmp_path),
            "--json",
        ]
    )
    browser_agent = json.loads(capsys.readouterr().out)
    assert code == 0
    assert browser_agent["schema_version"] == (
        "fyralis.byoc.source.browser_agent_runner_receipt.v1"
    )
    assert browser_agent["source"] == "slack"
    assert browser_agent["status"] == "waiting_for_admin"
    assert browser_agent["handoff_url"] == "https://api.slack.com/apps"
    assert any(
        action["id"] == "execute_slack_browser_dom_plan" and action["status"] == "ready"
        for action in browser_agent["action_results"]
    )
    assert "fyralis-slack-app-manifest" in json.dumps(
        browser_agent["generated_artifacts"]
    )
    assert "browser-dom-plan" in browser_agent["generated_artifacts"]
    generated_manifest = (
        tmp_path
        / "sources"
        / "slack"
        / "browser-agent-provider-setup"
        / "fyralis-slack-app-manifest.yaml"
    )
    generated_events_manifest = (
        tmp_path
        / "sources"
        / "slack"
        / "browser-agent-provider-setup"
        / "fyralis-slack-app-events-manifest.yaml"
    )
    assert generated_manifest.is_file()
    assert generated_events_manifest.is_file()
    generated_dom_plan = (
        tmp_path
        / "sources"
        / "slack"
        / "browser-agent-provider-setup"
        / "browser-dom-plan.json"
    )
    assert generated_dom_plan.is_file()
    dom_plan = json.loads(generated_dom_plan.read_text(encoding="utf-8"))
    assert dom_plan["source"] == "slack"
    assert any(
        step["action"] == "paste_or_upload_manifest" for step in dom_plan["steps"]
    )
    generated_launcher = (
        tmp_path
        / "sources"
        / "slack"
        / "browser-agent-provider-setup"
        / "run-provider-browser-agent.sh"
    )
    generated_refs = (
        tmp_path
        / "sources"
        / "slack"
        / "browser-agent-provider-setup"
        / "customer-cloud-generated-refs.json"
    )
    assert generated_launcher.is_file()
    assert "--execute-browser-dom" in generated_launcher.read_text(encoding="utf-8")
    assert generated_refs.is_file()
    refs_payload = json.loads(generated_refs.read_text(encoding="utf-8"))
    assert refs_payload["raw_secret_values_included"] is False
    assert "https://fyralis-ingress.customer.example/integrations/slack/callback" in (
        generated_manifest.read_text(encoding="utf-8")
    )
    assert "https://fyralis-ingress.customer.example/webhooks/slack/events" in (
        generated_events_manifest.read_text(encoding="utf-8")
    )
    assert (tmp_path / "sources" / "slack" / "browser-agent-receipt.json").is_file()


def test_byoc_source_browser_agent_maps_oauth_callback_waiting(
    tmp_path: Path,
    capsys,
    monkeypatch,
) -> None:
    run_path = tmp_path / "slack-run.json"
    native_payload_path = tmp_path / "native-payload.json"
    native_payload_path.write_text("{}\n", encoding="utf-8")
    run_path.write_text(
        json.dumps(
            {
                "schema_version": "fyralis.byoc.source.browser_agent_run.v1",
                "source": "slack",
                "state": "waiting_for_admin",
                "handoff_url": None,
                "native_connect": {
                    "kind": "oauth_callback_native_connect",
                    "preflight_path": "/integrations/slack/connect/preflight",
                    "finalize_path": "/integrations/slack/connect/finalize",
                    "payload_fields": ["installation_id"],
                },
                "action_queue": [
                    {
                        "id": "run_native_finalize",
                        "owner": "fyralis_agent",
                        "status": "pending",
                    }
                ],
            }
        )
        + "\n",
        encoding="utf-8",
    )

    class _Response:
        status_code = 202

        def json(self):
            return {
                "ok": True,
                "state": "waiting_for_provider_callback",
                "message": "Provider callback is still pending.",
            }

    class _Client:
        def __init__(self, *, timeout):
            self.timeout = timeout

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def post(self, endpoint, *, json, headers):
            assert (
                endpoint
                == "https://gateway.example/integrations/slack/connect/finalize"
            )
            assert json == {}
            return _Response()

    monkeypatch.setattr(
        "services.platform.runtime.source_browser_agent_runner.httpx.AsyncClient",
        _Client,
    )

    code = main(
        [
            "byoc",
            "source",
            "browser-agent",
            "--source",
            "slack",
            "--run-artifact",
            str(run_path),
            "--gateway-api-base",
            "https://gateway.example",
            "--native-payload",
            str(native_payload_path),
            "--execute-native",
            "--admin-approved",
            "--json",
        ]
    )
    payload = json.loads(capsys.readouterr().out)

    assert code == 0
    assert payload["status"] == "waiting_for_admin"
    assert payload["waiting_action_count"] == 1
    assert payload["action_results"][0]["status"] == "waiting"
    assert (
        payload["action_results"][0]["detail"] == "Provider callback is still pending."
    )


def test_byoc_source_autopilot_uses_preauthorized_ref_manifest(
    tmp_path: Path,
    capsys,
) -> None:
    manifest = tmp_path / "refs.json"
    manifest.write_text(
        json.dumps(
            {
                "sources": {
                    "slack": {
                        "credential_ref": "aws-secretsmanager:/customer/slack",
                        "required_refs": {
                            "bot_token": "aws-secretsmanager:/customer/slack/bot",
                            "signing_secret": (
                                "aws-secretsmanager:/customer/slack/signing"
                            ),
                        },
                    }
                }
            }
        ),
        encoding="utf-8",
    )

    code = main(
        [
            "byoc",
            "source",
            "autopilot",
            "--source",
            "slack",
            "--provider-authorization-mode",
            "preauthorized-ref",
            "--preauthorized-ref-manifest",
            str(manifest),
            "--workdir",
            str(tmp_path / "agent"),
            "--json",
        ]
    )
    output = capsys.readouterr().out
    payload = json.loads(output)

    assert code == 0
    source = payload["sources"][0]
    assert source["preauthorized_refs_present"] is True
    assert source["customer_action_required"] == []
    assert "aws-secretsmanager:/customer/slack" not in output


def test_byoc_source_autopilot_can_prepare_all_sources(
    tmp_path: Path,
    capsys,
) -> None:
    code = main(
        [
            "byoc",
            "source",
            "autopilot",
            "--source",
            "all",
            "--scopes",
            "auto",
            "--sync-mode",
            "dry-run",
            "--auto-activate",
            "--workdir",
            str(tmp_path),
            "--json",
        ]
    )
    output = capsys.readouterr().out
    payload = json.loads(output)

    assert code == 0
    assert payload["source"] == "all"
    assert payload["source_count"] == 27
    assert payload["active_source_count"] == 27
    assert {item["source"] for item in payload["sources"]} >= {
        "slack",
        "github",
        "gmail",
        "aws",
        "quickbooks",
    }
    assert "aws-secretsmanager:/fyralis/sources" not in output
    expected_bundle_kinds = {
        "ashby": "api_token_provider_setup",
        "aws": "aws_iam_role_setup",
        "brex": "api_token_provider_setup",
        "carta": "oauth_provider_setup",
        "deel": "api_token_provider_setup",
        "discord": "discord_application_setup",
        "facebook_pages": "oauth_provider_setup",
        "figma": "figma_deployment_oauth_app_setup",
        "fireflies": "api_token_provider_setup",
        "github": "github_app_manifest",
        "gmail": "google_workspace_dwd_setup",
        "google-calendar": "google_workspace_dwd_setup",
        "google-drive": "google_workspace_dwd_setup",
        "grafana": "api_token_provider_setup",
        "gusto": "oauth_provider_setup",
        "hibob": "api_token_provider_setup",
        "jira": "jira_api_token_webhook_setup",
        "linkedin": "oauth_provider_setup",
        "mercury": "api_token_provider_setup",
        "miro": "api_token_provider_setup",
        "notion": "notion_integration_setup",
        "quickbooks": "oauth_provider_setup",
        "ramp": "api_token_provider_setup",
        "signal": "local_gateway_session_setup",
        "slack": "slack_app_manifest",
        "telegram": "local_gateway_session_setup",
        "whatsapp": "whatsapp_webhook_setup",
    }
    for source_id, expected_kind in expected_bundle_kinds.items():
        connection = json.loads(
            (tmp_path / "sources" / source_id / "connection.json").read_text(
                encoding="utf-8"
            )
        )
        bundle = connection["browser_agent_run"]["provider_setup_bundle"]
        assert bundle["kind"] == expected_kind
        assert bundle["kind"] != "generic_provider_setup_contract"
        assert bundle["artifacts"]
        assert bundle["browser_tasks"]
        assert bundle["browser_dom_plan"]["schema_version"] == (
            "fyralis.byoc.source.browser_dom_plan.v1"
        )
        assert bundle["browser_dom_plan"]["steps"]
        assert any(
            action["kind"] == "materialize_provider_setup_bundle"
            for action in bundle["agent_actions"]
        )
        assert any(
            action["kind"] == "materialize_browser_dom_plan"
            for action in bundle["agent_actions"]
        )
    expected_preflight_finalize_sources = {
        "ashby",
        "aws",
        "brex",
        "carta",
        "deel",
        "discord",
        "facebook_pages",
        "fireflies",
        "github",
        "gmail",
        "google-calendar",
        "google-drive",
        "grafana",
        "gusto",
        "hibob",
        "jira",
        "linkedin",
        "mercury",
        "miro",
        "notion",
        "quickbooks",
        "ramp",
        "signal",
        "slack",
        "telegram",
        "whatsapp",
    }
    for source_id in expected_preflight_finalize_sources:
        connection = json.loads(
            (tmp_path / "sources" / source_id / "connection.json").read_text(
                encoding="utf-8"
            )
        )
        native_connect = connection["browser_agent_run"]["native_connect"]
        assert native_connect["preflight_path"].startswith(
            f"/integrations/{source_id.replace('-', '_')}/connect/"
        )
        assert native_connect["finalize_path"].endswith("/connect/finalize")
        assert native_connect["payload_fields"]
        assert any(
            action["id"] == "run_native_preflight"
            for action in connection["browser_agent_run"]["action_queue"]
        )
    assert (tmp_path / "sources" / "slack" / "connection.json").is_file()
    google_calendar_connection = json.loads(
        (tmp_path / "sources" / "google-calendar" / "connection.json").read_text(
            encoding="utf-8"
        )
    )
    discord_connection = json.loads(
        (tmp_path / "sources" / "discord" / "connection.json").read_text(
            encoding="utf-8"
        )
    )
    notion_connection = json.loads(
        (tmp_path / "sources" / "notion" / "connection.json").read_text(
            encoding="utf-8"
        )
    )
    assert google_calendar_connection["method"] == "dwd"
    assert (
        google_calendar_connection["native_connect"]["kind"] == "google_workspace_dwd"
    )
    assert google_calendar_connection["native_connect"]["preflight_path"] == (
        "/integrations/google_calendar/connect/preflight"
    )
    assert google_calendar_connection["provider_ingress_endpoints"] == [
        (
            "https://fyralis-ingress.customer.example"
            "/webhooks/google_calendar/push"
        )
    ]
    assert google_calendar_connection["browser_agent_run"]["events_request_url"] == (
        "https://fyralis-ingress.customer.example/webhooks/google_calendar/push"
    )
    assert "poll-only" not in json.dumps(google_calendar_connection)
    assert discord_connection["method"] == "oauth_plus_gateway"
    assert notion_connection["method"] == "oauth"
    figma_connection = json.loads(
        (tmp_path / "sources" / "figma" / "connection.json").read_text(encoding="utf-8")
    )
    assert figma_connection["method"] == "oauth"
    assert figma_connection["native_connect"] == {
        "kind": "figma_oauth_file_scoped_connect",
        "start_path": "/integrations/figma/oauth/start",
        "status_path": "/integrations/figma/connect/status",
        "retry_path": "/integrations/figma/connect/retry",
        "disconnect_path": "/integrations/figma/connect",
        "payload_fields": ["file_urls", "return_path"],
    }
    assert not any(
        action["id"] in {"run_native_preflight", "run_native_finalize"}
        for action in figma_connection["browser_agent_run"]["action_queue"]
    )
    assert "api_token" not in json.dumps(figma_connection)
    assert (
        "https://fyralis-ingress.customer.example/webhooks/notion/events"
        in notion_connection["provider_ingress_endpoints"]
    )
    assert "oauth_client" not in json.dumps(google_calendar_connection)
    code = main(
        [
            "byoc",
            "source",
            "browser-agent",
            "--source",
            "google-calendar",
            "--workdir",
            str(tmp_path),
            "--gateway-api-base",
            "https://fyralis-ingress.customer.example",
            "--json",
        ]
    )
    google_agent = json.loads(capsys.readouterr().out)
    assert code == 0
    assert google_agent["native_connect_kind"] == "google_workspace_dwd"
    assert any(
        action["id"] == "run_native_preflight" and action["status"] == "ready"
        for action in google_agent["action_results"]
    )
    assert (
        tmp_path / "sources" / "google-calendar" / "browser-agent-receipt.json"
    ).is_file()
    code = main(
        [
            "byoc",
            "source",
            "browser-agent",
            "--source",
            "all",
            "--workdir",
            str(tmp_path),
            "--gateway-api-base",
            "https://fyralis-ingress.customer.example",
            "--json",
        ]
    )
    browser_agents = json.loads(capsys.readouterr().out)
    assert code == 0
    assert browser_agents["schema_version"] == (
        "fyralis.byoc.source.browser_agent_run_set.v1"
    )
    assert browser_agents["orchestration_mode"] == "parallel_per_source_browser_agents"
    assert browser_agents["source_count"] == 27
    assert browser_agents["waiting_source_count"] >= 1
    assert browser_agents["automated_action_count"] >= 27
    assert (tmp_path / "sources" / "latest-browser-agent.json").is_file()
    assert (
        tmp_path / "sources" / "google-drive" / "browser-agent-receipt.json"
    ).is_file()
    assert (
        tmp_path
        / "sources"
        / "google-drive"
        / "browser-agent-provider-setup"
        / "fyralis-google-drive-dwd-preflight.json"
    ).is_file()
    assert (
        tmp_path
        / "sources"
        / "quickbooks"
        / "browser-agent-provider-setup"
        / "fyralis-quickbooks-oauth-setup.json"
    ).is_file()
    assert (
        tmp_path
        / "sources"
        / "aws"
        / "browser-agent-provider-setup"
        / "fyralis-aws-iam-role-setup.json"
    ).is_file()
    for source_id in expected_bundle_kinds:
        assert (
            tmp_path
            / "sources"
            / source_id
            / "browser-agent-provider-setup"
            / "browser-dom-plan.json"
        ).is_file()
    assert (tmp_path / "sources" / "quickbooks" / "provider-setup.json").is_file()
    assert (tmp_path / "sources" / "aws" / "readiness-receipt.json").is_file()


def test_byoc_source_lifecycle_connects_slack_from_preauthorized_refs(
    tmp_path: Path,
    capsys,
) -> None:
    workdir = tmp_path / "agent"
    manifest = tmp_path / "customer-source-refs.json"
    manifest.write_text(
        json.dumps(
            {
                "sources": {
                    "slack": {
                        "credential_ref": "aws-secretsmanager:/customer/slack",
                        "required_refs": {
                            "oauth_client": "aws-secretsmanager:/customer/slack/oauth",
                            "bot_token": "aws-secretsmanager:/customer/slack/bot",
                            "signing_secret": (
                                "aws-secretsmanager:/customer/slack/signing"
                            ),
                        },
                    }
                }
            }
        ),
        encoding="utf-8",
    )
    common = [
        "--source",
        "slack",
        "--provider-authorization-mode",
        "preauthorized-ref",
        "--preauthorized-ref-manifest",
        str(manifest),
        "--workdir",
        str(workdir),
        "--json",
    ]

    code = main(["byoc", "source", "discover", *common])
    output = capsys.readouterr().out
    discovery = json.loads(output)
    assert code == 0
    assert discovery["schema_version"] == "fyralis.byoc.source.discovery_run.v1"
    assert discovery["status"] == "ready_to_plan"
    assert discovery["sources"][0]["human_gates"] == []
    assert "aws-secretsmanager:/customer/slack" not in output

    code = main(
        [
            "byoc",
            "source",
            "plan",
            *common,
            "--sync-mode",
            "limited-backfill",
            "--backfill-window",
            "30d",
        ]
    )
    plan = json.loads(capsys.readouterr().out)
    assert code == 0
    assert plan["schema_version"] == "fyralis.byoc.source.plan_run.v1"
    assert plan["status"] == "ready_for_approval"
    assert plan["sources"][0]["human_gate_count"] == 0
    assert (workdir / "sources" / "slack" / "source-contract.json").is_file()
    assert (workdir / "sources" / "slack" / "source-plan.json").is_file()

    code = main(
        [
            "byoc",
            "source",
            "apply",
            *common,
            "--requires-approval",
            "--plan",
            "latest",
            "--sync-mode",
            "limited-backfill",
            "--backfill-window",
            "30d",
        ]
    )
    apply = json.loads(capsys.readouterr().out)
    assert code == 0
    assert apply["schema_version"] == "fyralis.byoc.source.apply_run.v1"
    assert apply["status"] == "applied"
    assert (workdir / "sources" / "slack" / "provider-setup.json").is_file()
    assert (workdir / "sources" / "slack" / "secret-refs.json").is_file()
    assert (workdir / "sources" / "slack" / "connection.json").is_file()
    assert (workdir / "sources" / "slack" / "apply-receipt.json").is_file()

    code = main(["byoc", "source", "validate", *common, "--live"])
    validation = json.loads(capsys.readouterr().out)
    assert code == 0
    assert validation["schema_version"] == "fyralis.byoc.source.validation_run.v1"
    assert validation["status"] == "passed"
    assert (workdir / "sources" / "slack" / "validation.json").is_file()

    code = main(
        [
            "byoc",
            "source",
            "activate",
            *common,
            "--requires-approval",
            "--start-first-sync",
            "--sync-mode",
            "limited-backfill",
            "--backfill-window",
            "30d",
        ]
    )
    activation = json.loads(capsys.readouterr().out)
    assert code == 0
    assert activation["schema_version"] == "fyralis.byoc.source.activation_run.v1"
    assert activation["status"] == "active"
    assert (workdir / "sources" / "slack" / "first-sync.json").is_file()
    assert (workdir / "sources" / "slack" / "activation.json").is_file()
    assert (workdir / "sources" / "slack" / "readiness-receipt.json").is_file()


def test_byoc_source_apply_blocks_when_human_gates_are_unresolved(
    tmp_path: Path,
    capsys,
) -> None:
    workdir = tmp_path / "agent"

    code = main(
        [
            "byoc",
            "source",
            "plan",
            "--source",
            "slack",
            "--provider-authorization-mode",
            "preauthorized-ref",
            "--workdir",
            str(workdir),
            "--json",
        ]
    )
    plan = json.loads(capsys.readouterr().out)
    assert code == 0
    assert plan["status"] == "blocked_on_human_gates"
    assert plan["sources"][0]["human_gate_count"] == 1

    code = main(
        [
            "byoc",
            "source",
            "apply",
            "--source",
            "slack",
            "--provider-authorization-mode",
            "preauthorized-ref",
            "--requires-approval",
            "--plan",
            "latest",
            "--workdir",
            str(workdir),
            "--json",
        ]
    )
    apply = json.loads(capsys.readouterr().out)
    assert code == 1
    assert apply["status"] == "blocked_on_human_gates"
    assert apply["sources"][0]["human_gates"][0]["can_agent_complete"] is False
    assert (workdir / "sources" / "slack" / "apply-blocker.json").is_file()


def test_byoc_source_plan_generates_contracts_for_all_sources(
    tmp_path: Path,
    capsys,
) -> None:
    code = main(
        [
            "byoc",
            "source",
            "plan",
            "--source",
            "all",
            "--scopes",
            "auto",
            "--sync-mode",
            "dry-run",
            "--workdir",
            str(tmp_path),
            "--json",
        ]
    )
    output = capsys.readouterr().out
    payload = json.loads(output)

    assert code == 0
    assert payload["schema_version"] == "fyralis.byoc.source.plan_run.v1"
    assert payload["source_count"] == 27
    assert payload["status"] == "blocked_on_human_gates"
    assert "aws-secretsmanager:/fyralis/sources" not in output
    assert (tmp_path / "sources" / "slack" / "source-contract.json").is_file()
    assert (tmp_path / "sources" / "quickbooks" / "source-plan.json").is_file()
    assert (tmp_path / "sources" / "aws" / "source-contract.json").is_file()


def test_byoc_agent_apply_requires_explicit_approval(
    tmp_path: Path,
    capsys,
) -> None:
    code = main(
        [
            "byoc",
            "agent",
            "apply",
            "--plan",
            "latest",
            "--workdir",
            str(tmp_path),
            "--json",
        ]
    )
    captured = capsys.readouterr()

    assert code == 2
    assert "requires --requires-approval" in captured.err
    assert captured.out == ""
