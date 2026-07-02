from __future__ import annotations

import json
from pathlib import Path

from services.platform.cli.fyralis import main


ROLE_ARN = "arn:aws:iam::123456789012:role/fyralis-dep-example01-bootstrap"


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
    assert (
        workdir / "templates" / "setup-role-template.json"
    ).is_file()
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
        tmp_path
        / "provider"
        / "aws-cloudformation"
        / "fyralis-byoc-template.json"
    ).is_file()
    assert (
        tmp_path
        / "provider"
        / "aws-cloudformation"
        / "provider-executor-report.json"
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
        tmp_path
        / "provider"
        / "aws-cloudformation"
        / "provider-executor-report.json"
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
    events_manifest = (
        setup_dir / "fyralis-slack-app-events-manifest.yaml"
    ).read_text(encoding="utf-8")
    env_example = (setup_dir / "slack-app.env.example").read_text(encoding="utf-8")
    assert "https://fyralis-slack.example/integrations/slack/callback" in manifest
    assert "channels:history" in manifest
    assert "im:history" in manifest
    assert "https://fyralis-slack.example/webhooks/slack/events" in events_manifest
    assert "message.channels" in events_manifest
    assert "SLACK_CLIENT_SECRET=" in env_example
    assert "OAUTH_STATE_HMAC_KEY=replace-with" not in env_example


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
        "jira": ("jira-provider-setup.json", "jira.env.example"),
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
        tmp_path
        / "provider"
        / "aws-cloudformation"
        / "provider-executor-report.json"
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
    assert "aws-secretsmanager:/fyralis/sources/slack/oauth" not in output
    assert (tmp_path / "sources" / "slack" / "connection.json").is_file()
    assert (tmp_path / "sources" / "slack" / "provider-setup.json").is_file()
    assert (tmp_path / "sources" / "slack" / "secret-refs.json").is_file()
    assert (tmp_path / "sources" / "slack" / "readiness-receipt.json").is_file()
    assert (tmp_path / "sources" / "slack" / "activation.json").is_file()


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
    assert payload["source_count"] == 26
    assert payload["active_source_count"] == 26
    assert {item["source"] for item in payload["sources"]} >= {
        "slack",
        "github",
        "gmail",
        "aws",
        "quickbooks",
    }
    assert "aws-secretsmanager:/fyralis/sources" not in output
    assert (tmp_path / "sources" / "slack" / "connection.json").is_file()
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
    assert payload["source_count"] == 26
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
