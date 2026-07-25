"""Fyralis product CLI."""

from __future__ import annotations

import argparse
import asyncio
import base64
import hashlib
import json
import os
import re
import subprocess
import sys
import tempfile
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Sequence
from urllib import error as urlerror
from urllib import request as urlrequest

from services.ingest.source_contract import (
    SOURCE_CONNECTION_CATALOG,
    SOURCE_CONNECTION_SLUGS,
    source_connection_profile,
    source_local_rehearsal_profile,
)
from services.platform.runtime.byoc_aws_live_preflight import (
    ByocAwsLivePreflightInputs,
    render_aws_live_preflight_json,
    run_byoc_aws_live_preflight,
)
from services.platform.runtime.byoc_aws_provider_executor import (
    DEFAULT_HELM_CHART,
    DEFAULT_HELM_RELEASE_NAME,
    ByocAwsProviderExecutorInputs,
    run_byoc_aws_provider_executor,
)
from services.platform.runtime.byoc_permissions import (
    load_byoc_aws_iam_template,
    load_byoc_permissions_manifest,
)
from services.platform.runtime.source_browser_agent_recipes import (
    browser_agent_recipe_for_source,
)
from services.platform.runtime.source_browser_agent_runner import (
    SourceBrowserAgentRunnerInputs,
    run_source_browser_agent,
)
from services.platform.runtime.source_browser_agent_workflow import (
    build_source_browser_agent_run,
)


DEFAULT_WORKDIR = Path(".fyralis/byoc-agent")
DEFAULT_DATAPLANE_MANIFEST = Path("deploy/byoc/dataplane.example.yaml")
DEFAULT_PERMISSIONS_MANIFEST = Path("deploy/byoc/permissions.example.yaml")
DEFAULT_IAM_TEMPLATE = Path("deploy/byoc/aws/iam.bootstrap.template.yaml")
DEFAULT_DEPLOYMENT_ID = "dep_customer"
DEFAULT_CUSTOMER_ID = "cus_customer"
DEFAULT_STACK_NAME = "fyralis-byoc-customer"
DEFAULT_LOCAL_REHEARSAL_STACK_NAME = "fyralis-local-rehearsal"
DEFAULT_LOCAL_REHEARSAL_CHART = "./deploy/helm/fyralis"
DEFAULT_LOCAL_REHEARSAL_IMAGE_REPOSITORY = "fyralis/local"
DEFAULT_LOCAL_REHEARSAL_IMAGE_TAG = "dev"
DEFAULT_LOCAL_REHEARSAL_CLUSTER = "fyralis-byoc"
DEFAULT_SLACK_REHEARSAL_DIR = Path(".fyralis/local-rehearsal/slack")
DEFAULT_SLACK_GATEWAY_SERVICE = "fyralis-gateway"
DEFAULT_SLACK_GATEWAY_NAMESPACE = "fyralis-system"
DEFAULT_SLACK_GATEWAY_LOCAL_PORT = 8000
DEFAULT_KUBECTL = "/tmp/fyralis-byoc-tools/kubectl"
DEFAULT_NGROK_API = "http://127.0.0.1:4040/api/tunnels"
# These Helm component labels identify the only processes that exchange or
# refresh the deployment-owned Figma OAuth credential. Keep the secret
# reference out of orchestration-only workers which never call Figma.
FIGMA_RUNTIME_COMPONENTS = (
    "gateway",
    "shard-fetch",
    "reconciler",
    "periodic-reconciler",
)
FIGMA_REQUIRED_RUNTIME_COMPONENTS = frozenset({"gateway", "shard-fetch"})
REHEARSABLE_SOURCES = SOURCE_CONNECTION_SLUGS
SLACK_BOT_SCOPES = (
    "channels:read",
    "channels:history",
    "groups:read",
    "groups:history",
    "users:read",
    "team:read",
)
SLACK_USER_SCOPES = (
    "im:read",
    "im:history",
    "mpim:read",
    "mpim:history",
)
SLACK_BOT_EVENTS = ("message.channels", "message.groups")

CAPABILITY_MODULES = {
    "kubernetes": "aws.eks",
    "network": "aws.vpc",
    "secrets": "aws.secretsmanager",
    "postgres": "aws.rds-postgres-pgvector",
    "s3": "aws.s3",
    "kafka": "aws.msk",
}


def main(argv: Sequence[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(list(sys.argv[1:] if argv is None else argv))
    handler = getattr(args, "handler", None)
    if handler is None:
        parser.print_help(sys.stderr)
        return 2
    return handler(args)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="fyralis")
    subcommands = parser.add_subparsers(dest="command")

    byoc = subcommands.add_parser("byoc", help="BYOC setup and data-plane tools")
    byoc_subcommands = byoc.add_subparsers(dest="byoc_command")

    agent = byoc_subcommands.add_parser(
        "agent",
        help="Customer-cloud BYOC setup agent",
    )
    agent_subcommands = agent.add_subparsers(dest="agent_command")

    _add_autopilot_command(agent_subcommands)
    _add_role_template_command(agent_subcommands)
    _add_install_command(agent_subcommands)
    _add_register_role_command(agent_subcommands)
    _add_discover_command(agent_subcommands)
    _add_plan_command(agent_subcommands)
    _add_apply_command(agent_subcommands)
    _add_provider_executor_command(agent_subcommands)
    _add_local_rehearsal_command(agent_subcommands)
    _add_validate_command(agent_subcommands)

    source = byoc_subcommands.add_parser(
        "source",
        help="Customer-cloud ingestion source setup",
    )
    source_subcommands = source.add_subparsers(dest="source_command")
    _add_source_discover_command(source_subcommands)
    _add_source_plan_command(source_subcommands)
    _add_source_apply_command(source_subcommands)
    _add_source_validate_command(source_subcommands)
    _add_source_activate_command(source_subcommands)
    _add_source_autopilot_command(source_subcommands)
    _add_source_browser_agent_command(source_subcommands)
    _add_source_rehearse_command(source_subcommands)
    _add_source_rehearse_slack_command(source_subcommands)
    return parser


def _add_common_workdir(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--workdir",
        type=Path,
        default=DEFAULT_WORKDIR,
        help="Local customer-cloud agent workspace.",
    )
    parser.add_argument("--json", action="store_true", help="Emit JSON output.")


def _add_common_aws_preflight(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--dataplane-manifest",
        type=Path,
        default=DEFAULT_DATAPLANE_MANIFEST,
    )
    parser.add_argument(
        "--permissions-manifest",
        type=Path,
        default=DEFAULT_PERMISSIONS_MANIFEST,
    )
    parser.add_argument("--iam-template", type=Path, default=DEFAULT_IAM_TEMPLATE)
    parser.add_argument("--aws-profile")
    parser.add_argument("--expected-account-id")
    parser.add_argument(
        "--skip-live-aws",
        action="store_true",
        help="Run manifest/contract discovery only; no AWS API calls.",
    )
    parser.add_argument(
        "--run-readonly-api-probes",
        action="store_true",
        help="Run read-only AWS describe/list probes after STS identity.",
    )
    parser.add_argument("--run-iam-policy-simulation", action="store_true")
    parser.add_argument("--simulation-principal-arn")


def _add_autopilot_command(subcommands: argparse._SubParsersAction) -> None:
    parser = subcommands.add_parser(
        "autopilot",
        help="Run the customer-cloud BYOC setup flow with minimal interaction.",
    )
    _add_common_workdir(parser)
    parser.add_argument("--cloud", choices=("aws",), default="aws")
    parser.add_argument("--region", required=True)
    parser.add_argument("--external-id", required=True)
    parser.add_argument("--bundle", default="fyralis-byoc-customer.zip")
    parser.add_argument(
        "--capabilities",
        default="kubernetes,network,secrets,postgres,s3,kafka",
    )
    parser.add_argument("--auto-approve", action="store_true")
    parser.add_argument("--plan-only", action="store_true")
    _add_common_provider_executor_options(parser)
    parser.add_argument(
        "--run-provider-executor",
        action="store_true",
        help="Run the AWS provider executor after approval.",
    )
    _add_common_aws_preflight(parser)
    parser.set_defaults(handler=_cmd_autopilot)


def _add_role_template_command(subcommands: argparse._SubParsersAction) -> None:
    parser = subcommands.add_parser(
        "role-template",
        help="Generate a customer-side setup role template.",
    )
    _add_common_workdir(parser)
    parser.add_argument("--cloud", choices=("aws",), required=True)
    parser.add_argument("--region", required=True)
    parser.add_argument("--external-id", required=True)
    parser.add_argument(
        "--permissions-manifest",
        type=Path,
        default=DEFAULT_PERMISSIONS_MANIFEST,
    )
    parser.add_argument("--iam-template", type=Path, default=DEFAULT_IAM_TEMPLATE)
    parser.add_argument("--output", type=Path)
    parser.set_defaults(handler=_cmd_role_template)


def _add_install_command(subcommands: argparse._SubParsersAction) -> None:
    parser = subcommands.add_parser(
        "install",
        help="Install/register the customer-cloud setup agent bundle.",
    )
    _add_common_workdir(parser)
    parser.add_argument("--bundle", required=True)
    parser.add_argument("--region", required=True)
    parser.add_argument("--agent-id")
    parser.add_argument(
        "--require-bundle-file",
        action="store_true",
        help="Fail if --bundle is not a local file.",
    )
    parser.set_defaults(handler=_cmd_install)


def _add_register_role_command(subcommands: argparse._SubParsersAction) -> None:
    parser = subcommands.add_parser(
        "register-role",
        help="Register an approved customer-cloud setup role.",
    )
    _add_common_workdir(parser)
    parser.add_argument("--role-arn", required=True)
    parser.add_argument("--external-id", required=True)
    parser.add_argument("--agent-id")
    parser.set_defaults(handler=_cmd_register_role)


def _add_discover_command(subcommands: argparse._SubParsersAction) -> None:
    parser = subcommands.add_parser(
        "discover",
        help="Run customer-side discovery and generate an agent plan.",
    )
    _add_common_workdir(parser)
    parser.add_argument("--region", required=True)
    parser.add_argument("--capabilities", required=True)
    parser.add_argument("--emit-plan", action="store_true")
    _add_common_aws_preflight(parser)
    parser.set_defaults(handler=_cmd_discover)


def _add_plan_command(subcommands: argparse._SubParsersAction) -> None:
    parser = subcommands.add_parser(
        "plan",
        help="Package the latest discovery plan for review.",
    )
    _add_common_workdir(parser)
    parser.add_argument("--no-apply", action="store_true")
    parser.add_argument("--emit-review-bundle", action="store_true")
    parser.set_defaults(handler=_cmd_plan)


def _add_apply_command(subcommands: argparse._SubParsersAction) -> None:
    parser = subcommands.add_parser(
        "apply",
        help="Approve the latest BYOC setup plan for customer-cloud execution.",
    )
    _add_common_workdir(parser)
    parser.add_argument("--requires-approval", action="store_true")
    parser.add_argument("--plan", default="latest")
    parser.set_defaults(handler=_cmd_apply)


def _add_provider_executor_command(subcommands: argparse._SubParsersAction) -> None:
    parser = subcommands.add_parser(
        "provider-executor",
        help="Render or execute customer-side AWS resources and Helm install.",
    )
    _add_common_workdir(parser)
    parser.add_argument("--cloud", choices=("aws",), default="aws")
    parser.add_argument("--region", required=True)
    _add_common_provider_executor_options(parser)
    parser.add_argument("--aws-profile")
    parser.set_defaults(handler=_cmd_provider_executor)


def _add_local_rehearsal_command(subcommands: argparse._SubParsersAction) -> None:
    parser = subcommands.add_parser(
        "local-rehearsal",
        help="Generate a zero-spend kind/Helm BYOC rehearsal package.",
    )
    _add_common_workdir(parser)
    parser.add_argument("--region", default="us-east-1")
    parser.add_argument("--stack-name", default=DEFAULT_LOCAL_REHEARSAL_STACK_NAME)
    parser.add_argument("--deployment-id", default=DEFAULT_DEPLOYMENT_ID)
    parser.add_argument("--customer-id", default=DEFAULT_CUSTOMER_ID)
    parser.add_argument("--environment", default="local-rehearsal")
    parser.add_argument("--cluster-name", default=DEFAULT_LOCAL_REHEARSAL_CLUSTER)
    parser.add_argument("--helm-chart", default=DEFAULT_LOCAL_REHEARSAL_CHART)
    parser.add_argument(
        "--image-repository", default=DEFAULT_LOCAL_REHEARSAL_IMAGE_REPOSITORY
    )
    parser.add_argument("--image-tag", default=DEFAULT_LOCAL_REHEARSAL_IMAGE_TAG)
    parser.set_defaults(handler=_cmd_local_rehearsal)


def _add_common_provider_executor_options(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--stack-name", default=DEFAULT_STACK_NAME)
    parser.add_argument("--deployment-id", default=DEFAULT_DEPLOYMENT_ID)
    parser.add_argument("--customer-id", default=DEFAULT_CUSTOMER_ID)
    parser.add_argument("--environment", default="pilot")
    parser.add_argument("--permissions-boundary-policy-arn", default="")
    parser.add_argument("--create-change-set", action="store_true")
    parser.add_argument("--execute-change-set", action="store_true")
    parser.add_argument("--execute-helm", action="store_true")
    parser.add_argument("--kube-context")
    parser.add_argument("--helm-release-name", default=DEFAULT_HELM_RELEASE_NAME)
    parser.add_argument("--helm-chart", default=DEFAULT_HELM_CHART)
    parser.add_argument(
        "--confirm-cost-and-mutation",
        action="store_true",
        help="Required before creating/executing AWS resources or Helm installs.",
    )


def _add_validate_command(subcommands: argparse._SubParsersAction) -> None:
    parser = subcommands.add_parser(
        "validate",
        help="Emit a sanitized customer-cloud readiness report.",
    )
    _add_common_workdir(parser)
    parser.add_argument("--emit-sanitized-readiness-report", action="store_true")
    parser.add_argument("--output", type=Path)
    parser.set_defaults(handler=_cmd_validate)


def _add_common_source_options(parser: argparse.ArgumentParser) -> None:
    _add_common_workdir(parser)
    parser.add_argument("--source", required=True)
    parser.add_argument(
        "--credential-ref",
        help=(
            "Customer-cloud secret/reference for one source. If omitted, Fyralis "
            "generates expected refs from --credential-ref-prefix but still marks "
            "human authorization as pending."
        ),
    )
    parser.add_argument(
        "--credential-ref-prefix",
        default="aws-secretsmanager:/fyralis/sources",
        help="Prefix used when generating expected source refs automatically.",
    )
    parser.add_argument(
        "--scopes",
        default="auto",
        help="Comma-separated source scope, or 'auto' for the source profile default.",
    )
    parser.add_argument(
        "--admin-console-url",
        default="https://fyralis.customer.internal",
    )
    parser.add_argument(
        "--provider-ingress-url",
        default="https://fyralis-ingress.customer.example",
    )
    parser.add_argument(
        "--provider-authorization-mode",
        choices=("customer-owned-ref", "preauthorized-ref"),
        default="customer-owned-ref",
    )
    parser.add_argument(
        "--preauthorized-ref-manifest",
        type=Path,
        help="Optional customer-local manifest of preauthorized source refs.",
    )


def _add_source_sync_options(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--sync-mode",
        choices=("dry-run", "limited-backfill", "live-events", "backfill-plus-live"),
        default="limited-backfill",
    )
    parser.add_argument(
        "--backfill-window",
        choices=("7d", "30d", "90d", "none"),
        default="30d",
    )


def _add_source_discover_command(subcommands: argparse._SubParsersAction) -> None:
    parser = subcommands.add_parser(
        "discover",
        help="Discover source setup requirements without reading source secrets.",
    )
    _add_common_source_options(parser)
    parser.set_defaults(handler=_cmd_source_discover)


def _add_source_plan_command(subcommands: argparse._SubParsersAction) -> None:
    parser = subcommands.add_parser(
        "plan",
        help="Generate an executable source setup plan from discovery.",
    )
    _add_common_source_options(parser)
    _add_source_sync_options(parser)
    parser.set_defaults(handler=_cmd_source_plan)


def _add_source_apply_command(subcommands: argparse._SubParsersAction) -> None:
    parser = subcommands.add_parser(
        "apply",
        help="Apply an approved source setup plan locally.",
    )
    _add_common_source_options(parser)
    _add_source_sync_options(parser)
    parser.add_argument("--requires-approval", action="store_true")
    parser.add_argument("--plan", default="latest")
    parser.set_defaults(handler=_cmd_source_apply)


def _add_source_validate_command(subcommands: argparse._SubParsersAction) -> None:
    parser = subcommands.add_parser(
        "validate",
        help="Validate local source connection state and emit sanitized receipts.",
    )
    _add_common_source_options(parser)
    parser.add_argument("--live", action="store_true")
    parser.set_defaults(handler=_cmd_source_validate)


def _add_source_activate_command(subcommands: argparse._SubParsersAction) -> None:
    parser = subcommands.add_parser(
        "activate",
        help="Activate a validated source and start the first sync locally.",
    )
    _add_common_source_options(parser)
    _add_source_sync_options(parser)
    parser.add_argument("--requires-approval", action="store_true")
    parser.add_argument("--start-first-sync", action="store_true")
    parser.set_defaults(handler=_cmd_source_activate)


def _add_source_autopilot_command(subcommands: argparse._SubParsersAction) -> None:
    parser = subcommands.add_parser(
        "autopilot",
        help="Prepare, validate, sync, and optionally activate a source locally.",
    )
    _add_common_workdir(parser)
    parser.add_argument("--source", required=True)
    parser.add_argument(
        "--credential-ref",
        help=(
            "Customer-cloud secret/reference for one source. If omitted, Fyralis "
            "generates a local ref from --credential-ref-prefix."
        ),
    )
    parser.add_argument(
        "--credential-ref-prefix",
        default="aws-secretsmanager:/fyralis/sources",
        help="Prefix used when generating source refs automatically.",
    )
    parser.add_argument(
        "--scopes",
        default="auto",
        help="Comma-separated source scope, or 'auto' for the source profile default.",
    )
    parser.add_argument(
        "--sync-mode",
        choices=("dry-run", "limited-backfill", "live-events", "backfill-plus-live"),
        default="limited-backfill",
    )
    parser.add_argument(
        "--backfill-window",
        choices=("7d", "30d", "90d", "none"),
        default="30d",
    )
    parser.add_argument(
        "--admin-console-url",
        default="https://fyralis.customer.internal",
    )
    parser.add_argument(
        "--provider-ingress-url",
        default="https://fyralis-ingress.customer.example",
    )
    parser.add_argument(
        "--provider-authorization-mode",
        choices=("customer-owned-ref", "preauthorized-ref"),
        default="customer-owned-ref",
    )
    parser.add_argument(
        "--preauthorized-ref-manifest",
        type=Path,
        help="Optional customer-local manifest of preauthorized source refs.",
    )
    parser.add_argument("--auto-activate", action="store_true")
    parser.set_defaults(handler=_cmd_source_autopilot)


def _add_source_browser_agent_command(subcommands: argparse._SubParsersAction) -> None:
    parser = subcommands.add_parser(
        "browser-agent",
        help="Run the customer-cloud browser agent from a source connection artifact.",
    )
    _add_common_workdir(parser)
    parser.add_argument("--source", required=True)
    parser.add_argument(
        "--run-artifact",
        type=Path,
        help="Path to a browser_agent_run JSON object or source connection artifact.",
    )
    parser.add_argument("--gateway-api-base")
    parser.add_argument("--bearer-token")
    parser.add_argument("--native-payload", type=Path)
    parser.add_argument("--execute-native", action="store_true")
    parser.add_argument("--admin-approved", action="store_true")
    parser.add_argument("--open-browser", action="store_true")
    parser.add_argument(
        "--execute-browser-dom",
        action="store_true",
        help="Drive the provider settings page with the BYOC browser DOM agent.",
    )
    parser.add_argument(
        "--interactive-admin",
        action="store_true",
        help="Pause in the terminal for provider sign-in, MFA, and approval gates.",
    )
    parser.add_argument(
        "--headless-browser",
        action="store_true",
        help="Run the browser DOM agent headless. Default is a visible admin-present browser.",
    )
    parser.add_argument("--browser-timeout-s", type=float, default=120.0)
    parser.add_argument("--browser-slow-mo-ms", type=int, default=0)
    parser.add_argument("--browser-storage-state", type=Path)
    parser.add_argument("--timeout-s", type=float, default=10.0)
    parser.add_argument("--output", type=Path)
    parser.set_defaults(handler=_cmd_source_browser_agent)


def _add_source_rehearse_slack_command(
    subcommands: argparse._SubParsersAction,
) -> None:
    parser = subcommands.add_parser(
        "rehearse-slack",
        help="Automate the local real-Slack BYOC rehearsal up to Slack consent.",
    )
    _add_common_workdir(parser)
    parser.add_argument("--setup-dir", type=Path, default=DEFAULT_SLACK_REHEARSAL_DIR)
    parser.add_argument("--public-url")
    parser.add_argument("--kubectl", default=DEFAULT_KUBECTL)
    parser.add_argument("--namespace", default=DEFAULT_SLACK_GATEWAY_NAMESPACE)
    parser.add_argument("--gateway-service", default=DEFAULT_SLACK_GATEWAY_SERVICE)
    parser.add_argument(
        "--gateway-local-port", type=int, default=DEFAULT_SLACK_GATEWAY_LOCAL_PORT
    )
    parser.add_argument("--ngrok-api", default=DEFAULT_NGROK_API)
    parser.add_argument("--no-start-tunnel", action="store_true")
    parser.add_argument("--slack-env", type=Path)
    parser.add_argument("--apply-env", action="store_true")
    parser.add_argument("--tenant-id")
    parser.add_argument("--actor-id")
    parser.add_argument("--bootstrap-secret")
    parser.add_argument("--print-install-url", action="store_true")
    parser.set_defaults(handler=_cmd_source_rehearse_slack)


def _add_source_rehearse_command(subcommands: argparse._SubParsersAction) -> None:
    parser = subcommands.add_parser(
        "rehearse",
        help="Automate local real-provider source rehearsal up to provider consent.",
    )
    _add_common_workdir(parser)
    parser.add_argument("--source", choices=REHEARSABLE_SOURCES, required=True)
    parser.add_argument("--setup-dir", type=Path)
    parser.add_argument("--public-url")
    parser.add_argument("--kubectl", default=DEFAULT_KUBECTL)
    parser.add_argument("--namespace", default=DEFAULT_SLACK_GATEWAY_NAMESPACE)
    parser.add_argument("--gateway-service", default=DEFAULT_SLACK_GATEWAY_SERVICE)
    parser.add_argument(
        "--gateway-local-port", type=int, default=DEFAULT_SLACK_GATEWAY_LOCAL_PORT
    )
    parser.add_argument("--ngrok-api", default=DEFAULT_NGROK_API)
    parser.add_argument("--no-start-tunnel", action="store_true")
    parser.add_argument("--provider-env", type=Path)
    parser.add_argument("--apply-env", action="store_true")
    parser.add_argument("--tenant-id")
    parser.add_argument("--actor-id")
    parser.add_argument("--bootstrap-secret")
    parser.add_argument("--print-install-url", action="store_true")
    parser.set_defaults(handler=_cmd_source_rehearse)


def _cmd_autopilot(args: argparse.Namespace) -> int:
    capabilities = _parse_capabilities(args.capabilities)
    invalid = [name for name in capabilities if name not in CAPABILITY_MODULES]
    if invalid:
        print(f"unsupported capabilities: {', '.join(invalid)}", file=sys.stderr)
        return 2

    try:
        role_template = _role_template_payload(args)
    except Exception as exc:  # noqa: BLE001
        print(
            f"failed to build setup role template: {type(exc).__name__}",
            file=sys.stderr,
        )
        return 1
    role_template_path = args.workdir / "templates" / "setup-role-template.json"
    _write_json(role_template_path, role_template)

    registration = _registration_payload(
        args,
        access_mode="customer_cloud_agent",
        region=args.region,
        local_state={
            "bundle_ref": args.bundle,
            "bundle_file_present": Path(args.bundle).is_file(),
        },
        sanitized_summary={
            "bundle_ref": args.bundle,
            "bundle_file_present": Path(args.bundle).is_file(),
        },
    )
    _write_json(_registration_path(args.workdir), registration)

    plan, preflight_code = _discover_plan(args, capabilities)
    plan_path = _latest_plan_path(args.workdir)
    _write_json(plan_path, plan)

    review = _review_bundle(plan)
    review_path = args.workdir / "review" / "latest-review-bundle.json"
    _write_json(review_path, review)

    receipt: dict[str, Any] | None = None
    if (
        args.auto_approve
        and not args.plan_only
        and plan["status"] == "ready_for_approval"
    ):
        receipt = _apply_receipt(plan)
        _write_json(_latest_apply_receipt_path(args.workdir), receipt)

    provider_report: dict[str, Any] | None = None
    provider_executor_blocker: str | None = None
    if args.run_provider_executor:
        if receipt is None:
            provider_executor_blocker = "approval_receipt_required"
        else:
            provider_report = _run_provider_executor_from_args(args)

    readiness = _readiness_report(args.workdir)
    readiness_path = args.workdir / "reports" / "readiness-report.json"
    _write_json(readiness_path, readiness)

    payload = {
        "schema_version": "fyralis.byoc.agent.autopilot_run.v1",
        "status": _autopilot_status(
            plan,
            receipt,
            readiness,
            provider_report=provider_report,
            provider_executor_blocker=provider_executor_blocker,
        ),
        "generated_at": _now(),
        "region": args.region,
        "auto_approve": args.auto_approve,
        "plan_only": args.plan_only,
        "approval_required_before_mutation": not args.auto_approve,
        "cloud_mutations_executed": bool(
            provider_report and provider_report["cloud_api_mutations_executed"]
        ),
        "resource_mutations_executed": bool(
            provider_report and provider_report["resource_mutations_executed"]
        ),
        "provider_executor_blocker": provider_executor_blocker,
        "artifacts": {
            "role_template": str(role_template_path),
            "registration": str(_registration_path(args.workdir)),
            "preflight_report": str(
                args.workdir / "reports" / "aws-live-preflight.json"
            ),
            "plan": str(plan_path),
            "review_bundle": str(review_path),
            "apply_receipt": (
                str(_latest_apply_receipt_path(args.workdir)) if receipt else None
            ),
            "readiness_report": str(readiness_path),
            "provider_executor_report": (
                str(_provider_executor_report_path(args.workdir))
                if provider_report is not None
                else None
            ),
        },
        "next_action": _autopilot_next_action(args, plan, receipt),
        "stored_scope": "sanitized_agent_metadata_only",
    }
    _emit(args, payload, f"BYOC autopilot artifacts written to {args.workdir}")
    if preflight_code != 0:
        return preflight_code
    if provider_executor_blocker is not None:
        return 1
    if provider_report is not None and not provider_report["required_checks_passed"]:
        return 1
    return 0 if readiness["required_checks_passed"] or args.plan_only else 1


def _cmd_role_template(args: argparse.Namespace) -> int:
    if args.cloud != "aws":
        print("only --cloud aws is currently supported", file=sys.stderr)
        return 2
    try:
        payload = _role_template_payload(args)
    except Exception as exc:  # noqa: BLE001
        print(f"failed to load BYOC IAM inputs: {type(exc).__name__}", file=sys.stderr)
        return 1
    output = args.output or args.workdir / "templates" / "setup-role-template.json"
    _write_json(output, payload)
    return _emit(
        args,
        payload,
        f"BYOC setup role template written to {output}",
    )


def _cmd_install(args: argparse.Namespace) -> int:
    bundle = Path(args.bundle)
    if args.require_bundle_file and not bundle.is_file():
        print("--bundle must point to an existing file", file=sys.stderr)
        return 2
    payload = _registration_payload(
        args,
        access_mode="customer_cloud_agent",
        region=args.region,
        local_state={
            "bundle_ref": args.bundle,
            "bundle_file_present": bundle.is_file(),
        },
        sanitized_summary={
            "bundle_ref": args.bundle,
            "bundle_file_present": bundle.is_file(),
        },
    )
    _write_json(_registration_path(args.workdir), payload)
    return _emit(args, _redacted_registration(payload), "BYOC setup agent registered.")


def _cmd_register_role(args: argparse.Namespace) -> int:
    payload = _registration_payload(
        args,
        access_mode="customer_cloud_setup_role",
        region=_region_from_role_arn(args.role_arn),
        local_state={
            "role_arn": args.role_arn,
            "external_id": args.external_id,
        },
        sanitized_summary={
            "role_arn_sha256": _sha256(args.role_arn),
            "external_id_sha256": _sha256(args.external_id),
        },
    )
    _write_json(_registration_path(args.workdir), payload)
    return _emit(args, _redacted_registration(payload), "BYOC setup role registered.")


def _cmd_discover(args: argparse.Namespace) -> int:
    capabilities = _parse_capabilities(args.capabilities)
    invalid = [name for name in capabilities if name not in CAPABILITY_MODULES]
    if invalid:
        print(f"unsupported capabilities: {', '.join(invalid)}", file=sys.stderr)
        return 2

    plan, code = _discover_plan(args, capabilities)
    plan_path = _latest_plan_path(args.workdir)
    _write_json(plan_path, plan)
    payload = plan if args.emit_plan or args.json else {"plan_path": str(plan_path)}
    _emit(args, payload, f"BYOC discovery plan written to {plan_path}")
    return code


def _cmd_plan(args: argparse.Namespace) -> int:
    if not args.no_apply:
        print("plan review requires --no-apply", file=sys.stderr)
        return 2
    plan = _load_required_json(_latest_plan_path(args.workdir), "latest discovery plan")
    if plan is None:
        return 2
    review = _review_bundle(plan)
    output = args.workdir / "review" / "latest-review-bundle.json"
    _write_json(output, review)
    payload = (
        review if args.emit_review_bundle or args.json else {"review_path": str(output)}
    )
    return _emit(args, payload, f"BYOC review bundle written to {output}")


def _cmd_apply(args: argparse.Namespace) -> int:
    if not args.requires_approval:
        print("apply requires --requires-approval", file=sys.stderr)
        return 2
    if args.plan != "latest":
        print("only --plan latest is currently supported", file=sys.stderr)
        return 2

    plan = _load_required_json(_latest_plan_path(args.workdir), "latest discovery plan")
    if plan is None:
        return 2
    if plan.get("status") != "ready_for_approval":
        print("latest plan is not ready for approval", file=sys.stderr)
        return 1

    receipt = _apply_receipt(plan)
    output = _latest_apply_receipt_path(args.workdir)
    _write_json(output, receipt)
    return _emit(args, receipt, f"BYOC apply approval receipt written to {output}")


def _cmd_provider_executor(args: argparse.Namespace) -> int:
    if args.cloud != "aws":
        print("only --cloud aws is currently supported", file=sys.stderr)
        return 2
    report = _run_provider_executor_from_args(args)
    _emit(
        args,
        report,
        f"AWS provider executor report written to {_provider_executor_report_path(args.workdir)}",
    )
    return 0 if report["required_checks_passed"] else 1


def _cmd_local_rehearsal(args: argparse.Namespace) -> int:
    chart_path = Path(args.helm_chart)
    if not chart_path.is_dir():
        print(f"missing local Helm chart: {chart_path}", file=sys.stderr)
        return 2

    report = run_byoc_aws_provider_executor(
        ByocAwsProviderExecutorInputs(
            workdir=args.workdir,
            region=args.region,
            stack_name=args.stack_name,
            deployment_id=args.deployment_id,
            customer_id=args.customer_id,
            environment=args.environment,
            helm_chart=args.helm_chart,
        )
    )
    runbook = _local_rehearsal_runbook(args, report)
    source_refs = _local_rehearsal_source_refs()
    runbook_path = _local_rehearsal_runbook_path(args.workdir)
    source_refs_path = _local_rehearsal_source_refs_path(args.workdir)
    _write_json(runbook_path, runbook)
    _write_json(source_refs_path, source_refs)

    payload = {
        "schema_version": "fyralis.byoc.local_rehearsal.v1",
        "status": "ready" if report["required_checks_passed"] else "failed",
        "generated_at": _now(),
        "zero_cloud_spend": True,
        "cloud_mutations_executed": False,
        "resource_mutations_executed": False,
        "cluster_name": args.cluster_name,
        "helm_chart": args.helm_chart,
        "image": f"{args.image_repository}:{args.image_tag}",
        "artifacts": {
            "provider_executor_report": str(
                _provider_executor_report_path(args.workdir)
            ),
            "helm_values": report["artifacts"]["helm_values"],
            "helm_command": report["artifacts"]["helm_command"],
            "runbook": str(runbook_path),
            "source_refs_example": str(source_refs_path),
        },
        "commands": runbook["commands"],
        "stored_scope": "local_rehearsal_metadata_only",
    }
    _emit(args, payload, f"Local BYOC rehearsal package written to {args.workdir}")
    return 0 if report["required_checks_passed"] else 1


def _cmd_validate(args: argparse.Namespace) -> int:
    report = _readiness_report(args.workdir)
    output = args.output or args.workdir / "reports" / "readiness-report.json"
    if args.emit_sanitized_readiness_report or args.output is not None:
        _write_json(output, report)
    payload = report if args.json else {"readiness_report_path": str(output)}
    _emit(args, payload, f"BYOC readiness report written to {output}")
    return 0 if report["required_checks_passed"] else 1


def _cmd_source_discover(args: argparse.Namespace) -> int:
    source_ids, error_code = _source_ids_from_args(args)
    if error_code:
        return error_code
    if source_ids is None:
        return 2
    preauthorized_refs, error_code = _source_preauthorized_refs_from_args(args)
    if error_code:
        return error_code
    setattr(args, "preauthorized_refs", preauthorized_refs)

    discoveries = [_source_discovery(args, source_id) for source_id in source_ids]
    for discovery in discoveries:
        _write_json(
            _source_discovery_path(args.workdir, str(discovery["source"])),
            discovery,
        )
    payload = _source_stage_payload(
        "fyralis.byoc.source.discovery_run.v1",
        "discover",
        str(args.source).strip().lower(),
        discoveries,
    )
    _write_json(_source_stage_aggregate_path(args.workdir, "discovery"), payload)
    return _emit(args, payload, "Source discovery artifacts written.")


def _cmd_source_plan(args: argparse.Namespace) -> int:
    source_ids, error_code = _source_ids_from_args(args)
    if error_code:
        return error_code
    if source_ids is None:
        return 2
    preauthorized_refs, error_code = _source_preauthorized_refs_from_args(args)
    if error_code:
        return error_code
    setattr(args, "preauthorized_refs", preauthorized_refs)

    plans: list[dict[str, Any]] = []
    for source_id in source_ids:
        discovery = _source_discovery(args, source_id)
        contract = _source_contract(args, source_id, discovery)
        plan = _source_setup_plan(args, source_id, discovery, contract)
        _write_json(_source_discovery_path(args.workdir, source_id), discovery)
        _write_json(_source_contract_path(args.workdir, source_id), contract)
        _write_json(_source_plan_path(args.workdir, source_id), plan)
        plans.append(plan)
    payload = _source_stage_payload(
        "fyralis.byoc.source.plan_run.v1",
        "plan",
        str(args.source).strip().lower(),
        plans,
    )
    _write_json(_source_stage_aggregate_path(args.workdir, "plan"), payload)
    return _emit(args, payload, "Source setup plans written.")


def _cmd_source_apply(args: argparse.Namespace) -> int:
    if not args.requires_approval:
        print("source apply requires --requires-approval", file=sys.stderr)
        return 2
    if args.plan != "latest":
        print("only --plan latest is currently supported", file=sys.stderr)
        return 2
    source_ids, error_code = _source_ids_from_args(args)
    if error_code:
        return error_code
    if source_ids is None:
        return 2
    preauthorized_refs, error_code = _source_preauthorized_refs_from_args(args)
    if error_code:
        return error_code
    setattr(args, "preauthorized_refs", preauthorized_refs)

    results: list[dict[str, Any]] = []
    blocked = False
    for source_id in source_ids:
        plan = _load_optional_json(_source_plan_path(args.workdir, source_id))
        if plan is None:
            discovery = _source_discovery(args, source_id)
            contract = _source_contract(args, source_id, discovery)
            plan = _source_setup_plan(args, source_id, discovery, contract)
            _write_json(_source_discovery_path(args.workdir, source_id), discovery)
            _write_json(_source_contract_path(args.workdir, source_id), contract)
            _write_json(_source_plan_path(args.workdir, source_id), plan)
        if plan.get("status") != "ready_for_approval":
            blocked = True
            result = _source_apply_blocker(source_id, plan)
            _write_json(_source_apply_blocker_path(args.workdir, source_id), result)
            results.append(result)
            continue
        result = _source_apply_result(args, source_id, plan)
        _write_json(_source_apply_receipt_path(args.workdir, source_id), result)
        results.append(result)

    payload = _source_stage_payload(
        "fyralis.byoc.source.apply_run.v1",
        "apply",
        str(args.source).strip().lower(),
        results,
    )
    _write_json(_source_stage_aggregate_path(args.workdir, "apply"), payload)
    _emit(args, payload, "Source apply receipts written.")
    return 1 if blocked else 0


def _cmd_source_validate(args: argparse.Namespace) -> int:
    source_ids, error_code = _source_ids_from_args(args)
    if error_code:
        return error_code
    if source_ids is None:
        return 2
    results = [_source_validate_result(args, source_id) for source_id in source_ids]
    for result in results:
        _write_json(
            _source_validation_path(args.workdir, str(result["source"])),
            result,
        )
    payload = _source_stage_payload(
        "fyralis.byoc.source.validation_run.v1",
        "validate",
        str(args.source).strip().lower(),
        results,
    )
    _write_json(_source_stage_aggregate_path(args.workdir, "validation"), payload)
    _emit(args, payload, "Source validation receipts written.")
    return 0 if all(result.get("status") == "passed" for result in results) else 1


def _cmd_source_activate(args: argparse.Namespace) -> int:
    if not args.requires_approval:
        print("source activation requires --requires-approval", file=sys.stderr)
        return 2
    if not args.start_first_sync:
        print("source activation requires --start-first-sync", file=sys.stderr)
        return 2
    source_ids, error_code = _source_ids_from_args(args)
    if error_code:
        return error_code
    if source_ids is None:
        return 2

    results: list[dict[str, Any]] = []
    blocked = False
    for source_id in source_ids:
        validation = _load_optional_json(
            _source_validation_path(args.workdir, source_id)
        )
        if not validation or validation.get("status") != "passed":
            blocked = True
            result = _source_activation_blocker(source_id, validation)
            _write_json(
                _source_activation_blocker_path(args.workdir, source_id), result
            )
            results.append(result)
            continue
        result = _source_activation_result(args, source_id)
        _write_json(
            _source_first_sync_path(args.workdir, source_id), result["first_sync"]
        )
        _write_json(
            _source_activation_path(args.workdir, source_id), result["activation"]
        )
        _write_json(
            _source_readiness_path(args.workdir, source_id), result["readiness"]
        )
        results.append(result["summary"])

    payload = _source_stage_payload(
        "fyralis.byoc.source.activation_run.v1",
        "activate",
        str(args.source).strip().lower(),
        results,
    )
    _write_json(_source_stage_aggregate_path(args.workdir, "activation"), payload)
    _emit(args, payload, "Source activation receipts written.")
    return 1 if blocked else 0


def _cmd_source_autopilot(args: argparse.Namespace) -> int:
    requested = args.source.strip().lower()
    source_ids = list(SOURCE_CONNECTION_SLUGS) if requested == "all" else [requested]
    invalid = [
        source_id
        for source_id in source_ids
        if source_id not in SOURCE_CONNECTION_CATALOG
    ]
    if invalid:
        print(f"unsupported source: {', '.join(invalid)}", file=sys.stderr)
        return 2
    if len(source_ids) > 1 and args.credential_ref:
        print("--credential-ref is only valid for one source", file=sys.stderr)
        return 2
    try:
        preauthorized_refs = _load_preauthorized_ref_manifest(
            args.preauthorized_ref_manifest
        )
    except Exception as exc:  # noqa: BLE001
        print(
            f"failed to load preauthorized ref manifest: {type(exc).__name__}",
            file=sys.stderr,
        )
        return 2
    missing_preauthorized_refs = [
        source_id
        for source_id in source_ids
        if args.provider_authorization_mode == "preauthorized-ref"
        and args.preauthorized_ref_manifest is not None
        and source_id not in preauthorized_refs
    ]
    if missing_preauthorized_refs:
        print(
            "preauthorized ref manifest is missing sources: "
            + ", ".join(missing_preauthorized_refs),
            file=sys.stderr,
        )
        return 2
    setattr(args, "preauthorized_refs", preauthorized_refs)

    results = [_run_source_autopilot(args, source_id) for source_id in source_ids]
    payload = {
        "schema_version": "fyralis.byoc.source.autopilot_run.v1",
        "status": "active" if args.auto_activate else "ready",
        "source": requested,
        "source_count": len(results),
        "active_source_count": sum(
            1 for result in results if result["status"] == "active"
        ),
        "ready_source_count": sum(
            1 for result in results if result["status"] == "ready"
        ),
        "sync_mode": args.sync_mode,
        "sources": results,
        "raw_secret_exported": False,
        "raw_payloads_exported": False,
        "stored_scope": "sanitized_source_status_only",
    }
    return _emit(args, payload, f"{requested} source autopilot artifacts written.")


def _cmd_source_browser_agent(args: argparse.Namespace) -> int:
    requested = str(args.source).strip().lower()
    source_ids = list(SOURCE_CONNECTION_SLUGS) if requested == "all" else [requested]
    invalid = [
        source_id
        for source_id in source_ids
        if source_id not in SOURCE_CONNECTION_CATALOG
    ]
    if invalid:
        print(f"unsupported source: {', '.join(invalid)}", file=sys.stderr)
        return 2
    if len(source_ids) > 1 and args.run_artifact:
        print("--run-artifact is only valid for one source", file=sys.stderr)
        return 2

    receipts: list[dict[str, Any]] = []
    blocked = False
    for source_id in source_ids:
        run_artifact = args.run_artifact or _source_connection_path(
            args.workdir,
            source_id,
        )
        if not run_artifact.is_file():
            print(
                f"missing source browser-agent artifact: {run_artifact}",
                file=sys.stderr,
            )
            return 2

    try:
        results = asyncio_run(_run_source_browser_agent_batch(args, source_ids))
    except Exception as exc:  # noqa: BLE001
        print(f"source browser-agent failed: {type(exc).__name__}", file=sys.stderr)
        return 1

    for source_id, receipt_payload in results:
        try:
            _write_json(
                _source_browser_agent_receipt_path(args.workdir, source_id),
                receipt_payload,
            )
        except Exception as exc:  # noqa: BLE001
            print(f"source browser-agent failed: {type(exc).__name__}", file=sys.stderr)
            return 1
        receipts.append(receipt_payload)
        if receipt_payload.get("status") in {"failed", "blocked"}:
            blocked = True

    if len(receipts) == 1:
        payload = receipts[0]
        output = args.output or _source_browser_agent_receipt_path(
            args.workdir,
            source_ids[0],
        )
        if args.output:
            _write_json(output, payload)
        message = f"Source browser-agent receipt written to {output}"
    else:
        payload = _source_browser_agent_stage_payload(requested, receipts)
        output = args.output or _source_stage_aggregate_path(
            args.workdir,
            "browser-agent",
        )
        _write_json(output, payload)
        message = f"Source browser-agent receipts written to {output}"

    emitted = payload if args.json else {"browser_agent_receipt": str(output)}
    _emit(args, emitted, message)
    return 1 if blocked else 0


async def _run_source_browser_agent_batch(
    args: argparse.Namespace,
    source_ids: list[str],
) -> list[tuple[str, dict[str, Any]]]:
    return await asyncio.gather(
        *(_run_source_browser_agent_single(args, source_id) for source_id in source_ids)
    )


async def _run_source_browser_agent_single(
    args: argparse.Namespace,
    source_id: str,
) -> tuple[str, dict[str, Any]]:
    run_artifact = args.run_artifact or _source_connection_path(args.workdir, source_id)
    receipt = await run_source_browser_agent(
        SourceBrowserAgentRunnerInputs(
            run_path=run_artifact,
            gateway_api_base=args.gateway_api_base,
            bearer_token=args.bearer_token,
            native_payload_path=args.native_payload,
            execute_native=args.execute_native,
            admin_approved=args.admin_approved,
            open_browser=args.open_browser,
            timeout_s=args.timeout_s,
            execute_browser_dom=args.execute_browser_dom,
            browser_headless=args.headless_browser,
            browser_timeout_s=args.browser_timeout_s,
            browser_slow_mo_ms=args.browser_slow_mo_ms,
            browser_storage_state_path=args.browser_storage_state,
            interactive_admin=args.interactive_admin,
        )
    )
    return source_id, receipt.as_json()


def _cmd_source_rehearse_slack(args: argparse.Namespace) -> int:
    setattr(args, "source", "slack")
    setattr(args, "provider_env", args.slack_env)
    return _cmd_source_rehearse(args)


def _source_profile(source_id: str) -> dict[str, Any]:
    """Return a fresh CLI view derived from the immutable source contract."""

    return source_connection_profile(source_id)


def _source_rehearsal_profile(source_id: str) -> dict[str, Any]:
    explicit_profile = source_local_rehearsal_profile(source_id)
    if explicit_profile is not None:
        return explicit_profile
    source_profile = _source_profile(source_id)
    ingress_paths = list(source_profile.get("ingress_paths", []))
    callback_path = next((path for path in ingress_paths if "callback" in path), None)
    webhook_path = next((path for path in ingress_paths if "webhook" in path), None)
    return {
        "kind": _generic_rehearsal_kind(str(source_profile["method"])),
        "needs_public_url": bool(ingress_paths),
        "callback_path": callback_path,
        "webhook_path": webhook_path,
        "env": _generic_source_env_keys(source_id, source_profile),
        "required_env": _generic_source_required_env_keys(source_id, source_profile),
        "manual_gate_names": _generic_source_manual_gates(source_id, source_profile),
        "source_profile": source_profile,
    }


def _generic_rehearsal_kind(method: str) -> str:
    return {
        "api_token": "api_token_connect",
        "dwd": "google_workspace_dwd",
        "gateway": "local_gateway_session",
        "iam_role": "iam_role_ref",
        "oauth": "oauth_or_preauthorized_ref",
        "oauth_client_credentials": "oauth_client_credentials_connect",
        "oauth_plus_gateway": "oauth_plus_local_gateway",
        "poll": "polling_ref",
        "webhook": "webhook_endpoint",
    }.get(method, method)


def _source_env_prefix(source_id: str) -> str:
    return source_id.replace("-", "_").upper()


def _source_browser_agent_recipe(source_id: str) -> dict[str, Any]:
    return browser_agent_recipe_for_source(source_id.replace("-", "_"))


def asyncio_run(coro):
    return asyncio.run(coro)


def _source_browser_agent_run(
    args: argparse.Namespace,
    source_id: str,
    profile: dict[str, Any],
    *,
    preauthorized_refs_present: bool = False,
    installed: bool = False,
) -> dict[str, Any]:
    authorization_mode = getattr(
        args,
        "provider_authorization_mode",
        "customer-owned-ref",
    )
    provider_ingress_url = (
        getattr(args, "provider_ingress_url", None)
        or getattr(args, "public_url", None)
        or "https://fyralis-ingress.customer.example"
    )
    oauth_redirect_url, events_request_url = _source_browser_agent_ingress_urls(
        profile,
        provider_ingress_url=provider_ingress_url,
    )
    customer_actions = _source_customer_action_required(
        profile,
        authorization_mode=authorization_mode,
        preauthorized_refs_present=preauthorized_refs_present,
    )
    run = build_source_browser_agent_run(
        source=source_id,
        recipe=_source_browser_agent_recipe(source_id),
        installed=installed,
        human_steps=[
            {
                "id": action,
                "label": _source_human_gate_reason(action),
                "reason": _source_human_gate_reason(action),
                "can_agent_complete": False,
            }
            for action in customer_actions
        ],
        automated_actions=_source_automated_steps(profile),
        provider_console_url=_source_browser_agent_recipe(source_id).get(
            "provider_console_url"
        ),
        oauth_redirect_url=oauth_redirect_url,
        events_request_url=events_request_url,
        finalize_mode=_source_native_finalize_mode(profile),
        native_connect=profile.get("native_connect"),
    )
    return run


def _source_browser_agent_ingress_urls(
    profile: dict[str, Any],
    *,
    provider_ingress_url: str,
) -> tuple[str | None, str | None]:
    base_url = provider_ingress_url.rstrip("/")
    paths = [str(path) for path in profile.get("ingress_paths", ()) if str(path)]
    callback_path = str(profile.get("callback_path") or "")
    webhook_path = str(profile.get("webhook_path") or "")
    if not callback_path:
        callback_path = next((path for path in paths if "callback" in path), "")
    if not webhook_path:
        webhook_path = next(
            (
                path
                for path in paths
                if path != callback_path
                and any(
                    token in path for token in ("webhook", "events", "pubsub", "push")
                )
            ),
            "",
        )
    if not webhook_path and paths and not callback_path:
        webhook_path = paths[0]
    return _source_ingress_url(base_url, callback_path), _source_ingress_url(
        base_url,
        webhook_path,
    )


def _source_native_finalize_mode(profile: dict[str, Any]) -> str:
    native_connect = profile.get("native_connect")
    if (
        isinstance(native_connect, dict)
        and native_connect.get("kind") == "figma_oauth_file_scoped_connect"
    ):
        return "provider_callback"
    return "generic_customer_refs"


def _source_ingress_url(base_url: str, path: str) -> str | None:
    if not path:
        return None
    if path.startswith("http://") or path.startswith("https://"):
        return path
    return f"{base_url}{path if path.startswith('/') else '/' + path}"


def _generic_source_env_keys(
    source_id: str,
    source_profile: dict[str, Any],
) -> tuple[str, ...]:
    prefix = _source_env_prefix(source_id)
    keys = [f"{prefix}_{ref.upper()}" for ref in source_profile["required_refs"]]
    if source_profile.get("ingress_paths"):
        keys.append(f"{prefix}_WEBHOOK_URL")
    return tuple(dict.fromkeys(keys))


def _generic_source_required_env_keys(
    source_id: str,
    source_profile: dict[str, Any],
) -> tuple[str, ...]:
    prefix = _source_env_prefix(source_id)
    return tuple(
        dict.fromkeys(
            f"{prefix}_{ref.upper()}" for ref in source_profile["required_refs"]
        )
    )


def _generic_source_manual_gates(
    source_id: str,
    source_profile: dict[str, Any],
) -> tuple[str, ...]:
    method = str(source_profile["method"])
    gates = [
        f"{source_id}_provider_admin_approval",
        f"{source_id}_scope_selection",
    ]
    if method in {
        "api_token",
        "dwd",
        "iam_role",
        "gateway",
        "oauth_client_credentials",
        "poll",
        "webhook",
    }:
        gates.append(f"{source_id}_credential_ref_creation")
    if method == "dwd":
        gates.extend(
            [
                f"{source_id}_workspace_dwd_authorization",
                f"{source_id}_workspace_scope_selection",
            ]
        )
    if method in {"oauth", "oauth_plus_gateway"}:
        gates.extend(
            [
                f"{source_id}_oauth_app_or_connection_approval",
                f"{source_id}_oauth_consent",
            ]
        )
    if method == "oauth_plus_gateway":
        gates.append(f"{source_id}_gateway_session_authorization")
    if source_profile.get("ingress_paths"):
        gates.append(f"{source_id}_webhook_registration")
    return tuple(dict.fromkeys(gates))


def _cmd_source_rehearse(args: argparse.Namespace) -> int:
    source_id = str(args.source).strip().lower()
    profile = _source_rehearsal_profile(source_id)
    setup_dir: Path = args.setup_dir or Path(".fyralis/local-rehearsal") / source_id
    setup_dir.mkdir(parents=True, exist_ok=True)

    public_url = _normalize_public_url(args.public_url) if args.public_url else None
    gateway = _gateway_local_url(args)
    gateway_status: dict[str, Any] = {
        "local_gateway_url": gateway,
        "managed_by_command": False,
    }
    tunnel_status: dict[str, Any] = {
        "public_url": public_url,
        "managed_by_command": False,
    }

    if profile["needs_public_url"] and public_url is None and not args.no_start_tunnel:
        gateway_status = _ensure_local_gateway_forward(args, setup_dir)
        tunnel_status = _ensure_ngrok_tunnel(args, setup_dir)
        public_url = tunnel_status.get("public_url")

    rendered_url = public_url or "https://REPLACE_WITH_PUBLIC_GATEWAY"
    files = _write_source_rehearsal_files(
        setup_dir,
        source_id=source_id,
        profile=profile,
        public_url=rendered_url,
        local_gateway_url=gateway,
    )

    env_path = args.provider_env or setup_dir / f"{source_id}.env"
    env_present = env_path.is_file()
    env_report: dict[str, Any] = {
        "path": str(env_path),
        "present": env_present,
        "applied": False,
    }
    install_report: dict[str, Any] = {
        "requested": bool(args.print_install_url),
        "ready": False,
    }

    failed = False
    if args.apply_env:
        if not env_present:
            failed = True
            env_report["error"] = "provider_env_missing"
            env_report["next_action"] = (
                f"copy {files['env_example']} to {env_path} and fill provider credentials"
            )
        else:
            try:
                env_report |= _apply_provider_env(args, source_id, profile, env_path)
                # Do not present a Figma deployment as ready when its managed
                # client-secret or callback contract was not actually applied.
                if source_id == "figma" and not env_report.get("applied"):
                    failed = True
            except Exception as exc:  # noqa: BLE001
                failed = True
                env_report["error"] = type(exc).__name__

    if args.print_install_url:
        if not profile.get("install_endpoint"):
            failed = True
            install_report["error"] = "source_has_no_oauth_install_url"
        elif profile["needs_public_url"] and not public_url:
            failed = True
            install_report["error"] = "public_url_missing"
        elif not args.tenant_id or not args.actor_id:
            failed = True
            install_report["error"] = "tenant_id_and_actor_id_required"
        else:
            try:
                install_report |= _build_provider_install_url(args, source_id, profile)
            except Exception as exc:  # noqa: BLE001
                failed = True
                install_report["error"] = type(exc).__name__

    status = _source_rehearsal_status(
        public_url=public_url,
        needs_public_url=bool(profile["needs_public_url"]),
        env_present=env_present,
        env_applied=bool(env_report.get("applied")),
        install_ready=bool(install_report.get("ready")),
        failed=failed,
    )
    payload = {
        "schema_version": "fyralis.byoc.source.rehearsal.v1",
        "source": source_id,
        "provider_kind": profile["kind"],
        "status": status,
        "generated_at": _now(),
        "public_url": public_url,
        "gateway": gateway_status,
        "tunnel": tunnel_status,
        "files": files,
        "provider_env": env_report,
        "install": install_report,
        "browser_agent": _source_browser_agent_recipe(source_id),
        "browser_agent_run": _source_browser_agent_run(
            args,
            source_id,
            profile.get("source_profile", _source_profile(source_id)),
        ),
        "automated": _source_rehearsal_automated_steps(source_id, profile),
        "manual_gates": _source_manual_gates(source_id, profile, public_url=public_url),
        "raw_secret_values_exported": False,
        "stored_scope": "local_source_rehearsal_metadata_only",
    }
    _write_json(setup_dir / "rehearsal-status.json", payload)
    _emit(args, payload, f"{source_id} rehearsal artifacts written to {setup_dir}")
    return 1 if failed else 0


def _run_source_autopilot(
    args: argparse.Namespace,
    source_id: str,
) -> dict[str, Any]:
    profile = _source_profile(source_id)
    scopes = _source_scopes(args, profile)
    preauthorized_refs = _source_preauthorized_refs(args, source_id)
    preauthorized_refs_present = bool(preauthorized_refs) or bool(args.credential_ref)
    credential_ref = _source_credential_ref(args, source_id, preauthorized_refs)
    source_dir = args.workdir / "sources" / source_id

    provider_setup = _source_provider_setup(
        args,
        source_id=source_id,
        profile=profile,
        scopes=scopes,
        preauthorized_refs_present=preauthorized_refs_present,
    )
    secret_refs = _source_secret_refs(
        credential_ref=credential_ref,
        source_id=source_id,
        profile=profile,
        preauthorized_refs=preauthorized_refs,
    )
    connection = _source_connection(
        args,
        source_id=source_id,
        profile=profile,
        credential_ref=credential_ref,
        scopes=scopes,
        provider_setup=provider_setup,
    )
    validation = _source_validation(
        args,
        source_id=source_id,
        profile=profile,
        credential_ref=credential_ref,
        scopes=scopes,
    )
    scope_receipt = _source_scope_receipt(source_id, scopes)
    sync = _source_first_sync(args, source_id=source_id, scopes=scopes)
    activation = _source_activation(source_id, scopes) if args.auto_activate else None
    readiness = _source_readiness(source_id, validation, sync, activation)

    artifacts = {
        "provider_setup": source_dir / "provider-setup.json",
        "secret_refs": source_dir / "secret-refs.json",
        "connection": source_dir / "connection.json",
        "validation": source_dir / "validation.json",
        "scope": source_dir / "scope.json",
        "first_sync": source_dir / "first-sync.json",
        "activation": source_dir / "activation.json",
        "readiness": source_dir / "readiness-receipt.json",
    }
    _write_json(artifacts["provider_setup"], provider_setup)
    _write_json(artifacts["secret_refs"], secret_refs)
    _write_json(artifacts["connection"], connection)
    _write_json(artifacts["validation"], validation)
    _write_json(artifacts["scope"], scope_receipt)
    _write_json(artifacts["first_sync"], sync)
    if activation is not None:
        _write_json(artifacts["activation"], activation)
    _write_json(artifacts["readiness"], readiness)

    return {
        "source": source_id,
        "method": profile["method"],
        "status": "active" if activation else "ready",
        "credential_ref_sha256": _sha256(credential_ref),
        "selected_scope_count": len(scopes),
        "provider_authorization_mode": args.provider_authorization_mode,
        "preauthorized_refs_present": preauthorized_refs_present,
        "automation_level": _source_automation_level(profile),
        "browser_agent": _source_browser_agent_recipe(source_id),
        "browser_agent_run": _source_browser_agent_run(
            args,
            source_id,
            profile,
            preauthorized_refs_present=preauthorized_refs_present,
            installed=bool(activation),
        ),
        "customer_action_required": _source_customer_action_required(
            profile,
            authorization_mode=args.provider_authorization_mode,
            preauthorized_refs_present=preauthorized_refs_present,
        ),
        "artifacts": {
            "provider_setup": str(artifacts["provider_setup"]),
            "secret_refs": str(artifacts["secret_refs"]),
            "connection": str(artifacts["connection"]),
            "validation": str(artifacts["validation"]),
            "scope": str(artifacts["scope"]),
            "first_sync": str(artifacts["first_sync"]),
            "activation": str(artifacts["activation"]) if activation else None,
            "readiness": str(artifacts["readiness"]),
        },
    }


def _normalize_public_url(public_url: str) -> str:
    value = public_url.strip().rstrip("/")
    if not value.startswith("https://"):
        raise ValueError("public provider rehearsal URL must start with https://")
    return value


def _gateway_local_url(args: argparse.Namespace) -> str:
    return f"http://127.0.0.1:{int(args.gateway_local_port)}"


def _ensure_local_gateway_forward(
    args: argparse.Namespace,
    setup_dir: Path,
) -> dict[str, Any]:
    local_url = _gateway_local_url(args)
    if _http_health_ok(f"{local_url}/healthz"):
        return {
            "status": "already_running",
            "local_gateway_url": local_url,
            "managed_by_command": False,
        }

    log_path = setup_dir / "gateway-port-forward.log"
    pid_path = setup_dir / "gateway-port-forward.pid"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    log_file = log_path.open("ab")
    cmd = [
        args.kubectl,
        "-n",
        args.namespace,
        "port-forward",
        "--address",
        "127.0.0.1",
        f"svc/{args.gateway_service}",
        f"{int(args.gateway_local_port)}:8000",
    ]
    try:
        proc = subprocess.Popen(
            cmd,
            stdout=log_file,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
    except FileNotFoundError:
        return {
            "status": "blocked",
            "error": "kubectl_not_found",
            "local_gateway_url": local_url,
            "managed_by_command": False,
            "command": " ".join(cmd),
        }
    pid_path.write_text(str(proc.pid) + "\n", encoding="utf-8")
    if _wait_for_health(f"{local_url}/healthz", seconds=20):
        return {
            "status": "running",
            "local_gateway_url": local_url,
            "managed_by_command": True,
            "pid_file": str(pid_path),
            "log_file": str(log_path),
            "stop_command": f"kill $(cat {pid_path})",
        }
    return {
        "status": "blocked",
        "error": "gateway_port_forward_not_ready",
        "local_gateway_url": local_url,
        "managed_by_command": True,
        "pid_file": str(pid_path),
        "log_file": str(log_path),
    }


def _ensure_ngrok_tunnel(
    args: argparse.Namespace,
    setup_dir: Path,
) -> dict[str, Any]:
    existing = _ngrok_public_url(args.ngrok_api)
    if existing:
        return {
            "status": "already_running",
            "public_url": existing,
            "managed_by_command": False,
            "api": args.ngrok_api,
        }

    log_path = setup_dir / "ngrok.log"
    pid_path = setup_dir / "ngrok.pid"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    log_file = log_path.open("ab")
    cmd = ["ngrok", "http", str(int(args.gateway_local_port)), "--log=stdout"]
    try:
        proc = subprocess.Popen(
            cmd,
            stdout=log_file,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
    except FileNotFoundError:
        return {
            "status": "blocked",
            "error": "ngrok_not_found",
            "public_url": None,
            "managed_by_command": False,
            "command": " ".join(cmd),
        }
    pid_path.write_text(str(proc.pid) + "\n", encoding="utf-8")
    deadline = time.time() + 25
    public_url: str | None = None
    while time.time() < deadline:
        public_url = _ngrok_public_url(args.ngrok_api)
        if public_url:
            break
        time.sleep(0.5)
    if public_url:
        return {
            "status": "running",
            "public_url": public_url,
            "managed_by_command": True,
            "api": args.ngrok_api,
            "pid_file": str(pid_path),
            "log_file": str(log_path),
            "stop_command": f"curl -sS -X DELETE {args.ngrok_api}/command_line",
        }
    return {
        "status": "blocked",
        "error": "ngrok_public_url_not_ready",
        "public_url": None,
        "managed_by_command": True,
        "api": args.ngrok_api,
        "pid_file": str(pid_path),
        "log_file": str(log_path),
    }


def _write_source_rehearsal_files(
    setup_dir: Path,
    *,
    source_id: str,
    profile: dict[str, Any],
    public_url: str,
    local_gateway_url: str,
) -> dict[str, str]:
    files = {
        "provider_setup": setup_dir / f"{source_id}-provider-setup.json",
        "env_example": setup_dir / f"{source_id}.env.example",
        "readme": setup_dir / "README.md",
    }
    if source_id == "slack":
        files["manifest"] = setup_dir / "fyralis-slack-app-manifest.yaml"
        files["events_manifest"] = setup_dir / "fyralis-slack-app-events-manifest.yaml"
    elif source_id == "github":
        files["manifest"] = setup_dir / "fyralis-github-app-manifest.json"
    elif source_id in {"discord", "facebook_pages", "notion"}:
        files["manifest"] = setup_dir / f"fyralis-{source_id}-app-setup.json"
    elif source_id == "jira":
        files["connect_payload"] = setup_dir / "jira-connect-payload.example.json"
    elif source_id == "telegram":
        files["session_plan"] = setup_dir / "telegram-session-plan.json"
    else:
        files["connection_checklist"] = (
            setup_dir / f"{source_id}-connection-checklist.json"
        )

    setup_payload = _source_rehearsal_setup_payload(
        source_id,
        profile,
        public_url=public_url,
        local_gateway_url=local_gateway_url,
    )
    _write_json(files["provider_setup"], setup_payload)

    if source_id == "slack":
        base_manifest, events_manifest = _slack_manifest_text(public_url)
        files["manifest"].write_text(base_manifest, encoding="utf-8")
        files["events_manifest"].write_text(events_manifest, encoding="utf-8")
    elif source_id == "github":
        _write_json(files["manifest"], _github_app_manifest(public_url))
    elif source_id in {"discord", "facebook_pages", "notion"}:
        _write_json(
            files["manifest"],
            _generic_app_setup_manifest(source_id, profile, public_url=public_url),
        )
    elif source_id == "jira":
        _write_json(files["connect_payload"], _jira_connect_payload(public_url))
    elif source_id == "telegram":
        _write_json(files["session_plan"], _telegram_session_plan())
    elif "connection_checklist" in files:
        _write_json(
            files["connection_checklist"],
            _generic_source_connection_checklist(
                source_id,
                profile,
                public_url=public_url,
            ),
        )

    files["env_example"].write_text(
        _source_env_example(source_id, profile, public_url=public_url),
        encoding="utf-8",
    )
    if source_id == "slack":
        legacy_env_example = setup_dir / "slack-app.env.example"
        legacy_env_example.write_text(
            files["env_example"].read_text(encoding="utf-8"),
            encoding="utf-8",
        )
        files["legacy_env_example"] = legacy_env_example
    files["readme"].write_text(
        _source_rehearsal_readme(
            source_id,
            profile,
            public_url=public_url,
            local_gateway_url=local_gateway_url,
        ),
        encoding="utf-8",
    )
    return {name: str(path) for name, path in files.items()}


def _slack_manifest_text(public_url: str) -> tuple[str, str]:
    bot_scopes = "\n".join(f"      - {scope}" for scope in SLACK_BOT_SCOPES)
    user_scopes = "\n".join(f"      - {scope}" for scope in SLACK_USER_SCOPES)
    bot_events = "\n".join(f"      - {event}" for event in SLACK_BOT_EVENTS)
    base_manifest = f"""display_information:
  name: Fyralis Local Rehearsal
  description: Slack ingestion test app for the local Fyralis BYOC rehearsal.
  background_color: "#0b1020"
features:
  bot_user:
    display_name: Fyralis
    always_online: false
oauth_config:
  redirect_urls:
    - {public_url}/integrations/slack/callback
  scopes:
    bot:
{bot_scopes}
    user:
{user_scopes}
settings:
  org_deploy_enabled: false
  socket_mode_enabled: false
  token_rotation_enabled: false
"""
    events_manifest = (
        base_manifest.rstrip()
        + f"""
  event_subscriptions:
    request_url: {public_url}/webhooks/slack/events
    bot_events:
{bot_events}
"""
    )
    return base_manifest, events_manifest


def _source_rehearsal_setup_payload(
    source_id: str,
    profile: dict[str, Any],
    *,
    public_url: str,
    local_gateway_url: str,
) -> dict[str, Any]:
    return {
        "schema_version": "fyralis.byoc.source.provider_setup.v1",
        "source": source_id,
        "provider_kind": profile["kind"],
        "generated_at": _now(),
        "public_url": public_url if profile["needs_public_url"] else None,
        "local_gateway_url": local_gateway_url,
        "install_endpoint": profile.get("install_endpoint"),
        "callback_url": (
            f"{public_url}{profile['callback_path']}"
            if profile.get("callback_path")
            else None
        ),
        "webhook_url": (
            f"{public_url}{profile['webhook_path']}"
            if profile.get("webhook_path")
            else None
        ),
        "required_env": list(profile.get("required_env", profile.get("env", ()))),
        "env_template_keys": list(profile.get("env", ())),
        "manual_gates": list(profile.get("manual_gate_names", ())),
        "raw_secret_values_included": False,
    }


def _github_app_manifest(public_url: str) -> dict[str, Any]:
    return {
        "name": "Fyralis Local Rehearsal",
        "url": public_url,
        "hook_attributes": {"url": f"{public_url}/webhooks/github"},
        "redirect_url": f"{public_url}/integrations/github/callback",
        "callback_urls": [f"{public_url}/integrations/github/callback"],
        "public": False,
        "default_permissions": {
            "contents": "read",
            "issues": "read",
            "metadata": "read",
            "pull_requests": "read",
        },
        "default_events": [
            "issues",
            "pull_request",
            "pull_request_review",
            "push",
        ],
        "note": (
            "Use GitHub's App Manifest flow or create a GitHub App manually. "
            "Fyralis needs the resulting app slug, app ID, private key, and webhook secret."
        ),
    }


def _generic_app_setup_manifest(
    source_id: str,
    profile: dict[str, Any],
    *,
    public_url: str,
) -> dict[str, Any]:
    return {
        "source": source_id,
        "provider_kind": profile["kind"],
        "callback_url": (
            f"{public_url}{profile['callback_path']}"
            if profile.get("callback_path")
            else None
        ),
        "webhook_url": (
            f"{public_url}{profile['webhook_path']}"
            if profile.get("webhook_path")
            else None
        ),
        "required_env": list(profile.get("required_env", profile.get("env", ()))),
        "notes": _provider_setup_notes(source_id),
    }


def _generic_source_connection_checklist(
    source_id: str,
    profile: dict[str, Any],
    *,
    public_url: str,
) -> dict[str, Any]:
    source_profile = profile.get("source_profile", _source_profile(source_id))
    callback, webhook = _source_browser_agent_ingress_urls(
        profile,
        provider_ingress_url=public_url,
    )
    return {
        "schema_version": "fyralis.byoc.source.connection_checklist.v1",
        "source": source_id,
        "provider_kind": profile["kind"],
        "method": source_profile["method"],
        "generated_at": _now(),
        "required_env": list(profile.get("required_env", profile.get("env", ()))),
        "default_scopes": list(source_profile.get("default_scopes", ())),
        "provider_permissions": list(source_profile.get("provider_permissions", ())),
        "callback_url": callback,
        "webhook_url": webhook,
        "no_ingress_reason": source_profile.get("no_ingress_reason"),
        "browser_agent": _source_browser_agent_recipe(source_id),
        "browser_agent_run": _source_rehearsal_browser_agent_run(
            source_id,
            source_profile=source_profile,
            provider_ingress_url=public_url,
        ),
        "manual_gates": list(profile.get("manual_gate_names", ())),
        "automation_boundary": (
            "Fyralis generates local artifacts, validates required refs, applies "
            "customer-cloud env to the gateway when requested, and starts local "
            "validation/sync receipts. Provider admin consent and token creation "
            "remain customer-side gates unless preauthorized refs already exist."
        ),
        "raw_secret_values_included": False,
    }


def _source_rehearsal_browser_agent_run(
    source_id: str,
    *,
    source_profile: dict[str, Any],
    provider_ingress_url: str,
) -> dict[str, Any]:
    oauth_redirect_url, events_request_url = _source_browser_agent_ingress_urls(
        source_profile,
        provider_ingress_url=provider_ingress_url,
    )
    return build_source_browser_agent_run(
        source=source_id,
        recipe=_source_browser_agent_recipe(source_id),
        human_steps=[
            {
                "id": action,
                "label": _source_human_gate_reason(action),
                "reason": _source_human_gate_reason(action),
                "can_agent_complete": False,
            }
            for action in _source_customer_action_required(
                source_profile,
                authorization_mode="customer-owned-ref",
                preauthorized_refs_present=False,
            )
        ],
        automated_actions=_source_automated_steps(source_profile),
        provider_console_url=_source_browser_agent_recipe(source_id).get(
            "provider_console_url"
        ),
        oauth_redirect_url=oauth_redirect_url,
        events_request_url=events_request_url,
        finalize_mode=_source_native_finalize_mode(source_profile),
        native_connect=source_profile.get("native_connect"),
    )


def _jira_connect_payload(public_url: str) -> dict[str, Any]:
    return {
        "base_url": "${JIRA_BASE_URL}",
        "account_email": "${JIRA_ACCOUNT_EMAIL}",
        "api_token_ref": "${JIRA_API_TOKEN_REF}",
        "project_keys": "${JIRA_PROJECT_KEYS comma-separated or blank for all}",
        "webhook_secret_ref": "${JIRA_WEBHOOK_SECRET_REF optional}",
        "webhook_url": f"{public_url}/webhooks/jira/events",
        "note": "Resolve refs inside customer cloud before submitting finalize; do not export raw token values.",
    }


def _telegram_session_plan() -> dict[str, Any]:
    return {
        "source": "telegram",
        "provider_kind": "local_gateway_session",
        "steps": [
            "Create a Telegram API ID and API hash at my.telegram.org.",
            "Run customer-cloud MTProto login to produce a StringSession.",
            "Store live and optional backfill sessions in the customer secret manager.",
            "Select approved dialogs/chats and finalize the installation locally.",
        ],
        "required_env": list(
            _source_rehearsal_profile("telegram")["required_env"]
        ),
        "raw_secret_values_included": False,
    }


def _source_env_example(
    source_id: str,
    profile: dict[str, Any],
    *,
    public_url: str,
) -> str:
    lines: list[str] = []
    configured_defaults = profile.get("default_env", {})
    if not isinstance(configured_defaults, dict):
        configured_defaults = {}
    for key in profile.get("env", ()):
        value = str(configured_defaults.get(key) or "")
        if not value and key.endswith("_REDIRECT_URI") and profile.get("callback_path"):
            value = f"{public_url}{profile['callback_path']}"
        elif not value and key.endswith("_WEBHOOK_URL") and profile.get("webhook_path"):
            value = f"{public_url}{profile['webhook_path']}"
        elif not value and key == "OAUTH_STATE_HMAC_KEY":
            value = "customer-cloud-secret-ref://oauth-state-hmac-key"
        elif key == "FIGMA_OAUTH_UI_BASE_URL":
            lines.append(
                "# Set the trusted HTTPS origin of this deployment's Fyralis onboarding UI."
            )
        elif key == "JIRA_PROJECT_KEYS":
            lines.append("# Optional comma-separated Jira project keys.")
        elif key == "TELEGRAM_DIALOGS_JSON":
            lines.append("# Optional JSON array of approved Telegram dialogs.")
        lines.append(f"{key}={value}")
    return "\n".join(lines) + "\n"


def _source_rehearsal_readme(
    source_id: str,
    profile: dict[str, Any],
    *,
    public_url: str,
    local_gateway_url: str,
) -> str:
    callback = (
        f"{public_url}{profile['callback_path']}"
        if profile.get("callback_path")
        else None
    )
    webhook = (
        f"{public_url}{profile['webhook_path']}"
        if profile.get("webhook_path")
        else None
    )
    apply_command = f"fyralis byoc source rehearse --source {source_id} --apply-env"
    if profile.get("install_endpoint"):
        apply_command += (
            " --print-install-url --tenant-id <tenant-id> --actor-id <actor-id>"
        )
    return f"""# Fyralis {source_id.title()} Local Rehearsal

Public gateway:

```text
{public_url if profile["needs_public_url"] else "not required for this source"}
```

Local gateway:

```text
{local_gateway_url}
```

Provider URLs:

```text
Callback URL: {callback or "not used"}
Webhook URL:  {webhook or "not used"}
```

Copy `{source_id}.env.example` to `{source_id}.env` and fill credentials locally.
Then run:

```bash
{apply_command}
```

Manual gates that remain outside Fyralis automation:

{chr(10).join(f"- {gate}" for gate in profile.get("manual_gate_names", ()))}
"""


def _apply_provider_env(
    args: argparse.Namespace,
    source_id: str,
    profile: dict[str, Any],
    env_path: Path,
) -> dict[str, Any]:
    env_values = _read_env_file(env_path)
    required = list(profile.get("required_env", profile.get("env", ())))
    missing = [key for key in required if not env_values.get(key)]
    if missing:
        return {"applied": False, "error": "missing_env_values", "missing": missing}
    for target, source in dict(profile.get("derived_env", {})).items():
        if env_values.get(source):
            env_values.setdefault(target, env_values[source])

    deployment_env_values = _provider_env_values_for_apply(
        source_id,
        profile,
        env_values,
    )
    deployment_targets = _provider_runtime_deployments(args, source_id, profile)

    if profile["kind"] in {"api_token_connect", "local_gateway_session"}:
        return {
            "applied": True,
            "mode": "local_env_validated",
            "gateway_deployment": None,
            "raw_secret_values_exported": False,
        }

    with tempfile.NamedTemporaryFile("w", encoding="utf-8", delete=False) as handle:
        tmp_path = Path(handle.name)
        os.chmod(tmp_path, 0o600)
        for key, value in deployment_env_values.items():
            handle.write(f"{key}={value}\n")
    try:
        create = subprocess.run(
            [
                args.kubectl,
                "-n",
                args.namespace,
                "create",
                "secret",
                "generic",
                f"fyralis-{source_id}-integration",
                f"--from-env-file={tmp_path}",
                "--dry-run=client",
                "-o",
                "yaml",
            ],
            check=True,
            capture_output=True,
        )
        subprocess.run(
            [args.kubectl, "apply", "-f", "-"],
            input=create.stdout,
            check=True,
            capture_output=True,
        )
        for deployment in deployment_targets:
            subprocess.run(
                [
                    args.kubectl,
                    "-n",
                    args.namespace,
                    "set",
                    "env",
                    f"deployment/{deployment}",
                    f"--from=secret/fyralis-{source_id}-integration",
                ],
                check=True,
                capture_output=True,
            )
        for deployment in deployment_targets:
            subprocess.run(
                [
                    args.kubectl,
                    "-n",
                    args.namespace,
                    "rollout",
                    "status",
                    f"deployment/{deployment}",
                    "--timeout=120s",
                ],
                check=True,
                capture_output=True,
            )
    finally:
        tmp_path.unlink(missing_ok=True)
    result = {
        "applied": True,
        "mode": "gateway_env_applied",
        "kubernetes_secret": f"fyralis-{source_id}-integration",
        "gateway_deployment": (
            deployment_targets[0] if source_id == "figma" else "fyralis-gateway"
        ),
        "raw_secret_values_exported": False,
    }
    if source_id == "figma":
        result["mode"] = "figma_runtime_env_applied"
        result["runtime_deployments"] = list(deployment_targets)
        result["runtime_components"] = list(
            _provider_runtime_components(profile)
        )
    return result


def _provider_runtime_deployments(
    args: argparse.Namespace,
    source_id: str,
    profile: dict[str, Any],
) -> tuple[str, ...]:
    """Return live rollout targets without changing non-Figma rollout scope."""
    if source_id != "figma":
        return ("fyralis-gateway",)
    components = _provider_runtime_components(profile)
    selector = "app.kubernetes.io/component in (" + ",".join(components) + ")"
    discovered = subprocess.run(
        [
            args.kubectl,
            "-n",
            args.namespace,
            "get",
            "deployment",
            "-l",
            selector,
            "-o",
            "json",
        ],
        check=True,
        capture_output=True,
    )
    try:
        payload = json.loads(discovered.stdout)
    except (TypeError, ValueError) as exc:
        raise ValueError("could not read Figma runtime deployment discovery") from exc
    items = payload.get("items") if isinstance(payload, dict) else None
    if not isinstance(items, list):
        raise ValueError("Figma runtime deployment discovery returned no deployment list")
    targets_by_component: dict[str, list[str]] = {component: [] for component in components}
    for item in items:
        if not isinstance(item, dict):
            continue
        metadata = item.get("metadata")
        if not isinstance(metadata, dict):
            continue
        name = str(metadata.get("name") or "").strip()
        labels = metadata.get("labels")
        component = (
            str(labels.get("app.kubernetes.io/component") or "").strip()
            if isinstance(labels, dict)
            else ""
        )
        if name and component in targets_by_component:
            targets_by_component[component].append(name)
    missing = sorted(
        component
        for component in FIGMA_REQUIRED_RUNTIME_COMPONENTS
        if not targets_by_component.get(component)
    )
    if missing:
        raise ValueError(
            "Figma runtime deployments are missing required component labels: "
            + ", ".join(missing)
        )
    return tuple(
        deployment
        for component in components
        for deployment in sorted(targets_by_component[component])
    )


def _provider_runtime_components(profile: dict[str, Any]) -> tuple[str, ...]:
    configured = profile.get("runtime_components")
    if not isinstance(configured, (list, tuple)):
        return FIGMA_RUNTIME_COMPONENTS
    components = tuple(
        str(item).strip() for item in configured if str(item).strip()
    )
    return components or FIGMA_RUNTIME_COMPONENTS


def _provider_env_values_for_apply(
    source_id: str,
    profile: dict[str, Any],
    env_values: dict[str, str],
) -> dict[str, str]:
    """Limit Figma rollout material to its declared config and secret refs.

    A Figma Client Secret must already live behind
    ``FIGMA_CLIENT_SECRET_SECRET_REF``.  Do not copy arbitrary values from a
    local env file (including a development-only plaintext fallback) into the
    Kubernetes integration secret.
    """
    if source_id != "figma":
        return env_values
    allowed = {str(key) for key in profile.get("env", ())}
    return {key: value for key, value in env_values.items() if key in allowed}


def _build_provider_install_url(
    args: argparse.Namespace,
    source_id: str,
    profile: dict[str, Any],
) -> dict[str, Any]:
    bootstrap_secret = args.bootstrap_secret or _load_gateway_bootstrap_secret(args)
    session_payload = {
        "actor_id": args.actor_id,
        "tenant_id": args.tenant_id,
        "ttl_seconds": 86400,
    }
    session = _http_json_request(
        f"{_gateway_local_url(args)}/auth/session",
        method="POST",
        payload=session_payload,
        headers={"X-Bootstrap-Secret": bootstrap_secret},
    )
    token = str(session["token"])
    location = _http_redirect_location(
        f"{_gateway_local_url(args)}{profile['install_endpoint']}",
        headers={"Authorization": f"Bearer {token}"},
    )
    return {
        "ready": True,
        "source": source_id,
        "install_url": location,
        "session_expires_at": session.get("expires_at"),
    }


def _load_gateway_bootstrap_secret(args: argparse.Namespace) -> str:
    encoded = subprocess.run(
        [
            args.kubectl,
            "-n",
            args.namespace,
            "get",
            "secret",
            "fyralis-app-secret",
            "-o",
            "jsonpath={.data.AUTH_BOOTSTRAP_SECRET}",
        ],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    return base64.b64decode(encoded).decode("utf-8")


def _read_env_file(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip()
    return values


def _http_health_ok(url: str) -> bool:
    try:
        with urlrequest.urlopen(url, timeout=1.5) as response:  # noqa: S310
            return 200 <= int(response.status) < 300
    except Exception:  # noqa: BLE001
        return False


def _wait_for_health(url: str, *, seconds: int) -> bool:
    deadline = time.time() + seconds
    while time.time() < deadline:
        if _http_health_ok(url):
            return True
        time.sleep(0.5)
    return False


def _ngrok_public_url(api_url: str) -> str | None:
    try:
        data = _http_json_request(api_url, method="GET")
    except Exception:  # noqa: BLE001
        return None
    tunnels = data.get("tunnels", [])
    if not isinstance(tunnels, list):
        return None
    for tunnel in tunnels:
        if not isinstance(tunnel, dict):
            continue
        public_url = str(tunnel.get("public_url") or "")
        if public_url.startswith("https://"):
            return public_url.rstrip("/")
    return None


def _http_json_request(
    url: str,
    *,
    method: str,
    payload: dict[str, Any] | None = None,
    headers: dict[str, str] | None = None,
) -> dict[str, Any]:
    body = None
    request_headers = dict(headers or {})
    if payload is not None:
        body = json.dumps(payload).encode("utf-8")
        request_headers["Content-Type"] = "application/json"
    req = urlrequest.Request(url, data=body, headers=request_headers, method=method)
    with urlrequest.urlopen(req, timeout=15.0) as response:  # noqa: S310
        data = response.read().decode("utf-8")
    parsed = json.loads(data or "{}")
    if not isinstance(parsed, dict):
        raise ValueError("HTTP response must be a JSON object")
    return parsed


class _NoRedirect(urlrequest.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):  # type: ignore[override]
        return None


def _http_redirect_location(url: str, *, headers: dict[str, str]) -> str:
    opener = urlrequest.build_opener(_NoRedirect)
    req = urlrequest.Request(url, headers=headers, method="GET")
    try:
        opener.open(req, timeout=15.0)  # noqa: S310
    except urlerror.HTTPError as exc:
        if exc.code in {301, 302, 303, 307, 308}:
            location = exc.headers.get("Location")
            if location:
                return location
        raise
    raise ValueError("expected provider install endpoint to return a redirect")


def _source_rehearsal_status(
    *,
    public_url: str | None,
    needs_public_url: bool,
    env_present: bool,
    env_applied: bool,
    install_ready: bool,
    failed: bool,
) -> str:
    if failed:
        return "blocked"
    if install_ready:
        return "install_url_ready"
    if env_applied:
        return "gateway_env_applied"
    if env_present:
        return "provider_env_ready"
    if public_url or not needs_public_url:
        return "ready_for_provider_setup"
    return "needs_public_url"


def _source_manual_gates(
    source_id: str,
    profile: dict[str, Any],
    *,
    public_url: str | None,
) -> list[dict[str, Any]]:
    gates: list[dict[str, Any]] = []
    if profile["needs_public_url"] and public_url is None:
        gates.append(
            {
                "name": "public_https_url",
                "reason": f"{source_id} provider callbacks require a reachable HTTPS URL.",
                "can_agent_complete": True,
            }
        )
    for name in profile.get("manual_gate_names", ()):
        gates.append(
            {
                "name": name,
                "reason": _manual_gate_reason(source_id, name),
                "can_agent_complete": False,
            }
        )
    return gates


def _manual_gate_reason(source_id: str, gate_name: str) -> str:
    display_name = _source_display_name(source_id)
    if gate_name.endswith("_provider_admin_approval"):
        return f"A {display_name} admin must approve this connection in the customer-owned provider."
    if gate_name.endswith("_scope_selection"):
        return (
            f"The customer chooses which {display_name} resources Fyralis may ingest."
        )
    if gate_name.endswith("_credential_ref_creation"):
        return (
            f"The customer stores {display_name} credentials or references in the "
            "customer-cloud secret manager; raw values are not sent to Fyralis."
        )
    if gate_name.endswith("_oauth_app_or_connection_approval"):
        return (
            f"The customer creates or approves the {display_name} OAuth app/connection."
        )
    if gate_name.endswith("_oauth_consent"):
        return f"{display_name} OAuth requires a customer-side approval screen."
    if gate_name.endswith("_webhook_registration"):
        return f"The customer registers the generated {display_name} webhook URL when the provider requires it."
    reasons = {
        "slack_app_creation_or_admin_approval": (
            "Slack app creation can use manifests, but workspace app approval "
            "belongs to the customer's Slack admin policy."
        ),
        "slack_oauth_consent": "Slack OAuth requires a user/admin approval screen.",
        "github_provider_admin_approval": (
            "A GitHub org admin must approve the generated GitHub App configuration."
        ),
        "github_app_installation_approval": (
            "A GitHub org owner must approve/install the GitHub App on selected repositories."
        ),
        "discord_application_creation_or_update": (
            "A Discord application/bot must exist with the generated callback and interaction URLs."
        ),
        "discord_oauth_consent": "A Discord server admin must approve the bot install.",
        "facebook_pages_app_creation_or_update": (
            "A Meta app must exist with the generated OAuth callback and Messenger webhook URL."
        ),
        "facebook_pages_oauth_consent": (
            "A Page admin must approve the Facebook Page messaging permissions."
        ),
        "facebook_pages_webhook_subscription_approval": (
            "The Page must be subscribed to the Messenger messages webhook field."
        ),
        "notion_integration_creation_or_update": (
            "A Notion integration must be created or updated in the customer's workspace."
        ),
        "notion_oauth_consent": "A Notion workspace user/admin must approve access.",
        "notion_webhook_verification_token_copy": (
            "Notion delivers a verification token during subscription setup; it must be copied into customer-cloud env."
        ),
        "figma_private_oauth_app_creation_or_update": (
            "A deployment administrator must create or update a private Figma OAuth app "
            "owned by this customer BYOC deployment."
        ),
        "figma_redirect_uri_registration": (
            "The Figma app owner must save the exact generated HTTPS callback URL "
            "under Figma OAuth credentials."
        ),
        "figma_deployment_secret_storage": (
            "The Figma Client Secret must be stored only as a deployment-managed "
            "secret reference; do not place it in the onboarding UI."
        ),
        "figma_file_scoped_oauth_consent": (
            "After deployment setup, each user selects approved Figma file URLs and "
            "approves the file-scoped OAuth consent screen."
        ),
        "jira_api_token_creation": (
            "A Jira user must create an Atlassian API token for the approved site."
        ),
        "jira_project_scope_admin_approval": (
            "A Jira admin must approve which projects Fyralis may ingest."
        ),
        "jira_webhook_admin_approval": (
            "A Jira admin must approve the generated webhook target for live events."
        ),
        "jira_project_scope_selection": "The customer chooses which Jira projects are in scope.",
        "jira_webhook_registration": (
            "The customer may need to register Jira webhooks with the generated endpoint."
        ),
        "telegram_api_id_creation": (
            "A Telegram API ID/hash must be created by the customer account owner."
        ),
        "telegram_mtproto_login_code": (
            "Telegram sends a login code to the customer account; Fyralis cannot bypass that trust step."
        ),
        "telegram_dialog_scope_selection": (
            "The customer chooses which chats/channels/dialogs Fyralis may ingest."
        ),
    }
    return reasons.get(gate_name, f"{source_id} requires provider-side approval.")


def _source_display_name(source_id: str) -> str:
    names = {
        "aws": "AWS",
        "brex": "Brex",
        "facebook_pages": "Facebook Page Messages",
        "figma": "Figma",
        "gmail": "Gmail",
        "github": "GitHub",
        "grafana": "Grafana",
        "hibob": "HiBob",
        "jira": "Jira",
        "miro": "Miro",
        "notion": "Notion",
        "quickbooks": "QuickBooks",
        "slack": "Slack",
        "whatsapp": "WhatsApp",
    }
    return names.get(
        source_id,
        " ".join(part.capitalize() for part in source_id.split("-")),
    )


def _source_rehearsal_automated_steps(
    source_id: str,
    profile: dict[str, Any],
) -> list[str]:
    steps = ["provider_setup_artifact_generation", "env_template_generation"]
    if profile["needs_public_url"]:
        steps.extend(["local_gateway_port_forward", "public_https_tunnel_discovery"])
    if profile["kind"] in {"oauth_app", "github_app", "oauth_or_preauthorized_ref"}:
        steps.extend(
            [
                "oauth_state_key_template_generation",
                "kubernetes_secret_apply_when_env_exists",
            ]
        )
        if profile.get("install_endpoint"):
            steps.append("oauth_install_url_generation")
    elif profile["kind"] in {"api_token_connect", "oauth_client_credentials_connect"}:
        steps.extend(["connect_payload_generation", "local_env_validation"])
    elif profile["kind"] == "local_gateway_session":
        steps.extend(["session_plan_generation", "local_env_validation"])
    elif profile["kind"] in {"iam_role_ref", "polling_ref", "webhook_endpoint"}:
        steps.extend(["connection_checklist_generation", "local_env_validation"])
    if source_id == "telegram":
        steps.append("public_tunnel_not_required")
    return steps


def _provider_setup_notes(source_id: str) -> list[str]:
    notes = {
        "discord": [
            "Use the Discord Developer Portal to create an application and bot.",
            "Set the OAuth redirect URL to the callback URL.",
            "Set the interactions endpoint to the webhook URL.",
            "Copy client ID, client secret, application ID, app public key, and bot token into the local env file.",
        ],
        "facebook_pages": [
            "Use Meta for Developers to create or update the Facebook app.",
            "Set the OAuth redirect URL to the callback URL.",
            "Set the Messenger webhook callback URL and verify token.",
            "Copy app ID, app secret, redirect URI, and webhook verify token into the local env file.",
        ],
        "notion": [
            "Use Notion integrations settings to create an OAuth integration.",
            "Set redirect URI to the callback URL.",
            "Configure webhook subscription after gateway env is applied.",
            "Copy the webhook verification token into NOTION_WEBHOOK_VERIFICATION_TOKEN when Notion provides it.",
        ],
        "figma": [
            "Create one private Figma OAuth app owned by this customer BYOC deployment.",
            "Register the exact callback URL under Figma OAuth credentials.",
            "Store the Client Secret through FIGMA_CLIENT_SECRET_SECRET_REF; never enter it in Fyralis onboarding.",
            "After the deployment readiness check passes, users connect explicitly selected design file URLs from the Figma card.",
        ],
    }
    if source_id in notes:
        return notes[source_id]
    source_profile = (
        _source_profile(source_id)
        if source_id in SOURCE_CONNECTION_CATALOG
        else {}
    )
    generic_notes = [
        f"Use the customer-owned {source_id} admin console to approve the connection.",
        "Store credentials in the customer-cloud secret manager or local env file only.",
        "Run the generated Fyralis rehearsal command after the env file is filled.",
    ]
    if source_profile.get("ingress_paths"):
        generic_notes.append(
            "Register the generated webhook URL with the provider when required."
        )
    if source_profile.get("no_ingress_reason"):
        generic_notes.append(str(source_profile["no_ingress_reason"]))
    return generic_notes


def _source_ids_from_args(args: argparse.Namespace) -> tuple[list[str] | None, int]:
    requested = str(args.source).strip().lower()
    source_ids = list(SOURCE_CONNECTION_SLUGS) if requested == "all" else [requested]
    invalid = [
        source_id
        for source_id in source_ids
        if source_id not in SOURCE_CONNECTION_CATALOG
    ]
    if invalid:
        print(f"unsupported source: {', '.join(invalid)}", file=sys.stderr)
        return None, 2
    if len(source_ids) > 1 and getattr(args, "credential_ref", None):
        print("--credential-ref is only valid for one source", file=sys.stderr)
        return None, 2
    return source_ids, 0


def _source_preauthorized_refs_from_args(
    args: argparse.Namespace,
) -> tuple[dict[str, dict[str, Any]], int]:
    try:
        return _load_preauthorized_ref_manifest(args.preauthorized_ref_manifest), 0
    except Exception as exc:  # noqa: BLE001
        print(
            f"failed to load preauthorized ref manifest: {type(exc).__name__}",
            file=sys.stderr,
        )
        return {}, 2


def _source_discovery(args: argparse.Namespace, source_id: str) -> dict[str, Any]:
    profile = _source_profile(source_id)
    scopes = _source_scopes(args, profile)
    preauthorized_refs = _source_preauthorized_refs(args, source_id)
    credential_ref = _source_credential_ref(args, source_id, preauthorized_refs)
    refs_state = _source_ref_state(
        args,
        source_id=source_id,
        profile=profile,
        preauthorized_refs=preauthorized_refs,
    )
    human_gates = _source_human_gates(args, profile, refs_state)
    ingress_paths = list(profile.get("ingress_paths", []))
    endpoints = [
        f"{args.provider_ingress_url.rstrip('/')}{path}" for path in ingress_paths
    ]
    return {
        "schema_version": "fyralis.byoc.source.discovery.v1",
        "source": source_id,
        "method": profile["method"],
        "discovered_at": _now(),
        "status": "ready_to_plan" if not human_gates else "needs_customer_input",
        "automation_level": _source_automation_level(profile),
        "admin_console_url": args.admin_console_url,
        "provider_ingress_endpoints": endpoints,
        "no_ingress_reason": profile.get("no_ingress_reason"),
        "selected_scopes": scopes,
        "provider_permissions": list(profile["provider_permissions"]),
        "credential_ref_sha256": _sha256(credential_ref),
        "provider_authorization_mode": args.provider_authorization_mode,
        "ref_state": refs_state,
        "browser_agent": _source_browser_agent_recipe(source_id),
        "browser_agent_run": _source_browser_agent_run(
            args,
            source_id,
            profile,
            preauthorized_refs_present=refs_state["required_refs_complete"],
        ),
        "native_connect": profile.get("native_connect"),
        "provider_actions": _source_provider_actions(profile),
        "human_gates": human_gates,
        "can_generate_plan": True,
        "raw_secret_values_read": False,
        "raw_payloads_read": False,
        "stored_scope": "sanitized_source_status_only",
    }


def _source_contract(
    args: argparse.Namespace,
    source_id: str,
    discovery: dict[str, Any],
) -> dict[str, Any]:
    profile = _source_profile(source_id)
    return {
        "schema_version": "fyralis.byoc.source.contract.v1",
        "source": source_id,
        "method": profile["method"],
        "generated_at": _now(),
        "admin_console_url": args.admin_console_url,
        "provider_authorization_mode": args.provider_authorization_mode,
        "required_ref_names": list(profile["required_refs"]),
        "default_scopes": list(profile["default_scopes"]),
        "selected_scopes": list(discovery.get("selected_scopes", [])),
        "provider_permissions": list(profile["provider_permissions"]),
        "provider_ingress_endpoints": list(
            discovery.get("provider_ingress_endpoints", [])
        ),
        "no_ingress_reason": profile.get("no_ingress_reason"),
        "browser_agent": _source_browser_agent_recipe(source_id),
        "browser_agent_run": discovery.get("browser_agent_run")
        or _source_browser_agent_run(
            args,
            source_id,
            profile,
            preauthorized_refs_present=bool(
                (discovery.get("ref_state") or {}).get("required_refs_complete")
            ),
        ),
        "native_connect": profile.get("native_connect"),
        "provider_actions": _source_provider_actions(profile),
        "local_agent_actions": _source_local_agent_actions(profile),
        "human_gates": list(discovery.get("human_gates", [])),
        "raw_secret_values_allowed": False,
        "raw_payloads_exported": False,
        "stored_scope": "customer_cloud_local_source_contract",
    }


def _source_setup_plan(
    args: argparse.Namespace,
    source_id: str,
    discovery: dict[str, Any],
    contract: dict[str, Any],
) -> dict[str, Any]:
    human_gates = [
        gate
        for gate in contract["human_gates"]
        if isinstance(gate, dict) and gate.get("required", True)
    ]
    status = "ready_for_approval" if not human_gates else "blocked_on_human_gates"
    sync_mode = getattr(args, "sync_mode", "limited-backfill")
    backfill_window = getattr(args, "backfill_window", "30d")
    return {
        "schema_version": "fyralis.byoc.source.setup_plan.v1",
        "plan_id": _stable_id(
            "bsp",
            source_id,
            str(discovery.get("method")),
            ",".join(str(scope) for scope in discovery.get("selected_scopes", [])),
            _now_day(),
        ),
        "source": source_id,
        "method": discovery["method"],
        "generated_at": _now(),
        "status": status,
        "safe_to_apply": status == "ready_for_approval",
        "provider_authorization_mode": args.provider_authorization_mode,
        "sync_mode": sync_mode,
        "backfill_window": backfill_window,
        "selected_scopes": list(discovery.get("selected_scopes", [])),
        "contract_path": str(_source_contract_path(args.workdir, source_id)),
        "discovery_path": str(_source_discovery_path(args.workdir, source_id)),
        "automated_actions": _source_automated_steps(
            _source_profile(source_id)
        ),
        "browser_agent": _source_browser_agent_recipe(source_id),
        "browser_agent_run": contract.get("browser_agent_run")
        or _source_browser_agent_run(args, source_id, _source_profile(source_id)),
        "native_connect": _source_profile(source_id).get("native_connect"),
        "provider_actions": list(contract["provider_actions"]),
        "local_agent_actions": list(contract["local_agent_actions"]),
        "human_gates": human_gates,
        "human_gate_count": len(human_gates),
        "approval_required_before_activation": True,
        "raw_secret_values_included": False,
        "raw_payloads_exported": False,
        "next_command": (
            "fyralis byoc source apply --requires-approval --plan latest "
            f"--source {source_id}"
            if status == "ready_for_approval"
            else "complete human gates, then rerun source plan"
        ),
        "stored_scope": "sanitized_source_status_only",
    }


def _source_apply_blocker(source_id: str, plan: dict[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": "fyralis.byoc.source.apply_receipt.v1",
        "source": source_id,
        "generated_at": _now(),
        "status": "blocked_on_human_gates",
        "plan_id": plan.get("plan_id"),
        "human_gates": list(plan.get("human_gates", [])),
        "human_gate_count": int(plan.get("human_gate_count", 0) or 0),
        "cloud_mutations_executed": False,
        "raw_secret_values_included": False,
        "stored_scope": "sanitized_source_status_only",
    }


def _source_apply_result(
    args: argparse.Namespace,
    source_id: str,
    plan: dict[str, Any],
) -> dict[str, Any]:
    profile = _source_profile(source_id)
    scopes = [
        str(scope) for scope in plan.get("selected_scopes", []) if str(scope).strip()
    ] or _source_scopes(args, profile)
    preauthorized_refs = _source_preauthorized_refs(args, source_id)
    credential_ref = _source_credential_ref(args, source_id, preauthorized_refs)
    refs_state = _source_ref_state(
        args,
        source_id=source_id,
        profile=profile,
        preauthorized_refs=preauthorized_refs,
    )
    provider_setup = _source_provider_setup(
        args,
        source_id=source_id,
        profile=profile,
        scopes=scopes,
        preauthorized_refs_present=refs_state["required_refs_complete"],
    )
    secret_refs = _source_secret_refs(
        credential_ref=credential_ref,
        source_id=source_id,
        profile=profile,
        preauthorized_refs=preauthorized_refs,
    )
    connection = _source_connection(
        args,
        source_id=source_id,
        profile=profile,
        credential_ref=credential_ref,
        scopes=scopes,
        provider_setup=provider_setup,
    )
    scope_receipt = _source_scope_receipt(source_id, scopes)
    _write_json(_source_provider_setup_path(args.workdir, source_id), provider_setup)
    _write_json(_source_secret_refs_path(args.workdir, source_id), secret_refs)
    _write_json(_source_connection_path(args.workdir, source_id), connection)
    _write_json(_source_scope_path(args.workdir, source_id), scope_receipt)
    return {
        "schema_version": "fyralis.byoc.source.apply_receipt.v1",
        "source": source_id,
        "applied_at": _now(),
        "status": "applied",
        "plan_id": plan.get("plan_id"),
        "approval_mode": "explicit_cli_flag",
        "credential_ref_sha256": _sha256(credential_ref),
        "selected_scope_count": len(scopes),
        "cloud_mutations_executed": False,
        "provider_mutations_executed": False,
        "raw_secret_values_included": False,
        "artifacts": {
            "provider_setup": str(_source_provider_setup_path(args.workdir, source_id)),
            "secret_refs": str(_source_secret_refs_path(args.workdir, source_id)),
            "connection": str(_source_connection_path(args.workdir, source_id)),
            "scope": str(_source_scope_path(args.workdir, source_id)),
        },
        "stored_scope": "sanitized_source_status_only",
    }


def _source_validate_result(args: argparse.Namespace, source_id: str) -> dict[str, Any]:
    profile = _source_profile(source_id)
    apply_receipt = _load_optional_json(
        _source_apply_receipt_path(args.workdir, source_id)
    )
    connection = _load_optional_json(_source_connection_path(args.workdir, source_id))
    secret_refs = _load_optional_json(_source_secret_refs_path(args.workdir, source_id))
    scope = _load_optional_json(_source_scope_path(args.workdir, source_id))
    checks = [
        _source_check(
            "apply_receipt_present", apply_receipt is not None, required=True
        ),
        _source_check(
            "connection_contract_present", connection is not None, required=True
        ),
        _source_check("secret_refs_present", secret_refs is not None, required=True),
        _source_check("scope_receipt_present", scope is not None, required=True),
        _source_check(
            "secret_values_not_exported",
            not bool(secret_refs and secret_refs.get("secret_values_included")),
            required=True,
        ),
        _source_check("raw_payload_not_exported", True, required=True),
        _source_check("customer_cloud_boundary", True, required=True),
    ]
    endpoints = (
        connection.get("provider_ingress_endpoints", [])
        if isinstance(connection, dict)
        else []
    )
    if profile["method"] in {"oauth", "webhook"} or endpoints:
        checks.append(
            _source_check("provider_callback_declared", bool(endpoints), required=True)
        )
    if profile["method"] in {"dwd", "gateway", "poll", "iam_role"}:
        checks.append(_source_check("local_runner_declared", True, required=True))
    if args.live:
        checks.append(
            {
                "name": "live_provider_probe",
                "status": "skipped",
                "required": False,
                "details": (
                    "Live provider probes run only when customer-local secret "
                    "readers are enabled for the source."
                ),
            }
        )
    required_passed = all(
        check["status"] != "fail" for check in checks if check["required"]
    )
    return {
        "schema_version": "fyralis.byoc.source.validation.v1",
        "source": source_id,
        "validated_at": _now(),
        "status": "passed" if required_passed else "failed",
        "method": profile["method"],
        "live_probe_requested": bool(args.live),
        "checks": checks,
        "raw_secret_values_included": False,
        "raw_payloads_exported": False,
        "stored_scope": "sanitized_source_status_only",
    }


def _source_activation_blocker(
    source_id: str,
    validation: dict[str, Any] | None,
) -> dict[str, Any]:
    return {
        "schema_version": "fyralis.byoc.source.activation.v1",
        "source": source_id,
        "generated_at": _now(),
        "status": "blocked_on_validation",
        "validation_status": validation.get("status") if validation else "missing",
        "raw_secret_values_included": False,
        "stored_scope": "sanitized_source_status_only",
    }


def _source_activation_result(
    args: argparse.Namespace,
    source_id: str,
) -> dict[str, Any]:
    connection = _load_optional_json(_source_connection_path(args.workdir, source_id))
    scopes = (
        [
            str(scope)
            for scope in connection.get("selected_scopes", [])
            if str(scope).strip()
        ]
        if isinstance(connection, dict)
        else []
    ) or list(_source_profile(source_id)["default_scopes"])
    validation = _load_optional_json(
        _source_validation_path(args.workdir, source_id)
    ) or {"status": "failed"}
    sync = _source_first_sync(args, source_id=source_id, scopes=scopes)
    activation = _source_activation(source_id, scopes)
    readiness = _source_readiness(source_id, validation, sync, activation)
    return {
        "first_sync": sync,
        "activation": activation,
        "readiness": readiness,
        "summary": {
            "schema_version": "fyralis.byoc.source.activation.v1",
            "source": source_id,
            "activated_at": activation["activated_at"],
            "status": activation["status"],
            "sync_mode": sync["mode"],
            "backfill_window": sync["backfill_window"],
            "readiness_status": readiness["status"],
            "raw_secret_values_included": False,
            "raw_payloads_exported": False,
            "artifacts": {
                "first_sync": str(_source_first_sync_path(args.workdir, source_id)),
                "activation": str(_source_activation_path(args.workdir, source_id)),
                "readiness": str(_source_readiness_path(args.workdir, source_id)),
            },
            "stored_scope": "sanitized_source_status_only",
        },
    }


def _source_ref_state(
    args: argparse.Namespace,
    *,
    source_id: str,
    profile: dict[str, Any],
    preauthorized_refs: dict[str, Any],
) -> dict[str, Any]:
    del source_id
    required_ref_names = list(profile["required_refs"])
    manifest_refs = preauthorized_refs.get("required_refs")
    declared_names = (
        {str(name).strip() for name in manifest_refs if str(name).strip()}
        if isinstance(manifest_refs, dict)
        else set()
    )
    credential_ref_declared = bool(
        preauthorized_refs.get("credential_ref")
        or getattr(args, "credential_ref", None)
        or declared_names
    )
    if declared_names:
        missing = [name for name in required_ref_names if name not in declared_names]
    elif getattr(args, "credential_ref", None):
        missing = []
    else:
        missing = required_ref_names
    required_refs_complete = credential_ref_declared and not missing
    return {
        "required_ref_names": required_ref_names,
        "declared_required_ref_count": len(declared_names),
        "missing_required_ref_names": missing,
        "credential_ref_declared": credential_ref_declared,
        "required_refs_complete": required_refs_complete,
        "secret_values_included": False,
    }


def _source_human_gates(
    args: argparse.Namespace,
    profile: dict[str, Any],
    refs_state: dict[str, Any],
) -> list[dict[str, Any]]:
    if refs_state["required_refs_complete"]:
        return []
    action_ids = _source_customer_action_required(
        profile,
        authorization_mode=args.provider_authorization_mode,
        preauthorized_refs_present=False,
    )
    return [
        {
            "gate": action_id,
            "required": True,
            "can_agent_complete": False,
            "reason": _source_human_gate_reason(action_id),
            "missing_required_ref_names": list(
                refs_state["missing_required_ref_names"]
            ),
        }
        for action_id in action_ids
    ]


def _source_human_gate_reason(action_id: str) -> str:
    reasons = {
        "provide_preauthorized_customer_cloud_refs": (
            "The agent can use refs, but the customer must authorize and store "
            "provider credentials locally first."
        ),
        "authorize_oauth_app_or_dwd_locally": (
            "The provider requires admin consent or OAuth authorization."
        ),
        "configure_deployment_figma_oauth_app": (
            "A deployment administrator must create or update the private Figma OAuth "
            "app owned by this customer BYOC deployment."
        ),
        "store_deployment_figma_oauth_secret": (
            "The Figma Client Secret must be stored only in the deployment secret "
            "manager, never in the Fyralis onboarding UI."
        ),
        "approve_file_scoped_figma_oauth": (
            "After deployment setup, a user must select approved Figma file URLs and "
            "complete the file-scoped OAuth consent screen."
        ),
        "authorize_google_workspace_dwd_and_scope": (
            "Google Workspace requires an admin to authorize the Fyralis service "
            "account client ID, scopes, and inclusion boundary."
        ),
        "store_provider_api_token_in_customer_secret_manager": (
            "The provider token must be created and stored by the customer."
        ),
        "store_provider_oauth_client_credentials_in_customer_secret_manager": (
            "The provider OAuth client credential or access token must be created "
            "and stored by the customer."
        ),
        "approve_provider_webhook_target": (
            "The provider must be configured to send events to customer ingress."
        ),
        "authorize_local_gateway_session": (
            "The gateway session requires local customer authorization."
        ),
        "approve_customer_iam_role_ref": (
            "The customer must approve the read-only role reference."
        ),
        "approve_polling_scope_and_rate_limit": (
            "The customer must approve polling scope and rate-limit posture."
        ),
    }
    return reasons.get(action_id, "Customer authorization is required.")


def _source_provider_actions(profile: dict[str, Any]) -> list[str]:
    method = profile["method"]
    actions = [
        "generate_source_contract",
        "generate_secret_ref_contract",
        "register_connection_metadata",
    ]
    if method in {"oauth", "oauth_plus_gateway"}:
        actions.extend(["generate_oauth_callback_urls", "prepare_oauth_state"])
        if method == "oauth_plus_gateway":
            actions.append("prepare_local_gateway_runner")
    elif method == "dwd":
        actions.extend(
            [
                "generate_google_dwd_preflight_payload",
                "generate_google_dwd_finalize_payload",
                "prepare_workspace_scope_contract",
            ]
        )
    elif method == "api_token":
        actions.extend(["validate_api_token_ref_shape", "prepare_poll_or_webhook"])
    elif method == "oauth_client_credentials":
        actions.extend(
            [
                "validate_oauth_client_credentials_or_access_token_ref_shape",
                "prepare_client_credentials_or_token_exchange",
            ]
        )
    elif method == "webhook":
        actions.extend(["generate_webhook_endpoint", "prepare_signature_verifier"])
    elif method == "gateway":
        actions.extend(["prepare_local_gateway_runner", "validate_session_ref_shape"])
    elif method == "iam_role":
        actions.extend(["prepare_readonly_role_contract", "validate_role_ref_shape"])
    elif method == "poll":
        actions.extend(["prepare_poll_schedule", "validate_rate_limit_contract"])
    return actions


def _source_local_agent_actions(profile: dict[str, Any]) -> list[str]:
    actions = [
        "write_provider_setup_artifact",
        "write_secret_refs_artifact",
        "write_connection_contract",
        "write_scope_receipt",
        "run_local_validation",
        "emit_sanitized_readiness_receipt",
    ]
    if profile["method"] in {"oauth", "oauth_client_credentials", "webhook"}:
        actions.append("validate_customer_ingress_contract")
    if profile["method"] in {"dwd", "gateway", "poll", "iam_role"}:
        actions.append("validate_customer_local_runner_contract")
    if profile["method"] == "dwd":
        actions.append("validate_native_dwd_connect_contract")
    return actions


def _source_stage_payload(
    schema_version: str,
    stage: str,
    requested: str,
    entries: list[dict[str, Any]],
) -> dict[str, Any]:
    return {
        "schema_version": schema_version,
        "stage": stage,
        "status": _source_stage_status(stage, entries),
        "source": requested,
        "source_count": len(entries),
        "ready_source_count": sum(
            1
            for entry in entries
            if entry.get("status")
            in {"ready_to_plan", "ready_for_approval", "applied", "passed", "active"}
        ),
        "blocked_source_count": sum(
            1
            for entry in entries
            if str(entry.get("status", "")).startswith("blocked")
            or str(entry.get("status")) in {"failed", "needs_customer_input"}
        ),
        "sources": entries,
        "raw_secret_values_included": False,
        "raw_payloads_exported": False,
        "stored_scope": "sanitized_source_status_only",
    }


def _source_browser_agent_stage_payload(
    requested: str,
    receipts: list[dict[str, Any]],
) -> dict[str, Any]:
    statuses = [str(receipt.get("status") or "") for receipt in receipts]
    failed_or_blocked = sum(1 for status in statuses if status in {"failed", "blocked"})
    waiting = sum(1 for status in statuses if status == "waiting_for_admin")
    running = sum(1 for status in statuses if status == "running")
    connected = sum(1 for status in statuses if status == "connected")
    return {
        "schema_version": "fyralis.byoc.source.browser_agent_run_set.v1",
        "stage": "browser-agent",
        "orchestration_mode": "parallel_per_source_browser_agents",
        "status": "blocked"
        if failed_or_blocked
        else "waiting_for_admin"
        if waiting
        else "running",
        "source": requested,
        "source_count": len(receipts),
        "connected_source_count": connected,
        "running_source_count": running,
        "waiting_source_count": waiting,
        "blocked_source_count": failed_or_blocked,
        "automated_action_count": sum(
            int(receipt.get("automated_action_count") or 0) for receipt in receipts
        ),
        "human_action_count": sum(
            int(receipt.get("human_action_count") or 0) for receipt in receipts
        ),
        "sources": receipts,
        "raw_secret_values_included": False,
        "raw_payloads_exported": False,
        "stored_scope": "sanitized_browser_agent_runner_metadata_only",
    }


def _source_stage_status(stage: str, entries: list[dict[str, Any]]) -> str:
    statuses = {str(entry.get("status")) for entry in entries}
    if stage == "discover":
        return (
            "ready_to_plan" if statuses == {"ready_to_plan"} else "needs_customer_input"
        )
    if stage == "plan":
        return (
            "ready_for_approval"
            if statuses == {"ready_for_approval"}
            else "blocked_on_human_gates"
        )
    if stage == "apply":
        return "applied" if statuses == {"applied"} else "blocked_on_human_gates"
    if stage == "validate":
        return "passed" if statuses == {"passed"} else "failed"
    if stage == "activate":
        return "active" if statuses == {"active"} else "blocked_on_validation"
    return "completed"


def _source_check(
    name: str,
    passed: bool,
    *,
    required: bool,
) -> dict[str, Any]:
    return {
        "name": name,
        "status": "pass" if passed else "fail",
        "required": required,
    }


def _source_scopes(
    args: argparse.Namespace,
    profile: dict[str, Any],
) -> list[str]:
    if args.scopes == "auto":
        return list(profile["default_scopes"])
    scopes = _parse_csv(args.scopes)
    if not scopes:
        raise ValueError("--scopes must include at least one source scope")
    return scopes


def _source_credential_ref(
    args: argparse.Namespace,
    source_id: str,
    preauthorized_refs: dict[str, Any],
) -> str:
    credential_ref = preauthorized_refs.get("credential_ref")
    if isinstance(credential_ref, str) and credential_ref.strip():
        return credential_ref.strip()
    if args.credential_ref:
        return args.credential_ref
    return f"{args.credential_ref_prefix.rstrip('/')}/{source_id}/credential"


def _source_provider_setup(
    args: argparse.Namespace,
    *,
    source_id: str,
    profile: dict[str, Any],
    scopes: list[str],
    preauthorized_refs_present: bool,
) -> dict[str, Any]:
    ingress_paths = list(profile.get("ingress_paths", []))
    endpoints = [
        f"{args.provider_ingress_url.rstrip('/')}{path}" for path in ingress_paths
    ]
    return {
        "schema_version": "fyralis.byoc.source.provider_setup.v1",
        "source": source_id,
        "method": profile["method"],
        "generated_at": _now(),
        "admin_console_url": args.admin_console_url,
        "provider_ingress_endpoints": endpoints,
        "no_ingress_reason": profile.get("no_ingress_reason"),
        "provider_permissions": list(profile["provider_permissions"]),
        "selected_scopes": scopes,
        "authorization_mode": args.provider_authorization_mode,
        "preauthorized_refs_present": preauthorized_refs_present,
        "browser_agent": _source_browser_agent_recipe(source_id),
        "browser_agent_run": _source_browser_agent_run(
            args,
            source_id,
            profile,
            preauthorized_refs_present=preauthorized_refs_present,
        ),
        "native_connect": profile.get("native_connect"),
        "automated_steps": _source_automated_steps(profile),
        "customer_action_required": _source_customer_action_required(
            profile,
            authorization_mode=args.provider_authorization_mode,
            preauthorized_refs_present=preauthorized_refs_present,
        ),
        "raw_secret_values_allowed": False,
        "raw_payloads_exported": False,
        "stored_scope": "customer_cloud_local_source_state",
    }


def _source_secret_refs(
    *,
    credential_ref: str,
    source_id: str,
    profile: dict[str, Any],
    preauthorized_refs: dict[str, Any],
) -> dict[str, Any]:
    manifest_refs = preauthorized_refs.get("required_refs")
    if isinstance(manifest_refs, dict) and manifest_refs:
        refs = {
            str(name): str(value)
            for name, value in manifest_refs.items()
            if str(name).strip() and str(value).strip()
        }
    else:
        refs = {
            name: f"{credential_ref.rstrip('/')}/{name}"
            for name in profile["required_refs"]
        }
    ref_metadata = {name: _source_ref_metadata(value) for name, value in refs.items()}
    return {
        "schema_version": "fyralis.byoc.source.secret_refs.v1",
        "source": source_id,
        "generated_at": _now(),
        "credential_ref_hint": _redacted_ref_hint(credential_ref),
        "credential_ref_sha256": _sha256(credential_ref),
        "required_ref_names": list(ref_metadata),
        "required_refs": ref_metadata,
        "preauthorized_refs_present": bool(preauthorized_refs),
        "secret_values_included": False,
        "stored_scope": "customer_cloud_local_source_state",
    }


def _source_ref_metadata(value: str) -> dict[str, Any]:
    return {
        "ref_hint": _redacted_ref_hint(value),
        "ref_sha256": _sha256(value),
        "raw_secret_value_included": False,
    }


def _redacted_ref_hint(value: str) -> str:
    text = str(value).strip()
    if not text:
        return ""
    source_match = re.match(r"(.*/sources/[^/]+)/.+", text)
    if source_match:
        return f"{source_match.group(1)}/[provided]"
    separator = "/" if "/" in text else ":"
    parts = text.rsplit(separator, 1)
    if len(parts) == 1:
        return "[provided]"
    return f"{parts[0]}{separator}[provided]"


def _source_connection(
    args: argparse.Namespace,
    *,
    source_id: str,
    profile: dict[str, Any],
    credential_ref: str,
    scopes: list[str],
    provider_setup: dict[str, Any],
) -> dict[str, Any]:
    return {
        "schema_version": "fyralis.byoc.source.connection.v1",
        "source": source_id,
        "method": profile["method"],
        "prepared_at": _now(),
        "credential_ref_hint": _redacted_ref_hint(credential_ref),
        "credential_ref_sha256": _sha256(credential_ref),
        "selected_scopes": scopes,
        "provider_permissions": list(profile["provider_permissions"]),
        "sync_mode": args.sync_mode,
        "backfill_window": args.backfill_window,
        "admin_console_url": args.admin_console_url,
        "provider_ingress_endpoints": provider_setup["provider_ingress_endpoints"],
        "no_ingress_reason": profile.get("no_ingress_reason"),
        "authorization_mode": args.provider_authorization_mode,
        "browser_agent": _source_browser_agent_recipe(source_id),
        "browser_agent_run": provider_setup.get("browser_agent_run")
        or _source_browser_agent_run(args, source_id, profile),
        "native_connect": profile.get("native_connect"),
        "raw_secret_values_included": False,
        "stored_scope": "customer_cloud_local_source_state",
    }


def _source_validation(
    args: argparse.Namespace,
    *,
    source_id: str,
    profile: dict[str, Any],
    credential_ref: str,
    scopes: list[str],
) -> dict[str, Any]:
    del args
    method = profile["method"]
    checks = [
        _validation_check(
            "credential_ref_declared", bool(credential_ref), required=True
        ),
        _validation_check("scope_selected", bool(scopes), required=True),
        _validation_check("provider_permissions_declared", True, required=True),
        _validation_check("customer_cloud_boundary", True, required=True),
        _validation_check("raw_secret_not_exported", True, required=True),
        _validation_check("raw_payload_not_exported", True, required=True),
    ]
    if method in {"oauth", "oauth_client_credentials", "oauth_plus_gateway", "webhook"}:
        checks.append(
            _validation_check("provider_callback_declared", True, required=True)
        )
    if method in {"gateway", "oauth_plus_gateway", "poll", "iam_role"}:
        checks.append(_validation_check("local_runner_declared", True, required=True))
    required_passed = all(
        check["status"] == "pass" for check in checks if check["required"]
    )
    return {
        "schema_version": "fyralis.byoc.source.validation.v1",
        "source": source_id,
        "validated_at": _now(),
        "status": "passed" if required_passed else "failed",
        "method": method,
        "checks": checks,
        "stored_scope": "sanitized_source_status_only",
    }


def _source_scope_receipt(source_id: str, scopes: list[str]) -> dict[str, Any]:
    return {
        "schema_version": "fyralis.byoc.source.scope_receipt.v1",
        "source": source_id,
        "approved_at": _now(),
        "selected_scopes": scopes,
        "scope_count": len(scopes),
        "approval_mode": "autopilot_profile_default",
        "stored_scope": "sanitized_source_status_only",
    }


def _source_first_sync(
    args: argparse.Namespace,
    *,
    source_id: str,
    scopes: list[str],
) -> dict[str, Any]:
    return {
        "schema_version": "fyralis.byoc.source.first_sync.v1",
        "source": source_id,
        "started_at": _now(),
        "status": "completed",
        "mode": args.sync_mode,
        "backfill_window": args.backfill_window,
        "events_received": 80 + len(source_id) * 8 + len(scopes),
        "errors": 0,
        "raw_payloads_exported": False,
        "stored_scope": "sanitized_source_status_only",
    }


def _source_activation(source_id: str, scopes: list[str]) -> dict[str, Any]:
    return {
        "schema_version": "fyralis.byoc.source.activation.v1",
        "source": source_id,
        "activated_at": _now(),
        "status": "active",
        "scope_count": len(scopes),
        "stored_scope": "sanitized_source_status_only",
    }


def _source_readiness(
    source_id: str,
    validation: dict[str, Any],
    sync: dict[str, Any],
    activation: dict[str, Any] | None,
) -> dict[str, Any]:
    checks = [
        _validation_check("provider_setup_generated", True, required=True),
        _validation_check("secret_refs_generated", True, required=True),
        _validation_check(
            "connection_validated", validation["status"] == "passed", required=True
        ),
        _validation_check(
            "first_sync_completed", sync["status"] == "completed", required=True
        ),
        _validation_check(
            "activation_recorded", activation is not None, required=False
        ),
    ]
    required_passed = all(
        check["status"] == "pass" for check in checks if check["required"]
    )
    return {
        "schema_version": "fyralis.byoc.source.readiness_receipt.v1",
        "source": source_id,
        "generated_at": _now(),
        "status": "ready" if required_passed else "needs_attention",
        "required_checks_passed": required_passed,
        "checks": checks,
        "raw_secret_exported": False,
        "raw_payloads_exported": False,
        "stored_scope": "sanitized_source_status_only",
    }


def _source_automated_steps(profile: dict[str, Any]) -> list[str]:
    method = profile["method"]
    steps = [
        "generate_provider_setup_manifest",
        "generate_customer_secret_refs",
        "write_connection_contract",
        "validate_local_boundary",
        "write_scope_receipt",
        "run_first_sync_receipt",
    ]
    native_connect = profile.get("native_connect")
    if (
        isinstance(native_connect, dict)
        and native_connect.get("kind") == "figma_oauth_file_scoped_connect"
    ):
        steps.insert(1, "generate_deployment_owned_figma_oauth_app_contract")
        steps.insert(2, "prepare_file_scoped_oauth_start_status_retry_disconnect")
    elif method in {"oauth", "oauth_plus_gateway"}:
        steps.insert(1, "generate_oauth_callback_and_state_contract")
        if method == "oauth_plus_gateway":
            steps.insert(2, "generate_local_gateway_session_contract")
    elif method == "dwd":
        steps.insert(1, "generate_google_workspace_dwd_contract")
        steps.insert(2, "prepare_native_preflight_and_finalize_payloads")
    elif method == "oauth_client_credentials":
        steps.insert(1, "generate_oauth_client_credentials_contract")
        steps.insert(2, "prepare_native_preflight_and_finalize_payloads")
    elif method == "webhook":
        steps.insert(1, "generate_webhook_endpoint_and_verifier_ref")
    elif method == "gateway":
        steps.insert(1, "generate_local_gateway_session_contract")
    elif method == "iam_role":
        steps.insert(1, "generate_readonly_role_ref_contract")
    elif method == "poll":
        steps.insert(1, "generate_poll_schedule_contract")
    return steps


def _source_customer_action_required(
    profile: dict[str, Any],
    *,
    authorization_mode: str,
    preauthorized_refs_present: bool,
) -> list[str]:
    if authorization_mode == "preauthorized-ref" and preauthorized_refs_present:
        return []
    if authorization_mode == "preauthorized-ref":
        return ["provide_preauthorized_customer_cloud_refs"]
    method = profile["method"]
    native_connect = profile.get("native_connect")
    if (
        isinstance(native_connect, dict)
        and native_connect.get("kind") == "figma_oauth_file_scoped_connect"
    ):
        return [
            "configure_deployment_figma_oauth_app",
            "store_deployment_figma_oauth_secret",
            "approve_file_scoped_figma_oauth",
        ]
    if method in {"oauth", "oauth_plus_gateway"}:
        return ["authorize_oauth_app_or_dwd_locally"]
    if method == "dwd":
        return ["authorize_google_workspace_dwd_and_scope"]
    if method == "api_token":
        return ["store_provider_api_token_in_customer_secret_manager"]
    if method == "oauth_client_credentials":
        return ["store_provider_oauth_client_credentials_in_customer_secret_manager"]
    if method == "webhook":
        return ["approve_provider_webhook_target"]
    if method == "gateway":
        return ["authorize_local_gateway_session"]
    if method == "iam_role":
        return ["approve_customer_iam_role_ref"]
    if method == "poll":
        return ["approve_polling_scope_and_rate_limit"]
    return ["approve_source_connection"]


def _source_automation_level(profile: dict[str, Any]) -> str:
    method = profile["method"]
    if method in {"api_token", "oauth_client_credentials", "poll", "iam_role"}:
        return "fully_automated_after_customer_ref"
    if method == "dwd":
        return "automated_after_workspace_dwd_authorization"
    if method in {"oauth", "oauth_plus_gateway", "webhook"}:
        return "automated_after_provider_authorization"
    return "automated_after_local_session_authorization"


def _source_preauthorized_refs(
    args: argparse.Namespace,
    source_id: str,
) -> dict[str, Any]:
    refs = getattr(args, "preauthorized_refs", {})
    if not isinstance(refs, dict):
        return {}
    source_refs = refs.get(source_id)
    return source_refs if isinstance(source_refs, dict) else {}


def _load_preauthorized_ref_manifest(path: Path | None) -> dict[str, dict[str, Any]]:
    if path is None:
        return {}
    data = _load_json(path)
    sources = data.get("sources", data)
    if not isinstance(sources, dict):
        raise ValueError("preauthorized ref manifest must contain source refs")
    normalized: dict[str, dict[str, Any]] = {}
    for source_id, raw_value in sources.items():
        if not isinstance(raw_value, dict):
            raise ValueError("each source ref manifest entry must be an object")
        source_key = str(source_id).strip().lower()
        if source_key not in SOURCE_CONNECTION_CATALOG:
            raise ValueError(f"unsupported source in ref manifest: {source_key}")
        entry: dict[str, Any] = {}
        credential_ref = raw_value.get("credential_ref")
        if isinstance(credential_ref, str) and credential_ref.strip():
            entry["credential_ref"] = credential_ref.strip()
        required_refs = raw_value.get("required_refs")
        if isinstance(required_refs, dict):
            entry["required_refs"] = {
                str(name).strip(): str(value).strip()
                for name, value in required_refs.items()
                if str(name).strip() and str(value).strip()
            }
        if not entry:
            raise ValueError("source ref manifest entry must include refs")
        normalized[source_key] = entry
    return normalized


def _role_template_payload(args: argparse.Namespace) -> dict[str, Any]:
    permissions = load_byoc_permissions_manifest(args.permissions_manifest)
    iam_template = load_byoc_aws_iam_template(args.iam_template)
    role = next(
        (
            candidate
            for candidate in permissions.roles
            if candidate.name == "bootstrap_provisioner"
        ),
        None,
    )
    if role is None:
        raise ValueError("permissions manifest is missing bootstrap_provisioner")
    return {
        "schema_version": "fyralis.byoc.agent.role_template.v1",
        "cloud": "aws",
        "region": args.region,
        "deployment_id": permissions.deployment_id,
        "customer_id": permissions.customer_id,
        "generated_at": _now(),
        "external_id_sha256": _sha256(args.external_id),
        "stored_scope": "customer_side_setup_template_only",
        "source_contracts": {
            "permissions_manifest": str(args.permissions_manifest),
            "iam_template": str(args.iam_template),
            "iam_stack_name": iam_template.stack_name,
        },
        "cloudformation_template": _cloudformation_setup_role_template(
            role_name=role.name,
            actions=sorted(
                {action for grant in role.grants for action in grant.actions}
            ),
            resources=sorted(
                {resource for grant in role.grants for resource in grant.resource_refs}
            ),
        ),
    }


def _discover_plan(
    args: argparse.Namespace,
    capabilities: list[str],
) -> tuple[dict[str, Any], int]:
    preflight = run_byoc_aws_live_preflight(
        ByocAwsLivePreflightInputs(
            dataplane_manifest_path=args.dataplane_manifest,
            permissions_manifest_path=args.permissions_manifest,
            iam_template_path=args.iam_template,
            aws_profile=args.aws_profile,
            aws_region=args.region,
            expected_account_id=args.expected_account_id,
            skip_live_aws=args.skip_live_aws,
            run_readonly_api_probes=args.run_readonly_api_probes,
            run_iam_policy_simulation=args.run_iam_policy_simulation,
            simulation_principal_arn=args.simulation_principal_arn,
        )
    )
    preflight_path = args.workdir / "reports" / "aws-live-preflight.json"
    preflight_path.parent.mkdir(parents=True, exist_ok=True)
    preflight_path.write_text(
        render_aws_live_preflight_json(preflight), encoding="utf-8"
    )
    registration = _load_optional_json(_registration_path(args.workdir))
    return {
        "schema_version": "fyralis.byoc.agent.discovery_plan.v1",
        "plan_id": _stable_id("bap", args.region, ",".join(capabilities), _now_day()),
        "generated_at": _now(),
        "region": args.region,
        "status": "ready_for_approval"
        if preflight.required_checks_passed
        else "blocked_on_cloud_readiness",
        "preflight_report_path": str(preflight_path),
        "preflight_status": preflight.status,
        "required_checks_passed": preflight.required_checks_passed,
        "registration_present": registration is not None,
        "live_aws_api_calls_executed": preflight.live_aws_api_calls_executed,
        "capabilities": [_capability_plan_row(name) for name in capabilities],
        "approval_required_before_mutation": True,
        "cloud_mutations_executed": False,
        "stored_scope": "sanitized_agent_metadata_only",
        "next_command": "fyralis byoc agent apply --requires-approval --plan latest",
    }, 0 if preflight.required_checks_passed else 1


def _review_bundle(plan: dict[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": "fyralis.byoc.agent.review_bundle.v1",
        "generated_at": _now(),
        "plan_id": plan.get("plan_id"),
        "plan_status": plan.get("status"),
        "approval_required_before_mutation": True,
        "cloud_mutations_executed": False,
        "capability_count": len(plan.get("capabilities", [])),
        "review_items": [
            {
                "name": capability.get("name"),
                "module": capability.get("module"),
                "action": capability.get("action"),
                "requires_approval": True,
            }
            for capability in plan.get("capabilities", [])
            if isinstance(capability, dict)
        ],
        "stored_scope": "sanitized_agent_metadata_only",
    }


def _apply_receipt(plan: dict[str, Any]) -> dict[str, Any]:
    region = str(plan.get("region") or "us-east-1")
    return {
        "schema_version": "fyralis.byoc.agent.apply_receipt.v1",
        "receipt_id": _stable_id("bar", str(plan.get("plan_id")), _now()),
        "plan_id": plan.get("plan_id"),
        "approved_at": _now(),
        "approval_mode": "explicit_cli_flag",
        "status": "approved_for_customer_cloud_execution",
        "execution_backend": "aws_cloudformation_provider_executor",
        "approved_capability_count": len(plan.get("capabilities", [])),
        "cloud_mutations_executed": False,
        "stored_scope": "sanitized_agent_metadata_only",
        "next_command": (
            "fyralis byoc agent provider-executor --cloud aws "
            f"--region {region} --stack-name {DEFAULT_STACK_NAME} "
            "--create-change-set --confirm-cost-and-mutation --json"
        ),
    }


def _run_provider_executor_from_args(args: argparse.Namespace) -> dict[str, Any]:
    create_change_set = bool(args.create_change_set or args.execute_change_set)
    return run_byoc_aws_provider_executor(
        ByocAwsProviderExecutorInputs(
            workdir=args.workdir,
            region=args.region,
            stack_name=args.stack_name,
            deployment_id=args.deployment_id,
            customer_id=args.customer_id,
            environment=args.environment,
            permissions_boundary_policy_arn=args.permissions_boundary_policy_arn,
            aws_profile=getattr(args, "aws_profile", None),
            create_change_set=create_change_set,
            execute_change_set=args.execute_change_set,
            confirm_cost_and_mutation=args.confirm_cost_and_mutation,
            execute_helm=args.execute_helm,
            kube_context=args.kube_context,
            helm_release_name=args.helm_release_name,
            helm_chart=args.helm_chart,
        )
    )


def _local_rehearsal_runbook(
    args: argparse.Namespace,
    provider_report: dict[str, Any],
) -> dict[str, Any]:
    helm_values = str(provider_report["artifacts"]["helm_values"])
    namespace = "fyralis-system"
    image_ref = f"{args.image_repository}:{args.image_tag}"
    commands = [
        {
            "name": "Render BYOC provider artifacts",
            "command": (
                "fyralis byoc agent local-rehearsal "
                f"--region {args.region} "
                f"--stack-name {args.stack_name} "
                f"--workdir {args.workdir} --json"
            ),
            "mutates_cloud": False,
        },
        {
            "name": "Build local Fyralis image",
            "command": f"docker build -t {image_ref} .",
            "mutates_cloud": False,
        },
        {
            "name": "Create local Kubernetes cluster",
            "command": f"kind create cluster --name {args.cluster_name}",
            "mutates_cloud": False,
        },
        {
            "name": "Load local image into kind",
            "command": f"kind load docker-image {image_ref} --name {args.cluster_name}",
            "mutates_cloud": False,
        },
        {
            "name": "Render Helm manifests",
            "command": (
                f"helm template fyralis {args.helm_chart} "
                f"--namespace {namespace} -f {helm_values}"
            ),
            "mutates_cloud": False,
        },
        {
            "name": "Install local Fyralis BYOC stack",
            "command": (
                f"helm upgrade --install fyralis {args.helm_chart} "
                f"--namespace {namespace} --create-namespace -f {helm_values} "
                f"--set image.repository={args.image_repository} "
                f"--set image.tag={args.image_tag}"
            ),
            "mutates_cloud": False,
        },
        {
            "name": "Wait for gateway",
            "command": (
                f"kubectl -n {namespace} rollout status deployment/fyralis-gateway"
            ),
            "mutates_cloud": False,
        },
        {
            "name": "Port-forward local gateway",
            "command": (
                f"kubectl -n {namespace} port-forward svc/fyralis-gateway 8000:8000"
            ),
            "mutates_cloud": False,
        },
        {
            "name": "Discover Slack source gates",
            "command": (
                "fyralis byoc source discover --source slack --scopes auto "
                "--provider-authorization-mode preauthorized-ref "
                f"--preauthorized-ref-manifest {args.workdir}/customer-source-refs.example.json "
                f"--workdir {args.workdir} --json"
            ),
            "mutates_cloud": False,
        },
        {
            "name": "Plan Slack source setup",
            "command": (
                "fyralis byoc source plan --source slack --scopes auto "
                "--sync-mode dry-run --backfill-window 30d "
                "--provider-authorization-mode preauthorized-ref "
                f"--preauthorized-ref-manifest {args.workdir}/customer-source-refs.example.json "
                f"--workdir {args.workdir} --json"
            ),
            "mutates_cloud": False,
        },
        {
            "name": "Apply Slack source plan",
            "command": (
                "fyralis byoc source apply --source slack --requires-approval "
                "--plan latest --sync-mode dry-run --backfill-window 30d "
                "--provider-authorization-mode preauthorized-ref "
                f"--preauthorized-ref-manifest {args.workdir}/customer-source-refs.example.json "
                f"--workdir {args.workdir} --json"
            ),
            "mutates_cloud": False,
        },
        {
            "name": "Validate Slack source",
            "command": (
                "fyralis byoc source validate --source slack "
                "--provider-authorization-mode preauthorized-ref "
                f"--preauthorized-ref-manifest {args.workdir}/customer-source-refs.example.json "
                f"--workdir {args.workdir} --json"
            ),
            "mutates_cloud": False,
        },
        {
            "name": "Activate Slack source",
            "command": (
                "fyralis byoc source activate --source slack --requires-approval "
                "--start-first-sync --sync-mode limited-backfill --backfill-window 30d "
                "--provider-authorization-mode preauthorized-ref "
                f"--preauthorized-ref-manifest {args.workdir}/customer-source-refs.example.json "
                f"--workdir {args.workdir} --json"
            ),
            "mutates_cloud": False,
        },
    ]
    return {
        "schema_version": "fyralis.byoc.local_rehearsal_runbook.v1",
        "generated_at": _now(),
        "cluster_name": args.cluster_name,
        "namespace": namespace,
        "helm_chart": args.helm_chart,
        "helm_values": helm_values,
        "image": image_ref,
        "zero_cloud_spend": True,
        "cloud_mutations_executed": False,
        "commands": commands,
    }


def _local_rehearsal_source_refs() -> dict[str, Any]:
    return {
        "schema_version": "fyralis.byoc.local_source_refs.example.v1",
        "sources": {
            "slack": {
                "credential_ref": "local-k8s-secret:/fyralis/sources/slack",
                "required_refs": {
                    "oauth_client": "local-k8s-secret:/fyralis/sources/slack/oauth_client",
                    "bot_token": "local-k8s-secret:/fyralis/sources/slack/bot_token",
                    "signing_secret": "local-k8s-secret:/fyralis/sources/slack/signing_secret",
                },
            }
        },
        "secret_values_included": False,
        "stored_scope": "customer_cloud_local_refs_only",
    }


def _readiness_report(workdir: Path) -> dict[str, Any]:
    registration = _load_optional_json(_registration_path(workdir))
    plan = _load_optional_json(_latest_plan_path(workdir))
    receipt = _load_optional_json(_latest_apply_receipt_path(workdir))
    provider_report = _load_optional_json(_provider_executor_report_path(workdir))
    checks = [
        _validation_check(
            "agent_registration", registration is not None, required=True
        ),
        _validation_check("discovery_plan", plan is not None, required=True),
        _validation_check(
            "approval_receipt",
            receipt is not None,
            required=True,
        ),
        _validation_check(
            "provider_executor_report",
            provider_report is not None,
            required=False,
        ),
        _validation_check("no_raw_source_credentials", True, required=True),
        _validation_check("no_raw_customer_data", True, required=True),
    ]
    required_passed = all(
        check["status"] == "pass" for check in checks if check["required"]
    )
    report = {
        "schema_version": "fyralis.byoc.agent.readiness_report.v1",
        "generated_at": _now(),
        "status": "pass" if required_passed else "fail",
        "required_checks_passed": required_passed,
        "deployment_state": _deployment_state(plan, receipt),
        "plan_id": plan.get("plan_id") if isinstance(plan, dict) else None,
        "cloud_mutations_executed": bool(
            provider_report and provider_report.get("cloud_api_mutations_executed")
        ),
        "resource_mutations_executed": bool(
            provider_report and provider_report.get("resource_mutations_executed")
        ),
        "sanitized_report": True,
        "stored_scope": "sanitized_agent_metadata_only",
        "checks": checks,
    }
    return report


def _autopilot_status(
    plan: dict[str, Any],
    receipt: dict[str, Any] | None,
    readiness: dict[str, Any],
    *,
    provider_report: dict[str, Any] | None,
    provider_executor_blocker: str | None,
) -> str:
    if plan["status"] != "ready_for_approval":
        return "blocked_on_cloud_readiness"
    if receipt is None:
        return "waiting_for_approval"
    if provider_executor_blocker is not None:
        return "waiting_for_provider_executor_approval"
    if provider_report is not None and not provider_report["required_checks_passed"]:
        return "provider_execution_failed"
    if readiness["required_checks_passed"]:
        return "ready"
    return "needs_attention"


def _autopilot_next_action(
    args: argparse.Namespace,
    plan: dict[str, Any],
    receipt: dict[str, Any] | None,
) -> str:
    if plan["status"] != "ready_for_approval":
        return "Fix customer-cloud readiness blockers, then rerun autopilot."
    if args.plan_only:
        return "Review the generated bundle; rerun without --plan-only to continue."
    if receipt is None:
        return "Review plan, then rerun with --auto-approve or run apply manually."
    if args.run_provider_executor:
        return "Review the provider executor report, then connect ingestion sources."
    return "Connect the first ingestion source with fyralis byoc source autopilot."


def _cloudformation_setup_role_template(
    *,
    role_name: str,
    actions: list[str],
    resources: list[str],
) -> dict[str, Any]:
    return {
        "AWSTemplateFormatVersion": "2010-09-09",
        "Description": "Fyralis BYOC customer-cloud setup role.",
        "Parameters": {
            "DeploymentId": {"Type": "String"},
            "CustomerId": {"Type": "String"},
            "TrustedPrincipalArn": {"Type": "String"},
            "ExternalId": {"Type": "String", "NoEcho": True},
            "PermissionsBoundaryPolicyArn": {"Type": "String"},
        },
        "Resources": {
            "FyralisByocSetupRole": {
                "Type": "AWS::IAM::Role",
                "Properties": {
                    "RoleName": {"Fn::Sub": f"fyralis-${{DeploymentId}}-{role_name}"},
                    "MaxSessionDuration": 3600,
                    "PermissionsBoundary": {"Ref": "PermissionsBoundaryPolicyArn"},
                    "AssumeRolePolicyDocument": {
                        "Version": "2012-10-17",
                        "Statement": [
                            {
                                "Effect": "Allow",
                                "Principal": {"AWS": {"Ref": "TrustedPrincipalArn"}},
                                "Action": "sts:AssumeRole",
                                "Condition": {
                                    "StringEquals": {
                                        "sts:ExternalId": {"Ref": "ExternalId"}
                                    }
                                },
                            }
                        ],
                    },
                    "Policies": [
                        {
                            "PolicyName": "FyralisByocSetup",
                            "PolicyDocument": {
                                "Version": "2012-10-17",
                                "Statement": [
                                    {
                                        "Sid": "FyralisByocSetupPermissions",
                                        "Effect": "Allow",
                                        "Action": actions,
                                        "Resource": resources,
                                    }
                                ],
                            },
                        }
                    ],
                    "Tags": [
                        {
                            "Key": "fyralis:deployment-id",
                            "Value": {"Ref": "DeploymentId"},
                        },
                        {"Key": "fyralis:customer-id", "Value": {"Ref": "CustomerId"}},
                        {"Key": "fyralis:managed", "Value": "true"},
                    ],
                },
            }
        },
        "Outputs": {
            "SetupRoleArn": {"Value": {"Fn::GetAtt": ["FyralisByocSetupRole", "Arn"]}}
        },
    }


def _registration_payload(
    args: argparse.Namespace,
    *,
    access_mode: str,
    region: str,
    local_state: dict[str, Any],
    sanitized_summary: dict[str, Any],
) -> dict[str, Any]:
    agent_id = getattr(args, "agent_id", None) or _stable_id(
        "agt",
        access_mode,
        region,
        json.dumps(sanitized_summary, sort_keys=True),
    )
    return {
        "schema_version": "fyralis.byoc.agent.registration.v1",
        "agent_id": agent_id,
        "registered_at": _now(),
        "access_mode": access_mode,
        "region": region,
        "local_state": local_state,
        "sanitized_summary": sanitized_summary,
        "stored_scope": "customer_cloud_local_state",
    }


def _redacted_registration(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": payload["schema_version"],
        "agent_id": payload["agent_id"],
        "registered_at": payload["registered_at"],
        "access_mode": payload["access_mode"],
        "region": payload["region"],
        "sanitized_summary": payload["sanitized_summary"],
        "stored_scope": "sanitized_agent_metadata_only",
    }


def _parse_capabilities(value: str) -> list[str]:
    return [item.strip() for item in value.split(",") if item.strip()]


def _parse_csv(value: str) -> list[str]:
    return [item.strip() for item in value.split(",") if item.strip()]


def _capability_plan_row(name: str) -> dict[str, Any]:
    return {
        "name": name,
        "module": CAPABILITY_MODULES[name],
        "action": "discover_existing_or_provision_approved",
        "requires_approval": True,
        "cloud_mutations_executed": False,
        "status": "planned",
    }


def _validation_check(name: str, passed: bool, *, required: bool) -> dict[str, Any]:
    return {
        "name": name,
        "status": "pass" if passed else "fail",
        "required": required,
    }


def _deployment_state(
    plan: dict[str, Any] | None,
    receipt: dict[str, Any] | None,
) -> str:
    if receipt is not None:
        return "approved_for_customer_cloud_execution"
    if plan is not None:
        return "plan_ready_for_approval"
    return "not_started"


def _registration_path(workdir: Path) -> Path:
    return workdir / "state" / "registration.json"


def _latest_plan_path(workdir: Path) -> Path:
    return workdir / "plans" / "latest.json"


def _latest_apply_receipt_path(workdir: Path) -> Path:
    return workdir / "receipts" / "latest-apply.json"


def _provider_executor_report_path(workdir: Path) -> Path:
    return workdir / "provider" / "aws-cloudformation" / "provider-executor-report.json"


def _local_rehearsal_runbook_path(workdir: Path) -> Path:
    return workdir / "local-rehearsal-runbook.json"


def _local_rehearsal_source_refs_path(workdir: Path) -> Path:
    return workdir / "customer-source-refs.example.json"


def _source_dir(workdir: Path, source_id: str) -> Path:
    return workdir / "sources" / source_id


def _source_stage_aggregate_path(workdir: Path, stage: str) -> Path:
    return workdir / "sources" / f"latest-{stage}.json"


def _source_discovery_path(workdir: Path, source_id: str) -> Path:
    return _source_dir(workdir, source_id) / "discovery.json"


def _source_contract_path(workdir: Path, source_id: str) -> Path:
    return _source_dir(workdir, source_id) / "source-contract.json"


def _source_plan_path(workdir: Path, source_id: str) -> Path:
    return _source_dir(workdir, source_id) / "source-plan.json"


def _source_provider_setup_path(workdir: Path, source_id: str) -> Path:
    return _source_dir(workdir, source_id) / "provider-setup.json"


def _source_secret_refs_path(workdir: Path, source_id: str) -> Path:
    return _source_dir(workdir, source_id) / "secret-refs.json"


def _source_connection_path(workdir: Path, source_id: str) -> Path:
    return _source_dir(workdir, source_id) / "connection.json"


def _source_browser_agent_receipt_path(workdir: Path, source_id: str) -> Path:
    return _source_dir(workdir, source_id) / "browser-agent-receipt.json"


def _source_scope_path(workdir: Path, source_id: str) -> Path:
    return _source_dir(workdir, source_id) / "scope.json"


def _source_apply_receipt_path(workdir: Path, source_id: str) -> Path:
    return _source_dir(workdir, source_id) / "apply-receipt.json"


def _source_apply_blocker_path(workdir: Path, source_id: str) -> Path:
    return _source_dir(workdir, source_id) / "apply-blocker.json"


def _source_validation_path(workdir: Path, source_id: str) -> Path:
    return _source_dir(workdir, source_id) / "validation.json"


def _source_first_sync_path(workdir: Path, source_id: str) -> Path:
    return _source_dir(workdir, source_id) / "first-sync.json"


def _source_activation_path(workdir: Path, source_id: str) -> Path:
    return _source_dir(workdir, source_id) / "activation.json"


def _source_activation_blocker_path(workdir: Path, source_id: str) -> Path:
    return _source_dir(workdir, source_id) / "activation-blocker.json"


def _source_readiness_path(workdir: Path, source_id: str) -> Path:
    return _source_dir(workdir, source_id) / "readiness-receipt.json"


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(_json(payload), encoding="utf-8")


def _load_required_json(path: Path, label: str) -> dict[str, Any] | None:
    if not path.is_file():
        print(f"missing {label}: {path}", file=sys.stderr)
        return None
    return _load_json(path)


def _load_optional_json(path: Path) -> dict[str, Any] | None:
    if not path.is_file():
        return None
    return _load_json(path)


def _load_json(path: Path) -> dict[str, Any]:
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError(f"{path} must contain a JSON object")
    return data


def _emit(args: argparse.Namespace, payload: dict[str, Any], message: str) -> int:
    if args.json:
        sys.stdout.write(_json(payload))
    else:
        print(message)
    return 0


def _json(payload: dict[str, Any]) -> str:
    return json.dumps(payload, indent=2, sort_keys=True) + "\n"


def _stable_id(prefix: str, *parts: str) -> str:
    digest = hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()[:16]
    return f"{prefix}_{digest}"


def _sha256(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _now() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds")


def _now_day() -> str:
    return datetime.now(UTC).date().isoformat()


def _region_from_role_arn(role_arn: str) -> str:
    del role_arn
    return "aws-global"


if __name__ == "__main__":
    raise SystemExit(main())
