#!/usr/bin/env python3
"""Sign and optionally run BYOC read-only control-plane smoke checks."""
from __future__ import annotations

import argparse
import json
import os
import secrets
import sys
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Sequence

from pydantic import BaseModel, ValidationError

from services.platform.runtime.byoc_agent_control_plane import (
    ByocAgentFleetList,
    ByocAgentFleetQuery,
)
from services.platform.runtime.byoc_control_plane_intake import (
    ByocEvidencePackageReceiptList,
    ByocEvidencePackageReceiptQuery,
    signed_evidence_receipt_read_headers,
)
from services.platform.runtime.byoc_control_panel_state import (
    ByocControlPanelState,
    ByocControlPanelStateQuery,
)
from services.platform.runtime.byoc_deployment_overview import (
    ByocDeploymentOverview,
    ByocDeploymentOverviewQuery,
)
from services.platform.runtime.byoc_preflight_intake import (
    ByocPreflightReportReceiptList,
    ByocPreflightReportReceiptQuery,
)
from services.platform.runtime.byoc_runner_evidence_intake import (
    ByocRunnerEvidenceReceiptList,
    ByocRunnerEvidenceReceiptQuery,
)


DEFAULT_SIGNING_SECRET_ENV = "FYRALIS_BYOC_EVIDENCE_READ_SIGNING_KEY"
SCHEMA_VERSION = "fyralis.byoc.control_plane_read_smoke.v1"


@dataclass(frozen=True, slots=True)
class _ReadSurface:
    name: str
    path: str
    query: str


def _parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--deployment-id", help="BYOC deployment id.")
    parser.add_argument("--customer-id", help="Optional BYOC customer id bound.")
    parser.add_argument("--limit", type=int, default=20, help="List limit, 1-100.")
    parser.add_argument(
        "--control-panel-recent-limit",
        type=int,
        default=10,
        help="Control-panel aggregate recent receipt limit, 1-20.",
    )
    parser.add_argument(
        "--signing-secret-env",
        default=DEFAULT_SIGNING_SECRET_ENV,
        help="Environment variable containing local read signing-key material.",
    )
    parser.add_argument("--key-ref", help="Control-plane receipt/read key reference.")
    parser.add_argument(
        "--nonce-prefix",
        help="Nonce prefix. Generated when omitted; surface names are appended.",
    )
    parser.add_argument(
        "--timestamp",
        type=str,
        help="ISO timestamp to use when signing every read.",
    )
    parser.add_argument(
        "--base-url",
        help=(
            "Optional control-plane base URL. Omit to print signed GET requests "
            "without network access."
        ),
    )
    parser.add_argument("--timeout-seconds", type=float, default=10.0)
    parser.add_argument(
        "--schema",
        action="store_true",
        help="Print the control-plane read smoke schema bundle and exit.",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(list(sys.argv[1:] if argv is None else argv))
    if args.schema:
        print(json.dumps(_schema_bundle(), indent=2, sort_keys=True))
        return 0
    if not args.key_ref:
        _print_errors("BYOC control-plane read smoke failed", ["--key-ref is required"])
        return 2
    if args.timeout_seconds <= 0:
        _print_errors(
            "BYOC control-plane read smoke failed",
            ["--timeout-seconds must be positive"],
        )
        return 2
    signing_secret = os.environ.get(args.signing_secret_env, "")
    if not signing_secret.strip():
        _print_errors(
            "BYOC control-plane read smoke failed",
            [f"{args.signing_secret_env} must contain signing-key material"],
        )
        return 2

    try:
        surfaces = _read_surfaces(
            deployment_id=args.deployment_id,
            customer_id=args.customer_id,
            limit=args.limit,
            control_panel_recent_limit=args.control_panel_recent_limit,
        )
        timestamp = _parse_timestamp(args.timestamp)
        nonce_prefix = args.nonce_prefix or _nonce_prefix()
        signed_requests = {
            surface.name: {
                "method": "GET",
                "path": surface.path,
                "query": surface.query,
                "headers": signed_evidence_receipt_read_headers(
                    method="GET",
                    path=surface.path,
                    query=surface.query,
                    signing_secret=signing_secret,
                    key_ref=args.key_ref,
                    nonce=_nonce(surface.name, nonce_prefix=nonce_prefix),
                    timestamp=timestamp,
                ),
            }
            for surface in surfaces
        }
    except (ValidationError, TypeError, ValueError) as exc:
        _print_errors(
            "BYOC control-plane read smoke failed",
            [f"{type(exc).__name__}: {exc}"],
        )
        return 1
    except Exception as exc:  # noqa: BLE001
        _print_errors(
            "BYOC control-plane read smoke failed",
            [f"{type(exc).__name__}: {exc}"],
        )
        return 1

    if not args.base_url:
        print(
            json.dumps(
                {
                    "schema_version": SCHEMA_VERSION,
                    "mode": "signed_requests",
                    "deployment_id": args.deployment_id,
                    "customer_id": args.customer_id,
                    "limit": args.limit,
                    "control_panel_recent_limit": args.control_panel_recent_limit,
                    "requests": signed_requests,
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 0

    try:
        responses = {
            surface.name: {
                "path": surface.path,
                "query": surface.query,
                "response": _get_json(
                    _url_for(args.base_url, surface.path, surface.query),
                    signed_requests[surface.name]["headers"],
                    timeout_seconds=args.timeout_seconds,
                ),
            }
            for surface in surfaces
        }
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        _print_errors(
            "BYOC control-plane read smoke was rejected",
            [f"HTTP {exc.code}: {body}"],
        )
        return 1
    except urllib.error.URLError as exc:
        _print_errors(
            "BYOC control-plane read smoke was unreachable",
            [str(exc.reason)],
        )
        return 1
    except ValueError as exc:
        _print_errors("BYOC control-plane read smoke failed", [str(exc)])
        return 1

    print(
        json.dumps(
            {
                "schema_version": SCHEMA_VERSION,
                "mode": "executed",
                "deployment_id": args.deployment_id,
                "customer_id": args.customer_id,
                "limit": args.limit,
                "control_panel_recent_limit": args.control_panel_recent_limit,
                "responses": responses,
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


def _read_surfaces(
    *,
    deployment_id: str | None,
    customer_id: str | None,
    limit: int,
    control_panel_recent_limit: int,
) -> tuple[_ReadSurface, ...]:
    fleet_query = ByocAgentFleetQuery(
        deployment_id=deployment_id,
        customer_id=customer_id,
        limit=limit,
    )
    overview_query = ByocDeploymentOverviewQuery(
        deployment_id=deployment_id,
        customer_id=customer_id,
    )
    control_panel_query = ByocControlPanelStateQuery(
        deployment_id=deployment_id,
        customer_id=customer_id,
        recent_limit=control_panel_recent_limit,
    )
    evidence_query = ByocEvidencePackageReceiptQuery(
        deployment_id=deployment_id,
        customer_id=customer_id,
        limit=limit,
    )
    preflight_query = ByocPreflightReportReceiptQuery(
        deployment_id=deployment_id,
        customer_id=customer_id,
        limit=limit,
    )
    runner_query = ByocRunnerEvidenceReceiptQuery(
        deployment_id=deployment_id,
        customer_id=customer_id,
        limit=limit,
    )
    return (
        _ReadSurface(
            name="agent_fleet",
            path="/byoc/control-plane/agents",
            query=_query_string(fleet_query),
        ),
        _ReadSurface(
            name="deployment_overview",
            path="/byoc/control-plane/deployment-overview",
            query=_query_string(overview_query),
        ),
        _ReadSurface(
            name="control_panel_state",
            path="/byoc/control-plane/control-panel-state",
            query=_query_string(control_panel_query),
        ),
        _ReadSurface(
            name="evidence_packages",
            path="/byoc/control-plane/evidence-packages",
            query=_query_string(evidence_query),
        ),
        _ReadSurface(
            name="preflight_reports",
            path="/byoc/control-plane/preflight-reports",
            query=_query_string(preflight_query),
        ),
        _ReadSurface(
            name="runner_evidence",
            path="/byoc/control-plane/runner-evidence",
            query=_query_string(runner_query),
        ),
    )


def _schema_bundle() -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "agent_fleet_query": ByocAgentFleetQuery.model_json_schema(),
        "agent_fleet_list": ByocAgentFleetList.model_json_schema(),
        "deployment_overview_query": ByocDeploymentOverviewQuery.model_json_schema(),
        "deployment_overview": ByocDeploymentOverview.model_json_schema(),
        "control_panel_state_query": ByocControlPanelStateQuery.model_json_schema(),
        "control_panel_state": ByocControlPanelState.model_json_schema(),
        "evidence_package_receipt_query": (
            ByocEvidencePackageReceiptQuery.model_json_schema()
        ),
        "evidence_package_receipt_list": (
            ByocEvidencePackageReceiptList.model_json_schema()
        ),
        "preflight_report_receipt_query": (
            ByocPreflightReportReceiptQuery.model_json_schema()
        ),
        "preflight_report_receipt_list": (
            ByocPreflightReportReceiptList.model_json_schema()
        ),
        "runner_evidence_receipt_query": (
            ByocRunnerEvidenceReceiptQuery.model_json_schema()
        ),
        "runner_evidence_receipt_list": (
            ByocRunnerEvidenceReceiptList.model_json_schema()
        ),
    }


def _query_string(query: BaseModel) -> str:
    params = [
        (key, str(value))
        for key, value in query.model_dump().items()
        if value is not None
    ]
    return urllib.parse.urlencode(params)


def _url_for(base_url: str, path: str, query: str) -> str:
    parsed = urllib.parse.urlsplit(base_url)
    if parsed.query or parsed.fragment:
        raise ValueError("--base-url must not include a query string or fragment")
    base_path = parsed.path.rstrip("/")
    full_path = f"{base_path}{path}"
    return urllib.parse.urlunsplit(
        (parsed.scheme, parsed.netloc, full_path, query, "")
    )


def _get_json(
    url: str,
    headers: dict[str, str],
    *,
    timeout_seconds: float,
) -> dict[str, Any]:
    request = urllib.request.Request(
        url,
        headers={
            "Accept": "application/json",
            **headers,
        },
        method="GET",
    )
    with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
        response_body = response.read().decode("utf-8")
    parsed = json.loads(response_body)
    if not isinstance(parsed, dict):
        raise ValueError("control-plane read response must be a JSON object")
    return parsed


def _parse_timestamp(raw: str | None) -> datetime | None:
    if raw is None:
        return None
    return datetime.fromisoformat(raw.replace("Z", "+00:00"))


def _nonce_prefix() -> str:
    return "nonce-smoke-" + secrets.token_urlsafe(18)


def _nonce(surface: str, *, nonce_prefix: str) -> str:
    return f"{nonce_prefix}-{surface.replace('_', '-')}"


def _print_errors(title: str, errors: list[str]) -> None:
    print(f"{title}:", file=sys.stderr)
    for error in errors:
        print(f"  {error}", file=sys.stderr)


if __name__ == "__main__":
    raise SystemExit(main())
