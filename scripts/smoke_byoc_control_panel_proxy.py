#!/usr/bin/env python3
"""Smoke the bearer-authenticated BYOC control-panel proxy routes."""
from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Sequence


DEFAULT_BEARER_TOKEN_ENV = "FYRALIS_GATEWAY_BEARER_TOKEN"
DEPLOYMENTS_PATH = "/byoc/control-panel/deployments"
STATE_PATH = "/byoc/control-panel/state"


def _parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--base-url",
        help=(
            "Gateway base URL. Omit to print a redacted request plan without "
            "network access."
        ),
    )
    parser.add_argument("--customer-id", help="Optional BYOC customer id filter.")
    parser.add_argument(
        "--deployment-id",
        help="Optional BYOC deployment id. Defaults to the first deployment grant.",
    )
    parser.add_argument(
        "--recent-limit",
        type=int,
        default=10,
        help="Recent receipt limit for /byoc/control-panel/state.",
    )
    parser.add_argument(
        "--bearer-token-env",
        default=DEFAULT_BEARER_TOKEN_ENV,
        help="Environment variable containing the gateway bearer token.",
    )
    parser.add_argument("--timeout-seconds", type=float, default=10.0)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(list(sys.argv[1:] if argv is None else argv))
    try:
        if args.recent_limit < 1:
            raise ValueError("--recent-limit must be positive")
        if args.timeout_seconds <= 0:
            raise ValueError("--timeout-seconds must be positive")
        if not args.base_url:
            _print_json(_request_plan(args))
            return 0
        token = os.environ.get(args.bearer_token_env, "")
        if not token.strip():
            raise RuntimeError(
                f"{args.bearer_token_env} must contain a gateway bearer token"
            )
        summary = _execute(args, bearer_token=token)
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        _print_errors("BYOC control-panel proxy smoke was rejected", [f"HTTP {exc.code}: {body}"])
        return 1
    except urllib.error.URLError as exc:
        _print_errors("BYOC control-panel proxy smoke was unreachable", [str(exc.reason)])
        return 1
    except (RuntimeError, TypeError, ValueError) as exc:
        _print_errors(
            "BYOC control-panel proxy smoke failed",
            [f"{type(exc).__name__}: {exc}"],
        )
        return 2
    _print_json(summary)
    return 0


def _request_plan(args: argparse.Namespace) -> dict[str, Any]:
    deployment_query = _query_string({"customer_id": args.customer_id})
    state_query = _query_string(
        {
            "deployment_id": args.deployment_id or "<dep_...>",
            "customer_id": args.customer_id,
            "recent_limit": str(args.recent_limit),
        }
    )
    return {
        "schema_version": "fyralis.byoc.control_panel_proxy_smoke_plan.v1",
        "mode": "request_plan",
        "bearer_token_env": args.bearer_token_env,
        "headers": {"Authorization": "Bearer <redacted>"},
        "requests": [
            {
                "method": "GET",
                "path": DEPLOYMENTS_PATH,
                "query": deployment_query,
            },
            {
                "method": "GET",
                "path": STATE_PATH,
                "query": state_query,
            },
        ],
        "stored_scope": "sanitized_control_panel_proxy_smoke_metadata_only",
    }


def _execute(args: argparse.Namespace, *, bearer_token: str) -> dict[str, Any]:
    deployments_query = _query_string({"customer_id": args.customer_id})
    deployments = _get_json(
        _url(args.base_url, DEPLOYMENTS_PATH, deployments_query),
        bearer_token=bearer_token,
        timeout_seconds=args.timeout_seconds,
    )
    selected = _selected_deployment(
        deployments,
        requested_deployment_id=args.deployment_id,
    )
    customer_id = args.customer_id or selected["customer_id"]
    state_query = _query_string(
        {
            "deployment_id": selected["deployment_id"],
            "customer_id": customer_id,
            "recent_limit": str(args.recent_limit),
        }
    )
    state = _get_json(
        _url(args.base_url, STATE_PATH, state_query),
        bearer_token=bearer_token,
        timeout_seconds=args.timeout_seconds,
    )
    sections = state.get("sections", ())
    actions = state.get("actions", ())
    return {
        "schema_version": "fyralis.byoc.control_panel_proxy_smoke.v1",
        "mode": "executed",
        "deployment_grant_count": deployments.get("result_count"),
        "selected_customer_id": customer_id,
        "selected_deployment_id": selected["deployment_id"],
        "state_schema_version": state.get("schema_version"),
        "state_stored_scope": state.get("stored_scope"),
        "section_count": len(sections) if isinstance(sections, list) else 0,
        "action_count": len(actions) if isinstance(actions, list) else 0,
        "raw_state_body_included": False,
        "bearer_token_included": False,
        "read_hmac_material_included": False,
        "endpoint_urls_included": False,
        "raw_reports_included": False,
        "logs_included": False,
        "prompts_included": False,
        "pii_included": False,
        "stored_scope": "sanitized_control_panel_proxy_smoke_metadata_only",
    }


def _selected_deployment(
    deployments: dict[str, Any],
    *,
    requested_deployment_id: str | None,
) -> dict[str, str]:
    items = deployments.get("items")
    if not isinstance(items, list) or not items:
        raise ValueError("control-panel deployments response did not include grants")
    for item in items:
        deployment_ids = item.get("deployment_ids")
        if not isinstance(deployment_ids, list) or not deployment_ids:
            continue
        customer_id = item.get("customer_id")
        if not isinstance(customer_id, str):
            continue
        for deployment_id in deployment_ids:
            if not isinstance(deployment_id, str):
                continue
            if requested_deployment_id is None or requested_deployment_id == deployment_id:
                return {
                    "customer_id": customer_id,
                    "deployment_id": deployment_id,
                }
    raise ValueError("requested deployment was not present in deployment grants")


def _query_string(params: dict[str, str | None]) -> str:
    return urllib.parse.urlencode(
        [(key, value) for key, value in params.items() if value]
    )


def _url(base_url: str, path: str, query: str) -> str:
    parsed = urllib.parse.urlsplit(base_url.rstrip("/") + path)
    return urllib.parse.urlunsplit(
        (parsed.scheme, parsed.netloc, parsed.path, query, parsed.fragment)
    )


def _get_json(
    url: str,
    *,
    bearer_token: str,
    timeout_seconds: float,
) -> dict[str, Any]:
    request = urllib.request.Request(
        url,
        headers={
            "Accept": "application/json",
            "Authorization": f"Bearer {bearer_token}",
        },
        method="GET",
    )
    with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
        body = response.read().decode("utf-8")
    parsed = json.loads(body)
    if not isinstance(parsed, dict):
        raise ValueError("control-panel proxy response must be a JSON object")
    return parsed


def _print_json(payload: dict[str, Any]) -> None:
    print(json.dumps(payload, indent=2, sort_keys=True))


def _print_errors(title: str, errors: list[str]) -> None:
    print(f"{title}:", file=sys.stderr)
    for error in errors:
        print(f"  {error}", file=sys.stderr)


if __name__ == "__main__":
    raise SystemExit(main())
