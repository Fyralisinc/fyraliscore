#!/usr/bin/env python3
"""Render or check the gateway route access inventory."""
from __future__ import annotations

import argparse
import contextlib
import json
import sys
from pathlib import Path
from typing import Sequence, cast

from fastapi import FastAPI

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

with contextlib.redirect_stdout(sys.stderr):
    from services.app.gateway.route_access import (  # noqa: E402
        RouteAccess,
        iter_gateway_route_inventory,
    )
    from services.app.gateway.route_mounts import mount_gateway_routes  # noqa: E402
    from services.app.gateway.settings import GatewaySettings  # noqa: E402


def _production_settings() -> GatewaySettings:
    return GatewaySettings(
        environment="production",
        auth_bootstrap_secret="prod-bootstrap-secret-32chars-minimum",
        debug_endpoints_enabled=False,
        finance_panel_enabled=False,
        slack_dm_panel_enabled=False,
        spec_demo_routes_enabled=False,
        websocket_query_token_auth_enabled=False,
        view_ceo_static_tokens_enabled=False,
        mount_sim=False,
        require_realtime=False,
        require_github_integration=False,
        require_ingestion_data_plane=True,
        start_grt_scheduler=True,
    )


def _build_inventory_app(
    *,
    debug_endpoints_enabled: bool,
    production: bool = False,
) -> FastAPI:
    settings = (
        _production_settings()
        if production
        else GatewaySettings(debug_endpoints_enabled=debug_endpoints_enabled)
    )
    app = FastAPI(title="Gateway Route Inventory")
    app.state.gateway_settings = settings
    with contextlib.redirect_stdout(sys.stderr):
        mount_gateway_routes(app, settings=settings, emit_mount_logs=False)
    return app


def _as_rows(app: FastAPI) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for entry in iter_gateway_route_inventory(app):
        rows.append(
            {
                "methods": list(entry.methods),
                "path": entry.path,
                "name": entry.name,
                "tags": list(entry.tags),
                "access": entry.policy.access.value,
                "gateway_bearer_required": entry.policy.gateway_bearer_required,
                "reason": entry.policy.reason,
            }
        )
    return rows


def _render_markdown(rows: list[dict[str, object]]) -> str:
    lines = [
        "| Methods | Path | Access | Gateway Bearer | Tags | Reason |",
        "| --- | --- | --- | --- | --- | --- |",
    ]
    for row in rows:
        methods = ", ".join(
            str(method) for method in cast(list[object], row["methods"])
        )
        tags = ", ".join(str(tag) for tag in cast(list[object], row["tags"])) or "-"
        bearer = "yes" if row["gateway_bearer_required"] else "no"
        lines.append(
            f"| `{methods}` | `{row['path']}` | `{row['access']}` | "
            f"{bearer} | {tags} | {row['reason']} |"
        )
    return "\n".join(lines)


def _check(
    rows: list[dict[str, object]],
    *,
    debug_endpoints_enabled: bool,
    production: bool = False,
    repo_root: Path = REPO_ROOT,
) -> list[str]:
    errors: list[str] = []
    if not debug_endpoints_enabled:
        debug_rows = [
            row for row in rows if row["access"] == RouteAccess.DEBUG.value
        ]
        if debug_rows:
            paths = ", ".join(str(row["path"]) for row in debug_rows)
            errors.append(f"debug routes mounted while debug endpoints are disabled: {paths}")
    for row in rows:
        access = str(row["access"])
        if access == RouteAccess.PUBLIC.value and row["path"] not in {
            "/healthz",
            "/legal/local-test-privacy",
            "/readyz",
            "/metrics",
        }:
            errors.append(f"unexpected fully public route: {row['path']}")
        if str(row["path"]).startswith("/api/admin/") and (
            access != RouteAccess.ADMIN.value
            or row["gateway_bearer_required"] is not True
        ):
            errors.append(
                "/api/admin/* routes must remain admin-only behind gateway "
                f"bearer auth; found {row['path']} as {access}"
            )
        if str(row["path"]).startswith("/ingest/") and (
            access != RouteAccess.BEARER.value
            or row["gateway_bearer_required"] is not True
        ):
            errors.append(
                "/ingest/{channel} must remain gateway bearer-authenticated "
                f"internal/dev intake; found {row['path']} as {access}"
            )
        if production:
            path = str(row["path"])
            for prefix in ("/v1/spec", "/v1/demo", "/debug"):
                if path.startswith(prefix):
                    errors.append(
                        f"production route must not mount {prefix} surface: {path}"
                    )
            tags = {str(tag) for tag in cast(list[object], row["tags"])}
            if "substrate" in tags and (
                access != RouteAccess.BEARER.value
                or row["gateway_bearer_required"] is not True
            ):
                errors.append(
                    "substrate routes must remain gateway bearer-authenticated "
                    f"before row-level access checks; found {path} as {access}"
                )
    if production:
        errors.extend(_check_production_source_invariants(repo_root=repo_root))
        errors.extend(_check_substrate_access_invariants(repo_root=repo_root))
    return errors


def _check_production_source_invariants(*, repo_root: Path = REPO_ROOT) -> list[str]:
    errors: list[str] = []
    gateway_dir = repo_root / "services/app/gateway"
    for path in gateway_dir.rglob("*.py"):
        rel = path.relative_to(repo_root)
        rel_text = rel.as_posix()
        if "/tests/" in rel_text:
            continue
        text = path.read_text(encoding="utf-8")
        if '"stub": True' in text or "'stub': True" in text:
            errors.append(f"production gateway code returns stub=true: {rel}")
        if path.name != "spec_routes.py" and (
            "/v1/spec/" in text or "/v1/spec" in text
        ):
            errors.append(f"spec seed route appears outside spec_routes.py: {rel}")
    return errors


def _check_substrate_access_invariants(*, repo_root: Path = REPO_ROOT) -> list[str]:
    """Keep legacy substrate list endpoints behind row-level access checks."""

    path = repo_root / "services/app/gateway/substrate_router.py"
    try:
        text = path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return ["substrate route access invariant could not read substrate_router.py"]

    required_snippets = {
        "await can_read(": "substrate rows must be filtered through can_read",
        "await _record_override_if_needed(": (
            "substrate override reads must be recorded through the local helper"
        ),
        "await record_override(": (
            "substrate override helper must write access_override_log records"
        ),
    }
    errors: list[str] = []
    for snippet, message in required_snippets.items():
        if snippet not in text:
            errors.append(message)
    return errors


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--debug-endpoints",
        action="store_true",
        help="include routes that are mounted only when DEBUG_ENDPOINTS_ENABLED is true",
    )
    parser.add_argument(
        "--format",
        choices=("markdown", "json"),
        default="markdown",
        help="output format",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="fail if the inventory violates production exposure invariants",
    )
    parser.add_argument(
        "--production",
        action="store_true",
        help="build the route inventory with production gateway settings",
    )
    args = parser.parse_args(argv)

    app = _build_inventory_app(
        debug_endpoints_enabled=args.debug_endpoints,
        production=args.production,
    )
    rows = _as_rows(app)
    if args.check:
        errors = _check(
            rows,
            debug_endpoints_enabled=args.debug_endpoints,
            production=args.production,
        )
        if errors:
            for error in errors:
                print(error, file=sys.stderr)
            return 1

    if args.format == "json":
        print(json.dumps(rows, indent=2, sort_keys=True))
    else:
        print(_render_markdown(rows))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
