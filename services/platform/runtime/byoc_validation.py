"""BYOC post-deployment validation helpers.

The validator is deliberately provider-neutral. It can run in an offline
contract mode from a laptop/CI job, and it can run live probes from inside a
customer data plane when URLs and DSNs are supplied.
"""
from __future__ import annotations

import asyncio
import json
import socket
import time
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Literal
from urllib.parse import urlparse
from urllib.request import Request, urlopen

from pydantic import ValidationError

from lib.shared.db import assert_database_startup_safety
from services.platform.runtime.byoc_contract import (
    ByocDataPlaneManifest,
    effective_runtime_processes,
    load_byoc_manifest,
    render_validation_errors,
    validate_byoc_manifest_contract,
)


ValidationStatus = Literal["pass", "fail", "skipped"]

_PASS: ValidationStatus = "pass"
_FAIL: ValidationStatus = "fail"
_SKIPPED: ValidationStatus = "skipped"
_BLOCKING_STATUSES = {_FAIL}
_FALSE_VALUES = {"0", "false", "no", "off"}
_RAW_AGENT_SECRET_KEYS = (
    "FYRALIS_BYOC_INSTALL_TOKEN",
    "FYRALIS_DATA_PLANE_AGENT_PRIVATE_KEY",
)


@dataclass(frozen=True, slots=True)
class ByocValidationCheck:
    name: str
    status: ValidationStatus
    required: bool
    details: str
    metrics: dict[str, object] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class ByocValidationReport:
    status: ValidationStatus
    required_checks_passed: bool
    manifest_path: str
    env_path: str | None
    elapsed_seconds: float
    checks: list[ByocValidationCheck]

    def as_json(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class ByocValidationInputs:
    manifest_path: Path
    env_path: Path | None = None
    gateway_url: str | None = None
    worker_health_urls: Mapping[str, str] = field(default_factory=dict)
    database_url: str | None = None
    kafka_bootstrap_servers: str | None = None
    object_store_url: str | None = None
    require_live: bool = False
    timeout_s: float = 5.0


def parse_env_file(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export ") :].lstrip()
        if "=" not in line:
            continue
        key, raw_value = line.split("=", 1)
        key = key.strip()
        value = _strip_inline_comment(raw_value.strip())
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
            value = value[1:-1]
        if key:
            values[key] = value
    return values


def _strip_inline_comment(value: str) -> str:
    quote: str | None = None
    for index, char in enumerate(value):
        if char in {"'", '"'}:
            quote = None if quote == char else char
        elif char == "#" and quote is None:
            return value[:index].rstrip()
    return value.rstrip()


def _check(
    name: str,
    status: ValidationStatus,
    *,
    required: bool,
    details: str,
    metrics: dict[str, object] | None = None,
) -> ByocValidationCheck:
    return ByocValidationCheck(
        name=name,
        status=status,
        required=required,
        details=details,
        metrics=metrics or {},
    )


def _skip_or_fail(
    name: str,
    *,
    required: bool,
    require_live: bool,
    details: str,
) -> ByocValidationCheck:
    return _check(
        name,
        _FAIL if require_live and required else _SKIPPED,
        required=required,
        details=details,
    )


def _validate_manifest(
    path: Path,
) -> tuple[ByocDataPlaneManifest | None, list[ByocValidationCheck]]:
    try:
        manifest = load_byoc_manifest(path)
    except ValidationError as exc:
        return None, [
            _check(
                "manifest_schema",
                _FAIL,
                required=True,
                details="; ".join(render_validation_errors(exc)),
            )
        ]
    except Exception as exc:  # noqa: BLE001
        return None, [
            _check(
                "manifest_schema",
                _FAIL,
                required=True,
                details=f"{type(exc).__name__}: {exc}",
            )
        ]

    violations = validate_byoc_manifest_contract(manifest)
    checks = [
        _check(
            "manifest_schema",
            _PASS,
            required=True,
            details="BYOC manifest schema is valid.",
        )
    ]
    if violations:
        checks.append(
            _check(
                "manifest_contract",
                _FAIL,
                required=True,
                details="; ".join(violation.render() for violation in violations),
            )
        )
    else:
        checks.append(
            _check(
                "manifest_contract",
                _PASS,
                required=True,
                details="BYOC manifest preserves egress-only and data-residency guarantees.",
            )
        )
    return manifest, checks


def _value_is_false(value: str | None) -> bool:
    return (value or "").strip().lower() in _FALSE_VALUES


def _validate_env_contract(
    manifest: ByocDataPlaneManifest,
    env_path: Path | None,
) -> list[ByocValidationCheck]:
    if env_path is None:
        return [
            _check(
                "env_contract",
                _SKIPPED,
                required=False,
                details="No env file supplied.",
            )
        ]

    try:
        values = parse_env_file(env_path)
    except Exception as exc:  # noqa: BLE001
        return [
            _check(
                "env_contract",
                _FAIL,
                required=True,
                details=f"Could not parse env file: {type(exc).__name__}: {exc}",
            )
        ]

    expected = {
        "FYRALIS_DEPLOYMENT_MODE": "byoc",
        "FYRALIS_BYOC_DEPLOYMENT_ID": manifest.deployment_id,
        "FYRALIS_BYOC_CUSTOMER_ID": manifest.customer_id,
        "FYRALIS_BYOC_CLOUD_PROVIDER": manifest.cloud_provider,
        "FYRALIS_BYOC_REGION": manifest.region,
        "FYRALIS_CONTROL_PLANE_URL": manifest.connectivity.control_plane_url,
        "FYRALIS_CONTROL_PLANE_CONNECTIVITY": "egress_only",
        "FYRALIS_DATA_PLANE_AGENT_ENABLED": "1",
        "FYRALIS_DATA_PLANE_AGENT_AUTH": "mtls",
        "FYRALIS_TELEMETRY_MODE": manifest.telemetry.mode,
        "FYRALIS_TELEMETRY_RAW_LOGS_ALLOWED": "0",
        "FYRALIS_TELEMETRY_RAW_PAYLOADS_ALLOWED": "0",
        "FYRALIS_CONTROL_PLANE_INBOUND_ALLOWED": "0",
        "MASTER_KEK_PROVIDER": manifest.secrets.provider,
        "MASTER_KEK_SECRET_REF": manifest.secrets.master_kek_secret_ref,
        "SECRET_PROVIDER_REGION": manifest.secrets.region,
    }
    missing = sorted(key for key in expected if key not in values)
    mismatched = sorted(
        key for key, expected_value in expected.items()
        if key in values and values[key] != expected_value
    )
    raw_secret_keys = sorted(key for key in _RAW_AGENT_SECRET_KEYS if values.get(key))
    required_refs = {
        "FYRALIS_DATA_PLANE_AGENT_INSTALL_TOKEN_SECRET_REF",
        "FYRALIS_DATA_PLANE_AGENT_CLIENT_CERT_SECRET_REF",
    }
    blank_refs = sorted(key for key in required_refs if not values.get(key))
    unsafe_flags = sorted(
        key
        for key in (
            "FYRALIS_TELEMETRY_RAW_LOGS_ALLOWED",
            "FYRALIS_TELEMETRY_RAW_PAYLOADS_ALLOWED",
            "FYRALIS_CONTROL_PLANE_INBOUND_ALLOWED",
        )
        if key in values and not _value_is_false(values.get(key))
    )

    issues: list[str] = []
    if missing:
        issues.append("missing " + ", ".join(missing))
    if mismatched:
        rendered = ", ".join(
            f"{key}={values[key]!r} expected {expected[key]!r}"
            for key in mismatched
        )
        issues.append("mismatched " + rendered)
    if raw_secret_keys:
        issues.append("raw BYOC agent secrets present: " + ", ".join(raw_secret_keys))
    if blank_refs:
        issues.append("blank managed refs: " + ", ".join(blank_refs))
    if unsafe_flags:
        issues.append("unsafe flags enabled: " + ", ".join(unsafe_flags))

    if issues:
        return [
            _check(
                "env_contract",
                _FAIL,
                required=True,
                details="; ".join(issues),
            )
        ]
    return [
        _check(
            "env_contract",
            _PASS,
            required=True,
            details=(
                "BYOC env values match manifest identity, agent, secret, "
                "and telemetry contracts."
            ),
            metrics={"checked_keys": len(expected) + len(required_refs)},
        )
    ]


def _validate_runtime_contract(
    manifest: ByocDataPlaneManifest,
) -> list[ByocValidationCheck]:
    enabled = effective_runtime_processes(manifest)
    missing_health = sorted(
        process.name for process in enabled if not process.has_healthcheck
    )
    if missing_health:
        return [
            _check(
                "runtime_process_contract",
                _FAIL,
                required=True,
                details="Enabled runtime processes lack healthchecks: "
                + ", ".join(missing_health),
            )
        ]
    return [
        _check(
            "runtime_process_contract",
            _PASS,
            required=True,
            details="Enabled production runtime processes expose health checks.",
            metrics={"enabled_processes": len(enabled)},
        )
    ]


def _http_get_status(url: str, *, timeout_s: float) -> int:
    request = Request(url, method="GET")
    with urlopen(request, timeout=timeout_s) as response:  # noqa: S310
        return int(response.status)


def _join_endpoint(base_url: str, path: str) -> str:
    return base_url.rstrip("/") + path


def _validate_gateway_health(
    gateway_url: str | None,
    *,
    require_live: bool,
    timeout_s: float,
) -> list[ByocValidationCheck]:
    if not gateway_url:
        return [
            _skip_or_fail(
                "gateway_health",
                required=True,
                require_live=require_live,
                details="No gateway URL supplied.",
            )
        ]
    checks: list[ByocValidationCheck] = []
    for name, path in (("gateway_health", "/healthz"), ("gateway_readiness", "/readyz")):
        url = _join_endpoint(gateway_url, path)
        try:
            status = _http_get_status(url, timeout_s=timeout_s)
        except Exception as exc:  # noqa: BLE001
            checks.append(
                _check(
                    name,
                    _FAIL,
                    required=True,
                    details=f"{url} failed: {type(exc).__name__}: {exc}",
                )
            )
            continue
        checks.append(
            _check(
                name,
                _PASS if status == 200 else _FAIL,
                required=True,
                details=f"{url} returned HTTP {status}.",
                metrics={"http_status": status},
            )
        )
    return checks


def _validate_worker_health(
    manifest: ByocDataPlaneManifest,
    worker_health_urls: Mapping[str, str],
    *,
    require_live: bool,
    timeout_s: float,
) -> list[ByocValidationCheck]:
    expected = {
        process.name
        for process in effective_runtime_processes(manifest)
        if process.name != "gateway" and process.has_healthcheck
    }
    supplied = set(worker_health_urls)
    missing = sorted(expected - supplied)
    checks: list[ByocValidationCheck] = []
    if missing and require_live:
        checks.append(
            _check(
                "worker_health_coverage",
                _FAIL,
                required=True,
                details="Missing worker health URLs: " + ", ".join(missing),
                metrics={"missing_count": len(missing)},
            )
        )
    elif missing:
        checks.append(
            _check(
                "worker_health_coverage",
                _SKIPPED,
                required=False,
                details=(
                    "Worker health URLs not supplied for: "
                    + ", ".join(missing[:12])
                    + ("" if len(missing) <= 12 else f"; +{len(missing) - 12} more")
                ),
                metrics={"missing_count": len(missing)},
            )
        )
    else:
        checks.append(
            _check(
                "worker_health_coverage",
                _PASS,
                required=True,
                details="Health URLs supplied for every enabled worker process.",
                metrics={"worker_count": len(expected)},
            )
        )

    for worker_name, base_url in sorted(worker_health_urls.items()):
        if worker_name not in expected:
            checks.append(
                _check(
                    f"worker_health.{worker_name}",
                    _FAIL,
                    required=True,
                    details="Worker health URL does not match an enabled BYOC process.",
                )
            )
            continue
        url = _join_endpoint(base_url, "/healthz")
        try:
            status = _http_get_status(url, timeout_s=timeout_s)
        except Exception as exc:  # noqa: BLE001
            checks.append(
                _check(
                    f"worker_health.{worker_name}",
                    _FAIL,
                    required=True,
                    details=f"{url} failed: {type(exc).__name__}: {exc}",
                )
            )
            continue
        checks.append(
            _check(
                f"worker_health.{worker_name}",
                _PASS if status == 200 else _FAIL,
                required=True,
                details=f"{url} returned HTTP {status}.",
                metrics={"http_status": status},
            )
        )
    return checks


async def _assert_db_safety(database_url: str) -> None:
    import asyncpg

    conn = await asyncpg.connect(database_url)
    try:
        await assert_database_startup_safety(conn)
    finally:
        await conn.close()


def _validate_database_safety(
    database_url: str | None,
    *,
    require_live: bool,
) -> list[ByocValidationCheck]:
    if not database_url:
        return [
            _skip_or_fail(
                "database_rls_safety",
                required=True,
                require_live=require_live,
                details="No database URL supplied.",
            )
        ]
    try:
        asyncio.run(_assert_db_safety(database_url))
    except Exception as exc:  # noqa: BLE001
        return [
            _check(
                "database_rls_safety",
                _FAIL,
                required=True,
                details=f"{type(exc).__name__}: {exc}",
            )
        ]
    return [
        _check(
            "database_rls_safety",
            _PASS,
            required=True,
            details="Database role and strict tenant RLS checks passed.",
        )
    ]


def _tcp_connect(host: str, port: int, *, timeout_s: float) -> None:
    with socket.create_connection((host, port), timeout=timeout_s):
        return


def _bootstrap_endpoints(bootstrap_servers: str) -> list[tuple[str, int]]:
    endpoints: list[tuple[str, int]] = []
    for raw in bootstrap_servers.split(","):
        value = raw.strip()
        if not value:
            continue
        if "://" in value:
            parsed = urlparse(value)
            host = parsed.hostname
            port = parsed.port
        else:
            host_part, _, port_part = value.rpartition(":")
            host = host_part or value
            port = int(port_part) if port_part else 9092
        if not host or port is None:
            raise ValueError(f"invalid Kafka bootstrap endpoint: {raw!r}")
        endpoints.append((host, int(port)))
    if not endpoints:
        raise ValueError("no Kafka bootstrap endpoints configured")
    return endpoints


def _validate_broker_reachability(
    bootstrap_servers: str | None,
    *,
    require_live: bool,
    timeout_s: float,
) -> list[ByocValidationCheck]:
    if not bootstrap_servers:
        return [
            _skip_or_fail(
                "broker_reachability",
                required=True,
                require_live=require_live,
                details="No Kafka/bootstrap endpoint supplied.",
            )
        ]
    try:
        endpoints = _bootstrap_endpoints(bootstrap_servers)
        host, port = endpoints[0]
        _tcp_connect(host, port, timeout_s=timeout_s)
    except Exception as exc:  # noqa: BLE001
        return [
            _check(
                "broker_reachability",
                _FAIL,
                required=True,
                details=f"{type(exc).__name__}: {exc}",
            )
        ]
    return [
        _check(
            "broker_reachability",
            _PASS,
            required=True,
            details="Broker TCP endpoint accepted a connection.",
            metrics={"configured_endpoints": len(endpoints)},
        )
    ]


def _validate_object_store_reachability(
    object_store_url: str | None,
    *,
    require_live: bool,
    timeout_s: float,
) -> list[ByocValidationCheck]:
    if not object_store_url:
        return [
            _skip_or_fail(
                "object_store_reachability",
                required=True,
                require_live=require_live,
                details="No object-store endpoint URL supplied.",
            )
        ]
    parsed = urlparse(object_store_url)
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        return [
            _check(
                "object_store_reachability",
                _FAIL,
                required=True,
                details="Object-store endpoint must be an HTTP(S) URL.",
            )
        ]
    if parsed.username or parsed.password:
        return [
            _check(
                "object_store_reachability",
                _FAIL,
                required=True,
                details="Object-store endpoint URL must not contain credentials.",
            )
        ]
    port = parsed.port or (443 if parsed.scheme == "https" else 80)
    try:
        _tcp_connect(parsed.hostname, port, timeout_s=timeout_s)
    except Exception as exc:  # noqa: BLE001
        return [
            _check(
                "object_store_reachability",
                _FAIL,
                required=True,
                details=f"{type(exc).__name__}: {exc}",
            )
        ]
    return [
        _check(
            "object_store_reachability",
            _PASS,
            required=True,
            details="Object-store endpoint accepted a TCP connection.",
        )
    ]


def run_byoc_post_deploy_validation(
    inputs: ByocValidationInputs,
) -> ByocValidationReport:
    started = time.monotonic()
    checks: list[ByocValidationCheck] = []
    manifest, manifest_checks = _validate_manifest(inputs.manifest_path)
    checks.extend(manifest_checks)

    if manifest is not None:
        checks.extend(_validate_env_contract(manifest, inputs.env_path))
        checks.extend(_validate_runtime_contract(manifest))
        checks.extend(
            _validate_gateway_health(
                inputs.gateway_url,
                require_live=inputs.require_live,
                timeout_s=inputs.timeout_s,
            )
        )
        checks.extend(
            _validate_worker_health(
                manifest,
                inputs.worker_health_urls,
                require_live=inputs.require_live,
                timeout_s=inputs.timeout_s,
            )
        )
        checks.extend(
            _validate_database_safety(
                inputs.database_url,
                require_live=inputs.require_live,
            )
        )
        checks.extend(
            _validate_broker_reachability(
                inputs.kafka_bootstrap_servers,
                require_live=inputs.require_live,
                timeout_s=inputs.timeout_s,
            )
        )
        checks.extend(
            _validate_object_store_reachability(
                inputs.object_store_url,
                require_live=inputs.require_live,
                timeout_s=inputs.timeout_s,
            )
        )

    required_checks_passed = all(
        check.status not in _BLOCKING_STATUSES
        for check in checks
        if check.required
    )
    status: ValidationStatus = _PASS if required_checks_passed else _FAIL
    return ByocValidationReport(
        status=status,
        required_checks_passed=required_checks_passed,
        manifest_path=str(inputs.manifest_path),
        env_path=str(inputs.env_path) if inputs.env_path else None,
        elapsed_seconds=round(time.monotonic() - started, 3),
        checks=checks,
    )


def render_report_json(report: ByocValidationReport) -> str:
    return json.dumps(report.as_json(), indent=2, sort_keys=True) + "\n"


def render_report_markdown(report: ByocValidationReport) -> str:
    lines = [
        "# BYOC Post-Deploy Validation",
        "",
        f"- Status: `{report.status}`",
        f"- Required checks passed: `{str(report.required_checks_passed).lower()}`",
        f"- Manifest: `{report.manifest_path}`",
        f"- Env: `{report.env_path or 'not supplied'}`",
        f"- Elapsed seconds: `{report.elapsed_seconds:.3f}`",
        "",
        "| Check | Required | Status | Details |",
        "| --- | --- | --- | --- |",
    ]
    for check in report.checks:
        details = check.details.replace("|", "/")
        lines.append(
            f"| {check.name} | {str(check.required).lower()} | "
            f"{check.status} | {details} |"
        )
    return "\n".join(lines) + "\n"


def parse_worker_health_args(values: Sequence[str]) -> dict[str, str]:
    parsed: dict[str, str] = {}
    for value in values:
        if "=" not in value:
            raise ValueError(
                "--worker-health must use NAME=URL, e.g. think_worker=http://..."
            )
        name, url = value.split("=", 1)
        name = name.strip()
        url = url.strip()
        if not name or not url:
            raise ValueError("--worker-health NAME and URL must be non-empty")
        parsed[name] = url
    return parsed


__all__ = [
    "ByocValidationCheck",
    "ByocValidationInputs",
    "ByocValidationReport",
    "parse_env_file",
    "parse_worker_health_args",
    "render_report_json",
    "render_report_markdown",
    "run_byoc_post_deploy_validation",
]
