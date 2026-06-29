#!/usr/bin/env python3
"""Fail when product workflow SLO burn is above deploy rollback thresholds."""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any


DEFAULT_PROMETHEUS_URL = "http://localhost:9090"
DEFAULT_ERROR_BURN_MAX = 2.0
DEFAULT_LATENCY_BURN_MAX = 2.0

SLO_QUERIES = {
    "error_budget_burn": "max(fyralis:product_workflow_error_budget_burn:5m)",
    "latency_budget_burn": "max(fyralis:product_workflow_latency_budget_burn:5m)",
}


@dataclass(frozen=True, slots=True)
class SLOGateResult:
    values: dict[str, float | None]
    findings: list[str]

    @property
    def ok(self) -> bool:
        return not self.findings


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    try:
        return float(raw)
    except ValueError as exc:
        raise SystemExit(f"{name} must be a float") from exc


def _query_prometheus(
    *,
    prometheus_url: str,
    query: str,
    timeout_seconds: float,
) -> dict[str, Any]:
    base = prometheus_url.rstrip("/")
    url = f"{base}/api/v1/query?{urllib.parse.urlencode({'query': query})}"
    with urllib.request.urlopen(url, timeout=timeout_seconds) as response:
        payload = response.read().decode("utf-8")
    data = json.loads(payload)
    if data.get("status") != "success":
        raise RuntimeError(f"Prometheus query failed: {data!r}")
    return data


def _extract_scalar_value(payload: dict[str, Any]) -> float | None:
    result = payload.get("data", {}).get("result", [])
    if not result:
        return None
    value = result[0].get("value")
    if not isinstance(value, list) or len(value) < 2:
        return None
    try:
        return float(value[1])
    except (TypeError, ValueError):
        return None


def evaluate_slo_gate(
    values: dict[str, float | None],
    *,
    error_burn_max: float,
    latency_burn_max: float,
) -> SLOGateResult:
    findings: list[str] = []
    error_burn = values.get("error_budget_burn")
    latency_burn = values.get("latency_budget_burn")
    if error_burn is not None and error_burn > error_burn_max:
        findings.append(
            f"product error budget burn {error_burn:.3g} exceeds {error_burn_max:.3g}"
        )
    if latency_burn is not None and latency_burn > latency_burn_max:
        findings.append(
            f"product latency budget burn {latency_burn:.3g} exceeds {latency_burn_max:.3g}"
        )
    return SLOGateResult(values=values, findings=findings)


def run_once(args: argparse.Namespace) -> SLOGateResult:
    values: dict[str, float | None] = {}
    for name, query in SLO_QUERIES.items():
        payload = _query_prometheus(
            prometheus_url=args.prometheus_url,
            query=query,
            timeout_seconds=args.timeout_seconds,
        )
        values[name] = _extract_scalar_value(payload)
    return evaluate_slo_gate(
        values,
        error_burn_max=args.error_burn_max,
        latency_burn_max=args.latency_burn_max,
    )


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--prometheus-url",
        default=os.environ.get("PRODUCT_SLO_GATE_PROMETHEUS_URL", DEFAULT_PROMETHEUS_URL),
    )
    parser.add_argument(
        "--error-burn-max",
        type=float,
        default=_env_float("PRODUCT_SLO_GATE_ERROR_BURN_MAX", DEFAULT_ERROR_BURN_MAX),
    )
    parser.add_argument(
        "--latency-burn-max",
        type=float,
        default=_env_float("PRODUCT_SLO_GATE_LATENCY_BURN_MAX", DEFAULT_LATENCY_BURN_MAX),
    )
    parser.add_argument(
        "--wait-seconds",
        type=float,
        default=_env_float("PRODUCT_SLO_GATE_WAIT_SECONDS", 0.0),
    )
    parser.add_argument(
        "--interval-seconds",
        type=float,
        default=_env_float("PRODUCT_SLO_GATE_INTERVAL_SECONDS", 15.0),
    )
    parser.add_argument(
        "--timeout-seconds",
        type=float,
        default=_env_float("PRODUCT_SLO_GATE_QUERY_TIMEOUT_SECONDS", 5.0),
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    deadline = time.monotonic() + max(0.0, args.wait_seconds)
    last_error: Exception | None = None
    while True:
        try:
            result = run_once(args)
            values = ", ".join(
                f"{key}={value if value is not None else 'no_data'}"
                for key, value in sorted(result.values.items())
            )
            print(f"product SLO gate values: {values}")
            if result.ok:
                return 0
            for finding in result.findings:
                print(finding, file=sys.stderr)
            return 1
        except Exception as exc:  # noqa: BLE001 - CLI must summarize any query failure
            last_error = exc
            if time.monotonic() >= deadline:
                print(f"product SLO gate query failed: {exc}", file=sys.stderr)
                return 1
            time.sleep(max(1.0, args.interval_seconds))
        if time.monotonic() >= deadline:
            if last_error is not None:
                print(f"product SLO gate query failed: {last_error}", file=sys.stderr)
            return 1


if __name__ == "__main__":
    raise SystemExit(main())
