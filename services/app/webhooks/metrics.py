"""services/app/webhooks/metrics.py — verification-failure counters.

Per spec FR-011 every verification failure increments a counter
labeled with `{provider, reason}`. This module provides an in-process
counter that the observability stack (structlog handlers, Prometheus
exporter, etc.) can read or wrap.

The implementation is deliberately minimal — a thread-safe dict —
because the project does not currently ship a Prometheus client and
the constitution's simplicity principle (X) says don't add one until
there's a second caller. Tests read the counter directly to assert
labeling correctness.
"""
from __future__ import annotations

import threading
from typing import Mapping


_lock = threading.Lock()
_counters: dict[tuple[str, str], int] = {}

# ---------------------------------------------------------------------
# Tenant resolver metrics (FR-018) — three named families:
#   webhook_resolver_outcomes_total{provider, outcome}
#   webhook_resolver_cache_total{provider, result}
#   webhook_resolver_duration_seconds{provider}  (sample-based p95)
#
# Labels are bounded by the 5-provider enum × small outcome/result
# enums. installation_id is NEVER a label (FR-015).
# ---------------------------------------------------------------------
_resolver_outcomes: dict[tuple[str, str], int] = {}
_resolver_cache: dict[tuple[str, str], int] = {}
_resolver_samples: dict[str, list[float]] = {}
# Cap stored samples per provider to bound memory. P95 on a rolling
# 1024-sample window is the assertion API the integration test uses.
_RESOLVER_SAMPLE_CAP = 1024

# ---------------------------------------------------------------------
# M5.3 cutover metrics — `webhook_router_kafka_path_total{provider, outcome}`.
#
# outcome ∈ {success, fallback}.
#   success  → flag=TRUE, Kafka publish succeeded, response is 202.
#   fallback → flag=TRUE, Kafka publish failed; router fell back to
#              inline ingest() and returned 200/201. This is graceful
#              degradation: user-visible behaviour is preserved under
#              shadow-path outage. Sustained increment of `fallback`
#              is the operator's smoke detector — the cutover path
#              has connectivity problems that need investigation, but
#              the customer experience stays uninterrupted.
#
# Not labeled with tenant_id (high-cardinality). The cutover flag
# itself is per-tenant, so the operator drills down via the database
# / runbook procedure documented in M5.4.
# ---------------------------------------------------------------------
_kafka_path_outcomes: dict[tuple[str, str], int] = {}


def record_failure(provider: str, reason: str) -> None:
    """Increment the (provider, reason) failure counter by 1."""
    key = (provider, reason)
    with _lock:
        _counters[key] = _counters.get(key, 0) + 1


def get_count(provider: str, reason: str) -> int:
    with _lock:
        return _counters.get((provider, reason), 0)


def snapshot() -> Mapping[tuple[str, str], int]:
    """Read-only snapshot of all counters. Used by tests."""
    with _lock:
        return dict(_counters)


def reset() -> None:
    """Test helper — clear all counters."""
    with _lock:
        _counters.clear()
        _resolver_outcomes.clear()
        _resolver_cache.clear()
        _resolver_samples.clear()
        _kafka_path_outcomes.clear()


def record_kafka_path_outcome(provider: str, outcome: str) -> None:
    """Increment `webhook_router_kafka_path_total{provider, outcome}`.

    See the module-level comment on `_kafka_path_outcomes` for the
    full semantic of each outcome value. Two valid outcomes:
      - "success"  : the cutover path produced a 202 response.
      - "fallback" : the cutover path failed and the router fell
        back to inline ingest() — graceful degradation, not a 4xx.
    """
    with _lock:
        key = (provider, outcome)
        _kafka_path_outcomes[key] = _kafka_path_outcomes.get(key, 0) + 1


def get_kafka_path_count(provider: str, outcome: str) -> int:
    with _lock:
        return _kafka_path_outcomes.get((provider, outcome), 0)


# ---------------------------------------------------------------------
# Resolver metric helpers
# ---------------------------------------------------------------------

def record_resolver_outcome(provider: str, outcome: str) -> None:
    """Increment webhook_resolver_outcomes_total{provider, outcome}.

    outcome ∈ {resolved, unknown_installation, payload_missing}.
    """
    with _lock:
        key = (provider, outcome)
        _resolver_outcomes[key] = _resolver_outcomes.get(key, 0) + 1


def record_resolver_cache(provider: str, result: str) -> None:
    """Increment webhook_resolver_cache_total{provider, result}.

    result ∈ {hit, miss, bypass}.
    """
    with _lock:
        key = (provider, result)
        _resolver_cache[key] = _resolver_cache.get(key, 0) + 1


def observe_resolver_duration(provider: str, seconds: float) -> None:
    """Record one resolver-duration sample for the given provider.

    Sample-based histogram (capped at 1024 entries per provider). The
    integration test reads p95 via `resolver_duration_p95`. Cap is
    intentional — sample-based histograms don't bound memory by
    default.
    """
    with _lock:
        samples = _resolver_samples.setdefault(provider, [])
        samples.append(seconds)
        if len(samples) > _RESOLVER_SAMPLE_CAP:
            # Drop the oldest sample. This is O(n) but n is bounded
            # and resolver invocations are infrequent vs network IO.
            del samples[0]


def get_resolver_outcome_count(provider: str, outcome: str) -> int:
    with _lock:
        return _resolver_outcomes.get((provider, outcome), 0)


def get_resolver_cache_count(provider: str, result: str) -> int:
    with _lock:
        return _resolver_cache.get((provider, result), 0)


def _p95_nearest_rank(samples: list[float]) -> float | None:
    """p95 by the nearest-rank method: sort, pick the ⌈0.95 * N⌉-th item.

    Pure (takes its own copy of the data) so callers can invoke it
    OUTSIDE `_lock` — important because `_lock` is a non-reentrant
    `threading.Lock` and `render_prometheus` already holds a snapshot.
    """
    if not samples:
        return None
    ordered = sorted(samples)
    n = len(ordered)
    # Nearest-rank for p95: index = ceil(0.95 * N) - 1, clamped.
    idx = max(0, min(n - 1, -(-95 * n // 100) - 1))
    return ordered[idx]


def resolver_duration_p95(provider: str) -> float | None:
    """Return p95 of stored samples for this provider, or None if no
    samples exist.
    """
    with _lock:
        samples = list(_resolver_samples.get(provider, ()))
    return _p95_nearest_rank(samples)


def snapshot_resolver() -> dict[str, dict[tuple[str, str], int]]:
    """Read-only snapshot of resolver counters (outcomes + cache).

    Used by integration tests to assert exact counter values.
    """
    with _lock:
        return {
            "outcomes": dict(_resolver_outcomes),
            "cache": dict(_resolver_cache),
        }


# ---------------------------------------------------------------------
# Prometheus text exposition (FR-011 scrape path).
#
# The in-process counters above were recorded but never exported, so ops
# dashboards had no scrape path. This renderer emits every counter family
# in Prometheus text format (version 0.0.4) for the gateway's GET /metrics
# route to serve. Hand-rolled text — NO prometheus_client dependency —
# mirroring services/ingest/ingestion/observability.py, so the
# constitution's "don't add a Prometheus client until there's a second
# caller" principle (X) still holds.
#
# Cardinality note: every label here is a bounded enum (provider × a small
# reason/outcome/result set). installation_id / tenant_id are NEVER labels
# (FR-015). The counters are per-process; a single-process gateway is the
# deployment assumption (multiprocess aggregation is a future concern).
# ---------------------------------------------------------------------

def _escape_label_value(value: str) -> str:
    """Escape a Prometheus label value: backslash, double-quote, newline."""
    return (
        str(value)
        .replace("\\", "\\\\")
        .replace('"', '\\"')
        .replace("\n", "\\n")
    )


def _counter_lines(
    name: str,
    help_text: str,
    label_names: tuple[str, ...],
    data: Mapping[tuple[str, str], int],
) -> list[str]:
    out = [f"# HELP {name} {help_text}", f"# TYPE {name} counter"]
    for key, value in sorted(data.items()):
        labels = ",".join(
            f'{ln}="{_escape_label_value(lv)}"'
            for ln, lv in zip(label_names, key)
        )
        out.append(f"{name}{{{labels}}} {value}")
    return out


def render_prometheus() -> str:
    """Render all webhook counters as Prometheus text exposition (0.0.4).

    Families:
      - webhook_verification_failures_total{provider,reason}   (FR-011)
      - webhook_resolver_outcomes_total{provider,outcome}      (FR-018)
      - webhook_resolver_cache_total{provider,result}          (FR-018)
      - webhook_router_kafka_path_total{provider,outcome}      (M5.3)
      - webhook_resolver_duration_p95_seconds{provider} (gauge) (FR-018)

    Takes one consistent snapshot under `_lock`, then formats outside it.
    """
    with _lock:
        failures = dict(_counters)
        resolver_outcomes = dict(_resolver_outcomes)
        resolver_cache = dict(_resolver_cache)
        kafka_path = dict(_kafka_path_outcomes)
        samples_by_provider = {
            p: list(s) for p, s in _resolver_samples.items()
        }

    lines: list[str] = []
    lines += _counter_lines(
        "webhook_verification_failures_total",
        "Webhook signature/verification failures by provider and reason (FR-011).",
        ("provider", "reason"),
        failures,
    )
    lines += _counter_lines(
        "webhook_resolver_outcomes_total",
        "Tenant-resolver outcomes by provider (FR-018).",
        ("provider", "outcome"),
        resolver_outcomes,
    )
    lines += _counter_lines(
        "webhook_resolver_cache_total",
        "Tenant-resolver cache results by provider (FR-018).",
        ("provider", "result"),
        resolver_cache,
    )
    lines += _counter_lines(
        "webhook_router_kafka_path_total",
        "M5.3 cutover-path outcomes by provider (success|fallback).",
        ("provider", "outcome"),
        kafka_path,
    )

    lines.append(
        "# HELP webhook_resolver_duration_p95_seconds "
        "Tenant-resolver p95 duration over a rolling sample window (FR-018)."
    )
    lines.append("# TYPE webhook_resolver_duration_p95_seconds gauge")
    for provider in sorted(samples_by_provider):
        p95 = _p95_nearest_rank(samples_by_provider[provider])
        if p95 is None:
            continue
        lines.append(
            f"webhook_resolver_duration_p95_seconds"
            f'{{provider="{_escape_label_value(provider)}"}} {p95}'
        )

    return "\n".join(lines) + "\n"


__all__ = [
    "record_failure",
    "get_count",
    "snapshot",
    "reset",
    "record_resolver_outcome",
    "record_resolver_cache",
    "observe_resolver_duration",
    "get_resolver_outcome_count",
    "get_resolver_cache_count",
    "resolver_duration_p95",
    "snapshot_resolver",
    "record_kafka_path_outcome",
    "get_kafka_path_count",
    "render_prometheus",
]
