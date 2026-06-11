"""lib/observability — shared, dependency-free Prometheus instrumentation.

The repo's constitution (principle X, see services/app/webhooks/metrics.py)
keeps prometheus_client out; every exposition surface is hand-rolled text.
This package centralizes that convention so new instrumentation (Ollama,
asyncpg pools, Kafka producer, OAuth refresh, gateway HTTP) shares one
registry instead of growing more bespoke renderers.

Design notes live in docs/architecture/observability_architecture.md.
"""
from lib.observability.metrics import (
    DEFAULT_BUCKETS,
    Counter,
    Gauge,
    Histogram,
    Registry,
    counter,
    gauge,
    histogram,
    render_default,
    reset_default_for_tests,
)

__all__ = [
    "DEFAULT_BUCKETS",
    "Counter",
    "Gauge",
    "Histogram",
    "Registry",
    "counter",
    "gauge",
    "histogram",
    "render_default",
    "reset_default_for_tests",
]
