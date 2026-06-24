"""lib/observability/metrics.py — labeled Counter/Gauge/Histogram + text render.

Hand-rolled Prometheus text exposition (version 0.0.4) with real labeled
families and cumulative-bucket histograms — the two things the existing
per-module counter dicts can't express. No prometheus_client dependency
(constitution principle X).

Cardinality rules (docs/architecture/observability_architecture.md §4):
label values MUST come from bounded enums. tenant_id / installation_id /
free-form ids are forbidden as label values — callers aggregate per-tenant
data in Postgres instead.

Thread safety: each family takes one lock per mutation; rendering snapshots
under the lock and formats outside it (same pattern as
services/app/webhooks/metrics.py).
"""
from __future__ import annotations

import math
import threading
import time
from typing import Callable, Mapping, Sequence


# Default latency buckets (seconds). Chosen to cover sub-10ms cache hits
# through 60s LLM/backfill calls.
DEFAULT_BUCKETS: tuple[float, ...] = (
    0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5,
    1.0, 2.5, 5.0, 10.0, 30.0, 60.0,
)


def _escape_label_value(value: str) -> str:
    return (
        str(value)
        .replace("\\", "\\\\")
        .replace('"', '\\"')
        .replace("\n", "\\n")
    )


def _format_value(v: float) -> str:
    if v == math.inf:
        return "+Inf"
    if v == -math.inf:
        return "-Inf"
    if isinstance(v, float) and v.is_integer():
        return str(int(v))
    return repr(float(v)) if isinstance(v, float) else str(v)


def _label_str(label_names: Sequence[str], label_values: Sequence[str]) -> str:
    if not label_names:
        return ""
    inner = ",".join(
        f'{n}="{_escape_label_value(v)}"'
        for n, v in zip(label_names, label_values)
    )
    return "{" + inner + "}"


class _Family:
    """Base for one named metric family with a fixed label-name tuple."""

    kind = "untyped"

    def __init__(self, name: str, help_text: str, label_names: Sequence[str] = ()) -> None:
        self.name = name
        self.help = help_text
        self.label_names = tuple(label_names)
        self._lock = threading.Lock()

    def _key(self, labels: Mapping[str, str]) -> tuple[str, ...]:
        if set(labels) != set(self.label_names):
            raise ValueError(
                f"{self.name}: expected labels {self.label_names}, got "
                f"{tuple(sorted(labels))}"
            )
        return tuple(str(labels[n]) for n in self.label_names)

    def _header(self) -> list[str]:
        return [f"# HELP {self.name} {self.help}", f"# TYPE {self.name} {self.kind}"]

    def render(self) -> list[str]:  # pragma: no cover - overridden
        raise NotImplementedError


class Counter(_Family):
    kind = "counter"

    def __init__(self, name: str, help_text: str, label_names: Sequence[str] = ()) -> None:
        super().__init__(name, help_text, label_names)
        self._values: dict[tuple[str, ...], float] = {}

    def inc(self, value: float = 1.0, **labels: str) -> None:
        if value < 0:
            raise ValueError(f"{self.name}: counters only go up")
        key = self._key(labels)
        with self._lock:
            self._values[key] = self._values.get(key, 0.0) + value

    def get(self, **labels: str) -> float:
        with self._lock:
            return self._values.get(self._key(labels), 0.0)

    def reset(self) -> None:
        with self._lock:
            self._values.clear()

    def render(self) -> list[str]:
        with self._lock:
            items = sorted(self._values.items())
        out = self._header()
        for key, value in items:
            out.append(
                f"{self.name}{_label_str(self.label_names, key)} "
                f"{_format_value(value)}"
            )
        return out


class Gauge(_Family):
    kind = "gauge"

    def __init__(self, name: str, help_text: str, label_names: Sequence[str] = ()) -> None:
        super().__init__(name, help_text, label_names)
        self._values: dict[tuple[str, ...], float] = {}

    def set(self, value: float, **labels: str) -> None:
        key = self._key(labels)
        with self._lock:
            self._values[key] = float(value)

    def inc(self, value: float = 1.0, **labels: str) -> None:
        key = self._key(labels)
        with self._lock:
            self._values[key] = self._values.get(key, 0.0) + value

    def get(self, **labels: str) -> float:
        with self._lock:
            return self._values.get(self._key(labels), 0.0)

    def reset(self) -> None:
        with self._lock:
            self._values.clear()

    def render(self) -> list[str]:
        with self._lock:
            items = sorted(self._values.items())
        out = self._header()
        for key, value in items:
            out.append(
                f"{self.name}{_label_str(self.label_names, key)} "
                f"{_format_value(value)}"
            )
        return out


class Histogram(_Family):
    """Cumulative-bucket histogram (`_bucket{le=...}`, `_sum`, `_count`)."""

    kind = "histogram"

    def __init__(
        self,
        name: str,
        help_text: str,
        label_names: Sequence[str] = (),
        buckets: Sequence[float] = DEFAULT_BUCKETS,
    ) -> None:
        super().__init__(name, help_text, label_names)
        bks = tuple(sorted(float(b) for b in buckets))
        if not bks:
            raise ValueError(f"{self.name}: at least one bucket required")
        self.buckets = bks
        # per labelset: ([count per bucket], sum, count)
        self._series: dict[tuple[str, ...], tuple[list[int], float, int]] = {}

    def observe(self, value: float, **labels: str) -> None:
        key = self._key(labels)
        with self._lock:
            counts, total, n = self._series.get(
                key, ([0] * len(self.buckets), 0.0, 0)
            )
            for i, b in enumerate(self.buckets):
                if value <= b:
                    counts[i] += 1
                    break
            # values above the top bucket only land in +Inf (count).
            self._series[key] = (counts, total + float(value), n + 1)

    def get_count(self, **labels: str) -> int:
        with self._lock:
            series = self._series.get(self._key(labels))
            return series[2] if series else 0

    def get_sum(self, **labels: str) -> float:
        with self._lock:
            series = self._series.get(self._key(labels))
            return series[1] if series else 0.0

    def reset(self) -> None:
        with self._lock:
            self._series.clear()

    def render(self) -> list[str]:
        with self._lock:
            items = sorted(
                (k, (list(c), s, n)) for k, (c, s, n) in self._series.items()
            )
        out = self._header()
        for key, (counts, total, n) in items:
            cumulative = 0
            for i, b in enumerate(self.buckets):
                cumulative += counts[i]
                labels = _label_str(
                    self.label_names + ("le",), key + (_format_value(b),)
                )
                out.append(f"{self.name}_bucket{labels} {cumulative}")
            inf_labels = _label_str(self.label_names + ("le",), key + ("+Inf",))
            out.append(f"{self.name}_bucket{inf_labels} {n}")
            base = _label_str(self.label_names, key)
            out.append(f"{self.name}_sum{base} {_format_value(total)}")
            out.append(f"{self.name}_count{base} {n}")
        return out


class Registry:
    """Process-wide family registry + scrape-time collectors.

    Collectors are zero-arg callables returning pre-rendered exposition
    text; they let scrape-time data (pool stats, aggregated per-source
    counters) appear without a background sampler. A collector that
    raises is skipped — scraping must never 500 because one subsystem
    is mid-shutdown.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._families: dict[str, _Family] = {}
        self._collectors: list[Callable[[], str]] = []

    def _get_or_create(self, cls: type, name: str, help_text: str,
                       label_names: Sequence[str], **kwargs) -> _Family:
        with self._lock:
            existing = self._families.get(name)
            if existing is not None:
                if not isinstance(existing, cls) or existing.label_names != tuple(label_names):
                    raise ValueError(
                        f"metric {name!r} re-registered with a different "
                        f"type or label set"
                    )
                return existing
            fam = cls(name, help_text, label_names, **kwargs)
            self._families[name] = fam
            return fam

    def counter(self, name: str, help_text: str,
                label_names: Sequence[str] = ()) -> Counter:
        return self._get_or_create(Counter, name, help_text, label_names)  # type: ignore[return-value]

    def gauge(self, name: str, help_text: str,
              label_names: Sequence[str] = ()) -> Gauge:
        return self._get_or_create(Gauge, name, help_text, label_names)  # type: ignore[return-value]

    def histogram(self, name: str, help_text: str,
                  label_names: Sequence[str] = (),
                  buckets: Sequence[float] = DEFAULT_BUCKETS) -> Histogram:
        return self._get_or_create(  # type: ignore[return-value]
            Histogram, name, help_text, label_names, buckets=buckets
        )

    def add_collector(self, fn: Callable[[], str]) -> None:
        with self._lock:
            if fn not in self._collectors:
                self._collectors.append(fn)

    def remove_collector(self, fn: Callable[[], str]) -> None:
        with self._lock:
            if fn in self._collectors:
                self._collectors.remove(fn)

    def render_text(self) -> str:
        with self._lock:
            families = sorted(self._families.values(), key=lambda f: f.name)
            collectors = list(self._collectors)
        lines: list[str] = []
        for fam in families:
            rendered = fam.render()
            # Skip empty families (header-only) to keep scrapes lean.
            if len(rendered) > 2:
                lines.extend(rendered)
        for fn in collectors:
            try:
                text = fn()
            except Exception:  # noqa: BLE001 — scrape must not 500
                continue
            if text:
                lines.append(text.rstrip("\n"))
        return ("\n".join(lines) + "\n") if lines else ""

    def reset_for_tests(self) -> None:
        with self._lock:
            families = list(self._families.values())
        for fam in families:
            fam.reset()  # type: ignore[attr-defined]


_DEFAULT = Registry()


def default_registry() -> Registry:
    return _DEFAULT


def counter(name: str, help_text: str, label_names: Sequence[str] = ()) -> Counter:
    return _DEFAULT.counter(name, help_text, label_names)


def gauge(name: str, help_text: str, label_names: Sequence[str] = ()) -> Gauge:
    return _DEFAULT.gauge(name, help_text, label_names)


def histogram(name: str, help_text: str, label_names: Sequence[str] = (),
              buckets: Sequence[float] = DEFAULT_BUCKETS) -> Histogram:
    return _DEFAULT.histogram(name, help_text, label_names, buckets)


def render_default() -> str:
    """Render the default registry (families + collectors)."""
    return _DEFAULT.render_text()


def reset_default_for_tests() -> None:
    _DEFAULT.reset_for_tests()


# ---------------------------------------------------------------------
# Schema-version ledger metrics (BYOC §12 G1).
#
# Singletons defined on the default registry so the migration runner can
# import-and-set them at the site where it applies migrations, and every
# worker that serves /metrics (each renders render_default()) exposes the
# deployment's schema state to the fleet control plane. Cross-deployment,
# no tenant labels — same cardinality rules as the rest of this module.
# ---------------------------------------------------------------------
SCHEMA_VERSION = gauge(
    "fyralis_schema_version",
    "Highest applied db/migrations numeric prefix (monotonic schema version). "
    "0 if the ledger is empty / unreadable.",
)
SCHEMA_APPLIED_TOTAL = gauge(
    "fyralis_schema_applied_count",
    "Number of rows in the schema_migrations ledger (migrations applied).",
)
SCHEMA_LAST_FAILED = gauge(
    "fyralis_schema_last_failed_migration",
    "1 while the most recent migration apply attempt FAILED (per `filename` "
    "label); cleared to 0 once that file applies cleanly. A sustained 1 means "
    "the deployment is wedged on a broken/pending migration.",
    ("filename",),
)


# ---------------------------------------------------------------------
# Data-loss counters (BYOC §12 G6).
#
# Promote the two silent-data-loss LOG lines — producer flush-undelivered on
# shutdown and the observation_writer shadow-drop — to real counters so the
# fleet control plane can alert on data loss instead of grepping logs. Defined
# here (not at each call site) per the BYOC instrumentation track so the
# control-plane SLI build has one canonical name to scrape. Incremented at the
# exact sites: kafka/producer.py:stop() and writers/observation_writer.py.
# ---------------------------------------------------------------------
KAFKA_PRODUCER_SHUTDOWN_UNDELIVERED = counter(
    "fyralis_kafka_producer_shutdown_undelivered_total",
    "Messages still undelivered when the idempotent producer was stopped "
    "(flush timed out on shutdown) — silent loss on restart. Sum across stops.",
)
WRITER_SHADOW_DROP = counter(
    "fyralis_writer_shadow_drop_total",
    "Envelopes that reached the writer shadow path and were DROPPED (no row, "
    "no DLQ, offset committed). ingress_kind=backfill is an INVARIANT VIOLATION "
    "(silent data loss); live is by-design (inline path persists it).",
    ("ingress_kind",),
)


# Process start marker for *_uptime gauges rendered by collectors.
PROCESS_STARTED_AT = time.time()


__all__ = [
    "DEFAULT_BUCKETS",
    "Counter",
    "Gauge",
    "Histogram",
    "Registry",
    "counter",
    "gauge",
    "histogram",
    "default_registry",
    "render_default",
    "reset_default_for_tests",
    "PROCESS_STARTED_AT",
    "SCHEMA_VERSION",
    "SCHEMA_APPLIED_TOTAL",
    "SCHEMA_LAST_FAILED",
    "KAFKA_PRODUCER_SHUTDOWN_UNDELIVERED",
    "WRITER_SHADOW_DROP",
]
