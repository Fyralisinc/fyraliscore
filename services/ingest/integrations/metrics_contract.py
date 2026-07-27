"""Source-owned metric export values consumed by the shared scrape collector.

Integration metric modules own access to their private counters and expose one
catalog-bound ``export_metrics(source_id)`` callable.  The shared collector
only groups the returned samples; it never needs to know which sources exist
or which private counter shape a source uses.
"""
from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import Literal


MetricKind = Literal["counter", "gauge"]
MetricLabels = tuple[tuple[str, str], ...]


@dataclass(frozen=True, slots=True)
class MetricSample:
    """One Prometheus sample with immutable family metadata."""

    name: str
    kind: MetricKind
    labels: MetricLabels
    value: float
    help_text: str | None = None


MetricsExporter = Callable[[str], tuple[MetricSample, ...]]

_REQUEST_HELP = "Outbound API request outcomes per source."
_PROVISION_HELP = "Provisioning outcomes per source."
_INSTALL_HELP = "OAuth install outcomes per source."
_UNINSTALL_HELP = "Uninstall outcomes per source."
_FETCH_HELP = "Fetch/poll events per source."
_INSTALL_P95_HELP = "p95 install duration per source (rolling sample window)."
_OTHER_HELP = "Per-source counters with no normalized family."


def _sample(
    name: str,
    kind: MetricKind,
    labels: Iterable[tuple[str, object]],
    value: object,
    *,
    help_text: str | None = None,
) -> MetricSample:
    return MetricSample(
        name=name,
        kind=kind,
        labels=tuple((key, str(label_value)) for key, label_value in labels),
        value=float(value),
        help_text=help_text,
    )


def make_snapshot_exporter(
    snapshot: Callable[[], Mapping[str, int | float]],
) -> MetricsExporter:
    """Adapt a source's flat ``<source>.<family>.<label>`` snapshot."""

    def export_metrics(source_id: str) -> tuple[MetricSample, ...]:
        samples: list[MetricSample] = []
        for key, value in snapshot().items():
            parts = str(key).split(".")
            if len(parts) == 3 and parts[1] == "request":
                samples.append(
                    _sample(
                        "integration_requests_total",
                        "counter",
                        (("source", parts[0]), ("outcome", parts[2])),
                        value,
                        help_text=_REQUEST_HELP,
                    )
                )
            elif len(parts) == 3 and parts[1] == "provision":
                samples.append(
                    _sample(
                        "integration_provision_total",
                        "counter",
                        (("source", parts[0]), ("outcome", parts[2])),
                        value,
                        help_text=_PROVISION_HELP,
                    )
                )
            else:
                samples.append(
                    _sample(
                        "integration_counter_total",
                        "counter",
                        (("source", source_id), ("key", key)),
                        value,
                        help_text=_OTHER_HELP,
                    )
                )
        return tuple(samples)

    return export_metrics


def export_install_metrics(
    source_id: str,
    *,
    install_outcomes: Mapping[str, int | float],
    uninstall_outcomes: Mapping[str, int | float],
    install_durations_s: Sequence[float],
) -> tuple[MetricSample, ...]:
    """Normalize OAuth install state copied under the source module's lock."""

    samples = [
        _sample(
            "integration_install_total",
            "counter",
            (("source", source_id), ("outcome", outcome)),
            value,
            help_text=_INSTALL_HELP,
        )
        for outcome, value in install_outcomes.items()
    ]
    samples.extend(
        _sample(
            "integration_uninstall_total",
            "counter",
            (("source", source_id), ("outcome", outcome)),
            value,
            help_text=_UNINSTALL_HELP,
        )
        for outcome, value in uninstall_outcomes.items()
    )
    p95 = percentile_95(install_durations_s)
    if p95 is not None:
        samples.append(
            _sample(
                "integration_install_duration_p95_seconds",
                "gauge",
                (("source", source_id),),
                p95,
                help_text=_INSTALL_P95_HELP,
            )
        )
    return tuple(samples)


def export_install_fetch_metrics(
    source_id: str,
    *,
    install_outcomes: Mapping[str, int | float],
    fetch_counts: Mapping[str, int | float],
) -> tuple[MetricSample, ...]:
    """Normalize install outcomes and fetch event counters."""

    samples = [
        _sample(
            "integration_install_total",
            "counter",
            (("source", source_id), ("outcome", outcome)),
            value,
            help_text=_INSTALL_HELP,
        )
        for outcome, value in install_outcomes.items()
    ]
    samples.extend(
        _sample(
            "integration_fetch_total",
            "counter",
            (("source", source_id), ("event", event)),
            value,
            help_text=_FETCH_HELP,
        )
        for event, value in fetch_counts.items()
    )
    return tuple(samples)


def export_provision_fetch_metrics(
    source_id: str,
    *,
    provision_outcomes: Mapping[str, int | float],
    fetch_counts: Mapping[str, int | float],
) -> tuple[MetricSample, ...]:
    """Normalize provisioning outcomes and fetch event counters."""

    samples = [
        _sample(
            "integration_provision_total",
            "counter",
            (("source", source_id), ("outcome", outcome)),
            value,
            help_text=_PROVISION_HELP,
        )
        for outcome, value in provision_outcomes.items()
    ]
    samples.extend(
        _sample(
            "integration_fetch_total",
            "counter",
            (("source", source_id), ("event", event)),
            value,
            help_text=_FETCH_HELP,
        )
        for event, value in fetch_counts.items()
    )
    return tuple(samples)


def export_labeled_metrics(
    *,
    counters: Mapping[tuple[str, Iterable[tuple[str, str]]], int | float],
    gauges: Mapping[tuple[str, Iterable[tuple[str, str]]], int | float] | None = None,
    histograms: Mapping[str, Sequence[float]] | None = None,
) -> tuple[MetricSample, ...]:
    """Preserve provider-owned metric names while copying labeled state."""

    samples: list[MetricSample] = []
    for (name, labels), value in sorted(
        counters.items(),
        key=lambda item: (item[0][0], tuple(sorted(item[0][1]))),
    ):
        samples.append(
            _sample(
                name,
                "counter",
                tuple(sorted(labels)),
                value,
            )
        )
    for (name, labels), value in sorted(
        (gauges or {}).items(),
        key=lambda item: (item[0][0], tuple(sorted(item[0][1]))),
    ):
        samples.append(
            _sample(
                name,
                "gauge",
                tuple(sorted(labels)),
                value,
            )
        )
    for name, values in sorted((histograms or {}).items()):
        p95 = percentile_95(values)
        if p95 is not None:
            samples.append(_sample(f"{name}_p95", "gauge", (), p95))
    return tuple(samples)


def percentile_95(samples: Sequence[float]) -> float | None:
    """Return the same nearest-rank p95 used by the legacy exporter."""

    if not samples:
        return None
    ordered = sorted(samples)
    count = len(ordered)
    index = max(0, min(count - 1, -(-95 * count // 100) - 1))
    return float(ordered[index])


__all__ = [
    "MetricKind",
    "MetricLabels",
    "MetricSample",
    "MetricsExporter",
    "export_install_fetch_metrics",
    "export_install_metrics",
    "export_labeled_metrics",
    "export_provision_fetch_metrics",
    "make_snapshot_exporter",
    "percentile_95",
]
