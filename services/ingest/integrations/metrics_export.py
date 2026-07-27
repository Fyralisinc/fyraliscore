"""Render source-owned integration counters into the shared registry.

Metric producers and their private counter adapters are declared by each
``SourceDefinition.metrics_export_bindings`` entry.  This scrape-time
collector resolves those callables lazily, asks each one for immutable
``MetricSample`` values, and groups matching Prometheus families.

The collector has no source registry or source-specific dispatch. Optional
integration modules and failing exporters are skipped so a scrape never fails
because a source is absent from a particular deployment.
"""
from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass

from lib.observability.metrics import default_registry
from services.ingest.integrations.metrics_contract import MetricSample
from services.ingest.source_contract.catalog import SOURCE_DEFINITIONS
from services.ingest.source_contract.runtime import resolve_callable_reference


_NORMALIZED_FAMILY_ORDER = (
    "integration_requests_total",
    "integration_provision_total",
    "integration_install_total",
    "integration_uninstall_total",
    "integration_fetch_total",
    "integration_install_duration_p95_seconds",
    "integration_counter_total",
)
_NORMALIZED_FAMILY_RANK = {
    family: rank for rank, family in enumerate(_NORMALIZED_FAMILY_ORDER)
}


@dataclass(slots=True)
class _Family:
    """One validated Prometheus family accumulated across source exporters."""

    name: str
    kind: str
    help_text: str | None
    first_seen: int
    samples: list[MetricSample]

    def add(self, sample: MetricSample) -> None:
        if sample.kind != self.kind or sample.help_text != self.help_text:
            raise ValueError(
                f"metric family {self.name!r} has conflicting metadata"
            )
        self.samples.append(sample)

    def render(self) -> list[str]:
        if not self.samples:
            return []
        lines: list[str] = []
        if self.help_text is not None:
            lines.append(f"# HELP {self.name} {self.help_text}")
        lines.append(f"# TYPE {self.name} {self.kind}")
        for sample in sorted(self.samples, key=lambda item: item.labels):
            label_text = ",".join(
                f'{name}="{_escape(value)}"' for name, value in sample.labels
            )
            labels = f"{{{label_text}}}" if label_text else ""
            lines.append(f"{self.name}{labels} {sample.value:g}")
        return lines


def _escape(value: str) -> str:
    return (
        str(value)
        .replace("\\", "\\\\")
        .replace('"', '\\"')
        .replace("\n", "\\n")
    )


def _read_source_samples(
    source_id: str,
    bindings: Iterable[str],
) -> tuple[MetricSample, ...]:
    """Resolve one source's optional exporters without breaking a scrape."""

    samples: list[MetricSample] = []
    for binding in bindings:
        try:
            exporter = resolve_callable_reference(binding)
            exported = tuple(exporter(source_id))
            if not all(isinstance(sample, MetricSample) for sample in exported):
                raise TypeError(
                    f"metrics exporter {binding!r} returned a non-MetricSample"
                )
        except Exception:  # noqa: BLE001 - scrape availability is mandatory
            continue
        samples.extend(exported)
    return tuple(samples)


def render_integration_metrics() -> str:
    """Render every available catalog-owned metrics exporter."""

    families: dict[str, _Family] = {}
    first_seen = 0
    for source in SOURCE_DEFINITIONS:
        source_samples = _read_source_samples(
            source.source_id,
            source.metrics_export_bindings,
        )
        for sample in source_samples:
            family = families.get(sample.name)
            if family is None:
                family = _Family(
                    name=sample.name,
                    kind=sample.kind,
                    help_text=sample.help_text,
                    first_seen=first_seen,
                    samples=[],
                )
                families[sample.name] = family
                first_seen += 1
            try:
                family.add(sample)
            except (TypeError, ValueError):
                # A broken optional exporter must not make /metrics fail.
                continue

    ordered = sorted(
        families.values(),
        key=lambda family: (
            0
            if family.name in _NORMALIZED_FAMILY_RANK
            else 1,
            _NORMALIZED_FAMILY_RANK.get(family.name, family.first_seen),
        ),
    )
    lines: list[str] = []
    for family in ordered:
        lines.extend(family.render())
    return ("\n".join(lines) + "\n") if lines else ""


default_registry().add_collector(render_integration_metrics)


__all__ = ["render_integration_metrics"]
