"""services/ingest/integrations/metrics_export.py — per-source counters → Prometheus.

The 19 integration `metrics.py` modules record install/request/provision
outcomes in-process but were never rendered to any `/metrics` endpoint
("trapped in logs"). This module aggregates them all into the shared
`lib.observability` default registry as a scrape-time collector, so every
process that serves a metrics endpoint (gateway, ingestion workers) exposes
whatever per-source counters live in that process.

Normalization: the simple per-source families collapse into one
`integration_*` namespace with a bounded `source` label —

  <source>.request.<outcome>    → integration_requests_total{source,outcome}
  <source>.provision.<outcome>  → integration_provision_total{source,outcome}
  install / uninstall outcomes  → integration_install_total / integration_uninstall_total
  fetch events                  → integration_fetch_total{source,event}
  install duration samples      → integration_install_duration_p95_seconds{source}

— so one PromQL expression covers all sources. GitHub keeps its richer
FR-017 names (`github_webhook_*`, `github_outbound_*`, …) verbatim, as does
the Discord Gateway worker (`discord_gateway_*`).

This collector reads each module's documented-internal counter dicts under
their own locks; `tests/test_metrics_export.py` pins one family per shape so
an internal rename fails loudly instead of silently dropping a source.
Modules that fail to import or read are skipped — a scrape must never 500.
"""
from __future__ import annotations

import importlib
import threading
from typing import Any, Iterable

from lib.observability.metrics import default_registry


# Sources whose metrics module follows the flat-snapshot() shape
# (`<source>.request.<outcome>` / `<source>.provision.<outcome>` keys).
_SNAPSHOT_SOURCES = (
    "ashby", "brex", "carta", "deel", "figma", "fireflies", "gusto",
    "hibob", "linkedin", "mercury", "miro", "quickbooks", "ramp",
)

_PKG = "services.ingest.integrations"


def _module(name: str) -> Any | None:
    try:
        return importlib.import_module(f"{_PKG}.{name}.metrics")
    except Exception:  # noqa: BLE001 — optional source not on this deploy
        return None


def _gateway_module() -> Any | None:
    try:
        return importlib.import_module(f"{_PKG}.discord.gateway.metrics")
    except Exception:  # noqa: BLE001
        return None


def _p95(samples: list[float]) -> float | None:
    if not samples:
        return None
    ordered = sorted(samples)
    n = len(ordered)
    idx = max(0, min(n - 1, -(-95 * n // 100) - 1))
    return ordered[idx]


class _FamilyAcc:
    """Accumulates samples for one family, rendered as a grouped block."""

    def __init__(self, name: str, kind: str, help_text: str,
                 label_names: tuple[str, ...]) -> None:
        self.name = name
        self.kind = kind
        self.help = help_text
        self.label_names = label_names
        self.samples: list[tuple[tuple[str, ...], float]] = []

    def add(self, label_values: tuple[str, ...], value: float) -> None:
        self.samples.append((label_values, value))

    def render(self) -> list[str]:
        if not self.samples:
            return []
        out = [f"# HELP {self.name} {self.help}",
               f"# TYPE {self.name} {self.kind}"]
        for values, v in sorted(self.samples):
            labels = ",".join(
                f'{n}="{_esc(val)}"'
                for n, val in zip(self.label_names, values)
            )
            label_str = "{" + labels + "}" if labels else ""
            out.append(f"{self.name}{label_str} {v:g}")
        return out


def _esc(value: str) -> str:
    return (
        str(value)
        .replace("\\", "\\\\")
        .replace('"', '\\"')
        .replace("\n", "\\n")
    )


def _families() -> dict[str, _FamilyAcc]:
    return {
        "requests": _FamilyAcc(
            "integration_requests_total", "counter",
            "Outbound API request outcomes per source.", ("source", "outcome")),
        "provision": _FamilyAcc(
            "integration_provision_total", "counter",
            "Provisioning outcomes per source.", ("source", "outcome")),
        "install": _FamilyAcc(
            "integration_install_total", "counter",
            "OAuth install outcomes per source.", ("source", "outcome")),
        "uninstall": _FamilyAcc(
            "integration_uninstall_total", "counter",
            "Uninstall outcomes per source.", ("source", "outcome")),
        "fetch": _FamilyAcc(
            "integration_fetch_total", "counter",
            "Fetch/poll events per source.", ("source", "event")),
        "install_p95": _FamilyAcc(
            "integration_install_duration_p95_seconds", "gauge",
            "p95 install duration per source (rolling sample window).",
            ("source",)),
        "other": _FamilyAcc(
            "integration_counter_total", "counter",
            "Per-source counters with no normalized family.", ("source", "key")),
    }


def _collect_snapshot_sources(fams: dict[str, _FamilyAcc]) -> None:
    for source in _SNAPSHOT_SOURCES:
        mod = _module(source)
        if mod is None or not hasattr(mod, "snapshot"):
            continue
        try:
            snap = mod.snapshot()
        except Exception:  # noqa: BLE001
            continue
        for key, value in snap.items():
            parts = str(key).split(".")
            if len(parts) == 3 and parts[1] == "request":
                fams["requests"].add((parts[0], parts[2]), float(value))
            elif len(parts) == 3 and parts[1] == "provision":
                fams["provision"].add((parts[0], parts[2]), float(value))
            else:
                fams["other"].add((source, str(key)), float(value))


def _read_locked(mod: Any, attr: str) -> dict | list | None:
    data = getattr(mod, attr, None)
    if data is None:
        return None
    lock = getattr(mod, "_lock", None) or getattr(mod, "_LOCK", None)
    if isinstance(lock, type(threading.Lock())):
        with lock:
            return dict(data) if isinstance(data, dict) else list(data)
    return dict(data) if isinstance(data, dict) else list(data)


def _collect_install_shaped(fams: dict[str, _FamilyAcc]) -> None:
    # slack / discord: install + uninstall outcomes + duration samples.
    for source in ("slack", "discord"):
        mod = _module(source)
        if mod is None:
            continue
        installs = _read_locked(mod, "_install_outcomes") or {}
        for outcome, n in installs.items():
            fams["install"].add((source, outcome), float(n))
        uninstalls = _read_locked(mod, "_uninstall_outcomes") or {}
        for outcome, n in uninstalls.items():
            fams["uninstall"].add((source, outcome), float(n))
        durations = _read_locked(mod, "_install_durations_s") or []
        p95 = _p95(list(durations))
        if p95 is not None:
            fams["install_p95"].add((source,), p95)

    # notion: install outcomes + fetch events.
    notion = _module("notion")
    if notion is not None:
        for outcome, n in (_read_locked(notion, "_install_outcomes") or {}).items():
            fams["install"].add(("notion", outcome), float(n))
        for event, n in (_read_locked(notion, "_fetch_counts") or {}).items():
            fams["fetch"].add(("notion", event), float(n))

    # google_calendar / google_drive: provision outcomes + fetch events.
    for source in ("google_calendar", "google_drive"):
        mod = _module(source)
        if mod is None:
            continue
        for outcome, n in (_read_locked(mod, "_provision_outcomes") or {}).items():
            fams["provision"].add((source, outcome), float(n))
        for event, n in (_read_locked(mod, "_fetch_counts") or {}).items():
            fams["fetch"].add((source, event), float(n))


def _render_labeled_dict(
    counters: dict[tuple[str, Iterable[tuple[str, str]]], float],
    kind: str,
) -> list[str]:
    """Render {(name, ((label, value), ...)): n} preserving original names,
    grouped per family (GitHub + Discord Gateway shapes)."""
    by_name: dict[str, list[tuple[tuple[tuple[str, str], ...], float]]] = {}
    for (name, labels), n in counters.items():
        by_name.setdefault(name, []).append((tuple(sorted(labels)), float(n)))
    out: list[str] = []
    for name in sorted(by_name):
        out.append(f"# TYPE {name} {kind}")
        for labels, n in sorted(by_name[name]):
            label_str = ",".join(f'{k}="{_esc(v)}"' for k, v in labels)
            out.append(f"{name}{{{label_str}}} {n:g}" if label_str
                       else f"{name} {n:g}")
    return out


def _collect_github() -> list[str]:
    mod = _module("github")
    if mod is None:
        return []
    lock = getattr(mod, "_LOCK", None)
    raw_counters = getattr(mod, "_COUNTERS", None)
    raw_hist = getattr(mod, "_HIST", None)
    if raw_counters is None:
        return []
    if lock is not None:
        with lock:
            counters = dict(raw_counters)
            hist = {k: list(v) for k, v in (raw_hist or {}).items()}
    else:  # pragma: no cover — module always has _LOCK today
        counters = dict(raw_counters)
        hist = {k: list(v) for k, v in (raw_hist or {}).items()}
    out = _render_labeled_dict(counters, "counter")
    for name in sorted(hist):
        p95 = _p95(hist[name])
        if p95 is not None:
            out.append(f"# TYPE {name}_p95 gauge")
            out.append(f"{name}_p95 {p95:g}")
    return out


def _collect_discord_gateway() -> list[str]:
    mod = _gateway_module()
    if mod is None:
        return []
    raw_counters = getattr(mod, "_counters", None)
    raw_gauges = getattr(mod, "_gauges", None)
    if raw_counters is None:
        return []
    counters = {
        (name, tuple(sorted(labels))): float(v)
        for (name, labels), v in dict(raw_counters).items()
    }
    gauges = {
        (name, tuple(sorted(labels))): float(v)
        for (name, labels), v in dict(raw_gauges or {}).items()
    }
    out = _render_labeled_dict(counters, "counter")
    out += _render_labeled_dict(gauges, "gauge")
    return out


def render_integration_metrics() -> str:
    """Scrape-time collector for the shared default registry."""
    fams = _families()
    _collect_snapshot_sources(fams)
    _collect_install_shaped(fams)
    lines: list[str] = []
    for fam in fams.values():
        lines.extend(fam.render())
    lines.extend(_collect_github())
    lines.extend(_collect_discord_gateway())
    return ("\n".join(lines) + "\n") if lines else ""


default_registry().add_collector(render_integration_metrics)


__all__ = ["render_integration_metrics"]
