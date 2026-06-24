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
]


# --- doc_memory metrics (document-memory-substrate) ---
# Phase 2 (proactive + observability) of the document-memory substrate
# (docs/plans/document-memory-substrate.md §7 step 12 / §10). Registered on the
# default registry as module-level singletons (same pattern as the per-module
# instrumentation in services/reasoning/retrieval/* and kafka/producer.py) so
# they render via render_default() with no extra wiring. Conceptual dotted names
# from the plan (doc_memory.models_minted / .scope_unresolved / .mint_failure)
# map to snake_case Prometheus family names with the `_total` counter suffix the
# rest of the codebase uses. The worker-side DISPATCH count is exposed separately
# as doc_memory_enriched_t1_total (renamed off the former, misleading
# doc_memory_models_minted_total) while doc_memory_models_minted_total now means
# the TRUE mint count — see the two definitions below. Cardinality: no
# tenant_id / id labels (§4 rule) —
# the `source` label is a bounded enum over the doc-memory channel allowlist
# (google_drive / notion / fireflies / other).
#
# Appended as a self-contained block at EOF so this addition never overlaps the
# file body other tracks edit (zero-conflict guarantee).

# doc_memory.enriched_t1 — a summarized document was handed to Think to mint
# document Models from (the worker enriched a T1 with the structured summary +
# resolved scope). Under the ratified Option A, Think — not the worker — is the
# Model author, so this is the DISPATCH count, NOT a mint count. It remains a
# useful signal (documents handed to Think for distillation) and is the
# denominator for a `doc → models_minted` conversion rate (§4.1) once the true
# mint counter below is wired at Think's apply site. Renamed from the former,
# misleading `doc_memory_models_minted_total` (which measured dispatches).
DOC_MEMORY_ENRICHED_T1 = counter(
    "doc_memory_enriched_t1_total",
    "Documents dispatched to Think on an enriched T1 to mint document Models "
    "from (structured summary + re-resolved scope), by source channel. This is "
    "a DISPATCH count — Think mints the Models later (Option A).",
    ("source",),
)

# doc_memory.models_minted — the TRUE mint counter: a document-derived Model was
# actually minted by Think (a Model whose born_from_event_id is the document
# observation — i.e. document provenance, §4.4). Distinct from the dispatch
# counter above: this counts Models that Think genuinely inserted, so
# `models_minted / enriched_t1` is the real distillation conversion rate.
#
# WIRING NOTE (cross-branch constraint): under ratified Option A the only place a
# document-derived Model is actually inserted is Think's apply path
# (`services/reasoning/think/applier.py::_apply_claim_insert`, via
# `ModelsRepo.insert`). That file is owned by the parallel reasoning/BYOC track
# and is NOT one of the document-memory-substrate files — wiring the increment
# there would create a new cross-branch intersection. The increment helper
# `record_doc_memory_model_minted()` below is therefore provided ready-to-call
# (one line, keyed on document provenance) so whichever track owns the applier
# can drop it in at the mint site without depending on this module's internals.
# See docs/plans/document-memory-substrate.md §4.1 / §4.4.
DOC_MEMORY_MODELS_MINTED = counter(
    "doc_memory_models_minted_total",
    "Document-derived Models actually minted by Think (born_from_event_id is the "
    "document observation), by source channel. The real mint count (Option A).",
    ("source",),
)


def record_doc_memory_model_minted(source_channel: str | None) -> None:
    """Record that Think actually minted a document-derived Model.

    Call this from the Think apply site exactly once per inserted Model whose
    provenance is a document observation (born_from_event_id = the document
    observation). Kept as a tiny helper so the call site stays a single line and
    does not need to know the metric's label shape. See the WIRING NOTE above for
    why the call site itself is not wired from this module.
    """
    DOC_MEMORY_MODELS_MINTED.inc(source=doc_memory_source_label(source_channel))

# doc_memory.scope_unresolved — re-resolution ran but produced NO scoped recall
# surface (no resolved entities and no resolved actors), so the document Models
# will fall back to semantic-only (Pathway B) recall. Watched as a rate (§10).
DOC_MEMORY_SCOPE_UNRESOLVED = counter(
    "doc_memory_scope_unresolved_total",
    "Documents whose structured-summary scope re-resolution found no entities "
    "or actors (scoped recall degrades to semantic-only), by source channel.",
    ("source",),
)

# doc_memory.mint_failure — the mint path failed in the worker (scope
# re-resolution raised); strictly failure-isolated so the summary still lands.
# The §10 alert (DocMemoryMintFailure) fires on a sustained rate of this.
DOC_MEMORY_MINT_FAILURE = counter(
    "doc_memory_mint_failure_total",
    "Document-memory mint-path failures in the summarization worker "
    "(re-resolution error; summary still succeeds), by source channel.",
    ("source",),
)

# map-reduce section counts — distribution of how many sections a large document
# was split into during map-reduce summarization (Layer 0, §3.2). A histogram so
# both the section-count distribution and the count of map-reduced documents are
# queryable. Small integer buckets sized to the realistic section fan-out.
DOC_MEMORY_MAPREDUCE_SECTIONS = histogram(
    "doc_memory_mapreduce_sections",
    "Number of sections a document was split into for map-reduce "
    "summarization (Layer 0). +Inf bucket counts map-reduced documents.",
    buckets=(1, 2, 3, 5, 8, 13, 21, 34, 55),
)


def doc_memory_source_label(source_channel: str | None) -> str:
    """Collapse a free-form source_channel to a bounded doc-memory label value.

    Keeps metric cardinality bounded (§4): the channel allowlist is
    google_drive:file / notion:object / fireflies:transcript; anything else
    (or missing) buckets to "other".
    """
    if not source_channel:
        return "other"
    head = source_channel.split(":", 1)[0].strip().lower()
    if head in {"google_drive", "notion", "fireflies"}:
        return head
    return "other"


__all__ += [
    "DOC_MEMORY_ENRICHED_T1",
    "DOC_MEMORY_MODELS_MINTED",
    "DOC_MEMORY_SCOPE_UNRESOLVED",
    "DOC_MEMORY_MINT_FAILURE",
    "DOC_MEMORY_MAPREDUCE_SECTIONS",
    "doc_memory_source_label",
    "record_doc_memory_model_minted",
]
