"""mimir_client — a thin, per-tenant Mimir query client for usage metering (FR-F3).

The control plane's metering reads **aggregate Tier-1 metrics** out of the central
Grafana Mimir, one tenant at a time, by setting the **``X-Scope-OrgID``** header to the
tenant id on every request. That header is exactly how Mimir multi-tenancy is keyed
(SPRINT_PLAN.md C5 / WS-MIMIR README): the operator-side query path supplies
``X-Scope-OrgID: <tenant>`` so a tenant only ever sees its own series.

What we query
-------------
Metering only ever touches **counters/gauges that are already aggregate and PII-free**
(Invariant I1 — *No PII at T1*). Specifically:

  * ``writer_full_mode_writes_total{source=...}``  — observations written per source
    (the obs-per-source / ingestion-volume counter; same series the fleet-sli
    ``fyralis:ingest_write_rate:5m`` rule is built from).
  * ``think_runs_total``                            — reasoning runs executed.
  * ``think_cost_recent_usd_total``                 — cumulative LLM/think spend in USD
    (the ``cost_usd`` rollup; the series behind ``fyralis:llm_spend_usd_per_hour``).

These are Prometheus **cumulative counters**, so the usage *over a period* is the
``increase(<counter>[<period>])`` evaluated at the end of the period. We use Mimir's
**instant query** endpoint with an ``increase()`` window so a single instant query
returns the period delta (this also rides Mimir's downsampled blocks rather than pulling
raw range samples).

Endpoints (Prometheus-compatible, served by Mimir at ``/prometheus``):

    GET {base}/prometheus/api/v1/query?query=<promql>&time=<rfc3339|unix>
        Headers: X-Scope-OrgID: <tenant_id>

This module is **transport only** — it issues queries and parses the Prometheus
``vector`` result into plain Python. The rollup math lives in ``rollup.py``. The HTTP
client is :mod:`httpx` (already a control-plane dependency). The client is constructed
with the Mimir base URL (default the in-cluster ``http://mimir:9009`` the auth-proxy /
operator path uses) and is given the tenant's ``X-Scope-OrgID`` per call, so a single
client instance can roll up the whole fleet.
"""

from __future__ import annotations

import datetime as _dt
from dataclasses import dataclass
from typing import Any, Callable, Iterable, Mapping, Optional

import httpx

__all__ = [
    "DEFAULT_MIMIR_URL",
    "ORG_HEADER",
    "Sample",
    "MimirQueryError",
    "MimirClient",
    "period_to_promql_range",
]

# The auth-proxy upstream / operator query path. Mimir serves Prometheus-compatible
# query APIs under /prometheus (WS-MIMIR README). In the CP this is reached through the
# auth-proxy, but the metering job runs inside cp-net and may hit mimir:9009 directly,
# supplying X-Scope-OrgID itself (the same header the proxy would inject from the SAN).
DEFAULT_MIMIR_URL = "http://mimir:9009"

# Mimir multi-tenancy header (SPRINT_PLAN.md C5). NEVER trusted from outside cp-net.
ORG_HEADER = "X-Scope-OrgID"


class MimirQueryError(RuntimeError):
    """Raised when a Mimir query fails (HTTP error, non-``success`` status, bad shape)."""


@dataclass(frozen=True)
class Sample:
    """One element of a Prometheus instant-query ``vector`` result.

    ``labels`` is the metric label set (without ``__name__`` stripped — Mimir returns it
    as a label). ``value`` is the scalar sample value (already a float). ``timestamp`` is
    the evaluation instant (unix seconds, as Mimir returns it).
    """

    labels: Mapping[str, str]
    value: float
    timestamp: float

    def label(self, name: str, default: str = "") -> str:
        return self.labels.get(name, default)


def period_to_promql_range(start: _dt.datetime, end: _dt.datetime) -> str:
    """Render the inclusive ``[start, end]`` window as a PromQL range-vector duration.

    We always query ``increase(<counter>[<range>])`` evaluated **at ``end``**, so the
    range must span the whole period. Mimir/Prometheus range durations are integer-unit
    strings (e.g. ``744h``); we emit **seconds** (``<n>s``) which is always exact and
    avoids rounding a month down to whole hours. Raises ``ValueError`` if ``end<=start``.
    """
    start = _ensure_utc(start)
    end = _ensure_utc(end)
    secs = int((end - start).total_seconds())
    if secs <= 0:
        raise ValueError(f"period end ({end}) must be strictly after start ({start})")
    return f"{secs}s"


def _ensure_utc(dt: _dt.datetime) -> _dt.datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=_dt.timezone.utc)
    return dt.astimezone(_dt.timezone.utc)


# A QueryFn is the seam the self-test stubs: given (promql, tenant_id, eval_time) it
# returns the parsed list of Samples. The real implementation hits Mimir over httpx;
# the stub returns canned vectors. rollup.py depends only on this seam, never on httpx.
QueryFn = Callable[[str, str, _dt.datetime], list[Sample]]


class MimirClient:
    """Per-tenant instant-query client against the central Mimir.

    Parameters
    ----------
    base_url:
        Mimir base, default :data:`DEFAULT_MIMIR_URL` (``http://mimir:9009``). The
        ``/prometheus/api/v1/query`` path is appended.
    timeout:
        Per-request timeout in seconds.
    transport:
        Optional ``httpx.BaseTransport`` — the self-test injects an
        ``httpx.MockTransport`` so the *real* request/parse code path is exercised
        against a canned Mimir without a network or a running Mimir.
    extra_headers:
        Optional static headers (e.g. an operator bearer token if the query path is
        fronted by the auth-proxy). ``X-Scope-OrgID`` is always set per call and may not
        be overridden here.
    """

    def __init__(
        self,
        base_url: str = DEFAULT_MIMIR_URL,
        *,
        timeout: float = 30.0,
        transport: Optional[httpx.BaseTransport] = None,
        extra_headers: Optional[Mapping[str, str]] = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self._timeout = timeout
        headers = dict(extra_headers or {})
        # Guard: X-Scope-OrgID is set per-call from the verified tenant, never statically.
        headers.pop(ORG_HEADER, None)
        self._client = httpx.Client(
            base_url=self.base_url,
            timeout=timeout,
            transport=transport,
            headers=headers,
        )

    # -- lifecycle ---------------------------------------------------------- #

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> "MimirClient":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    # -- the one query primitive everything is built on --------------------- #

    def instant_query(
        self,
        promql: str,
        *,
        tenant_id: str,
        at: Optional[_dt.datetime] = None,
    ) -> list[Sample]:
        """Run an **instant query** ``promql`` for ``tenant_id`` at instant ``at``.

        Sets ``X-Scope-OrgID: <tenant_id>`` so the result is scoped to that tenant only.
        Returns the parsed ``vector`` as a list of :class:`Sample`. Raises
        :class:`MimirQueryError` on any HTTP / protocol / shape failure (fail-loud — a
        metering job must not silently bill zero because a query 500'd).
        """
        if not tenant_id:
            raise ValueError("tenant_id is required (it becomes X-Scope-OrgID)")
        at = _ensure_utc(at or _dt.datetime.now(_dt.timezone.utc))
        params = {"query": promql, "time": _to_unix(at)}
        headers = {ORG_HEADER: tenant_id}
        try:
            resp = self._client.get(
                "/prometheus/api/v1/query", params=params, headers=headers
            )
        except httpx.HTTPError as exc:  # transport-level failure
            raise MimirQueryError(
                f"Mimir query transport error for tenant {tenant_id!r}: {exc}"
            ) from exc

        if resp.status_code != 200:
            body = _safe_body(resp)
            raise MimirQueryError(
                f"Mimir query HTTP {resp.status_code} for tenant {tenant_id!r}: {body}"
            )
        return _parse_vector(resp.json(), tenant_id=tenant_id, promql=promql)

    def increase_over_period(
        self,
        metric_selector: str,
        *,
        tenant_id: str,
        range_promql: str,
        at: _dt.datetime,
        by: Optional[Iterable[str]] = None,
    ) -> list[Sample]:
        """``increase(<metric_selector>[<range>])`` over the period, evaluated at ``at``.

        ``metric_selector`` is a counter name with an optional label matcher, e.g.
        ``writer_full_mode_writes_total`` or ``writer_full_mode_writes_total{source="github"}``.
        ``range_promql`` is the period range (see :func:`period_to_promql_range`).
        ``by`` optionally wraps the increase in ``sum by (...)`` so per-source counters
        collapse to one sample per label tuple (e.g. ``by=("source",)`` for obs-per-source).

        Returns the resulting vector. An empty vector (tenant had no activity / no such
        series) is a valid, non-error result — the caller treats a missing series as 0.
        """
        inner = f"increase({_inject_range(metric_selector, range_promql)})"
        if by is not None:
            by_list = ",".join(by)
            promql = f"sum by ({by_list}) ({inner})"
        else:
            promql = f"sum({inner})"
        return self.instant_query(promql, tenant_id=tenant_id, at=at)


# --------------------------------------------------------------------------- #
# Parsing / helpers                                                           #
# --------------------------------------------------------------------------- #


def _inject_range(metric_selector: str, range_promql: str) -> str:
    """Append ``[<range>]`` to a metric selector, between the name/labels and the bracket.

    ``writer_full_mode_writes_total{source="x"}`` -> ``writer_full_mode_writes_total{source="x"}[744h]``
    ``think_runs_total``                          -> ``think_runs_total[744h]``
    """
    return f"{metric_selector}[{range_promql}]"


def _to_unix(dt: _dt.datetime) -> str:
    return repr(_ensure_utc(dt).timestamp())


def _safe_body(resp: httpx.Response) -> str:
    try:
        return resp.text[:500]
    except Exception:  # pragma: no cover - defensive
        return "<unreadable body>"


def _parse_vector(payload: Any, *, tenant_id: str, promql: str) -> list[Sample]:
    """Parse a Prometheus ``/query`` JSON body whose ``data.resultType == 'vector'``."""
    if not isinstance(payload, dict):
        raise MimirQueryError(f"non-object Mimir response for {tenant_id!r}: {payload!r}")
    status = payload.get("status")
    if status != "success":
        err = payload.get("error") or payload.get("errorType") or payload
        raise MimirQueryError(
            f"Mimir query status={status!r} for tenant {tenant_id!r} ({promql}): {err}"
        )
    data = payload.get("data") or {}
    rtype = data.get("resultType")
    result = data.get("result", [])
    if rtype != "vector":
        # A scalar query (rare here) is wrapped to a single sample for convenience.
        if rtype == "scalar" and isinstance(result, list) and len(result) == 2:
            ts, val = result
            return [Sample(labels={}, value=_to_float(val), timestamp=float(ts))]
        raise MimirQueryError(
            f"unexpected resultType {rtype!r} for tenant {tenant_id!r} ({promql})"
        )
    samples: list[Sample] = []
    for item in result:
        metric = item.get("metric", {}) or {}
        ts, raw = item.get("value", [None, None])
        if ts is None or raw is None:
            continue
        samples.append(
            Sample(labels=dict(metric), value=_to_float(raw), timestamp=float(ts))
        )
    return samples


def _to_float(raw: Any) -> float:
    """Prometheus encodes sample values as strings ('1234', '+Inf', 'NaN')."""
    try:
        return float(raw)
    except (TypeError, ValueError):
        return 0.0
