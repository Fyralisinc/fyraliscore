"""metering_view — D1 read-only USAGE / METERING view (console feature router).

Operators need to see, per tenant, how much each customer's BYOC deployment is
USING the control plane this period: ingestion volume (observations written),
reasoning runs, and LLM/think spend. This router renders that view, computed by
the committed metering engine (``metering/rollup.py`` + ``metering/mimir_client.py``)
which reads the **aggregate Tier-1 counters** out of the central Mimir, one tenant
at a time (``X-Scope-OrgID: <tenant>``):

  * ``writer_full_mode_writes_total{source}`` -> obs-per-source (ingestion volume)
  * ``think_runs_total``                       -> reasoning runs
  * ``think_cost_recent_usd_total``            -> LLM/think spend (USD)

Invariant I1 (No PII at T1): this view reads ONLY aggregate counters — per-source
write counts, run counts, and a USD spend gauge. It never reads a payload or any
label that could carry PII. The numbers shown are counts/floats + a tenant id only.

Read-only. There is NO operator WRITE here, so no ``require_operator`` /
``deps.audit`` / ``deps.signer`` — operator READS stay open on the operator LAN
(deps.py / roadmap §6). The SIGNED export of a rollup (tamper-evident, FR-F2) lives
in ``metering/export.py`` + the metering job; this page only surfaces the live
numbers and notes that the canonical export is signed.

Endpoints
---------
* ``GET /api/v1/metering``  -> JSON  {period, generated_at, mimir_configured,
                                      tenants:[{tenant_id, deployments, metrics,
                                      totals, error?}], fleet_totals, note}
* ``GET /metering``         -> HTML  per-tenant usage table + the signed-export note.

Both accept ``?month=YYYY-MM`` (default: the current UTC calendar month).

Tenants are enumerated from the live fleet registry (``deps.store.list_records()``),
so the view always reflects the deployments the console actually knows about. A
tenant whose series are all empty (no activity this period) rolls up to 0 — a valid,
non-error result (you can bill a tenant for zero usage).

Mimir client / testability
---------------------------
The Mimir base URL comes from ``deps.settings.mimir_url`` (env CP_MIMIR_URL/MIMIR_URL).
If unset, the JSON view reports ``mimir_configured: false`` and skips the queries
(the page still renders the fleet's tenants with no numbers) — the console must not
500 just because Mimir is unreachable.

For ISOLATED testing without a live Mimir, an ``httpx.BaseTransport`` (e.g. an
``httpx.MockTransport`` serving canned T1 counters) may be injected via
``deps.metering_transport`` or ``app.state.metering_transport``; the REAL
``MimirClient`` request/parse path is then exercised against a canned Mimir. When a
transport is injected the base URL need not be configured.
"""

from __future__ import annotations

import datetime as _dt
import html as _html
import logging
import os
import sys
from typing import Any, Optional

from fastapi import Query
from fastapi.responses import HTMLResponse

# --- import the committed metering engine (control-plane/metering, flat modules) ---
_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.normpath(os.path.join(_HERE, "..", ".."))  # control-plane/
_METERING_DIR = os.path.join(_ROOT, "metering")
for _p in (_METERING_DIR,):
    if _p not in sys.path:
        sys.path.insert(0, _p)

_LOG = logging.getLogger("fyralis.console")

__all__ = ["register"]


# --------------------------------------------------------------------------- #
# helpers                                                                      #
# --------------------------------------------------------------------------- #


def _import_metering():
    """Import the metering engine lazily so a stack without httpx/metering can't
    break the console mount (the mount loop logs+skips, but import-at-register is
    cleaner). Returns ``(rollup_module, mimir_module)`` or ``(None, None)``."""
    try:
        import mimir_client as mc  # noqa: WPS433
        import rollup as ru  # noqa: WPS433

        return ru, mc
    except Exception:  # pragma: no cover - defensive (missing dep)
        _LOG.warning("metering_view: metering engine unavailable; view disabled", exc_info=True)
        return None, None


def _period_for(ru, month: Optional[str]):
    """Resolve a :class:`rollup.Period` from ``?month=YYYY-MM`` (default current UTC month)."""
    if month:
        y, m = month.split("-", 1)
        return ru.Period.month(int(y), int(m))
    now = _dt.datetime.now(_dt.timezone.utc)
    return ru.Period.month(now.year, now.month)


def _tenants_from_fleet(store) -> dict[str, list[str]]:
    """Map ``tenant_id -> [deployment_id, ...]`` from the live fleet registry.

    Deterministic order: tenants sorted, deployments sorted within a tenant.
    """
    by_tenant: dict[str, list[str]] = {}
    try:
        records = store.list_records()
    except Exception:  # pragma: no cover - store should never raise here
        _LOG.warning("metering_view: list_records failed", exc_info=True)
        records = []
    for rec in records:
        tid = getattr(rec, "tenant_id", "") or ""
        dep = getattr(rec, "deployment_id", "") or ""
        if not tid:
            continue
        by_tenant.setdefault(tid, [])
        if dep:
            by_tenant[tid].append(dep)
    return {t: sorted(deps_) for t, deps_ in sorted(by_tenant.items())}


def _make_client(ru, mc, *, mimir_url: str, transport):
    """Build a :class:`MimirClient` from settings, honoring an injected transport.

    When ``transport`` is set (a test's ``httpx.MockTransport``) the base URL may be
    empty — the mock answers regardless. Otherwise a configured ``mimir_url`` is
    required (caller checks ``_mimir_ready`` first)."""
    base = mimir_url or mc.DEFAULT_MIMIR_URL
    return mc.MimirClient(base, transport=transport)


def _resolve_transport(deps, app):
    """Find an injected httpx transport (test seam) on deps or app.state, else None."""
    t = getattr(deps, "metering_transport", None)
    if t is not None:
        return t
    state = getattr(app, "state", None)
    return getattr(state, "metering_transport", None)


def _rollup_payload(rollup) -> dict:
    """The per-tenant usage block shown in the view (aggregate counts only, I1)."""
    metrics = rollup.to_dict()["metrics"]
    return {
        "metrics": {
            "obs_per_source": metrics["obs_per_source"],
            "ingestion_volume": metrics["ingestion_volume"],
            "think_runs": metrics["think_runs"],
            "think_cost_usd": metrics["think_cost_usd"],
        },
        "totals": {
            "observations": metrics["ingestion_volume"],
            "think_runs": metrics["think_runs"],
            "cost_usd": metrics["think_cost_usd"],
        },
    }


def _compute_view(ru, mc, *, store, period, mimir_url: str, transport) -> dict:
    """Compute the whole-fleet usage view for ``period``.

    Per-tenant: one signed-free rollup (live numbers). A query failure for ONE
    tenant is captured as ``error`` on that tenant's row and does NOT abort the
    other tenants (a metering view must degrade gracefully).
    """
    by_tenant = _tenants_from_fleet(store)
    mimir_ready = bool(transport) or bool(mimir_url)

    tenants: list[dict] = []
    fleet = {"observations": 0.0, "think_runs": 0.0, "cost_usd": 0.0}

    client = None
    if mimir_ready:
        client = _make_client(ru, mc, mimir_url=mimir_url, transport=transport)
    try:
        for tid, deployments in by_tenant.items():
            row: dict[str, Any] = {"tenant_id": tid, "deployments": deployments}
            if not client:
                row["metrics"] = None
                row["totals"] = None
                tenants.append(row)
                continue
            try:
                rollup = ru.compute_rollup(client, tenant_id=tid, period=period)
                payload = _rollup_payload(rollup)
                row.update(payload)
                fleet["observations"] += payload["totals"]["observations"]
                fleet["think_runs"] += payload["totals"]["think_runs"]
                fleet["cost_usd"] += payload["totals"]["cost_usd"]
            except Exception as exc:  # one tenant's query failed; keep going
                _LOG.warning("metering_view: rollup failed for tenant %r: %s", tid, exc)
                row["metrics"] = None
                row["totals"] = None
                row["error"] = str(exc)
            tenants.append(row)
    finally:
        if client is not None:
            try:
                client.close()
            except Exception:  # pragma: no cover
                pass

    return {
        "period": period.to_dict(),
        "generated_at": _now_rfc3339(ru),
        "mimir_configured": mimir_ready,
        "tenants": tenants,
        "fleet_totals": {
            "observations": round(fleet["observations"], 3),
            "think_runs": round(fleet["think_runs"], 3),
            "cost_usd": round(fleet["cost_usd"], 6),
        },
        "note": (
            "Aggregate Tier-1 counters only (no PII, I1). The canonical, "
            "tamper-evident usage export is ed25519-signed (metering/export.py)."
        ),
    }


def _now_rfc3339(ru) -> str:
    try:
        # rollup re-exports signing_lib.now_rfc3339 indirectly; use UTC now otherwise.
        return _dt.datetime.now(_dt.timezone.utc).replace(microsecond=0).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )
    except Exception:  # pragma: no cover
        return ""


# --------------------------------------------------------------------------- #
# HTML rendering                                                               #
# --------------------------------------------------------------------------- #


def _fmt_num(v: Any) -> str:
    if v is None:
        return "—"
    f = float(v)
    if f == int(f):
        return f"{int(f):,}"
    return f"{f:,.3f}"


def _fmt_usd(v: Any) -> str:
    if v is None:
        return "—"
    return f"${float(v):,.4f}"


def _render_html(view: dict) -> str:
    e = _html.escape
    period = view["period"]
    rows: list[str] = []
    for t in view["tenants"]:
        tid = e(str(t["tenant_id"]))
        deployments = e(", ".join(t.get("deployments", []) or []) or "—")
        totals = t.get("totals")
        if t.get("error"):
            cells = (
                f'<td colspan="3" class="err">query error: {e(str(t["error"]))}</td>'
            )
        elif totals is None:
            cells = '<td colspan="3" class="muted">no metrics (Mimir not configured)</td>'
        else:
            obs_src = t["metrics"]["obs_per_source"] or {}
            src_detail = (
                ", ".join(f"{e(k)}={_fmt_num(v)}" for k, v in obs_src.items()) or "—"
            )
            cells = (
                f'<td class="num">{_fmt_num(totals["observations"])}'
                f'<div class="src">{src_detail}</div></td>'
                f'<td class="num">{_fmt_num(totals["think_runs"])}</td>'
                f'<td class="num">{_fmt_usd(totals["cost_usd"])}</td>'
            )
        rows.append(
            f"<tr><td>{tid}</td><td class='dep'>{deployments}</td>{cells}</tr>"
        )

    if not rows:
        rows.append(
            '<tr><td colspan="5" class="muted">no deployments in the fleet yet</td></tr>'
        )

    ft = view["fleet_totals"]
    mimir_badge = (
        '<span class="ok">Mimir configured</span>'
        if view["mimir_configured"]
        else '<span class="warn">Mimir not configured — set CP_MIMIR_URL</span>'
    )

    return f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<title>Usage / Metering — Fyralis Control Plane</title>
<meta name="viewport" content="width=device-width, initial-scale=1">
<style>
  body {{ font: 14px/1.5 system-ui, sans-serif; margin: 2rem; color: #1a1a1a; }}
  h1 {{ font-size: 1.4rem; margin: 0 0 .25rem; }}
  nav a {{ margin-right: 1rem; color: #2563eb; text-decoration: none; }}
  .meta {{ color: #555; margin: .5rem 0 1rem; }}
  table {{ border-collapse: collapse; width: 100%; max-width: 1100px; }}
  th, td {{ border-bottom: 1px solid #e5e7eb; padding: .5rem .6rem; text-align: left; vertical-align: top; }}
  th {{ background: #f8fafc; font-weight: 600; }}
  td.num {{ text-align: right; font-variant-numeric: tabular-nums; }}
  td.dep {{ font-family: ui-monospace, monospace; font-size: .85em; color: #444; }}
  .src {{ color: #777; font-size: .8em; font-weight: 400; }}
  tfoot td {{ font-weight: 700; background: #f8fafc; }}
  .muted {{ color: #888; }}
  .err {{ color: #b91c1c; }}
  .ok {{ color: #15803d; font-weight: 600; }}
  .warn {{ color: #b45309; font-weight: 600; }}
  .note {{ margin-top: 1.25rem; padding: .75rem 1rem; background: #f0f9ff;
           border-left: 3px solid #38bdf8; color: #0c4a6e; max-width: 1100px; }}
</style></head><body>
<nav><a href="/">&larr; fleet</a><a href="/audit">audit</a><a href="/alerts">alerts</a><a href="/metering">metering</a></nav>
<h1>Usage / Metering</h1>
<div class="meta">
  Period <strong>{_html.escape(period.get("label",""))}</strong>
  ({_html.escape(period.get("start",""))} &rarr; {_html.escape(period.get("end",""))})
  &middot; generated {_html.escape(view["generated_at"])}
  &middot; {mimir_badge}
</div>
<table>
  <thead><tr>
    <th>Tenant</th><th>Deployments</th>
    <th style="text-align:right">Observations<div class="src">(per source)</div></th>
    <th style="text-align:right">Think runs</th>
    <th style="text-align:right">Think cost (USD)</th>
  </tr></thead>
  <tbody>
    {"".join(rows)}
  </tbody>
  <tfoot><tr>
    <td colspan="2">Fleet total</td>
    <td class="num">{_fmt_num(ft["observations"])}</td>
    <td class="num">{_fmt_num(ft["think_runs"])}</td>
    <td class="num">{_fmt_usd(ft["cost_usd"])}</td>
  </tr></tfoot>
</table>
<div class="note">{_html.escape(view["note"])}</div>
</body></html>"""


# --------------------------------------------------------------------------- #
# register                                                                     #
# --------------------------------------------------------------------------- #


def register(app, deps) -> None:
    """Mount the D1 read-only metering view onto ``app`` using ``deps``."""

    ru, mc = _import_metering()

    def _build_view(month: Optional[str]) -> dict:
        if ru is None or mc is None:
            return {
                "period": {"label": month or "", "start": "", "end": ""},
                "generated_at": "",
                "mimir_configured": False,
                "tenants": [],
                "fleet_totals": {"observations": 0.0, "think_runs": 0.0, "cost_usd": 0.0},
                "note": "metering engine unavailable on this console",
            }
        period = _period_for(ru, month)
        mimir_url = getattr(getattr(deps, "settings", None), "mimir_url", "") or ""
        transport = _resolve_transport(deps, app)
        return _compute_view(
            ru, mc, store=deps.store, period=period, mimir_url=mimir_url, transport=transport
        )

    @app.get(
        "/api/v1/metering",
        tags=["metering"],
        summary="Per-tenant Tier-1 usage rollup (read-only, aggregate-only, I1).",
    )
    def metering_json(month: Optional[str] = Query(default=None, description="YYYY-MM (UTC)")):
        return _build_view(month)

    @app.get(
        "/metering",
        tags=["metering"],
        summary="HTML per-tenant usage / metering view.",
        response_class=HTMLResponse,
    )
    def metering_html(month: Optional[str] = Query(default=None, description="YYYY-MM (UTC)")):
        return HTMLResponse(_render_html(_build_view(month)))
