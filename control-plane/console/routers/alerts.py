"""alerts.py — C2 ALERT SURFACE (read-only operator alert view).

A feature router (router-plugin arch): drops in under ``console/routers/`` and
exposes ``register(app, deps)``. It mounts TWO read-only endpoints that surface the
fleet's *active* alerts straight off the central Mimir **ruler**:

  * ``GET /api/v1/alerts``  — JSON: alerts grouped by severity, plus a summary.
  * ``GET /alerts``         — HTML: the operator-facing alert page (top-nav sibling
                              of /, /audit, /metering).

Where the alerts come from
--------------------------
Mimir's ruler exposes a Prometheus-compatible rules API:

    GET {mimir_url}/prometheus/api/v1/rules
        Headers: X-Scope-OrgID: <fleet_org_id>

This returns the rule groups; each ``alerting`` rule carries a ``state``
(``inactive``|``pending``|``firing``) and a list of ``alerts`` (the active alert
instances, each with its own ``labels``, ``annotations`` and ``state``). We keep only
``type == "alerting"`` rules, surface every *active* (pending/firing) alert instance,
read ``severity`` (``page``|``ticket``) and ``deployment_id`` off its labels, the
``summary`` off its annotations, and GROUP BY severity. The mimir base URL + fleet
org id come off ``deps.settings`` (``CP_MIMIR_URL``/``MIMIR_URL`` default
``http://mimir:9009``; ``CP_FLEET_ORG_ID``/``FLEET_ORG_ID`` default ``fleet``).

This is **read-only** — no operator write, no signer/audit. It is reachable on the
operator LAN like the other read pages (roadmap §6: a future sprint puts an
authenticated session in front of the operator READ surface too).

Resilience
----------
Mimir being unreachable must NOT take the page down: the fetch is wrapped, any
error (transport, non-200, unparseable body, non-``success`` status) degrades to a
clear "alert source unavailable" state — the JSON carries ``source_ok: false`` +
``error``, the HTML shows a banner. This mirrors I3's spirit (a degraded dependency
never crashes the surface).

Testability
-----------
The Mimir fetch is isolated behind an injectable ``fetcher`` (``app.state`` override
``alerts_fetcher`` or a closure default that uses :mod:`httpx`). The self-test mounts
ONLY this router on a bare app and injects a fake fetcher returning a canned ruler
payload — the real grouping/render code path runs with no live Mimir.
"""

from __future__ import annotations

import html as _html
import logging
from typing import Any, Callable, Dict, List

from fastapi.responses import HTMLResponse, JSONResponse

__all__ = ["register"]

_LOG = logging.getLogger("fyralis.console")

DEFAULT_MIMIR_URL = "http://mimir:9009"
DEFAULT_FLEET_ORG_ID = "fleet"
ORG_HEADER = "X-Scope-OrgID"
RULES_PATH = "/prometheus/api/v1/rules"

# Active = worth showing on the operator alert surface. ``inactive`` rules are not.
_ACTIVE_STATES = ("firing", "pending")

# Severity buckets we render, in display order. Anything else falls into "other".
_SEVERITY_ORDER = ("page", "ticket", "other")


# --------------------------------------------------------------------------- #
# Mimir ruler fetch (injectable for tests)                                     #
# --------------------------------------------------------------------------- #


def _default_fetcher(mimir_url: str, fleet_org_id: str, *, timeout: float = 10.0) -> Dict[str, Any]:
    """Fetch + parse the Mimir ruler ``/api/v1/rules`` payload over httpx.

    Returns the decoded JSON dict (the Prometheus ``{status, data:{groups:[...]}}``
    envelope). Raises on any transport / HTTP / decode error — the caller wraps it.
    """
    import httpx  # lazy: importing this router never requires httpx at import time

    base = (mimir_url or DEFAULT_MIMIR_URL).rstrip("/")
    url = base + RULES_PATH
    headers = {ORG_HEADER: fleet_org_id or DEFAULT_FLEET_ORG_ID}
    resp = httpx.get(url, headers=headers, timeout=timeout)
    resp.raise_for_status()
    return resp.json()


def _resolve_fetcher(app, deps) -> Callable[..., Dict[str, Any]]:
    """The fetcher to use: an ``app.state.alerts_fetcher`` override else the default."""
    override = getattr(getattr(app, "state", None), "alerts_fetcher", None)
    if callable(override):
        return override
    return _default_fetcher


# --------------------------------------------------------------------------- #
# parse / group                                                                #
# --------------------------------------------------------------------------- #


def _bucket_for(severity: str) -> str:
    sev = (severity or "").strip().lower()
    return sev if sev in ("page", "ticket") else "other"


def _normalize_alert(rule: Dict[str, Any], inst: Dict[str, Any]) -> Dict[str, Any]:
    """Flatten one active alert instance into a stable, render-friendly dict.

    Labels/annotations are read off the INSTANCE (``inst``) when present (those carry
    the resolved per-series label values), falling back to the rule-level labels.
    """
    inst_labels = dict(inst.get("labels") or {})
    rule_labels = dict(rule.get("labels") or {})
    labels = {**rule_labels, **inst_labels}
    annotations = dict(inst.get("annotations") or {})
    severity = labels.get("severity", "")
    return {
        "name": str(rule.get("name", "") or labels.get("alertname", "")),
        "state": str(inst.get("state") or rule.get("state") or ""),
        "severity": str(severity),
        "deployment_id": str(labels.get("deployment_id", "")),
        "summary": str(annotations.get("summary", "")),
        "description": str(annotations.get("description", "")),
        "active_at": str(inst.get("activeAt", "")),
        "value": str(inst.get("value", "")),
        "labels": labels,
    }


def _extract_active_alerts(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Pull every ACTIVE (firing|pending) alert instance from a ruler payload.

    Handles the standard Prometheus envelope ``{status, data:{groups:[{rules:[...]}]}}``
    defensively: filters ``type == "alerting"`` rules, then emits one normalized row
    per active instance in ``rule["alerts"]``. A rule whose ``state`` is active but
    that reports no instances still yields one synthetic row (so a firing rule is never
    silently dropped).
    """
    data = (payload or {}).get("data") or {}
    groups = data.get("groups") or []
    out: List[Dict[str, Any]] = []
    for grp in groups:
        for rule in (grp or {}).get("rules") or []:
            if (rule or {}).get("type") != "alerting":
                continue
            instances = [
                a
                for a in (rule.get("alerts") or [])
                if str((a or {}).get("state", "")).lower() in _ACTIVE_STATES
            ]
            if instances:
                for inst in instances:
                    out.append(_normalize_alert(rule, inst))
            elif str(rule.get("state", "")).lower() in _ACTIVE_STATES:
                # Active rule with no enumerated instances — surface it anyway.
                out.append(_normalize_alert(rule, {}))
    return out


def _group_by_severity(alerts: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    grouped: Dict[str, List[Dict[str, Any]]] = {k: [] for k in _SEVERITY_ORDER}
    for a in alerts:
        grouped[_bucket_for(a["severity"])].append(a)
    return grouped


def _build_view(deps, fetcher: Callable[..., Dict[str, Any]]) -> Dict[str, Any]:
    """Fetch + parse + group; degrade gracefully if Mimir is unreachable.

    Returns a render-ready view dict consumed by BOTH the JSON and HTML endpoints:
    ``{source_ok, error, mimir_url, fleet_org_id, total, counts:{}, groups:{}}``.
    """
    settings = getattr(deps, "settings", None)
    mimir_url = getattr(settings, "mimir_url", "") or DEFAULT_MIMIR_URL
    fleet_org_id = getattr(settings, "fleet_org_id", "") or DEFAULT_FLEET_ORG_ID

    view: Dict[str, Any] = {
        "source_ok": True,
        "error": None,
        "mimir_url": mimir_url,
        "fleet_org_id": fleet_org_id,
        "total": 0,
        "counts": {k: 0 for k in _SEVERITY_ORDER},
        "groups": {k: [] for k in _SEVERITY_ORDER},
    }
    try:
        payload = fetcher(mimir_url, fleet_org_id)
        if not isinstance(payload, dict):
            raise ValueError(f"ruler returned non-object payload: {type(payload).__name__}")
        status = payload.get("status")
        if status is not None and status != "success":
            raise ValueError(f"ruler status != success: {status!r}")
        alerts = _extract_active_alerts(payload)
    except Exception as exc:  # mimir unreachable / bad shape — never crash the page
        _LOG.warning("alert source unavailable (%s): %s", mimir_url, exc)
        view["source_ok"] = False
        view["error"] = f"{type(exc).__name__}: {exc}"
        return view

    grouped = _group_by_severity(alerts)
    view["groups"] = grouped
    view["counts"] = {k: len(v) for k, v in grouped.items()}
    view["total"] = sum(view["counts"].values())
    return view


# --------------------------------------------------------------------------- #
# HTML render (self-contained, matches the fleet console look)                 #
# --------------------------------------------------------------------------- #

_SEV_STYLE = {
    "page": ("#a11", "#fde6e6"),
    "ticket": ("#8a6d00", "#fff6d6"),
    "other": ("#333", "#eee"),
}
_SEV_TITLE = {"page": "PAGE", "ticket": "TICKET", "other": "OTHER"}


def _esc(v: Any) -> str:
    return _html.escape(str(v))


def _alert_row_html(a: Dict[str, Any]) -> str:
    state = a["state"].lower()
    state_fg = "#a11" if state == "firing" else "#8a6d00"
    dep = a["deployment_id"]
    dep_html = (
        f'<a href="/deployments/{_esc(dep)}">{_esc(dep)}</a>' if dep else '<span class="muted">—</span>'
    )
    summary = _esc(a["summary"]) if a["summary"] else '<span class="muted">—</span>'
    return (
        "<tr>"
        f'<td class="mono">{_esc(a["name"])}</td>'
        f'<td><span class="state" style="color:{state_fg}">{_esc(state.upper())}</span></td>'
        f'<td class="mono">{dep_html}</td>'
        f"<td>{summary}</td>"
        f'<td class="mono" title="{_esc(a["active_at"])}">{_esc(a["active_at"][:19])}</td>'
        "</tr>"
    )


def _group_section_html(severity: str, alerts: List[Dict[str, Any]]) -> str:
    fg, bg = _SEV_STYLE[severity]
    title = _SEV_TITLE[severity]
    badge = (
        f'<span class="sev" style="color:{fg};background:{bg};border:1px solid {fg}">'
        f"{title}</span>"
    )
    if not alerts:
        rows = '<tr><td colspan="5" class="empty">No active alerts at this severity.</td></tr>'
    else:
        rows = "\n".join(_alert_row_html(a) for a in alerts)
    return f"""
  <section class="grp">
    <h2>{badge} <span class="count">{len(alerts)}</span></h2>
    <table>
      <thead><tr><th>Alert</th><th>State</th><th>Deployment</th><th>Summary</th><th>Active since</th></tr></thead>
      <tbody>
{rows}
      </tbody>
    </table>
  </section>"""


def render_alerts(view: Dict[str, Any]) -> str:
    """Render the operator alert page from a ``_build_view`` view dict."""
    if not view["source_ok"]:
        banner = (
            '<div class="banner err">Alert source unavailable — could not reach the '
            f'Mimir ruler at <code>{_esc(view["mimir_url"])}</code>. '
            f'<span class="mono">{_esc(view.get("error") or "")}</span></div>'
        )
        sections = ""
        counts_html = '<span class="c-total">source unavailable</span>'
    else:
        banner = ""
        sections = "\n".join(
            _group_section_html(sev, view["groups"][sev]) for sev in _SEVERITY_ORDER
        )
        c = view["counts"]
        counts_html = (
            f'<span class="c-total">{view["total"]} active</span>'
            f'<span class="c-page">{c["page"]} page</span>'
            f'<span class="c-ticket">{c["ticket"]} ticket</span>'
            f'<span class="c-other">{c["other"]} other</span>'
        )

    return f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Fyralis Console — Alerts</title>
<style>
  :root {{ color-scheme: light dark; }}
  body {{ font-family: -apple-system, Segoe UI, Roboto, Helvetica, Arial, sans-serif;
          margin: 2rem; color: #1a1a1a; background: #fafafa; }}
  h1 {{ font-size: 1.4rem; margin: 0 0 .25rem; }}
  h2 {{ font-size: 1rem; margin: 1.4rem 0 .4rem; }}
  .sub {{ color: #666; font-size: .85rem; margin-bottom: 1rem; }}
  nav.top {{ margin: 0 0 1rem; font-size:.85rem; }}
  nav.top a {{ display:inline-block; margin-right:.9rem; color:#2257a8;
               text-decoration:none; font-weight:600; }}
  nav.top a:hover {{ text-decoration:underline; }}
  .counts {{ margin: 0 0 1rem; font-size: .9rem; }}
  .counts span {{ display: inline-block; margin-right: .9rem; padding: .15rem .55rem;
                  border-radius: 999px; font-weight: 600; }}
  .c-total {{ color:#333; background:#eee; }}
  .c-page {{ color:#a11; background:#fde6e6; }}
  .c-ticket {{ color:#8a6d00; background:#fff6d6; }}
  .c-other {{ color:#555; background:#ececec; }}
  table {{ border-collapse: collapse; width: 100%; background: #fff;
           box-shadow: 0 1px 3px rgba(0,0,0,.08); }}
  th, td {{ text-align: left; padding: .5rem .7rem; border-bottom: 1px solid #eee;
            font-size: .88rem; vertical-align: top; }}
  th {{ background: #f3f3f3; font-weight: 600; }}
  tr:hover td {{ background: #fbfbfb; }}
  .mono {{ font-family: ui-monospace, SFMono-Regular, Menlo, monospace; font-size: .82rem; }}
  .sev {{ display:inline-block; padding:.1rem .5rem; border-radius:4px;
          font-weight:700; font-size:.75rem; letter-spacing:.03em; }}
  .state {{ font-weight:700; font-size:.78rem; letter-spacing:.02em; }}
  .count {{ color:#999; font-weight:600; font-size:.85rem; }}
  .muted {{ color:#aaa; }}
  .empty {{ text-align:center; color:#777; padding:1.2rem; }}
  td a {{ color:#2257a8; text-decoration:none; }}
  td a:hover {{ text-decoration:underline; }}
  .banner {{ padding:.7rem .9rem; border-radius:6px; margin:0 0 1rem; font-size:.88rem; }}
  .banner.err {{ color:#a11; background:#fde6e6; border:1px solid #f3b3b3; }}
  .banner code {{ font-family: ui-monospace, monospace; }}
  footer {{ margin-top: 1.4rem; color:#999; font-size:.78rem; }}
</style>
</head>
<body>
  <h1>Fyralis BYOC — Active Alerts</h1>
  <nav class="top">
    <a href="/">Fleet</a>
    <a href="/audit">Audit</a>
    <a href="/alerts">Alerts</a>
    <a href="/metering">Metering</a>
  </nav>
  <div class="sub">Active (firing &amp; pending) alerts off the Mimir ruler
    (org <span class="mono">{_esc(view["fleet_org_id"])}</span>), grouped by severity.
    Read-only.</div>
  {banner}
  <div class="counts">{counts_html}</div>
{sections}
  <footer>Source: <span class="mono">{_esc(view["mimir_url"])}{_esc(RULES_PATH)}</span>
    &middot; GET /api/v1/alerts for JSON.</footer>
</body>
</html>"""


# --------------------------------------------------------------------------- #
# register                                                                     #
# --------------------------------------------------------------------------- #


def register(app, deps) -> None:
    """Mount the C2 alert surface (``GET /api/v1/alerts`` + ``GET /alerts``)."""

    @app.get(
        "/api/v1/alerts",
        tags=["alerts"],
        summary="Active fleet alerts (firing/pending) off the Mimir ruler, grouped by severity.",
    )
    def alerts_json() -> JSONResponse:
        view = _build_view(deps, _resolve_fetcher(app, deps))
        status_code = 200 if view["source_ok"] else 503
        return JSONResponse(
            status_code=status_code,
            content={
                "source_ok": view["source_ok"],
                "error": view["error"],
                "mimir_url": view["mimir_url"],
                "fleet_org_id": view["fleet_org_id"],
                "total": view["total"],
                "counts": view["counts"],
                "groups": view["groups"],
            },
        )

    @app.get(
        "/alerts",
        response_class=HTMLResponse,
        tags=["alerts"],
        summary="Operator alert page (HTML).",
    )
    def alerts_html() -> HTMLResponse:
        view = _build_view(deps, _resolve_fetcher(app, deps))
        # Page itself always renders 200 (degraded banner if the source is down);
        # the JSON endpoint is the machine-readable surface that signals 503.
        return HTMLResponse(content=render_alerts(view))
