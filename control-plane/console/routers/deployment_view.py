#!/usr/bin/env python3
"""deployment_view — the C1 PER-DEPLOYMENT DRILL-DOWN (read view).

The fleet rollup (``GET /``) links each deployment row to ``/deployments/{id}``;
this feature router renders that page. It is the **shell every operator action
lives in**: a single server-rendered HTML page for one deployment that shows, side
by side, what IS (the agent's reported ACTUAL/applied facets + the C4 record) and
what the operator WANTS (the DESIRED state), with DRIFT badges between them
(``lib.compute_drift``), a deep-link into that tenant's Grafana dashboard, and the
last few audit events touching this deployment.

It plugs into the console exactly like ``example_router``:

  * a module-level ``def register(app, deps): ...`` mounts the endpoint;
  * everything it needs (store, settings, audit) comes off ``deps``;
  * it NEVER edits ``app.py``.

Read-only by design (console-roadmap §4): the page itself needs no operator token
(operator READS stay open on the operator LAN). The operator WRITE actions
(re-pull config, push config/release, suspend, queue action…) are rendered only as
LINKS/forms pointing at the OTHER feature routers' guarded endpoints — this page
mutates nothing. If a sibling action router is not mounted its link still renders;
clicking it simply 404s until that feature ships (the drill-down degrades
gracefully and is never coupled to another agent's file existing).
"""

from __future__ import annotations

import datetime as _dt
import html as _html
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi.responses import HTMLResponse

# Reach the control-plane shared lib the same way the foundation does. store.py /
# app.py already front-loaded the root onto sys.path before this router is
# imported (the mount loop runs inside create_app), so these imports resolve; we
# add a defensive anchor so the module is also importable in an isolated test that
# imports it directly.
_HERE = Path(__file__).resolve().parent
for _cand in (_HERE, *_HERE.parents):
    if (_cand / "SPRINT_PLAN.md").is_file() or (_cand / "lib" / "deployment.py").is_file():
        _root = str(_cand)
        if _root not in sys.path:
            sys.path.insert(0, _root)
        break

from lib.deployment import DeploymentRecord, Health  # noqa: E402
from lib.desired_state import DesiredState, compute_drift  # noqa: E402
from lib.primitives import to_rfc3339, utcnow  # noqa: E402


# --------------------------------------------------------------------------- #
# small view helpers (kept local so this feature never edits app.py)          #
# --------------------------------------------------------------------------- #

_BADGE_COLORS = {
    Health.GREEN: ("#0a7d28", "#e6f6ea"),
    Health.YELLOW: ("#8a6d00", "#fff6d6"),
    Health.RED: ("#a11", "#fde6e6"),
}

# Default operator Grafana base (host-published in the dev stack). Overridable via
# deps.settings.grafana_url (CP_GRAFANA_URL/GRAFANA_URL); the contract pins :3030
# for the per-customer drill-down dashboard.
_DEFAULT_GRAFANA_BASE = "http://localhost:3030"
_TENANT_DASHBOARD_UID = "fyralis-tenant-drilldown"


def _humanize_age(seconds: float) -> str:
    s = int(max(0.0, seconds))
    if s < 60:
        return f"{s}s"
    if s < 3600:
        return f"{s // 60}m {s % 60}s"
    if s < 86400:
        return f"{s // 3600}h {(s % 3600) // 60}m"
    return f"{s // 86400}d {(s % 86400) // 3600}h"


def _esc(value: Any) -> str:
    return _html.escape("" if value is None else str(value))


def _drift_badge(label: str, drifted: bool) -> str:
    """A small DRIFT pill — red when drifted, green when converged."""
    if drifted:
        fg, bg = "#a11", "#fde6e6"
        text = f"{label}: DRIFT"
    else:
        fg, bg = "#0a7d28", "#e6f6ea"
        text = f"{label}: OK"
    return (
        f'<span class="badge" style="color:{fg};background:{bg};'
        f'border:1px solid {fg}">{_esc(text)}</span>'
    )


def _kv_rows(pairs: List[tuple[str, str]]) -> str:
    out: List[str] = []
    for k, v in pairs:
        out.append(f"<tr><th>{_esc(k)}</th><td>{v}</td></tr>")
    return "\n".join(out)


def _grafana_tenant_url(grafana_base: str, tenant_id: str) -> str:
    """Deep-link to the per-customer Grafana dashboard scoped to ``tenant_id``.

    The per-tenant datasource is ``Mimir <tenant>`` with uid ``mimir-<tenant>``;
    the dashboard variable ``var-tenant_ds`` selects it (console-roadmap C1).
    """
    base = (grafana_base or _DEFAULT_GRAFANA_BASE).rstrip("/")
    ds_uid = f"mimir-{tenant_id}"
    return (
        f"{base}/d/{_TENANT_DASHBOARD_UID}/fyralis-tenant-drilldown"
        f"?var-tenant_ds={_html.escape(ds_uid)}&var-tenant={_html.escape(tenant_id)}"
    )


def _read_audit_events(deps, deployment_id: str, limit: int = 8) -> Optional[List[dict]]:
    """Best-effort read of the last few audit events touching this deployment.

    The shared ``deps.audit`` is a write facade (``append``); it does not promise a
    reader. We reach the lazily-opened underlying log THROUGH it without forcing it
    open (so a deployment with no operator writes does not create the log), and
    filter to entries whose ``target`` is this deployment_id. Returns ``None`` when
    no audit reader is available (the page then skips the section gracefully).
    """
    audit = getattr(deps, "audit", None)
    if audit is None:
        return None
    # Reach the underlying AuditLog only if it has already been opened (an operator
    # write happened); never force-open it from a read-only page.
    log = getattr(audit, "_log", None)
    if log is None:
        ensure = getattr(audit, "_ensure", None)
        if not callable(ensure):
            return None
        try:
            log = ensure()
        except Exception:
            return None
    if log is None:
        return None
    try:
        entries = list(log.entries())
    except Exception:
        return None

    matched: List[dict] = []
    for e in entries:
        target = getattr(e, "target", "") or ""
        meta = getattr(e, "metadata", {}) or {}
        meta_dep = meta.get("deployment_id") if isinstance(meta, dict) else None
        if target == deployment_id or meta_dep == deployment_id:
            matched.append(
                {
                    "ts": getattr(e, "ts", ""),
                    "actor": getattr(e, "actor", ""),
                    "action": getattr(e, "action", ""),
                    "target": target,
                }
            )
    # newest last on disk -> show newest first, capped.
    matched.reverse()
    return matched[:limit]


# --------------------------------------------------------------------------- #
# the drill-down renderer                                                      #
# --------------------------------------------------------------------------- #


def render_deployment(
    *,
    record: Optional[DeploymentRecord],
    desired: Optional[DesiredState],
    applied: Dict[str, Any],
    drift: Dict[str, Any],
    grafana_url: str,
    audit_events: Optional[List[dict]],
    deployment_id: str,
    now: _dt.datetime,
) -> str:
    """Render the full per-deployment drill-down HTML (self-contained document)."""

    # ---- ACTUAL (from the C4 record + applied facets) --------------------
    if record is not None:
        fg, bg = _BADGE_COLORS[record.health]
        health_badge = (
            f'<span class="badge" style="color:{fg};background:{bg};'
            f'border:1px solid {fg}">{_esc(record.health.value.upper())}</span>'
        )
        hb = record.last_heartbeat_ts
        if hb.tzinfo is None:
            hb = hb.replace(tzinfo=_dt.timezone.utc)
        age_s = max(0.0, (now - hb).total_seconds())
        tenant_id = record.tenant_id
        actual_rows = _kv_rows(
            [
                ("Tenant", f'<span class="mono">{_esc(record.tenant_id)}</span>'),
                ("Region", _esc(record.region)),
                ("Telemetry tier", _esc(record.telemetry_tier.value)),
                ("Version", f'<span class="mono">{_esc(record.version)}</span>'),
                ("Health", health_badge),
                (
                    "Last heartbeat",
                    f'<span class="mono" title="{_esc(to_rfc3339(record.last_heartbeat_ts))}">'
                    f"{_esc(_humanize_age(age_s))} ago</span>",
                ),
                ("License expiry", f'<span class="mono">{_esc(to_rfc3339(record.license_expiry))}</span>'),
            ]
        )
    else:
        tenant_id = ""
        actual_rows = (
            '<tr><td colspan="2" class="empty">No registry row for this '
            "deployment (never registered, or deregistered).</td></tr>"
        )

    # ---- APPLIED / SLI snapshot (agent-reported facets) ------------------
    if applied:
        applied_rows = _kv_rows(
            [
                ("Applied config version", _esc(applied.get("applied_config_version", 0))),
                ("Applied release", f'<span class="mono">{_esc(applied.get("applied_release"))}</span>'),
                ("License state applied", _esc(applied.get("license_state_applied", "—"))),
                (
                    "Acked actions",
                    f'<span class="mono">{_esc(", ".join(str(a) for a in (applied.get("acked_action_ids") or [])) or "—")}</span>',
                ),
            ]
        )
    else:
        applied_rows = (
            '<tr><td colspan="2" class="empty">No applied facets reported yet '
            "(the agent has not reconciled, or sends no applied facet).</td></tr>"
        )

    # ---- DESIRED (operator-written) --------------------------------------
    if desired is not None:
        cfg = desired.desired_config or {}
        flags = (cfg.get("feature_flags") or {}) if isinstance(cfg, dict) else {}
        flags_str = ", ".join(f"{k}={v}" for k, v in flags.items()) or "—"
        pa = desired.pending_actions or []
        if pa:
            pa_items = "".join(
                f"<li><span class=mono>{_esc(a.get('id'))}</span> "
                f"<b>{_esc(a.get('type'))}</b> "
                f"<span class=mono>{_esc(a.get('params') or {})}</span></li>"
                for a in pa
            )
            pending_html = f"<ul class=actions>{pa_items}</ul>"
        else:
            pending_html = "—"
        desired_rows = _kv_rows(
            [
                ("Desired config version", _esc(desired.desired_config_version)),
                (
                    "Telemetry tier",
                    _esc(cfg.get("telemetry_tier", "—")) if isinstance(cfg, dict) else "—",
                ),
                (
                    "Interval (s)",
                    _esc(cfg.get("interval_s", "—")) if isinstance(cfg, dict) else "—",
                ),
                (
                    "Sampling",
                    _esc(cfg.get("sampling", "—")) if isinstance(cfg, dict) else "—",
                ),
                ("Feature flags", f'<span class="mono">{_esc(flags_str)}</span>'),
                ("Desired release", f'<span class="mono">{_esc(desired.desired_release)}</span>'),
                ("License state", _esc(desired.license_state)),
                ("Pending actions", pending_html),
                (
                    "Config signed",
                    "yes" if desired.desired_config_sig else "no",
                ),
                ("Updated by", f'<span class="mono">{_esc(desired.updated_by) or "—"}</span>'),
                ("Updated at", f'<span class="mono">{_esc(desired.updated_at) or "—"}</span>'),
                ("Reason", _esc(desired.reason) or "—"),
            ]
        )
    else:
        desired_rows = (
            '<tr><td colspan="2" class="empty">No desired state written yet. '
            "Use the operator actions below to push config / release / actions.</td></tr>"
        )

    # ---- DRIFT badges ----------------------------------------------------
    action_drift = bool(drift.get("actions"))
    drift_badges = " ".join(
        [
            _drift_badge("Config", bool(drift.get("config"))),
            _drift_badge("Release", bool(drift.get("release"))),
            _drift_badge("License", bool(drift.get("license"))),
            _drift_badge("Actions", action_drift),
        ]
    )
    if action_drift:
        unacked = ", ".join(_esc(a) for a in drift["actions"])
        drift_detail = f'<div class="sub">Unacked actions: <span class="mono">{unacked}</span></div>'
    else:
        drift_detail = ""

    # ---- audit events ----------------------------------------------------
    if audit_events is None:
        audit_html = '<div class="sub">Audit trail not available on this console.</div>'
    elif not audit_events:
        audit_html = '<div class="sub">No audit events for this deployment yet.</div>'
    else:
        rows = "".join(
            "<tr>"
            f'<td class="mono">{_esc(e["ts"])}</td>'
            f'<td class="mono">{_esc(e["actor"])}</td>'
            f"<td>{_esc(e['action'])}</td>"
            "</tr>"
            for e in audit_events
        )
        audit_html = (
            "<table class=mini><thead><tr><th>When</th><th>Actor</th>"
            f"<th>Action</th></tr></thead><tbody>{rows}</tbody></table>"
        )

    # ---- operator action links (point at OTHER feature routers) ----------
    # Read-only page: these are LINKS to the guarded action endpoints another
    # agent builds. They render whether or not those routers are mounted; a
    # missing target simply 404s when clicked (graceful degradation).
    enc_id = _html.escape(deployment_id, quote=True)
    grafana_href = _grafana_tenant_url(grafana_url, tenant_id) if tenant_id else ""
    grafana_link = (
        f'<a class="btn" href="{grafana_href}" target="_blank" rel="noopener">'
        "Open Grafana (per-tenant)</a>"
        if grafana_href
        else ""
    )
    actions_html = (
        f'<a class="btn" href="/deployments/{enc_id}/config">Edit desired config</a>'
        f'<a class="btn" href="/deployments/{enc_id}/release">Set desired release</a>'
        f'<a class="btn" href="/deployments/{enc_id}/actions">Queue action</a>'
        f'<a class="btn" href="/deployments/{enc_id}/license">License state</a>'
        f"{grafana_link}"
    )

    generated = to_rfc3339(now)
    title_id = _esc(deployment_id)
    return f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Fyralis — {title_id}</title>
<style>
  :root {{ color-scheme: light dark; }}
  body {{ font-family: -apple-system, Segoe UI, Roboto, Helvetica, Arial, sans-serif;
          margin: 2rem; color: #1a1a1a; background: #fafafa; }}
  h1 {{ font-size: 1.3rem; margin: 0 0 .15rem; }}
  h2 {{ font-size: 1rem; margin: 0 0 .5rem; color:#333; }}
  .sub {{ color: #666; font-size: .85rem; margin-bottom: 1rem; }}
  nav.top {{ margin: 0 0 1rem; font-size:.85rem; }}
  nav.top a {{ display:inline-block; margin-right:.9rem; color:#2257a8;
               text-decoration:none; font-weight:600; }}
  nav.top a:hover {{ text-decoration:underline; }}
  .grid {{ display:grid; grid-template-columns: 1fr 1fr; gap: 1rem; align-items:start; }}
  @media (max-width: 760px) {{ .grid {{ grid-template-columns: 1fr; }} }}
  .card {{ background:#fff; border:1px solid #eee; border-radius:8px; padding:1rem;
           box-shadow: 0 1px 3px rgba(0,0,0,.06); margin-bottom:1rem; }}
  table {{ border-collapse: collapse; width: 100%; }}
  th, td {{ text-align: left; padding: .4rem .6rem; border-bottom: 1px solid #f0f0f0;
            font-size: .86rem; vertical-align: top; }}
  table th {{ color:#555; font-weight:600; width: 40%; }}
  .mono {{ font-family: ui-monospace, SFMono-Regular, Menlo, monospace; font-size: .82rem; }}
  .badge {{ display:inline-block; padding:.1rem .5rem; border-radius:4px;
            font-weight:700; font-size:.72rem; letter-spacing:.02em; }}
  .drift {{ margin: .25rem 0 1rem; }}
  .drift .badge {{ margin-right:.4rem; }}
  .empty {{ text-align:center; color:#777; padding:1rem; }}
  table.mini th {{ width:auto; }}
  ul.actions {{ margin:.2rem 0; padding-left:1.1rem; }}
  .btn {{ display:inline-block; margin:.2rem .4rem .2rem 0; padding:.35rem .7rem;
          background:#2257a8; color:#fff; border-radius:5px; text-decoration:none;
          font-size:.82rem; font-weight:600; }}
  .btn:hover {{ background:#1b4685; }}
  footer {{ margin-top: 1rem; color:#999; font-size:.78rem; }}
</style>
</head>
<body>
  <nav class="top">
    <a href="/">&larr; Fleet</a>
    <a href="/audit">Audit</a>
    <a href="/alerts">Alerts</a>
    <a href="/metering">Metering</a>
  </nav>
  <h1>Deployment <span class="mono">{title_id}</span></h1>
  <div class="sub">Per-deployment drill-down: ACTUAL vs DESIRED, with drift. Read-only;
    operator actions are below.</div>

  <div class="card">
    <h2>Drift</h2>
    <div class="drift">{drift_badges}</div>
    {drift_detail}
  </div>

  <div class="grid">
    <div class="card">
      <h2>Actual</h2>
      <table><tbody>
{actual_rows}
      </tbody></table>
      <h2 style="margin-top:1rem">Applied / SLI snapshot</h2>
      <table><tbody>
{applied_rows}
      </tbody></table>
    </div>
    <div class="card">
      <h2>Desired</h2>
      <table><tbody>
{desired_rows}
      </tbody></table>
    </div>
  </div>

  <div class="card">
    <h2>Operator actions</h2>
    <div class="sub">These link to the guarded operator endpoints (operator token required).</div>
    {actions_html}
  </div>

  <div class="card">
    <h2>Recent audit events</h2>
    {audit_html}
  </div>

  <footer>Generated {generated} &middot;
    <a href="/api/v1/deployments/{enc_id}">record JSON</a></footer>
</body>
</html>"""


# --------------------------------------------------------------------------- #
# register                                                                    #
# --------------------------------------------------------------------------- #


def register(app, deps) -> None:
    """Mount ``GET /deployments/{deployment_id}`` (the C1 drill-down) onto ``app``."""

    grafana_url = ""
    settings = getattr(deps, "settings", None)
    if settings is not None:
        grafana_url = getattr(settings, "grafana_url", "") or ""

    @app.get(
        "/deployments/{deployment_id}",
        response_class=HTMLResponse,
        tags=["console"],
        summary="Per-deployment drill-down (C1): actual vs desired + drift.",
    )
    def deployment_view(deployment_id: str) -> HTMLResponse:
        store = deps.store
        now = utcnow()
        record = store.record(deployment_id)
        desired = store.get_desired(deployment_id)
        applied = store.get_applied(deployment_id)
        # Drift is meaningful only when there is a desired state; with none, every
        # facet is "OK" (nothing the operator wants is unmet).
        if desired is not None:
            drift = compute_drift(desired, applied)
        else:
            drift = {"config": False, "release": False, "actions": [], "license": False}
        audit_events = _read_audit_events(deps, deployment_id)
        return HTMLResponse(
            content=render_deployment(
                record=record,
                desired=desired,
                applied=applied,
                drift=drift,
                grafana_url=grafana_url,
                audit_events=audit_events,
                deployment_id=deployment_id,
                now=now,
            )
        )
