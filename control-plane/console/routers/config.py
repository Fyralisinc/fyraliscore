"""config — the A1 REMOTE CONFIG PUSH feature router (operator -> desired_config).

This is the flagship operator surface of the desired-state console (roadmap A1): an
operator pushes a new agent config for a deployment; the console VALIDATES the
schema, SIGNS it (I6), bumps the monotonic ``desired_config_version``, stores it as
DESIRED state, and AUDITS the write (I5). The outbound-only agent then PULLS the
desired state, VERIFIES the signature against its trust root, applies it, and reports
``applied_config_version`` back — the console's drift view (and the GET endpoint here)
shows the config drift closing.

Endpoints (mounted by the foundation's router-plugin loop via ``register(app, deps)``):

  PUT  /api/v1/deployments/{id}/desired-config   (operator-authed, I4 + I5 + I6)
      Body: {telemetry_tier: "T1"|"T2"|"T3", interval_s: int>0, sampling: 0..1,
             feature_flags: {str: bool}}  (+ optional ``reason``)
      -> signs + stores + audits; returns the new desired_config + version + sig meta.

  GET  /api/v1/deployments/{id}/desired-config   (operator read; open on the LAN)
      -> the current desired_config + version + the live DRIFT vs the agent's
         last-reported applied facets (so an operator sees "pushed but not yet applied").

  GET  /deployments/{id}/config                  (operator read; tiny HTML form)
      -> a minimal operator form to view + edit the desired config (calls the PUT).

Invariants: I4 (operator token, distinct from the agent token; per-deployment scope),
I5 (every PUT audited), I6 (every config signed before it can become desired state —
the agent re-verifies before applying). Operator READS stay open on the operator LAN;
only the WRITE requires ``deps.require_operator``.
"""

from __future__ import annotations

import html
import json
from typing import Any, Dict, Optional

from fastapi import Body, Depends, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse

# DesiredState + drift come from the shared lib (both console and agent import it).
from lib.desired_state import DesiredState, compute_drift

TELEMETRY_TIERS = ("T1", "T2", "T3")


# --------------------------------------------------------------------------- #
# schema validation (explicit + strict — a config that becomes signed desired   #
# state must be well-formed; we reject garbage before we ever sign it)          #
# --------------------------------------------------------------------------- #


def _validate_config(body: Dict[str, Any]) -> Dict[str, Any]:
    """Validate + normalize an inbound desired-config body.

    Returns the cleaned config dict ({telemetry_tier, interval_s, sampling,
    feature_flags}). Raises ``HTTPException(422)`` with a precise reason on any
    schema violation — we never sign or store an invalid config.
    """
    if not isinstance(body, dict):
        raise HTTPException(status_code=422, detail="config body must be a JSON object")

    cfg: Dict[str, Any] = {}

    tier = body.get("telemetry_tier")
    if tier not in TELEMETRY_TIERS:
        raise HTTPException(
            status_code=422,
            detail=f"telemetry_tier must be one of {list(TELEMETRY_TIERS)}, got {tier!r}",
        )
    cfg["telemetry_tier"] = tier

    interval = body.get("interval_s")
    if isinstance(interval, bool) or not isinstance(interval, int):
        raise HTTPException(status_code=422, detail="interval_s must be an integer (seconds)")
    if interval <= 0:
        raise HTTPException(status_code=422, detail="interval_s must be > 0")
    cfg["interval_s"] = interval

    sampling = body.get("sampling")
    if isinstance(sampling, bool) or not isinstance(sampling, (int, float)):
        raise HTTPException(status_code=422, detail="sampling must be a number in [0, 1]")
    if not (0.0 <= float(sampling) <= 1.0):
        raise HTTPException(status_code=422, detail="sampling must be in [0, 1]")
    cfg["sampling"] = float(sampling)

    flags = body.get("feature_flags", {})
    if flags is None:
        flags = {}
    if not isinstance(flags, dict):
        raise HTTPException(status_code=422, detail="feature_flags must be an object of {name: bool}")
    clean_flags: Dict[str, bool] = {}
    for k, v in flags.items():
        if not isinstance(k, str):
            raise HTTPException(status_code=422, detail="feature_flags keys must be strings")
        if not isinstance(v, bool):
            raise HTTPException(
                status_code=422, detail=f"feature_flags[{k!r}] must be a boolean, got {type(v).__name__}"
            )
        clean_flags[k] = v
    cfg["feature_flags"] = clean_flags

    return cfg


def _now_iso() -> str:
    from datetime import datetime, timezone

    return datetime.now(timezone.utc).isoformat()


# --------------------------------------------------------------------------- #
# the router                                                                    #
# --------------------------------------------------------------------------- #


def register(app, deps) -> None:
    """Mount the A1 remote-config-push endpoints onto ``app`` using ``deps``."""

    @app.put(
        "/api/v1/deployments/{deployment_id}/desired-config",
        tags=["config"],
        summary="Push a new signed desired config for a deployment (operator).",
        dependencies=[Depends(deps.require_operator)],
    )
    def push_desired_config(
        deployment_id: str,
        body: Dict[str, Any] = Body(...),
    ) -> JSONResponse:
        # 1. VALIDATE the schema before we ever sign anything.
        cfg = _validate_config(body)
        reason = ""
        if isinstance(body, dict) and isinstance(body.get("reason"), str):
            reason = body["reason"]

        # 2. Read the current desired state to bump the monotonic version forward.
        current = deps.store.get_desired(deployment_id)
        next_version = (int(current.desired_config_version) if current else 0) + 1

        # 3. SIGN the config dict (I6). deps.signer signs canonical_json_bytes(cfg)
        #    as kind="config" with the trust root's active key -> {sig, manifest,
        #    signed_by}. A sign failure (no key material) is a 500, NOT a fake sig.
        try:
            sig_envelope = deps.signer(cfg, kind="config", version=str(next_version))
        except Exception as exc:  # RuntimeError when no active private key, etc.
            raise HTTPException(
                status_code=500,
                detail=f"could not sign config (signing unavailable): {exc}",
            )

        # 4. Build + persist the DESIRED state (preserve other facets the operator
        #    may have set: release / license_state / pending_actions).
        base = current.model_copy() if current else DesiredState(deployment_id=deployment_id)
        desired = base.model_copy(
            update={
                "deployment_id": deployment_id,
                "desired_config": cfg,
                "desired_config_version": next_version,
                "desired_config_sig": sig_envelope,
                "updated_by": "operator",
                "updated_at": _now_iso(),
                "reason": reason,
            }
        )
        stored = deps.store.put_desired(deployment_id, desired)

        # 5. AUDIT the operator write (I5). Hash-chained, tamper-evident; best-effort
        #    (the store write is the source of truth and is already committed).
        deps.audit.append(
            {
                "actor": "operator",
                "action": "config.push",
                "target": deployment_id,
                "metadata": {
                    "desired_config_version": next_version,
                    "telemetry_tier": cfg["telemetry_tier"],
                    "interval_s": cfg["interval_s"],
                    "sampling": cfg["sampling"],
                    "feature_flags": cfg["feature_flags"],
                    "signed_by": sig_envelope.get("signed_by"),
                    "reason": reason,
                },
            }
        )

        return JSONResponse(
            status_code=200,
            content={
                "deployment_id": deployment_id,
                "desired_config": stored.desired_config,
                "desired_config_version": stored.desired_config_version,
                "signed_by": sig_envelope.get("signed_by"),
                "updated_at": stored.updated_at,
                "reason": stored.reason,
            },
        )

    @app.get(
        "/api/v1/deployments/{deployment_id}/desired-config",
        tags=["config"],
        summary="Read the current desired config + live drift for a deployment.",
    )
    def get_desired_config(deployment_id: str) -> JSONResponse:
        desired = deps.store.get_desired(deployment_id)
        if desired is None or desired.desired_config is None:
            raise HTTPException(
                status_code=404,
                detail=f"no desired config set for deployment {deployment_id!r}",
            )
        applied = deps.store.get_applied(deployment_id)
        drift = compute_drift(desired, applied)
        return JSONResponse(
            status_code=200,
            content={
                "deployment_id": deployment_id,
                "desired_config": desired.desired_config,
                "desired_config_version": desired.desired_config_version,
                "signed_by": (desired.desired_config_sig or {}).get("signed_by"),
                "updated_by": desired.updated_by,
                "updated_at": desired.updated_at,
                "reason": desired.reason,
                "applied_config_version": int(applied.get("applied_config_version", 0) or 0),
                "drift": {"config": drift["config"]},
            },
        )

    @app.get(
        "/deployments/{deployment_id}/config",
        tags=["config"],
        summary="Operator HTML form to view + edit a deployment's desired config.",
        response_class=HTMLResponse,
    )
    def config_form(deployment_id: str) -> HTMLResponse:
        desired = deps.store.get_desired(deployment_id)
        cfg = (desired.desired_config if desired else None) or {}
        version = desired.desired_config_version if desired else 0
        applied = deps.store.get_applied(deployment_id)
        applied_v = int(applied.get("applied_config_version", 0) or 0)
        drifting = applied_v < int(version)

        return HTMLResponse(_render_form(deployment_id, cfg, version, applied_v, drifting))


# --------------------------------------------------------------------------- #
# tiny operator HTML form (read + edit; PUTs with a pasted operator token)       #
# --------------------------------------------------------------------------- #


def _render_form(
    deployment_id: str,
    cfg: Dict[str, Any],
    version: int,
    applied_v: int,
    drifting: bool,
) -> str:
    did = html.escape(deployment_id)
    tier = cfg.get("telemetry_tier", "T2")
    interval = cfg.get("interval_s", 60)
    sampling = cfg.get("sampling", 1.0)
    flags_json = html.escape(json.dumps(cfg.get("feature_flags", {}), indent=2))
    tier_opts = "".join(
        f'<option value="{t}"{" selected" if t == tier else ""}>{t}</option>'
        for t in TELEMETRY_TIERS
    )
    drift_badge = (
        f'<span style="color:#b00">DRIFTING (agent at v{applied_v}, desired v{version})</span>'
        if drifting
        else f'<span style="color:#070">converged (v{version})</span>'
    )

    return f"""<!doctype html>
<html><head><meta charset="utf-8"><title>Config — {did}</title>
<style>
 body{{font-family:system-ui,sans-serif;max-width:680px;margin:2rem auto;padding:0 1rem}}
 label{{display:block;margin:.75rem 0 .25rem;font-weight:600}}
 input,select,textarea{{width:100%;padding:.4rem;font-size:1rem;box-sizing:border-box}}
 textarea{{font-family:monospace;height:6rem}}
 .row{{margin-bottom:.5rem}} button{{margin-top:1rem;padding:.5rem 1rem;font-size:1rem}}
 pre#out{{background:#f4f4f4;padding:.75rem;white-space:pre-wrap}}
</style></head><body>
<h1>Remote config — <code>{did}</code></h1>
<p>Desired config version: <b>v{version}</b> · {drift_badge}</p>
<form id="f">
 <div class="row"><label>telemetry_tier</label>
   <select id="telemetry_tier">{tier_opts}</select></div>
 <div class="row"><label>interval_s</label>
   <input id="interval_s" type="number" min="1" value="{int(interval)}"></div>
 <div class="row"><label>sampling (0–1)</label>
   <input id="sampling" type="number" min="0" max="1" step="0.01" value="{float(sampling)}"></div>
 <div class="row"><label>feature_flags (JSON object of name → bool)</label>
   <textarea id="feature_flags">{flags_json}</textarea></div>
 <div class="row"><label>reason</label><input id="reason" placeholder="why this change"></div>
 <div class="row"><label>operator bearer token</label>
   <input id="token" type="password" placeholder="OPERATOR_TOKEN"></div>
 <button type="submit">Push config</button>
</form>
<h3>Result</h3><pre id="out">(submit to push)</pre>
<script>
document.getElementById('f').addEventListener('submit', async (e) => {{
  e.preventDefault();
  const out = document.getElementById('out');
  let flags;
  try {{ flags = JSON.parse(document.getElementById('feature_flags').value || '{{}}'); }}
  catch (err) {{ out.textContent = 'feature_flags is not valid JSON: ' + err; return; }}
  const body = {{
    telemetry_tier: document.getElementById('telemetry_tier').value,
    interval_s: parseInt(document.getElementById('interval_s').value, 10),
    sampling: parseFloat(document.getElementById('sampling').value),
    feature_flags: flags,
    reason: document.getElementById('reason').value,
  }};
  out.textContent = 'pushing…';
  try {{
    const r = await fetch({json.dumps(f"/api/v1/deployments/{deployment_id}/desired-config")}, {{
      method: 'PUT',
      headers: {{
        'Content-Type': 'application/json',
        'Authorization': 'Bearer ' + document.getElementById('token').value,
      }},
      body: JSON.stringify(body),
    }});
    const text = await r.text();
    out.textContent = 'HTTP ' + r.status + '\\n' + text;
    if (r.ok) setTimeout(() => location.reload(), 600);
  }} catch (err) {{ out.textContent = 'request failed: ' + err; }}
}});
</script>
</body></html>"""
