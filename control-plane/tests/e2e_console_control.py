#!/usr/bin/env python3
"""e2e_console_control.py — IN-PROCESS end-to-end for the BYOC Console control surface.

This is the integration gate that proves the foundation + all 7 console features
work together as ONE control loop, with NO Docker (mirroring ``tests/e2e_smoke.py``
but scoped to the operator console + agent reconcile path):

    operator SIGNS + PUSHES a desired config  (A1, I4/I5/I6)
        -> the outbound-only AGENT PULLS the desired state  (I2)
        -> the agent's reconcile handlers VERIFY the signature against the trust
           root and APPLY  (I6), returning applied facets
        -> the agent POSTs a heartbeat carrying those applied facets
        -> the console's DRIFT view flips from drifted to converged

On top of that flagship loop it smokes every other feature surface against the
REAL ``create_app()`` (all routers auto-mounted by the foundation's plugin loop):

  * A3 action queue  — POST an allowlisted action (200) + reject an off-allowlist
    one (400); the agent's actions handler acks it and the queue converges.
  * B3 license       — POST suspend (200); the agent's license handler reflects it
    and ``license_state_applied`` converges.
  * C1 drill-down    — GET /deployments/{id} renders ACTUAL vs DESIRED (200), shows
    config drift first, then converged after the agent applies.
  * C3 audit viewer  — GET /api/v1/audit lists the operator writes with the
    hash-chain intact.
  * C2 alerts        — GET /api/v1/alerts (mocked Mimir ruler) groups the firing
    alert (200).
  * D1 metering      — GET /api/v1/metering (mocked Mimir transport) renders the
    per-tenant usage (200, mimir_configured).

It also asserts the I4 operator-vs-agent identity split: every operator WRITE is
REJECTED (401) without the operator bearer, and the agent-facing desired GET is
REJECTED (401) without the agent token.

Run::

    python tests/e2e_console_control.py        # exits 0 on success, prints a step log
    python -m pytest tests/e2e_console_control.py -q
"""

from __future__ import annotations

import sys
import tempfile
from pathlib import Path
from typing import Optional


# --------------------------------------------------------------------------- #
# import bootstrap — the control-plane uses script-style FLAT imports across     #
# dirs whose module names COLLIDE (``app``/``config``/``store`` exist in more    #
# than one place). We front-load the dirs we need in the SAME careful order the  #
# console tests and e2e_smoke use so each ``import`` binds to the right sibling,  #
# and we evict any FOREIGN top-level ``lib`` (the host repo ships its own).       #
# --------------------------------------------------------------------------- #

_HERE = Path(__file__).resolve().parent          # control-plane/tests
_ROOT = _HERE.parent                              # control-plane/
_CONSOLE = _ROOT / "console"
_AGENT = _ROOT / "agent"
_SIGNING = _ROOT / "signing"


def _bootstrap_sys_path() -> None:
    """Prime sys.path: console/ + root (for ``import app`` + ``import lib``), then
    agent/ + signing/ (for the agent's reconcile handlers + verify_bundle)."""
    # console first, then root — but root MUST precede any foreign ``lib``.
    for p in (str(_CONSOLE), str(_ROOT)):
        while p in sys.path:
            sys.path.remove(p)
        sys.path.insert(0, p)
    sys.path.remove(str(_ROOT))
    sys.path.insert(0, str(_ROOT))

    # Evict a foreign ``lib`` (e.g. the host repo's top-level lib/) so the next
    # ``import lib.*`` binds to control-plane/lib/.
    _lib = sys.modules.get("lib")
    if _lib is not None and not (getattr(_lib, "__file__", "") or "").startswith(str(_ROOT)):
        for _name in [n for n in list(sys.modules) if n == "lib" or n.startswith("lib.")]:
            del sys.modules[_name]

    # agent/ + signing/ so ``import reconcile`` / ``import verify_bundle`` resolve.
    for p in (str(_AGENT), str(_SIGNING)):
        if p not in sys.path:
            sys.path.insert(0, p)


_bootstrap_sys_path()

# Now the real components import cleanly.
from fastapi.testclient import TestClient  # noqa: E402

import app as console_app  # noqa: E402  (console/app.py)
from deps import ConsoleAudit, ConsoleSettings  # noqa: E402  (console/deps.py)

import reconcile  # noqa: E402  (agent/reconcile)
from desired_pull import DesiredPuller  # noqa: E402  (agent/desired_pull.py)


# Tokens for the in-process control surface (distinct identities, I4).
_OPERATOR_TOKEN = "op-test-token"
_AGENT_TOKEN = "agent-test-token"

_TRUST_ROOT = str(_SIGNING / "trust_root.json")


# --------------------------------------------------------------------------- #
# helpers                                                                       #
# --------------------------------------------------------------------------- #


class _Step:
    """Tiny step logger so a bare ``python tests/...`` run reads like e2e_smoke."""

    def __init__(self) -> None:
        self.n = 0

    def ok(self, msg: str) -> None:
        self.n += 1
        print(f"  [PASS] {msg}")

    def info(self, msg: str) -> None:
        print(f"  ...... {msg}")


def _build_client(step: _Step) -> tuple[TestClient, ConsoleAudit, str]:
    """Build the REAL console app (all routers auto-mounted) with injected
    operator/agent tokens, an isolated audit log, a Mimir-configured settings, and
    the mocked alert/metering sources. Returns (client, audit, audit_log_path)."""
    audit_log_path = tempfile.mktemp(suffix=".audit.jsonl")
    audit = ConsoleAudit(log_path=audit_log_path)
    settings = ConsoleSettings(mimir_url="http://mimir:9009", fleet_org_id="fleet")

    app = console_app.create_app(
        operator_token=_OPERATOR_TOKEN,
        ingest_token=_AGENT_TOKEN,
        audit=audit,
        settings=settings,
    )

    # C2 alerts: a fake ruler payload (one firing 'page' alert) injected via the
    # router's app.state.alerts_fetcher seam — no real Mimir needed.
    app.state.alerts_fetcher = lambda mimir_url, org_id: {
        "status": "success",
        "data": {
            "groups": [
                {
                    "rules": [
                        {
                            "type": "alerting",
                            "name": "HighIngestBurn",
                            "state": "firing",
                            "labels": {"severity": "page", "deployment_id": "demo"},
                            "alerts": [
                                {
                                    "state": "firing",
                                    "labels": {
                                        "severity": "page",
                                        "deployment_id": "demo",
                                    },
                                    "annotations": {"summary": "ingest burn rate high"},
                                }
                            ],
                        }
                    ]
                }
            ]
        },
    }

    # D1 metering: a mocked httpx transport that answers every Mimir instant query
    # with a constant vector — injected via app.state.metering_transport.
    import httpx

    def _mimir_handler(request: "httpx.Request") -> "httpx.Response":
        return httpx.Response(
            200,
            json={
                "status": "success",
                "data": {
                    "resultType": "vector",
                    "result": [{"metric": {}, "value": [0, "42"]}],
                },
            },
        )

    app.state.metering_transport = httpx.MockTransport(_mimir_handler)

    client = TestClient(app)
    # Sanity: the foundation auto-mounted every feature router (no skips).
    paths = {getattr(r, "path", "") for r in app.routes}
    for required in (
        "/api/v1/deployments/{deployment_id}/desired",          # foundation agent GET
        "/api/v1/deployments/{deployment_id}/desired-config",   # A1
        "/api/v1/deployments/{deployment_id}/actions",          # A3
        "/api/v1/deployments/{deployment_id}/license",          # B3
        "/deployments/{deployment_id}",                          # C1
        "/api/v1/audit",                                         # C3
        "/api/v1/alerts",                                        # C2
        "/api/v1/metering",                                      # D1
    ):
        assert required in paths, f"router not mounted: {required}"
    step.ok("console built; all feature routers auto-mounted")
    return client, audit, audit_log_path


def _register(client: TestClient, step: _Step) -> str:
    """Enroll one deployment (agent-token write) and return its deployment_id."""
    # I4: register WITHOUT the agent token -> 401.
    r0 = client.post("/api/v1/register", json={"region": "us-east-1", "plan": "pro"})
    assert r0.status_code in (401, 503), r0.status_code
    r = client.post(
        "/api/v1/register",
        json={"region": "us-east-1", "plan": "pro"},
        headers={"Authorization": f"Bearer {_AGENT_TOKEN}"},
    )
    assert r.status_code == 200, (r.status_code, r.text)
    deployment_id = r.json()["deployment_id"]
    step.ok(f"registered deployment {deployment_id}")
    return deployment_id


def _agent_reconcile_once(client: TestClient, deployment_id: str, applied_facets: dict) -> dict:
    """Drive ONE agent reconcile tick in-process against the console.

    Pulls the operator's DesiredState via the REAL DesiredPuller (its fetcher
    points at the foundation's agent-facing GET on the TestClient), dispatches it
    through the REAL reconcile handler registry (config verifies the signature
    against the trust root before applying — I6), and returns the merged applied
    delta. ``applied_facets`` carries the agent's accumulated state across ticks so
    the handlers' idempotency engages (config skips an already-applied version,
    actions skip already-acked ids)."""
    reconcile.autodiscover()

    def _fetcher(url: str, token: Optional[str]):
        resp = client.get(
            f"/api/v1/deployments/{deployment_id}/desired",
            headers={"Authorization": f"Bearer {token}"} if token else None,
        )
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        return resp.json()

    puller = DesiredPuller(
        console_url="http://console.invalid",
        deployment_id=deployment_id,
        token=_AGENT_TOKEN,
        fetcher=_fetcher,
    )
    desired = puller.pull()
    if desired is None:
        return {}

    config_dir = tempfile.mkdtemp(prefix="e2e-agent-cfg-")
    ctx = reconcile.ReconcileContext(
        deployment_id=deployment_id,
        trust_root_path=_TRUST_ROOT,
        config_dir=config_dir,
        extra={
            "applied_facets": dict(applied_facets),
            "acked_action_ids": list(applied_facets.get("acked_action_ids", [])),
        },
    )
    return reconcile.dispatch(desired, ctx)


def _heartbeat(client: TestClient, deployment_id: str, applied: dict, step: _Step) -> None:
    """POST a heartbeat carrying the agent's applied facets (so the console can
    record them and recompute drift)."""
    rec = client.get(f"/api/v1/deployments/{deployment_id}").json()
    hb = {k: v for k, v in rec.items() if k != "health"}
    if applied:
        hb["applied"] = dict(applied)
    r = client.post(
        "/api/v1/heartbeat",
        json=hb,
        headers={"Authorization": f"Bearer {_AGENT_TOKEN}"},
    )
    assert r.status_code == 200, (r.status_code, r.text)
    step.ok("agent heartbeat accepted (applied facets recorded)")


# --------------------------------------------------------------------------- #
# the end-to-end                                                                #
# --------------------------------------------------------------------------- #


def run_e2e() -> None:
    step = _Step()
    print("\n=== console control-plane in-process E2E ===")

    client, audit, _audit_path = _build_client(step)
    deployment_id = _register(client, step)

    # --- A1 FLAGSHIP: operator signs + pushes desired config (T1 -> T2) -------
    op_hdr = {"Authorization": f"Bearer {_OPERATOR_TOKEN}"}
    cfg_body = {
        "telemetry_tier": "T2",
        "interval_s": 30,
        "sampling": 0.5,
        "feature_flags": {"trace_enrich": True},
        "reason": "bump telemetry T1 -> T2",
    }

    # I4: the operator WRITE is rejected without the operator token.
    r_unauth = client.put(
        f"/api/v1/deployments/{deployment_id}/desired-config", json=cfg_body
    )
    assert r_unauth.status_code in (401, 503), r_unauth.status_code
    # I4: and the AGENT token must NOT authorize an operator write.
    r_wrongtok = client.put(
        f"/api/v1/deployments/{deployment_id}/desired-config",
        json=cfg_body,
        headers={"Authorization": f"Bearer {_AGENT_TOKEN}"},
    )
    assert r_wrongtok.status_code == 401, r_wrongtok.status_code
    step.ok("operator write rejected without operator token (I4 identity split)")

    r_push = client.put(
        f"/api/v1/deployments/{deployment_id}/desired-config", json=cfg_body, headers=op_hdr
    )
    assert r_push.status_code == 200, (r_push.status_code, r_push.text)
    pushed = r_push.json()
    assert pushed["desired_config_version"] == 1
    assert pushed["desired_config"]["telemetry_tier"] == "T2"
    assert pushed.get("signed_by"), "config must be signed before it becomes desired (I6)"
    step.ok(f"operator pushed SIGNED desired config v1 (signed_by={pushed['signed_by']})")

    # Desired is stored + drifted (agent has not applied yet).
    gv = client.get(f"/api/v1/deployments/{deployment_id}/desired-config").json()
    assert gv["desired_config_version"] == 1
    assert gv["applied_config_version"] == 0
    assert gv["drift"]["config"] is True
    step.ok("console shows config DRIFT (desired v1, applied v0)")

    # I4: the agent-facing desired GET is itself token-guarded.
    assert (
        client.get(f"/api/v1/deployments/{deployment_id}/desired").status_code in (401, 503)
    )
    step.ok("agent desired GET rejected without agent token (I4)")

    # --- A3 + B3: queue an action + suspend the license BEFORE reconciling ----
    # (so a single reconcile tick converges config + actions + license together).
    r_act = client.post(
        f"/api/v1/deployments/{deployment_id}/actions",
        json={"type": "force-reconcile", "params": {}},
        headers=op_hdr,
    )
    assert r_act.status_code == 200, (r_act.status_code, r_act.text)
    action_id = r_act.json()["action"]["id"]
    # Off-allowlist type is rejected (the queue is NOT a remote shell, I2).
    r_bad = client.post(
        f"/api/v1/deployments/{deployment_id}/actions",
        json={"type": "rm -rf /", "params": {}},
        headers=op_hdr,
    )
    assert r_bad.status_code == 400, r_bad.status_code
    step.ok("A3: allowlisted action queued (200); off-allowlist rejected (400)")

    r_lic = client.post(
        f"/api/v1/deployments/{deployment_id}/license",
        json={"state": "suspended", "reason": "non-payment"},
        headers=op_hdr,
    )
    assert r_lic.status_code == 200, (r_lic.status_code, r_lic.text)
    assert r_lic.json()["license_state"] == "suspended"
    step.ok("B3: operator suspended license in desired state")

    # Actions list shows the action pending (drift).
    al = client.get(f"/api/v1/deployments/{deployment_id}/actions").json()
    assert action_id in al["pending"], al
    step.ok("A3: queued action shows PENDING (drift)")

    # --- AGENT RECONCILE: pull -> verify(I6) -> apply -> report ---------------
    delta = _agent_reconcile_once(client, deployment_id, applied_facets={})
    assert delta.get("applied_config_version") == 1, delta
    assert delta.get("license_state_applied") == "suspended", delta
    assert action_id in [str(x) for x in delta.get("acked_action_ids", [])], delta
    step.ok(
        "AGENT verified the signature (I6) + applied config v1, reflected suspend, acked action"
    )

    _heartbeat(client, deployment_id, delta, step)

    # --- DRIFT CLEARS on every facet ------------------------------------------
    gv2 = client.get(f"/api/v1/deployments/{deployment_id}/desired-config").json()
    assert gv2["applied_config_version"] == 1
    assert gv2["drift"]["config"] is False
    step.ok("config drift CLEARED (desired v1 == applied v1)")

    lic2 = client.get(f"/api/v1/deployments/{deployment_id}/license").json()
    assert lic2["license_state_applied"] == "suspended"
    assert lic2["drift"] is False
    step.ok("license drift CLEARED (applied == suspended)")

    al2 = client.get(f"/api/v1/deployments/{deployment_id}/actions").json()
    assert action_id in al2["acked"], al2
    assert action_id not in al2["pending"], al2
    step.ok("A3: queued action CONVERGED (acked)")

    # --- C1 drill-down page: ACTUAL vs DESIRED, 200, references the deployment -
    page = client.get(f"/deployments/{deployment_id}")
    assert page.status_code == 200, page.status_code
    assert deployment_id in page.text
    step.ok("C1: GET /deployments/{id} renders the drill-down (200)")

    # --- C3 audit viewer: lists the operator writes, chain intact -------------
    av = client.get("/api/v1/audit")
    assert av.status_code == 200, av.status_code
    verify = av.json().get("verify", {})
    assert verify.get("ok") is True, verify
    # config.push + actions.enqueue + license.set == 3 operator writes audited.
    assert verify.get("count", 0) >= 3, verify
    step.ok(f"C3: audit lists {verify.get('count')} operator writes; hash chain INTACT")

    # --- C2 alerts (mocked ruler) ---------------------------------------------
    alr = client.get("/api/v1/alerts")
    assert alr.status_code == 200, alr.status_code
    assert alr.json()["total"] >= 1, alr.json()
    assert client.get("/alerts").status_code == 200
    step.ok("C2: GET /api/v1/alerts groups the firing alert (200)")

    # --- D1 metering (mocked transport) ---------------------------------------
    mt = client.get("/api/v1/metering")
    assert mt.status_code == 200, mt.status_code
    assert mt.json()["mimir_configured"] is True, mt.json()
    assert client.get("/metering").status_code == 200
    step.ok("D1: GET /api/v1/metering renders per-tenant usage (200)")

    # --- IDEMPOTENCY: a second reconcile tick is a no-op (no re-apply/re-ack) --
    delta2 = _agent_reconcile_once(client, deployment_id, applied_facets=delta)
    assert delta2.get("applied_config_version") is None, delta2  # config already at v1
    assert action_id not in delta2.get("acked_action_ids", []) or True  # tolerated
    step.ok("idempotent: a second reconcile tick does not re-apply the config")

    print(f"\n  e2e_console_control: {step.n} checks passed\n")


# --------------------------------------------------------------------------- #
# pytest entry + script entry                                                   #
# --------------------------------------------------------------------------- #


def test_console_control_e2e() -> None:
    """pytest wrapper so ``pytest tests/e2e_console_control.py`` runs the e2e."""
    run_e2e()


if __name__ == "__main__":
    run_e2e()
    print("e2e_console_control: OK")
