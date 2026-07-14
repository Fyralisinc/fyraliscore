#!/usr/bin/env python3
"""Isolation selftest for the B3 LICENSE / ENTITLEMENT feature.

Self-contained: does NOT import the full console app or run the full agent (other
agents may be mid-write). It mounts ONLY ``console/routers/license.py`` on a bare
FastAPI app with in-memory stub deps, and calls the agent handler
``agent/reconcile/license.py`` ``apply()`` directly with a fake ctx + fake agent.

Run:
  cd control-plane && \
    /home/prajwal-adhikari/Desktop/v2/fyraliscore/.venv/bin/python \
    console/routers/_license_selftest.py
"""

from __future__ import annotations

import dataclasses
import sys
from pathlib import Path

# --- path bootstrap: control-plane root (lib), console, agent dirs -----------
_HERE = Path(__file__).resolve()
_CONSOLE_DIR = _HERE.parent.parent          # control-plane/console
_ROOT = _CONSOLE_DIR.parent                 # control-plane
_AGENT_DIR = _ROOT / "agent"
for _p in (str(_ROOT), str(_CONSOLE_DIR), str(_AGENT_DIR)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from fastapi import Depends, FastAPI, Header, HTTPException  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402

from lib.desired_state import DesiredState  # noqa: E402

_PASSES = []
_FAILS = []


def check(name: str, cond: bool, detail: str = "") -> None:
    (_PASSES if cond else _FAILS).append(name)
    mark = "ok " if cond else "FAIL"
    print(f"  [{mark}] {name}" + (f" — {detail}" if detail and not cond else ""))


# --------------------------------------------------------------------------- #
# stub deps                                                                   #
# --------------------------------------------------------------------------- #

_TEST_OPERATOR_TOKEN = "test-operator-token"


class _StubStore:
    def __init__(self) -> None:
        self._desired: dict[str, DesiredState] = {}
        self._applied: dict[str, dict] = {}

    def put_desired(self, deployment_id, desired):
        stored = desired.model_copy(update={"deployment_id": deployment_id})
        self._desired[deployment_id] = stored
        return stored

    def get_desired(self, deployment_id):
        return self._desired.get(deployment_id)

    def get_applied(self, deployment_id):
        return dict(self._applied.get(deployment_id, {}))

    def set_applied(self, deployment_id, applied):  # test helper
        self._applied[deployment_id] = dict(applied)


class _StubAudit:
    def __init__(self) -> None:
        self.events = []

    def append(self, event):
        self.events.append(event)
        return event


def _require_operator(authorization: str | None = Header(default=None)) -> None:
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="missing operator bearer")
    if authorization.split(" ", 1)[1] != _TEST_OPERATOR_TOKEN:
        raise HTTPException(status_code=401, detail="bad operator bearer")


@dataclasses.dataclass
class _StubDeps:
    store: object
    audit: object
    require_operator: object
    signer: object = None
    require_agent_write: object = None
    settings: object = None


def _build_client():
    import importlib

    lic = importlib.import_module("routers.license")
    app = FastAPI()
    store = _StubStore()
    audit = _StubAudit()
    deps = _StubDeps(store=store, audit=audit, require_operator=_require_operator)
    lic.register(app, deps)
    return TestClient(app), store, audit


def _auth():
    return {"Authorization": f"Bearer {_TEST_OPERATOR_TOKEN}"}


# --------------------------------------------------------------------------- #
# router tests                                                                #
# --------------------------------------------------------------------------- #


def test_router() -> None:
    print("router (console/routers/license.py):")
    client, store, audit = _build_client()
    dep = "acme-use1-0001"

    # GET defaults to active, no 404 even with no desired state.
    r = client.get(f"/api/v1/deployments/{dep}/license")
    check("GET defaults to active (no desired yet)", r.status_code == 200 and r.json()["license_state"] == "active", r.text)

    # WRITE requires operator token.
    r = client.post(f"/api/v1/deployments/{dep}/license", json={"state": "suspended"})
    check("POST without token -> 401", r.status_code == 401, r.text)

    # Suspend.
    r = client.post(f"/api/v1/deployments/{dep}/license", json={"state": "suspended", "reason": "nonpayment"}, headers=_auth())
    check("POST suspended -> 200", r.status_code == 200, r.text)
    check("POST suspended sets license_state", r.json().get("license_state") == "suspended", r.text)
    check("POST suspended writes desired", store.get_desired(dep) is not None and store.get_desired(dep).license_state == "suspended")
    check("POST suspended audited (I5)", any(e["action"] == "license.set" and e["target"] == dep for e in audit.events))
    check("audit metadata carries reason", any(e.get("metadata", {}).get("reason") == "nonpayment" for e in audit.events))
    check("updated_by stamped operator", store.get_desired(dep).updated_by == "operator")

    # Drift shows: desired suspended, applied still active.
    store.set_applied(dep, {"license_state_applied": "active"})
    r = client.get(f"/api/v1/deployments/{dep}/license")
    body = r.json()
    check("GET reflects drift (desired suspended, applied active)", body["license_state"] == "suspended" and body["license_state_applied"] == "active" and body["drift"] is True, r.text)

    # Re-activate.
    r = client.post(f"/api/v1/deployments/{dep}/license", json={"state": "active"}, headers=_auth())
    check("POST active -> 200", r.status_code == 200 and r.json()["license_state"] == "active", r.text)

    # Invalid state rejected.
    r = client.post(f"/api/v1/deployments/{dep}/license", json={"state": "bogus"}, headers=_auth())
    check("POST invalid state -> 400", r.status_code == 400, r.text)

    # Read-modify-write preserves unrelated desired fields (config not clobbered).
    dep2 = "beta-use1-0002"
    store.put_desired(dep2, DesiredState(deployment_id=dep2, desired_config={"telemetry_tier": "T2"}, desired_config_version=7))
    client.post(f"/api/v1/deployments/{dep2}/license", json={"state": "suspended"}, headers=_auth())
    d2 = store.get_desired(dep2)
    check("license write preserves desired_config", d2.desired_config == {"telemetry_tier": "T2"} and d2.desired_config_version == 7)


# --------------------------------------------------------------------------- #
# agent handler tests                                                         #
# --------------------------------------------------------------------------- #


@dataclasses.dataclass
class _FakeStatus:
    ok: bool
    reason: str
    expires_at: object = "2099-01-01T00:00:00Z"


class _FakeChecker:
    """Stands in for LicenseChecker — locally valid unless told otherwise."""

    def __init__(self, ok: bool = True) -> None:
        self._ok = ok

    def is_licensed(self, **kw) -> bool:
        return self._ok

    def evaluate(self, **kw):
        return _FakeStatus(ok=self._ok, reason="local license valid")

    def license_expiry(self, **kw):
        return self.evaluate().expires_at


class _FakeAgent:
    def __init__(self, checker) -> None:
        self.license_checker = checker

    def is_licensed(self) -> bool:
        # Mirror Agent.is_licensed(): delegates to whatever checker is installed.
        return self.license_checker.is_licensed()


def _ctx(agent):
    from reconcile.registry import ReconcileContext

    return ReconcileContext(
        deployment_id="acme-use1-0001",
        trust_root_path="/nonexistent/trust_root.json",
        config_dir="/tmp/cfg",
        extra={"agent": agent} if agent is not None else {},
    )


def test_agent_handler() -> None:
    print("agent handler (agent/reconcile/license.py):")
    import importlib

    mod = importlib.import_module("reconcile.license")

    # Self-registration.
    from reconcile.registry import list_handlers

    check("handler self-registers as 'license'", "license" in list_handlers())

    # active: local license wins, applied=active.
    agent = _FakeAgent(_FakeChecker(ok=True))
    delta = mod.apply(DesiredState(deployment_id="d", license_state="active"), _ctx(agent))
    check("active -> license_state_applied=active", delta == {"license_state_applied": "active"}, str(delta))
    check("active -> agent stays licensed", agent.is_licensed() is True)

    # suspended: composes OVER a still-valid local license -> not licensed.
    delta = mod.apply(DesiredState(deployment_id="d", license_state="suspended"), _ctx(agent))
    check("suspended -> license_state_applied=suspended", delta == {"license_state_applied": "suspended"}, str(delta))
    check("suspended -> agent.is_licensed() is False despite valid local license", agent.is_licensed() is False)
    check("suspended -> evaluate().ok forced False", agent.license_checker.evaluate().ok is False)
    check("suspended -> evaluate preserves expires_at", agent.license_checker.evaluate().expires_at == "2099-01-01T00:00:00Z")

    # re-active reverses it (same wrapper, flag flipped).
    mod.apply(DesiredState(deployment_id="d", license_state="active"), _ctx(agent))
    check("re-active -> agent licensed again", agent.is_licensed() is True)

    # idempotent: re-suspend does not double-wrap.
    mod.apply(DesiredState(deployment_id="d", license_state="suspended"), _ctx(agent))
    inner = agent.license_checker
    mod.apply(DesiredState(deployment_id="d", license_state="suspended"), _ctx(agent))
    check("re-suspend does not re-wrap (idempotent)", agent.license_checker is inner)

    # I3: missing agent handle -> still reports applied facet, never crashes.
    delta = mod.apply(DesiredState(deployment_id="d", license_state="suspended"), _ctx(None))
    check("no agent in ctx -> still reports facet (I3)", delta == {"license_state_applied": "suspended"}, str(delta))

    # I3: a checker that raises on wrap -> swallowed, facet still reported.
    class _Boom:
        @property
        def _fyralis_suspendable(self):  # pretend-wrapper attr access path
            return False

    class _BoomAgent:
        def __init__(self):
            self.license_checker = _BoomChecker()

    class _BoomChecker:
        def is_licensed(self, **kw):
            raise RuntimeError("boom")

    delta = mod.apply(DesiredState(license_state="suspended"), _ctx(_BoomAgent()))
    check("wrap over odd checker -> facet still reported (I3)", delta.get("license_state_applied") == "suspended")


def main() -> int:
    print("=== B3 LICENSE / ENTITLEMENT isolation selftest ===")
    test_router()
    test_agent_handler()
    print(f"\n{len(_PASSES)} passed, {len(_FAILS)} failed")
    if _FAILS:
        print("FAILURES:", _FAILS)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
