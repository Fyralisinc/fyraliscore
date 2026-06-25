#!/usr/bin/env python3
"""Isolated self-test for the C1 deployment-view router.

Mounts ONLY ``deployment_view.register`` on a bare FastAPI app with an in-memory
stub deps (fake store + a no-op audit + dummy settings). Does NOT import the full
console app or the other (possibly mid-write) feature routers. Run::

    cd control-plane && \
      /home/prajwal-adhikari/Desktop/v2/fyraliscore/.venv/bin/python \
      console/routers/_test_deployment_view.py
"""

from __future__ import annotations

import sys
from pathlib import Path

_HERE = Path(__file__).resolve().parent
_CONSOLE = _HERE.parent          # console/
_ROOT = _CONSOLE.parent          # control-plane/
for _p in (str(_ROOT), str(_CONSOLE)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from fastapi import FastAPI
from fastapi.testclient import TestClient

from lib.deployment import DeploymentRecord  # noqa: E402
from lib.desired_state import DesiredState  # noqa: E402
from lib.primitives import utcnow  # noqa: E402

import routers.deployment_view as dv  # noqa: E402


# --- in-memory stubs --------------------------------------------------------


class FakeStore:
    def __init__(self):
        self._rec = {}
        self._desired = {}
        self._applied = {}

    def record(self, dep_id):
        return self._rec.get(dep_id)

    def get_desired(self, dep_id):
        return self._desired.get(dep_id)

    def get_applied(self, dep_id):
        return dict(self._applied.get(dep_id, {}))


class FakeEntry:
    def __init__(self, ts, actor, action, target, metadata=None):
        self.ts, self.actor, self.action, self.target = ts, actor, action, target
        self.metadata = metadata or {}


class FakeLog:
    def __init__(self, entries):
        self._entries = entries

    def entries(self):
        return list(self._entries)


class FakeAudit:
    """Mimics ConsoleAudit's _log / _ensure shape so the reader can find it."""

    def __init__(self, entries=None):
        self._log = FakeLog(entries) if entries is not None else None

    def append(self, event):  # never called by a read-only page
        return None


class FakeSettings:
    grafana_url = "http://grafana.example:3030"


class FakeDeps:
    def __init__(self, store, audit, settings):
        self.store = store
        self.audit = audit
        self.settings = settings


def _mk_record(dep_id="acme-use1-7f3a", tenant="acme"):
    now = utcnow()
    return DeploymentRecord.heartbeat(
        tenant_id=tenant,
        deployment_id=dep_id,
        version="1.4.2",
        region="us-east-1",
        license_expiry=now.replace(year=now.year + 1),
        telemetry_tier="T1",
        last_heartbeat_ts=now,
        now=now,
    )


def _client(deps):
    app = FastAPI()
    dv.register(app, deps)
    return TestClient(app)


def main() -> int:
    failures = []

    def check(name, cond):
        print(("PASS" if cond else "FAIL"), name)
        if not cond:
            failures.append(name)

    dep_id = "acme-use1-7f3a"

    # 1) Full populated page: record + desired (config v2) + applied (v1) => config DRIFT.
    store = FakeStore()
    store._rec[dep_id] = _mk_record(dep_id)
    store._desired[dep_id] = DesiredState(
        deployment_id=dep_id,
        desired_config={"telemetry_tier": "T2", "interval_s": 30,
                        "feature_flags": {"x": True}},
        desired_config_version=2,
        desired_config_sig={"sig": "abc", "manifest": {}, "signed_by": "k1"},
        desired_release="1.5.0",
        license_state="active",
        pending_actions=[{"id": "a1", "type": "force-reconcile", "params": {}}],
        updated_by="ops@fyralis",
        reason="bump tier",
    )
    store._applied[dep_id] = {
        "applied_config_version": 1,
        "applied_release": "1.4.2",
        "acked_action_ids": [],
        "license_state_applied": "active",
    }
    audit = FakeAudit(
        entries=[
            FakeEntry("2026-06-25T00:00:00Z", "ops@fyralis", "desired.config.write", dep_id),
            FakeEntry("2026-06-25T00:01:00Z", "ops@fyralis", "other.event", "someone-else"),
        ]
    )
    c = _client(FakeDeps(store, audit, FakeSettings()))
    r = c.get(f"/deployments/{dep_id}")
    body = r.text
    check("200 on populated deployment", r.status_code == 200)
    check("renders tenant", "acme" in body)
    check("renders version 1.4.2", "1.4.2" in body)
    check("renders desired tier T2", "T2" in body)
    check("Config drift shown", "Config: DRIFT" in body)
    check("Release drift shown", "Release: DRIFT" in body)
    check("License OK (no drift)", "License: OK" in body)
    check("Actions drift shown", "Actions: DRIFT" in body)
    check("unacked action id a1 listed", "a1" in body)
    check("grafana deep-link present", "mimir-acme" in body and "fyralis-tenant-drilldown" in body)
    check("grafana base from settings used", "grafana.example:3030" in body)
    check("audit event for this dep shown", "desired.config.write" in body)
    check("audit event for OTHER dep filtered out", "other.event" not in body)
    check("operator action links present", f"/deployments/{dep_id}/config" in body)

    # 2) No desired state => no drift, graceful empty desired section.
    store2 = FakeStore()
    store2._rec[dep_id] = _mk_record(dep_id)
    c2 = _client(FakeDeps(store2, FakeAudit(entries=[]), FakeSettings()))
    r2 = c2.get(f"/deployments/{dep_id}")
    check("200 with record but no desired", r2.status_code == 200)
    check("no-desired => Config: OK", "Config: OK" in r2.text)
    check("no-desired => empty desired msg", "No desired state written yet" in r2.text)
    check("no-desired => audit empty msg", "No audit events" in r2.text)

    # 3) Unknown deployment (no record, no desired) still renders (not 500/404).
    c3 = _client(FakeDeps(FakeStore(), FakeAudit(entries=None), FakeSettings()))
    r3 = c3.get("/deployments/ghost-x")
    check("200 for unknown deployment", r3.status_code == 200)
    check("unknown => no registry row msg", "No registry row" in r3.text)
    check("audit reader unavailable => graceful", "Audit trail not available" in r3.text)

    # 4) License drift path: desired suspended, applied active.
    store4 = FakeStore()
    store4._rec[dep_id] = _mk_record(dep_id)
    store4._desired[dep_id] = DesiredState(deployment_id=dep_id, license_state="suspended")
    store4._applied[dep_id] = {"license_state_applied": "active"}
    c4 = _client(FakeDeps(store4, FakeAudit(entries=[]), FakeSettings()))
    r4 = c4.get(f"/deployments/{dep_id}")
    check("license drift shown", "License: DRIFT" in r4.text)

    # 5) No settings.grafana_url => default base used, still links.
    class NoGrafana:
        grafana_url = ""
    store5 = FakeStore()
    store5._rec[dep_id] = _mk_record(dep_id)
    c5 = _client(FakeDeps(store5, FakeAudit(entries=[]), NoGrafana()))
    r5 = c5.get(f"/deployments/{dep_id}")
    check("default grafana base when unset", "localhost:3030" in r5.text)

    print()
    if failures:
        print(f"{len(failures)} FAILED: {failures}")
        return 1
    print("ALL GREEN")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
