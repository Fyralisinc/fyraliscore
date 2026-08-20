#!/usr/bin/env python3
"""Self-contained isolation test for the A3 ACTION QUEUE feature.

Tests the router (console/routers/actions.py) by mounting ONLY its register() on a
bare FastAPI app with stub deps, and tests the agent handler
(agent/reconcile/actions.py) by calling apply(desired, ctx) directly with a fake
ctx. Does NOT import the full console app or the full agent.

Run:  cd control-plane && /path/to/.venv/bin/python console/routers/_actions_isolation_test.py
"""

from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace

# Put the control-plane root on sys.path so `lib`, `routers`, `reconcile` resolve.
_ROOT = Path(__file__).resolve().parents[2]  # control-plane/
for _p in (str(_ROOT), str(_ROOT / "agent"), str(_ROOT / "console")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from fastapi import FastAPI, HTTPException, Header  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402

from lib.desired_state import DesiredState, compute_drift  # noqa: E402

OP_TOKEN = "test-operator-token"
PASSED = []
FAILED = []


def check(name: str, cond: bool) -> None:
    (PASSED if cond else FAILED).append(name)
    print(f"  [{'PASS' if cond else 'FAIL'}] {name}")


# --------------------------------------------------------------------------- #
# fakes                                                                        #
# --------------------------------------------------------------------------- #


class FakeStore:
    def __init__(self) -> None:
        self._desired = {}
        self._applied = {}

    def get_desired(self, did):
        return self._desired.get(did)

    def put_desired(self, did, desired):
        stored = desired.model_copy(update={"deployment_id": did})
        self._desired[did] = stored
        return stored

    def get_applied(self, did):
        return dict(self._applied.get(did, {}))

    def record_applied(self, did, applied):
        cur = dict(self._applied.get(did, {}))
        cur.update(applied)
        self._applied[did] = cur

    def __len__(self):
        return len(self._desired)


class FakeAudit:
    def __init__(self):
        self.events = []

    def append(self, event):
        self.events.append(event)
        return event


def make_require_operator(token):
    def require_operator(authorization=Header(default=None)):
        if not token:
            raise HTTPException(status_code=503, detail="unconfigured")
        presented = None
        if authorization:
            parts = authorization.split(None, 1)
            if len(parts) == 2 and parts[0].lower() == "bearer":
                presented = parts[1].strip()
        if presented != token:
            raise HTTPException(status_code=401, detail="bad operator token")

    return require_operator


def build_deps():
    store = FakeStore()
    audit = FakeAudit()
    deps = SimpleNamespace(
        store=store,
        audit=audit,
        signer=lambda payload, **kw: {"sig": "x", "manifest": {}, "signed_by": "k"},
        require_operator=make_require_operator(OP_TOKEN),
        require_agent_write=lambda: None,
        settings=SimpleNamespace(),
    )
    return deps


def make_app(deps):
    from routers import actions as actions_router

    app = FastAPI()
    actions_router.register(app, deps)
    return app


# --------------------------------------------------------------------------- #
# router tests                                                                 #
# --------------------------------------------------------------------------- #


def test_router():
    print("ROUTER tests:")
    deps = build_deps()
    app = make_app(deps)
    client = TestClient(app)
    DID = "dep-1"
    auth = {"Authorization": f"Bearer {OP_TOKEN}"}

    # enqueue without operator token -> 401
    r = client.post(f"/api/v1/deployments/{DID}/actions", json={"type": "flush-dlq"})
    check("POST without operator token -> 401", r.status_code == 401)

    # enqueue with bad operator token -> 401
    r = client.post(
        f"/api/v1/deployments/{DID}/actions",
        json={"type": "flush-dlq"},
        headers={"Authorization": "Bearer wrong"},
    )
    check("POST with wrong operator token -> 401", r.status_code == 401)

    # off-allowlist type -> 400
    r = client.post(
        f"/api/v1/deployments/{DID}/actions",
        json={"type": "rm-rf", "params": {}},
        headers=auth,
    )
    check("POST off-allowlist type -> 400", r.status_code == 400)

    # valid enqueue -> 200, mints id+created_at, appends to pending_actions
    r = client.post(
        f"/api/v1/deployments/{DID}/actions",
        json={"type": "flush-dlq", "params": {"topic": "dlq.x"}},
        headers=auth,
    )
    check("POST valid action -> 200", r.status_code == 200)
    body = r.json()
    a1 = body["action"]
    check("response action has minted id", bool(a1.get("id")))
    check("response action has created_at", bool(a1.get("created_at")))
    check("response action type preserved", a1.get("type") == "flush-dlq")
    check("params preserved", a1.get("params") == {"topic": "dlq.x"})

    stored = deps.store.get_desired(DID)
    check("stored in pending_actions", len(stored.pending_actions) == 1)
    check("audit event appended (I5)", any(e["action"] == "actions.enqueue" for e in deps.audit.events))
    check(
        "audit metadata carries type+id",
        deps.audit.events[-1]["metadata"]["type"] == "flush-dlq"
        and deps.audit.events[-1]["metadata"]["action_id"] == a1["id"],
    )

    # second enqueue appends (does not clobber)
    r = client.post(
        f"/api/v1/deployments/{DID}/actions",
        json={"type": "re-pull-config"},
        headers=auth,
    )
    check("second POST -> 200", r.status_code == 200)
    a2 = r.json()["action"]
    stored = deps.store.get_desired(DID)
    check("both actions pending (append, no clobber)", len(stored.pending_actions) == 2)

    # GET lists both as pending
    r = client.get(f"/api/v1/deployments/{DID}/actions")
    check("GET -> 200", r.status_code == 200)
    g = r.json()
    check("GET lists 2 actions", len(g["actions"]) == 2)
    check("both pending (none acked)", set(g["pending"]) == {a1["id"], a2["id"]})
    check("acked empty", g["acked"] == [])
    check("GET exposes allowlist", "flush-dlq" in g["allowlist"])

    # simulate the agent acking a1 -> GET reflects acked status
    deps.store.record_applied(DID, {"acked_action_ids": [a1["id"]]})
    r = client.get(f"/api/v1/deployments/{DID}/actions")
    g = r.json()
    statuses = {a["id"]: a["status"] for a in g["actions"]}
    check("a1 now acked", statuses[a1["id"]] == "acked")
    check("a2 still pending", statuses[a2["id"]] == "pending")
    check("GET pending = [a2]", g["pending"] == [a2["id"]])
    check("GET acked = [a1]", g["acked"] == [a1["id"]])

    # drift: a2 still unacked
    desired = deps.store.get_desired(DID)
    drift = compute_drift(desired, deps.store.get_applied(DID))
    check("compute_drift shows a2 unacked", drift["actions"] == [a2["id"]])

    return a1["id"], a2["id"]


# --------------------------------------------------------------------------- #
# agent handler tests                                                          #
# --------------------------------------------------------------------------- #


def make_ctx(extra=None):
    import logging

    from reconcile.registry import ReconcileContext

    return ReconcileContext(
        deployment_id="dep-1",
        trust_root_path="/nonexistent/trust_root.json",
        config_dir="/tmp/cfg",
        logger=logging.getLogger("test.actions"),
        extra=extra or {},
    )


def test_handler():
    print("HANDLER tests:")
    from reconcile import actions as handler_mod

    # fresh desired with two allowlisted actions
    desired = DesiredState(
        deployment_id="dep-1",
        pending_actions=[
            {"id": "a1", "type": "flush-dlq", "params": {}, "created_at": "t"},
            {"id": "a2", "type": "re-pull-config", "params": {}, "created_at": "t"},
        ],
    )

    ctx = make_ctx()
    delta = handler_mod.apply(desired, ctx)
    check("acks both new actions", set(delta.get("acked_action_ids", [])) == {"a1", "a2"})
    check("re-pull-config set the flag", ctx.extra.get("repull_config_requested") is True)

    # idempotency: a1 already acked -> only a2 newly executed, both in result
    ctx2 = make_ctx(extra={"acked_action_ids": ["a1"]})
    delta2 = handler_mod.apply(desired, ctx2)
    check("idempotent ack contains both ids", set(delta2.get("acked_action_ids", [])) == {"a1", "a2"})

    # all already acked -> stable, no re-exec (flag NOT set this pass)
    ctx3 = make_ctx(extra={"acked_action_ids": ["a1", "a2"]})
    delta3 = handler_mod.apply(desired, ctx3)
    check("all acked -> returns stable set", set(delta3.get("acked_action_ids", [])) == {"a1", "a2"})
    check("no re-exec of re-pull-config when all acked", ctx3.extra.get("repull_config_requested") is None)

    # off-allowlist action in a tampered desired blob -> not acked
    tampered = DesiredState(
        deployment_id="dep-1",
        pending_actions=[{"id": "bad", "type": "exec-shell", "params": {}}],
    )
    ctx4 = make_ctx()
    delta4 = handler_mod.apply(tampered, ctx4)
    check("off-allowlist action NOT acked", "bad" not in delta4.get("acked_action_ids", []))

    # I3: a raising executor leaves that action pending (not acked)
    import reconcile.actions as ra

    orig = ra._EXECUTORS["force-reconcile"]

    def boom(action, c):
        raise RuntimeError("kaboom")

    ra._EXECUTORS["force-reconcile"] = boom
    try:
        d = DesiredState(
            deployment_id="dep-1",
            pending_actions=[{"id": "fr", "type": "force-reconcile", "params": {}}],
        )
        delta5 = handler_mod.apply(d, make_ctx())
        check("raising executor -> action left pending (I3)", "fr" not in delta5.get("acked_action_ids", []))
    finally:
        ra._EXECUTORS["force-reconcile"] = orig

    # empty desired -> empty delta
    delta6 = handler_mod.apply(DesiredState(deployment_id="dep-1"), make_ctx())
    check("no actions -> empty delta", delta6 == {})

    # end-to-end drift convergence: after handler acks, compute_drift clears
    desired_full = DesiredState(
        deployment_id="dep-1",
        pending_actions=[{"id": "a1", "type": "flush-dlq", "params": {}}],
    )
    applied = handler_mod.apply(desired_full, make_ctx())
    drift = compute_drift(desired_full, applied)
    check("drift cleared after agent acks the action", drift["actions"] == [])


def main():
    a1, a2 = test_router()
    test_handler()
    print()
    print(f"TOTAL: {len(PASSED)} passed, {len(FAILED)} failed")
    if FAILED:
        for f in FAILED:
            print(f"  FAILED: {f}")
        sys.exit(1)
    print("ALL GREEN")


if __name__ == "__main__":
    main()
