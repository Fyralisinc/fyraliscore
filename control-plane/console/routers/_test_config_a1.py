#!/usr/bin/env python3
"""Isolation test for the A1 REMOTE CONFIG PUSH feature.

Self-contained: does NOT import the full console app or run the full agent (other
agents may be mid-write). It mounts ONLY ``console/routers/config.register`` on a
bare FastAPI app with stub deps (in-memory store, no-op audit, a REAL signer wired
to the actual trust root so the I6 round-trip is genuinely proven), and exercises
the agent handler ``agent/reconcile/config.apply`` directly with a fake ctx.

Run:
  /home/prajwal-adhikari/Desktop/v2/fyraliscore/.venv/bin/python \
    console/routers/_test_config_a1.py
from control-plane/ .
"""

from __future__ import annotations

import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, Optional

HERE = Path(__file__).resolve().parent
CONSOLE_DIR = HERE.parent
ROOT = CONSOLE_DIR.parent
SIGNING_DIR = ROOT / "signing"
AGENT_DIR = ROOT / "agent"
for p in (str(ROOT), str(CONSOLE_DIR), str(SIGNING_DIR)):
    if p not in sys.path:
        sys.path.insert(0, p)

from fastapi import FastAPI, Header, HTTPException
from fastapi.testclient import TestClient

from lib.desired_state import DesiredState
from deps import build_signer  # the REAL signer wrapper
import config as config_router  # console/routers/config.py


TEST_OP_TOKEN = "test-operator-token"
PASS = "\033[32mPASS\033[0m"
FAIL = "\033[31mFAIL\033[0m"
_results: list[tuple[bool, str]] = []


def check(cond: bool, label: str) -> None:
    _results.append((bool(cond), label))
    print(f"  {PASS if cond else FAIL}  {label}")


# --------------------------------------------------------------------------- #
# stub deps                                                                     #
# --------------------------------------------------------------------------- #


class FakeStore:
    def __init__(self) -> None:
        self._desired: Dict[str, DesiredState] = {}
        self._applied: Dict[str, dict] = {}

    def put_desired(self, did: str, desired: DesiredState) -> DesiredState:
        stored = desired.model_copy(update={"deployment_id": did})
        self._desired[did] = stored
        return stored

    def get_desired(self, did: str) -> Optional[DesiredState]:
        return self._desired.get(did)

    def record_applied(self, did: str, applied: dict) -> None:
        cur = dict(self._applied.get(did, {}))
        cur.update(applied or {})
        self._applied[did] = cur

    def get_applied(self, did: str) -> dict:
        return dict(self._applied.get(did, {}))


class FakeAudit:
    def __init__(self) -> None:
        self.events: list[dict] = []

    def append(self, event: dict):
        self.events.append(event)
        return event


def _require_operator(authorization: Optional[str] = Header(default=None)) -> None:
    if not authorization or authorization != f"Bearer {TEST_OP_TOKEN}":
        raise HTTPException(status_code=401, detail="bad operator token")


@dataclass
class StubDeps:
    store: Any
    signer: Callable[..., Dict[str, Any]]
    audit: Any
    require_operator: Callable[..., None]
    require_agent_write: Callable[..., None] = lambda: None
    settings: Any = None


def build_app():
    store = FakeStore()
    audit = FakeAudit()
    signer = build_signer(
        trust_root_path=str(SIGNING_DIR / "trust_root.json"),
        keys_dir=str(SIGNING_DIR / "keys"),
    )
    deps = StubDeps(store=store, signer=signer, audit=audit, require_operator=_require_operator)
    app = FastAPI()
    config_router.register(app, deps)
    return app, store, audit


# --------------------------------------------------------------------------- #
# router tests                                                                  #
# --------------------------------------------------------------------------- #


def test_router():
    print("ROUTER (console/routers/config.py):")
    app, store, audit = build_app()
    client = TestClient(app)
    did = "dep-001"
    url = f"/api/v1/deployments/{did}/desired-config"
    auth = {"Authorization": f"Bearer {TEST_OP_TOKEN}"}
    good = {"telemetry_tier": "T2", "interval_s": 30, "sampling": 0.5,
            "feature_flags": {"x": True}, "reason": "initial"}

    # auth
    check(client.put(url, json=good).status_code == 401, "PUT without operator token -> 401")
    check(client.put(url, json=good, headers={"Authorization": "Bearer wrong"}).status_code == 401,
          "PUT with wrong operator token -> 401")

    # validation
    for bad, why in [
        ({**good, "telemetry_tier": "T9"}, "bad tier"),
        ({**good, "interval_s": 0}, "interval_s<=0"),
        ({**good, "interval_s": "x"}, "interval_s not int"),
        ({**good, "sampling": 1.5}, "sampling out of range"),
        ({**good, "feature_flags": {"x": "yes"}}, "flag not bool"),
        ({**good, "feature_flags": []}, "flags not object"),
    ]:
        r = client.put(url, json=bad, headers=auth)
        check(r.status_code == 422, f"PUT invalid ({why}) -> 422")

    # happy path push v1
    r = client.put(url, json=good, headers=auth)
    check(r.status_code == 200, "PUT valid config -> 200")
    body = r.json()
    check(body["desired_config_version"] == 1, "first push -> version 1")
    check(body["signed_by"] == "cp-signing-2026-06", "push reports signed_by active key")

    # stored + signed (I6 envelope present)
    stored = store.get_desired(did)
    check(stored is not None and stored.desired_config == {
        "telemetry_tier": "T2", "interval_s": 30, "sampling": 0.5, "feature_flags": {"x": True}},
        "stored desired_config matches (validated/normalized)")
    sig = stored.desired_config_sig or {}
    check(bool(sig.get("sig")) and isinstance(sig.get("manifest"), dict) and sig.get("signed_by"),
          "stored desired_config_sig has {sig, manifest, signed_by} (I6)")

    # audited (I5)
    check(len(audit.events) == 1 and audit.events[0]["action"] == "config.push"
          and audit.events[0]["target"] == did,
          "operator write audited as config.push (I5)")

    # version monotonic bump
    r2 = client.put(url, json={**good, "interval_s": 60}, headers=auth)
    check(r2.json()["desired_config_version"] == 2, "second push -> version 2 (monotonic)")

    # GET desired + drift (no applied yet -> config drift True)
    g = client.get(url)
    check(g.status_code == 200, "GET desired-config -> 200")
    gb = g.json()
    check(gb["desired_config_version"] == 2 and gb["drift"]["config"] is True,
          "GET shows v2 + config drift True (agent behind)")

    # record applied -> drift closes
    store.record_applied(did, {"applied_config_version": 2})
    gb2 = client.get(url).json()
    check(gb2["drift"]["config"] is False, "after applied v2 reported, drift closes")

    # GET unknown -> 404
    check(client.get("/api/v1/deployments/nope/desired-config").status_code == 404,
          "GET desired-config for unknown deployment -> 404")

    # HTML form renders
    f = client.get(f"/deployments/{did}/config")
    check(f.status_code == 200 and "Remote config" in f.text and did in f.text,
          "operator HTML form renders for a deployment")

    return stored  # carry the signed desired into the agent test


# --------------------------------------------------------------------------- #
# agent handler tests                                                           #
# --------------------------------------------------------------------------- #


def test_agent_handler(signed_desired: DesiredState):
    print("AGENT HANDLER (agent/reconcile/config.py):")
    # Import the handler the same way the agent would (with the agent dir on path).
    if str(AGENT_DIR) not in sys.path:
        sys.path.insert(0, str(AGENT_DIR))
    import importlib
    recon_config = importlib.import_module("reconcile.config")
    from reconcile.registry import ReconcileContext

    import logging
    logging.basicConfig(level=logging.CRITICAL)  # quiet expected reject logs

    trust_root = str(SIGNING_DIR / "trust_root.json")
    cfg_dir = Path(tempfile.mkdtemp(prefix="a1-cfgdir-"))

    def make_ctx(applied_facets: Optional[dict] = None) -> ReconcileContext:
        extra: Dict[str, Any] = {}
        if applied_facets is not None:
            extra["applied_facets"] = applied_facets
        return ReconcileContext(
            deployment_id=signed_desired.deployment_id,
            trust_root_path=trust_root,
            config_dir=str(cfg_dir),
            logger=logging.getLogger("test.reconcile"),
            extra=extra,
        )

    # 1. valid signed config -> verified + applied, reports version
    delta = recon_config.apply(signed_desired, make_ctx())
    check(delta.get("applied_config_version") == signed_desired.desired_config_version,
          "valid signed config VERIFIED + applied, reports applied_config_version (I6)")
    applied_file = cfg_dir / recon_config.REMOTE_CONFIG_NAME
    check(applied_file.is_file(), "verified config written to agent config dir")

    # 2. already-applied (extra carries applied_facets at the same version) -> no-op
    delta2 = recon_config.apply(
        signed_desired, make_ctx({"applied_config_version": signed_desired.desired_config_version})
    )
    check(delta2 == {}, "already at desired version -> no-op (no re-apply)")

    # 3. no desired_config -> no-op
    check(recon_config.apply(DesiredState(deployment_id="d"), make_ctx()) == {},
          "no desired_config -> no-op")

    # 4. config present but NO signature -> REJECT (I6), nothing applied
    unsigned = signed_desired.model_copy(update={
        "desired_config_version": signed_desired.desired_config_version + 1,
        "desired_config_sig": None,
    })
    check(recon_config.apply(unsigned, make_ctx()) == {},
          "config with NO signature -> REJECTED, empty delta (I6)")

    # 5. TAMPERED config (sig no longer matches mutated config body) -> REJECT (I6)
    tampered = signed_desired.model_copy(update={
        "desired_config_version": signed_desired.desired_config_version + 1,
        "desired_config": {**signed_desired.desired_config, "interval_s": 999999},
    })
    check(recon_config.apply(tampered, make_ctx()) == {},
          "TAMPERED config (body changed after signing) -> REJECTED (I6)")

    # 6. RELABELED manifest (artifact swapped to 'release') -> REJECT (I6)
    bad_sig = dict(signed_desired.desired_config_sig)
    bad_manifest = dict(bad_sig["manifest"])
    bad_manifest["artifact"] = "release"
    bad_sig["manifest"] = bad_manifest
    relabeled = signed_desired.model_copy(update={
        "desired_config_version": signed_desired.desired_config_version + 1,
        "desired_config_sig": bad_sig,
    })
    check(recon_config.apply(relabeled, make_ctx()) == {},
          "RELABELED manifest (artifact->release) -> REJECTED (I6)")

    # 7. handler self-registered under name 'config'
    from reconcile.registry import list_handlers
    check("config" in list_handlers(), "handler self-registered as 'config' on import")

    # 8. resilient: a raising handler can't crash dispatch — exercise apply never raises
    #    on a malformed sig envelope (dict missing manifest).
    weird = signed_desired.model_copy(update={
        "desired_config_version": signed_desired.desired_config_version + 1,
        "desired_config_sig": {"sig": "x"},  # no manifest
    })
    try:
        out = recon_config.apply(weird, make_ctx())
        check(out == {}, "malformed sig envelope (no manifest) -> no-op, never raises (I3)")
    except Exception as exc:
        check(False, f"apply raised on malformed sig envelope: {exc}")


def main() -> int:
    print("=" * 70)
    print("A1 REMOTE CONFIG PUSH — isolation test")
    print("=" * 70)
    signed = test_router()
    test_agent_handler(signed)
    print("-" * 70)
    n_pass = sum(1 for ok, _ in _results if ok)
    n_total = len(_results)
    print(f"{n_pass}/{n_total} checks passed")
    return 0 if n_pass == n_total else 1


if __name__ == "__main__":
    raise SystemExit(main())
