#!/usr/bin/env python3
"""Isolation self-test for the C3 audit viewer router.

Mounts ONLY ``audit_view.register`` on a bare FastAPI app (no console app.py, no
other routers) with a stub ``deps`` whose ``audit`` facade wraps a REAL
``AuditLog`` over a throwaway temp file. Exercises:

  1. empty trail            -> JSON ok/GENESIS, HTML "No audit events yet"
  2. populated intact chain -> events surfaced, verify ok, reason/target shown
  3. tampered chain         -> verify ok:false with bad_seq (tamper-evident, I5)
  4. no audit on deps        -> graceful empty/GENESIS, never 500

Run: control-plane/.venv python console/routers/_audit_view_selftest.py
(uses the fyraliscore .venv per the task).
"""

from __future__ import annotations

import json
import os
import sys
import tempfile
from types import SimpleNamespace

_HERE = os.path.dirname(os.path.abspath(__file__))
_CONSOLE = os.path.dirname(_HERE)
_ROOT = os.path.dirname(_CONSOLE)
_AUDIT_DIR = os.path.join(_ROOT, "audit")
_SIGNING_DIR = os.path.join(_ROOT, "signing")
for _p in (_HERE, _CONSOLE, _ROOT, _AUDIT_DIR, _SIGNING_DIR):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from fastapi import FastAPI  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402

import audit_log as al  # noqa: E402  (control-plane/audit)
import audit_view  # noqa: E402  (this dir)


class _StubAudit:
    """Mimics ConsoleAudit: lazy ``_ensure()`` returns a real AuditLog."""

    def __init__(self, log_path: str):
        self._log = al.open_log(log_path)  # no keyring => chain-only, unsigned

    def _ensure(self):
        return self._log

    def append(self, event):
        return self._log.append(
            actor=event.get("actor", "operator"),
            action=event.get("action", "x"),
            target=event.get("target", ""),
            metadata=event.get("metadata", {}),
        )


def _client(deps) -> TestClient:
    app = FastAPI()
    audit_view.register(app, deps)
    return TestClient(app)


def _ok(cond, msg):
    if not cond:
        raise AssertionError(msg)
    print(f"  PASS: {msg}")


def main() -> int:
    tmp = tempfile.mkdtemp(prefix="audit_view_selftest_")
    log_path = os.path.join(tmp, "audit.log.jsonl")

    # ---- (1) empty trail ----
    deps = SimpleNamespace(audit=_StubAudit(log_path))
    c = _client(deps)
    r = c.get("/api/v1/audit")
    _ok(r.status_code == 200, "empty: JSON 200")
    body = r.json()
    _ok(body["count"] == 0, "empty: 0 events")
    _ok(body["verify"]["ok"] is True, "empty: verify ok (GENESIS)")
    rh = c.get("/audit")
    _ok(rh.status_code == 200, "empty: HTML 200")
    _ok("No audit events yet" in rh.text, "empty: HTML shows GENESIS empty state")
    _ok("CHAIN INTACT" in rh.text, "empty: HTML shows intact banner")

    # ---- (2) populated, intact chain ----
    deps.audit.append(
        {
            "actor": "operator",
            "action": "desired.config.write",
            "target": "dep-123",
            "metadata": {"reason": "raise tier to T2", "deployment_id": "dep-123"},
        }
    )
    deps.audit.append(
        {
            "actor": "operator",
            "action": "license.suspend",
            "target": "dep-999",
            "metadata": {"reason": "nonpayment"},
        }
    )
    r = c.get("/api/v1/audit")
    body = r.json()
    _ok(body["count"] == 2, "populated: 2 events")
    _ok(body["verify"]["ok"] is True, "populated: chain verify ok")
    _ok(body["verify"]["count"] == 2, "populated: verify count 2")
    actions = {e["action"] for e in body["events"]}
    _ok("desired.config.write" in actions, "populated: config write event present")
    cfg = next(e for e in body["events"] if e["action"] == "desired.config.write")
    _ok(cfg["target_deployment"] == "dep-123", "populated: target deployment surfaced")
    _ok(cfg["reason"] == "raise tier to T2", "populated: reason surfaced")
    sus = next(e for e in body["events"] if e["action"] == "license.suspend")
    _ok(sus["target_deployment"] == "dep-999", "populated: target from entry.target")
    rh = c.get("/audit")
    _ok("dep-123" in rh.text and "raise tier to T2" in rh.text, "populated: HTML row rendered")
    _ok("CHAIN INTACT" in rh.text, "populated: HTML intact banner")

    # ---- (3) tampered chain (flip a field in entry 0 on disk) ----
    with open(log_path, "r", encoding="utf-8") as fh:
        lines = fh.readlines()
    rec0 = json.loads(lines[0])
    rec0["actor"] = "attacker"  # change a field WITHOUT fixing entry_hash
    lines[0] = json.dumps(rec0, sort_keys=True, separators=(",", ":")) + "\n"
    with open(log_path, "w", encoding="utf-8") as fh:
        fh.writelines(lines)
    # fresh deps so the AuditLog re-reads from disk
    deps2 = SimpleNamespace(audit=_StubAudit(log_path))
    c2 = _client(deps2)
    r = c2.get("/api/v1/audit")
    _ok(r.status_code == 200, "tampered: JSON still 200 (read never crashes)")
    body = r.json()
    _ok(body["verify"]["ok"] is False, "tampered: verify ok:false (I5 detected)")
    _ok(body["verify"]["bad_seq"] == 0, "tampered: bad_seq pinpoints entry 0")
    rh = c2.get("/audit")
    _ok("CHAIN BROKEN" in rh.text, "tampered: HTML shows broken banner")

    # ---- (4) deps with no audit at all ----
    deps3 = SimpleNamespace()
    c3 = _client(deps3)
    r = c3.get("/api/v1/audit")
    _ok(r.status_code == 200, "no-audit: JSON 200 (graceful)")
    _ok(r.json()["count"] == 0, "no-audit: 0 events")
    rh = c3.get("/audit")
    _ok(rh.status_code == 200 and "GENESIS" in rh.text, "no-audit: HTML GENESIS")

    print("\nALL AUDIT-VIEWER ISOLATION TESTS PASSED")
    return 0


if __name__ == "__main__":
    sys.exit(main())
