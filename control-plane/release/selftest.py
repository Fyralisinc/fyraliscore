#!/usr/bin/env python3
"""selftest — end-to-end proof for WS-RELEASE (build+sign+verify, tamper-reject, rollout).

Run::

    /home/prajwal-adhikari/Desktop/v2/fyraliscore/.venv/bin/python control-plane/release/selftest.py

It exercises every deliverable against the REAL committed siblings (no mocks of the
crypto or the console):

  T1  BUILD + SIGN a release, then VERIFY it with ``control-plane/signing/verify_bundle``
      -> ACCEPT.
  T2  TAMPER the tarball bytes -> ``verify_bundle`` REJECTS (and the agent's
      ``config_pull`` enforcement point would refuse to apply).
  T3  PUBLISH the signed bundle into the registry; refuse to publish a tampered one.
  T4  ROLLOUT against the REAL console (``GET /api/v1/deployments`` via TestClient)
      with a HEALTHY canary -> canary goes green on the target -> FLEET PROMOTED.
  T5  ROLLOUT with an UNHEALTHY canary -> halt-on-drift -> FLEET NOT PROMOTED
      (and the canary is rolled back to its prior version).
  T6  ROLLBACK the whole fleet to a prior version.

Exits 0 iff every check passes; prints a PASS/FAIL line per check.
"""

from __future__ import annotations

import datetime as _dt
import shutil
import sys
import tempfile
from pathlib import Path

import _bootstrap  # noqa: F401  (side-effect: sys.path for lib + signing)

import build_release as br  # noqa: E402
import publish as pub  # noqa: E402
import rollout as ro  # noqa: E402
import verify_bundle as vb  # noqa: E402  (the agent's enforcement point)
from signing_ctx import SigningContext  # noqa: E402

from lib.deployment import DeploymentRecord, derive_health  # noqa: E402
from lib.primitives import utcnow  # noqa: E402


# --------------------------------------------------------------------------- #
# A fake fleet built on the REAL console app + the REAL C4 record/health math   #
# --------------------------------------------------------------------------- #


class FakeFleet:
    """A real console (FastAPI app + DeploymentStore) plus a build-outcome model.

    The controller talks to the genuine console over a ``TestClient``, so the health
    rollup it reads is derived by the real ``lib.deployment`` code. ``adopt`` models
    "the agent pulled the signed bundle for ``version``, applied it, and heartbeated":

      * a deployment whose build outcome for ``version`` is GOOD heartbeats *now*
        on that version  -> console derives GREEN.
      * a BAD build heartbeats with a stale heartbeat far in the past on that version
        -> console derives RED (drift the controller must catch).
    """

    def __init__(self, *, bad_versions: dict[str, set[str]] | None = None):
        # Import the real console app factory + store. The console package is a set
        # of flat modules (``app.py`` does ``from store import ...``), so put the
        # console dir on sys.path first — same convention the console uses itself.
        import importlib

        console_dir = str(_bootstrap.CONTROL_PLANE_ROOT / "console")
        if console_dir not in sys.path:
            sys.path.insert(0, console_dir)
        console_app = importlib.import_module("app")  # control-plane/console/app.py
        store_mod = importlib.import_module("store")
        # persist=False => pure in-memory registry (no disk reads/writes, isolated per fleet).
        self._store = store_mod.DeploymentStore(persist=False)
        self._app = console_app.create_app(self._store)
        from fastapi.testclient import TestClient

        self._client = TestClient(self._app)
        # deployment_id -> set of versions whose "build" is bad on that deployment.
        self._bad: dict[str, set[str]] = bad_versions or {}

    # -- seed deployments --------------------------------------------------- #

    def seed(self, *, count: int, version: str, region: str = "us-east-1") -> list[str]:
        ids: list[str] = []
        now = utcnow()
        exp = now + _dt.timedelta(days=365)
        for i in range(count):
            did = f"acme-{region[:4]}-{i:03d}"
            rec = DeploymentRecord.heartbeat(
                tenant_id="acme",
                deployment_id=did,
                version=version,
                region=region,
                license_expiry=exp,
                now=now,
            )
            r = self._client.post("/api/v1/heartbeat", json=rec.to_registry_dict())
            assert r.status_code == 200, r.text
            ids.append(did)
        return ids

    # -- the Promoter the controller calls ---------------------------------- #

    def adopt(self, deployment_id: str, version: str) -> None:
        """Promote (deployment_id -> version): heartbeat as if the agent applied it.

        GOOD build => fresh heartbeat (green). BAD build => heartbeat stamped far in
        the past => the console derives RED on read (the canary 'drift').
        """
        existing = self._client.get(f"/api/v1/deployments/{deployment_id}").json()
        now = utcnow()
        exp = now + _dt.timedelta(days=365)
        is_bad = version in self._bad.get(deployment_id, set())
        # A bad build reports a heartbeat well past the red threshold (>300s).
        hb = now - _dt.timedelta(seconds=10_000) if is_bad else now
        rec = DeploymentRecord.heartbeat(
            tenant_id=existing["tenant_id"],
            deployment_id=deployment_id,
            version=version,
            region=existing["region"],
            license_expiry=exp,
            last_heartbeat_ts=hb,
            now=now,
        )
        r = self._client.post("/api/v1/heartbeat", json=rec.to_registry_dict())
        assert r.status_code == 200, r.text

    # -- the ConsoleClient the controller reads ----------------------------- #

    def list_deployments(self) -> list[dict]:
        r = self._client.get("/api/v1/deployments")
        r.raise_for_status()
        return r.json()


# --------------------------------------------------------------------------- #
# Checks                                                                        #
# --------------------------------------------------------------------------- #

_PASS = "PASS"
_FAIL = "FAIL"
_results: list[tuple[str, bool, str]] = []


def check(name: str, cond: bool, detail: str = "") -> bool:
    _results.append((name, bool(cond), detail))
    tag = _PASS if cond else _FAIL
    print(f"[{tag}] {name}" + (f" — {detail}" if detail else ""))
    return bool(cond)


def main() -> int:
    work = Path(tempfile.mkdtemp(prefix="ws-release-selftest-"))
    try:
        # --- a throwaway signing context (does NOT touch committed signing/) ----
        signing_root = work / "signing"
        ctx = SigningContext.ephemeral(signing_root)

        # --- a tiny "data-plane source tree" to package -------------------------
        src = work / "dataplane"
        (src / "svc").mkdir(parents=True)
        (src / "svc" / "main.py").write_text("print('fyralis dataplane v1')\n", encoding="utf-8")
        (src / "VERSION").write_text("1.4.2\n", encoding="utf-8")
        # A would-be secret that must be EXCLUDED from the tarball.
        (src / "secret.private.pem").write_text("-----BEGIN PRIVATE KEY-----\n", encoding="utf-8")

        out = work / "dist"

        # === T1: build + sign + verify (accept) =================================
        rb = br.build_release(
            src=src, version="1.4.2", out_dir=out, signing_root=signing_root
        )
        res = vb.verify_file(
            rb.tarball_path, trust_root_path=str(ctx.trust_root_path)
        )
        check("T1.build_signed", Path(rb.signed.sig_path).is_file() and Path(rb.signed.manifest_path).is_file(),
              f"v{rb.version} key={rb.key_id} files={rb.file_count}")
        check("T1.verify_accept", res.ok, res.reason)
        check("T1.artifact_is_release", (res.artifact or "") == "release", f"artifact={res.artifact}")

        # determinism: rebuild yields the same sha256
        rb2 = br.build_release(
            src=src, version="1.4.2", out_dir=work / "dist2", signing_root=signing_root
        )
        check("T1.deterministic_tarball", rb.tarball_sha256 == rb2.tarball_sha256,
              f"{rb.tarball_sha256[:12]} == {rb2.tarball_sha256[:12]}")

        # secret excluded from the tarball
        import tarfile

        with tarfile.open(rb.tarball_path) as tf:
            names = tf.getnames()
        check("T1.secret_excluded", not any("secret.private.pem" in n for n in names),
              "no *.private.pem in tarball")

        # === T2: tamper -> reject ==============================================
        tampered = work / "tampered.tar.gz"
        shutil.copyfile(rb.tarball_path, tampered)
        shutil.copyfile(rb.signed.sig_path, str(tampered) + ".sig")
        shutil.copyfile(rb.signed.manifest_path, str(tampered) + ".manifest.json")
        with open(tampered, "ab") as fh:
            fh.write(b"\x00malicious-append\x00")  # flip the bytes
        tres = vb.verify_file(str(tampered), trust_root_path=str(ctx.trust_root_path))
        check("T2.tamper_rejected", not tres.ok, tres.reason)

        # unknown-key rejection: a DIFFERENT trust root must not verify this sig.
        other_root = work / "other-signing"
        SigningContext.ephemeral(other_root, key_id="cp-signing-other")
        ores = vb.verify_file(
            rb.tarball_path, trust_root_path=str(other_root / "trust_root.json")
        )
        check("T2.unknown_key_rejected", not ores.ok, ores.reason)

        # === T3: publish (verify-before-publish) ================================
        registry_root = work / "registry"
        reg = pub.ReleaseRegistry(registry_root, signing_root=signing_root)
        published = reg.publish(rb.tarball_path)
        check("T3.published", reg.latest() == "1.4.2" and "1.4.2" in reg.list_versions(),
              f"latest={reg.latest()} versions={reg.list_versions()}")
        # publishing the tampered bundle must be refused
        refused = False
        try:
            reg.publish(str(tampered), force=True)
        except ValueError:
            refused = True
        check("T3.refuse_publish_tampered", refused, "registry refuses unverified bundle")
        # the on-disk published bundle re-verifies (what an agent would pull)
        bp = reg.bundle_paths("1.4.2")
        pres = vb.verify_file(bp["tarball"], trust_root_path=str(ctx.trust_root_path))
        check("T3.published_bundle_verifies", pres.ok, pres.reason)

        # build + publish a SECOND version so we have something to roll out to.
        rb143 = br.build_release(
            src=src, version="1.4.3", out_dir=work / "dist143", signing_root=signing_root
        )
        reg.publish(rb143.tarball_path)

        # === T4: rollout, healthy canary -> fleet promoted ======================
        fleet_ok = FakeFleet()  # all builds good
        ids = fleet_ok.seed(count=4, version="1.4.2")
        ctrl_ok = ro.RolloutController(
            console=fleet_ok, promoter=fleet_ok.adopt, sleep=lambda s: None
        )
        r_ok = ctrl_ok.rollout(
            "1.4.3", canary_count=1, watch_seconds=5, poll_seconds=0.0
        )
        # every deployment ended up on 1.4.3 and green
        final = {d["deployment_id"]: d for d in fleet_ok.list_deployments()}
        all_on_target = all(final[i]["version"] == "1.4.3" for i in ids)
        all_green = all(final[i]["health"] == "green" for i in ids)
        check("T4.canary_promoted", r_ok.canary_promoted, f"canary={r_ok.plan.canary_ids}")
        check("T4.canary_healthy", r_ok.canary_healthy, str(r_ok.canary_health))
        check("T4.fleet_promoted", r_ok.fleet_promoted and r_ok.ok, r_ok.reason)
        check("T4.all_on_target_green", all_on_target and all_green,
              f"on_target={all_on_target} green={all_green}")

        # === T5: rollout, UNHEALTHY canary -> halt, fleet NOT promoted ==========
        # The canary is the lowest deployment_id among eligible (deterministic) — we
        # seed first, then mark THAT id's 1.4.3 build bad, so the test never hard-codes
        # the id shape.
        fleet_bad = FakeFleet()
        bad_ids = fleet_bad.seed(count=4, version="1.4.2")
        canary_id = sorted(bad_ids)[0]
        fleet_bad._bad[canary_id] = {"1.4.3"}  # this deployment's 1.4.3 build is bad
        assert canary_id in bad_ids
        ctrl_bad = ro.RolloutController(
            console=fleet_bad, promoter=fleet_bad.adopt, sleep=lambda s: None
        )
        r_bad = ctrl_bad.rollout(
            "1.4.3", canary_count=1, watch_seconds=2, poll_seconds=0.0
        )
        post = {d["deployment_id"]: d for d in fleet_bad.list_deployments()}
        # fleet (the non-canary deployments) must still be on the PRIOR version.
        fleet_remainder = [i for i in bad_ids if i != canary_id]
        fleet_untouched = all(post[i]["version"] == "1.4.2" for i in fleet_remainder)
        canary_rolled_back = post[canary_id]["version"] == "1.4.2"
        check("T5.halted", r_bad.halted and not r_bad.fleet_promoted, r_bad.reason)
        check("T5.canary_was_selected", canary_id in r_bad.plan.canary_ids,
              f"canary={r_bad.plan.canary_ids}")
        check("T5.fleet_not_promoted", fleet_untouched,
              f"remainder still on prior: {fleet_untouched}")
        check("T5.canary_rolled_back", r_bad.rolled_back and canary_rolled_back,
              f"{canary_id} -> {post[canary_id]['version']}")

        # === T6: rollback the whole (healthy) fleet to the prior version ========
        r_rb = ctrl_ok.rollback_all("1.4.2")
        after = {d["deployment_id"]: d for d in fleet_ok.list_deployments()}
        all_back = all(after[i]["version"] == "1.4.2" for i in ids)
        check("T6.rollback_all", r_rb.fleet_promoted and all_back, r_rb.reason)

        # also drive the registry latest pointer back (operator rollback path)
        reg.set_latest("1.4.2")
        check("T6.registry_latest_rolled_back", reg.latest() == "1.4.2", f"latest={reg.latest()}")

    finally:
        shutil.rmtree(work, ignore_errors=True)

    passed = sum(1 for _, ok, _ in _results if ok)
    total = len(_results)
    print(f"\n{'=' * 60}\nWS-RELEASE self-test: {passed}/{total} checks passed")
    failed = [n for n, ok, _ in _results if not ok]
    if failed:
        print("FAILED: " + ", ".join(failed))
        return 1
    print("ALL GREEN")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
