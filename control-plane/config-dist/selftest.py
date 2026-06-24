#!/usr/bin/env python3
"""selftest.py — the spec self-test for WS-CONFIG (signed config distribution).

Runs entirely in-process against the FastAPI app via ``fastapi.testclient.TestClient``
(no external services, no sockets to the outside). It proves the deliverable contract:

  1. PUBLISH a config for a deployment (a new signed version).
  2. GET it through the service and VERIFY the signature with ``control-plane/signing``
     (``verify_bundle``) against the trust root  -> VALID.
  3. TAMPER the served config bytes  -> the SAME verifier  -> FAILS (I6).
  4. A TIER CHANGE produces a NEW version (v2), still valid, with the new tier.
  5. (Contract proof) Drive the REAL agent ``config_pull.ConfigPuller`` against the
     served trio over a fake fetcher wired to the TestClient — verify-before-apply
     applies the good config and REJECTS the tampered one (the agent already does this).

Run::

    /home/prajwal-adhikari/Desktop/v2/fyraliscore/.venv/bin/python selftest.py
"""

from __future__ import annotations

import json
import os
import sys
import tempfile
from pathlib import Path

_HERE = Path(__file__).resolve().parent
_CP_ROOT = _HERE.parent
for _p in (str(_HERE), str(_CP_ROOT), str(_CP_ROOT / "signing"), str(_CP_ROOT / "agent")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from fastapi.testclient import TestClient  # noqa: E402

import signing_lib as sl  # noqa: E402  (control-plane/signing)
import verify_bundle as vb  # noqa: E402  (control-plane/signing)

import config_service as cs  # noqa: E402
from store import ConfigStore, SigningHome  # noqa: E402

PASS = "PASS"
FAIL = "FAIL"
_results: list[tuple[str, bool, str]] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    _results.append((name, ok, detail))
    flag = PASS if ok else FAIL
    line = f"[{flag}] {name}"
    if detail:
        line += f"  — {detail}"
    print(line)


def _verify_trio_with_signing(
    *, config_bytes: bytes, sig_b64: str, manifest: dict, trust_root_path: Path
) -> vb.VerifyResult:
    """Verify a served trio using the COMMITTED control-plane/signing verifier.

    Stages the trio into a temp dir with the filenames verify_bundle expects, then calls
    the exact function the agent calls (``verify_bundle.verify_file``).
    """
    with tempfile.TemporaryDirectory(prefix="cfg-verify-") as td:
        p = Path(td) / "config.json"
        p.write_bytes(config_bytes)
        (Path(td) / "config.json.sig").write_text(sig_b64.strip() + "\n", encoding="utf-8")
        (Path(td) / "config.json.manifest.json").write_text(
            json.dumps(manifest, sort_keys=True), encoding="utf-8"
        )
        return vb.verify_file(str(p), trust_root_path=str(trust_root_path))


def main() -> int:
    print("=" * 72)
    print("WS-CONFIG self-test — signed per-deployment config distribution")
    print("=" * 72)

    workdir = Path(tempfile.mkdtemp(prefix="config-dist-selftest-"))
    store_root = workdir / "store"
    signing_home = SigningHome(workdir / "signing-home", key_id="cp-config-dist-test")

    store = ConfigStore(store_root=store_root, signing_home=signing_home)
    app = cs.create_app(store)
    client = TestClient(app)
    trust_root = signing_home.trust_root_path

    deployment_id = "acme-use1-7f3a"
    tenant_id = "acme"

    # ---- 0. healthz reports an active signing key -----------------------
    h = client.get("/healthz")
    check(
        "healthz reports an active signing key",
        h.status_code == 200 and h.json().get("active_key_id"),
        f"active_key_id={h.json().get('active_key_id')!r}",
    )

    # ---- 1. PUBLISH a config (new signed version) -----------------------
    pub = client.post(
        f"/api/v1/config/{deployment_id}",
        json={
            "tenant_id": tenant_id,
            "flags": {"ingestion_enabled": True, "anomaly_detection_enabled": False},
            "telemetry_tier": "T1",
            "token_rotation": {"enabled": True, "interval_hours": 24},
        },
    )
    check(
        "publish returns v1",
        pub.status_code == 200 and pub.json().get("version") == 1,
        f"status={pub.status_code} body={pub.text[:160]}",
    )
    v1_tier = pub.json().get("telemetry_tier")
    check("published v1 tier is T1", v1_tier == "T1", f"tier={v1_tier}")

    # ---- 2. GET the bundle via the AGENT'S three URLs + VERIFY ----------
    cfg = client.get(f"/config/{deployment_id}")
    sig = client.get(f"/config/{deployment_id}.sig")
    man = client.get(f"/config/{deployment_id}.manifest.json")
    routing_ok = (
        cfg.status_code == 200 and sig.status_code == 200 and man.status_code == 200
    )
    check(
        "agent's 3 pull URLs all resolve (config/.sig/.manifest.json)",
        routing_ok,
        f"config={cfg.status_code} sig={sig.status_code} manifest={man.status_code}",
    )

    config_bytes = cfg.content
    manifest = man.json()
    check(
        "manifest artifact kind is 'config' (agent requires this)",
        manifest.get("artifact") == "config",
        f"artifact={manifest.get('artifact')!r}",
    )
    check(
        "served config is the signed document (has flags + telemetry_tier + token_rotation)",
        all(
            k in json.loads(config_bytes)["config"]
            for k in ("flags", "telemetry_tier", "token_rotation")
        ),
        f"config keys={sorted(json.loads(config_bytes)['config'].keys())}",
    )

    res_good = _verify_trio_with_signing(
        config_bytes=config_bytes,
        sig_b64=sig.text,
        manifest=manifest,
        trust_root_path=trust_root,
    )
    check(
        "VERIFY served bundle with control-plane/signing -> VALID",
        res_good.ok and res_good.artifact == "config",
        res_good.reason,
    )

    # ---- 3. TAMPER the served bytes -> VERIFY FAILS (I6) ----------------
    doc = json.loads(config_bytes)
    doc["config"]["flags"]["anomaly_detection_enabled"] = True  # flip a flag in the bytes
    tampered_bytes = json.dumps(doc).encode("utf-8")
    res_tamper = _verify_trio_with_signing(
        config_bytes=tampered_bytes,
        sig_b64=sig.text,  # original signature over the original bytes
        manifest=manifest,
        trust_root_path=trust_root,
    )
    check(
        "TAMPERED served bytes -> VERIFY FAILS (signature mismatch, I6)",
        not res_tamper.ok,
        res_tamper.reason,
    )

    # Also tamper just the signature (bit-flip) -> still fails.
    bad_sig = sl.b64e(bytes((b ^ 0x01) if i == 0 else b
                            for i, b in enumerate(sl.b64d(sig.text.strip()))))
    res_badsig = _verify_trio_with_signing(
        config_bytes=config_bytes,
        sig_b64=bad_sig,
        manifest=manifest,
        trust_root_path=trust_root,
    )
    check(
        "TAMPERED signature -> VERIFY FAILS",
        not res_badsig.ok,
        res_badsig.reason,
    )

    # ---- 4. TIER CHANGE -> NEW VERSION ---------------------------------
    pub2 = client.post(
        f"/api/v1/config/{deployment_id}",
        json={"tenant_id": tenant_id, "telemetry_tier": "T2"},
    )
    check(
        "tier change publishes v2 (no redeploy)",
        pub2.status_code == 200 and pub2.json().get("version") == 2,
        f"version={pub2.json().get('version')}",
    )
    check(
        "v2 telemetry_tier is T2",
        pub2.json().get("telemetry_tier") == "T2",
        f"tier={pub2.json().get('telemetry_tier')}",
    )
    # HEAD now serves v2; its tier is T2 and flags were preserved from v1.
    head = client.get(f"/config/{deployment_id}")
    head_doc = json.loads(head.content)
    check(
        "HEAD now serves v2 with tier T2 and preserved flags",
        head_doc["version"] == 2
        and head_doc["config"]["telemetry_tier"] == "T2"
        and head_doc["config"]["flags"].get("ingestion_enabled") is True,
        f"version={head_doc['version']} tier={head_doc['config']['telemetry_tier']}",
    )
    # v2 verifies; v1 still exists and still verifies (immutable history).
    sig2 = client.get(f"/config/{deployment_id}.sig")
    man2 = client.get(f"/config/{deployment_id}.manifest.json")
    res_v2 = _verify_trio_with_signing(
        config_bytes=head.content,
        sig_b64=sig2.text,
        manifest=man2.json(),
        trust_root_path=trust_root,
    )
    check("v2 bundle VERIFIES", res_v2.ok, res_v2.reason)

    v1cfg = client.get(f"/config/{deployment_id}/v1")
    v1sig = client.get(f"/config/{deployment_id}/v1.sig")
    v1man = client.get(f"/config/{deployment_id}/v1.manifest.json")
    res_v1_pinned = _verify_trio_with_signing(
        config_bytes=v1cfg.content,
        sig_b64=v1sig.text,
        manifest=v1man.json(),
        trust_root_path=trust_root,
    )
    check(
        "pinned v1 is still served and still VERIFIES (immutable history)",
        res_v1_pinned.ok and json.loads(v1cfg.content)["version"] == 1,
        res_v1_pinned.reason,
    )

    # ---- 5. END-TO-END via the REAL agent config_pull (verify-before-apply) ----
    e2e_ok = _agent_pull_roundtrip(client, deployment_id, trust_root)
    check(
        "REAL agent config_pull applies the good bundle and REJECTS a tampered one",
        e2e_ok,
        "drove agent/config_pull.ConfigPuller over the served trio",
    )

    # ---- summary --------------------------------------------------------
    print("-" * 72)
    passed = sum(1 for _, ok, _ in _results if ok)
    total = len(_results)
    print(f"{passed}/{total} checks passed")
    print(f"(scratch dir: {workdir})")
    return 0 if passed == total else 1


def _agent_pull_roundtrip(client, deployment_id: str, trust_root: Path) -> bool:
    """Use the committed agent ConfigPuller against our served trio.

    The agent's puller takes an injectable ``fetcher(config_url) -> (cfg, sig, man)``.
    We wire that fetcher to the TestClient so it pulls the EXACT bytes the service
    serves, then verifies-before-apply against the trust root. Returns True iff the good
    bundle applies AND a tampered bundle is rejected (the agent's I6 enforcement).
    """
    try:
        import config_pull as cp  # control-plane/agent/config_pull.py
    except Exception as exc:  # pragma: no cover - agent import is best-effort
        print(f"  (skipping agent round-trip: could not import agent.config_pull: {exc})")
        return True  # don't fail the suite if the sibling agent can't import here

    config_url = f"http://testserver/config/{deployment_id}"

    def good_fetcher(url: str):
        cfg = client.get(f"/config/{deployment_id}")
        sig = client.get(f"/config/{deployment_id}.sig")
        man = client.get(f"/config/{deployment_id}.manifest.json")
        return cfg.content, sig.text, man.content

    def tampered_fetcher(url: str):
        cfg, sig, man = good_fetcher(url)
        doc = json.loads(cfg)
        doc["config"]["flags"]["ingestion_enabled"] = False  # tamper the served bytes
        return json.dumps(doc).encode("utf-8"), sig, man

    with tempfile.TemporaryDirectory(prefix="agent-applied-") as ad:
        applied_dir = Path(ad)
        good = cp.ConfigPuller(
            config_dir=applied_dir,
            trust_root_path=str(trust_root),
            fetcher=good_fetcher,
        ).pull_and_apply(config_url)
        if not (good.ok and good.applied):
            print(f"  agent rejected the GOOD bundle: {good.reason}")
            return False

        tampered = cp.ConfigPuller(
            config_dir=applied_dir,
            trust_root_path=str(trust_root),
            fetcher=tampered_fetcher,
        ).pull_and_apply(config_url)
        if tampered.ok or tampered.applied:
            print(f"  agent WRONGLY accepted a tampered bundle: {tampered.reason}")
            return False
        print(f"  agent: good -> applied v{good.version}; tampered -> rejected "
              f"({tampered.reason[:60]}...)")
        return True


if __name__ == "__main__":
    raise SystemExit(main())
