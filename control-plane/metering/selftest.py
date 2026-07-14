#!/usr/bin/env python3
"""selftest.py — end-to-end proof of WS-METER, against a MOCK Mimir, via the REAL signing lib.

Scenarios (each must hold; any failure exits non-zero):

  1. COMPUTE  — query a mock Mimir (httpx.MockTransport serving canned T1 counters for
                tenant ``acme``) and assemble a usage rollup with the right per-source
                obs, think_runs, and cost_usd.
  2. SIGN     — sign the rollup via ``control-plane/signing`` (ed25519, detached sig +
                manifest) and VERIFY it -> VALID (FR-F2 tamper-evidence).
  3. TAMPER   — flip a usage number in the signed rollup.json -> verify -> INVALID.
  4. EXPORT   — JSON export round-trips back to identical usage numbers; CSV export
                carries every source + a TOTAL row with a signature receipt.
  5. GUARDS   — X-Scope-OrgID is sent per tenant; a tenant with no activity rolls up to 0;
                export of an unverifiable bundle is REFUSED (fail-closed).

The mock Mimir is an ``httpx.MockTransport`` so the REAL ``MimirClient`` request building
+ Prometheus-vector parsing is exercised (no network, no running Mimir). Signing/verify is
the real ``signing/sign_bundle`` + ``signing/verify_bundle`` against a throwaway trust root
under a temp dir — no crypto is faked and the repo's signing state is untouched.

Run::  python selftest.py        (exit 0 = all green)
"""

from __future__ import annotations

import json
import os
import shutil
import sys
import tempfile
import urllib.parse

HERE = os.path.dirname(os.path.abspath(__file__))
SIGNING_DIR = os.path.normpath(os.path.join(HERE, "..", "signing"))
for _p in (HERE, SIGNING_DIR):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import httpx  # noqa: E402

import signing_lib as sl  # noqa: E402
import sign_bundle as sb  # noqa: E402
import verify_bundle as vb  # noqa: E402

import mimir_client as mc  # noqa: E402
import rollup as ru  # noqa: E402
import export as ex  # noqa: E402

PASS = "PASS"
FAIL = "FAIL"
_results: list[tuple[str, bool, str]] = []


def check(name: str, cond: bool, detail: str = "") -> None:
    _results.append((name, bool(cond), detail))
    mark = PASS if cond else FAIL
    line = f"[{mark}] {name}"
    if detail:
        line += f"  — {detail}"
    print(line)


# --------------------------------------------------------------------------- #
# Mock Mimir: a canned per-tenant Tier-1 metric store.                         #
# --------------------------------------------------------------------------- #

# Per-tenant canned usage for the period. The mock answers `increase(<counter>[..])`
# by matching the counter name in the PromQL and returning the right vector. We also
# ASSERT the X-Scope-OrgID header equals the tenant the test asked for.
_CANNED = {
    "acme": {
        ru.METRIC_OBS_WRITES: [  # obs-per-source breakdown
            ({"source": "github"}, 1234.0),
            ({"source": "slack"}, 88.0),
            ({"source": "jira"}, 42.0),
        ],
        ru.METRIC_THINK_RUNS: [({}, 57.0)],
        ru.METRIC_THINK_COST_USD: [({}, 3.141592)],
    },
    # globex onboarded but had zero activity this period (every series empty).
    "globex": {},
}

# Record which org ids the mock saw, so the test can prove per-tenant scoping.
_SEEN_ORGS: list[str] = []


def _mock_handler(request: httpx.Request) -> httpx.Response:
    org = request.headers.get(mc.ORG_HEADER)
    _SEEN_ORGS.append(org or "<none>")
    if org is None:
        # Mimir rejects a query with no X-Scope-OrgID (multitenancy hard-on).
        return httpx.Response(401, json={"status": "error", "error": "no org id"})

    qs = urllib.parse.parse_qs(urllib.parse.urlparse(str(request.url)).query)
    promql = (qs.get("query") or [""])[0]

    tenant_metrics = _CANNED.get(org, {})
    # Figure out which counter this query is about by substring match.
    for counter, series in (
        (ru.METRIC_OBS_WRITES, tenant_metrics.get(ru.METRIC_OBS_WRITES)),
        (ru.METRIC_THINK_RUNS, tenant_metrics.get(ru.METRIC_THINK_RUNS)),
        (ru.METRIC_THINK_COST_USD, tenant_metrics.get(ru.METRIC_THINK_COST_USD)),
    ):
        if counter in promql:
            result = []
            for labels, val in (series or []):
                metric = dict(labels)
                metric["__name__"] = counter
                result.append({"metric": metric, "value": [1_750_000_000, str(val)]})
            return httpx.Response(
                200,
                json={
                    "status": "success",
                    "data": {"resultType": "vector", "result": result},
                },
            )
    # Unknown query -> empty success vector (a real Mimir would too).
    return httpx.Response(
        200, json={"status": "success", "data": {"resultType": "vector", "result": []}}
    )


def _mint_throwaway_trust_root(signing_root: str, key_id: str = "cp-signing-meter-selftest") -> str:
    """Mint a real ed25519 key + trust root under ``signing_root`` (isolated from the repo)."""
    keys_dir = os.path.join(signing_root, "keys")
    os.makedirs(keys_dir, exist_ok=True)
    priv, pub = sl.generate_keypair()
    with open(os.path.join(keys_dir, f"{key_id}.private.pem"), "wb") as fh:
        fh.write(sl.private_key_to_pem(priv))
    ring = sl.Keyring()
    ring.add_key(key_id, public=pub, private=priv, make_active=True)
    trust_root_path = os.path.join(signing_root, "trust_root.json")
    with open(trust_root_path, "w", encoding="utf-8") as fh:
        json.dump(ring.to_trust_root(), fh, indent=2, sort_keys=True)
    return trust_root_path


def main() -> int:
    tmp = tempfile.mkdtemp(prefix="ws-meter-selftest-")
    print(f"# WS-METER selftest (workdir {tmp})\n")

    # Point the real signing module at a throwaway trust root so we never touch repo state.
    signing_root = os.path.join(tmp, "signing")
    trust_root_path = _mint_throwaway_trust_root(signing_root)
    sb.KEYS_DIR = os.path.join(signing_root, "keys")
    sb.TRUST_ROOT_PATH = trust_root_path
    vb.TRUST_ROOT_PATH = trust_root_path

    TENANT = "acme"
    period = ru.Period.month(2026, 6)

    # ----------------------------------------------------------------- #
    # 1. COMPUTE: query the mock Mimir and assemble the rollup.         #
    # ----------------------------------------------------------------- #
    transport = httpx.MockTransport(_mock_handler)
    with mc.MimirClient("http://mimir:9009", transport=transport) as client:
        rollup = ru.compute_rollup(client, tenant_id=TENANT, period=period)

    obs = rollup.obs_per_source
    check(
        "compute: per-source obs match the mock",
        obs.get("github") == 1234.0 and obs.get("slack") == 88.0 and obs.get("jira") == 42.0,
        str(obs),
    )
    check("compute: ingestion_volume = sum(obs_per_source)", rollup.ingestion_volume == 1364.0,
          str(rollup.ingestion_volume))
    check("compute: think_runs from think_runs_total", rollup.think_runs == 57.0, str(rollup.think_runs))
    check("compute: cost_usd from think_cost_recent_usd_total",
          abs(rollup.think_cost_usd - 3.141592) < 1e-9, str(rollup.think_cost_usd))
    # Per-tenant scoping: every query carried X-Scope-OrgID: acme.
    check("scope: every query sent X-Scope-OrgID == tenant",
          len(_SEEN_ORGS) >= 3 and set(_SEEN_ORGS) == {TENANT}, str(set(_SEEN_ORGS)))

    # Rollup document shape sanity.
    doc = rollup.to_dict()
    check(
        "rollup doc has {tenant_id, period, metrics, totals}",
        all(k in doc for k in ("tenant_id", "period", "metrics", "totals"))
        and doc["tenant_id"] == TENANT
        and doc["period"]["label"] == "2026-06"
        and doc["totals"]["observations"] == 1364.0,
        json.dumps(doc["totals"]),
    )

    # ----------------------------------------------------------------- #
    # 2. SIGN + VERIFY -> VALID (FR-F2 tamper-evidence).                #
    # ----------------------------------------------------------------- #
    good_dir = os.path.join(tmp, "rollup-acme-good")
    paths = ru.sign_rollup(rollup, out_dir=good_dir)
    bundle_ok = all(os.path.isfile(p) for p in (paths["rollup_path"], paths["sig_path"], paths["manifest_path"]))
    check("sign: signed trio written (rollup.json + .sig + .manifest.json)", bundle_ok)

    res = ru.verify_rollup(good_dir, trust_root_path=trust_root_path)
    check("verify(untampered signed rollup) -> VALID", res.ok, res.reason)

    # ----------------------------------------------------------------- #
    # 3. TAMPER a usage number -> verify -> INVALID.                    #
    # ----------------------------------------------------------------- #
    tamper_dir = os.path.join(tmp, "rollup-acme-tamper")
    shutil.copytree(good_dir, tamper_dir)
    tpath = os.path.join(tamper_dir, ru.ROLLUP_FILENAME)
    with open(tpath, "r", encoding="utf-8") as fh:
        tdoc = json.load(fh)
    # Under-report cost to pay less — the classic billing-tamper.
    tdoc["metrics"]["think_cost_usd"] = 0.01
    tdoc["totals"]["cost_usd"] = 0.01
    with open(tpath, "w", encoding="utf-8") as fh:
        json.dump(tdoc, fh, indent=2, sort_keys=True)

    res_t = ru.verify_rollup(tamper_dir, trust_root_path=trust_root_path)
    check("verify(tampered cost_usd) -> INVALID (signature fails)", (not res_t.ok), res_t.reason)

    # Also prove an obs tamper is caught.
    tdoc["metrics"]["obs_per_source"]["github"] = 1.0
    with open(tpath, "w", encoding="utf-8") as fh:
        json.dump(tdoc, fh, indent=2, sort_keys=True)
    res_t2 = ru.verify_rollup(tamper_dir, trust_root_path=trust_root_path)
    check("verify(tampered obs count) -> INVALID", (not res_t2.ok), res_t2.reason)

    # ----------------------------------------------------------------- #
    # 4. EXPORT round-trips (JSON) + CSV carries sources + receipt.     #
    # ----------------------------------------------------------------- #
    signed = ex.collect_signed_rollups([good_dir], trust_root_path=trust_root_path)
    check("export: collect_signed_rollups verified the bundle", signed[0].verified, signed[0].key_id)

    json_path = os.path.join(tmp, "billing.json")
    ex.export_json(signed, out_path=json_path)
    back = ex.read_json_export(json_path)
    rt_ok = (
        len(back) == 1
        and back[0].tenant_id == TENANT
        and back[0].obs_per_source == rollup.obs_per_source
        and back[0].think_runs == rollup.think_runs
        and abs(back[0].think_cost_usd - rollup.think_cost_usd) < 1e-9
        and back[0].ingestion_volume == rollup.ingestion_volume
    )
    check("export(JSON) round-trips to identical usage numbers", rt_ok,
          f"obs={back[0].obs_per_source if back else None}")

    receipts = ex.read_json_export_receipts(json_path)
    check("export(JSON) carries a signature receipt (key_id + sha256 + signature)",
          bool(receipts) and receipts[0].get("key_id") and receipts[0].get("sha256")
          and receipts[0].get("signature"),
          str({k: bool(v) for k, v in (receipts[0] if receipts else {}).items()}))

    csv_path = os.path.join(tmp, "billing.csv")
    ex.export_csv(signed, out_path=csv_path)
    rows = ex.read_csv_export(csv_path)
    csv_sources = {r["source"] for r in rows}
    total_row = next((r for r in rows if r["source"] == ex.TOTAL_SOURCE), None)
    csv_ok = (
        {"github", "slack", "jira", ex.TOTAL_SOURCE}.issubset(csv_sources)
        and total_row is not None
        and float(total_row["observations"]) == 1364.0
        and float(total_row["think_runs"]) == 57.0
        and total_row["key_id"] == signed[0].key_id
    )
    check("export(CSV) has per-source rows + a TOTAL row with the receipt", csv_ok,
          f"sources={sorted(csv_sources)}")

    # ----------------------------------------------------------------- #
    # 5a. GUARD: a zero-activity tenant rolls up to all-zeros (no error)#
    # ----------------------------------------------------------------- #
    with mc.MimirClient("http://mimir:9009", transport=transport) as client:
        z = ru.compute_rollup(client, tenant_id="globex", period=period)
    check("zero-activity tenant -> totals all 0 (valid, not an error)",
          z.ingestion_volume == 0.0 and z.think_runs == 0.0 and z.think_cost_usd == 0.0,
          json.dumps(z.to_dict()["totals"]))

    # ----------------------------------------------------------------- #
    # 5b. GUARD: export of a tampered/unverifiable bundle is REFUSED.   #
    # ----------------------------------------------------------------- #
    refused = False
    try:
        ex.collect_signed_rollups([tamper_dir], trust_root_path=trust_root_path)
    except ValueError:
        refused = True
    check("export REFUSES an unverifiable (tampered) bundle (fail-closed)", refused)

    # ----------------------------------------------------------------- #
    # 5c. GUARD: a rollup signed by an UNKNOWN key fails verify.        #
    # ----------------------------------------------------------------- #
    other_root = _mint_throwaway_trust_root(os.path.join(tmp, "signing-other"), key_id="cp-other")
    res_uk = ru.verify_rollup(good_dir, trust_root_path=other_root)
    check("verify(signed by key unknown to this trust root) -> INVALID", (not res_uk.ok), res_uk.reason)

    print()
    n_pass = sum(1 for _, ok, _ in _results if ok)
    n_total = len(_results)
    all_green = n_pass == n_total
    print(f"# {n_pass}/{n_total} checks passed — {'ALL GREEN' if all_green else 'FAILURES PRESENT'}")

    shutil.rmtree(tmp, ignore_errors=True)
    return 0 if all_green else 1


if __name__ == "__main__":
    raise SystemExit(main())
