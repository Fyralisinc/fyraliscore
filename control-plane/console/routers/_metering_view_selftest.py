#!/usr/bin/env python3
"""Isolated self-test for the D1 metering view router (console/routers/metering_view.py).

Mounts ONLY ``metering_view.register(app, deps)`` on a bare FastAPI app with an
in-memory stub deps (fake fleet store + an httpx.MockTransport serving canned T1
counters), so it runs without the full console app and without a live Mimir / stack.

Proves:
  * GET /api/v1/metering returns per-tenant aggregate usage computed through the
    REAL MimirClient/rollup path against a canned Mimir (MockTransport).
  * per-tenant X-Scope-OrgID scoping (each tenant only sees its own canned series).
  * a tenant with no activity rolls up to 0 (valid, not an error).
  * fleet_totals = sum across tenants; aggregate-only (no PII, I1).
  * GET /metering renders HTML with the per-tenant table + the signed-export note.
  * mimir_configured:false path (no transport, no url) degrades to no-metrics rows
    without 500.

Run:  /home/prajwal-adhikari/Desktop/v2/fyraliscore/.venv/bin/python \
        console/routers/_metering_view_selftest.py
"""

from __future__ import annotations

import os
import sys
import urllib.parse

# Make the package importable as `metering_view` (its register signature is what we test),
# and make the metering engine importable the way the router does.
_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.normpath(os.path.join(_HERE, "..", ".."))  # control-plane/
for _p in (_HERE, os.path.join(_ROOT, "metering"), os.path.join(_ROOT, "signing")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import httpx  # noqa: E402
from fastapi import FastAPI  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402

import metering_view  # noqa: E402  (the router under test)
import mimir_client as mc  # noqa: E402
import rollup as ru  # noqa: E402

_PASS = 0
_FAIL = 0


def check(name: str, ok: bool, detail: str = "") -> None:
    global _PASS, _FAIL
    mark = "PASS" if ok else "FAIL"
    if ok:
        _PASS += 1
    else:
        _FAIL += 1
    line = f"[{mark}] {name}"
    if detail and not ok:
        line += f"  — {detail}"
    print(line)


# --------------------------------------------------------------------------- #
# Canned Mimir (per-tenant Tier-1 counters) via httpx.MockTransport.          #
# --------------------------------------------------------------------------- #

_CANNED = {
    "acme": {
        ru.METRIC_OBS_WRITES: [
            ({"source": "github"}, 1234.0),
            ({"source": "slack"}, 88.0),
        ],
        ru.METRIC_THINK_RUNS: [({}, 57.0)],
        ru.METRIC_THINK_COST_USD: [({}, 3.141592)],
    },
    "globex": {},  # onboarded, zero activity this period
}
_SEEN_ORGS: list[str] = []


def _mock_handler(request: httpx.Request) -> httpx.Response:
    org = request.headers.get(mc.ORG_HEADER)
    _SEEN_ORGS.append(org or "<none>")
    if org is None:
        return httpx.Response(401, json={"status": "error", "error": "no org id"})
    qs = urllib.parse.parse_qs(urllib.parse.urlparse(str(request.url)).query)
    promql = (qs.get("query") or [""])[0]
    tenant_metrics = _CANNED.get(org, {})
    for counter in (ru.METRIC_OBS_WRITES, ru.METRIC_THINK_RUNS, ru.METRIC_THINK_COST_USD):
        if counter in promql:
            result = []
            for labels, val in tenant_metrics.get(counter, []):
                metric = dict(labels)
                metric["__name__"] = counter
                result.append({"metric": metric, "value": [1_750_000_000, str(val)]})
            return httpx.Response(
                200, json={"status": "success", "data": {"resultType": "vector", "result": result}}
            )
    return httpx.Response(
        200, json={"status": "success", "data": {"resultType": "vector", "result": []}}
    )


# --------------------------------------------------------------------------- #
# Stub deps (fake fleet store + settings + injected transport).               #
# --------------------------------------------------------------------------- #


class _Rec:
    def __init__(self, tenant_id: str, deployment_id: str) -> None:
        self.tenant_id = tenant_id
        self.deployment_id = deployment_id


class _FakeStore:
    def __init__(self, records):
        self._records = records

    def list_records(self, *, now=None):
        return list(self._records)

    def __len__(self):
        return len(self._records)


class _Settings:
    def __init__(self, mimir_url: str = "") -> None:
        self.mimir_url = mimir_url


class _Deps:
    def __init__(self, *, store, settings, transport=None):
        self.store = store
        self.settings = settings
        self.metering_transport = transport
        # unused-by-this-router but present on the real ConsoleDeps:
        self.signer = None
        self.audit = None
        self.require_operator = lambda: None
        self.require_agent_write = lambda: None


def _app(deps) -> FastAPI:
    app = FastAPI()
    metering_view.register(app, deps)
    return app


def main() -> int:
    fleet = [
        _Rec("acme", "acme-use1-aa11"),
        _Rec("acme", "acme-euw1-bb22"),  # second deployment, same tenant
        _Rec("globex", "globex-use1-cc33"),
    ]
    store = _FakeStore(fleet)
    transport = httpx.MockTransport(_mock_handler)
    deps = _Deps(store=store, settings=_Settings("http://mimir:9009"), transport=transport)
    client = TestClient(_app(deps))

    # --- JSON view -------------------------------------------------------- #
    r = client.get("/api/v1/metering?month=2026-06")
    check("GET /api/v1/metering -> 200", r.status_code == 200, str(r.status_code))
    body = r.json()
    check("mimir_configured true (transport injected)", body["mimir_configured"] is True)
    check("period label is 2026-06", body["period"]["label"] == "2026-06", str(body["period"]))

    tenants = {t["tenant_id"]: t for t in body["tenants"]}
    check("two tenants enumerated from fleet", set(tenants) == {"acme", "globex"}, str(set(tenants)))

    acme = tenants.get("acme", {})
    check(
        "acme deployments listed (both, sorted)",
        acme.get("deployments") == ["acme-euw1-bb22", "acme-use1-aa11"],
        str(acme.get("deployments")),
    )
    check(
        "acme obs total = sum per-source (1234+88)",
        acme.get("totals", {}).get("observations") == 1322.0,
        str(acme.get("totals")),
    )
    check(
        "acme per-source breakdown present (github,slack)",
        acme.get("metrics", {}).get("obs_per_source") == {"github": 1234.0, "slack": 88.0},
        str(acme.get("metrics", {}).get("obs_per_source")),
    )
    check("acme think_runs = 57", acme.get("totals", {}).get("think_runs") == 57.0, str(acme.get("totals")))
    check(
        "acme cost_usd ~ 3.141592",
        abs(acme.get("totals", {}).get("cost_usd", 0) - 3.141592) < 1e-6,
        str(acme.get("totals")),
    )

    globex = tenants.get("globex", {})
    check(
        "globex (no activity) rolls up to 0, not an error",
        globex.get("totals", {}).get("observations") == 0.0
        and "error" not in globex
        and globex.get("totals", {}).get("think_runs") == 0.0,
        str(globex),
    )

    ft = body["fleet_totals"]
    check("fleet observations = 1322 (acme only)", ft["observations"] == 1322.0, str(ft))
    check("fleet think_runs = 57", ft["think_runs"] == 57.0, str(ft))
    check("fleet cost_usd ~ 3.141592", abs(ft["cost_usd"] - 3.141592) < 1e-6, str(ft))

    # Per-tenant scoping: each tenant only queried under its own X-Scope-OrgID.
    check(
        "X-Scope-OrgID scoping: only acme+globex orgs seen, never cross-tenant",
        set(_SEEN_ORGS) == {"acme", "globex"} and "<none>" not in _SEEN_ORGS,
        str(set(_SEEN_ORGS)),
    )

    check(
        "I1 note says export is signed",
        "signed" in body["note"].lower() and "export" in body["note"].lower(),
        body["note"],
    )

    # --- HTML view -------------------------------------------------------- #
    h = client.get("/metering?month=2026-06")
    check("GET /metering -> 200 html", h.status_code == 200 and "text/html" in h.headers["content-type"])
    text = h.text
    check("HTML contains tenant acme", "acme" in text)
    check("HTML contains per-source detail (github)", "github" in text)
    check("HTML contains the signed-export note", "signed" in text.lower())
    check("HTML has fleet total row", "Fleet total" in text)
    check("HTML top-nav links to /audit /alerts", "/audit" in text and "/alerts" in text)

    # --- degraded path: no transport, no mimir url -> no 500 -------------- #
    deps2 = _Deps(store=store, settings=_Settings(""), transport=None)
    client2 = TestClient(_app(deps2))
    r2 = client2.get("/api/v1/metering")
    check("no-mimir JSON -> 200 (no 500)", r2.status_code == 200, str(r2.status_code))
    b2 = r2.json()
    check("no-mimir reports mimir_configured false", b2["mimir_configured"] is False)
    check(
        "no-mimir still lists fleet tenants with null metrics",
        len(b2["tenants"]) == 2 and all(t.get("totals") is None for t in b2["tenants"]),
        str(b2["tenants"]),
    )
    h2 = client2.get("/metering")
    check("no-mimir HTML -> 200", h2.status_code == 200 and "not configured" in h2.text.lower())

    print(f"\n{_PASS} passed, {_FAIL} failed")
    return 0 if _FAIL == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
