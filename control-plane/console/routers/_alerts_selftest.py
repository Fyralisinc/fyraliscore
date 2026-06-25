#!/usr/bin/env python3
"""Isolation self-test for the C2 alert surface (console/routers/alerts.py).

Mounts ONLY ``alerts.register(app, deps)`` on a BARE FastAPI app (no console app.py,
no real Mimir) with a stub deps namespace and an injected fake ruler fetcher. Exercises
the real grouping/render/degrade code paths.

Run:  cd control-plane && /home/prajwal-adhikari/Desktop/v2/fyraliscore/.venv/bin/python \
        console/routers/_alerts_selftest.py
"""

from __future__ import annotations

import os
import sys
from dataclasses import dataclass
from pathlib import Path

# Make ``import alerts`` resolve to this dir (router lives beside us); also allow the
# package-style import that app.py uses (routers.alerts) by adding console/ to path.
_HERE = Path(__file__).resolve().parent
_CONSOLE = _HERE.parent
for _p in (str(_HERE), str(_CONSOLE)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from fastapi import FastAPI  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402

import alerts  # noqa: E402


# --------------------------------------------------------------------------- #
# stub deps                                                                    #
# --------------------------------------------------------------------------- #


@dataclass
class _Settings:
    mimir_url: str = "http://mimir-test:9009"
    fleet_org_id: str = "fleet-test"
    loki_url: str = ""
    grafana_url: str = ""


@dataclass
class _Deps:
    settings: _Settings


# A canned Mimir ruler payload: 2 firing (1 page + 1 ticket), 1 pending (page, no
# deployment_id), 1 inactive (must be dropped), 1 recording rule (must be dropped),
# 1 alert with an unknown severity (-> "other").
_CANNED = {
    "status": "success",
    "data": {
        "groups": [
            {
                "name": "fleet-availability",
                "rules": [
                    {
                        "type": "alerting",
                        "name": "AgentDown",
                        "state": "firing",
                        "labels": {"severity": "page"},
                        "alerts": [
                            {
                                "state": "firing",
                                "labels": {"severity": "page", "deployment_id": "dep-a"},
                                "annotations": {"summary": "Agent dep-a stopped heartbeating"},
                                "activeAt": "2026-06-25T10:00:00Z",
                                "value": "1",
                            }
                        ],
                    },
                    {
                        "type": "alerting",
                        "name": "HighDLQDepth",
                        "state": "firing",
                        "labels": {"severity": "ticket"},
                        "alerts": [
                            {
                                "state": "firing",
                                "labels": {"severity": "ticket", "deployment_id": "dep-b"},
                                "annotations": {"summary": "DLQ depth high on dep-b"},
                                "activeAt": "2026-06-25T09:30:00Z",
                            }
                        ],
                    },
                    {
                        "type": "recording",
                        "name": "fyralis:ingest_write_rate:5m",
                        "alerts": [],
                    },
                ],
            },
            {
                "name": "fleet-misc",
                "rules": [
                    {
                        "type": "alerting",
                        "name": "ConfigDriftPending",
                        "state": "pending",
                        "labels": {"severity": "page"},
                        "alerts": [
                            {
                                "state": "pending",
                                "labels": {"severity": "page"},
                                "annotations": {"summary": "Config drift detected"},
                                "activeAt": "2026-06-25T10:05:00Z",
                            }
                        ],
                    },
                    {
                        "type": "alerting",
                        "name": "QuietRule",
                        "state": "inactive",
                        "labels": {"severity": "page"},
                        "alerts": [],
                    },
                    {
                        "type": "alerting",
                        "name": "WeirdSeverity",
                        "state": "firing",
                        "labels": {"severity": "wat"},
                        "alerts": [
                            {
                                "state": "firing",
                                "labels": {"severity": "wat", "deployment_id": "dep-c"},
                                "annotations": {"summary": "unknown severity bucket"},
                            }
                        ],
                    },
                ],
            },
        ]
    },
}


def _make_client(fetcher):
    app = FastAPI()
    deps = _Deps(settings=_Settings())
    alerts.register(app, deps)
    app.state.alerts_fetcher = fetcher
    return TestClient(app, raise_server_exceptions=True)


def _expect(cond, msg):
    if not cond:
        raise AssertionError(msg)


def main() -> int:
    # --- happy path: canned ruler payload ---------------------------------- #
    seen = {}

    def good_fetcher(mimir_url, fleet_org_id):
        seen["mimir_url"] = mimir_url
        seen["fleet_org_id"] = fleet_org_id
        return _CANNED

    c = _make_client(good_fetcher)

    r = c.get("/api/v1/alerts")
    _expect(r.status_code == 200, f"JSON ok expected 200, got {r.status_code}")
    body = r.json()
    _expect(body["source_ok"] is True, "source_ok should be True")
    # 4 active: AgentDown(page), HighDLQDepth(ticket), ConfigDriftPending(page), WeirdSeverity(other)
    _expect(body["total"] == 4, f"expected 4 active alerts, got {body['total']}: {body['counts']}")
    _expect(body["counts"]["page"] == 2, f"expected 2 page, got {body['counts']}")
    _expect(body["counts"]["ticket"] == 1, f"expected 1 ticket, got {body['counts']}")
    _expect(body["counts"]["other"] == 1, f"expected 1 other, got {body['counts']}")

    # deployment_id + summary surfaced; recording + inactive dropped
    page_names = {a["name"] for a in body["groups"]["page"]}
    _expect("AgentDown" in page_names and "ConfigDriftPending" in page_names, page_names)
    _expect("QuietRule" not in str(body), "inactive rule must be dropped")
    _expect("fyralis:ingest_write_rate:5m" not in str(body), "recording rule must be dropped")
    a0 = body["groups"]["page"][0]
    _expect(a0["deployment_id"] == "dep-a", f"deployment_id not surfaced: {a0}")
    _expect("heartbeating" in a0["summary"], f"summary not surfaced: {a0}")

    # settings flowed into the fetcher (mimir url + fleet org id from deps.settings)
    _expect(seen["mimir_url"] == "http://mimir-test:9009", seen)
    _expect(seen["fleet_org_id"] == "fleet-test", seen)

    # HTML renders 200 and contains the alert + a drill-down link + the nav
    rh = c.get("/alerts")
    _expect(rh.status_code == 200, f"HTML expected 200, got {rh.status_code}")
    txt = rh.text
    _expect("AgentDown" in txt and "/deployments/dep-a" in txt, "html missing alert/link")
    _expect('href="/audit"' in txt and 'href="/metering"' in txt, "top-nav missing")
    _expect("PAGE" in txt and "TICKET" in txt, "severity badges missing")

    # --- degraded path: mimir unreachable ---------------------------------- #
    def boom_fetcher(mimir_url, fleet_org_id):
        raise ConnectionError("connection refused")

    c2 = _make_client(boom_fetcher)
    r2 = c2.get("/api/v1/alerts")
    _expect(r2.status_code == 503, f"unreachable JSON expected 503, got {r2.status_code}")
    b2 = r2.json()
    _expect(b2["source_ok"] is False, "source_ok should be False when unreachable")
    _expect(b2["error"] and "ConnectionError" in b2["error"], f"error not surfaced: {b2['error']}")
    _expect(b2["total"] == 0, "no alerts when source down")

    r2h = c2.get("/alerts")
    _expect(r2h.status_code == 200, "HTML must still render 200 when source down")
    _expect("alert source unavailable" in r2h.text.lower(), "degraded banner missing")

    # --- bad-shape path: non-success status -------------------------------- #
    def bad_status_fetcher(mimir_url, fleet_org_id):
        return {"status": "error", "errorType": "bad_data", "error": "nope"}

    c3 = _make_client(bad_status_fetcher)
    r3 = c3.get("/api/v1/alerts")
    _expect(r3.status_code == 503, f"bad status expected 503, got {r3.status_code}")
    _expect(r3.json()["source_ok"] is False, "bad status should degrade")

    # --- empty fleet: success but no groups -------------------------------- #
    def empty_fetcher(mimir_url, fleet_org_id):
        return {"status": "success", "data": {"groups": []}}

    c4 = _make_client(empty_fetcher)
    r4 = c4.get("/api/v1/alerts")
    _expect(r4.status_code == 200, "empty fleet is OK -> 200")
    _expect(r4.json()["total"] == 0, "empty fleet -> 0 alerts")
    _expect(c4.get("/alerts").status_code == 200, "empty HTML renders")

    print("ALERTS SELFTEST: all checks passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
