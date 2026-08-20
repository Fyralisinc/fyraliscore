#!/usr/bin/env python3
"""Validate the Fyralis control-plane Grafana provisioning (P3).

Checks, per the task contract:
  1. yaml.safe_load the datasource provisioning + dashboard provider provisioning.
  2. json.load every dashboard JSON.
  3. Assert the X-Scope-OrgID header is configured on EVERY Mimir/Loki datasource
     (the operator query-path tenant scoping, C5).
  4. Assert the per-customer datasources use the templated ${tenant_scope} value
     and the fleet datasources use the fleet/admin org id.
  5. Assert the per-customer dashboard declares the `tenant_scope` variable that
     drives that header, and that dashboards reference real provisioned DS uids.
  6. Sanity: compose fragment parses and exposes :3000 on cp-net, depends on
     mimir + loki, mounts provisioning + dashboards.

Run:
  /home/prajwal-adhikari/Desktop/v2/fyraliscore/.venv/bin/python grafana/validate.py
Exit 0 = all assertions held.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import NoReturn

import yaml

HERE = Path(__file__).resolve().parent
DS_FILE = HERE / "provisioning" / "datasources" / "datasources.yaml"
DASH_PROVIDER_FILE = HERE / "provisioning" / "dashboards" / "dashboards.yaml"
COMPOSE_FILE = HERE / "service.compose.yml"
DASH_DIR = HERE / "dashboards"

SCOPE_HEADER = "X-Scope-OrgID"
TENANT_SCOPE_VALUE = "${tenant_scope}"
FLEET_ORG_DEFAULT = "__fleet__"


def _fail(msg: str) -> NoReturn:
    print(f"  FAIL: {msg}")
    sys.exit(1)


def check_datasources() -> dict[str, dict]:
    print(f"[1] yaml.safe_load datasources: {DS_FILE.relative_to(HERE)}")
    doc = yaml.safe_load(DS_FILE.read_text())
    assert isinstance(doc, dict), "datasources.yaml is not a mapping"
    assert doc.get("apiVersion") == 1, "datasources apiVersion must be 1"
    dses = doc.get("datasources")
    assert isinstance(dses, list) and dses, "no datasources defined"

    by_uid: dict[str, dict] = {}
    mimir_seen = loki_seen = False
    for ds in dses:
        name = ds.get("name", "?")
        uid = ds.get("uid")
        assert uid, f"datasource {name!r} missing uid"
        by_uid[uid] = ds

        # --- single-tenant CP self-obs Prometheus is NOT X-Scope-OrgID scoped ---
        # The dedicated control-plane watchdog Prometheus (uid fyralis-cp-prometheus)
        # is its own single-tenant store, NOT central Mimir; it carries no
        # X-Scope-OrgID header. Skip the multi-tenancy assertions for it.
        if uid == "fyralis-cp-prometheus":
            assert ds.get("type") == "prometheus"
            print(f"    OK  {name:<16} type={ds.get('type'):<11} "
                  f"(single-tenant CP self-obs; no {SCOPE_HEADER})")
            continue

        # --- (3) X-Scope-OrgID header MUST be configured ---
        jd = ds.get("jsonData") or {}
        sd = ds.get("secureJsonData") or {}
        header_names = {k: v for k, v in jd.items() if k.startswith("httpHeaderName")}
        assert SCOPE_HEADER in header_names.values(), (
            f"datasource {name!r} does NOT configure the {SCOPE_HEADER} header "
            f"(jsonData httpHeaderName*); found {header_names}"
        )
        # find the index that holds X-Scope-OrgID, then assert its value is set
        idx = next(k[len("httpHeaderName"):] for k, v in header_names.items()
                   if v == SCOPE_HEADER)
        val_key = f"httpHeaderValue{idx}"
        assert val_key in sd, (
            f"datasource {name!r} sets {SCOPE_HEADER} name but no "
            f"secureJsonData.{val_key}"
        )
        val = sd[val_key]

        # --- (4) per-customer => templated/literal ; fleet => fleet org id ---
        if "fleet" in name.lower() or "fleet" in (uid or ""):
            # Fleet DSes read cross-tenant. They reference EITHER the documented
            # "__fleet__" ruler org id, OR (with tenant_federation enabled) a
            # bar-joined real-tenant fan-out so RAW series federate. The fan-out
            # MUST be a LITERAL "acme|globex" — Grafana provisioning does not honor
            # ${VAR:default} in secureJsonData (it sends an empty header -> Mimir
            # "no org id"), so a bare "|" in the value is the valid fleet signal.
            is_fanout = "|" in val
            assert (FLEET_ORG_DEFAULT in val or "FYRALIS_FLEET_ORG_ID" in val or is_fanout), (
                f"fleet datasource {name!r} {SCOPE_HEADER} value {val!r} does not "
                f"reference the fleet/admin org id {FLEET_ORG_DEFAULT!r} or a "
                f"bar-joined tenant fan-out (e.g. 'acme|globex')"
            )
        elif (uid or "").startswith("mimir-"):
            # Fixed per-tenant Mimir datasource (uid mimir-<tenant>): carries a
            # LITERAL X-Scope-OrgID (the dashboard tenant_ds var picks which one).
            # Grafana 11 won't interpolate a template var into secureJsonData, so
            # a literal value here is REQUIRED, not a defect.
            assert isinstance(val, str) and val and "${" not in val, (
                f"per-tenant datasource {name!r} (uid {uid!r}) must carry a LITERAL "
                f"{SCOPE_HEADER} value, got {val!r}"
            )
        else:
            assert val == TENANT_SCOPE_VALUE, (
                f"per-customer datasource {name!r} {SCOPE_HEADER} value must be the "
                f"templated {TENANT_SCOPE_VALUE!r}, got {val!r}"
            )

        # type bookkeeping
        if ds.get("type") == "prometheus":
            mimir_seen = True
            assert jd.get("prometheusType") == "Mimir", (
                f"metrics datasource {name!r} should declare prometheusType: Mimir"
            )
        elif ds.get("type") == "loki":
            loki_seen = True

        print(f"    OK  {name:<16} type={ds.get('type'):<11} "
              f"{SCOPE_HEADER}={val}")

    assert mimir_seen, "no Mimir (prometheus-type) datasource found"
    assert loki_seen, "no Loki datasource found"
    print(f"    -> {len(dses)} datasources, all carry {SCOPE_HEADER}.")
    return by_uid


def check_dashboard_provider() -> list[str]:
    print(f"[2] yaml.safe_load dashboard provider: "
          f"{DASH_PROVIDER_FILE.relative_to(HERE)}")
    doc = yaml.safe_load(DASH_PROVIDER_FILE.read_text())
    assert doc.get("apiVersion") == 1
    providers = doc.get("providers")
    assert isinstance(providers, list) and providers, "no dashboard providers"
    paths = []
    for p in providers:
        path = (p.get("options") or {}).get("path")
        assert path, f"provider {p.get('name')!r} missing options.path"
        paths.append(path)
        print(f"    OK  provider {p.get('name'):<16} folder={p.get('folder'):<14} "
              f"path={path}")
    return paths


def check_dashboards(known_uids: set[str]) -> None:
    print(f"[3] json.load dashboards under {DASH_DIR.relative_to(HERE)}")
    files = sorted(DASH_DIR.rglob("*.json"))
    assert files, "no dashboard JSON files found"
    titles = []
    tenant_var_ok = False
    for f in files:
        data = json.loads(f.read_text())  # raises on malformed JSON
        title = data.get("title", "?")
        uid = data.get("uid")
        titles.append(title)
        assert uid, f"{f.name} missing dashboard uid"

        # the per-customer dashboard MUST define the variable that scopes its
        # panels to one tenant. Either the legacy `tenant_scope` query var OR the
        # `tenant_ds` datasource-type var (Grafana-11-safe: picks a fixed
        # per-tenant Mimir DS, since a template var can't be interpolated into
        # the secureJsonData header).
        var_names = {v.get("name") for v in
                     (data.get("templating") or {}).get("list", [])}

        # every panel datasource uid must be a provisioned DS, a builtin, or the
        # `${tenant_ds}` datasource-VARIABLE binding (resolved per-tenant at query
        # time to one of the fixed per-tenant Mimir DSes).
        ds_var_refs = {f"${{{n}}}" for n in var_names}
        referenced = set()
        for panel in data.get("panels", []):
            for tgt in panel.get("targets", []) or []:
                d = tgt.get("datasource") or {}
                if isinstance(d, dict) and d.get("uid"):
                    referenced.add(d["uid"])
        builtins = {"-- Grafana --", "-- Mixed --", "-- Dashboard --"}
        unknown = {u for u in referenced
                   if u not in known_uids and u not in builtins
                   and u not in ds_var_refs}
        assert not unknown, (
            f"{f.name} references unknown datasource uid(s): {unknown}"
        )

        if "tenant" in f.parent.name or "tenant" in title.lower() or \
                "per-customer" in title.lower():
            assert "tenant_scope" in var_names or "tenant_ds" in var_names, (
                f"{f.name} is the per-customer dashboard but has no "
                f"`tenant_scope`/`tenant_ds` template variable to scope its panels"
            )
            tenant_var_ok = True

        print(f"    OK  {f.relative_to(DASH_DIR)!s:<34} title={title!r} "
              f"vars={sorted(var_names)}")

    assert tenant_var_ok, "no per-customer dashboard with tenant_scope variable found"
    fleet = [t for t in titles if "fleet" in t.lower()]
    assert fleet, "no FLEET OVERVIEW dashboard found"
    print(f"    -> {len(files)} dashboards parsed; fleet + per-customer present.")


def check_compose(known_uids: set[str]) -> None:
    print(f"[4] yaml.safe_load compose fragment: "
          f"{COMPOSE_FILE.relative_to(HERE)}")
    doc = yaml.safe_load(COMPOSE_FILE.read_text())
    svc = (doc.get("services") or {}).get("grafana")
    assert svc, "no grafana service in compose fragment"
    assert "grafana/grafana" in svc.get("image", ""), "not the grafana/grafana image"
    ports = svc.get("ports") or []
    assert any("3000:3000" in str(p) for p in ports), "grafana does not expose 3000"
    nets = svc.get("networks") or []
    assert "cp-net" in nets, "grafana not on cp-net"
    dep = svc.get("depends_on") or []
    deps = dep if isinstance(dep, list) else list(dep)
    assert "mimir" in deps and "loki" in deps, "grafana must depend_on mimir + loki"
    vols = svc.get("volumes") or []
    joined = " ".join(vols)
    assert "/etc/grafana/provisioning" in joined, "provisioning not mounted"
    assert "/var/lib/grafana/dashboards" in joined, "dashboards not mounted"
    print(f"    OK  grafana image={svc['image']} ports={ports} "
          f"nets={nets} depends_on={deps}")


def main() -> int:
    print("=== Fyralis CP Grafana provisioning validation ===")
    by_uid = check_datasources()
    known_uids = set(by_uid)
    check_dashboard_provider()
    check_dashboards(known_uids)
    check_compose(known_uids)
    print("\nALL CHECKS PASSED ✅")
    return 0


if __name__ == "__main__":
    sys.exit(main())
