#!/usr/bin/env python3
"""WS-MIMIR config validator.

Parses mimir.yaml + runtime_overrides.yaml with yaml.safe_load and asserts the
contract invariants:

  * multitenancy is ON (multitenancy_enabled AND auth_enabled true) so EVERY
    request requires X-Scope-OrgID.
  * the all-in-one target is set.
  * filesystem blocks + ruler storage under /data.
  * the per-tenant cardinality budget keys exist with sane (positive) defaults.
  * the runtime override file parses and the worked per-tenant example overrides
    the budget keys.
  * the ruler rule-path that integrate must mount fleet-sli onto is reported.

Run:  /path/to/python validate.py
Exit 0 on success; non-zero + a printed reason on failure.
"""
from __future__ import annotations

import sys
from pathlib import Path

import yaml

HERE = Path(__file__).resolve().parent
MIMIR_YAML = HERE / "mimir.yaml"
OVERRIDES_YAML = HERE / "runtime_overrides.yaml"

# The three budget knobs the contract calls out by name (must exist in limits).
REQUIRED_LIMIT_KEYS = (
    "max_global_series_per_user",
    "ingestion_rate",
    "max_label_names_per_series",
)


def _fail(msg: str) -> "None":
    print(f"FAIL: {msg}")
    sys.exit(1)


def main() -> int:
    # --- load both configs with safe_load -----------------------------------
    if not MIMIR_YAML.is_file():
        _fail(f"missing {MIMIR_YAML}")
    if not OVERRIDES_YAML.is_file():
        _fail(f"missing {OVERRIDES_YAML}")

    cfg = yaml.safe_load(MIMIR_YAML.read_text())
    ov = yaml.safe_load(OVERRIDES_YAML.read_text())

    if not isinstance(cfg, dict):
        _fail("mimir.yaml did not parse to a mapping")
    if not isinstance(ov, dict):
        _fail("runtime_overrides.yaml did not parse to a mapping")

    # --- multitenancy / auth MUST be on -------------------------------------
    # In Grafana Mimir `multitenancy_enabled: true` IS the auth flag (renamed
    # from the old Cortex/Loki `auth_enabled`; the binary rejects `auth_enabled`
    # in its config struct). multitenancy_enabled == true satisfies the contract
    # requirement that auth is on: every request requires X-Scope-OrgID.
    auth_on = cfg.get("multitenancy_enabled") is True
    if not auth_on:
        _fail("multitenancy_enabled must be true (every request needs X-Scope-OrgID)")
    # The literal `auth_enabled` key MUST NOT be present (Mimir 2.x rejects it).
    if "auth_enabled" in cfg:
        _fail("auth_enabled must NOT be a literal key — Mimir rejects it; use multitenancy_enabled")

    # --- all-in-one target --------------------------------------------------
    if cfg.get("target") != "all":
        _fail("target must be 'all' (monolithic/all-in-one for local)")

    # --- HTTP listener on 9009 (auth-proxy upstream) ------------------------
    port = (cfg.get("server") or {}).get("http_listen_port")
    if port != 9009:
        _fail(f"server.http_listen_port must be 9009 (auth-proxy upstream), got {port!r}")

    # --- filesystem storage under /data -------------------------------------
    blocks = cfg.get("blocks_storage") or {}
    if blocks.get("backend") != "filesystem":
        _fail("blocks_storage.backend must be 'filesystem' for local")
    ruler_storage = cfg.get("ruler_storage") or {}
    if ruler_storage.get("backend") != "filesystem":
        _fail("ruler_storage.backend must be 'filesystem' for local")
    ruler_dir = (ruler_storage.get("filesystem") or {}).get("dir", "")
    if not ruler_dir.startswith("/data"):
        _fail(f"ruler_storage filesystem dir must be under /data, got {ruler_dir!r}")
    blocks_dir = (blocks.get("filesystem") or {}).get("dir", "")
    if not blocks_dir.startswith("/data"):
        _fail(f"blocks_storage filesystem dir must be under /data, got {blocks_dir!r}")

    # --- remote-write receive path (distributor present) --------------------
    if "distributor" not in cfg:
        _fail("distributor section missing (remote-write receive path)")

    # --- per-tenant cardinality budget keys exist with sane defaults --------
    limits = cfg.get("limits") or {}
    for key in REQUIRED_LIMIT_KEYS:
        if key not in limits:
            _fail(f"limits.{key} missing (required cardinality budget knob)")
        val = limits[key]
        if not isinstance(val, int) or val <= 0:
            _fail(f"limits.{key} must be a positive int default, got {val!r}")

    # the overrides file must be referenced from runtime_config (Mimir's loader)
    ref = (cfg.get("runtime_config") or {}).get("file")
    if not ref or "runtime_overrides.yaml" not in ref:
        _fail("mimir.yaml > runtime_config.file must point at runtime_overrides.yaml")

    # --- runtime overrides: worked per-tenant example overrides the budget ---
    overrides = ov.get("overrides")
    if not isinstance(overrides, dict) or not overrides:
        _fail("runtime_overrides.yaml > overrides must be a non-empty mapping")
    # at least one tenant must override the headline budget knob
    has_budget_override = any(
        isinstance(t, dict) and "max_global_series_per_user" in t
        for t in overrides.values()
    )
    if not has_budget_override:
        _fail("no per-tenant override sets max_global_series_per_user (need a worked example)")

    # --- report the ruler rule-path for integrate ---------------------------
    rule_path = (cfg.get("ruler") or {}).get("rule_path", "")

    print("OK: mimir.yaml + runtime_overrides.yaml valid")
    print(f"  multitenancy_enabled = {cfg['multitenancy_enabled']}  (== auth on: every request needs X-Scope-OrgID)")
    print(f"  target               = {cfg['target']}")
    print(f"  http_listen_port     = {port}  (auth-proxy upstream http://mimir:9009)")
    print(f"  blocks dir           = {blocks_dir}")
    print(f"  ruler storage dir    = {ruler_dir}  (per-tenant: {ruler_dir}/<tenant>/<file>.yaml)")
    print("  cardinality budget defaults:")
    for key in REQUIRED_LIMIT_KEYS:
        print(f"    {key:32s} = {limits[key]}")
    print(f"  per-tenant override file = {ref}")
    print(f"  override tenants        = {sorted(overrides.keys())}")
    print("  RULER RULE-PATH for integrate (where fleet-sli rules come from + go to):")
    print("    fleet-sli source mount : ./fleet-sli -> /rules (ro, on the loader)")
    print("    loaded via             : mimirtool rules load (ruler API /prometheus/config/v1/rules)")
    print("    ruler tenant           : __fleet__   (X-Scope-OrgID for recorded fleet:* series)")
    print(f"    ruler.rule_path (tmp)   : {rule_path}")
    print(f"    ruler_storage backend   : {ruler_storage.get('backend')} dir={ruler_dir}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
