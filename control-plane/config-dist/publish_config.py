#!/usr/bin/env python3
"""publish_config.py — set a deployment's config -> mint a NEW signed version (FR-C3/D4).

The operator-side CLI for the config-distribution store. Publishing is **additive**: a
tier change or a flag flip produces a brand-new immutable, ed25519-signed version (C2/I6)
that the agent picks up on its next pull — **no redeploy**. The signed bytes are the
canonical config document; signing is delegated to ``control-plane/signing`` (same code
path the agent verifies with).

Examples
--------
    # Flip a flag (layers onto the deployment's current config; v -> v+1)
    python publish_config.py acme-use1-7f3a --tenant-id acme \
        --flag anomaly_detection_enabled=true

    # Change the telemetry tier (T1 -> T2) — a new signed version
    python publish_config.py acme-use1-7f3a --tenant-id acme --tier T2

    # Set a token-rotation schedule field (FR-D4)
    python publish_config.py acme-use1-7f3a --tenant-id acme \
        --rotation interval_hours=12 --rotation enabled=true

    # Replace the whole config body from a JSON file
    python publish_config.py acme-use1-7f3a --tenant-id acme --config-file body.json

    # Inspect history
    python publish_config.py acme-use1-7f3a --list
    python publish_config.py --list-deployments

By default writes under the config-dist-owned store (``config-dist/_data/store``) and
signs with the config-dist signing home (``config-dist/_data/signing-home``); override
with ``CONFIG_DIST_STORE_ROOT`` / ``CONFIG_DIST_SIGNING_HOME`` env or the matching flags.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any, Dict

_HERE = Path(__file__).resolve().parent
if str(_HERE) not in sys.path:
    sys.path.insert(0, str(_HERE))

from store import (  # noqa: E402
    ConfigStore,
    ConfigStoreError,
    DEFAULT_SIGNING_HOME,
    DEFAULT_STORE_ROOT,
    SigningHome,
    default_config_payload,
)


def _coerce_scalar(raw: str) -> Any:
    """Parse a ``key=value`` value into a JSON scalar (bool/int/float/null/str).

    ``true``/``false``/``null`` -> JSON literals; numeric -> int/float; a value that
    parses as JSON (e.g. ``"[1,2]"``) is taken as that JSON; else the raw string.
    """
    low = raw.strip().lower()
    if low in ("true", "false"):
        return low == "true"
    if low in ("null", "none"):
        return None
    try:
        return json.loads(raw)
    except (ValueError, TypeError):
        return raw


def _parse_kv_list(pairs: list[str], what: str) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for item in pairs or []:
        if "=" not in item:
            raise SystemExit(f"--{what} expects key=value, got {item!r}")
        k, v = item.split("=", 1)
        k = k.strip()
        if not k:
            raise SystemExit(f"--{what} has empty key in {item!r}")
        out[k] = _coerce_scalar(v)
    return out


def build_store(args) -> ConfigStore:
    store_root = (
        args.store_root
        or os.environ.get("CONFIG_DIST_STORE_ROOT")
        or str(DEFAULT_STORE_ROOT)
    )
    signing_home_root = (
        args.signing_home
        or os.environ.get("CONFIG_DIST_SIGNING_HOME")
        or str(DEFAULT_SIGNING_HOME)
    )
    key_id = os.environ.get("CONFIG_DIST_KEY_ID", "cp-config-dist")
    home = SigningHome(Path(signing_home_root), key_id=key_id)
    return ConfigStore(store_root=store_root, signing_home=home)


def cmd_list_deployments(store: ConfigStore) -> int:
    deps = store.list_deployments()
    if not deps:
        print("(no deployments published yet)")
        return 0
    for d in deps:
        head = store.current_version(d)
        cv = store.get_head(d)
        tier = cv.telemetry_tier if cv else "?"
        print(f"{d}\tv{head}\ttier={tier}")
    return 0


def cmd_list_versions(store: ConfigStore, deployment_id: str) -> int:
    try:
        versions = store.list_versions(deployment_id)
    except ConfigStoreError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    if not versions:
        print(f"(no versions for {deployment_id})")
        return 0
    head = store.current_version(deployment_id)
    for v in versions:
        cv = store.get_version(deployment_id, v)
        tier = cv.telemetry_tier if cv else "?"
        marker = "  <- HEAD" if v == head else ""
        print(f"v{v}\ttier={tier}\tkey={cv.key_id if cv else '?'}{marker}")
    return 0


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        description="Publish a signed config version for a deployment (FR-C3/D4)."
    )
    ap.add_argument(
        "deployment_id",
        nargs="?",
        help="the deployment to publish for (omit only with --list-deployments)",
    )
    ap.add_argument("--tenant-id", help="tenant owning the deployment (required to publish)")
    ap.add_argument(
        "--flag",
        action="append",
        default=[],
        metavar="KEY=VALUE",
        help="set a feature flag (repeatable); value parsed as bool/int/float/json/str",
    )
    ap.add_argument(
        "--tier",
        choices=["T1", "T2", "T3"],
        help="set the telemetry tier (C3) — a change is a new signed version",
    )
    ap.add_argument(
        "--rotation",
        action="append",
        default=[],
        metavar="KEY=VALUE",
        help="set a token-rotation schedule field (FR-D4; repeatable)",
    )
    ap.add_argument(
        "--config-file",
        help="replace the WHOLE config body with this JSON file (mutually exclusive "
        "with --flag/--tier/--rotation)",
    )
    ap.add_argument("--store-root", help="override the store root dir")
    ap.add_argument("--signing-home", help="override the signing home dir")
    ap.add_argument("--list", action="store_true", help="list this deployment's versions")
    ap.add_argument(
        "--list-deployments", action="store_true", help="list all deployments"
    )
    ap.add_argument("--json", action="store_true", help="print the publish result as JSON")
    args = ap.parse_args(argv)

    store = build_store(args)

    if args.list_deployments:
        return cmd_list_deployments(store)

    if not args.deployment_id:
        ap.error("deployment_id is required (or use --list-deployments)")

    if args.list:
        return cmd_list_versions(store, args.deployment_id)

    # --- publish path ---------------------------------------------------- #
    if not args.tenant_id:
        ap.error("--tenant-id is required to publish")

    pieces_given = bool(args.flag or args.tier or args.rotation)
    if args.config_file and pieces_given:
        ap.error("--config-file is mutually exclusive with --flag/--tier/--rotation")

    if args.config_file:
        try:
            body = json.loads(Path(args.config_file).read_text(encoding="utf-8"))
        except (OSError, ValueError) as exc:
            print(f"error: could not read --config-file: {exc}", file=sys.stderr)
            return 2
        if not isinstance(body, dict):
            print("error: --config-file must contain a JSON object", file=sys.stderr)
            return 2
    else:
        # Layer the requested changes over the deployment's current config (or default).
        current = store.get_head(args.deployment_id)
        if current is not None:
            body = dict(current.document().get("config", {}))
        else:
            body = default_config_payload(
                tenant_id=args.tenant_id, deployment_id=args.deployment_id
            )
        if args.flag:
            flags = dict(body.get("flags", {}))
            flags.update(_parse_kv_list(args.flag, "flag"))
            body["flags"] = flags
        if args.tier:
            body["telemetry_tier"] = args.tier
        if args.rotation:
            rot = dict(body.get("token_rotation", {}))
            rot.update(_parse_kv_list(args.rotation, "rotation"))
            body["token_rotation"] = rot
        if not (args.flag or args.tier or args.rotation) and current is not None:
            print(
                "warning: no --flag/--tier/--rotation given; republishing current "
                "config unchanged as a new version",
                file=sys.stderr,
            )

    try:
        cv = store.publish(
            deployment_id=args.deployment_id,
            tenant_id=args.tenant_id,
            config_body=body,
        )
    except ConfigStoreError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2

    # Self-verify the freshly-published version (I6) before reporting success.
    res = store.verify_version(cv.deployment_id, cv.version)
    if not res.ok:
        print(f"error: published version failed self-verify: {res.reason}", file=sys.stderr)
        return 3

    result = {
        "deployment_id": cv.deployment_id,
        "version": cv.version,
        "key_id": cv.key_id,
        "telemetry_tier": cv.telemetry_tier,
        "sha256": cv.manifest.get("sha256"),
        "signed_at": cv.manifest.get("signed_at"),
        "dir": str(cv.dir),
        "verified": res.ok,
        "config_url_path": f"/config/{cv.deployment_id}",
    }
    if args.json:
        print(json.dumps(result, indent=2, sort_keys=True))
    else:
        print(f"published {cv.deployment_id} -> v{cv.version} (signed by {cv.key_id})")
        print(f"  telemetry_tier : {cv.telemetry_tier}")
        print(f"  flags          : {json.dumps(body.get('flags', {}))}")
        print(f"  token_rotation : {json.dumps(body.get('token_rotation', {}))}")
        print(f"  sha256         : {cv.manifest.get('sha256')}")
        print(f"  verified (I6)  : {res.ok} — {res.reason}")
        print(f"  agent pulls at : /config/{cv.deployment_id}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
