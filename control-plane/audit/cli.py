#!/usr/bin/env python3
"""cli — admin CLI for the WS-AUDIT append-only audit log + break-glass workflow (FR-G / I5).

Two command groups over the same hash-chained audit log:

  audit ...        — append/verify/list the tamper-evident audit trail.
  breakglass ...   — the customer-granted, scoped, time-boxed break-glass workflow.

The signing key (for the signed checkpoint, I6) is loaded from ``control-plane/signing`` when
present; without one the log still chains (intra-file tamper-evidence) but the checkpoint is
unsigned. Pass ``--log`` / ``--store`` to point at a specific file (defaults live in this dir).

Examples
--------
    # append + verify
    python cli.py audit append --actor ops@fyralis --action config.apply --target acme --meta '{"v":7}'
    python cli.py audit verify
    python cli.py audit list --limit 20

    # break-glass: request -> customer approves -> use within window
    python cli.py breakglass request --actor sre@fyralis --scope tenant:acme/logs:read --ttl 900
    python cli.py breakglass approve --grant-id bg-xxxx --approved-by acme-admin@acme.com
    python cli.py breakglass check   --actor sre@fyralis --scope tenant:acme/logs:read
    python cli.py breakglass list
"""

from __future__ import annotations

import argparse
import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
SIGNING_DIR = os.path.normpath(os.path.join(HERE, "..", "signing"))
for _p in (HERE, SIGNING_DIR):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import audit_log as al  # noqa: E402
import breakglass as bg  # noqa: E402
import signing_lib as sl  # noqa: E402


# --------------------------------------------------------------------------- #
# Wiring: load the signing keyring (for the signed checkpoint) if available    #
# --------------------------------------------------------------------------- #


def _load_signing_keyring(trust_root_path: str, keys_dir: str) -> "sl.Keyring | None":
    """Load a *signing* keyring (with the active private key) so the CLI can re-sign the
    checkpoint on append. Returns None if no key material is on this host (verifier-only)."""
    if not os.path.exists(trust_root_path):
        return None
    with open(trust_root_path, "r", encoding="utf-8") as fh:
        doc = json.load(fh)
    ring = sl.Keyring.from_trust_root(doc)  # public-only to start
    active = doc.get("active_key_id")
    if not active:
        return None
    priv_path = os.path.join(keys_dir, f"{active}.private.pem")
    if not os.path.exists(priv_path):
        return None  # no private material here — checkpoint will be unsigned
    with open(priv_path, "rb") as fh:
        priv = sl.load_private_key_pem(fh.read())
    # Rebuild a signing ring: re-add the active key WITH its private material.
    signing_ring = sl.Keyring()
    for kid, meta in doc.get("keys", {}).items():
        if kid == active:
            signing_ring.add_key(kid, public=priv.public_key(), private=priv, make_active=True)
        else:
            signing_ring.add_key(
                kid,
                public=sl.public_key_from_b64(meta["pubkey"]),
                status=meta.get("status", "retired"),
            )
    return signing_ring


def _open_log(args) -> al.AuditLog:
    trust_root = args.trust_root or os.path.join(SIGNING_DIR, "trust_root.json")
    keys_dir = args.keys_dir or os.path.join(SIGNING_DIR, "keys")
    ring = _load_signing_keyring(trust_root, keys_dir)
    return al.open_log(args.log, signing_keyring=ring, trust_root_path=trust_root)


# --------------------------------------------------------------------------- #
# audit subcommands                                                            #
# --------------------------------------------------------------------------- #


def cmd_audit_append(args) -> int:
    log = _open_log(args)
    meta = json.loads(args.meta) if args.meta else {}
    e = log.append(actor=args.actor, action=args.action, target=args.target, metadata=meta)
    print(f"appended seq={e.seq} hash={e.entry_hash[:16]}… ts={e.ts}")
    return 0


def cmd_audit_verify(args) -> int:
    log = _open_log(args)
    res = log.verify_chain(check_signature=not args.no_signature)
    sig = {True: "valid", False: "INVALID", None: "n/a"}[res.signature_ok]
    if res.ok:
        print(f"CHAIN OK: {res.reason} | head={(res.head_hash or '')[:16]}… | checkpoint-sig={sig}")
        return 0
    where = f" at seq {res.bad_seq}" if res.bad_seq is not None else ""
    print(f"CHAIN BROKEN{where}: {res.reason}", file=sys.stderr)
    return 1


def cmd_audit_list(args) -> int:
    log = _open_log(args)
    entries = log.entries()
    if args.limit:
        entries = entries[-args.limit :]
    for e in entries:
        meta = json.dumps(e.metadata, sort_keys=True, ensure_ascii=False)
        print(f"[{e.seq:>4}] {e.ts}  {e.actor}  {e.action}  -> {e.target}  {meta}")
    return 0


# --------------------------------------------------------------------------- #
# breakglass subcommands                                                       #
# --------------------------------------------------------------------------- #


def _open_breakglass(args) -> bg.BreakGlass:
    log = _open_log(args)
    return bg.BreakGlass(log, store_path=args.store)


def cmd_bg_request(args) -> int:
    mgr = _open_breakglass(args)
    g = mgr.request_grant(actor=args.actor, scope=args.scope, ttl=args.ttl, reason=args.reason or "")
    print(f"requested grant {g.grant_id} for {g.actor} scope={g.scope!r} ttl={g.ttl_seconds}s")
    print("  -> AWAITING CUSTOMER APPROVAL (inert until approved)")
    return 0


def cmd_bg_approve(args) -> int:
    mgr = _open_breakglass(args)
    g = mgr.approve_grant(args.grant_id, approved_by=args.approved_by)
    print(f"approved {g.grant_id} by {g.approved_by}; expires in {g.ttl_seconds}s")
    return 0


def cmd_bg_deny(args) -> int:
    mgr = _open_breakglass(args)
    g = mgr.deny_grant(args.grant_id, denied_by=args.denied_by, reason=args.reason or "")
    print(f"denied {g.grant_id}")
    return 0


def cmd_bg_revoke(args) -> int:
    mgr = _open_breakglass(args)
    g = mgr.revoke_grant(args.grant_id, revoked_by=args.revoked_by, reason=args.reason or "")
    print(f"revoked {g.grant_id}")
    return 0


def cmd_bg_check(args) -> int:
    mgr = _open_breakglass(args)
    d = mgr.check_access(actor=args.actor, scope=args.scope)
    if d.allowed:
        print(f"ALLOW: {d.reason}")
        return 0
    print(f"DENY: {d.reason}", file=sys.stderr)
    return 1


def cmd_bg_sweep(args) -> int:
    mgr = _open_breakglass(args)
    expired = mgr.sweep_expirations()
    print(f"expired {len(expired)} grant(s): {', '.join(expired) if expired else '(none)'}")
    return 0


def cmd_bg_list(args) -> int:
    mgr = _open_breakglass(args)
    mgr.sweep_expirations()  # reflect expiries in the listing
    for g in mgr.grants():
        print(
            f"{g.grant_id}  {g.state:>9}  actor={g.actor}  scope={g.scope!r}  "
            f"ttl={g.ttl_seconds}s  uses={g.use_count}"
        )
    return 0


# --------------------------------------------------------------------------- #
# argparse wiring                                                              #
# --------------------------------------------------------------------------- #


def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(description="WS-AUDIT admin CLI (audit log + break-glass).")
    ap.add_argument("--log", default=None, help="audit log path (default: audit/audit.log.jsonl)")
    ap.add_argument("--store", default=None, help="break-glass grant store (default beside the log)")
    ap.add_argument("--trust-root", default=None, help="signing trust_root.json (for checkpoint sig)")
    ap.add_argument("--keys-dir", default=None, help="signing keys dir (for the active private key)")
    sub = ap.add_subparsers(dest="group", required=True)

    # audit group
    a = sub.add_parser("audit", help="append/verify/list the tamper-evident audit trail")
    asub = a.add_subparsers(dest="cmd", required=True)
    ap_app = asub.add_parser("append", help="append one hash-chained entry")
    ap_app.add_argument("--actor", required=True)
    ap_app.add_argument("--action", required=True)
    ap_app.add_argument("--target", required=True)
    ap_app.add_argument("--meta", default=None, help="JSON metadata object")
    ap_app.set_defaults(func=cmd_audit_append)
    ap_ver = asub.add_parser("verify", help="verify the whole chain (detects tampering)")
    ap_ver.add_argument("--no-signature", action="store_true", help="skip the signed-checkpoint check")
    ap_ver.set_defaults(func=cmd_audit_verify)
    ap_lst = asub.add_parser("list", help="print entries")
    ap_lst.add_argument("--limit", type=int, default=0, help="show only the last N")
    ap_lst.set_defaults(func=cmd_audit_list)

    # breakglass group
    b = sub.add_parser("breakglass", help="customer-granted, scoped, time-boxed emergency access")
    bsub = b.add_subparsers(dest="cmd", required=True)
    bp_req = bsub.add_parser("request", help="request a grant (inert until approved)")
    bp_req.add_argument("--actor", required=True)
    bp_req.add_argument("--scope", required=True)
    bp_req.add_argument("--ttl", type=float, required=True, help="time-box in seconds")
    bp_req.add_argument("--reason", default=None)
    bp_req.set_defaults(func=cmd_bg_request)
    bp_app = bsub.add_parser("approve", help="CUSTOMER approves a grant (starts the time-box)")
    bp_app.add_argument("--grant-id", required=True)
    bp_app.add_argument("--approved-by", required=True, help="customer principal")
    bp_app.set_defaults(func=cmd_bg_approve)
    bp_den = bsub.add_parser("deny", help="customer denies a grant")
    bp_den.add_argument("--grant-id", required=True)
    bp_den.add_argument("--denied-by", required=True)
    bp_den.add_argument("--reason", default=None)
    bp_den.set_defaults(func=cmd_bg_deny)
    bp_rev = bsub.add_parser("revoke", help="revoke an approved grant before TTL")
    bp_rev.add_argument("--grant-id", required=True)
    bp_rev.add_argument("--revoked-by", required=True)
    bp_rev.add_argument("--reason", default=None)
    bp_rev.set_defaults(func=cmd_bg_revoke)
    bp_chk = bsub.add_parser("check", help="check (and audit) access under live grants")
    bp_chk.add_argument("--actor", required=True)
    bp_chk.add_argument("--scope", required=True)
    bp_chk.set_defaults(func=cmd_bg_check)
    bp_swp = bsub.add_parser("sweep", help="expire any elapsed grants now (audits each)")
    bp_swp.set_defaults(func=cmd_bg_sweep)
    bp_lst = bsub.add_parser("list", help="list grants + states")
    bp_lst.set_defaults(func=cmd_bg_list)

    return ap


def main(argv: list[str] | None = None) -> int:
    ap = build_parser()
    args = ap.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
