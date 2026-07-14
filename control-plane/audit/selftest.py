#!/usr/bin/env python3
"""selftest — end-to-end proof of WS-AUDIT (append-only hash-chained log + break-glass, FR-G/I5).

Runs entirely in an isolated temp workspace (never touches the repo's audit log or signing keys),
minting a REAL throwaway ed25519 signing key via ``control-plane/signing`` so the **signed
checkpoint** path (whole-file tamper-evidence, I6) is exercised, not faked.

Required scenarios (every one must hold; any failure exits non-zero):

  A. APPEND + VERIFY    — append several entries → ``verify_chain()`` OK (hash chain + signed
                          checkpoint both valid).
  B. TAMPER DETECTED    — flip a field in ONE past entry on disk → ``verify_chain()`` DETECTS it
                          (and pinpoints the broken seq). Also: rewrite the WHOLE file recomputing
                          every hash → the hash chain re-links but the SIGNED CHECKPOINT catches it.
  C. BREAK-GLASS WINDOW — request → customer-approve a 1-second grant → ``check_access`` ALLOWED
                          within the window; after expiry → DENIED.
  D. EVERY EVENT AUDITED— assert each grant lifecycle transition (request, approve, USE, expire)
                          produced an audit entry, and the trail stays chain-valid throughout.

Run::  python selftest.py        (exit 0 = all green)
"""

from __future__ import annotations

import json
import os
import sys
import time
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
SIGNING_DIR = os.path.normpath(os.path.join(HERE, "..", "signing"))
for _p in (HERE, SIGNING_DIR):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import signing_lib as sl  # noqa: E402
import audit_log as al  # noqa: E402
import breakglass as bg  # noqa: E402

_results: list[tuple[str, bool, str]] = []


def check(name: str, cond: bool, detail: str = "") -> None:
    _results.append((name, bool(cond), detail))
    mark = "PASS" if cond else "FAIL"
    line = f"[{mark}] {name}"
    if detail:
        line += f"  — {detail}"
    print(line)


def _mint_signing_keyring(tmp: str) -> tuple[sl.Keyring, str]:
    """Mint a real ed25519 signing key + trust root under ``tmp`` (isolated from the repo)."""
    priv, pub = sl.generate_keypair()
    ring = sl.Keyring()
    ring.add_key("cp-signing-audit-selftest", public=pub, private=priv, make_active=True)
    trust_root_path = os.path.join(tmp, "trust_root.json")
    with open(trust_root_path, "w", encoding="utf-8") as fh:
        json.dump(ring.to_trust_root(), fh, indent=2, sort_keys=True)
    return ring, trust_root_path


def main() -> int:
    tmp = tempfile.mkdtemp(prefix="ws-audit-selftest-")
    print(f"# WS-AUDIT selftest (workdir {tmp})\n")

    ring, trust_root_path = _mint_signing_keyring(tmp)
    log_path = os.path.join(tmp, "audit.log.jsonl")

    # ----------------------------------------------------------------- #
    # A. APPEND several entries -> verify_chain() OK                    #
    # ----------------------------------------------------------------- #
    log = al.AuditLog(log_path, signing_keyring=ring, trust_root_path=trust_root_path)
    e0 = log.append("ops@fyralis", "cert.issue", "acme", {"fingerprint": "ab12"})
    e1 = log.append("ops@fyralis", "config.apply", "acme", {"version": 7})
    e2 = log.append("release-cd", "release.publish", "1.4.2", {"key_id": "cp-signing-2026-06"})
    e3 = log.append("ops@fyralis", "cert.revoke", "bossco", {"reason": "churn"})

    check("A1: appended 4 entries with contiguous seq 0..3",
          [e0.seq, e1.seq, e2.seq, e3.seq] == [0, 1, 2, 3])
    # chain links: each prev_hash == prior entry_hash; genesis prev == GENESIS
    check("A2: genesis prev_hash is the GENESIS sentinel", e0.prev_hash == al.GENESIS_HASH)
    check("A3: entry chain links (e1.prev==e0.hash, e3.prev==e2.hash)",
          e1.prev_hash == e0.entry_hash and e3.prev_hash == e2.entry_hash)

    res = log.verify_chain()
    check("A4: verify_chain() OK on an untampered log", res.ok, res.reason)
    check("A5: signed checkpoint is VALID (whole-file tamper-evidence)",
          res.signature_ok is True, f"signature_ok={res.signature_ok}")

    # ----------------------------------------------------------------- #
    # B. TAMPER one past entry on disk -> verify_chain() DETECTS it     #
    # ----------------------------------------------------------------- #
    # Make a copy we can tamper without disturbing the live store/checkpoint logic.
    tamper_log = os.path.join(tmp, "tampered.log.jsonl")
    with open(log_path, "r", encoding="utf-8") as fh:
        lines = fh.readlines()
    # Flip the target of entry seq=1 (config.apply acme -> ELEVATED) but keep its old entry_hash,
    # exactly the attack the chain must catch: a silently edited past record.
    rec = json.loads(lines[1])
    rec["target"] = "ELEVATED-bossco"      # privilege/scope escalation attempt
    rec["metadata"] = {"version": 999}
    lines[1] = json.dumps(rec, sort_keys=True, separators=(",", ":"), ensure_ascii=False) + "\n"
    with open(tamper_log, "w", encoding="utf-8") as fh:
        fh.writelines(lines)
    # Copy the (now-stale) checkpoint next to it so the verifier has one to check against.
    import shutil
    shutil.copy(log_path + ".checkpoint.json", tamper_log + ".checkpoint.json")

    tlog = al.AuditLog(tamper_log, trust_root_path=trust_root_path)
    tres = tlog.verify_chain()
    check("B1: verify_chain() DETECTS a tampered past entry", not tres.ok, tres.reason)
    check("B2: tamper is pinpointed at the edited seq (1)", tres.bad_seq == 1,
          f"bad_seq={tres.bad_seq}")

    # B3: whole-file rewrite — recompute EVERY hash forward so the chain re-links, but the head no
    # longer matches the SIGNED checkpoint (an attacker without the private key cannot re-sign).
    rewrite_log = os.path.join(tmp, "rewrite.log.jsonl")
    with open(log_path, "r", encoding="utf-8") as fh:
        orig = [json.loads(l) for l in fh if l.strip()]
    orig[1]["target"] = "ELEVATED-bossco"
    orig[1]["metadata"] = {"version": 999}
    prev = al.GENESIS_HASH
    out_lines = []
    for r in orig:
        body = {k: r[k] for k in ("seq", "ts", "actor", "action", "target", "metadata")}
        body["prev_hash"] = prev
        h = al.compute_entry_hash(body)   # forge a self-consistent hash
        body["entry_hash"] = h
        prev = h
        out_lines.append(json.dumps(body, sort_keys=True, separators=(",", ":"), ensure_ascii=False) + "\n")
    with open(rewrite_log, "w", encoding="utf-8") as fh:
        fh.writelines(out_lines)
    shutil.copy(log_path + ".checkpoint.json", rewrite_log + ".checkpoint.json")
    rwlog = al.AuditLog(rewrite_log, trust_root_path=trust_root_path)
    rwres = rwlog.verify_chain()
    check("B3: whole-file rewrite passes the hash chain but FAILS the signed checkpoint",
          (not rwres.ok) and rwres.signature_ok is False, rwres.reason)
    # sanity: the same rewrite is "ok" if we (wrongly) skip the signature — proves the chain alone
    # re-links and it is specifically the signed checkpoint catching the whole-file attack.
    rwres_nosig = rwlog.verify_chain(check_signature=False)
    check("B4: (control) hash chain alone re-links after a full rewrite",
          rwres_nosig.ok, rwres_nosig.reason)

    # The original live log is still intact.
    check("B5: original log remains chain-valid", log.verify_chain().ok)

    # ----------------------------------------------------------------- #
    # C + D. BREAK-GLASS: 1s grant allowed in-window, denied after,    #
    #         and EVERY lifecycle event is audited.                    #
    # ----------------------------------------------------------------- #
    n_before = len(log.entries())
    mgr = bg.BreakGlass(log, store_path=os.path.join(tmp, "grants.json"))

    ACTOR = "sre@fyralis"
    SCOPE = "tenant:acme/logs:read"

    # request (inert) -> check denied BEFORE approval (customer-granted gate)
    grant = mgr.request_grant(ACTOR, SCOPE, ttl=1.0, reason="incident-4127")
    pre = mgr.check_access(ACTOR, SCOPE)
    check("C1: access DENIED before customer approval (inert request)", not pre.allowed, pre.reason)

    # customer approves -> starts the 1-second time-box
    mgr.approve_grant(grant.grant_id, approved_by="acme-admin@acme.com")

    # within the window -> ALLOWED
    within = mgr.check_access(ACTOR, SCOPE)
    check("C2: access ALLOWED within the 1s window after approval", within.allowed, within.reason)
    check("C3: allowed via the right grant + scope",
          within.grant_id == grant.grant_id and within.scope == SCOPE)

    # scoped: a DIFFERENT scope is NOT authorized even within the window
    other = mgr.check_access(ACTOR, "tenant:bossco/logs:read")
    check("C4: a different scope is DENIED (grant is scoped)", not other.allowed, other.reason)

    # wait out the time-box -> DENIED + auto-expired
    time.sleep(1.2)
    after = mgr.check_access(ACTOR, SCOPE)
    check("C5: access DENIED after the 1s TTL elapses (time-boxed)", not after.allowed, after.reason)
    g_after = mgr.get(grant.grant_id)
    check("C6: the grant auto-transitioned to EXPIRED", g_after.state == bg.STATE_EXPIRED,
          f"state={g_after.state}")

    # D. assert every grant/use/expiry produced an audit entry.
    new_entries = log.entries()[n_before:]
    actions = [e.action for e in new_entries]
    check("D1: REQUEST event audited", bg.ACTION_REQUEST in actions)
    check("D2: APPROVE event audited", bg.ACTION_APPROVE in actions)
    check("D3: USE event audited (the in-window allowed access)", bg.ACTION_USE in actions)
    check("D4: EXPIRE event audited (the time-box elapsing)", bg.ACTION_EXPIRE in actions)
    # every USE references the granting grant_id (the access is attributable)
    uses = [e for e in new_entries if e.action == bg.ACTION_USE]
    check("D5: each USE event references its grant_id",
          all(e.metadata.get("grant_id") == grant.grant_id for e in uses) and len(uses) >= 1,
          f"{len(uses)} use event(s)")
    # exactly ONE expire event for the grant (idempotent expiry — not re-emitted on later checks)
    mgr.check_access(ACTOR, SCOPE)  # another post-expiry check
    expires = [e for e in log.entries() if e.action == bg.ACTION_EXPIRE
               and e.metadata.get("grant_id") == grant.grant_id]
    check("D6: expiry audited EXACTLY once (idempotent, not re-emitted)", len(expires) == 1,
          f"{len(expires)} expire event(s)")

    # D7: the audit trail is STILL chain-valid after all the break-glass writes.
    final = log.verify_chain()
    check("D7: chain (and signed checkpoint) still valid after all break-glass events",
          final.ok and final.signature_ok is True, final.reason)

    print()
    n_pass = sum(1 for _, ok, _ in _results if ok)
    n_total = len(_results)
    all_green = n_pass == n_total
    print(f"# {n_pass}/{n_total} checks passed — {'ALL GREEN' if all_green else 'FAILURES PRESENT'}")

    import shutil as _sh
    _sh.rmtree(tmp, ignore_errors=True)
    return 0 if all_green else 1


# --- pytest entrypoint (optional) ------------------------------------------------- #

def test_ws_audit_end_to_end():
    assert main() == 0


if __name__ == "__main__":
    raise SystemExit(main())
