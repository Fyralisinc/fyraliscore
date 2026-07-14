#!/usr/bin/env python3
"""trust_bundle.py — non-disruptive CA trust-chain OVERLAP for control-plane upgrades.

This is the helper that makes **CA rotation / revocation (FR-A5) non-disruptive**
to in-flight agent mTLS. It implements the single most important upgrade primitive:

    ADD a new CA's certs to the auth-proxy trust bundle BEFORE you start issuing
    leaves from it (and BEFORE you retire the old CA), so that during cutover the
    proxy trusts BOTH the old and the new CA. Every agent — whether its current
    client cert was signed by the old CA or a freshly-rotated one signed by the new
    CA — keeps verifying. No in-flight mTLS handshake ever breaks.

Why this works (mechanics, not magic)
--------------------------------------
The auth-proxy (``auth-proxy/proxy.py``) builds its server SSL context with

    ctx.load_verify_locations(cafile=AUTH_PROXY_CA_CHAIN)   # ca/pki/ca-chain.crt
    ctx.verify_mode = ssl.CERT_REQUIRED

``load_verify_locations`` accepts a **concatenated PEM bundle with multiple trust
anchors**. OpenSSL builds a path from the presented client leaf to *any* root in
that bundle. The sibling chain verifier (``ca/verify_chain.py::_split_roots``) does
the same: it sorts the bundle into roots + intermediates and accepts a leaf that
chains to *any* root. So a trust bundle that contains

    [ old_intermediate, old_root, new_intermediate, new_root ]

trusts leaves from both CAs simultaneously. That overlap window is the whole game:

    1. ADD new CA  -> bundle = {old, new}        (this helper: `add`)   <- BOTH trusted
    2. reload auth-proxy (rolling, zero-drop — see rolling_upgrade.sh)
    3. start issuing new leaves from the new CA; rotate agents at their own pace
    4. once every active agent presents a new-CA leaf, REMOVE the old CA
       (this helper: `remove`)                    -> bundle = {new}      <- old now untrusted
    5. reload auth-proxy again

Steps 1–2 must complete BEFORE step 3 (issue) and step 4 (remove). Doing it in the
other order (remove-then-add, or issue-before-add) is exactly the disruption this
helper exists to prevent: an agent mid-handshake whose CA just vanished from the
bundle gets a TLS failure.

I6 / verify-before-apply
------------------------
A CA trust-bundle is itself a trust-bearing artifact, so this helper integrates the
control-plane signing path (``signing/``): every WRITE produces a detached ed25519
signature + C2 manifest next to the new bundle (``--sign``), and every bundle can be
VERIFIED against the keyring before the proxy is told to load it (``verify`` /
``--require-signature``). That closes the loop: the upgrade procedure never swaps in
a trust bundle it did not itself sign, mirroring how agents verify config/license/
release before apply.

Operations
----------
    add     <bundle> --add-ca <intermediate+root.pem>   append a CA (idempotent)
    remove  <bundle> --match-root-cn "<CN>"             drop a CA by its root subject
    list    <bundle>                                    show every trust anchor
    verify  <bundle> [--leaf cert.pem]                  parse + (optionally) check a leaf
    sign    <bundle>                                    (re)sign the bundle (C2 / I6)

All write operations are atomic (temp-file + ``os.replace``) and make a timestamped
``.bak`` so a bad bundle is one ``mv`` from rollback.

This module imports the COMMITTED siblings (``ca/verify_chain.py``,
``ca/ca_lib.py``, ``signing/``) — it never re-implements crypto.
"""

from __future__ import annotations

import argparse
import datetime as _dt
import os
import shutil
import sys
import tempfile
from dataclasses import dataclass
from typing import List, Optional

# --- import committed siblings (no re-implementation of crypto) --------------
_HERE = os.path.dirname(os.path.abspath(__file__))
_CP_ROOT = os.path.dirname(_HERE)
for _p in (os.path.join(_CP_ROOT, "ca"), os.path.join(_CP_ROOT, "signing")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from cryptography import x509  # noqa: E402
from cryptography.hazmat.primitives import hashes, serialization  # noqa: E402

import verify_chain as vc  # noqa: E402  (ca/verify_chain.py)

# signing is OPTIONAL at import time so `list`/`add`/`remove` work even if the
# keyring is absent; only --sign / --require-signature need it.
try:
    sys.path.insert(0, os.path.join(_CP_ROOT, "signing"))
    import signing_lib as sl  # noqa: E402  (signing/signing_lib.py)

    _HAVE_SIGNING = True
except Exception:  # pragma: no cover - signing libs always present in this repo
    _HAVE_SIGNING = False


# ---------------------------------------------------------------------------
# PEM bundle <-> list[x509.Certificate]
# ---------------------------------------------------------------------------

def _read_certs(path: str) -> List[x509.Certificate]:
    if not os.path.isfile(path):
        raise FileNotFoundError(f"no such cert/bundle file: {path}")
    with open(path, "rb") as fh:
        data = fh.read()
    if not data.strip():
        return []
    # load_pem_x509_certificates parses a concatenated multi-cert PEM blob.
    return list(x509.load_pem_x509_certificates(data))


def _cert_pem(cert: x509.Certificate) -> bytes:
    return cert.public_bytes(serialization.Encoding.PEM)


def _fingerprint(cert: x509.Certificate) -> str:
    """Lowercase-hex SHA-256 of the DER cert — same key the CA/registry uses."""
    return cert.fingerprint(hashes.SHA256()).hex()


def _subject_cn(cert: x509.Certificate) -> str:
    try:
        attrs = cert.subject.get_attributes_for_oid(x509.NameOID.COMMON_NAME)
        return attrs[0].value if attrs else cert.subject.rfc4514_string()
    except Exception:
        return cert.subject.rfc4514_string()


def _is_self_signed(cert: x509.Certificate) -> bool:
    """A root CA is its own issuer. (verify_chain._split_roots uses the same test.)"""
    return cert.subject == cert.issuer


def _bundle_bytes(certs: List[x509.Certificate]) -> bytes:
    """Serialize a cert list back to a concatenated PEM bundle (the on-disk form)."""
    return b"".join(_cert_pem(c) for c in certs)


@dataclass
class CertInfo:
    index: int
    role: str  # "root" | "intermediate/leaf"
    subject_cn: str
    issuer_cn: str
    fingerprint: str
    not_after: str

    def describe(self) -> str:
        return (
            f"  [{self.index}] {self.role:<18} CN={self.subject_cn!r} "
            f"issuer={self.issuer_cn!r} fp={self.fingerprint[:16]}… "
            f"expires={self.not_after}"
        )


def describe_bundle(certs: List[x509.Certificate]) -> List[CertInfo]:
    out: List[CertInfo] = []
    for i, c in enumerate(certs):
        role = "root" if _is_self_signed(c) else "intermediate/leaf"
        try:
            issuer_cn_attrs = c.issuer.get_attributes_for_oid(x509.NameOID.COMMON_NAME)
            issuer_cn = issuer_cn_attrs[0].value if issuer_cn_attrs else c.issuer.rfc4514_string()
        except Exception:
            issuer_cn = c.issuer.rfc4514_string()
        out.append(
            CertInfo(
                index=i,
                role=role,
                subject_cn=_subject_cn(c),
                issuer_cn=issuer_cn,
                fingerprint=_fingerprint(c),
                not_after=c.not_valid_after_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
            )
        )
    return out


# ---------------------------------------------------------------------------
# Atomic write + backup
# ---------------------------------------------------------------------------

def _atomic_write(path: str, data: bytes) -> None:
    d = os.path.dirname(os.path.abspath(path)) or "."
    os.makedirs(d, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=d, prefix=os.path.basename(path) + ".", suffix=".tmp")
    try:
        with os.fdopen(fd, "wb") as fh:
            fh.write(data)
        os.replace(tmp, path)
    except Exception:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


def _backup(path: str) -> Optional[str]:
    if not os.path.isfile(path):
        return None
    ts = _dt.datetime.now(_dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    bak = f"{path}.bak.{ts}"
    shutil.copy2(path, bak)
    return bak


# ---------------------------------------------------------------------------
# Core operations
# ---------------------------------------------------------------------------

@dataclass
class OverlapResult:
    ok: bool
    reason: str
    added: int = 0
    removed: int = 0
    total: int = 0
    backup: Optional[str] = None


def add_ca(bundle_path: str, new_ca_path: str, *, sign: bool = False,
           key_dir: Optional[str] = None) -> OverlapResult:
    """Append every cert in ``new_ca_path`` to ``bundle_path`` (idempotent).

    This is the **trust-overlap** step: after this returns ok, the bundle trusts
    BOTH the certs it already held and the new CA. Skips any cert already present
    (matched by SHA-256 fingerprint), so running it twice is a no-op. Re-reloading
    the auth-proxy against the resulting bundle is what makes both CAs live —
    callers MUST reload (rolling) before relying on the new trust.
    """
    existing = _read_certs(bundle_path) if os.path.isfile(bundle_path) else []
    incoming = _read_certs(new_ca_path)
    if not incoming:
        return OverlapResult(False, f"{new_ca_path} contained no certificates")

    have = {_fingerprint(c) for c in existing}
    added: List[x509.Certificate] = []
    for c in incoming:
        fp = _fingerprint(c)
        if fp in have:
            continue
        have.add(fp)
        added.append(c)

    merged = existing + added
    if not added:
        return OverlapResult(
            True, "no-op: every cert in the new CA is already trusted (idempotent)",
            added=0, total=len(merged),
        )

    bak = _backup(bundle_path)
    _atomic_write(bundle_path, _bundle_bytes(merged))
    if sign:
        _sign_bundle(bundle_path, key_dir=key_dir)
    return OverlapResult(
        True,
        f"added {len(added)} cert(s); bundle now trusts {len(merged)} cert(s) "
        f"(OVERLAP: old + new CA both trusted)",
        added=len(added), total=len(merged), backup=bak,
    )


def remove_ca(bundle_path: str, *, match_root_cn: Optional[str] = None,
              match_fingerprint: Optional[str] = None, sign: bool = False,
              key_dir: Optional[str] = None) -> OverlapResult:
    """Remove a CA from the bundle — the FINAL step of a rotation (old CA retired).

    Selects the root to drop by ``match_root_cn`` (its subject CN) or by exact
    ``match_fingerprint``. Removes that root AND any intermediate it issued, so the
    whole old CA leaves the trust set together. REFUSES to empty the bundle (you may
    never leave the proxy with zero trust anchors). Run this ONLY after every active
    agent has rotated to a new-CA leaf — otherwise you cut off in-flight agents.
    """
    certs = _read_certs(bundle_path)
    if not certs:
        return OverlapResult(False, "bundle is empty — nothing to remove")

    roots = [c for c in certs if _is_self_signed(c)]
    target_roots: List[x509.Certificate] = []
    if match_fingerprint:
        mfp = match_fingerprint.lower()
        target_roots = [c for c in roots if _fingerprint(c) == mfp]
    elif match_root_cn:
        target_roots = [c for c in roots if _subject_cn(c) == match_root_cn]
    else:
        return OverlapResult(False, "specify --match-root-cn or --match-fingerprint")

    if not target_roots:
        return OverlapResult(
            False,
            f"no root matched (cn={match_root_cn!r} fp={match_fingerprint!r}); "
            f"bundle unchanged",
        )

    # Build the set of (root + the intermediates that chain to it) to drop.
    drop_subjects = {c.subject for c in target_roots}
    keep: List[x509.Certificate] = []
    removed = 0
    for c in certs:
        if c in target_roots:
            removed += 1
            continue
        # Drop an intermediate whose issuer is one of the targeted roots.
        if not _is_self_signed(c) and c.issuer in drop_subjects:
            removed += 1
            continue
        keep.append(c)

    if not keep:
        return OverlapResult(
            False,
            "refusing to remove: that would leave the trust bundle EMPTY (the "
            "auth-proxy would trust no client cert at all). Add the replacement CA "
            "first.",
        )

    bak = _backup(bundle_path)
    _atomic_write(bundle_path, _bundle_bytes(keep))
    if sign:
        _sign_bundle(bundle_path, key_dir=key_dir)
    return OverlapResult(
        True,
        f"removed {removed} cert(s) for the retired CA; bundle now trusts "
        f"{len(keep)} cert(s)",
        removed=removed, total=len(keep), backup=bak,
    )


def verify_bundle(bundle_path: str, *, leaf_path: Optional[str] = None,
                  require_signature: bool = False,
                  key_dir: Optional[str] = None) -> OverlapResult:
    """Sanity-check a trust bundle before the proxy loads it.

    * Parses every cert (a malformed bundle is rejected here, not at proxy boot).
    * Confirms at least one self-signed ROOT exists (the proxy needs ≥1 anchor).
    * If ``leaf_path`` is given, verifies that leaf chains to the bundle via the
      committed ``ca/verify_chain.py`` (the SAME verifier the proxy's resolver uses)
      — this is the concrete proof that "an old-CA agent still verifies" during the
      overlap window.
    * If ``require_signature``, also verifies the bundle's ed25519 detached sig +
      manifest against the keyring (I6).
    """
    try:
        certs = _read_certs(bundle_path)
    except Exception as exc:
        return OverlapResult(False, f"bundle does not parse: {exc}")
    if not certs:
        return OverlapResult(False, "bundle is empty (no trust anchors)")
    roots = [c for c in certs if _is_self_signed(c)]
    if not roots:
        return OverlapResult(
            False, "bundle has NO self-signed root — the proxy would trust nothing"
        )

    if require_signature:
        sig_res = _verify_bundle_signature(bundle_path, key_dir=key_dir)
        if not sig_res.ok:
            return sig_res

    if leaf_path:
        res = vc.verify_chain(open(leaf_path, "rb").read(), _bundle_bytes(certs))
        if not res:
            return OverlapResult(
                False,
                f"leaf {leaf_path} does NOT chain to this bundle: {res.reason}",
                total=len(certs),
            )
        return OverlapResult(
            True,
            f"OK: {len(certs)} cert(s), {len(roots)} root(s); leaf {os.path.basename(leaf_path)} "
            f"VERIFIES against the bundle",
            total=len(certs),
        )

    return OverlapResult(
        True,
        f"OK: bundle parses, {len(certs)} cert(s), {len(roots)} trust anchor(s)",
        total=len(certs),
    )


# ---------------------------------------------------------------------------
# Signing integration (I6) — reuse control-plane/signing for ALL signing/verify
# ---------------------------------------------------------------------------

def _default_key_dir(key_dir: Optional[str]) -> str:
    return key_dir or os.path.join(_CP_ROOT, "signing", "keys")


def _load_cp_keyring(key_dir: Optional[str]):
    """Load the control-plane keyring (with private material) for signing.

    Mirrors signing/sign_bundle.py: a trust_root.json (public) + per-key private
    PEMs under signing/keys/<key_id>.key. We load private keys for the active id.
    """
    import json
    sd = os.path.join(_CP_ROOT, "signing")
    tr_path = os.path.join(sd, "trust_root.json")
    if not os.path.isfile(tr_path):
        raise FileNotFoundError(
            f"no keyring trust root at {tr_path}; run signing/keygen.py first"
        )
    doc = json.load(open(tr_path, "r", encoding="utf-8"))
    ring = sl.Keyring.from_trust_root(doc)
    # attach private material for any key whose <key_id>.key PEM is on disk.
    kd = _default_key_dir(key_dir)
    for kid in ring.key_ids():
        pem_path = os.path.join(kd, f"{kid}.key")
        if os.path.isfile(pem_path):
            priv = sl.load_private_key_pem(open(pem_path, "rb").read())
            entry = ring.get(kid)
            entry.private = priv
    return ring


def _sign_bundle(bundle_path: str, *, key_dir: Optional[str] = None,
                 version: str = "trust-bundle") -> None:
    """Produce <bundle>.sig + <bundle>.manifest.json over the bundle bytes (C2)."""
    if not _HAVE_SIGNING:
        raise RuntimeError("signing libs unavailable; cannot --sign")
    ring = _load_cp_keyring(key_dir)
    raw = open(bundle_path, "rb").read()
    key_id = ring.active_key_id
    # I6: sign the canonical manifest binding, not the raw bundle bytes, so the manifest
    # fields (artifact/version/key_id) can't be relabeled while the signature still verifies.
    payload = sl.signed_payload_for(
        artifact_kind="config", version=version, key_id=key_id, signed_bytes=raw
    )
    _, sig = ring.sign_with_active(payload)
    manifest = sl.build_manifest(
        artifact_kind="config", version=version, signed_bytes=raw, key_id=key_id
    )
    _atomic_write(bundle_path + ".sig", sl.b64e(sig).encode("ascii"))
    import json
    _atomic_write(
        bundle_path + ".manifest.json",
        json.dumps(manifest, indent=2, sort_keys=True).encode("utf-8"),
    )


def _verify_bundle_signature(bundle_path: str, *,
                             key_dir: Optional[str] = None) -> OverlapResult:
    if not _HAVE_SIGNING:
        return OverlapResult(False, "signing libs unavailable; cannot verify signature")
    sig_path = bundle_path + ".sig"
    man_path = bundle_path + ".manifest.json"
    if not (os.path.isfile(sig_path) and os.path.isfile(man_path)):
        return OverlapResult(
            False,
            f"missing {os.path.basename(sig_path)} / "
            f"{os.path.basename(man_path)} — bundle is unsigned (run `sign` first)",
        )
    import json
    sd = os.path.join(_CP_ROOT, "signing")
    doc = json.load(open(os.path.join(sd, "trust_root.json"), "r", encoding="utf-8"))
    ring = sl.Keyring.from_trust_root(doc)
    manifest = json.load(open(man_path, "r", encoding="utf-8"))
    raw = open(bundle_path, "rb").read()
    sig = sl.b64d(open(sig_path, "r", encoding="utf-8").read().strip())
    kid = manifest.get("key_id")
    # I6: reconstruct the signed payload per the manifest binding version so a relabeled
    # manifest (swapped artifact/version/key_id) fails the cryptographic check below.
    digest = sl.sha256_hex(raw)
    sig_binding = manifest.get("sig_binding", sl.SIG_BINDING_V1)
    if sig_binding == sl.SIG_BINDING_V2:
        payload = sl.signing_payload_from_manifest(manifest, digest)
    elif sig_binding == sl.SIG_BINDING_V1:
        payload = raw  # legacy unbound signature (back-compat)
    else:
        return OverlapResult(
            False, f"unknown manifest sig_binding {sig_binding!r} (key_id={kid}) — do NOT load"
        )
    if not ring.verify_with(kid, payload, sig):
        return OverlapResult(
            False, f"ed25519 signature INVALID for bundle (key_id={kid}) — do NOT load"
        )
    if manifest.get("sha256") and manifest["sha256"] != digest:
        return OverlapResult(
            False, f"bundle sha256 mismatch (key_id={kid}) — do NOT load"
        )
    return OverlapResult(True, f"bundle signature OK (key_id={kid})")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _print(res: OverlapResult, ok_prefix: str = "OK") -> int:
    if res.ok:
        print(f"{ok_prefix}: {res.reason}")
        if res.backup:
            print(f"  backup: {res.backup}")
        return 0
    print(f"FAILED: {res.reason}", file=sys.stderr)
    return 1


def main(argv: Optional[List[str]] = None) -> int:
    ap = argparse.ArgumentParser(
        description="CA trust-overlap helper for non-disruptive control-plane upgrades."
    )
    sub = ap.add_subparsers(dest="cmd", required=True)

    p_add = sub.add_parser("add", help="append a new CA to the trust bundle (overlap)")
    p_add.add_argument("bundle", help="path to the proxy trust bundle (ca-chain.crt)")
    p_add.add_argument("--add-ca", required=True, help="PEM of the new CA (intermediate+root)")
    p_add.add_argument("--sign", action="store_true", help="re-sign the bundle (C2/I6)")
    p_add.add_argument("--key-dir", default=None, help="signing private-key dir")

    p_rm = sub.add_parser("remove", help="remove a retired CA from the trust bundle")
    p_rm.add_argument("bundle")
    p_rm.add_argument("--match-root-cn", default=None, help="root subject CN to drop")
    p_rm.add_argument("--match-fingerprint", default=None, help="root SHA-256 fp to drop")
    p_rm.add_argument("--sign", action="store_true")
    p_rm.add_argument("--key-dir", default=None)

    p_ls = sub.add_parser("list", help="list every trust anchor in the bundle")
    p_ls.add_argument("bundle")

    p_vf = sub.add_parser("verify", help="parse the bundle + optionally check a leaf chains to it")
    p_vf.add_argument("bundle")
    p_vf.add_argument("--leaf", default=None, help="a leaf cert that should chain to the bundle")
    p_vf.add_argument("--require-signature", action="store_true",
                      help="also verify the bundle's ed25519 signature (I6)")
    p_vf.add_argument("--key-dir", default=None)

    p_sg = sub.add_parser("sign", help="(re)sign the bundle with the active CP key (C2/I6)")
    p_sg.add_argument("bundle")
    p_sg.add_argument("--key-dir", default=None)

    args = ap.parse_args(argv)

    if args.cmd == "add":
        return _print(add_ca(args.bundle, args.add_ca, sign=args.sign, key_dir=args.key_dir))
    if args.cmd == "remove":
        return _print(remove_ca(
            args.bundle, match_root_cn=args.match_root_cn,
            match_fingerprint=args.match_fingerprint, sign=args.sign, key_dir=args.key_dir,
        ))
    if args.cmd == "list":
        try:
            certs = _read_certs(args.bundle)
        except Exception as exc:
            print(f"FAILED: {exc}", file=sys.stderr)
            return 1
        if not certs:
            print("(empty bundle — no trust anchors)")
            return 0
        print(f"trust bundle {args.bundle}: {len(certs)} cert(s)")
        for info in describe_bundle(certs):
            print(info.describe())
        return 0
    if args.cmd == "verify":
        return _print(verify_bundle(
            args.bundle, leaf_path=args.leaf,
            require_signature=args.require_signature, key_dir=args.key_dir,
        ))
    if args.cmd == "sign":
        try:
            _sign_bundle(args.bundle, key_dir=args.key_dir)
        except Exception as exc:
            print(f"FAILED: {exc}", file=sys.stderr)
            return 1
        print(f"OK: signed {args.bundle} (.sig + .manifest.json written)")
        return 0
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
