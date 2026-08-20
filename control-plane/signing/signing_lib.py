"""signing_lib — ed25519 signing primitives + a rotating Keyring for the Fyralis BYOC control plane.

Implements contract C2 / invariant I6 from ``control-plane/SPRINT_PLAN.md``:

    "Everything the control plane ships to a data plane is signed: release tarballs,
     license JSON, and config JSON. Signing uses ed25519 with a detached signature
     plus a manifest. A keyring maps key_id -> public_key (and, on the CP side only,
     the private key) to support rotation by key id. Agents ship with the keyring's
     public keys and VERIFY before apply."

This module is intentionally self-contained (no dependency on the sibling ``lib/``
package, which is owned by a parallel build agent) so it can be imported and tested
in isolation. It uses ``cryptography`` (Ed25519PrivateKey / Ed25519PublicKey).

Wire formats
------------
* **Private key** on disk: PKCS#8 PEM (``-----BEGIN PRIVATE KEY-----``), gitignored.
* **Public key** in the trust root: raw 32-byte ed25519 public key, base64 (std, padded).
* **Detached signature**: raw 64-byte ed25519 signature, base64 (std, padded) when
  written to ``<file>.sig``; raw bytes in-process.

Everything that crosses a trust boundary (trust_root.json, manifests, .sig files) is
text/base64 so it survives JSON and git cleanly.
"""

from __future__ import annotations

import base64
import datetime as _dt
import hashlib
import json
import os
from dataclasses import dataclass
from typing import Dict, Optional

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import (
    Ed25519PrivateKey,
    Ed25519PublicKey,
)

ALGO = "ed25519"

# Manifest binding schema (I6). ``v1`` = legacy: ed25519 signature covered ONLY the
# canonical artifact bytes, so a signed bundle could be RELABELED (manifest version /
# artifact-kind swapped) while the signature still verified. ``v2`` binds the manifest
# identity fields into the signed payload: we sign a canonical binding over
# {binding, algo, artifact, version, key_id, artifact_sha256} instead of the raw bytes,
# so any manifest relabel changes the signed payload and fails verification.
SIG_BINDING_V1 = "v1"  # legacy: signature over canonical artifact bytes only
SIG_BINDING_V2 = "v2"  # bound: signature over canonical {manifest-fields + artifact_sha256}
SIG_BINDING_CURRENT = SIG_BINDING_V2

# --------------------------------------------------------------------------- #
# Small shared primitives (kept local so signing_lib has no cross-dir imports) #
# --------------------------------------------------------------------------- #


def now_rfc3339() -> str:
    """Current time as an RFC-3339 UTC timestamp, e.g. ``2026-06-24T00:00:00Z``.

    Matches the ``signed_at`` / ``created`` / ``issued_at`` format used across the
    control plane contracts (C2, C4).
    """
    return (
        _dt.datetime.now(_dt.timezone.utc)
        .replace(microsecond=0)
        .strftime("%Y-%m-%dT%H:%M:%SZ")
    )


def sha256_hex(data: bytes) -> str:
    """Lowercase hex SHA-256 of ``data`` (the redundant integrity check in the manifest)."""
    return hashlib.sha256(data).hexdigest()


def canonical_json_bytes(obj) -> bytes:
    """Deterministic compact UTF-8 JSON bytes.

    Used for the "canonical signed bytes" of license/config artifacts (C2): sorted
    keys, no insignificant whitespace, ``ensure_ascii=False`` so the byte stream is
    stable and minimal. Signing JSON through this guarantees the signer and verifier
    hash the *same* bytes regardless of key ordering in the source file.
    """
    return json.dumps(
        obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode("utf-8")


def b64e(raw: bytes) -> str:
    return base64.b64encode(raw).decode("ascii")


def b64d(text: str) -> bytes:
    return base64.b64decode(text.encode("ascii"))


# --------------------------------------------------------------------------- #
# Core ed25519 keygen / sign / verify                                         #
# --------------------------------------------------------------------------- #


def generate_keypair() -> tuple[Ed25519PrivateKey, Ed25519PublicKey]:
    """Generate a fresh ed25519 keypair."""
    priv = Ed25519PrivateKey.generate()
    return priv, priv.public_key()


def sign(data: bytes, priv: Ed25519PrivateKey) -> bytes:
    """Sign ``data`` with ``priv``; returns the raw 64-byte ed25519 signature."""
    if not isinstance(data, (bytes, bytearray)):
        raise TypeError("sign() data must be bytes")
    return priv.sign(bytes(data))


def verify(data: bytes, sig: bytes, pub: Ed25519PublicKey) -> bool:
    """Verify ``sig`` over ``data`` against ``pub``.

    Returns ``True`` on a valid signature, ``False`` on any tamper/mismatch. Never
    raises for a bad signature — callers branch on the bool. (Programming errors such
    as a non-bytes argument still raise, by design.)
    """
    if not isinstance(data, (bytes, bytearray)):
        raise TypeError("verify() data must be bytes")
    try:
        pub.verify(bytes(sig), bytes(data))
        return True
    except InvalidSignature:
        return False


# --------------------------------------------------------------------------- #
# Key (de)serialization helpers                                               #
# --------------------------------------------------------------------------- #


def private_key_to_pem(priv: Ed25519PrivateKey) -> bytes:
    """Serialize a private key to unencrypted PKCS#8 PEM bytes (gitignored on disk)."""
    return priv.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )


def load_private_key_pem(pem: bytes) -> Ed25519PrivateKey:
    key = serialization.load_pem_private_key(pem, password=None)
    if not isinstance(key, Ed25519PrivateKey):
        raise ValueError("PEM did not contain an ed25519 private key")
    return key


def public_key_to_b64(pub: Ed25519PublicKey) -> str:
    """Serialize a public key to base64 of its raw 32 bytes (the trust_root.json form)."""
    raw = pub.public_bytes(
        encoding=serialization.Encoding.Raw,
        format=serialization.PublicFormat.Raw,
    )
    return b64e(raw)


def public_key_from_b64(b64: str) -> Ed25519PublicKey:
    return Ed25519PublicKey.from_public_bytes(b64d(b64))


# --------------------------------------------------------------------------- #
# Keyring: active signing key + retained verifiers, rotation by key_id        #
# --------------------------------------------------------------------------- #


@dataclass
class KeyEntry:
    """One key in the ring.

    ``private`` is present only on the control-plane side; agents/verifiers load a
    public-only ring from ``trust_root.json``. ``status`` is ``active`` (the current
    signing key) or ``retired`` (kept so old signatures still verify, never used to
    sign new artifacts).
    """

    key_id: str
    public: Ed25519PublicKey
    private: Optional[Ed25519PrivateKey] = None
    status: str = "active"  # "active" | "retired"

    @property
    def can_sign(self) -> bool:
        return self.private is not None and self.status == "active"


class Keyring:
    """Holds multiple ed25519 keys by ``key_id`` and supports rotation.

    Invariants:
      * At most one entry has ``status == "active"`` (the signing key).
      * Retired entries keep their public key so previously-signed artifacts still
        verify — that is what makes rotation non-breaking (C2 / I6).
      * A verifier-only ring (loaded from ``trust_root.json``) has no private keys; it
        can ``verify_*`` but not ``sign_*``.
    """

    def __init__(self) -> None:
        self._keys: Dict[str, KeyEntry] = {}
        self._active_id: Optional[str] = None

    # -- construction ------------------------------------------------------- #

    def add_key(
        self,
        key_id: str,
        public: Ed25519PublicKey,
        private: Optional[Ed25519PrivateKey] = None,
        status: str = "active",
        make_active: bool = False,
    ) -> KeyEntry:
        """Add a key to the ring.

        If ``make_active`` (or ``status == 'active'``), this becomes the signing key
        and any previously-active key is retired (its public key is retained).
        """
        if key_id in self._keys:
            raise ValueError(f"key_id already in ring: {key_id}")
        if status not in ("active", "retired"):
            raise ValueError(f"invalid status: {status}")
        entry = KeyEntry(key_id=key_id, public=public, private=private, status=status)
        self._keys[key_id] = entry
        if make_active or status == "active":
            self._promote(key_id)
        return entry

    def generate_active_key(self, key_id: str) -> KeyEntry:
        """Generate a brand-new keypair, add it, and make it the active signer."""
        priv, pub = generate_keypair()
        return self.add_key(key_id, public=pub, private=priv, make_active=True)

    def _promote(self, key_id: str) -> None:
        """Make ``key_id`` the sole active key; retire the previous active key."""
        if self._active_id and self._active_id != key_id:
            self._keys[self._active_id].status = "retired"
        self._keys[key_id].status = "active"
        self._active_id = key_id

    # -- rotation ----------------------------------------------------------- #

    def rotate_to(
        self,
        new_key_id: str,
        private: Optional[Ed25519PrivateKey] = None,
        public: Optional[Ed25519PublicKey] = None,
    ) -> KeyEntry:
        """Rotate signing to ``new_key_id``.

        The previously-active key is retired (its public key retained so its old
        signatures keep verifying). If no key material is supplied a fresh keypair is
        generated. Returns the new active entry.
        """
        if new_key_id in self._keys:
            # Key already known — just promote it.
            self._promote(new_key_id)
            return self._keys[new_key_id]
        if private is not None:
            pub = private.public_key()
            return self.add_key(new_key_id, public=pub, private=private, make_active=True)
        if public is not None:
            return self.add_key(new_key_id, public=public, make_active=True)
        return self.generate_active_key(new_key_id)

    def retire(self, key_id: str) -> None:
        """Force a key to ``retired`` (it can still verify, never signs)."""
        self._keys[key_id].status = "retired"
        if self._active_id == key_id:
            self._active_id = None

    # -- accessors ---------------------------------------------------------- #

    @property
    def active_key_id(self) -> str:
        if not self._active_id:
            raise RuntimeError("keyring has no active signing key")
        return self._active_id

    def active_entry(self) -> KeyEntry:
        return self._keys[self.active_key_id]

    def get(self, key_id: str) -> Optional[KeyEntry]:
        return self._keys.get(key_id)

    def key_ids(self) -> list[str]:
        return list(self._keys.keys())

    def __contains__(self, key_id: str) -> bool:  # pragma: no cover - trivial
        return key_id in self._keys

    # -- signing / verifying through the ring ------------------------------- #

    def sign_with_active(self, data: bytes) -> tuple[str, bytes]:
        """Sign ``data`` with the active key. Returns ``(key_id, raw_signature)``."""
        entry = self.active_entry()
        if not entry.can_sign:
            raise RuntimeError(
                f"active key {entry.key_id!r} has no private material (verifier-only ring)"
            )
        return entry.key_id, sign(data, entry.private)

    def verify_with(self, key_id: str, data: bytes, sig: bytes) -> bool:
        """Verify ``sig`` over ``data`` using the public key registered for ``key_id``.

        Returns ``False`` if ``key_id`` is unknown (an unknown/retired-and-removed key
        id must never verify — C2: "an artifact whose key_id is unknown/retired ... is
        never applied").
        """
        entry = self._keys.get(key_id)
        if entry is None:
            return False
        return verify(data, sig, entry.public)

    # -- trust-root (de)serialization --------------------------------------- #

    def to_trust_root(self) -> dict:
        """Export the **public** trust root: ``{key_id: {pubkey, status, algo}}``.

        Private material is never exported. This is what agents ship and what
        ``trust_root.json`` holds.
        """
        keys = {
            kid: {
                "pubkey": public_key_to_b64(e.public),
                "algo": ALGO,
                "status": e.status,
            }
            for kid, e in self._keys.items()
        }
        return {
            "version": 1,
            "active_key_id": self._active_id,
            "keys": keys,
        }

    @classmethod
    def from_trust_root(cls, doc: dict) -> "Keyring":
        """Build a **verifier-only** ring from a ``trust_root.json`` document.

        No private keys are loaded; the ring can verify but not sign. The active key id
        is restored so callers can introspect it, but signing will raise.
        """
        ring = cls()
        keys = doc.get("keys", {})
        for kid, meta in keys.items():
            ring._keys[kid] = KeyEntry(
                key_id=kid,
                public=public_key_from_b64(meta["pubkey"]),
                private=None,
                status=meta.get("status", "active"),
            )
        ring._active_id = doc.get("active_key_id")
        return ring


# --------------------------------------------------------------------------- #
# Manifest + detached-signature artifact helpers (C2 manifest shape)          #
# --------------------------------------------------------------------------- #

# Map a known artifact filename/extension hint -> the C2 "artifact" enum value.
_ARTIFACT_BY_HINT = {
    "release": "release",
    "license": "license",
    "config": "config",
}


def infer_artifact_kind(path: str) -> str:
    """Best-effort classify a file into the C2 ``artifact`` enum (release|license|config).

    Heuristic only; ``sign_bundle`` accepts an explicit ``--kind`` override. Defaults to
    ``release`` for tarball-like names, else inspects the filename stem.
    """
    name = os.path.basename(path).lower()
    if name.endswith((".tar.gz", ".tgz", ".tar", ".zip")):
        return "release"
    for hint, kind in _ARTIFACT_BY_HINT.items():
        if hint in name:
            return kind
    return "release"


def canonical_bytes_for_file(path: str, kind: str) -> bytes:
    """Return the **canonical signed bytes** for a file per C2.

    * For ``license`` / ``config`` JSON artifacts: the compact-canonical UTF-8 JSON of
      the parsed document (order-independent). If the file is not valid JSON we fall
      back to raw bytes (still signed, just not re-canonicalized).
    * For everything else (release tarballs, opaque blobs): the exact file bytes.
    """
    raw = open(path, "rb").read()
    if kind in ("license", "config"):
        try:
            return canonical_json_bytes(json.loads(raw.decode("utf-8")))
        except (ValueError, UnicodeDecodeError):
            # Not JSON (or not decodable) — sign the raw bytes as-is.
            return raw
    return raw


def signing_payload(
    *, artifact_kind: str, version: str, key_id: str, artifact_sha256: str
) -> bytes:
    """Return the **canonical binding bytes** that the ed25519 signature covers (I6, v2).

    The signed quantity is no longer the raw artifact bytes; it is a deterministic,
    order-independent JSON binding of the artifact's *identity*:

        {binding, algo, artifact, version, key_id, artifact_sha256}

    where ``artifact_sha256`` is the sha256 of the canonical artifact bytes (so the
    artifact content is still cryptographically bound, indirectly via its digest). Signing
    this binding means a verifier who recomputes it from (artifact + manifest) detects:

      * artifact tamper      -> ``artifact_sha256`` changes -> binding changes -> REJECT
      * version relabel       -> ``version`` changes        -> binding changes -> REJECT
      * artifact-kind relabel -> ``artifact`` changes       -> binding changes -> REJECT
      * key_id relabel        -> ``key_id`` changes         -> binding changes -> REJECT

    ``artifact_kind``/``version``/``key_id`` are stringified so the binding is stable
    regardless of how the manifest happened to type them.
    """
    return canonical_json_bytes(
        {
            "binding": SIG_BINDING_V2,
            "algo": ALGO,
            "artifact": str(artifact_kind),
            "version": str(version),
            "key_id": str(key_id),
            "artifact_sha256": str(artifact_sha256),
        }
    )


def signing_payload_from_manifest(manifest: dict, artifact_sha256: str) -> bytes:
    """Recompute the v2 signing binding from a parsed ``manifest`` + the artifact digest.

    The verifier calls this with the freshly-recomputed ``artifact_sha256`` of the
    canonical artifact bytes so that a relabeled manifest yields a different binding than
    what was actually signed.
    """
    return signing_payload(
        artifact_kind=manifest.get("artifact"),
        version=manifest.get("version"),
        key_id=manifest.get("key_id"),
        artifact_sha256=artifact_sha256,
    )


def build_manifest(
    *, artifact_kind: str, version: str, signed_bytes: bytes, key_id: str
) -> dict:
    """Construct the C2 manifest dict for ``signed_bytes``.

    Records ``sig_binding: "v2"`` so verifiers know the ed25519 signature covers the
    canonical *binding* (manifest identity fields + artifact sha256), not just the raw
    artifact bytes. ``sha256`` remains the digest of the canonical artifact bytes (the
    redundant integrity check AND the value that feeds the signed binding)."""
    return {
        "artifact": artifact_kind,
        "version": version,
        "sha256": sha256_hex(signed_bytes),
        "key_id": key_id,
        "algo": ALGO,
        "sig_binding": SIG_BINDING_V2,
        "signed_at": now_rfc3339(),
    }


def signed_payload_for(
    *,
    artifact_kind: str,
    version: str,
    key_id: str,
    signed_bytes: bytes,
) -> bytes:
    """Convenience for signers: the exact bytes to hand to :func:`sign` for a v2 bundle.

    Equivalent to ``signing_payload(..., artifact_sha256=sha256_hex(signed_bytes))``.
    Centralizes the "sign the binding, not the raw bytes" rule so every signing call
    site (sign_bundle, release, upgrade trust-bundle) stays consistent.
    """
    return signing_payload(
        artifact_kind=artifact_kind,
        version=version,
        key_id=key_id,
        artifact_sha256=sha256_hex(signed_bytes),
    )


__all__ = [
    "ALGO",
    "SIG_BINDING_V1",
    "SIG_BINDING_V2",
    "SIG_BINDING_CURRENT",
    "signing_payload",
    "signing_payload_from_manifest",
    "signed_payload_for",
    "now_rfc3339",
    "sha256_hex",
    "canonical_json_bytes",
    "b64e",
    "b64d",
    "generate_keypair",
    "sign",
    "verify",
    "private_key_to_pem",
    "load_private_key_pem",
    "public_key_to_b64",
    "public_key_from_b64",
    "KeyEntry",
    "Keyring",
    "infer_artifact_kind",
    "canonical_bytes_for_file",
    "build_manifest",
]
