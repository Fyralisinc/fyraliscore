"""signing_ctx — a thin, *configurable* wrapper over the committed signing module.

WS-RELEASE must **sign every release** (C2 / I6) and **verify before apply**. The
canonical implementation lives in ``control-plane/signing`` (``sign_bundle`` +
``verify_bundle`` + ``signing_lib``) and we reuse it verbatim — this module never
re-implements ed25519, the manifest shape, or the canonical-bytes rule.

Why a wrapper at all
--------------------
``sign_bundle`` resolves its trust root and private keys from *fixed* module-level
paths (``signing/trust_root.json`` and ``signing/keys/``). The release builder and
its self-test need to sign against a **chosen** trust root:

* In production the CP signer points at the real ``signing/`` directory (a key was
  minted there by ``signing/keygen.py``) — :func:`SigningContext.for_control_plane`.
* The self-test (and any hermetic CI signer that owns its own key store) points at a
  throwaway trust root + keys dir it minted itself — :func:`SigningContext.ephemeral`
  / :func:`SigningContext.for_dir` — *without writing into the committed ``signing/``
  tree* (we are write-disjoint).

In both cases the actual crypto goes through ``signing_lib`` exactly as
``sign_bundle.sign_file`` does, and verification goes through
``verify_bundle.verify_file`` (the very function the agent's ``config_pull`` calls),
so a bundle this module signs is byte-for-byte what the agent will accept.

Layout of a signing context (matches ``signing/``)
--------------------------------------------------
    <root>/trust_root.json            # { version, active_key_id, keys: {kid: {pubkey, algo, status}} }
    <root>/keys/<key_id>.private.pem  # PKCS#8 PEM, 0600 (CP side only)
    <root>/keys/<key_id>.public.b64   # convenience sidecar
"""

from __future__ import annotations

import json
import os
import stat
from dataclasses import dataclass
from pathlib import Path

import _bootstrap  # noqa: F401  (side-effect: sys.path for lib + signing)

import signing_lib as sl  # noqa: E402  (from control-plane/signing)
import verify_bundle as vb  # noqa: E402

__all__ = ["SigningContext", "SignedArtifact"]

# The real, committed control-plane signing dir (the default production signer).
_CP_SIGNING_DIR = _bootstrap.SIGNING_DIR


@dataclass(frozen=True)
class SignedArtifact:
    """The trio produced by signing ``path`` (paths to the sidecars + the manifest)."""

    artifact_path: str
    sig_path: str
    manifest_path: str
    manifest: dict

    @property
    def key_id(self) -> str:
        return self.manifest["key_id"]

    @property
    def version(self) -> str:
        return self.manifest["version"]

    @property
    def sha256(self) -> str:
        return self.manifest["sha256"]


class SigningContext:
    """Sign / verify against a *chosen* trust root + key store.

    ``root`` is a directory laid out like ``control-plane/signing`` (a
    ``trust_root.json`` plus a ``keys/`` dir of PKCS#8 PEM private keys). The
    production path uses the committed ``signing/`` dir; the self-test mints its own.
    """

    def __init__(self, root: "str | Path") -> None:
        self.root = Path(root)
        self.keys_dir = self.root / "keys"
        self.trust_root_path = self.root / "trust_root.json"

    # -- constructors ------------------------------------------------------- #

    @classmethod
    def for_control_plane(cls) -> "SigningContext":
        """The production signer: the committed ``control-plane/signing`` directory.

        Requires that ``signing/keygen.py`` has already minted a key (so a private
        PEM + ``trust_root.json`` exist). Raises a clear error otherwise.
        """
        ctx = cls(_CP_SIGNING_DIR)
        if not ctx.trust_root_path.is_file():
            raise FileNotFoundError(
                f"no trust root at {ctx.trust_root_path}; run "
                "`python control-plane/signing/keygen.py --activate` to mint a CP signing key"
            )
        return ctx

    @classmethod
    def for_dir(cls, root: "str | Path") -> "SigningContext":
        """A signer/verifier over an arbitrary ``root`` directory."""
        return cls(root)

    @classmethod
    def ephemeral(cls, root: "str | Path", *, key_id: str = "cp-signing-selftest") -> "SigningContext":
        """Create ``root`` and mint a fresh active signing key inside it.

        Used by the self-test and by any hermetic CI signer that owns its own key
        store. Reuses ``signing_lib`` for keygen + the public trust-root export, so
        the resulting context is indistinguishable (to the verifier) from one made
        by ``signing/keygen.py``.
        """
        ctx = cls(root)
        ctx.keys_dir.mkdir(parents=True, exist_ok=True)
        ctx.mint_active_key(key_id)
        return ctx

    # -- trust-root I/O ----------------------------------------------------- #

    def load_trust_root(self) -> dict:
        if not self.trust_root_path.is_file():
            return {"version": 1, "active_key_id": None, "keys": {}}
        with open(self.trust_root_path, "r", encoding="utf-8") as fh:
            return json.load(fh)

    def _write_trust_root(self, doc: dict) -> None:
        self.root.mkdir(parents=True, exist_ok=True)
        with open(self.trust_root_path, "w", encoding="utf-8") as fh:
            json.dump(doc, fh, indent=2, sort_keys=True)
            fh.write("\n")

    @property
    def active_key_id(self) -> str:
        kid = self.load_trust_root().get("active_key_id")
        if not kid:
            raise RuntimeError(
                f"trust root {self.trust_root_path} has no active_key_id; mint a key first"
            )
        return kid

    # -- key minting (only used for non-CP contexts; mirrors keygen.py) ----- #

    def mint_active_key(self, key_id: str) -> str:
        """Generate a keypair, persist the private PEM (0600), make it active.

        This is the same sequence as ``signing/keygen.py`` but writes into *this*
        context's ``keys/`` + ``trust_root.json`` rather than the committed tree.
        """
        self.keys_dir.mkdir(parents=True, exist_ok=True)
        priv, pub = sl.generate_keypair()

        priv_path = self.keys_dir / f"{key_id}.private.pem"
        fd = os.open(str(priv_path), os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, "wb") as fh:
            fh.write(sl.private_key_to_pem(priv))
        os.chmod(str(priv_path), stat.S_IRUSR | stat.S_IWUSR)  # 0600

        pub_b64 = sl.public_key_to_b64(pub)
        (self.keys_dir / f"{key_id}.public.b64").write_text(pub_b64 + "\n", encoding="utf-8")

        # Merge into the trust root, retiring any prior active key (rotation-safe).
        doc = self.load_trust_root()
        keys = doc.setdefault("keys", {})
        for meta in keys.values():
            if meta.get("status") == "active":
                meta["status"] = "retired"
        keys[key_id] = {"pubkey": pub_b64, "algo": sl.ALGO, "status": "active"}
        doc["active_key_id"] = key_id
        doc["version"] = doc.get("version", 1)
        self._write_trust_root(doc)
        return key_id

    def _load_private_key(self, key_id: str):
        priv_path = self.keys_dir / f"{key_id}.private.pem"
        if not priv_path.is_file():
            raise FileNotFoundError(
                f"private key for key_id {key_id!r} not found at {priv_path}; "
                "this host cannot sign with that key (verifier-only?)"
            )
        return sl.load_private_key_pem(priv_path.read_bytes())

    # -- sign / verify (reuse signing_lib + verify_bundle) ------------------ #

    def sign(self, path: "str | Path", *, kind: str, version: str, key_id: str | None = None) -> SignedArtifact:
        """Sign ``path`` and write ``<path>.sig`` + ``<path>.manifest.json``.

        Identical canonical-bytes / manifest behaviour to ``sign_bundle.sign_file``;
        the only difference is the configurable trust-root + key store. Returns a
        :class:`SignedArtifact`.
        """
        path = str(path)
        if not os.path.isfile(path):
            raise FileNotFoundError(f"file to sign not found: {path}")

        doc = self.load_trust_root()
        key_id = key_id or doc.get("active_key_id")
        if not key_id:
            raise RuntimeError("trust root has no active_key_id; mint a key first")
        if key_id not in doc.get("keys", {}):
            raise RuntimeError(f"key_id {key_id!r} is not in the trust root")

        priv = self._load_private_key(key_id)

        signed_bytes = sl.canonical_bytes_for_file(path, kind)
        raw_sig = sl.sign(signed_bytes, priv)

        # Sanity: the signer's pubkey must match the trust-root pubkey for this key.
        expected_pub = doc["keys"][key_id]["pubkey"]
        if sl.public_key_to_b64(priv.public_key()) != expected_pub:
            raise RuntimeError(
                f"private key for {key_id!r} does not match the trust-root public key"
            )

        sig_path = path + ".sig"
        manifest_path = path + ".manifest.json"
        with open(sig_path, "w", encoding="utf-8") as fh:
            fh.write(sl.b64e(raw_sig) + "\n")

        manifest = sl.build_manifest(
            artifact_kind=kind, version=str(version), signed_bytes=signed_bytes, key_id=key_id
        )
        with open(manifest_path, "w", encoding="utf-8") as fh:
            json.dump(manifest, fh, indent=2, sort_keys=True)
            fh.write("\n")

        return SignedArtifact(path, sig_path, manifest_path, manifest)

    def verify(self, path: "str | Path", *, allow_retired: bool = False) -> "vb.VerifyResult":
        """Verify ``path`` (+ sidecars) against THIS context's trust root.

        Delegates to ``verify_bundle.verify_file`` — the exact enforcement point the
        agent calls — so a bundle that verifies here is one the agent will apply (I6).
        """
        return vb.verify_file(
            str(path),
            trust_root_path=str(self.trust_root_path),
            allow_retired=allow_retired,
        )
