#!/usr/bin/env python3
"""build_release — package a Fyralis data-plane release and SIGN it (C2 / I6).

A "release" is a **versioned tarball** of a source tree plus a small **release
manifest** describing what went into it, and the whole tarball is signed with
ed25519 (detached ``.sig`` + the C2 signing manifest) exactly as
``control-plane/signing/sign_bundle`` does — so the agent's ``config_pull`` /
``verify_bundle`` accepts it unchanged.

What "build" produces (under ``--out``)
---------------------------------------
    fyralis-release-<version>.tar.gz                 # the deterministic release tarball
    fyralis-release-<version>.tar.gz.sig             # base64 ed25519 detached signature
    fyralis-release-<version>.tar.gz.manifest.json   # C2 signing manifest (artifact=release)
    fyralis-release-<version>.release.json           # human/CD release manifest (contents, sha, files)

The **signed bytes** are the exact tarball bytes (C2: release tarballs are signed
as opaque blobs). The signing manifest's ``sha256`` is the redundant integrity
check over those same bytes.

Determinism
-----------
The tarball is built reproducibly: entries are sorted by path, mtimes are pinned
to a fixed epoch, and uid/gid/uname/gname are zeroed, so the same source tree +
version yields byte-identical bytes (and thus a stable sha256). This makes
"did the release change?" a hash comparison, and lets two builders agree.

Signing key
-----------
By default it signs with the **control-plane** trust root
(``control-plane/signing/trust_root.json`` + ``keys/``), i.e. the key
``signing/keygen.py`` minted. ``--signing-root <dir>`` points at an alternate
context (a hermetic CI signer, or the self-test's throwaway key store) without
touching the committed ``signing/`` tree.

Usage
-----
    # package the data-plane source tree at ./dataplane as release 1.4.2 and sign it
    python build_release.py build --src ./dataplane --version 1.4.2 --out ./_dist

    # sign with an alternate (e.g. ephemeral) signing context
    python build_release.py build --src ./dataplane --version 1.4.2 \
        --out ./_dist --signing-root /tmp/cp-signing

    # verify a built release with the same context (round-trip check)
    python build_release.py verify ./_dist/fyralis-release-1.4.2.tar.gz
"""

from __future__ import annotations

import argparse
import fnmatch
import hashlib
import io
import json
import os
import sys
import tarfile
from dataclasses import dataclass
from pathlib import Path

import _bootstrap  # noqa: F401  (side-effect: sys.path for lib + signing)

from signing_ctx import SignedArtifact, SigningContext  # noqa: E402

__all__ = ["ReleaseBuild", "build_release", "verify_release", "DEFAULT_EXCLUDES"]

# A fixed epoch for reproducible tar mtimes (2020-01-01T00:00:00Z).
_FIXED_MTIME = 1577836800

# Paths/globs never packaged into a release (noise + never-ship secrets).
DEFAULT_EXCLUDES = (
    "*.pyc",
    "__pycache__",
    "__pycache__/*",
    ".git",
    ".git/*",
    "*.private.pem",   # never ship a private signing/CA key in a release
    "keys/*",          # belt-and-suspenders: never ship a key store
    "*.tar.gz",
    "*.tar.gz.sig",
    "*.tar.gz.manifest.json",
    ".DS_Store",
)


@dataclass(frozen=True)
class ReleaseBuild:
    """The result of building (and signing) a release."""

    version: str
    tarball_path: str
    release_manifest_path: str
    signed: SignedArtifact
    file_count: int
    tarball_sha256: str

    @property
    def key_id(self) -> str:
        return self.signed.key_id

    def as_dict(self) -> dict:
        return {
            "version": self.version,
            "tarball": self.tarball_path,
            "release_manifest": self.release_manifest_path,
            "sig": self.signed.sig_path,
            "manifest": self.signed.manifest_path,
            "key_id": self.key_id,
            "file_count": self.file_count,
            "tarball_sha256": self.tarball_sha256,
        }


def _is_excluded(rel_path: str, excludes: tuple[str, ...]) -> bool:
    """True if ``rel_path`` (posix, relative to src root) matches any exclude glob."""
    parts = rel_path.split("/")
    for pat in excludes:
        if fnmatch.fnmatch(rel_path, pat):
            return True
        # Match a directory component anywhere in the path (e.g. "__pycache__").
        if "/" not in pat and any(fnmatch.fnmatch(p, pat) for p in parts):
            return True
    return False


def _collect_files(src: Path, excludes: tuple[str, ...]) -> list[tuple[str, Path]]:
    """Return ``[(arcname, abspath), ...]`` sorted by arcname, excludes applied.

    ``arcname`` is posix-relative to ``src`` (so the tarball has a flat, portable
    layout regardless of the host OS path separator).
    """
    out: list[tuple[str, Path]] = []
    for dirpath, dirnames, filenames in os.walk(src):
        # Prune excluded directories in-place so we don't descend into them.
        dirnames[:] = [
            d
            for d in dirnames
            if not _is_excluded(
                str((Path(dirpath) / d).relative_to(src).as_posix()), excludes
            )
        ]
        for fn in filenames:
            ab = Path(dirpath) / fn
            rel = ab.relative_to(src).as_posix()
            if _is_excluded(rel, excludes):
                continue
            out.append((rel, ab))
    out.sort(key=lambda t: t[0])
    return out


def _build_tarball(files: list[tuple[str, Path]], version: str) -> tuple[bytes, dict]:
    """Build the deterministic ``.tar.gz`` bytes + a per-file inventory.

    Returns ``(tar_gz_bytes, inventory)`` where ``inventory`` maps arcname -> the
    file's own sha256 + size (recorded in the release manifest for auditability).
    """
    inventory: dict[str, dict] = {}
    raw = io.BytesIO()
    # gzip with a fixed mtime so the gzip header doesn't vary build-to-build.
    import gzip

    tar_buf = io.BytesIO()
    with tarfile.open(fileobj=tar_buf, mode="w") as tf:
        for arcname, ab in files:
            data = ab.read_bytes()
            inventory[arcname] = {
                "sha256": hashlib.sha256(data).hexdigest(),
                "size": len(data),
            }
            info = tarfile.TarInfo(name=f"fyralis-release-{version}/{arcname}")
            info.size = len(data)
            info.mtime = _FIXED_MTIME
            info.mode = 0o644
            info.uid = info.gid = 0
            info.uname = info.gname = ""
            info.type = tarfile.REGTYPE
            tf.addfile(info, io.BytesIO(data))

    with gzip.GzipFile(fileobj=raw, mode="wb", mtime=_FIXED_MTIME) as gz:
        gz.write(tar_buf.getvalue())
    return raw.getvalue(), inventory


def build_release(
    *,
    src: "str | Path",
    version: str,
    out_dir: "str | Path",
    signing_root: "str | Path | None" = None,
    key_id: str | None = None,
    excludes: tuple[str, ...] = DEFAULT_EXCLUDES,
    extra_metadata: dict | None = None,
) -> ReleaseBuild:
    """Package ``src`` as release ``version`` into ``out_dir`` and sign the tarball.

    ``signing_root`` selects the signing context: ``None`` uses the committed
    control-plane signing dir; a path uses :meth:`SigningContext.for_dir` (e.g. an
    ephemeral self-test key store). Returns a :class:`ReleaseBuild`.
    """
    src = Path(src)
    if not src.is_dir():
        raise NotADirectoryError(f"source tree not found: {src}")
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    files = _collect_files(src, excludes)
    if not files:
        raise ValueError(f"refusing to build an empty release: no files under {src}")

    tar_bytes, inventory = _build_tarball(files, version)
    tarball_path = out_dir / f"fyralis-release-{version}.tar.gz"
    tarball_path.write_bytes(tar_bytes)
    tarball_sha = hashlib.sha256(tar_bytes).hexdigest()

    # The human/CD release manifest (distinct from the C2 *signing* manifest).
    ctx = (
        SigningContext.for_dir(signing_root)
        if signing_root is not None
        else SigningContext.for_control_plane()
    )
    rel_manifest = {
        "artifact": "release",
        "version": str(version),
        "source_root": str(src),
        "file_count": len(files),
        "tarball": tarball_path.name,
        "tarball_sha256": tarball_sha,
        "built_at": _now(),
        "signing_key_id": key_id or ctx.active_key_id,
        "files": inventory,
    }
    if extra_metadata:
        rel_manifest["metadata"] = extra_metadata
    rel_manifest_path = out_dir / f"fyralis-release-{version}.release.json"
    with open(rel_manifest_path, "w", encoding="utf-8") as fh:
        json.dump(rel_manifest, fh, indent=2, sort_keys=True)
        fh.write("\n")

    # SIGN the tarball (C2: release tarballs signed as opaque blobs).
    signed = ctx.sign(tarball_path, kind="release", version=str(version), key_id=key_id)

    return ReleaseBuild(
        version=str(version),
        tarball_path=str(tarball_path),
        release_manifest_path=str(rel_manifest_path),
        signed=signed,
        file_count=len(files),
        tarball_sha256=tarball_sha,
    )


def verify_release(
    tarball_path: "str | Path",
    *,
    signing_root: "str | Path | None" = None,
    allow_retired: bool = False,
):
    """Verify a built release tarball against a signing context's trust root.

    Returns the ``verify_bundle.VerifyResult`` (``.ok`` tells you accept/reject).
    """
    ctx = (
        SigningContext.for_dir(signing_root)
        if signing_root is not None
        else SigningContext.for_control_plane()
    )
    return ctx.verify(tarball_path, allow_retired=allow_retired)


def _now() -> str:
    import signing_lib as sl  # reuse the CP RFC-3339 helper

    return sl.now_rfc3339()


# --------------------------------------------------------------------------- #
# CLI                                                                          #
# --------------------------------------------------------------------------- #


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Build + sign a Fyralis data-plane release (C2/I6).")
    sub = ap.add_subparsers(dest="cmd", required=True)

    bp = sub.add_parser("build", help="package + sign a source tree into a versioned release")
    bp.add_argument("--src", required=True, help="path to the source tree to package")
    bp.add_argument("--version", required=True, help="release version, e.g. 1.4.2")
    bp.add_argument("--out", required=True, help="output directory for the release artifacts")
    bp.add_argument(
        "--signing-root",
        default=None,
        help="alternate signing context dir (default: control-plane/signing)",
    )
    bp.add_argument("--key-id", default=None, help="signing key id (default: context active key)")

    vp = sub.add_parser("verify", help="verify a built release tarball against the trust root")
    vp.add_argument("tarball", help="path to fyralis-release-<v>.tar.gz")
    vp.add_argument("--signing-root", default=None, help="alternate signing context dir")
    vp.add_argument("--allow-retired", action="store_true", help="accept retired-key signatures")

    args = ap.parse_args(argv)

    if args.cmd == "build":
        try:
            rb = build_release(
                src=args.src,
                version=args.version,
                out_dir=args.out,
                signing_root=args.signing_root,
                key_id=args.key_id,
            )
        except Exception as exc:
            print(f"build failed: {exc}", file=sys.stderr)
            return 1
        print(f"built + signed release {rb.version}")
        print(f"  files        : {rb.file_count}")
        print(f"  tarball      : {rb.tarball_path}")
        print(f"  tarball sha  : {rb.tarball_sha256}")
        print(f"  key_id       : {rb.key_id}")
        print(f"  -> {rb.signed.sig_path}")
        print(f"  -> {rb.signed.manifest_path}")
        print(f"  -> {rb.release_manifest_path}")
        return 0

    if args.cmd == "verify":
        res = verify_release(
            args.tarball, signing_root=args.signing_root, allow_retired=args.allow_retired
        )
        if res.ok:
            print(f"VERIFY OK: {res.reason}")
            return 0
        print(f"VERIFY FAILED: {res.reason}", file=sys.stderr)
        return 1

    return 2


if __name__ == "__main__":
    raise SystemExit(main())
