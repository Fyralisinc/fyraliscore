#!/usr/bin/env python3
"""publish — the release registry: store signed release bundles by version + serve them.

WS-RELEASE ships **signed** bundles; this module is where a built bundle is
*published* so the fleet can pull it. It is two things:

1. A **registry on disk** (``ReleaseRegistry``) keyed by version. ``publish`` copies a
   built bundle's trio (``<tarball>``, ``<tarball>.sig``, ``<tarball>.manifest.json``,
   plus the human ``*.release.json``) into ``<registry>/<version>/`` and updates an
   ``index.json`` (every version + a ``latest`` pointer). Publishing **re-verifies**
   the bundle's signature first (refuse to publish an unsigned/tampered bundle — I6)
   and refuses to silently overwrite an existing version unless ``--force``.

2. An **HTTP server** (``serve``) that exposes those bundles in exactly the layout the
   data-plane agent already consumes — ``config_pull`` / ``verify_bundle`` fetch
   ``<url>``, ``<url>.sig``, ``<url>.manifest.json``. So:

       GET /releases/<version>/<tarball>                  -> the signed tarball bytes
       GET /releases/<version>/<tarball>.sig              -> the detached signature
       GET /releases/<version>/<tarball>.manifest.json    -> the C2 signing manifest
       GET /releases/latest        (JSON)  -> {version, url, sig_url, manifest_url, sha256}
       GET /releases/<version>     (JSON)  -> same, for a pinned version
       GET /index.json             (JSON)  -> the full registry index
       GET /healthz                        -> {status: ok, versions: N}

   The agent's ``ConfigPuller``/release puller points at ``…/<tarball>`` and gets a
   bundle it can verify-before-apply unchanged.

Registry layout
---------------
    <registry>/index.json
    <registry>/<version>/fyralis-release-<version>.tar.gz
    <registry>/<version>/fyralis-release-<version>.tar.gz.sig
    <registry>/<version>/fyralis-release-<version>.tar.gz.manifest.json
    <registry>/<version>/fyralis-release-<version>.release.json   (if present)

Usage
-----
    python publish.py publish ./_dist/fyralis-release-1.4.2.tar.gz --registry ./_registry
    python publish.py list   --registry ./_registry
    python publish.py serve  --registry ./_registry --port 8090
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
from dataclasses import dataclass
from pathlib import Path

import _bootstrap  # noqa: F401  (side-effect: sys.path for lib + signing)

from signing_ctx import SigningContext  # noqa: E402

__all__ = ["ReleaseRegistry", "PublishedRelease"]

INDEX_NAME = "index.json"


@dataclass(frozen=True)
class PublishedRelease:
    """One published version's on-disk + URL coordinates."""

    version: str
    tarball: str          # filename of the tarball within the version dir
    sha256: str
    key_id: str
    published_at: str

    def url_paths(self) -> dict:
        """Relative URL paths the HTTP server exposes for this version."""
        base = f"/releases/{self.version}/{self.tarball}"
        return {
            "url": base,
            "sig_url": base + ".sig",
            "manifest_url": base + ".manifest.json",
        }

    def as_index_entry(self) -> dict:
        d = {
            "version": self.version,
            "tarball": self.tarball,
            "sha256": self.sha256,
            "key_id": self.key_id,
            "published_at": self.published_at,
        }
        d.update(self.url_paths())
        return d


class ReleaseRegistry:
    """A versioned, signature-verified store of release bundles on disk."""

    def __init__(self, root: "str | Path", *, signing_root: "str | Path | None" = None) -> None:
        self.root = Path(root)
        self.signing_root = signing_root  # context used to RE-VERIFY before publish

    # -- index I/O ---------------------------------------------------------- #

    @property
    def index_path(self) -> Path:
        return self.root / INDEX_NAME

    def _load_index(self) -> dict:
        if not self.index_path.is_file():
            return {"version": 1, "latest": None, "releases": {}}
        with open(self.index_path, "r", encoding="utf-8") as fh:
            return json.load(fh)

    def _write_index(self, doc: dict) -> None:
        self.root.mkdir(parents=True, exist_ok=True)
        tmp = self.index_path.with_suffix(".json.tmp")
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(doc, fh, indent=2, sort_keys=True)
            fh.write("\n")
        os.replace(tmp, self.index_path)

    # -- publish ------------------------------------------------------------ #

    def publish(
        self,
        tarball_path: "str | Path",
        *,
        force: bool = False,
        make_latest: bool = True,
    ) -> PublishedRelease:
        """Verify + copy a built bundle into the registry and update the index.

        Refuses to publish a bundle whose signature does not verify (I6), and
        refuses to overwrite an existing version unless ``force``.
        """
        tarball_path = Path(tarball_path)
        sig_path = Path(str(tarball_path) + ".sig")
        manifest_path = Path(str(tarball_path) + ".manifest.json")
        for p in (tarball_path, sig_path, manifest_path):
            if not p.is_file():
                raise FileNotFoundError(f"missing bundle file (publish needs the full trio): {p}")

        # I6: never publish an unverifiable bundle. Verify against the signing context.
        ctx = (
            SigningContext.for_dir(self.signing_root)
            if self.signing_root is not None
            else SigningContext.for_control_plane()
        )
        res = ctx.verify(tarball_path)
        if not res.ok:
            raise ValueError(f"refusing to publish an unverified bundle: {res.reason}")
        if (res.artifact or "").lower() != "release":
            raise ValueError(
                f"refusing to publish a {res.artifact!r} bundle via the release registry"
            )

        version = res.version or _version_from_manifest(manifest_path)
        with open(manifest_path, "r", encoding="utf-8") as fh:
            manifest = json.load(fh)

        vdir = self.root / version
        if vdir.exists() and not force:
            raise FileExistsError(
                f"version {version} already published at {vdir} (use force=True to overwrite)"
            )
        vdir.mkdir(parents=True, exist_ok=True)

        # Copy the trio (+ the human release manifest if present) into the version dir.
        for src in (tarball_path, sig_path, manifest_path):
            shutil.copyfile(src, vdir / src.name)
        human = tarball_path.with_name(
            tarball_path.name.replace(".tar.gz", ".release.json")
        )
        if human.is_file():
            shutil.copyfile(human, vdir / human.name)

        published = PublishedRelease(
            version=version,
            tarball=tarball_path.name,
            sha256=manifest.get("sha256", ""),
            key_id=manifest.get("key_id", ""),
            published_at=_now(),
        )

        idx = self._load_index()
        idx.setdefault("releases", {})[version] = published.as_index_entry()
        if make_latest or idx.get("latest") is None:
            idx["latest"] = version
        self._write_index(idx)
        return published

    # -- query -------------------------------------------------------------- #

    def list_versions(self) -> list[str]:
        return sorted(self._load_index().get("releases", {}).keys())

    def latest(self) -> str | None:
        return self._load_index().get("latest")

    def get(self, version: str) -> dict | None:
        return self._load_index().get("releases", {}).get(version)

    def resolve_latest_entry(self) -> dict | None:
        latest = self.latest()
        return self.get(latest) if latest else None

    def set_latest(self, version: str) -> None:
        """Point ``latest`` at an already-published ``version`` (used by rollout/rollback)."""
        idx = self._load_index()
        if version not in idx.get("releases", {}):
            raise KeyError(f"version {version} is not published")
        idx["latest"] = version
        self._write_index(idx)

    def version_dir(self, version: str) -> Path:
        return self.root / version

    def bundle_paths(self, version: str) -> dict | None:
        """Absolute on-disk paths of a version's trio, or ``None`` if not published."""
        entry = self.get(version)
        if entry is None:
            return None
        vdir = self.version_dir(version)
        tar = vdir / entry["tarball"]
        return {
            "tarball": str(tar),
            "sig": str(tar) + ".sig",
            "manifest": str(tar) + ".manifest.json",
        }


def _version_from_manifest(manifest_path: Path) -> str:
    with open(manifest_path, "r", encoding="utf-8") as fh:
        return str(json.load(fh).get("version", "0"))


def _now() -> str:
    import signing_lib as sl

    return sl.now_rfc3339()


# --------------------------------------------------------------------------- #
# HTTP server — exposes the registry in the layout the agent already consumes  #
# --------------------------------------------------------------------------- #


def create_app(registry: ReleaseRegistry):
    """Build a FastAPI app serving ``registry`` (the agent fetches bundles here).

    Imported lazily by ``serve`` so the registry can be used (publish/list) without
    FastAPI installed.
    """
    from fastapi import FastAPI, HTTPException
    from fastapi.responses import FileResponse, JSONResponse

    application = FastAPI(
        title="Fyralis BYOC — Release Registry",
        version="0.1.0",
        description="Signed release bundles by version; agents pull + verify before apply (I6).",
    )
    application.state.registry = registry

    def _bundle_file(version: str, filename: str) -> Path:
        paths = registry.bundle_paths(version)
        if paths is None:
            raise HTTPException(status_code=404, detail=f"unknown release version {version!r}")
        vdir = registry.version_dir(version)
        # Constrain to files inside the version dir (no path traversal).
        candidate = (vdir / filename).resolve()
        if not str(candidate).startswith(str(vdir.resolve()) + os.sep):
            raise HTTPException(status_code=400, detail="invalid path")
        if not candidate.is_file():
            raise HTTPException(status_code=404, detail=f"no such artifact {filename!r}")
        return candidate

    @application.get("/index.json")
    def index() -> JSONResponse:
        return JSONResponse(content=registry._load_index())

    @application.get("/releases/latest")
    def latest() -> JSONResponse:
        entry = registry.resolve_latest_entry()
        if entry is None:
            raise HTTPException(status_code=404, detail="no releases published yet")
        return JSONResponse(content=entry)

    @application.get("/releases/{version}")
    def get_version(version: str) -> JSONResponse:
        # NB: declared AFTER /releases/latest so "latest" resolves to that route.
        entry = registry.get(version)
        if entry is None:
            raise HTTPException(status_code=404, detail=f"unknown release version {version!r}")
        return JSONResponse(content=entry)

    @application.get("/releases/{version}/{filename}")
    def get_artifact(version: str, filename: str):
        path = _bundle_file(version, filename)
        # Tarball -> octet-stream; .sig/.manifest.json -> their natural types. The
        # agent's fetcher reads bytes/text, so media type is cosmetic but correct.
        if filename.endswith(".manifest.json"):
            media = "application/json"
        elif filename.endswith(".sig"):
            media = "text/plain"
        else:
            media = "application/octet-stream"
        return FileResponse(str(path), media_type=media, filename=filename)

    @application.get("/healthz")
    def healthz() -> dict:
        return {"status": "ok", "versions": len(registry.list_versions()), "latest": registry.latest()}

    return application


def serve(registry_root: "str | Path", *, host: str = "0.0.0.0", port: int = 8090,
          signing_root: "str | Path | None" = None) -> None:
    import uvicorn

    registry = ReleaseRegistry(registry_root, signing_root=signing_root)
    uvicorn.run(create_app(registry), host=host, port=port, log_level="info")


# --------------------------------------------------------------------------- #
# CLI                                                                          #
# --------------------------------------------------------------------------- #


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Publish + serve signed release bundles by version.")
    sub = ap.add_subparsers(dest="cmd", required=True)

    pp = sub.add_parser("publish", help="verify + store a built bundle in the registry")
    pp.add_argument("tarball", help="path to the built fyralis-release-<v>.tar.gz")
    pp.add_argument("--registry", required=True, help="registry root directory")
    pp.add_argument("--signing-root", default=None, help="alternate signing context dir")
    pp.add_argument("--force", action="store_true", help="overwrite an existing version")
    pp.add_argument("--no-latest", action="store_true", help="do not move the latest pointer")

    lp = sub.add_parser("list", help="list published versions + the latest pointer")
    lp.add_argument("--registry", required=True)

    gp = sub.add_parser("set-latest", help="point latest at an already-published version")
    gp.add_argument("--registry", required=True)
    gp.add_argument("version")

    svp = sub.add_parser("serve", help="serve the registry over HTTP for agents to pull")
    svp.add_argument(
        "--registry",
        default=os.environ.get("CP_RELEASE_REGISTRY"),
        required="CP_RELEASE_REGISTRY" not in os.environ,
        help="registry root (default: $CP_RELEASE_REGISTRY)",
    )
    svp.add_argument("--host", default=os.environ.get("CP_RELEASE_HOST", "0.0.0.0"))
    svp.add_argument("--port", type=int, default=int(os.environ.get("CP_RELEASE_PORT", "8090")))
    svp.add_argument("--signing-root", default=None)

    args = ap.parse_args(argv)

    if args.cmd == "publish":
        reg = ReleaseRegistry(args.registry, signing_root=args.signing_root)
        try:
            pub = reg.publish(args.tarball, force=args.force, make_latest=not args.no_latest)
        except Exception as exc:
            print(f"publish failed: {exc}", file=sys.stderr)
            return 1
        print(f"published release {pub.version}")
        print(f"  sha256 : {pub.sha256}")
        print(f"  key_id : {pub.key_id}")
        for k, v in pub.url_paths().items():
            print(f"  {k:12s}: {v}")
        print(f"  latest : {reg.latest()}")
        return 0

    if args.cmd == "list":
        reg = ReleaseRegistry(args.registry)
        versions = reg.list_versions()
        latest = reg.latest()
        if not versions:
            print("(no releases published)")
            return 0
        for v in versions:
            tag = "  <- latest" if v == latest else ""
            entry = reg.get(v)
            print(f"  {v}  sha256={entry.get('sha256','')[:12]}…  key={entry.get('key_id','')}{tag}")
        return 0

    if args.cmd == "set-latest":
        reg = ReleaseRegistry(args.registry)
        try:
            reg.set_latest(args.version)
        except KeyError as exc:
            print(f"set-latest failed: {exc}", file=sys.stderr)
            return 1
        print(f"latest -> {args.version}")
        return 0

    if args.cmd == "serve":
        serve(args.registry, host=args.host, port=args.port, signing_root=args.signing_root)
        return 0

    return 2


if __name__ == "__main__":
    raise SystemExit(main())
