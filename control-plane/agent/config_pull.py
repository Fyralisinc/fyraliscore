"""config_pull — pull a signed config bundle and VERIFY it before applying (I6).

The control plane ships agent config (and releases) as **signed bundles**: the
config JSON plus a detached ed25519 signature and a manifest, exactly as
``signing/sign_bundle.py`` produces (``<name>``, ``<name>.sig``,
``<name>.manifest.json``).

This module is the data-plane enforcement point for invariant **I6**:

    "an artifact whose signature fails verification, or whose key_id is
     unknown/retired, is NEVER applied."

Flow
----
1. **Pull** the three artifacts over the agent's *outbound* channel (I2: the
   agent reaches out; the console never reaches in). The transport is injected
   so the daemon, a real https client, and the test-suite share one code path —
   the default fetcher does an outbound ``requests.get`` of
   ``<config_url>``, ``<config_url>.sig``, ``<config_url>.manifest.json``.
2. **Verify** the pulled bundle with ``control-plane/signing/verify_bundle``
   against the agent's trust root. Unverified / tampered / unknown-key / wrong
   artifact-kind  -> REJECT, and the on-disk applied config is left untouched.
3. **Apply** (atomically) only on a clean verify: write the config + its sig +
   manifest into ``config_dir`` so a restart re-reads a *verified* config.

``pull_and_apply`` returns a :class:`ConfigPullResult`; it never raises for a bad
bundle (the agent logs + keeps the previous config). Pulling is best-effort and
non-fatal: a config-pull failure must never crash the daemon or block local ops.
"""

from __future__ import annotations

import json
import os
import shutil
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

import _bootstrap  # noqa: F401  (side-effect: sys.path for lib + signing)
import verify_bundle as vb

__all__ = ["ConfigPullResult", "ConfigPuller", "BundleFetcher", "http_fetcher"]

# A fetcher returns (config_bytes, sig_text, manifest_bytes) for a config URL,
# or raises on a transport error. Injected so tests use a fake console.
BundleFetcher = Callable[[str], "tuple[bytes, str, bytes]"]

APPLIED_NAME = "agent-config.json"


@dataclass
class ConfigPullResult:
    ok: bool
    reason: str
    applied: bool = False
    version: str | None = None
    key_id: str | None = None
    applied_path: str | None = None


def http_fetcher(timeout_s: float = 5.0) -> BundleFetcher:
    """Default OUTBOUND fetcher: GET the bundle + its sidecars over https.

    Imports ``requests`` lazily so importing this module never requires a network
    stack (and tests that inject a fake fetcher don't need ``requests``).
    """
    import requests  # lazy

    def _fetch(config_url: str) -> tuple[bytes, str, bytes]:
        cfg = requests.get(config_url, timeout=timeout_s)
        cfg.raise_for_status()
        sig = requests.get(config_url + ".sig", timeout=timeout_s)
        sig.raise_for_status()
        man = requests.get(config_url + ".manifest.json", timeout=timeout_s)
        man.raise_for_status()
        return cfg.content, sig.text, man.content

    return _fetch


class ConfigPuller:
    """Pulls a signed config bundle and applies it only after verify (I6)."""

    def __init__(
        self,
        *,
        config_dir: "str | Path",
        trust_root_path: "str | Path | None" = None,
        fetcher: BundleFetcher | None = None,
    ) -> None:
        self.config_dir = Path(config_dir)
        self.trust_root_path = (
            str(trust_root_path) if trust_root_path is not None else None
        )
        self._fetcher = fetcher or http_fetcher()

    @property
    def applied_config_path(self) -> Path:
        return self.config_dir / APPLIED_NAME

    def pull_and_apply(self, config_url: str) -> ConfigPullResult:
        """Pull ``config_url`` + sidecars, verify, and apply on success."""
        # 1. Pull (outbound). A transport error is non-fatal — keep current config.
        try:
            cfg_bytes, sig_text, man_bytes = self._fetcher(config_url)
        except Exception as exc:
            return ConfigPullResult(False, f"config pull failed (transport): {exc}")

        # 2. Stage into a temp dir whose filenames match verify_bundle's contract
        #    (<file>, <file>.sig, <file>.manifest.json) and verify there.
        staged = Path(tempfile.mkdtemp(prefix="agent-cfg-"))
        try:
            staged_cfg = staged / APPLIED_NAME
            staged_cfg.write_bytes(cfg_bytes)
            (staged / (APPLIED_NAME + ".sig")).write_text(
                sig_text if sig_text.endswith("\n") else sig_text + "\n",
                encoding="utf-8",
            )
            (staged / (APPLIED_NAME + ".manifest.json")).write_bytes(man_bytes)

            res = vb.verify_file(str(staged_cfg), trust_root_path=self.trust_root_path)
            if not res.ok:
                # I6: reject — do NOT apply, leave the existing config untouched.
                return ConfigPullResult(
                    False,
                    f"config REJECTED (unverified): {res.reason}",
                    version=res.version,
                    key_id=res.key_id,
                )
            if (res.artifact or "").lower() != "config":
                return ConfigPullResult(
                    False,
                    f"signed artifact is {res.artifact!r}, not config (refusing to apply)",
                    version=res.version,
                    key_id=res.key_id,
                )

            # Sanity: the verified bytes must be valid JSON we can later read.
            try:
                json.loads(cfg_bytes.decode("utf-8"))
            except (ValueError, UnicodeDecodeError) as exc:
                return ConfigPullResult(
                    False,
                    f"verified config is not valid JSON: {exc}",
                    version=res.version,
                    key_id=res.key_id,
                )

            # 3. Apply atomically: copy the verified trio into config_dir.
            self.config_dir.mkdir(parents=True, exist_ok=True)
            for suffix in ("", ".sig", ".manifest.json"):
                src = staged / (APPLIED_NAME + suffix)
                self._atomic_replace(src, self.config_dir / (APPLIED_NAME + suffix))

            return ConfigPullResult(
                True,
                f"config v{res.version} verified ({res.key_id}) and applied",
                applied=True,
                version=res.version,
                key_id=res.key_id,
                applied_path=str(self.applied_config_path),
            )
        finally:
            shutil.rmtree(staged, ignore_errors=True)

    @staticmethod
    def _atomic_replace(src: Path, dst: Path) -> None:
        """Move ``src`` over ``dst`` atomically within the same filesystem."""
        tmp = dst.with_suffix(dst.suffix + ".tmp")
        shutil.copyfile(src, tmp)
        os.replace(tmp, dst)

    def load_applied_config(self) -> dict | None:
        """Return the currently-applied config dict, or ``None`` if none applied.

        Re-verifies on read so a tampered on-disk config (e.g. edited after apply)
        is not trusted: an unverifiable applied config returns ``None`` (I6).
        """
        path = self.applied_config_path
        if not path.is_file():
            return None
        res = vb.verify_file(str(path), trust_root_path=self.trust_root_path)
        if not res.ok or (res.artifact or "").lower() != "config":
            return None
        try:
            with open(path, "r", encoding="utf-8") as fh:
                return json.load(fh)
        except (ValueError, OSError):
            return None
