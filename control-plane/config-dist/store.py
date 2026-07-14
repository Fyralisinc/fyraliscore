"""store.py — the per-deployment signed-config store (FR-C3 / FR-C4 / FR-D4).

The config-distribution service serves a **signed per-deployment config bundle** to
the outbound-only agent: feature **flags**, the **telemetry_tier** (C3), and a
**token-rotation schedule** (FR-D4). This module owns the *persistence + versioning*
half of that responsibility; ``config_service.py`` is the HTTP surface on top of it.

Design (the part the spec calls out)
------------------------------------
* **Per-deployment.** Config is keyed by ``deployment_id`` (the C4 fleet-registry id).
  Each deployment has its own independent, monotonically-versioned config history.
* **Versioning = immutable signed snapshots.** Publishing a config (a tier change, a
  flag flip, a rotation-schedule edit) appends a **new version** — it never mutates an
  existing one. So "a tier change or flag flip is a new signed version, **no
  redeploy**": the agent just pulls and the version bumps. ``version`` is a
  per-deployment integer that starts at 1 and increments by 1.
* **Signed at rest.** Every version is stored as the C2 signing trio next to the
  config JSON: ``v<N>/config.json`` + ``config.json.sig`` + ``config.json.manifest.json``.
  Signing is delegated to ``control-plane/signing`` (``sign_bundle`` -> ``signing_lib``);
  we never re-implement crypto here. The signed quantity is the **canonical bytes** of
  the config document (C2), so the agent's ``verify_bundle`` (which recomputes canonical
  bytes for ``kind="config"``) verifies the exact same bytes.

On-disk layout (under ``<store_root>/deployments/<deployment_id>/``)::

    HEAD                       # plain-text: the current version integer, e.g. "3"
    v1/config.json
    v1/config.json.sig
    v1/config.json.manifest.json
    v2/config.json
    ...

The store is import-safe (no network, no global key generation at import). It binds to
a **signing home** (``signing_home``) that holds the keyring (``trust_root.json`` +
``keys/``); ``config_service.py`` owns bootstrapping that home so the *store* stays a
pure persistence object that can be unit-tested with any home you point it at.

This module writes ONLY under its ``store_root`` (default: a ``config-dist``-owned
directory) and reads the committed ``control-plane/signing`` package read-only.
"""

from __future__ import annotations

import json
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

# --------------------------------------------------------------------------- #
# Import the committed signing siblings (read-only). We point the signing CLIs #
# at a config-dist-owned signing home so we never write into signing/ (the     #
# write-disjoint rule) yet reuse its exact sign/verify code paths (C2/I6).     #
# --------------------------------------------------------------------------- #

_HERE = Path(__file__).resolve().parent
_CP_ROOT = _HERE.parent
_SIGNING_DIR = _CP_ROOT / "signing"
for _p in (str(_SIGNING_DIR), str(_CP_ROOT)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import signing_lib as sl  # noqa: E402  (control-plane/signing/signing_lib.py)
import sign_bundle as sb  # noqa: E402  (control-plane/signing/sign_bundle.py)
import verify_bundle as vb  # noqa: E402  (control-plane/signing/verify_bundle.py)

__all__ = [
    "ConfigStore",
    "ConfigVersion",
    "SigningHome",
    "ConfigStoreError",
    "DEFAULT_STORE_ROOT",
    "DEFAULT_SIGNING_HOME",
    "ARTIFACT_KIND",
    "CONFIG_FILENAME",
    "default_config_payload",
]

ARTIFACT_KIND = "config"  # the C2 manifest "artifact" value the agent requires
CONFIG_FILENAME = "config.json"  # what verify_bundle keys .sig / .manifest.json off

# config-dist owns these directories; both are created on demand and gitignored.
DEFAULT_STORE_ROOT = _HERE / "_data" / "store"
DEFAULT_SIGNING_HOME = _HERE / "_data" / "signing-home"


class ConfigStoreError(RuntimeError):
    """Raised for store-level failures (bad deployment id, signing home missing, ...)."""


# --------------------------------------------------------------------------- #
# The config payload shape (FR-C3 / FR-C4 / FR-D4)                             #
# --------------------------------------------------------------------------- #


def default_config_payload(
    *,
    tenant_id: str,
    deployment_id: str,
    telemetry_tier: str = "T1",
) -> Dict[str, Any]:
    """A minimal, valid default config body for a freshly-onboarded deployment.

    The body the agent applies is the *inner* ``config`` document (see
    :meth:`ConfigStore.build_document`). This is the FR-C3/FR-D4 surface:

    * ``flags``            — feature flags (booleans / scalars) the data plane reads.
    * ``telemetry_tier``   — the C3 tier (``T1|T2|T3``); a change is a new version.
    * ``token_rotation``   — the FR-D4 token-rotation schedule the agent honors.
    """
    return {
        "flags": {
            # Conservative, on-by-default-safe flags for a new deployment.
            "ingestion_enabled": True,
            "reasoning_enabled": True,
            "anomaly_detection_enabled": False,
        },
        "telemetry_tier": telemetry_tier,
        "token_rotation": {
            # FR-D4: how often the agent rotates its dial-home credentials, and a
            # rough wall-clock for the next rotation. "manual" = rotate on next pull.
            "enabled": True,
            "interval_hours": 24,
            "next_rotation_at": None,  # filled in by an operator/scheduler when known
            "grace_seconds": 3600,
        },
    }


# --------------------------------------------------------------------------- #
# SigningHome: a config-dist-owned keyring location for sign/verify            #
# --------------------------------------------------------------------------- #


@dataclass
class SigningHome:
    """A directory holding the signing keyring used to sign/verify configs.

    Layout::

        <root>/trust_root.json         # public keyring (key_id -> pubkey, active id)
        <root>/keys/<key_id>.private.pem

    We deliberately do NOT use the committed ``signing/`` directory as the home
    (write-disjoint: this work-stream writes only under ``config-dist/``). Instead we
    *retarget* the signing CLIs' module-level ``KEYS_DIR`` / ``TRUST_ROOT_PATH`` at this
    home for the duration of a sign/verify call, then restore them. The crypto and wire
    formats are 100% the committed code; only the storage location moves.
    """

    root: Path
    key_id: str = "cp-config-dist"

    @property
    def trust_root_path(self) -> Path:
        return self.root / "trust_root.json"

    @property
    def keys_dir(self) -> Path:
        return self.root / "keys"

    def exists(self) -> bool:
        return self.trust_root_path.is_file()

    # -- bootstrap -------------------------------------------------------- #

    def ensure_key(self) -> str:
        """Ensure an active signing key exists in this home; return its key_id.

        Idempotent: if a trust root with an active key already exists it is reused
        (so versions signed by earlier runs keep verifying). Otherwise a fresh
        ed25519 keypair is generated via ``signing_lib`` and written here.
        """
        self.root.mkdir(parents=True, exist_ok=True)
        self.keys_dir.mkdir(parents=True, exist_ok=True)
        if self.exists():
            doc = json.loads(self.trust_root_path.read_text(encoding="utf-8"))
            active = doc.get("active_key_id")
            if active and (self.keys_dir / f"{active}.private.pem").is_file():
                return active

        # Generate a new active key with signing_lib and persist trust root + PEM.
        priv, pub = sl.generate_keypair()
        pem = sl.private_key_to_pem(priv)
        priv_path = self.keys_dir / f"{self.key_id}.private.pem"
        fd = os.open(str(priv_path), os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, "wb") as fh:
            fh.write(pem)
        os.chmod(str(priv_path), 0o600)

        ring = sl.Keyring()
        ring.add_key(self.key_id, public=pub, private=priv, make_active=True)
        doc = ring.to_trust_root()
        self.trust_root_path.write_text(
            json.dumps(doc, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
        return self.key_id

    def active_key_id(self) -> Optional[str]:
        if not self.exists():
            return None
        doc = json.loads(self.trust_root_path.read_text(encoding="utf-8"))
        return doc.get("active_key_id")

    # -- retarget the signing CLIs at this home (write-disjoint) ---------- #

    def _retarget(self):
        """Context manager: point sign_bundle/verify_bundle at THIS home."""
        return _RetargetSigning(self)


class _RetargetSigning:
    """Temporarily redirect the signing CLIs' storage constants to a SigningHome.

    ``sign_bundle`` reads ``sb.TRUST_ROOT_PATH`` / ``sb.KEYS_DIR`` at call time, and
    ``verify_bundle.verify_file`` defaults ``trust_root_path`` to ``vb.TRUST_ROOT_PATH``
    at call time, so swapping the module attributes for the duration of a call cleanly
    moves the keystore without touching the committed files on disk.
    """

    def __init__(self, home: SigningHome) -> None:
        self._home = home
        self._saved: Dict[str, Any] = {}

    def __enter__(self) -> SigningHome:
        self._saved = {
            "sb.TRUST_ROOT_PATH": sb.TRUST_ROOT_PATH,
            "sb.KEYS_DIR": sb.KEYS_DIR,
            "vb.TRUST_ROOT_PATH": vb.TRUST_ROOT_PATH,
        }
        sb.TRUST_ROOT_PATH = str(self._home.trust_root_path)
        sb.KEYS_DIR = str(self._home.keys_dir)
        vb.TRUST_ROOT_PATH = str(self._home.trust_root_path)
        return self._home

    def __exit__(self, *exc) -> None:
        sb.TRUST_ROOT_PATH = self._saved["sb.TRUST_ROOT_PATH"]
        sb.KEYS_DIR = self._saved["sb.KEYS_DIR"]
        vb.TRUST_ROOT_PATH = self._saved["vb.TRUST_ROOT_PATH"]


# --------------------------------------------------------------------------- #
# A stored, signed config version                                             #
# --------------------------------------------------------------------------- #


@dataclass
class ConfigVersion:
    """One immutable, signed config snapshot for a deployment."""

    deployment_id: str
    version: int
    config_bytes: bytes  # the exact served config-document bytes (config.json)
    sig_b64: str  # base64 detached ed25519 signature (the .sig file content)
    manifest: Dict[str, Any]  # the C2 manifest dict (.manifest.json content)
    dir: Path  # the v<N>/ directory on disk

    @property
    def key_id(self) -> str:
        return self.manifest.get("key_id", "")

    @property
    def telemetry_tier(self) -> str:
        # The tier lives in the inner config body (document["config"]["telemetry_tier"]).
        doc = json.loads(self.config_bytes.decode("utf-8"))
        body = doc.get("config") if isinstance(doc.get("config"), dict) else doc
        return body.get("telemetry_tier", "")

    def document(self) -> Dict[str, Any]:
        return json.loads(self.config_bytes.decode("utf-8"))

    @property
    def sig_path(self) -> Path:
        return self.dir / (CONFIG_FILENAME + ".sig")

    @property
    def manifest_path(self) -> Path:
        return self.dir / (CONFIG_FILENAME + ".manifest.json")

    @property
    def config_path(self) -> Path:
        return self.dir / CONFIG_FILENAME


# --------------------------------------------------------------------------- #
# The store                                                                    #
# --------------------------------------------------------------------------- #


class ConfigStore:
    """Per-deployment, versioned, signed config persistence.

    Thread-safety: callers that publish concurrently for the *same* deployment should
    serialize (the service does, with a per-process lock). Reads are lock-free.
    """

    def __init__(
        self,
        *,
        store_root: "str | Path | None" = None,
        signing_home: Optional[SigningHome] = None,
    ) -> None:
        self.store_root = Path(store_root) if store_root else DEFAULT_STORE_ROOT
        self.signing_home = signing_home or SigningHome(DEFAULT_SIGNING_HOME)
        self.deployments_root = self.store_root / "deployments"
        self.deployments_root.mkdir(parents=True, exist_ok=True)
        # Bootstrap the signing key so the very first publish can sign.
        self.signing_home.ensure_key()

    # -- paths ------------------------------------------------------------ #

    @staticmethod
    def _safe_id(deployment_id: str) -> str:
        did = (deployment_id or "").strip()
        if not did or did in (".", "..") or "/" in did or "\\" in did or "\x00" in did:
            raise ConfigStoreError(f"invalid deployment_id: {deployment_id!r}")
        return did

    def _dep_dir(self, deployment_id: str) -> Path:
        return self.deployments_root / self._safe_id(deployment_id)

    def _head_path(self, deployment_id: str) -> Path:
        return self._dep_dir(deployment_id) / "HEAD"

    def _version_dir(self, deployment_id: str, version: int) -> Path:
        return self._dep_dir(deployment_id) / f"v{version}"

    # -- HEAD / version bookkeeping -------------------------------------- #

    def current_version(self, deployment_id: str) -> int:
        """The current (latest) version integer, or 0 if the deployment has none."""
        head = self._head_path(deployment_id)
        if not head.is_file():
            return 0
        try:
            return int(head.read_text(encoding="utf-8").strip() or "0")
        except ValueError:
            return 0

    def list_deployments(self) -> List[str]:
        if not self.deployments_root.is_dir():
            return []
        return sorted(
            p.name for p in self.deployments_root.iterdir() if p.is_dir()
        )

    def list_versions(self, deployment_id: str) -> List[int]:
        dep = self._dep_dir(deployment_id)
        if not dep.is_dir():
            return []
        out = []
        for p in dep.iterdir():
            if p.is_dir() and p.name.startswith("v"):
                try:
                    out.append(int(p.name[1:]))
                except ValueError:
                    continue
        return sorted(out)

    # -- the document we sign -------------------------------------------- #

    def build_document(
        self,
        *,
        tenant_id: str,
        deployment_id: str,
        version: int,
        config_body: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Wrap a config body into the served config document.

        The agent applies this whole document (``config_pull`` writes it verbatim and
        ``load_applied_config`` re-reads it). We include identity + version so the data
        plane can sanity-check the bundle is *its* config and detect staleness, plus
        ``created`` for audit. The signed bytes are the canonical form of this dict.
        """
        return {
            "schema": "fyralis.config/v1",
            "tenant_id": tenant_id,
            "deployment_id": deployment_id,
            "version": version,
            "created": sl.now_rfc3339(),
            "config": config_body,
        }

    # -- publish (the core write path) ----------------------------------- #

    def publish(
        self,
        *,
        deployment_id: str,
        tenant_id: str,
        config_body: Dict[str, Any],
    ) -> ConfigVersion:
        """Append a NEW signed version for ``deployment_id`` and advance HEAD.

        Returns the created :class:`ConfigVersion`. A tier change or a flag flip is just
        a different ``config_body`` -> a brand new immutable version (no redeploy).
        """
        deployment_id = self._safe_id(deployment_id)
        next_version = self.current_version(deployment_id) + 1
        vdir = self._version_dir(deployment_id, next_version)
        if vdir.exists():  # never overwrite an existing immutable version
            raise ConfigStoreError(
                f"version dir already exists (corrupt store?): {vdir}"
            )
        vdir.mkdir(parents=True, exist_ok=False)

        document = self.build_document(
            tenant_id=tenant_id,
            deployment_id=deployment_id,
            version=next_version,
            config_body=config_body,
        )

        # Write the config document. We persist the canonical bytes as the served
        # artifact so what we serve == what we signed (the agent re-canonicalizes
        # kind="config" anyway, but persisting canonical keeps bytes self-consistent).
        config_bytes = sl.canonical_json_bytes(document)
        cfg_path = vdir / CONFIG_FILENAME
        cfg_path.write_bytes(config_bytes)

        # Sign via the committed signing CLI (kind="config"); version recorded in the
        # C2 manifest is the integer config version.
        self.signing_home.ensure_key()
        with self.signing_home._retarget():
            sig_path, manifest_path = sb.sign_file(
                str(cfg_path),
                kind=ARTIFACT_KIND,
                version=str(next_version),
            )

        sig_b64 = Path(sig_path).read_text(encoding="utf-8").strip()
        manifest = json.loads(Path(manifest_path).read_text(encoding="utf-8"))

        # Advance HEAD last (after the trio is durable) so a crash mid-publish leaves
        # HEAD pointing at the previous good version.
        self._head_path(deployment_id).write_text(
            str(next_version), encoding="utf-8"
        )

        return ConfigVersion(
            deployment_id=deployment_id,
            version=next_version,
            config_bytes=config_bytes,
            sig_b64=sig_b64,
            manifest=manifest,
            dir=vdir,
        )

    # -- read paths ------------------------------------------------------- #

    def get_version(
        self, deployment_id: str, version: "int | None" = None
    ) -> Optional[ConfigVersion]:
        """Load a stored version (default: HEAD). Returns None if absent."""
        deployment_id = self._safe_id(deployment_id)
        if version is None:
            version = self.current_version(deployment_id)
        if version <= 0:
            return None
        vdir = self._version_dir(deployment_id, version)
        cfg_path = vdir / CONFIG_FILENAME
        if not cfg_path.is_file():
            return None
        config_bytes = cfg_path.read_bytes()
        sig_b64 = (vdir / (CONFIG_FILENAME + ".sig")).read_text(
            encoding="utf-8"
        ).strip()
        manifest = json.loads(
            (vdir / (CONFIG_FILENAME + ".manifest.json")).read_text(encoding="utf-8")
        )
        return ConfigVersion(
            deployment_id=deployment_id,
            version=version,
            config_bytes=config_bytes,
            sig_b64=sig_b64,
            manifest=manifest,
            dir=vdir,
        )

    def get_head(self, deployment_id: str) -> Optional[ConfigVersion]:
        return self.get_version(deployment_id, None)

    # -- verification (used by the self-test + an internal post-publish check) #

    def verify_version(
        self, deployment_id: str, version: "int | None" = None
    ) -> "vb.VerifyResult":
        """Verify a stored version against THIS signing home's trust root (I6).

        Delegates to the committed ``verify_bundle.verify_file`` (the exact code the
        agent runs), retargeted at the config-dist signing home.
        """
        deployment_id = self._safe_id(deployment_id)
        if version is None:
            version = self.current_version(deployment_id)
        vdir = self._version_dir(deployment_id, version)
        cfg_path = vdir / CONFIG_FILENAME
        with self.signing_home._retarget():
            return vb.verify_file(
                str(cfg_path),
                trust_root_path=str(self.signing_home.trust_root_path),
            )

    @property
    def trust_root_path(self) -> Path:
        return self.signing_home.trust_root_path
