"""Shared test fabric for the agent test-suite.

Everything is real and self-contained — no Docker, no external services, no
touching the committed ``signing/trust_root.json``:

* a **throwaway ed25519 keyring** minted in ``tmp_path`` with its own
  ``trust_root.json`` (the agent verifies against *this*, so tests never depend
  on a key existing in the repo);
* helpers to **sign** a license / config bundle exactly as
  ``signing/sign_bundle.py`` does (``<file>``, ``<file>.sig``,
  ``<file>.manifest.json``), so ``verify_bundle.verify_file`` accepts them;
* a fake, in-process console (``FakeConsole``) that records the heartbeats it
  receives — used both as an injected ``sender`` (unit) and behind a real
  loopback HTTP server (integration).
"""

from __future__ import annotations

import datetime as _dt
import json
import sys
from pathlib import Path

import pytest

# Make the agent package + its siblings importable (same convention as the daemon).
_AGENT_DIR = Path(__file__).resolve().parent.parent
_CP_ROOT = _AGENT_DIR.parent
_SIGNING_DIR = _CP_ROOT / "signing"
for _p in (str(_AGENT_DIR), str(_CP_ROOT), str(_SIGNING_DIR)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import signing_lib as sl  # noqa: E402


# --------------------------------------------------------------------------- #
# Throwaway signing keyring + trust root                                       #
# --------------------------------------------------------------------------- #


class SigningFabric:
    """A test signer: holds an active ed25519 key + writes a trust_root.json.

    ``sign(path, kind, version)`` writes ``<path>.sig`` + ``<path>.manifest.json``
    for the file at ``path``, using the same canonical bytes / manifest shape as
    the production ``sign_bundle.py`` — so the agent's ``verify_bundle`` accepts it.
    """

    def __init__(self, base: Path, key_id: str = "cp-test-2026-06") -> None:
        self.base = Path(base)
        self.base.mkdir(parents=True, exist_ok=True)
        self.key_id = key_id
        self.ring = sl.Keyring()
        self.entry = self.ring.generate_active_key(key_id)
        self.trust_root_path = base / "trust_root.json"
        self._write_trust_root()

    def _write_trust_root(self) -> None:
        self.trust_root_path.write_text(
            json.dumps(self.ring.to_trust_root(), indent=2, sort_keys=True),
            encoding="utf-8",
        )

    def rotate(self, new_key_id: str) -> None:
        """Rotate to a new active key and refresh the trust root (retires the old)."""
        self.entry = self.ring.rotate_to(new_key_id)
        self.key_id = new_key_id
        self._write_trust_root()

    def sign(self, path: "str | Path", *, kind: str, version: str = "1") -> Path:
        path = Path(path)
        signed_bytes = sl.canonical_bytes_for_file(str(path), kind)
        key_id, raw_sig = self.ring.sign_with_active(signed_bytes)
        (path.parent / (path.name + ".sig")).write_text(
            sl.b64e(raw_sig) + "\n", encoding="utf-8"
        )
        manifest = sl.build_manifest(
            artifact_kind=kind, version=str(version), signed_bytes=signed_bytes, key_id=key_id
        )
        (path.parent / (path.name + ".manifest.json")).write_text(
            json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
        return path


@pytest.fixture
def signing_fabric(tmp_path: Path) -> SigningFabric:
    return SigningFabric(tmp_path)


# --------------------------------------------------------------------------- #
# License helpers                                                              #
# --------------------------------------------------------------------------- #


def _rfc3339(dt: _dt.datetime) -> str:
    return dt.astimezone(_dt.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def make_license(
    fabric: SigningFabric,
    path: Path,
    *,
    tenant_id: str = "acme",
    deployment_id: str = "acme-use1-0001",
    plan: str = "enterprise",
    expires_in_days: int = 365,
    issued_days_ago: int = 1,
    features: list[str] | None = None,
    sign: bool = True,
) -> Path:
    """Write a (by default signed) license bundle to ``path``."""
    now = _dt.datetime.now(_dt.timezone.utc)
    body = {
        "tenant_id": tenant_id,
        "deployment_id": deployment_id,
        "plan": plan,
        "issued_at": _rfc3339(now - _dt.timedelta(days=issued_days_ago)),
        "expires_at": _rfc3339(now + _dt.timedelta(days=expires_in_days)),
        "features": features if features is not None else ["fleet", "anomaly"],
    }
    path.write_text(json.dumps(body, indent=2), encoding="utf-8")
    if sign:
        fabric.sign(path, kind="license", version=body["expires_at"])
    return path


def make_config_bundle(
    fabric: SigningFabric,
    path: Path,
    *,
    payload: dict | None = None,
    version: str = "7",
    sign: bool = True,
    kind: str = "config",
) -> Path:
    """Write a (by default signed) config bundle to ``path``."""
    body = payload if payload is not None else {"interval_s": 45, "telemetry_tier": "T1"}
    path.write_text(json.dumps(body, indent=2), encoding="utf-8")
    if sign:
        fabric.sign(path, kind=kind, version=version)
    return path


# --------------------------------------------------------------------------- #
# Fake in-process console                                                      #
# --------------------------------------------------------------------------- #


class FakeConsole:
    """Records heartbeats. Usable as an injected sender or behind an HTTP server.

    ``up`` toggles availability: when down, the sender returns False / the HTTP
    route 503s, simulating an unreachable console so the agent must buffer (I3).
    """

    def __init__(self) -> None:
        self.received: list[dict] = []
        self.up = True

    def sender(self, record: dict) -> bool:
        """Injectable ``HeartbeatSender``: True if 'delivered', False if down."""
        if not self.up:
            return False
        self.received.append(dict(record))
        return True

    @property
    def deployment_ids(self) -> list[str]:
        return [r.get("deployment_id") for r in self.received]


@pytest.fixture
def fake_console() -> FakeConsole:
    return FakeConsole()
