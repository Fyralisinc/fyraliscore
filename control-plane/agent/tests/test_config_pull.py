"""Signed config pull: verify-before-apply, reject tampered/unknown-key (I6)."""

from __future__ import annotations

import json
from pathlib import Path

from config_pull import ConfigPuller
from conftest import make_config_bundle


def _staged_fetcher(bundle_path: Path):
    """A fetcher that serves a bundle from disk (stands in for an outbound GET)."""
    sig = (bundle_path.parent / (bundle_path.name + ".sig"))
    man = (bundle_path.parent / (bundle_path.name + ".manifest.json"))

    def _fetch(_url: str):
        return (
            bundle_path.read_bytes(),
            sig.read_text() if sig.is_file() else "",
            man.read_bytes() if man.is_file() else b"{}",
        )

    return _fetch


def _puller(fabric, fetcher, applied_dir: Path) -> ConfigPuller:
    return ConfigPuller(
        config_dir=applied_dir,
        trust_root_path=str(fabric.trust_root_path),
        fetcher=fetcher,
    )


def test_verified_config_is_applied(signing_fabric, tmp_path):
    bundle = make_config_bundle(
        signing_fabric, tmp_path / "bundle.json", payload={"interval_s": 99}, version="12"
    )
    applied = tmp_path / "applied"
    puller = _puller(signing_fabric, _staged_fetcher(bundle), applied)

    res = puller.pull_and_apply("https://console/config")
    assert res.ok and res.applied, res.reason
    assert res.version == "12"

    # The applied config is on disk and re-reads (re-verified) cleanly.
    loaded = puller.load_applied_config()
    assert loaded == {"interval_s": 99}


def test_tampered_config_is_rejected_and_not_applied(signing_fabric, tmp_path):
    bundle = make_config_bundle(signing_fabric, tmp_path / "bundle.json")
    # Tamper AFTER signing.
    bad = json.loads(bundle.read_text())
    bad["interval_s"] = 999999
    bundle.write_text(json.dumps(bad), encoding="utf-8")

    applied = tmp_path / "applied"
    puller = _puller(signing_fabric, _staged_fetcher(bundle), applied)
    res = puller.pull_and_apply("https://console/config")
    assert not res.ok and not res.applied
    assert "unverified" in res.reason.lower() or "invalid" in res.reason.lower()
    # Nothing was written.
    assert puller.load_applied_config() is None


def test_unknown_key_config_is_rejected(signing_fabric, tmp_path):
    from conftest import SigningFabric

    bundle = make_config_bundle(signing_fabric, tmp_path / "bundle.json")
    other = SigningFabric(tmp_path / "other", key_id="rogue")
    # Verify against a trust root that does NOT contain the signer.
    puller = ConfigPuller(
        config_dir=tmp_path / "applied",
        trust_root_path=str(other.trust_root_path),
        fetcher=_staged_fetcher(bundle),
    )
    res = puller.pull_and_apply("https://console/config")
    assert not res.ok and not res.applied


def test_existing_config_preserved_when_new_pull_rejected(signing_fabric, tmp_path):
    # First, a good apply.
    good = make_config_bundle(signing_fabric, tmp_path / "good.json", payload={"v": 1})
    applied = tmp_path / "applied"
    puller = _puller(signing_fabric, _staged_fetcher(good), applied)
    assert puller.pull_and_apply("https://c/cfg").ok
    assert puller.load_applied_config() == {"v": 1}

    # Now a BAD pull (unsigned) must not clobber the previously-applied config.
    bad = make_config_bundle(signing_fabric, tmp_path / "bad.json", payload={"v": 2}, sign=False)
    puller2 = _puller(signing_fabric, _staged_fetcher(bad), applied)
    res = puller2.pull_and_apply("https://c/cfg")
    assert not res.ok
    assert puller2.load_applied_config() == {"v": 1}  # old config survives


def test_transport_failure_is_nonfatal(signing_fabric, tmp_path):
    def _boom(_url):
        raise ConnectionError("console down")

    puller = ConfigPuller(
        config_dir=tmp_path / "applied",
        trust_root_path=str(signing_fabric.trust_root_path),
        fetcher=_boom,
    )
    res = puller.pull_and_apply("https://console/config")
    assert not res.ok and not res.applied
    assert "transport" in res.reason.lower()


def test_relabeled_manifest_is_rejected_i6(signing_fabric, tmp_path):
    """I6: a correctly config-signed bundle whose manifest is RELABELED (version swapped)
    while the artifact bytes are byte-for-byte unchanged must be rejected by the agent's
    verify-before-apply path (the v2 manifest binding no longer matches the signature)."""
    bundle = make_config_bundle(
        signing_fabric, tmp_path / "bundle.json", payload={"interval_s": 99}, version="12"
    )
    man_path = bundle.parent / (bundle.name + ".manifest.json")
    body_before = bundle.read_bytes()

    man = json.loads(man_path.read_text())
    assert man["sig_binding"] == "v2"
    man["version"] = "9999"  # relabel only the manifest version; do NOT touch the artifact
    man_path.write_text(json.dumps(man), encoding="utf-8")
    assert bundle.read_bytes() == body_before  # artifact bytes untouched

    puller = _puller(signing_fabric, _staged_fetcher(bundle), tmp_path / "applied")
    res = puller.pull_and_apply("https://console/config")
    assert not res.ok and not res.applied, res.reason
    assert puller.load_applied_config() is None


def test_release_bundle_not_accepted_as_config(signing_fabric, tmp_path):
    # Signed, but artifact kind is 'release' not 'config'.
    bundle = make_config_bundle(
        signing_fabric, tmp_path / "bundle.json", kind="release", version="3"
    )
    puller = _puller(signing_fabric, _staged_fetcher(bundle), tmp_path / "applied")
    res = puller.pull_and_apply("https://c/cfg")
    assert not res.ok
    assert "not config" in res.reason
