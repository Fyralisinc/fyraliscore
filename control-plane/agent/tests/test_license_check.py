"""License verification + expiry gate (I6 + license gate)."""

from __future__ import annotations

import datetime as _dt
import json
from pathlib import Path

from conftest import make_license
from license_check import LicenseChecker


def _checker(fabric, lic_path: Path) -> LicenseChecker:
    return LicenseChecker(lic_path, trust_root_path=str(fabric.trust_root_path))


def test_valid_signed_license_is_licensed(signing_fabric, tmp_path):
    lic = make_license(signing_fabric, tmp_path / "license.json", expires_in_days=365)
    chk = _checker(signing_fabric, lic)
    status = chk.evaluate()
    assert status.ok, status.reason
    assert chk.is_licensed()
    assert status.plan == "enterprise"
    assert status.expires_at is not None


def test_expired_license_is_not_licensed(signing_fabric, tmp_path):
    # Signed correctly, but expires_at is in the past.
    lic = make_license(
        signing_fabric, tmp_path / "license.json", expires_in_days=-1, issued_days_ago=10
    )
    chk = _checker(signing_fabric, lic)
    status = chk.evaluate()
    assert not status.ok
    assert "EXPIRED" in status.reason
    assert chk.is_licensed() is False
    # Expiry is still surfaced (so the heartbeat can stamp it -> red health).
    assert status.expires_at is not None and status.expired


def test_tampered_license_body_is_rejected(signing_fabric, tmp_path):
    lic = make_license(signing_fabric, tmp_path / "license.json")
    # Edit the license AFTER signing: bytes no longer match the signature.
    body = json.loads(lic.read_text())
    body["plan"] = "free-but-i-said-enterprise"
    lic.write_text(json.dumps(body, indent=2), encoding="utf-8")
    chk = _checker(signing_fabric, lic)
    assert chk.is_licensed() is False
    assert "rejected" in chk.evaluate().reason.lower()


def test_missing_signature_is_rejected(signing_fabric, tmp_path):
    lic = make_license(signing_fabric, tmp_path / "license.json", sign=False)
    chk = _checker(signing_fabric, lic)
    assert chk.is_licensed() is False


def test_license_signed_by_unknown_key_is_rejected(signing_fabric, tmp_path):
    # Sign with fabric A, but verify against a DIFFERENT trust root (key unknown).
    from conftest import SigningFabric

    lic = make_license(signing_fabric, tmp_path / "license.json")
    other = SigningFabric(tmp_path / "other", key_id="some-other-key")
    chk = LicenseChecker(lic, trust_root_path=str(other.trust_root_path))
    assert chk.is_licensed() is False


def test_not_yet_in_effect_license_is_rejected(signing_fabric, tmp_path):
    # issued_at in the future.
    lic = make_license(
        signing_fabric, tmp_path / "license.json", issued_days_ago=-5, expires_in_days=365
    )
    chk = _checker(signing_fabric, lic)
    status = chk.evaluate()
    assert not status.ok
    assert "not yet in effect" in status.reason


def test_config_bundle_is_not_accepted_as_license(signing_fabric, tmp_path):
    # A correctly-signed CONFIG bundle must not be honored as a license.
    from conftest import make_config_bundle

    cfg = make_config_bundle(signing_fabric, tmp_path / "license.json", kind="config")
    chk = _checker(signing_fabric, cfg)
    assert chk.is_licensed() is False
    assert "not a license" in chk.evaluate().reason


def test_expiry_flips_while_running(signing_fabric, tmp_path):
    """A license valid now but expiring soon evaluates as expired at a later 'now'."""
    lic = make_license(signing_fabric, tmp_path / "license.json", expires_in_days=1)
    chk = _checker(signing_fabric, lic)
    assert chk.is_licensed() is True
    future = _dt.datetime.now(_dt.timezone.utc) + _dt.timedelta(days=2)
    assert chk.is_licensed(now=future) is False
