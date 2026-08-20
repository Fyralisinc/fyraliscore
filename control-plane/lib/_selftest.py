"""Self-test for the control-plane shared library.

Run with the control-plane root on sys.path so ``lib`` imports as a package::

    cd control-plane && python -m lib._selftest

Exercises, end to end:
  * DeploymentRecord construction + C4 round-trip (model → dict → model)
  * health derivation across fresh / stale / dead / expired-license cases
  * TelemetryTier policy (cumulative permits + redaction obligations)
  * TenantRegistry reader round-trip against a tiny sample registry under /tmp,
    including active / revoked / unknown decisions

It writes ONLY under /tmp (never into ca/). Exit code 0 = all assertions held.
"""

from __future__ import annotations

import datetime as _dt
import json
import tempfile
from pathlib import Path

from lib import (
    DeploymentRecord,
    Health,
    SignalClass,
    TelemetryTier,
    TenantRegistry,
    derive_health,
    fingerprint_der,
    tier_policy,
)
from lib.errors import (
    TenantNotFoundError,
    TenantRevokedError,
)
from lib.primitives import to_rfc3339, utcnow


def _check(cond: bool, msg: str) -> None:
    if not cond:
        raise AssertionError(msg)
    print(f"  ok: {msg}")


def test_deployment_and_health() -> None:
    print("[deployment + health]")
    now = _dt.datetime(2026, 6, 24, 0, 0, 0, tzinfo=_dt.timezone.utc)
    rec = DeploymentRecord.heartbeat(
        tenant_id="acme",
        deployment_id="acme-use1-7f3a",
        version="1.4.2",
        region="us-east-1",
        license_expiry="2027-06-24T00:00:00Z",
        telemetry_tier="T1",
        now=now,
    )
    _check(rec.health is Health.GREEN, "fresh heartbeat derives green")
    _check(rec.telemetry_tier is TelemetryTier.T1, "tier parsed to T1")

    # C4 round-trip: model -> exact wire dict -> model.
    wire = rec.to_registry_dict()
    _check(
        set(wire) == {
            "tenant_id", "deployment_id", "version", "region",
            "last_heartbeat_ts", "health", "license_expiry", "telemetry_tier",
        },
        "C4 dict has exactly the contract fields",
    )
    _check(wire["health"] == "green", "health serializes as 'green'")
    _check(wire["telemetry_tier"] == "T1", "tier serializes as 'T1'")
    _check(wire["last_heartbeat_ts"].endswith("Z"), "ts serializes as RFC-3339 Z")
    again = DeploymentRecord(**wire)
    _check(again.to_registry_dict() == wire, "C4 round-trip is stable")

    # Health derivation matrix.
    fresh = to_rfc3339(now - _dt.timedelta(seconds=10))
    stale = to_rfc3339(now - _dt.timedelta(seconds=120))
    dead = to_rfc3339(now - _dt.timedelta(seconds=900))
    _check(derive_health(fresh, now=now) is Health.GREEN, "10s old → green")
    _check(derive_health(stale, now=now) is Health.YELLOW, "120s old → yellow")
    _check(derive_health(dead, now=now) is Health.RED, "900s old → red")
    _check(
        derive_health(fresh, now=now, sli_breached=True) is Health.YELLOW,
        "fresh + SLI breach → yellow",
    )
    _check(
        derive_health(
            fresh, now=now, license_expiry="2020-01-01T00:00:00Z"
        )
        is Health.RED,
        "fresh but expired license → red",
    )
    future = to_rfc3339(now + _dt.timedelta(seconds=60))
    _check(derive_health(future, now=now) is Health.GREEN, "future heartbeat → green (skew clamp)")

    # Re-derivation on the read path: a once-green record gone silent reads red.
    silent = DeploymentRecord(**{**wire, "last_heartbeat_ts": dead})
    _check(
        silent.with_derived_health(now=now).health is Health.RED,
        "console re-derives a silent deployment to red",
    )


def test_tiers() -> None:
    print("[telemetry tiers]")
    t1 = tier_policy("T1")
    t2 = tier_policy(TelemetryTier.T2)
    t3 = tier_policy("t3")

    _check(t1.permits(SignalClass.METRICS), "T1 permits metrics")
    _check(not t1.permits(SignalClass.LOGS), "T1 drops logs")
    _check(not t1.permits(SignalClass.TRACES), "T1 drops traces")
    _check(not t1.carries_pii_risk(), "T1 carries zero-PII risk (I1)")

    _check(t2.permits(SignalClass.LOGS), "T2 permits logs (cumulative over T1)")
    _check(t2.permits(SignalClass.METRICS), "T2 still permits metrics")
    _check(not t2.permits(SignalClass.TRACES), "T2 drops traces")
    _check(t2.requires_redaction(SignalClass.LOGS), "T2 logs must be redacted")
    _check(not t2.requires_redaction(SignalClass.METRICS), "T2 metrics not redacted")

    _check(t3.permits(SignalClass.TRACES), "T3 permits traces")
    _check(t3.requires_redaction(SignalClass.TRACES), "T3 traces must be redacted")
    _check(TelemetryTier.T3.includes(TelemetryTier.T1), "T3 includes T1 (cumulative)")
    _check(not TelemetryTier.T1.includes(TelemetryTier.T2), "T1 does not include T2")


def test_registry_roundtrip() -> None:
    print("[tenant registry round-trip]")
    # Build a tiny sample registry under /tmp (NEVER under ca/).
    fp_active = fingerprint_der(b"acme-leaf-cert-der-bytes")
    fp_revoked = fingerprint_der(b"globex-leaf-cert-der-bytes")
    fp_unknown = fingerprint_der(b"never-issued-cert")

    sample = {
        fp_active: {
            "tenant_id": "acme",
            "issued_at": "2026-06-24T00:00:00Z",
            "status": "active",
        },
        fp_revoked: {
            "tenant_id": "globex",
            "issued_at": "2026-05-01T00:00:00Z",
            "status": "revoked",
        },
    }

    tmpdir = Path(tempfile.mkdtemp(prefix="cp-selftest-registry-"))
    reg_path = tmpdir / "tenant_registry.json"
    reg_path.write_text(json.dumps(sample, indent=2), encoding="utf-8")
    print(f"  wrote sample registry: {reg_path}")

    reg = TenantRegistry(registry_path=reg_path)
    _check(len(reg) == 2, "registry loaded 2 rows")

    # Active → resolves to tenant id.
    _check(reg.tenant_for_fingerprint(fp_active) == "acme", "active fp → acme")
    _check(reg.is_active(fp_active) is True, "active fp is_active True")
    _check(reg.is_revoked(fp_active) is False, "active fp is_revoked False")

    # Case/format-insensitive lookup (uppercase + colons).
    colonized = ":".join(
        fp_active.upper()[i : i + 2] for i in range(0, len(fp_active), 2)
    )
    _check(
        reg.tenant_for_fingerprint(colonized) == "acme",
        "uppercase+colon fp still resolves",
    )

    # Revoked → rejected (403 at the proxy).
    try:
        reg.tenant_for_fingerprint(fp_revoked)
        raise AssertionError("revoked fp should have raised")
    except TenantRevokedError:
        _check(True, "revoked fp → TenantRevokedError")
    _check(reg.is_revoked(fp_revoked) is True, "revoked fp is_revoked True")
    _check(reg.is_active(fp_revoked) is False, "revoked fp is_active False")

    # Unknown → rejected.
    try:
        reg.tenant_for_fingerprint(fp_unknown)
        raise AssertionError("unknown fp should have raised")
    except TenantNotFoundError:
        _check(True, "unknown fp → TenantNotFoundError")
    _check(reg.is_active(fp_unknown) is False, "unknown fp is_active False")
    # is_revoked is now a FAIL-CLOSED deny predicate (matches ca/registry):
    # an unknown fingerprint is "not authorized" ⇒ True. (See lib/test_tenant_failclosed.py)
    _check(reg.is_revoked(fp_unknown) is True, "unknown fp is_revoked True (fail-closed)")

    # Live revocation pickup (mtime cache invalidation): flip active → revoked.
    sample[fp_active]["status"] = "revoked"
    import time as _t

    _t.sleep(0.01)  # ensure mtime advances
    reg_path.write_text(json.dumps(sample, indent=2), encoding="utf-8")
    try:
        reg.tenant_for_fingerprint(fp_active)
        raise AssertionError("re-read should see the revocation")
    except TenantRevokedError:
        _check(True, "out-of-band revocation is picked up on next read")

    # Fail-closed on an unknown status value.
    bad = tmpdir / "bad_registry.json"
    bad.write_text(
        json.dumps({fp_active: {"tenant_id": "acme",
                                "issued_at": "2026-06-24T00:00:00Z",
                                "status": "pending"}}),
        encoding="utf-8",
    )
    from lib.errors import RegistryFormatError

    try:
        TenantRegistry(registry_path=bad).fingerprints()
        raise AssertionError("unknown status should fail closed")
    except RegistryFormatError:
        _check(True, "unknown status fails closed (RegistryFormatError)")


def main() -> int:
    print(f"control-plane lib self-test @ {to_rfc3339(utcnow())}")
    test_deployment_and_health()
    test_tiers()
    test_registry_roundtrip()
    print("\nALL SELF-TESTS PASSED")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
