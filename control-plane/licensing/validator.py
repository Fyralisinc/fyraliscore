#!/usr/bin/env python3
"""validator.py — local, fail-closed license validator the data-plane agent runs (I6 + FR-F).

``validate()`` is the single decision point: it answers "is this deployment allowed to
operate right now?" and is what the agent calls on boot and on every heartbeat. It is
**fail-closed**: anything it cannot positively prove valid → DENY. The four gates, ALL of
which must pass for ALLOW:

  1. SIGNATURE  — the license bundle verifies against the signing **trust root** via the
                  real ``signing/verify_bundle`` (ed25519, known+non-retired key, sha256
                  cross-check). A tampered field, wrong/unknown key, or missing sig → DENY.
  2. EXPIRY     — ``now < expires_at`` (the boundary instant counts as expired). An
                  already-expired or not-yet-valid (issued_at in the future) license → DENY.
  3. IDENTITY   — the license ``tenant_id`` / ``deployment_id`` match the deployment this
                  agent IS (passed in by the caller, or read from a DeploymentRecord). A
                  license minted for another tenant/deployment → DENY (lateral-reuse guard).
  4. REVOCATION — the license is NOT on the revocation list (``revoke.is_revoked``). This is
                  the only way to pull a still-signed, still-unexpired license (FR-F).

Why the signature check is first and unconditional: per I6 we *verify before use*. We do not
parse the license fields and trust them before the signature has been checked — the verified
on-disk ``license.json`` IS the source of the fields we then policy-check, so an attacker
cannot move expiry/tenant by editing the file (that breaks the signature → gate 1 denies).

Programmatic use (what the agent imports)::

    from validator import validate
    d = validate(license_dir="/etc/fyralis/license",
                 expected_tenant_id="acme", expected_deployment_id="acme-use1-7f3a")
    if not d.allow:
        log.error("license denied: %s", d.reason); refuse_to_operate()

CLI::

    python validator.py validate /etc/fyralis/license \
        --tenant-id acme --deployment-id acme-use1-7f3a
"""

from __future__ import annotations

import argparse
import datetime as _dt
import json
import os
import sys
from dataclasses import dataclass, field

HERE = os.path.dirname(os.path.abspath(__file__))
SIGNING_DIR = os.path.normpath(os.path.join(HERE, "..", "signing"))
for _p in (HERE, SIGNING_DIR):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from license_model import License, parse_rfc3339  # noqa: E402
import revoke as revoke_mod  # noqa: E402  (licensing/revoke.py — the LRL)

# The REAL verify path (ed25519 + trust root + key policy + sha256), I6.
import verify_bundle as vb  # noqa: E402  (control-plane/signing/verify_bundle.py)
import sign_bundle as sb  # noqa: E402  (for the default trust-root path)

DEFAULT_TRUST_ROOT_PATH = sb.TRUST_ROOT_PATH


@dataclass
class Decision:
    """The allow/deny result with a clear, single-sentence reason and a machine ``code``."""

    allow: bool
    reason: str
    code: str  # one of the CODE_* below
    license: dict | None = None
    checks: dict = field(default_factory=dict)  # per-gate booleans for observability

    @property
    def deny(self) -> bool:
        return not self.allow

    def to_dict(self) -> dict:
        return {
            "allow": self.allow,
            "reason": self.reason,
            "code": self.code,
            "license": self.license,
            "checks": self.checks,
        }


# Decision codes (stable strings for logs/metrics).
CODE_ALLOW = "allow"
CODE_NO_BUNDLE = "deny_no_bundle"
CODE_BAD_SIGNATURE = "deny_bad_signature"
CODE_MALFORMED = "deny_malformed_license"
CODE_WRONG_ARTIFACT = "deny_wrong_artifact_kind"
CODE_EXPIRED = "deny_expired"
CODE_NOT_YET_VALID = "deny_not_yet_valid"
CODE_TENANT_MISMATCH = "deny_tenant_mismatch"
CODE_DEPLOYMENT_MISMATCH = "deny_deployment_mismatch"
CODE_REVOKED = "deny_revoked"
CODE_REVLIST_ERROR = "deny_revocation_list_unreadable"


def _deny(code: str, reason: str, *, license: dict | None = None, checks: dict | None = None) -> Decision:
    return Decision(False, reason, code, license=license, checks=checks or {})


def validate(
    *,
    license_dir: str | None = None,
    license_path: str | None = None,
    expected_tenant_id: str | None = None,
    expected_deployment_id: str | None = None,
    trust_root_path: str | None = None,
    revocations_path: str | None = None,
    now: _dt.datetime | None = None,
    skew_seconds: int = 0,
    allow_retired_key: bool = False,
) -> Decision:
    """Validate a signed license bundle, fail-closed. Returns a :class:`Decision`.

    Supply EITHER ``license_dir`` (expects ``license.json`` + sidecars inside) OR an
    explicit ``license_path``. ``expected_tenant_id`` / ``expected_deployment_id`` are the
    identity of the deployment doing the validating; if omitted, the IDENTITY gate is
    *skipped* (use :func:`validate_for_deployment` to derive them from a DeploymentRecord and
    never skip). All other gates always run.

    This function never raises for a *bad* license — every failure becomes a DENY Decision.
    It only raises for a genuinely unreadable trust root (an operator/config error), which is
    itself fail-closed at the call site if wrapped.
    """
    now = now or _dt.datetime.now(_dt.timezone.utc)
    trust_root_path = trust_root_path or DEFAULT_TRUST_ROOT_PATH
    revocations_path = revocations_path or revoke_mod.DEFAULT_REVOCATIONS_PATH

    # Resolve the license path.
    if license_path is None:
        if license_dir is None:
            return _deny(CODE_NO_BUNDLE, "no license_dir or license_path supplied")
        license_path = os.path.join(license_dir, "license.json")
    if not os.path.isfile(license_path):
        return _deny(CODE_NO_BUNDLE, f"license file not found: {license_path}")

    checks: dict = {
        "signature": False,
        "expiry": False,
        "identity": False,
        "revocation": False,
    }

    # -- GATE 1: SIGNATURE (verify before use, I6) ------------------------------------- #
    # Crucially we verify the on-disk bytes FIRST, before trusting any field in them.
    vres = vb.verify_file(
        license_path,
        trust_root_path=trust_root_path,
        allow_retired=allow_retired_key,
    )
    if not vres.ok:
        return _deny(
            CODE_BAD_SIGNATURE,
            f"signature/trust check failed: {vres.reason}",
            checks=checks,
        )
    if vres.artifact != "license":
        # A signed config/release is not a license — refuse to treat it as one.
        return _deny(
            CODE_WRONG_ARTIFACT,
            f"bundle is a {vres.artifact!r} artifact, not a license",
            checks=checks,
        )
    checks["signature"] = True

    # Signature verified the file bytes → it is now safe to parse the fields.
    try:
        lic = License.from_file(license_path)
    except Exception as exc:
        return _deny(CODE_MALFORMED, f"license JSON malformed: {exc}", checks=checks)
    lic_dict = lic.to_dict()

    # -- GATE 2: EXPIRY (+ not-yet-valid) ---------------------------------------------- #
    issued = lic.issued_dt
    if issued > (now + _dt.timedelta(seconds=skew_seconds)):
        return _deny(
            CODE_NOT_YET_VALID,
            f"license not yet valid: issued_at {lic.issued_at} is in the future",
            license=lic_dict,
            checks=checks,
        )
    if lic.is_expired(now=now, skew_seconds=skew_seconds):
        return _deny(
            CODE_EXPIRED,
            f"license expired at {lic.expires_at} (now {_fmt(now)})",
            license=lic_dict,
            checks=checks,
        )
    checks["expiry"] = True

    # -- GATE 3: IDENTITY (tenant + deployment match) ---------------------------------- #
    if expected_tenant_id is not None and lic.tenant_id != expected_tenant_id:
        return _deny(
            CODE_TENANT_MISMATCH,
            f"tenant mismatch: license is for tenant {lic.tenant_id!r}, "
            f"this deployment is {expected_tenant_id!r}",
            license=lic_dict,
            checks=checks,
        )
    if expected_deployment_id is not None and lic.deployment_id != expected_deployment_id:
        return _deny(
            CODE_DEPLOYMENT_MISMATCH,
            f"deployment mismatch: license is for {lic.deployment_id!r}, "
            f"this deployment is {expected_deployment_id!r}",
            license=lic_dict,
            checks=checks,
        )
    checks["identity"] = True

    # -- GATE 4: REVOCATION (FR-F) ----------------------------------------------------- #
    try:
        hit = revoke_mod.revocation_match(lic, path=revocations_path)
    except Exception as exc:
        # An unreadable/corrupt LRL must NOT silently un-revoke — fail closed.
        return _deny(
            CODE_REVLIST_ERROR,
            f"revocation list unreadable (failing closed): {exc}",
            license=lic_dict,
            checks=checks,
        )
    if hit is not None:
        return _deny(
            CODE_REVOKED,
            f"license revoked: matched {hit.get('type')}={hit.get('value')!r}"
            + (f" (reason: {hit['reason']})" if hit.get("reason") else ""),
            license=lic_dict,
            checks=checks,
        )
    checks["revocation"] = True

    # All four gates passed.
    return Decision(
        True,
        f"license valid for tenant {lic.tenant_id!r} deployment {lic.deployment_id!r} "
        f"(plan {lic.plan}, expires {lic.expires_at})",
        CODE_ALLOW,
        license=lic_dict,
        checks=checks,
    )


def validate_for_deployment(
    deployment_record: dict,
    *,
    license_dir: str | None = None,
    license_path: str | None = None,
    trust_root_path: str | None = None,
    revocations_path: str | None = None,
    now: _dt.datetime | None = None,
    skew_seconds: int = 0,
) -> Decision:
    """Validate against the identity in a C4 ``DeploymentRecord`` dict.

    This is the agent's real call: the deployment knows *who it is* from its record, so the
    IDENTITY gate is never skipped. Expects ``deployment_record`` to carry ``tenant_id`` and
    ``deployment_id`` (the DeploymentRecord wire shape from ``lib.deployment``).
    """
    tid = deployment_record.get("tenant_id")
    did = deployment_record.get("deployment_id")
    if not tid or not did:
        return _deny(
            CODE_DEPLOYMENT_MISMATCH,
            "deployment record missing tenant_id/deployment_id — cannot bind license identity",
        )
    return validate(
        license_dir=license_dir,
        license_path=license_path,
        expected_tenant_id=tid,
        expected_deployment_id=did,
        trust_root_path=trust_root_path,
        revocations_path=revocations_path,
        now=now,
        skew_seconds=skew_seconds,
    )


def _fmt(dt: _dt.datetime) -> str:
    return dt.astimezone(_dt.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Validate a signed license bundle (fail-closed).")
    sub = ap.add_subparsers(dest="cmd", required=True)
    vp = sub.add_parser("validate", help="validate a license bundle directory (or file)")
    vp.add_argument("bundle", help="path to the bundle dir (with license.json) or the license.json file")
    vp.add_argument("--tenant-id", default=None, help="expected tenant_id (identity gate)")
    vp.add_argument("--deployment-id", default=None, help="expected deployment_id (identity gate)")
    vp.add_argument("--trust-root", default=None, help=f"trust root path (default {DEFAULT_TRUST_ROOT_PATH})")
    vp.add_argument("--revocations", default=None, help="revocation list path")
    vp.add_argument("--skew-seconds", type=int, default=0, help="allowed clock skew for expiry")
    vp.add_argument("--now", default=None, help="override 'now' (RFC-3339) for testing")
    vp.add_argument("--json", action="store_true", help="emit the Decision as JSON")
    args = ap.parse_args(argv)

    bundle = args.bundle
    if os.path.isdir(bundle):
        kwargs = {"license_dir": bundle}
    else:
        kwargs = {"license_path": bundle}

    now = parse_rfc3339(args.now) if args.now else None

    d = validate(
        expected_tenant_id=args.tenant_id,
        expected_deployment_id=args.deployment_id,
        trust_root_path=args.trust_root,
        revocations_path=args.revocations,
        skew_seconds=args.skew_seconds,
        now=now,
        **kwargs,
    )

    if args.json:
        print(json.dumps(d.to_dict(), indent=2, sort_keys=True))
    else:
        verdict = "ALLOW" if d.allow else "DENY"
        print(f"{verdict} [{d.code}]: {d.reason}")
    return 0 if d.allow else 1


if __name__ == "__main__":
    raise SystemExit(main())
