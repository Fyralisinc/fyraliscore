#!/usr/bin/env python3
"""onboard.py — the atomic per-tenant onboarding transaction (FR-E).

    onboard --tenant acme --region us-east --plan standard

Runs the BYOC enrollment as an **all-or-nothing transaction**. On success the
customer gets an *agent bundle* directory they deploy into their VPC; on **any**
step failure every side effect already applied is **rolled back** so no
half-onboarded state remains (FR-E).

Steps (each registers an undo before the next runs)
---------------------------------------------------
 1. REGISTER  — POST /api/v1/register to the console to mint a ``deployment_id``
                (and ``tenant_id`` if not supplied); or, with ``--local-ids``,
                mint them locally without a console.
 2. CERT      — issue a per-tenant mTLS client cert via ``ca/issue_cert`` (SAN =
                ``spiffe://fyralis/tenant/<tenant>``). This *adds the row to
                ``ca/tenant_registry.json``*, which is what makes the auth-proxy
                accept the cert — issuing the cert **is** the proxy binding.
                  undo: revoke the cert AND delete its registry row.
 3. LICENSE   — mint + ed25519-sign a license via ``signing`` (verify-before-use).
                  undo: (covered by deleting the bundle).
 4. BUNDLE    — assemble the agent bundle dir: tenant cert+key+chain, the signed
                license (+sig+manifest), the public trust root, and a signed agent
                config pointing at the console.
                  undo: delete the bundle dir.
 5. HEARTBEAT — push an initial DeploymentRecord (C4) so the deployment appears in
                the console immediately (the agent re-heartbeats from the DP).
 6. CONFIRM   — GET /api/v1/deployments/{id}; assert the deployment is listed.

If a console is used (not ``--local-ids``) and a *later* step fails, the rollback
also best-effort removes the deployment from the console.

Atomicity model
---------------
A small in-process **undo ledger** (LIFO). Each successful step pushes a
``(label, undo_callable)``. On exception we run the undos newest-first, logging
each, then re-raise the original failure. Rollback itself is best-effort and
never masks the original error: a failed undo is logged and the rest still run.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
import traceback
from dataclasses import dataclass, field
from typing import Callable, Optional

_HERE = os.path.dirname(os.path.abspath(__file__))
_CP_ROOT = os.path.dirname(_HERE)
_CA_DIR = os.path.join(_CP_ROOT, "ca")
_SIGNING_DIR = os.path.join(_CP_ROOT, "signing")
for _p in (_CP_ROOT, _CA_DIR, _SIGNING_DIR):
    if _p not in sys.path:
        sys.path.insert(0, _p)

# CA (committed P1 primitives)
import issue_cert  # noqa: E402  (ca/issue_cert.py)
import registry as ca_registry  # noqa: E402  (ca/registry.py)
import revoke as ca_revoke  # noqa: E402  (ca/revoke.py)
import sign_bundle  # noqa: E402  (signing/sign_bundle.py)

from lib.deployment import DeploymentRecord  # noqa: E402
from lib.errors import ControlPlaneError  # noqa: E402
from lib.primitives import to_rfc3339, utcnow  # noqa: E402

import console_client as cc  # noqa: E402  (onboarding/console_client.py)
import license_mint as lm  # noqa: E402  (onboarding/license_mint.py)

__all__ = [
    "OnboardError",
    "RollbackLedger",
    "onboard",
    "OnboardResult",
]

DEFAULT_BUNDLES_ROOT = os.path.join(_HERE, "bundles")
DEFAULT_PKI_DIR = os.path.join(_CA_DIR, "pki")
DEFAULT_REGISTRY_PATH = ca_registry.DEFAULT_REGISTRY_PATH
DEFAULT_TRUST_ROOT = os.path.join(_SIGNING_DIR, "trust_root.json")
DEFAULT_VERSION = "0.1.0"


class OnboardError(ControlPlaneError):
    """An onboarding step failed (after rollback has been attempted)."""


def _log(msg: str) -> None:
    print(msg, flush=True)


# --------------------------------------------------------------------------- #
# Rollback ledger                                                             #
# --------------------------------------------------------------------------- #
class RollbackLedger:
    """LIFO ledger of undo actions for the onboarding transaction.

    Push a ``(label, fn)`` after each successful side effect. On failure call
    :meth:`roll_back` to run them newest-first; each undo is best-effort and an
    undo that itself raises is logged but does not stop the remaining undos.
    """

    def __init__(self) -> None:
        self._undos: list[tuple[str, Callable[[], None]]] = []
        self.committed = False

    def push(self, label: str, fn: Callable[[], None]) -> None:
        self._undos.append((label, fn))

    def commit(self) -> None:
        """Mark the transaction successful — undos will not run."""
        self.committed = True

    def roll_back(self) -> list[str]:
        """Run undos newest-first. Returns the labels that were rolled back."""
        done: list[str] = []
        for label, fn in reversed(self._undos):
            try:
                fn()
                _log(f"  rollback: undid {label}")
                done.append(label)
            except Exception as exc:  # best-effort: log and keep going
                _log(f"  rollback: FAILED to undo {label}: {exc}")
        self._undos.clear()
        return done


# --------------------------------------------------------------------------- #
# Registry helpers (delete a row for clean rollback — revoke only flips status) #
# --------------------------------------------------------------------------- #
def _delete_registry_row(fingerprint: str, *, registry_path: str) -> bool:
    """Remove a fingerprint's row from the registry JSON (atomic write)."""
    fp = ca_registry._normalize_fp(fingerprint)
    reg = ca_registry.load_registry(registry_path)
    if fp in reg:
        del reg[fp]
        ca_registry.save_registry(reg, registry_path)
        return True
    return False


def _teardown_cert(fingerprint: str, tenant_id: str, *, registry_path: str) -> None:
    """Undo a cert issuance: revoke (proxy stops accepting it) then delete the row.

    Revoking first means that even if the row-delete were to fail, the proxy
    already rejects the cert (fail-closed). Deleting the row then leaves the
    registry exactly as it was before onboarding (no ``acme`` entry remains).
    """
    try:
        ca_revoke.revoke(fingerprint, registry_path=registry_path)
    except SystemExit:
        # revoke() raises SystemExit if the row is already gone — fine for undo.
        pass
    _delete_registry_row(fingerprint, registry_path=registry_path)


# --------------------------------------------------------------------------- #
# Result                                                                      #
# --------------------------------------------------------------------------- #
@dataclass
class OnboardResult:
    tenant_id: str
    deployment_id: str
    region: str
    plan: str
    bundle_dir: str
    fingerprint: str
    license_expiry: str
    telemetry_tier: str
    used_console: bool
    files: list[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "tenant_id": self.tenant_id,
            "deployment_id": self.deployment_id,
            "region": self.region,
            "plan": self.plan,
            "bundle_dir": self.bundle_dir,
            "fingerprint": self.fingerprint,
            "license_expiry": self.license_expiry,
            "telemetry_tier": self.telemetry_tier,
            "used_console": self.used_console,
            "files": self.files,
        }


# --------------------------------------------------------------------------- #
# The transaction                                                             #
# --------------------------------------------------------------------------- #
def onboard(
    *,
    tenant: str,
    region: str,
    plan: str,
    console_url: Optional[str] = None,
    console_app: object = None,
    local_ids: bool = False,
    bundles_root: str = DEFAULT_BUNDLES_ROOT,
    pki_dir: str = DEFAULT_PKI_DIR,
    registry_path: str = DEFAULT_REGISTRY_PATH,
    trust_root_path: str = DEFAULT_TRUST_ROOT,
    version: str = DEFAULT_VERSION,
    telemetry_tier: str = "T1",
    cert_valid_days: int = 90,
    license_valid_days: int = lm.DEFAULT_LICENSE_DAYS,
    console_token: Optional[str] = None,
    fail_after: Optional[str] = None,
) -> OnboardResult:
    """Run the atomic onboarding transaction. Returns :class:`OnboardResult`.

    Identity source:
      * ``local_ids=True``        -> mint tenant_id/deployment_id locally (no console).
      * ``console_app`` given     -> register against an in-process console (self-test).
      * ``console_url`` given     -> register against a real console over HTTP.

    ``fail_after`` (one of ``register|cert|license|bundle|heartbeat``) injects a
    deliberate failure *after* that step for the rollback self-test; leave ``None``
    in production.

    On any failure: rolls back every applied side effect and raises
    :class:`OnboardError`.
    """
    ledger = RollbackLedger()
    client: Optional[cc.ConsoleClient] = None
    use_console = not local_ids

    def _maybe_fail(step: str) -> None:
        if fail_after == step:
            raise OnboardError(f"injected failure after step {step!r} (rollback test)")

    try:
        # --- console handle (if used) --------------------------------------
        if use_console:
            client = cc.make_console_client(
                console_url=console_url, app=console_app, token=console_token
            )

        # --- STEP 1: REGISTER (or mint locally) ----------------------------
        if use_console:
            assert client is not None
            reg = client.register(region=region, plan=plan, tenant_id=tenant)
            tenant_id = reg["tenant_id"]
            deployment_id = reg["deployment_id"]
            # A real console's POST /api/v1/register CREATES a row immediately
            # (the fake console only mints ids until the first heartbeat). Either
            # way, push the console-deregister undo NOW — right after register —
            # so a failure at ANY later step (cert/license/bundle/heartbeat/
            # confirm) undoes the console row too (FR-E). Without this, a failure
            # between register and heartbeat would orphan the console row.
            ledger.push(
                f"console-deployment[{deployment_id}]",
                lambda _c=client, _d=deployment_id: _best_effort_console_remove(_c, _d),
            )
            _log(f"[1/6] registered with console: tenant={tenant_id} deployment={deployment_id}")
        else:
            tenant_id = tenant
            # Local mint mirrors the console's id shape: <tenant>-<region>-<rand>.
            import secrets

            slug = "".join(ch for ch in region.lower() if ch.isalnum())[:6] or "rgn"
            deployment_id = f"{tenant_id}-{slug}-{secrets.token_hex(2)}"
            _log(f"[1/6] minted ids locally: tenant={tenant_id} deployment={deployment_id}")
        _maybe_fail("register")

        bundle_dir = os.path.join(bundles_root, deployment_id)
        if os.path.exists(bundle_dir):
            raise OnboardError(
                f"bundle dir already exists: {bundle_dir} (already onboarded?)"
            )

        # --- STEP 2: ISSUE CERT (this is the proxy binding) ----------------
        cert_out = os.path.join(bundle_dir, "cert")
        cert = issue_cert.issue(
            tenant_id,
            pki_dir=pki_dir,
            out_dir=cert_out,
            valid_days=cert_valid_days,
            registry_path=registry_path,
        )
        fingerprint = cert["fingerprint_sha256"]

        # Issuing the cert (a) wrote files into `bundle_dir/cert/` — so the bundle
        # dir now exists — and (b) added an active registry row. Both are undone by
        # one ledger entry: revoke + delete the registry row AND rmtree the
        # (partial) bundle dir. Pushing it here (not at step 4) guarantees a
        # failure *anywhere* after the cert leaves no orphan bundle dir on disk.
        def _undo_cert_and_bundle(
            _fp: str = fingerprint, _bd: str = bundle_dir, _tid: str = tenant_id
        ) -> None:
            _teardown_cert(_fp, _tid, registry_path=registry_path)
            shutil.rmtree(_bd, ignore_errors=True)

        ledger.push(f"cert+registry-row+bundle[{fingerprint[:12]}]", _undo_cert_and_bundle)
        _log(f"[2/6] issued mTLS cert (SAN tenant={tenant_id}); registry row active "
             f"fp={fingerprint[:16]}…")
        _maybe_fail("cert")

        # --- STEP 3: MINT + SIGN LICENSE -----------------------------------
        lic = lm.mint_license(
            tenant_id=tenant_id,
            deployment_id=deployment_id,
            plan=plan,
            out_dir=bundle_dir,
            valid_days=license_valid_days,
        )
        license_expiry = lic["expires_at"]
        _log(f"[3/6] minted + signed license (plan={lic['plan']}, "
             f"expires={license_expiry}, key={lic['key_id']})")
        _maybe_fail("license")

        # --- STEP 4: ASSEMBLE THE AGENT BUNDLE -----------------------------
        # The bundle dir already holds the cert/ and the license files; add the
        # signed agent config + the public trust root the agent verifies with.
        _assemble_bundle_extras(
            bundle_dir=bundle_dir,
            tenant_id=tenant_id,
            deployment_id=deployment_id,
            region=region,
            plan=plan,
            version=version,
            telemetry_tier=telemetry_tier,
            license_expiry=license_expiry,
            console_url=console_url,
            console_token=console_token,
            cert=cert,
            lic=lic,
            trust_root_path=trust_root_path,
        )
        # No separate undo needed: the bundle dir (cert + license + config) is
        # already covered by the cert-step ledger entry's rmtree, since every
        # bundle file lives under bundle_dir.
        _log(f"[4/6] assembled agent bundle: {bundle_dir}")
        _maybe_fail("bundle")

        # --- STEP 5: SEED HEARTBEAT (so the deployment appears now) --------
        record = DeploymentRecord.heartbeat(
            tenant_id=tenant_id,
            deployment_id=deployment_id,
            version=version,
            region=region,
            license_expiry=license_expiry,
            telemetry_tier=telemetry_tier,
            last_heartbeat_ts=utcnow(),
        )
        if use_console:
            assert client is not None
            client.heartbeat(record.to_registry_dict())
            # The console-deregister undo was already pushed right after register
            # (step 1), so it covers this heartbeat-written row too — no second
            # ledger entry needed here.
            _log(f"[5/6] seeded heartbeat (health={record.health.value})")
        else:
            _log(f"[5/6] seeded heartbeat locally (no console; health={record.health.value})")
        _maybe_fail("heartbeat")

        # --- STEP 6: CONFIRM IT'S LISTED -----------------------------------
        if use_console:
            assert client is not None
            if not client.has_deployment(deployment_id):
                raise OnboardError(
                    f"confirmation failed: deployment {deployment_id} not listed by console"
                )
            _log(f"[6/6] confirmed: console lists deployment {deployment_id}")
        else:
            _log("[6/6] confirm skipped (--local-ids: no console to confirm against)")

        # --- COMMIT --------------------------------------------------------
        ledger.commit()
        # Write the bundle manifest last so it lists every other file (and then
        # re-list so the returned `files` includes BUNDLE.json itself).
        _write_bundle_summary(bundle_dir, tenant_id, deployment_id, region, plan,
                              license_expiry, telemetry_tier, fingerprint)
        files = _list_bundle_files(bundle_dir)
        _log(f"ONBOARDED {tenant_id} -> {deployment_id}. Bundle ready at {bundle_dir}")
        return OnboardResult(
            tenant_id=tenant_id,
            deployment_id=deployment_id,
            region=region,
            plan=plan,
            bundle_dir=bundle_dir,
            fingerprint=fingerprint,
            license_expiry=license_expiry,
            telemetry_tier=telemetry_tier,
            used_console=use_console,
            files=files,
        )

    except Exception as exc:
        _log(f"ONBOARD FAILED: {exc}")
        _log("Rolling back …")
        rolled = ledger.roll_back()
        _log(f"Rollback complete ({len(rolled)} undo(s)). No half-onboarded state remains.")
        if isinstance(exc, OnboardError):
            raise
        raise OnboardError(f"onboarding failed and was rolled back: {exc}") from exc
    finally:
        if client is not None:
            client.close()


def _best_effort_console_remove(client: "cc.ConsoleClient", deployment_id: str) -> None:
    """Remove a deployment from the console during rollback (FR-E).

    The console now exposes an **idempotent** ``DELETE /api/v1/deployments/{id}``
    verb, so the deployment row created by register/heartbeat is actually removed
    — no orphaned console row survives a rolled-back onboarding. The same verb
    works whether the client talks to a real console over HTTP or to an in-process
    console via ASGI (both the real ``console/app.py`` and the embedded
    ``fake_console`` expose it), so we route through ``deregister`` uniformly
    rather than reaching into a particular store's internals.

    Best-effort: any transport/console error (including ``removed=false`` when the
    row was already gone) is logged and swallowed so it never masks the original
    onboarding failure that triggered the rollback.
    """
    try:
        removed = client.deregister(deployment_id)
        _log(
            f"    deregistered {deployment_id} from console"
            if removed
            else f"    {deployment_id} already absent from console (idempotent)"
        )
    except Exception as exc:  # best-effort: never let rollback cleanup raise
        _log(f"    (console deregister of {deployment_id} failed, leaving best-effort: {exc})")


# --------------------------------------------------------------------------- #
# Bundle assembly                                                            #
# --------------------------------------------------------------------------- #
def _assemble_bundle_extras(
    *,
    bundle_dir: str,
    tenant_id: str,
    deployment_id: str,
    region: str,
    plan: str,
    version: str,
    telemetry_tier: str,
    license_expiry: str,
    console_url: Optional[str],
    console_token: Optional[str],
    cert: dict,
    lic: dict,
    trust_root_path: str,
) -> None:
    """Write the agent config (signed) + public trust root into the bundle.

    The bundle the customer deploys ends up as::

        bundles/<deployment_id>/
          cert/<tenant>.crt  <tenant>.key  <tenant>.bundle.crt   (mTLS material)
          <tenant>.license.json[.sig|.manifest.json]             (signed license)
          agent-config.json[.sig|.manifest.json]                 (signed config -> console)
          trust_root.json                                        (public keys to VERIFY both)
          BUNDLE.json                                            (manifest of the above)
    """
    # 1) agent config -> points the outbound-only agent at the console (I2/I3).
    config = {
        "tenant_id": tenant_id,
        "deployment_id": deployment_id,
        "region": region,
        "plan": plan,
        "version": version,
        "telemetry_tier": telemetry_tier,
        "console_url": console_url or "https://console.fyralis.example:8080",
        "license_file": os.path.basename(lic["license_path"]),
        "mtls": {
            "cert": f"cert/{os.path.basename(cert['cert_path'])}",
            "key": f"cert/{os.path.basename(cert['key_path'])}",
            "chain": f"cert/{os.path.basename(cert['bundle_path'])}",
        },
        # Bearer token for the console WRITE path (I4). The agent presents this on
        # every heartbeat; without it the console answers 401. Stamped into the
        # bundle so the customer's agent ships with its write credential.
        "console_token": console_token or "",
        "outbound_only": True,         # I2: agent opens no inbound listener
        "buffer_on_unreachable": True,  # I3: buffer telemetry/config when CP is down
        "verify_before_apply": True,    # I6
    }
    cfg_path = os.path.join(bundle_dir, "agent-config.json")
    with open(cfg_path, "w", encoding="utf-8") as fh:
        json.dump(config, fh, indent=2, sort_keys=True)
        fh.write("\n")
    # Sign the config so the agent verifies it before applying (I6/C2).
    try:
        sign_bundle.sign_file(cfg_path, kind="config", version=version)
    except Exception as exc:
        raise OnboardError(
            f"could not sign agent config: {exc} (is signing/ bootstrapped?)"
        ) from exc

    # 2) public trust root -> the agent ships this to VERIFY license + config.
    if os.path.exists(trust_root_path):
        shutil.copy2(trust_root_path, os.path.join(bundle_dir, "trust_root.json"))
    else:
        raise OnboardError(
            f"trust root not found at {trust_root_path}; agent could not verify "
            "anything (run signing/keygen.py)"
        )


def _list_bundle_files(bundle_dir: str) -> list[str]:
    out: list[str] = []
    for root, _dirs, files in os.walk(bundle_dir):
        for f in sorted(files):
            out.append(os.path.relpath(os.path.join(root, f), bundle_dir))
    return sorted(out)


def _write_bundle_summary(
    bundle_dir: str,
    tenant_id: str,
    deployment_id: str,
    region: str,
    plan: str,
    license_expiry: str,
    telemetry_tier: str,
    fingerprint: str,
) -> None:
    summary = {
        "tenant_id": tenant_id,
        "deployment_id": deployment_id,
        "region": region,
        "plan": plan,
        "license_expiry": license_expiry,
        "telemetry_tier": telemetry_tier,
        "cert_fingerprint_sha256": fingerprint,
        "created_at": to_rfc3339(utcnow()),
        "files": _list_bundle_files(bundle_dir),
        "deploy_hint": (
            "Copy this directory into the customer VPC and run the agent against "
            "agent-config.json; the agent verifies trust_root.json signatures "
            "before applying the license/config (I6) and dials the console "
            "outbound-only (I2)."
        ),
    }
    with open(os.path.join(bundle_dir, "BUNDLE.json"), "w", encoding="utf-8") as fh:
        json.dump(summary, fh, indent=2, sort_keys=True)
        fh.write("\n")


# --------------------------------------------------------------------------- #
# CLI                                                                        #
# --------------------------------------------------------------------------- #
def main(argv: Optional[list[str]] = None) -> int:
    ap = argparse.ArgumentParser(
        prog="onboard",
        description="Atomically onboard a BYOC tenant (cert + license + bundle + console).",
    )
    ap.add_argument("--tenant", required=True, help="tenant id (goes in the cert SAN)")
    ap.add_argument("--region", required=True, help="deployment region, e.g. us-east")
    ap.add_argument("--plan", default="standard",
                    help=f"plan (one of {sorted(lm.PLAN_FEATURES)}; default standard)")
    g = ap.add_mutually_exclusive_group()
    g.add_argument("--console-url", default=os.environ.get("CONSOLE_URL"),
                   help="console base URL (e.g. http://console:8080)")
    g.add_argument("--local-ids", action="store_true",
                   help="mint tenant/deployment ids locally without a console")
    g.add_argument("--embedded-console", action="store_true",
                   help="use an in-process fake console (dev/demo; no server)")
    ap.add_argument("--bundles-root", default=DEFAULT_BUNDLES_ROOT)
    ap.add_argument("--pki-dir", default=DEFAULT_PKI_DIR)
    ap.add_argument("--registry", default=DEFAULT_REGISTRY_PATH)
    ap.add_argument("--trust-root", default=DEFAULT_TRUST_ROOT)
    ap.add_argument("--version", default=DEFAULT_VERSION)
    ap.add_argument("--telemetry-tier", default="T1", choices=["T1", "T2", "T3"])
    ap.add_argument("--cert-valid-days", type=int, default=90)
    ap.add_argument("--license-valid-days", type=int, default=lm.DEFAULT_LICENSE_DAYS)
    ap.add_argument(
        "--console-token",
        default=os.environ.get("CONSOLE_INGEST_TOKEN"),
        help="bearer token for the console write path (I4); stamped into the agent "
        "bundle and used to authenticate the onboarding register/heartbeat calls. "
        "Defaults to $CONSOLE_INGEST_TOKEN.",
    )
    ap.add_argument("--fail-after", default=None,
                    choices=["register", "cert", "license", "bundle", "heartbeat"],
                    help="inject a failure after this step (rollback test only)")
    ap.add_argument("--json", action="store_true", help="print the result as JSON")
    args = ap.parse_args(argv)

    console_app = None
    if args.embedded_console:
        import fake_console
        console_app = fake_console.build_app()

    try:
        result = onboard(
            tenant=args.tenant,
            region=args.region,
            plan=args.plan,
            console_url=args.console_url,
            console_app=console_app,
            local_ids=args.local_ids,
            bundles_root=args.bundles_root,
            pki_dir=args.pki_dir,
            registry_path=args.registry,
            trust_root_path=args.trust_root,
            version=args.version,
            telemetry_tier=args.telemetry_tier,
            cert_valid_days=args.cert_valid_days,
            license_valid_days=args.license_valid_days,
            console_token=args.console_token,
            fail_after=args.fail_after,
        )
    except OnboardError as exc:
        print(f"onboard failed (rolled back): {exc}", file=sys.stderr)
        return 1
    except Exception as exc:  # unexpected — show the trace, return non-zero
        print(f"onboard crashed: {exc}", file=sys.stderr)
        traceback.print_exc()
        return 2

    if args.json:
        print(json.dumps(result.to_dict(), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
