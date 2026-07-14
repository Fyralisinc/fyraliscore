"""config — the A1 REMOTE CONFIG PUSH reconcile handler (verify-before-apply, I6).

This is the flagship proof that the whole pull/verify/apply pattern works:

  operator WRITES desired_config (signed) -> agent PULLS desired state ->
  THIS handler VERIFIES the signature against the trust root -> on a clean verify
  it APPLIES the config to the agent's config dir -> it reports
  ``applied_config_version`` on the next heartbeat -> the console renders the
  drift closing.

Invariant anchoring
-------------------
* **I6** — the config is NEVER applied unless ``desired_config_sig`` verifies
  against ``ctx.trust_root_path``: an unsigned, relabeled, wrong-key, retired-key,
  or tampered config is REJECTED and the previously-applied config is left
  untouched. We reuse the SAME ``signing/verify_bundle.verify_file`` path the
  ``ConfigPuller`` uses (stage the {config, sig, manifest} trio to a temp dir and
  verify there) so there is ONE verification code path — no bespoke crypto here.
* **I3** — advisory + resilient: a missing/older/invalid config is a no-op
  (return ``{}``); we never raise out of ``apply`` (the registry would skip us
  anyway, but we keep the agent's last-applied config rather than clobber it).

What "applied" means here: we write the verified config (plus its sig + manifest,
so a restart re-reads a *verified* config exactly like ``ConfigPuller``) into
``ctx.config_dir`` under :data:`REMOTE_CONFIG_NAME`, then return
``{"applied_config_version": desired.desired_config_version}`` so the heartbeat's
applied facet advances and :func:`lib.desired_state.compute_drift` reports the
config drift as closed.
"""

from __future__ import annotations

import json
import os
import shutil
import tempfile
from pathlib import Path
from typing import Any, Dict

from lib.desired_state import DesiredState

from .registry import ReconcileContext, register

# The on-disk name of the remotely-pushed, verified config (distinct from the
# ConfigPuller's pulled-bundle ``agent-config.json`` so a remote push and a bundle
# pull do not stomp each other).
REMOTE_CONFIG_NAME = "remote-config.json"


def _load_verifier():
    """Import the agent's ``verify_bundle`` (sys.path is wired by _bootstrap).

    Imported lazily inside ``apply`` so merely importing this handler module (which
    ``autodiscover`` does at agent start) never requires the signing stack to be
    importable — a verifier import failure degrades to a logged skip, not a crash.
    """
    import _bootstrap  # noqa: F401  (side-effect: sys.path for lib + signing)
    import verify_bundle as vb  # noqa: E402

    return vb


def _atomic_replace(src: Path, dst: Path) -> None:
    """Move ``src`` over ``dst`` atomically within the same filesystem."""
    tmp = dst.with_suffix(dst.suffix + ".tmp")
    shutil.copyfile(src, tmp)
    os.replace(tmp, dst)


def apply(desired: DesiredState, ctx: ReconcileContext) -> Dict[str, Any]:
    """Verify the operator's desired config and apply it (I6); return applied delta.

    No-op (returns ``{}``) when:
      * there is no ``desired_config`` to apply, or
      * the desired version is not ahead of what's already applied, or
      * the signature is missing / fails verification (REJECT — keep last config).
    """
    cfg = desired.desired_config
    if not cfg:
        # Nothing pushed yet — advisory no-op.
        return {}

    desired_v = int(desired.desired_config_version or 0)

    # Only apply forward: if the agent already has this version (or newer), skip the
    # verify+write work. ``ctx.extra`` may carry the agent's last-applied facets; if
    # not present we still verify+apply (idempotent write) and report the version.
    already_v = 0
    applied_facets = ctx.extra.get("applied_facets") if isinstance(ctx.extra, dict) else None
    if isinstance(applied_facets, dict):
        try:
            already_v = int(applied_facets.get("applied_config_version", 0) or 0)
        except (TypeError, ValueError):
            already_v = 0
    if desired_v and already_v >= desired_v:
        ctx.logger.debug(
            "config reconcile: already at v%s >= desired v%s (no-op)", already_v, desired_v
        )
        return {}

    sig = desired.desired_config_sig
    if not isinstance(sig, dict) or not sig.get("sig") or not isinstance(sig.get("manifest"), dict):
        # I6: an unsigned (or malformed-sig) config is NEVER applied.
        ctx.logger.warning(
            "config reconcile: desired_config v%s has NO valid signature envelope "
            "(missing sig/manifest) — REJECTING, keeping last-applied config (I6)",
            desired_v,
        )
        return {}

    try:
        vb = _load_verifier()
    except Exception as exc:  # signing stack not importable — degrade, never crash
        ctx.logger.error(
            "config reconcile: verifier unavailable (%s) — cannot verify, REJECTING (I6)", exc
        )
        return {}

    # Stage the {config, sig, manifest} trio under names verify_bundle expects and
    # verify there — the SAME path ConfigPuller uses. The config bytes MUST be the
    # canonical JSON of the dict the operator signed (deps.signer signed
    # canonical_json_bytes(desired_config)); verify_file re-canonicalizes config JSON
    # so writing the dict back out as plain JSON verifies byte-for-byte.
    staged = Path(tempfile.mkdtemp(prefix="agent-remote-cfg-"))
    try:
        staged_cfg = staged / REMOTE_CONFIG_NAME
        staged_cfg.write_text(json.dumps(cfg), encoding="utf-8")
        sig_text = str(sig["sig"])
        (staged / (REMOTE_CONFIG_NAME + ".sig")).write_text(
            sig_text if sig_text.endswith("\n") else sig_text + "\n", encoding="utf-8"
        )
        (staged / (REMOTE_CONFIG_NAME + ".manifest.json")).write_text(
            json.dumps(sig["manifest"]), encoding="utf-8"
        )

        res = vb.verify_file(str(staged_cfg), trust_root_path=ctx.trust_root_path)
        if not res.ok:
            # I6: reject — leave the existing applied config untouched.
            ctx.logger.warning(
                "config reconcile: desired_config v%s REJECTED (unverified): %s — "
                "keeping last-applied config (I6)",
                desired_v,
                res.reason,
            )
            return {}
        if (res.artifact or "").lower() != "config":
            ctx.logger.warning(
                "config reconcile: signed artifact is %r, not 'config' (relabel?) — "
                "REJECTING (I6)",
                res.artifact,
            )
            return {}

        # Clean verify -> apply atomically into the agent's config dir. Persist the
        # whole verified trio so a restart re-reads a verified config (I6 on restart).
        config_dir = Path(ctx.config_dir)
        config_dir.mkdir(parents=True, exist_ok=True)
        for suffix in ("", ".sig", ".manifest.json"):
            _atomic_replace(
                staged / (REMOTE_CONFIG_NAME + suffix),
                config_dir / (REMOTE_CONFIG_NAME + suffix),
            )

        ctx.logger.info(
            "config reconcile: desired_config v%s VERIFIED (%s) and APPLIED to %s",
            desired_v,
            res.key_id,
            config_dir / REMOTE_CONFIG_NAME,
        )
        return {"applied_config_version": desired_v}
    finally:
        shutil.rmtree(staged, ignore_errors=True)


# Self-register at import time so reconcile.autodiscover() wires this handler.
# (Replaces the example for the "config" concern name — distinct from "example".)
register("config", apply)
