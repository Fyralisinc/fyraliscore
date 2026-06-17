"""services/platform/extensions/redaction.py — per-channel egress redaction (E3.1b).

The host never streams a raw `observations` row to an extension. Every observation
crossing the egress boundary is projected to an :class:`ObservationView` (which
already drops ``content["_raw"]``) and then passed through a **channel-owned
redaction** function registered here. Default-deny: an unknown channel gets the
*strict* redaction (all actor-identity content keys stripped); a channel owner may
register a looser projection that keeps fields its signal genuinely needs (e.g.
github keeps the author login but still strips emails). A tenant admin may tighten
further (M5 consent), never loosen — enforced upstream.

Pure + dependency-light so it can run inside the Kafka projector and be unit-tested
without a broker or DB.
"""
from __future__ import annotations

import dataclasses
from typing import Callable

from lib.extensions.host_api.v1 import ObservationView

RedactionFn = Callable[[ObservationView], ObservationView]

# Email-shaped identity fields are stripped on EVERY channel (the baseline).
_BASELINE_IDENTITY = frozenset({
    "author_email", "sender_email", "user_email", "email", "committer_email",
})
# The strict set additionally removes actor handles/ids — applied to any channel
# whose owner has not registered a looser, reviewed projection.
_STRICT_IDENTITY = _BASELINE_IDENTITY | {
    "author", "sender", "user", "login", "actor", "user_id", "author_id",
    "committer", "assignee",
}

_REGISTRY: dict[str, RedactionFn] = {}


def _strip(view: ObservationView, keys: frozenset[str] | set[str]) -> ObservationView:
    content = {k: v for k, v in (view.content or {}).items() if k not in keys}
    return dataclasses.replace(view, content=content)


def default_redaction(view: ObservationView) -> ObservationView:
    """Strict, conservative redaction for any channel without a registered rule."""
    return _strip(view, _STRICT_IDENTITY)


def register_redaction(channel: str, fn: RedactionFn) -> None:
    """Register the channel owner's reviewed redaction projection."""
    _REGISTRY[channel] = fn


def redact(view: ObservationView) -> ObservationView:
    """Apply the channel's redaction (or the strict default). Always also ensures
    ``_raw`` is gone (defense in depth — the view projection drops it already)."""
    fn = _REGISTRY.get(view.source_channel, default_redaction)
    out = fn(view)
    if "_raw" in (out.content or {}):
        out = _strip(out, frozenset({"_raw"}))
    return out


# --- reference projection: github:webhook (E3.1b) ---------------------------------
def _github_webhook_redaction(view: ObservationView) -> ObservationView:
    """GitHub's signal value depends on the actor login (who did what), so keep it;
    strip only email-shaped identity fields (the channel owner's definition of
    sensitive)."""
    return _strip(view, _BASELINE_IDENTITY)


register_redaction("github:webhook", _github_webhook_redaction)


__all__ = ["RedactionFn", "redact", "register_redaction", "default_redaction"]
