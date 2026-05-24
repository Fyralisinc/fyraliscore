"""services/webhooks/signatures — per-provider Verifier implementations.

Each module exports a verifier class instance bound under its provider
name. `VERIFIERS` is the registry the router dispatches on.

Adding a sixth provider (Twilio, Shopify, …):

1. Add `services/webhooks/signatures/<provider>.py` exposing a
   `verifier: Verifier` module attribute.
2. Add `<provider>: <module>.verifier` to the VERIFIERS map below.
3. Add a tenant resolver in
   `services/webhooks/tenant_resolution.py`.
4. Add a `CHANNEL_TRUST_MAP` entry in
   `services/ingestion/handlers/__init__.py` plus a handler module.

The Verifier Protocol is in `services/webhooks/verifier.py`.
"""
from __future__ import annotations

from services.webhooks.signatures import (
    discord,
    github,
    linear,
    slack,
    stripe,
)
from services.webhooks.verifier import Verifier


VERIFIERS: dict[str, Verifier] = {
    "slack": slack.verifier,
    "github": github.verifier,
    "linear": linear.verifier,
    "stripe": stripe.verifier,
    "discord": discord.verifier,
}


__all__ = ["VERIFIERS"]
