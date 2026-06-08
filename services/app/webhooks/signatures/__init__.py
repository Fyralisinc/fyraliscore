"""services/app/webhooks/signatures — per-provider Verifier implementations.

Each module exports a verifier class instance bound under its provider
name. `VERIFIERS` is the registry the router dispatches on.

Adding a sixth provider (Twilio, Shopify, …):

1. Add `services/app/webhooks/signatures/<provider>.py` exposing a
   `verifier: Verifier` module attribute.
2. Add `<provider>: <module>.verifier` to the VERIFIERS map below.
3. Add a per-provider id extractor in
   `services/app/webhooks/tenant_resolver.py::PROVIDER_EXTRACTORS`.
4. Add a `CHANNEL_TRUST_MAP` entry in
   `services/ingest/ingestion/handlers/__init__.py` plus a handler module.

The Verifier Protocol is in `services/app/webhooks/verifier.py`.
"""
from __future__ import annotations

from services.app.webhooks.signatures import (
    brex,
    deel,
    discord,
    github,
    grafana,
    gusto,
    jira,
    linear,
    mercury,
    notion,
    quickbooks,
    ramp,
    slack,
    stripe,
)
from services.app.webhooks.verifier import Verifier


VERIFIERS: dict[str, Verifier] = {
    "slack": slack.verifier,
    "github": github.verifier,
    "linear": linear.verifier,
    "stripe": stripe.verifier,
    "discord": discord.verifier,
    "notion": notion.verifier,
    "jira": jira.verifier,
    "mercury": mercury.verifier,
    "quickbooks": quickbooks.verifier,
    "grafana": grafana.verifier,
    "brex": brex.verifier,
    "ramp": ramp.verifier,
    "gusto": gusto.verifier,
    "deel": deel.verifier,
}


__all__ = ["VERIFIERS"]
