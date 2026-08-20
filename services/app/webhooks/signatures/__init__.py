"""Verifier registry for non-source product webhooks."""

from __future__ import annotations

from services.app.webhooks.signatures import linear, stripe
from services.app.webhooks.verifier import Verifier


VERIFIERS: dict[str, Verifier] = {
    "linear": linear.verifier,
    "stripe": stripe.verifier,
}


__all__ = ["VERIFIERS"]
