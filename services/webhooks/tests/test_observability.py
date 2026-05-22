"""Observability tests. Spec: US6 / FR-011 / FR-016 / SC-005, SC-007.

Every verification failure increments a per-(provider, reason) counter
and emits a structured log. The log MUST NOT contain the raw body or
the candidate signature.
"""
from __future__ import annotations

import logging

import pytest

from services.webhooks import metrics
from services.webhooks.signatures.github import verifier as github_verifier
from services.webhooks.tests.conftest import github_sign
from services.webhooks.verifier import Secret, WebhookVerificationError


@pytest.mark.asyncio
async def test_metric_per_provider_reason_increments_via_router_path() -> None:
    """Direct verifier failure does not touch metrics — those are
    recorded by the router. Simulate the router's contract: when a
    WebhookVerificationError is caught, record_failure is called."""
    body = b"{}"
    sig = github_sign("right", body)
    try:
        await github_verifier.verify(
            body=b"different-body",
            headers={"X-Hub-Signature-256": sig},
            secrets=[Secret("github", "right")],
        )
    except WebhookVerificationError as e:
        metrics.record_failure(e.provider, e.reason)

    assert metrics.get_count("github", "signature_mismatch") == 1


@pytest.mark.asyncio
async def test_error_to_dict_shape() -> None:
    """The structured error MUST have {code, message, context} with
    provider + reason in context. No body or signature anywhere."""
    err = WebhookVerificationError(
        "signature_mismatch",
        "stripe signature does not match",
        provider="stripe",
        max_age_s=300,  # arbitrary extra context
    )
    d = err.to_dict()
    assert d["code"] == "webhook_verification_failed"
    assert d["context"]["provider"] == "stripe"
    assert d["context"]["reason"] == "signature_mismatch"
    # Defensive: assert no body/signature keys leak into the
    # serialized error. The error class doesn't store them, but if a
    # future change tried to add them this test would fail.
    serialized = repr(d)
    assert "body" not in serialized
    assert "signature_value" not in serialized
    assert "candidate" not in serialized


def test_failure_reasons_are_closed_set() -> None:
    """The reason field accepts only the six closed-set values from
    FR-005. The Literal type pins them; this test documents the set."""
    from services.webhooks.verifier import VerificationReason
    import typing

    assert set(typing.get_args(VerificationReason)) == {
        "missing_signature_header",
        "malformed_signature_header",
        "expired_timestamp",
        "signature_mismatch",
        "secret_not_configured",
        "tenant_not_resolved",
    }


def test_metric_snapshot_and_reset() -> None:
    metrics.record_failure("slack", "signature_mismatch")
    metrics.record_failure("slack", "signature_mismatch")
    metrics.record_failure("github", "expired_timestamp")
    snap = metrics.snapshot()
    assert snap[("slack", "signature_mismatch")] == 2
    assert snap[("github", "expired_timestamp")] == 1
    metrics.reset()
    assert metrics.snapshot() == {}
