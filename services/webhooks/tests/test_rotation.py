"""Secret rotation tests. Spec: US5 / FR-010 / SC-004.

A secret rotation overlap means BOTH the old and the new secret are
accepted simultaneously; once the old secret is removed from config,
requests signed with it are rejected. The verifier MUST report which
secret matched (via `VerifiedContext.secret_label`) so dashboards can
observe the cutover.
"""
from __future__ import annotations

import os

import pytest

from services.webhooks.secrets import load_secrets
from services.webhooks.signatures.github import verifier as github_verifier
from services.webhooks.tests.conftest import github_sign
from services.webhooks.verifier import Secret, WebhookVerificationError


@pytest.mark.asyncio
async def test_both_secrets_accepted_during_overlap() -> None:
    body = b'{"action":"opened"}'
    old = "old-secret"
    new = "new-secret"

    sig_old = github_sign(old, body)
    sig_new = github_sign(new, body)

    secrets = [
        Secret("github", old, label="old"),
        Secret("github", new, label="new"),
    ]

    ctx_old = await github_verifier.verify(
        body=body,
        headers={"X-Hub-Signature-256": sig_old},
        secrets=secrets,
    )
    ctx_new = await github_verifier.verify(
        body=body,
        headers={"X-Hub-Signature-256": sig_new},
        secrets=secrets,
    )
    assert ctx_old.secret_label == "old"
    assert ctx_new.secret_label == "new"


@pytest.mark.asyncio
async def test_old_secret_rejected_after_removal() -> None:
    body = b'{"action":"opened"}'
    old = "old-secret"
    new = "new-secret"
    sig_old = github_sign(old, body)

    # New-only configuration — the old secret is no longer active.
    secrets = [Secret("github", new, label="new")]

    with pytest.raises(WebhookVerificationError) as exc:
        await github_verifier.verify(
            body=body,
            headers={"X-Hub-Signature-256": sig_old},
            secrets=secrets,
        )
    assert exc.value.reason == "signature_mismatch"


def test_env_layout_parses_comma_separated(monkeypatch: pytest.MonkeyPatch) -> None:
    """The env-based secret store accepts comma-separated secrets,
    with optional `label=` prefix per entry, so a rotation can be
    expressed without process restart."""
    monkeypatch.setenv(
        "WEBHOOK_SECRET_GITHUB",
        "old=old-secret,new=new-secret",
    )
    secrets = load_secrets("github")
    assert len(secrets) == 2
    labels = {s.label for s in secrets}
    values = {s.value for s in secrets}
    assert labels == {"old", "new"}
    assert values == {"old-secret", "new-secret"}


def test_env_layout_unlabelled(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("WEBHOOK_SECRET_GITHUB", "plain-secret")
    secrets = load_secrets("github")
    assert len(secrets) == 1
    assert secrets[0].label is None
    assert secrets[0].value == "plain-secret"


def test_env_per_tenant_overrides_global(monkeypatch: pytest.MonkeyPatch) -> None:
    from uuid import UUID

    monkeypatch.setenv("WEBHOOK_SECRET_GITHUB", "global-secret")
    tenant = UUID("00000000-0000-0000-0000-000000000001")
    monkeypatch.setenv(
        f"WEBHOOK_SECRET_GITHUB__{tenant.hex.upper()}",
        "tenant-secret",
    )
    secrets = load_secrets("github", tenant_id=tenant)
    assert [s.value for s in secrets] == ["tenant-secret"]
