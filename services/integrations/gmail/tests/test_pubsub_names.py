"""Tests for the deterministic Pub/Sub resource naming."""
from __future__ import annotations

import os
from uuid import UUID

import pytest

from services.integrations.gmail.pubsub import (
    PubsubProvisioningError,
    resource_names_for_tenant,
)


class TestResourceNames:
    def test_deterministic(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("GMAIL_PUBSUB_PROJECT_ID", "fyralis-test")
        tenant = UUID("11111111-2222-3333-4444-555555555555")
        a = resource_names_for_tenant(tenant)
        b = resource_names_for_tenant(tenant)
        assert a == b

    def test_topic_and_subscription_distinct(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("GMAIL_PUBSUB_PROJECT_ID", "fyralis-test")
        tenant = UUID("11111111-2222-3333-4444-555555555555")
        names = resource_names_for_tenant(tenant)
        assert names.topic_name != names.subscription_name

    def test_includes_project_and_tenant(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("GMAIL_PUBSUB_PROJECT_ID", "fyralis-prod")
        tenant = UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
        names = resource_names_for_tenant(tenant)
        assert names.topic_name.startswith("projects/fyralis-prod/topics/gmail-")
        assert names.subscription_name.startswith(
            "projects/fyralis-prod/subscriptions/gmail-"
        )
        # tenant_id (without dashes) appears in both names.
        suffix = "aaaaaaaabbbbccccddddeeeeeeeeeeee"
        assert suffix in names.topic_name
        assert suffix in names.subscription_name

    def test_missing_project_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("GMAIL_PUBSUB_PROJECT_ID", raising=False)
        with pytest.raises(PubsubProvisioningError):
            resource_names_for_tenant(UUID("11111111-2222-3333-4444-555555555555"))
