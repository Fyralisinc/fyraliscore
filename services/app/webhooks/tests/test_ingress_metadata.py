"""Contract-owned webhook ingress-metadata dispatch."""

from __future__ import annotations

import pytest

from services.app.webhooks import router
from services.ingest.source_contract import (
    BindingResolutionError,
    WEBHOOK_INGRESS_CATALOG,
    build_webhook_ingress_metadata,
    resolve_webhook_ingress_metadata_builder,
)
from services.ingest.source_contract import runtime as contract_runtime


def test_every_webhook_metadata_binding_resolves_from_contract() -> None:
    for route_id, ingress in WEBHOOK_INGRESS_CATALOG.items():
        assert ingress.ingress_metadata_binding
        assert callable(resolve_webhook_ingress_metadata_builder(route_id))


def test_contract_dispatch_preserves_provider_metadata_semantics() -> None:
    assert build_webhook_ingress_metadata(
        "github",
        {
            "X-GitHub-Event": "pull_request",
            "X-GitHub-Delivery": "delivery-1",
        },
        {},
    ) == {
        "event_type": "pull_request",
        "delivery_id": "delivery-1",
    }
    assert build_webhook_ingress_metadata(
        "slack",
        {},
        {"event": {"type": "message"}},
    ) == {"event_type": "message"}
    assert build_webhook_ingress_metadata(
        "discord",
        {},
        {"type": 2},
    ) == {"event_type": "interaction:2"}
    assert build_webhook_ingress_metadata(
        "jira",
        {},
        {"webhookEvent": "jira:issue_updated"},
    ) == {"event_type": "unknown"}


def test_metadata_builder_output_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        contract_runtime,
        "resolve_webhook_ingress_metadata_builder",
        lambda _route_id: lambda _headers, _payload: {},
    )

    with pytest.raises(
        BindingResolutionError,
        match="non-empty string event_type",
    ):
        contract_runtime.build_webhook_ingress_metadata("slack", {}, {})


def test_router_has_no_provider_event_metadata_switch() -> None:
    assert not hasattr(router, "_event_type_for")
