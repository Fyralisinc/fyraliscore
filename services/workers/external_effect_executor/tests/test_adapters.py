from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import UUID, uuid4

import pytest

from lib.contracts.execution import ActionAdapterCapabilities
from lib.shared.errors import InvariantViolation
from services.ingest.integrations.slack.client import SlackApiError
from services.workers.external_effect_executor import adapters as subject


NOW = datetime(2026, 7, 16, 12, 0, tzinfo=timezone.utc)


def _capabilities(
    *,
    adapter_name: str = "slack-message-delivery",
    provider_name: str = "slack",
    operations: frozenset[str] = frozenset({"send_message"}),
    idempotency_supported: bool = True,
) -> ActionAdapterCapabilities:
    return ActionAdapterCapabilities(
        capability_id=uuid4(),
        tenant_id=uuid4(),
        capability_version="slack-adapter-v3",
        adapter_name=adapter_name,
        provider_name=provider_name,
        permitted_operations=operations,
        request_canonicalization_version="slack-message-request-v2",
        idempotency_supported=idempotency_supported,
        idempotency_scope=(
            "workspace/channel/client-message-id"
            if idempotency_supported
            else None
        ),
        idempotency_retention_until=(
            NOW + timedelta(days=2) if idempotency_supported else None
        ),
        reconciliation_supported=True,
        reconciliation_consistency_window_seconds=30,
        cancellation_supported=False,
        partial_effect_observable=True,
        compensation_supported=False,
        verified_at=NOW,
        expires_at=NOW + timedelta(days=1),
    )


def _request(**updates: Any) -> subject.ActionAdapterRequest:
    capabilities = updates.pop("capabilities", _capabilities())
    values = {
        "tenant_id": capabilities.tenant_id,
        "effect_attempt_id": uuid4(),
        "operation": "send_message",
        "parameters": {
            "channel_id": "C1",
            "text": "Review the Atlas escalation",
        },
        "provider_idempotency_key": "effect:atlas:1",
        "target_grounding_refs": (
            "referent:channel:customer-success:v2",
        ),
        "declared_preconditions": (
            "channel exists",
            "recipient may view concern",
        ),
        "capabilities": capabilities,
    }
    values.update(updates)
    return subject.ActionAdapterRequest(**values)


@dataclass
class _FakeAdapter:
    adapter_name: str
    provider_name: str

    async def preflight(
        self,
        request: subject.ActionAdapterRequest,
    ) -> subject.ActionPreflightResult:
        del request
        return subject.ActionPreflightResult(("fake:preflight",))

    async def dispatch(
        self,
        request: subject.ActionAdapterRequest,
    ) -> subject.ActionDispatchResult:
        del request
        return subject.ActionDispatchResult(
            fate=subject.ActionDispatchFate.UNKNOWN,
            reason="fake adapter does not dispatch",
        )


@pytest.mark.asyncio
async def test_static_registry_resolves_only_exact_capability_identity() -> None:
    expected = _FakeAdapter("slack-message-delivery", "slack")
    other = _FakeAdapter("email-delivery", "email")
    registry = subject.StaticActionAdapterRegistry((expected, other))

    assert await registry.resolve(_request()) is expected

    request = _request(
        capabilities=_capabilities(
            adapter_name="slack-message-delivery",
            provider_name="not-slack",
        )
    )
    with pytest.raises(InvariantViolation) as exc_info:
        await registry.resolve(request)
    assert exc_info.value.invariant == "EFFECT_ADAPTER_NOT_REGISTERED"


def test_static_registry_rejects_duplicate_identity() -> None:
    with pytest.raises(ValueError, match="duplicate action adapter registration"):
        subject.StaticActionAdapterRegistry(
            (
                _FakeAdapter("slack-message-delivery", "slack"),
                _FakeAdapter("slack-message-delivery", "slack"),
            )
        )


def test_action_adapter_request_validates_exact_required_fields() -> None:
    with pytest.raises(InvariantViolation) as exc_info:
        _request(operation="delete_message")
    assert exc_info.value.invariant == "EFFECT_ADAPTER_OPERATION_UNSUPPORTED"

    for updates, match in (
        ({"parameters": {}}, "parameters cannot be empty"),
        ({"provider_idempotency_key": " "}, "idempotency key cannot be empty"),
        ({"target_grounding_refs": ()}, "requires exact target grounding"),
        ({"declared_preconditions": ()}, "requires declared preconditions"),
    ):
        with pytest.raises(ValueError, match=match):
            _request(**updates)

    foreign_capability = _capabilities()
    with pytest.raises(InvariantViolation) as exc_info:
        _request(
            tenant_id=uuid4(),
            capabilities=foreign_capability,
        )
    assert exc_info.value.invariant == "EFFECT_ADAPTER_TENANT_MISMATCH"


def test_preflight_and_dispatch_results_reject_unsubstantiated_claims() -> None:
    with pytest.raises(ValueError, match="non-empty evidence refs"):
        subject.ActionPreflightResult(())
    with pytest.raises(ValueError, match="non-empty evidence refs"):
        subject.ActionPreflightResult((" ",))
    with pytest.raises(ValueError, match="reason cannot be empty"):
        subject.ActionDispatchResult(
            fate=subject.ActionDispatchFate.UNKNOWN,
            reason=" ",
        )
    with pytest.raises(ValueError, match="known provider fate"):
        subject.ActionDispatchResult(
            fate=subject.ActionDispatchFate.REJECTED,
            reason="provider rejected request",
        )
    with pytest.raises(ValueError, match="external state evidence"):
        subject.ActionDispatchResult(
            fate=subject.ActionDispatchFate.SUCCEEDED,
            reason="provider returned ok",
            provider_observation_refs=("provider:ok",),
        )

    unknown = subject.ActionDispatchResult(
        fate=subject.ActionDispatchFate.UNKNOWN,
        reason="transport ended without a knowable provider fate",
    )
    assert unknown.provider_observation_refs == ()


class _FakeSlackClient:
    def __init__(
        self,
        *,
        post_response: dict[str, Any] | None = None,
        post_error: Exception | None = None,
        channel_response: dict[str, Any] | None = None,
    ) -> None:
        self.post_response = post_response or {}
        self.post_error = post_error
        self.channel_response = channel_response or {"channel": {"id": "C1"}}
        self.closed = False
        self.post_arguments: dict[str, str] | None = None

    async def conversations_info(self, channel_id: str) -> dict[str, Any]:
        assert channel_id == "C1"
        return self.channel_response

    async def chat_post_message(
        self,
        *,
        channel: str,
        text: str,
        client_msg_id: str,
    ) -> dict[str, Any]:
        self.post_arguments = {
            "channel": channel,
            "text": text,
            "client_msg_id": client_msg_id,
        }
        if self.post_error is not None:
            raise self.post_error
        return self.post_response

    async def aclose(self) -> None:
        self.closed = True


def _slack_adapter_with_client(
    client: _FakeSlackClient,
) -> subject._SlackMessageAdapter:
    adapter = object.__new__(subject._SlackMessageAdapter)

    async def resolve_client(
        request: subject.ActionAdapterRequest,
    ) -> tuple[_FakeSlackClient, str]:
        del request
        return client, "T1"

    adapter._client = resolve_client  # type: ignore[method-assign]
    return adapter


@pytest.mark.asyncio
async def test_slack_preflight_returns_fresh_exact_target_evidence() -> None:
    client = _FakeSlackClient()
    request = _request(declared_preconditions=("channel exists",))
    result = await _slack_adapter_with_client(client).preflight(request)

    assert result.evidence_refs == (
        "slack-workspace:T1:enabled-installation",
        "slack-channel:C1:conversations.info:exists",
    )
    assert client.closed

    mismatch = _FakeSlackClient(channel_response={"channel": {"id": "C2"}})
    with pytest.raises(InvariantViolation) as exc_info:
        await _slack_adapter_with_client(mismatch).preflight(request)
    assert exc_info.value.invariant == "EFFECT_SLACK_PREFLIGHT_TARGET_MISMATCH"
    assert mismatch.closed

    unsupported = _FakeSlackClient()
    with pytest.raises(InvariantViolation) as exc_info:
        await _slack_adapter_with_client(unsupported).preflight(_request())
    assert exc_info.value.invariant == "EFFECT_SLACK_PRECONDITION_UNSUPPORTED"
    assert not unsupported.closed


@pytest.mark.asyncio
async def test_slack_dispatch_classifies_success_rejection_and_unknown() -> None:
    success_client = _FakeSlackClient(
        post_response={"channel": "C1", "ts": "1717.001"}
    )
    success = await _slack_adapter_with_client(success_client).dispatch(_request())
    assert success.fate is subject.ActionDispatchFate.SUCCEEDED
    assert success.external_state_evidence_refs == (
        "slack-message:C1:1717.001",
    )
    assert success_client.post_arguments == {
        "channel": "C1",
        "text": "Review the Atlas escalation",
        "client_msg_id": "effect:atlas:1",
    }
    assert success_client.closed

    rejected_client = _FakeSlackClient(
        post_error=SlackApiError(
            "Slack returned ok=false",
            slack_error="channel_not_found",
        )
    )
    rejected = await _slack_adapter_with_client(rejected_client).dispatch(
        _request()
    )
    assert rejected.fate is subject.ActionDispatchFate.REJECTED
    assert rejected.provider_observation_refs == (
        "slack:chat.postMessage:rejected:channel_not_found",
    )
    assert rejected_client.closed

    ambiguous_client = _FakeSlackClient(
        post_error=SlackApiError("retry budget exhausted")
    )
    ambiguous = await _slack_adapter_with_client(ambiguous_client).dispatch(
        _request()
    )
    assert ambiguous.fate is subject.ActionDispatchFate.UNKNOWN
    assert ambiguous.provider_observation_refs == (
        "slack:chat.postMessage:ambiguous-client-error",
    )
    assert ambiguous_client.closed

    missing_identity_client = _FakeSlackClient(
        post_response={"ok": True, "channel": "C1"}
    )
    missing_identity = await _slack_adapter_with_client(
        missing_identity_client
    ).dispatch(_request())
    assert missing_identity.fate is subject.ActionDispatchFate.UNKNOWN
    assert missing_identity_client.closed

    wrong_channel_client = _FakeSlackClient(
        post_response={"channel": "C2", "ts": "1717.002"}
    )
    wrong_channel = await _slack_adapter_with_client(
        wrong_channel_client
    ).dispatch(_request())
    assert wrong_channel.fate is subject.ActionDispatchFate.UNKNOWN
    assert wrong_channel.external_state_evidence_refs == (
        "slack-message:C2:1717.002",
    )


class _FakePool:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self.rows = rows
        self.calls: list[tuple[UUID, str | None]] = []

    async def fetch(
        self,
        query: str,
        tenant_id: UUID,
        team_id: str | None,
    ) -> list[dict[str, Any]]:
        assert "LIMIT 2" in query
        self.calls.append((tenant_id, team_id))
        if team_id is None:
            return self.rows[:2]
        return [
            row for row in self.rows if row["installation_id"] == team_id
        ][:2]


@pytest.mark.asyncio
async def test_slack_installation_selection_is_unambiguous_and_exact(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    constructed: list[dict[str, Any]] = []

    class FakeConstructedSlackClient:
        def __init__(self, **kwargs: Any) -> None:
            constructed.append(kwargs)

    monkeypatch.setattr(subject, "SlackClient", FakeConstructedSlackClient)
    rows = [
        {"id": uuid4(), "installation_id": "T1"},
        {"id": uuid4(), "installation_id": "T2"},
    ]
    adapter = object.__new__(subject._SlackMessageAdapter)
    adapter._pool = _FakePool(rows)
    adapter._secret_store = object()

    with pytest.raises(InvariantViolation) as exc_info:
        await adapter._client(_request())
    assert exc_info.value.invariant == "EFFECT_SLACK_INSTALLATION_AMBIGUOUS"

    selected_request = _request(
        parameters={
            "channel_id": "C1",
            "text": "Review the Atlas escalation",
            "team_id": "T2",
        }
    )
    client, team_id = await adapter._client(selected_request)
    assert isinstance(client, FakeConstructedSlackClient)
    assert team_id == "T2"
    assert constructed[-1]["installation_row_id"] == rows[1]["id"]
    assert constructed[-1]["team_id"] == "T2"
    assert constructed[-1]["max_attempts"] == 3
    assert adapter._pool.calls[-1] == (selected_request.tenant_id, "T2")

    missing = object.__new__(subject._SlackMessageAdapter)
    missing._pool = _FakePool([])
    missing._secret_store = object()
    with pytest.raises(InvariantViolation) as exc_info:
        await missing._client(_request())
    assert exc_info.value.invariant == "EFFECT_SLACK_INSTALLATION_MISSING"

    with pytest.raises(InvariantViolation) as exc_info:
        await adapter._client(
            _request(
                parameters={
                    "channel_id": "C1",
                    "text": "Review the Atlas escalation",
                    "team_id": " ",
                }
            )
        )
    assert exc_info.value.invariant == "EFFECT_SLACK_TEAM_ID_INVALID"
