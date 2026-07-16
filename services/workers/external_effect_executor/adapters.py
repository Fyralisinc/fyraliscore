"""Provider adapter boundary for fenced external-effect execution.

Adapters perform two deliberately separate operations:

* ``preflight`` is read-only and returns fresh evidence that the provider
  target exists and the adapter can evaluate its machine-checkable
  preconditions.
* ``dispatch`` is effectful and may only be called after the canonical effect
  ledger contains ``dispatch_intent_recorded`` for the exact request.

The registry resolves an adapter by the immutable adapter/provider identity in
the registered capability. It never guesses from an operation name alone.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Protocol
from uuid import UUID

import asyncpg
import httpx

from lib.contracts.execution import ActionAdapterCapabilities
from lib.shared.errors import InvariantViolation
from lib.shared.secrets import build_secret_store
from services.ingest.integrations.slack.client import SlackApiError, SlackClient


@dataclass(frozen=True, slots=True)
class ActionAdapterRequest:
    """Exact canonical provider request presented to an adapter."""

    tenant_id: UUID
    effect_attempt_id: UUID
    operation: str
    parameters: dict[str, Any]
    provider_idempotency_key: str
    target_grounding_refs: tuple[str, ...]
    declared_preconditions: tuple[str, ...]
    capabilities: ActionAdapterCapabilities

    def __post_init__(self) -> None:
        if self.tenant_id != self.capabilities.tenant_id:
            raise InvariantViolation(
                "EFFECT_ADAPTER_TENANT_MISMATCH",
                "adapter request and registered capability belong to different tenants",
                request_tenant_id=str(self.tenant_id),
                capability_tenant_id=str(self.capabilities.tenant_id),
            )
        if self.operation not in self.capabilities.permitted_operations:
            raise InvariantViolation(
                "EFFECT_ADAPTER_OPERATION_UNSUPPORTED",
                "exact adapter capability does not permit the requested operation",
                operation=self.operation,
                capability_id=str(self.capabilities.capability_id),
            )
        if not self.parameters:
            raise ValueError("adapter request parameters cannot be empty")
        if not self.provider_idempotency_key.strip():
            raise ValueError("provider idempotency key cannot be empty")
        if not self.target_grounding_refs:
            raise ValueError("adapter request requires exact target grounding")
        if not self.declared_preconditions:
            raise ValueError("adapter request requires declared preconditions")


@dataclass(frozen=True, slots=True)
class ActionPreflightResult:
    """Fresh read-only evidence obtained immediately before dispatch."""

    evidence_refs: tuple[str, ...]

    def __post_init__(self) -> None:
        if not self.evidence_refs or any(not ref.strip() for ref in self.evidence_refs):
            raise ValueError("successful preflight requires non-empty evidence refs")


class ActionDispatchFate(StrEnum):
    """Provider fate known after the effectful call returns."""

    SUCCEEDED = "succeeded"
    REJECTED = "rejected"
    FAILED = "failed"
    UNKNOWN = "unknown"


@dataclass(frozen=True, slots=True)
class ActionDispatchResult:
    """Typed provider result; unknown is never coerced into failure or success."""

    fate: ActionDispatchFate
    reason: str
    provider_observation_refs: tuple[str, ...] = ()
    external_state_evidence_refs: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not self.reason.strip():
            raise ValueError("dispatch result reason cannot be empty")
        if self.fate in {
            ActionDispatchFate.SUCCEEDED,
            ActionDispatchFate.REJECTED,
            ActionDispatchFate.FAILED,
        } and not (
            self.provider_observation_refs or self.external_state_evidence_refs
        ):
            raise ValueError("known provider fate requires provider evidence")
        if (
            self.fate is ActionDispatchFate.SUCCEEDED
            and not self.external_state_evidence_refs
        ):
            raise ValueError("successful dispatch requires external state evidence")


class ActionAdapter(Protocol):
    """Provider-specific read-check and effectful dispatch implementation."""

    adapter_name: str
    provider_name: str

    async def preflight(
        self,
        request: ActionAdapterRequest,
    ) -> ActionPreflightResult: ...

    async def dispatch(
        self,
        request: ActionAdapterRequest,
    ) -> ActionDispatchResult: ...


class ActionAdapterRegistry(Protocol):
    """Resolve the exact adapter identity frozen in a capability."""

    async def resolve(self, request: ActionAdapterRequest) -> ActionAdapter: ...


class StaticActionAdapterRegistry:
    """Small explicit registry used by tests and bounded deployments."""

    def __init__(self, adapters: tuple[ActionAdapter, ...]) -> None:
        self._adapters: dict[tuple[str, str], ActionAdapter] = {}
        for adapter in adapters:
            key = (adapter.adapter_name, adapter.provider_name)
            if key in self._adapters:
                raise ValueError(f"duplicate action adapter registration: {key}")
            self._adapters[key] = adapter

    async def resolve(self, request: ActionAdapterRequest) -> ActionAdapter:
        key = (
            request.capabilities.adapter_name,
            request.capabilities.provider_name,
        )
        adapter = self._adapters.get(key)
        if adapter is None:
            raise InvariantViolation(
                "EFFECT_ADAPTER_NOT_REGISTERED",
                "no action adapter matches the exact registered capability identity",
                adapter_name=key[0],
                provider_name=key[1],
            )
        return adapter


class _ProductionActionAdapterRegistry:
    """Production registry for adapters implemented by Fyralis Core."""

    def __init__(self, pool: asyncpg.Pool) -> None:
        self._slack = _SlackMessageAdapter(pool)

    async def resolve(self, request: ActionAdapterRequest) -> ActionAdapter:
        key = (
            request.capabilities.adapter_name,
            request.capabilities.provider_name,
        )
        supported = {
            (self._slack.adapter_name, self._slack.provider_name): self._slack,
        }
        adapter = supported.get(key)
        if adapter is None:
            raise InvariantViolation(
                "EFFECT_ADAPTER_NOT_REGISTERED",
                "no production adapter matches the exact registered capability",
                adapter_name=key[0],
                provider_name=key[1],
            )
        return adapter


class _SlackMessageAdapter:
    """Production Slack ``chat.postMessage`` adapter.

    A tenant with multiple Slack installations must name ``team_id`` in the
    immutable InterventionSpec parameters. A single enabled installation can
    be selected unambiguously without adding a redundant parameter.
    """

    adapter_name = "slack-message-delivery"
    provider_name = "slack"

    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool
        self._secret_store = build_secret_store(pool)

    async def preflight(
        self,
        request: ActionAdapterRequest,
    ) -> ActionPreflightResult:
        unsupported = {
            clause
            for clause in request.declared_preconditions
            if clause.strip().lower() not in {"channel exists"}
        }
        if unsupported:
            raise InvariantViolation(
                "EFFECT_SLACK_PRECONDITION_UNSUPPORTED",
                "Slack adapter cannot prove every declared precondition",
                unsupported_preconditions=sorted(unsupported),
            )
        channel_id = _required_text(request.parameters, "channel_id")
        client, team_id = await self._client(request)
        try:
            response = await client.conversations_info(channel_id)
        finally:
            await client.aclose()
        channel = response.get("channel") or {}
        provider_channel_id = str(channel.get("id") or "")
        if provider_channel_id != channel_id:
            raise InvariantViolation(
                "EFFECT_SLACK_PREFLIGHT_TARGET_MISMATCH",
                "Slack preflight did not return the exact requested channel",
                requested_channel_id=channel_id,
            )
        return ActionPreflightResult(
            evidence_refs=(
                f"slack-workspace:{team_id}:enabled-installation",
                f"slack-channel:{provider_channel_id}:conversations.info:exists",
            )
        )

    async def dispatch(
        self,
        request: ActionAdapterRequest,
    ) -> ActionDispatchResult:
        channel_id = _required_text(request.parameters, "channel_id")
        text = _required_text(request.parameters, "text")
        client, _team_id = await self._client(request)
        try:
            try:
                response = await client.chat_post_message(
                    channel=channel_id,
                    text=text,
                    client_msg_id=request.provider_idempotency_key,
                )
            except SlackApiError as exc:
                slack_error = str(exc.context.get("slack_error") or "")
                if slack_error:
                    return ActionDispatchResult(
                        fate=ActionDispatchFate.REJECTED,
                        reason=f"Slack rejected chat.postMessage: {slack_error}",
                        provider_observation_refs=(
                            f"slack:chat.postMessage:rejected:{slack_error}",
                        ),
                    )
                return ActionDispatchResult(
                    fate=ActionDispatchFate.UNKNOWN,
                    reason=(
                        "Slack dispatch outcome is ambiguous after transport or "
                        "retry-budget failure"
                    ),
                    provider_observation_refs=(
                        "slack:chat.postMessage:ambiguous-client-error",
                    ),
                )
            except httpx.HTTPStatusError as exc:
                status = exc.response.status_code
                if 400 <= status < 500 and status != 429:
                    return ActionDispatchResult(
                        fate=ActionDispatchFate.REJECTED,
                        reason=f"Slack rejected chat.postMessage with HTTP {status}",
                        provider_observation_refs=(
                            f"slack:chat.postMessage:http:{status}",
                        ),
                    )
                return ActionDispatchResult(
                    fate=ActionDispatchFate.UNKNOWN,
                    reason=f"Slack dispatch outcome is ambiguous after HTTP {status}",
                    provider_observation_refs=(
                        f"slack:chat.postMessage:ambiguous-http:{status}",
                    ),
                )
            except Exception as exc:  # noqa: BLE001
                return ActionDispatchResult(
                    fate=ActionDispatchFate.UNKNOWN,
                    reason=(
                        "Slack dispatch outcome is ambiguous after "
                        f"{type(exc).__name__}"
                    ),
                    provider_observation_refs=(
                        f"slack:chat.postMessage:ambiguous:{type(exc).__name__}",
                    ),
                )
        finally:
            await client.aclose()

        message = response.get("message") or {}
        ts = str(response.get("ts") or message.get("ts") or "")
        response_channel = str(response.get("channel") or channel_id)
        if response_channel != channel_id:
            return ActionDispatchResult(
                fate=ActionDispatchFate.UNKNOWN,
                reason=(
                    "Slack returned a message identity for a different channel; "
                    "the external effect requires reconciliation"
                ),
                provider_observation_refs=(
                    "slack:chat.postMessage:channel-mismatch",
                ),
                external_state_evidence_refs=(
                    f"slack-message:{response_channel}:{ts or 'unknown-ts'}",
                ),
            )
        if not ts:
            return ActionDispatchResult(
                fate=ActionDispatchFate.UNKNOWN,
                reason="Slack accepted the request but returned no message timestamp",
                provider_observation_refs=(
                    "slack:chat.postMessage:ok-without-message-id",
                ),
            )
        return ActionDispatchResult(
            fate=ActionDispatchFate.SUCCEEDED,
            reason="Slack returned the exact persisted message identity",
            provider_observation_refs=("slack:chat.postMessage:ok",),
            external_state_evidence_refs=(
                f"slack-message:{response_channel}:{ts}",
            ),
        )

    async def _client(
        self,
        request: ActionAdapterRequest,
    ) -> tuple[SlackClient, str]:
        requested_team_id = request.parameters.get("team_id")
        if requested_team_id is not None and (
            not isinstance(requested_team_id, str) or not requested_team_id.strip()
        ):
            raise InvariantViolation(
                "EFFECT_SLACK_TEAM_ID_INVALID",
                "Slack team_id must be a non-empty string when provided",
            )
        rows = await self._pool.fetch(
            """
            SELECT id, installation_id
            FROM provider_installations
            WHERE tenant_id=$1
              AND provider='slack'
              AND enabled=TRUE
              AND ($2::text IS NULL OR installation_id=$2)
            ORDER BY installed_at, id
            LIMIT 2
            """,
            request.tenant_id,
            requested_team_id,
        )
        if not rows:
            raise InvariantViolation(
                "EFFECT_SLACK_INSTALLATION_MISSING",
                "no enabled Slack installation matches the exact request",
            )
        if len(rows) != 1:
            raise InvariantViolation(
                "EFFECT_SLACK_INSTALLATION_AMBIGUOUS",
                "multiple enabled Slack installations require an exact team_id",
            )
        row = rows[0]
        team_id = str(row["installation_id"])
        max_attempts = 3 if request.capabilities.idempotency_supported else 1
        return (
            SlackClient(
                pool=self._pool,
                secret_store=self._secret_store,
                tenant_id=request.tenant_id,
                installation_row_id=row["id"],
                team_id=team_id,
                max_attempts=max_attempts,
            ),
            team_id,
        )


def _required_text(parameters: dict[str, Any], name: str) -> str:
    value = parameters.get(name)
    if not isinstance(value, str) or not value.strip():
        raise InvariantViolation(
            "EFFECT_ADAPTER_PARAMETER_INVALID",
            f"adapter parameter {name!r} must be a non-empty string",
            parameter=name,
        )
    return value


def build_production_action_adapter_registry(
    pool: asyncpg.Pool,
) -> ActionAdapterRegistry:
    """Build the closed production adapter registry."""

    return _ProductionActionAdapterRegistry(pool)


__all__ = [
    "ActionAdapter",
    "ActionAdapterRegistry",
    "ActionAdapterRequest",
    "ActionDispatchFate",
    "ActionDispatchResult",
    "ActionPreflightResult",
    "StaticActionAdapterRegistry",
    "build_production_action_adapter_registry",
]
