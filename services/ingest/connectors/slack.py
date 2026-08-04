"""Self-contained Slack connector capabilities.

This module owns Slack wire semantics and talks to Slack exclusively through
the governed HTTP and secret host ports.  It deliberately has no dependency on
the legacy ingestion planner, fetcher, handler, or their ambient runtime state.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import re
from datetime import datetime, timezone
from typing import Any

from services.ingest.source_contract.connector import BindingContext, OperationContext
from services.ingest.source_contract.errors import (
    AuthenticationRejectedError,
    PayloadRejectedError,
    RateLimitedError,
    ResourceNotFoundError,
    TransientSourceError,
)
from services.ingest.source_contract.host_services import GovernedHttpRequest
from services.ingest.source_contract.identity import SlotId
from services.ingest.source_contract.models import (
    BoundedWebhookRequest,
    CursorState,
    FetchRequest,
    FetchedPage,
    IdentityInput,
    NormalizationInput,
    ObservationDraft,
    PlanRequest,
    PlanResult,
    ReconciliationDecision,
    ReconciliationRequest,
    RepairShard,
    ShardPlan,
    SourceRecord,
    SourceObjectRef,
    VerifiedWebhookEvent,
    VerifiedWebhookResult,
)


_MENTION_RE = re.compile(r"<@([A-Z0-9]+)(?:\|[^>]+)?>")
_CHANNEL_RE = re.compile(r"<#([A-Z0-9]+)(?:\|[^>]+)?>")
_URL_RE = re.compile(r"<(https?://[^|>]+)(?:\|[^>]+)?>")
_CHANNEL_SHARD = "slack_channel_window"
_DM_SHARD = "slack_dm_window"


def _record(payload: dict[str, Any]) -> SourceRecord:
    return SourceRecord(native_type="event_callback", payload=payload)


def _event(payload: dict[str, Any]) -> dict[str, Any]:
    value = payload.get("event")
    return value if isinstance(value, dict) else payload


def slack_external_id(input: IdentityInput) -> str:
    event = _event(_payload(input.record))
    channel = event.get("channel") or event.get("channel_id")
    subtype = event.get("subtype")
    if subtype == "message_changed" and isinstance(event.get("message"), dict):
        timestamp = event["message"].get("ts")
    elif subtype == "message_deleted":
        timestamp = event.get("deleted_ts")
    else:
        timestamp = event.get("ts") or event.get("event_ts")
    if not isinstance(channel, str) or not isinstance(timestamp, str):
        raise PayloadRejectedError("Slack identity requires channel and timestamp")
    return f"{channel}:{timestamp}"


def _payload(record: SourceRecord) -> dict[str, Any]:
    if not isinstance(record.payload, dict):
        raise PayloadRejectedError("Slack requires a JSON object payload")
    return record.payload


def _parse_timestamp(value: Any) -> datetime:
    if not isinstance(value, str):
        raise PayloadRejectedError("Slack event is missing a timestamp")
    try:
        return datetime.fromtimestamp(float(value), tz=timezone.utc)
    except ValueError as exc:
        raise PayloadRejectedError("Slack timestamp is invalid") from exc


def _entities(text: str) -> tuple[dict[str, Any], ...]:
    values = [
        *(("slack_user", match.group(1)) for match in _MENTION_RE.finditer(text)),
        *(("slack_channel", match.group(1)) for match in _CHANNEL_RE.finditer(text)),
        *(("url", match.group(1)) for match in _URL_RE.finditer(text)),
    ]
    seen: set[tuple[str, str]] = set()
    result: list[dict[str, Any]] = []
    for kind, identifier in values:
        if (kind, identifier) not in seen:
            seen.add((kind, identifier))
            result.append({"type": kind, "id": identifier})
    return tuple(result)


class SlackNormalization:
    async def normalize(
        self, request: NormalizationInput, context: OperationContext
    ) -> tuple[ObservationDraft, ...]:
        payload = _payload(request.record)
        event = _event(payload)
        subtype = event.get("subtype")

        if subtype == "message_deleted":
            channel = event.get("channel") or event.get("channel_id")
            deleted_ts = event.get("deleted_ts")
            event_ts = event.get("event_ts") or event.get("ts") or deleted_ts
            previous = (
                event.get("previous_message")
                if isinstance(event.get("previous_message"), dict)
                else {}
            )
            if not isinstance(channel, str) or not isinstance(deleted_ts, str):
                raise PayloadRejectedError(
                    "Slack message deletion requires channel and deleted_ts"
                )
            if not isinstance(event_ts, str):
                raise PayloadRejectedError("Slack deletion requires an event timestamp")
            actor = previous.get("user") or previous.get("user_id")
            previous_revision = (
                (previous.get("edited") or {}).get("ts")
                if isinstance(previous.get("edited"), dict)
                else None
            ) or previous.get("edited_ts") or deleted_ts
            content = {
                "channel": channel,
                "deleted_ts": deleted_ts,
                "event_ts": event_ts,
                "previous_message": previous,
                "event_type": event.get("type"),
                "subtype": subtype,
                "team": event.get("team") or payload.get("team_id"),
            }
            return (
                ObservationDraft(
                    source_channel="slack:message",
                    content_text=f"Slack message {channel}:{deleted_ts} was deleted",
                    content=content,
                    occurred_at=_parse_timestamp(event_ts),
                    trust_tier="attested_agent",
                    kind="state_change",
                    source_actor_ref=f"slack:{actor}" if actor else None,
                    external_id=f"{channel}:{deleted_ts}",
                    raw_payload=payload,
                    source_object=SourceObjectRef(
                        object_type="message",
                        object_id=f"{channel}:{deleted_ts}",
                        revision_id=f"deleted:{event_ts}",
                        operation="delete",
                        source_recorded_at=_parse_timestamp(event_ts),
                        supersedes_revision_id=str(previous_revision),
                        container_object_type="channel",
                        container_object_id=channel,
                        thread_id=previous.get("thread_ts"),
                    ),
                ),
            )

        original_timestamp: str | None = None
        if subtype == "message_changed" and isinstance(event.get("message"), dict):
            message = event["message"]
            text = message.get("text")
            original_timestamp = message.get("ts")
            actor = message.get("user") or message.get("user_id")
            timestamp = (
                message.get("edited_ts")
                or (message.get("edited") or {}).get("ts")
                or event.get("event_ts")
                or event.get("ts")
                or original_timestamp
            )
        else:
            text = event.get("text")
            actor = event.get("user") or event.get("user_id")
            timestamp = event.get("ts") or event.get("event_ts")
        channel = event.get("channel") or event.get("channel_id")
        if not isinstance(text, str) or not isinstance(channel, str):
            raise PayloadRejectedError("Slack message requires text and channel")
        if not isinstance(timestamp, str):
            raise PayloadRejectedError("Slack message requires a timestamp")
        content: dict[str, Any] = {
            "channel": channel,
            "channel_type": event.get("channel_type"),
            "ts": timestamp,
            "user": actor,
            "text": text,
            "team": event.get("team") or payload.get("team_id"),
            "event_type": event.get("type"),
            "subtype": subtype,
        }
        if original_timestamp is not None:
            content["original_ts"] = original_timestamp
        stable_timestamp = original_timestamp or timestamp
        operation = "update" if original_timestamp is not None else "create"
        previous_revision = original_timestamp if original_timestamp is not None else None
        return (
            ObservationDraft(
                source_channel="slack:message",
                content_text=text,
                content=content,
                occurred_at=_parse_timestamp(timestamp),
                trust_tier="attested_agent",
                kind="signal",
                source_actor_ref=f"slack:{actor}" if actor else None,
                external_id=f"{channel}:{stable_timestamp}",
                entities_hint=_entities(text),
                raw_payload=payload,
                source_object=SourceObjectRef(
                    object_type="message",
                    object_id=f"{channel}:{stable_timestamp}",
                    revision_id=timestamp,
                    operation=operation,
                    source_recorded_at=_parse_timestamp(timestamp),
                    valid_from=_parse_timestamp(stable_timestamp),
                    supersedes_revision_id=previous_revision,
                    container_object_type="channel",
                    container_object_id=channel,
                    thread_id=message.get("thread_ts") if original_timestamp else event.get("thread_ts"),
                ),
            ),
        )


class SlackWebhook:
    def __init__(self, binding: BindingContext) -> None:
        self._binding = binding

    async def verify_and_decode(
        self, request: BoundedWebhookRequest, context: OperationContext
    ) -> VerifiedWebhookResult:
        secret = await self._binding.services.secrets.resolve(
            SlotId("webhook_signing_secret")
        )
        headers = {key.lower(): value for key, value in request.headers.items()}
        timestamp = headers.get("x-slack-request-timestamp", "")
        signature = headers.get("x-slack-signature", "")
        try:
            timestamp_seconds = int(timestamp)
        except ValueError as exc:
            raise AuthenticationRejectedError(
                "Slack webhook timestamp is invalid"
            ) from exc
        if abs(int(request.received_at.timestamp()) - timestamp_seconds) > 300:
            raise AuthenticationRejectedError("Slack webhook is outside replay window")
        signed = b"v0:" + timestamp.encode() + b":" + request.body
        expected = (
            "v0=" + hmac.new(secret.reveal_bytes(), signed, hashlib.sha256).hexdigest()
        )
        if not hmac.compare_digest(expected, signature):
            raise AuthenticationRejectedError("Slack webhook signature is invalid")
        try:
            payload = json.loads(request.body)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise PayloadRejectedError("Slack webhook body is not valid JSON") from exc
        if not isinstance(payload, dict):
            raise PayloadRejectedError("Slack webhook body must be an object")
        if payload.get("type") == "url_verification":
            return VerifiedWebhookResult(events=(), response_status_hint=200)
        event = _event(payload)
        external_id = payload.get("team_id") or (payload.get("team") or {}).get("id")
        if not isinstance(external_id, str) or not external_id:
            raise PayloadRejectedError("Slack webhook does not identify a workspace")
        return VerifiedWebhookResult(
            events=(
                VerifiedWebhookEvent(
                    external_installation_id=external_id,
                    native_event_type=str(event.get("type") or "event"),
                    record=_record(payload),
                    signed_at=datetime.fromtimestamp(timestamp_seconds, timezone.utc),
                    verification_evidence={"scheme": "slack-v0-hmac-sha256"},
                ),
            )
        )


class SlackPull:
    def __init__(self, binding: BindingContext) -> None:
        self._binding = binding

    async def _token(self) -> str:
        return (
            await self._binding.services.secrets.resolve(SlotId("oauth_access_token"))
        ).reveal_text()

    async def _call(
        self,
        context: OperationContext,
        method: str,
        *,
        token: str,
        query: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        response = await context.services.http.send(
            GovernedHttpRequest(
                method="GET",
                url=f"https://slack.com/api/{method}",
                headers=(("authorization", f"Bearer {token}"),),
                query=tuple((query or {}).items()),
            )
        )
        try:
            payload = json.loads(response.body)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise TransientSourceError("Slack returned malformed JSON") from exc
        if not isinstance(payload, dict):
            raise TransientSourceError("Slack returned an invalid response")
        if response.status_code == 429:
            raise RateLimitedError("Slack rate limit was reached")
        if response.status_code >= 500:
            raise TransientSourceError("Slack is temporarily unavailable")
        if response.status_code >= 400 or payload.get("ok") is not True:
            error = str(payload.get("error") or "provider_rejected")
            if error in {"channel_not_found", "not_in_channel"}:
                raise ResourceNotFoundError(
                    "Slack channel is inaccessible", details={"reason": error}
                )
            if error in {"invalid_auth", "token_revoked", "account_inactive"}:
                raise AuthenticationRejectedError("Slack credential was rejected")
            raise TransientSourceError(
                "Slack API request failed", details={"reason": error}
            )
        return payload

    async def _external_installation_id(self) -> str:
        data = await self._binding.services.installation_store.read("provider")
        value = (data.values if data is not None else {}).get(
            "external_installation_id"
        )
        return str(value or self._binding.installation.id)

    async def plan(self, request: PlanRequest, context: OperationContext) -> PlanResult:
        token = await self._token()
        team_id = await self._external_installation_id()
        cursor = ""
        shards: list[ShardPlan] = []
        selected = set(request.selected_resources)
        while True:
            query = {
                "types": "public_channel,private_channel",
                "exclude_archived": "true",
                "limit": "200",
            }
            if cursor:
                query["cursor"] = cursor
            payload = await self._call(
                context, "conversations.list", token=token, query=query
            )
            for channel in payload.get("channels") or ():
                if not isinstance(channel, dict) or not isinstance(
                    channel.get("id"), str
                ):
                    continue
                identifier = channel["id"]
                if selected and identifier not in selected:
                    continue
                shards.append(
                    ShardPlan(
                        kind=_CHANNEL_SHARD,
                        identifier={
                            "shard_kind": _CHANNEL_SHARD,
                            "channel_id": identifier,
                            "channel_name": channel.get("name"),
                            "team_id": channel.get("context_team_id") or team_id,
                            "installation_id": team_id,
                        },
                        window_start=request.window_start,
                        window_end=request.window_end,
                    )
                )
            cursor = str(
                ((payload.get("response_metadata") or {}).get("next_cursor")) or ""
            )
            if not cursor:
                break
        try:
            user_token = (
                await self._binding.services.secrets.resolve(
                    SlotId("oauth_user_access_token")
                )
            ).reveal_text()
        except Exception:
            user_token = None
        if user_token is not None:
            cursor = ""
            while True:
                query = {"types": "im,mpim", "exclude_archived": "true", "limit": "200"}
                if cursor:
                    query["cursor"] = cursor
                payload = await self._call(
                    context, "conversations.list", token=user_token, query=query
                )
                for channel in payload.get("channels") or ():
                    if not isinstance(channel, dict) or not isinstance(
                        channel.get("id"), str
                    ):
                        continue
                    identifier = channel["id"]
                    if selected and identifier not in selected:
                        continue
                    shards.append(
                        ShardPlan(
                            kind=_DM_SHARD,
                            identifier={
                                "shard_kind": _DM_SHARD,
                                "channel_id": identifier,
                                "channel_type": channel.get("channel_type")
                                or ("mpim" if identifier.startswith("G") else "im"),
                                "counterpart_user_id": channel.get("user"),
                                "team_id": channel.get("context_team_id") or team_id,
                                "installation_id": team_id,
                            },
                            priority=1.25,
                            window_start=request.window_start,
                            window_end=request.window_end,
                        )
                    )
                cursor = str(
                    ((payload.get("response_metadata") or {}).get("next_cursor")) or ""
                )
                if not cursor:
                    break
        return PlanResult(shards=tuple(shards))

    async def fetch(
        self, request: FetchRequest, context: OperationContext
    ) -> FetchedPage:
        identifier = request.shard.identifier
        channel = identifier.get("channel_id")
        if not isinstance(channel, str):
            raise PayloadRejectedError("Slack shard requires channel_id")
        current = dict(request.cursor.payload) if request.cursor else {}
        query = {
            "channel": channel,
            "limit": str(request.page_size_hint or 200),
        }
        if current.get("next_cursor"):
            query["cursor"] = str(current["next_cursor"])
        if request.shard.window_start:
            query["oldest"] = str(request.shard.window_start.timestamp())
        if request.shard.window_end:
            query["latest"] = str(request.shard.window_end.timestamp())
        token = await self._token()
        if request.shard.kind == _DM_SHARD:
            token = (
                await self._binding.services.secrets.resolve(
                    SlotId("oauth_user_access_token")
                )
            ).reveal_text()
        try:
            payload = await self._call(
                context, "conversations.history", token=token, query=query
            )
        except ResourceNotFoundError:
            return FetchedPage(end_of_data=True)
        messages = [
            item for item in payload.get("messages") or () if isinstance(item, dict)
        ]
        records: list[SourceRecord] = []
        oldest = current.get("oldest_seen_ts")
        newest = current.get("newest_seen_ts")
        for message in messages:
            timestamp = message.get("ts")
            if isinstance(timestamp, str):
                oldest = timestamp if oldest is None or timestamp < oldest else oldest
                newest = timestamp if newest is None or timestamp > newest else newest
            event = {**message, "channel": channel}
            if identifier.get("channel_type") is not None:
                event["channel_type"] = identifier["channel_type"]
            records.append(
                _record(
                    {
                        "type": "event_callback",
                        "team_id": identifier.get("team_id"),
                        "event": event,
                    }
                )
            )
        next_cursor = str(
            ((payload.get("response_metadata") or {}).get("next_cursor")) or ""
        )
        end = not next_cursor
        checkpoint = CursorState(
            schema_version=1,
            payload={
                "next_cursor": next_cursor or None,
                "oldest_seen_ts": oldest,
                "newest_seen_ts": newest,
                "messages_seen": int(current.get("messages_seen", 0)) + len(records),
            },
        )
        return FetchedPage(
            records=tuple(records),
            next_cursor=None if end else checkpoint,
            checkpoint=checkpoint,
            end_of_data=end,
        )

    async def reconcile(
        self, request: ReconciliationRequest, context: OperationContext
    ) -> ReconciliationDecision:
        repairs: list[RepairShard] = []
        tokens: dict[str, str] = {}
        for summary in request.shards:
            if summary.state != "done" or summary.cursor is None:
                continue
            newest = summary.cursor.payload.get("newest_seen_ts")
            channel = summary.shard.identifier.get("channel_id")
            if not isinstance(newest, str) or not isinstance(channel, str):
                continue
            slot = (
                "oauth_user_access_token"
                if summary.shard.kind == _DM_SHARD
                else "oauth_access_token"
            )
            if slot not in tokens:
                tokens[slot] = (
                    await self._binding.services.secrets.resolve(SlotId(slot))
                ).reveal_text()
            try:
                payload = await self._call(
                    context,
                    "conversations.history",
                    token=tokens[slot],
                    query={"channel": channel, "oldest": newest, "limit": "1"},
                )
            except ResourceNotFoundError:
                continue
            if payload.get("messages"):
                identifier = dict(summary.shard.identifier)
                identifier.update(
                    {
                        "parent_shard_id": str(summary.shard_id),
                        "gap_baseline_ts": newest,
                    }
                )
                repairs.append(
                    RepairShard(
                        shard=ShardPlan(
                            kind=summary.shard.kind,
                            identifier=identifier,
                            priority=1.5,
                        ),
                        parent_shard_id=summary.shard_id,
                    )
                )
        return ReconciliationDecision(
            has_gaps=bool(repairs),
            reason_code="newer_messages" if repairs else "clean",
            message=f"Slack reconciliation found {len(repairs)} gap(s)."
            if repairs
            else "",
            new_shards=tuple(repairs),
        )


__all__ = [
    "SlackNormalization",
    "SlackPull",
    "SlackWebhook",
    "slack_external_id",
]
