"""Google Workspace connector capabilities with real watch/cursor semantics."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

from services.ingest.connectors.fleet import (
    FleetConfiguration,
    FleetNormalization,
    FleetSecretRotation,
    _identity,
)
from services.ingest.connectors.native import (
    CredentialHealthProbe,
    LocalCredentialCleanup,
    NativeIdentity,
    NativeSourceConnector,
    _manifest,
)
from services.ingest.connectors.provider_spec import SourceProfile
from services.ingest.source_contract.capabilities import (
    CLEANUP_V1,
    CONFIGURATION_V1,
    HEALTH_PROBE_V1,
    HISTORICAL_PULL_V1,
    IDENTITY_V1,
    INCREMENTAL_POLL_V1,
    NORMALIZATION_V1,
    PUSH_SUBSCRIPTION_V1,
    RECONCILIATION_V1,
    SECRET_ROTATION_V1,
)
from services.ingest.source_contract.capabilities.ingestion import (
    SubscriptionRequest,
    SubscriptionState,
)
from services.ingest.source_contract.connector import BindingContext, OperationContext
from services.ingest.source_contract.errors import (
    AuthenticationRejectedError,
    PayloadRejectedError,
    RateLimitedError,
    StateIncompatibleError,
    TransientSourceError,
)
from services.ingest.source_contract.host_services import GovernedHttpRequest
from services.ingest.source_contract.identity import SlotId
from services.ingest.source_contract.models import (
    CursorState,
    FetchRequest,
    FetchedPage,
    PlanRequest,
    PlanResult,
    PollRequest,
    ReconciliationDecision,
    ReconciliationRequest,
    RepairShard,
    ShardPlan,
    SourceRecord,
)


GMAIL = SourceProfile(
    source="gmail", ingress_kinds=("backfill", "poll", "pubsub"),
    api_origin="https://gmail.googleapis.com", collection_path="/gmail/v1/users/me/messages",
    channel="gmail:message", native_type="message", record_keys=("messages",),
    identity_fields=("id", "threadId"), occurred_fields=("internalDate", "historyId"),
    text_fields=("snippet", "subject"), auth_slot="oauth_access_token",
    cursor_parameter="pageToken", limit_parameter="maxResults",
    next_cursor_fields=("nextPageToken",),
)
CALENDAR = SourceProfile(
    source="google_calendar", ingress_kinds=("backfill", "poll", "pubsub"),
    api_origin="https://www.googleapis.com", collection_path="/calendar/v3/calendars/primary/events",
    channel="google_calendar:event", native_type="event", record_keys=("items",),
    identity_fields=("id", "iCalUID"), occurred_fields=("updated", "created", "start.dateTime"),
    text_fields=("summary", "description", "location"), auth_slot="oauth_access_token",
    trust_tier="authoritative", cursor_parameter="pageToken", limit_parameter="maxResults",
    next_cursor_fields=("nextPageToken", "nextSyncToken"),
)
DRIVE = SourceProfile(
    source="google_drive", ingress_kinds=("backfill", "poll", "pubsub"),
    api_origin="https://www.googleapis.com", collection_path="/drive/v3/files",
    channel="google_drive:file", native_type="file", record_keys=("files",),
    identity_fields=("id", "name"), occurred_fields=("modifiedTime", "createdTime"),
    text_fields=("name", "description", "webViewLink"), auth_slot="oauth_access_token",
    trust_tier="authoritative", cursor_parameter="pageToken", limit_parameter="pageSize",
    next_cursor_fields=("nextPageToken", "newStartPageToken"),
)


def _payload(response: Any, source: str) -> dict[str, Any]:
    if response.status_code == 429:
        raise RateLimitedError(f"{source} rate limit was reached")
    if response.status_code in {401, 403}:
        raise AuthenticationRejectedError(f"{source} rejected the OAuth credential")
    if response.status_code == 410:
        raise StateIncompatibleError(f"{source} incremental cursor expired")
    if response.status_code >= 500:
        raise TransientSourceError(f"{source} is temporarily unavailable")
    if response.status_code >= 400:
        raise PayloadRejectedError(f"{source} rejected the request")
    try:
        value = json.loads(response.body)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise TransientSourceError(f"{source} returned malformed JSON") from exc
    if not isinstance(value, dict):
        raise TransientSourceError(f"{source} returned a non-object response")
    return value


class GoogleWorkspaceIngestion:
    def __init__(self, binding: BindingContext, profile: SourceProfile) -> None:
        self._binding = binding
        self._profile = profile

    async def _send(
        self,
        context: OperationContext,
        *,
        url: str,
        query: tuple[tuple[str, str], ...],
        method: str = "GET",
        body: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        token = await self._binding.services.secrets.resolve(SlotId("oauth_access_token"))
        response = await context.services.http.send(
            GovernedHttpRequest(
                method=method,
                url=url,
                headers=(("authorization", f"Bearer {token.reveal_text()}"), ("content-type", "application/json")),
                query=query,
                body=json.dumps(body).encode() if body is not None else None,
            )
        )
        return _payload(response, self._profile.source)

    async def plan(self, request: PlanRequest, context: OperationContext) -> PlanResult:
        defaults = {"gmail": "me", "google_calendar": "primary", "google_drive": "root"}
        resources = request.selected_resources or (defaults[self._profile.source],)
        return PlanResult(
            shards=tuple(
                ShardPlan(
                    kind=f"{self._profile.source}_resource",
                    identifier={"resource_id": resource},
                    window_start=request.window_start,
                    window_end=request.window_end,
                )
                for resource in resources
            )
        )

    async def fetch(self, request: FetchRequest, context: OperationContext) -> FetchedPage:
        token = _cursor_value(request.cursor, "page_token")
        query = [(self._profile.limit_parameter, str(request.page_size_hint or 100))]
        if token:
            query.append(("pageToken", token))
        if self._profile.source == "google_calendar":
            resource = str(request.shard.identifier.get("resource_id") or "primary")
            url = f"https://www.googleapis.com/calendar/v3/calendars/{resource}/events"
            query.extend((("singleEvents", "true"), ("showDeleted", "true")))
        elif self._profile.source == "google_drive":
            url = "https://www.googleapis.com/drive/v3/files"
            query.append(("fields", "nextPageToken,files(*)"))
        else:
            url = "https://gmail.googleapis.com/gmail/v1/users/me/messages"
        payload = await self._send(context, url=url, query=tuple(query))
        return self._page(payload, page_key="page_token", checkpoint_key="snapshot")

    async def poll(self, request: PollRequest, context: OperationContext) -> FetchedPage:
        if self._profile.source == "gmail":
            start = _cursor_value(request.cursor, "history_id")
            if not start:
                raise StateIncompatibleError("Gmail polling requires a history_id checkpoint")
            page = _cursor_value(request.cursor, "page_token")
            query = [("startHistoryId", start), ("maxResults", str(request.page_size_hint or 100))]
            if page:
                query.append(("pageToken", page))
            payload = await self._send(
                context,
                url="https://gmail.googleapis.com/gmail/v1/users/me/history",
                query=tuple(query),
            )
            values = payload.get("history") if isinstance(payload.get("history"), list) else []
            records = tuple(SourceRecord(native_type="history", payload=item) for item in values if isinstance(item, dict))
            next_page = payload.get("nextPageToken")
            high_water = str(payload.get("historyId") or start)
            return FetchedPage(
                records=records,
                next_cursor=(CursorState(schema_version=1, payload={"history_id": start, "page_token": str(next_page)}) if next_page else None),
                checkpoint=CursorState(schema_version=1, payload={"history_id": high_water}),
                end_of_data=not bool(next_page),
            )
        if self._profile.source == "google_calendar":
            sync = _cursor_value(request.cursor, "sync_token")
            page = _cursor_value(request.cursor, "page_token")
            query = [("singleEvents", "true"), ("showDeleted", "true"), ("maxResults", str(request.page_size_hint or 100))]
            if sync:
                query.append(("syncToken", sync))
            if page:
                query.append(("pageToken", page))
            payload = await self._send(
                context,
                url="https://www.googleapis.com/calendar/v3/calendars/primary/events",
                query=tuple(query),
            )
            return self._page(payload, page_key="page_token", checkpoint_key="sync_token", checkpoint_field="nextSyncToken", preserve=sync)
        page = _cursor_value(request.cursor, "page_token")
        start = _cursor_value(request.cursor, "start_page_token")
        if not (page or start):
            bootstrap = await self._send(
                context,
                url="https://www.googleapis.com/drive/v3/changes/startPageToken",
                query=(),
            )
            start = str(bootstrap.get("startPageToken") or "")
        active = page or start
        payload = await self._send(
            context,
            url="https://www.googleapis.com/drive/v3/changes",
            query=(("pageToken", str(active)), ("pageSize", str(request.page_size_hint or 100)), ("fields", "nextPageToken,newStartPageToken,changes(*)")),
        )
        values = payload.get("changes") if isinstance(payload.get("changes"), list) else []
        records = tuple(SourceRecord(native_type="change", payload=item) for item in values if isinstance(item, dict))
        next_page = payload.get("nextPageToken")
        high_water = str(payload.get("newStartPageToken") or start or active)
        return FetchedPage(
            records=records,
            next_cursor=(CursorState(schema_version=1, payload={"start_page_token": start, "page_token": str(next_page)}) if next_page else None),
            checkpoint=CursorState(schema_version=1, payload={"start_page_token": high_water}),
            end_of_data=not bool(next_page),
        )

    def _page(
        self,
        payload: dict[str, Any],
        *,
        page_key: str,
        checkpoint_key: str,
        checkpoint_field: str | None = None,
        preserve: str | None = None,
    ) -> FetchedPage:
        values: list[Any] = []
        for key in self._profile.record_keys:
            candidate = payload.get(key)
            if isinstance(candidate, list):
                values = candidate
                break
        records = tuple(SourceRecord(native_type=self._profile.native_type, payload=item) for item in values if isinstance(item, dict))
        next_page = payload.get("nextPageToken")
        checkpoint_value = payload.get(checkpoint_field) if checkpoint_field else None
        if not checkpoint_value:
            checkpoint_value = preserve or next_page
        if not checkpoint_value and records and isinstance(records[-1].payload, dict):
            for field in self._profile.identity_fields:
                candidate = records[-1].payload.get(field)
                if candidate not in (None, ""):
                    checkpoint_value = candidate
                    break
        checkpoint_value = checkpoint_value or "empty"
        return FetchedPage(
            records=records,
            next_cursor=(CursorState(schema_version=1, payload={page_key: str(next_page), **({checkpoint_key: preserve} if preserve else {})}) if next_page else None),
            checkpoint=CursorState(schema_version=1, payload={checkpoint_key: str(checkpoint_value)}),
            end_of_data=not bool(next_page),
        )

    async def reconcile(self, request: ReconciliationRequest, context: OperationContext) -> ReconciliationDecision:
        repairs = tuple(RepairShard(shard=item.shard, parent_shard_id=item.shard_id) for item in request.shards if item.state not in {"completed", "reconciled", "succeeded"})
        return ReconciliationDecision(has_gaps=bool(repairs), reason_code="incomplete_shards" if repairs else "clean", new_shards=repairs)


def _cursor_value(cursor: CursorState | None, key: str) -> str | None:
    value = cursor.payload.get(key) if cursor is not None else None
    return str(value) if value not in (None, "") else None


class GoogleWatchSubscription:
    def __init__(self, binding: BindingContext, profile: SourceProfile) -> None:
        self._binding = binding
        self._profile = profile
        self._ingestion = GoogleWorkspaceIngestion(binding, profile)

    async def ensure(self, request: SubscriptionRequest, context: OperationContext) -> SubscriptionState:
        if self._profile.source == "gmail":
            data = await context.services.installation_store.read("google_watch")
            topic = data.values.get("topic_name") if data is not None else None
            if not isinstance(topic, str) or not topic:
                raise PayloadRejectedError("Gmail watch requires google_watch.topic_name")
            payload = await self._ingestion._send(
                context,
                method="POST",
                url="https://gmail.googleapis.com/gmail/v1/users/me/watch",
                query=(),
                body={"topicName": topic, "labelIds": list(request.event_types)},
            )
            return SubscriptionState(
                external_subscription_id=f"gmail:{self._binding.installation.id}",
                expires_at_epoch_seconds=int(payload.get("expiration", 0)) // 1000 or None,
                state=CursorState(schema_version=1, payload={"history_id": str(payload.get("historyId", ""))}),
            )
        channel_type = "web_hook"
        if self._profile.source == "google_calendar":
            url = "https://www.googleapis.com/calendar/v3/calendars/primary/events/watch"
        else:
            url = "https://www.googleapis.com/drive/v3/changes/watch"
        payload = await self._ingestion._send(
            context,
            method="POST",
            url=url,
            query=(),
            body={
                "id": request.endpoint_id,
                "type": channel_type,
                "address": request.callback_url,
                "token": request.verification_token,
            },
        )
        return SubscriptionState(
            external_subscription_id=str(payload.get("id") or request.endpoint_id),
            expires_at_epoch_seconds=int(payload["expiration"]) // 1000 if payload.get("expiration") else None,
            state=CursorState(schema_version=1, payload={"resource_id": str(payload.get("resourceId") or "")}),
        )

    async def renew(self, subscription: SubscriptionState, context: OperationContext) -> SubscriptionState:
        allocation = await context.services.subscription_callbacks.allocate(f"{self._profile.source}.watch")
        return await self.ensure(
            SubscriptionRequest(callback_url=allocation.callback_url, endpoint_id=allocation.endpoint_id, event_types=()),
            context,
        )

    async def revoke(self, subscription: SubscriptionState, context: OperationContext) -> None:
        if self._profile.source == "gmail":
            await self._ingestion._send(context, method="POST", url="https://gmail.googleapis.com/gmail/v1/users/me/stop", query=(), body={})
            return
        resource = subscription.state.payload.get("resource_id") if subscription.state else None
        await self._ingestion._send(
            context,
            method="POST",
            url="https://www.googleapis.com/drive/v3/channels/stop",
            query=(),
            body={"id": subscription.external_subscription_id, "resourceId": resource},
        )


def _build(profile: SourceProfile) -> NativeSourceConnector:
    return NativeSourceConnector(
        _manifest(profile.source),
        {
            CONFIGURATION_V1.ref: lambda _context: FleetConfiguration(profile),
            SECRET_ROTATION_V1.ref: lambda _context: FleetSecretRotation(profile),
            HEALTH_PROBE_V1.ref: lambda context: CredentialHealthProbe(context, profile.secret_slots),
            CLEANUP_V1.ref: lambda _context: LocalCredentialCleanup(),
            HISTORICAL_PULL_V1.ref: lambda context: GoogleWorkspaceIngestion(context, profile),
            INCREMENTAL_POLL_V1.ref: lambda context: GoogleWorkspaceIngestion(context, profile),
            PUSH_SUBSCRIPTION_V1.ref: lambda context: GoogleWatchSubscription(context, profile),
            RECONCILIATION_V1.ref: lambda context: GoogleWorkspaceIngestion(context, profile),
            IDENTITY_V1.ref: lambda _context: NativeIdentity(lambda value: _identity(profile, value)),
            NORMALIZATION_V1.ref: lambda context: FleetNormalization(context, profile),
        },
    )


def build_gmail_connector() -> NativeSourceConnector:
    return _build(GMAIL)


def build_google_calendar_connector() -> NativeSourceConnector:
    return _build(CALENDAR)


def build_google_drive_connector() -> NativeSourceConnector:
    return _build(DRIVE)


__all__ = ["build_gmail_connector", "build_google_calendar_connector", "build_google_drive_connector"]
