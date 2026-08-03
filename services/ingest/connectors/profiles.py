"""Declarative provider wire profiles for the native connector fleet.

The profiles contain provider data only.  Execution lives in ``fleet.py`` and
is restricted to the source-contract host ports, which keeps the remaining
first-party connectors independent from the retired planner/fetcher/handler
dispatch implementation.
"""

from __future__ import annotations

from dataclasses import dataclass
from types import MappingProxyType
from typing import Literal

WebhookMode = Literal["hmac_sha256", "token", "ed25519"]


@dataclass(frozen=True)
class SourceProfile:
    source: str
    ingress_kinds: tuple[str, ...]
    api_origin: str
    collection_path: str
    channel: str
    native_type: str
    record_keys: tuple[str, ...]
    identity_fields: tuple[str, ...]
    occurred_fields: tuple[str, ...]
    text_fields: tuple[str, ...]
    auth_slot: str
    auth_scheme: str = "Bearer"
    webhook_mode: WebhookMode | None = None
    webhook_header: str | None = None
    webhook_secret_slot: str | None = None
    trust_tier: str = "attested_agent"

    @property
    def outbound_host(self) -> str:
        return self.api_origin.removeprefix("https://").split("/", 1)[0]

    @property
    def secret_slots(self) -> tuple[str, ...]:
        values = [self.auth_slot]
        if self.webhook_secret_slot is not None:
            values.append(self.webhook_secret_slot)
        return tuple(dict.fromkeys(values))


def _profile(
    source: str,
    ingress: tuple[str, ...],
    host: str,
    path: str,
    channel: str,
    native_type: str,
    records: tuple[str, ...],
    ids: tuple[str, ...],
    times: tuple[str, ...],
    text: tuple[str, ...],
    auth_slot: str,
    *,
    auth_scheme: str = "Bearer",
    webhook: WebhookMode | None = None,
    webhook_header: str | None = None,
    webhook_secret: str | None = None,
    trust: str = "attested_agent",
) -> SourceProfile:
    return SourceProfile(
        source=source,
        ingress_kinds=ingress,
        api_origin=f"https://{host}",
        collection_path=path,
        channel=channel,
        native_type=native_type,
        record_keys=records,
        identity_fields=ids,
        occurred_fields=times,
        text_fields=text,
        auth_slot=auth_slot,
        auth_scheme=auth_scheme,
        webhook_mode=webhook,
        webhook_header=webhook_header,
        webhook_secret_slot=webhook_secret,
        trust_tier=trust,
    )


_HMAC = {
    "webhook": "hmac_sha256",
    "webhook_header": "x-fyralis-signature",
    "webhook_secret": "webhook_signing_secret",
}


FLEET_PROFILES = MappingProxyType(
    {
        item.source: item
        for item in (
            _profile(
                "github",
                ("backfill", "webhook"),
                "api.github.com",
                "/events",
                "github:webhook",
                "event",
                ("items", "events"),
                ("id", "node_id", "delivery_id"),
                ("created_at", "updated_at", "timestamp"),
                ("title", "body", "action"),
                "oauth_access_token",
                webhook="hmac_sha256",
                webhook_header="x-hub-signature-256",
                webhook_secret="webhook_signing_secret",
                trust="authoritative",
            ),
            _profile(
                "discord",
                ("backfill", "gateway", "webhook"),
                "discord.com",
                "/api/v10/users/@me/guilds",
                "discord:message",
                "message",
                ("items", "messages"),
                ("id", "message_id"),
                ("timestamp", "edited_timestamp"),
                ("content", "name"),
                "bot_token",
                auth_scheme="Bot",
                webhook="ed25519",
                webhook_header="x-signature-ed25519",
                webhook_secret="webhook_public_key",
            ),
            _profile(
                "gmail",
                ("backfill", "poll"),
                "gmail.googleapis.com",
                "/gmail/v1/users/me/messages",
                "gmail:",
                "message",
                ("messages",),
                ("id", "threadId"),
                ("internalDate", "historyId"),
                ("snippet", "subject"),
                "oauth_access_token",
            ),
            _profile(
                "google_calendar",
                ("backfill", "poll"),
                "www.googleapis.com",
                "/calendar/v3/calendars/primary/events",
                "google_calendar:event",
                "event",
                ("items",),
                ("id", "iCalUID"),
                ("updated", "created", "start.dateTime"),
                ("summary", "description", "location"),
                "oauth_access_token",
                trust="authoritative",
            ),
            _profile(
                "google_drive",
                ("backfill", "poll"),
                "www.googleapis.com",
                "/drive/v3/files",
                "google_drive:file",
                "file",
                ("files", "items"),
                ("id", "name"),
                ("modifiedTime", "createdTime"),
                ("name", "description", "webViewLink"),
                "oauth_access_token",
                trust="authoritative",
            ),
            _profile(
                "jira",
                ("backfill", "poll", "webhook"),
                "api.atlassian.com",
                "/rest/api/3/search",
                "jira:issue",
                "issue",
                ("issues", "items"),
                ("id", "key"),
                ("fields.updated", "timestamp"),
                ("fields.summary", "fields.description", "webhookEvent"),
                "oauth_access_token",
                webhook="hmac_sha256",
                webhook_header="x-hub-signature",
                webhook_secret="webhook_signing_secret",
                trust="authoritative",
            ),
            _profile(
                "mercury",
                ("backfill", "poll", "webhook"),
                "api.mercury.com",
                "/api/v1/transactions",
                "mercury:transaction",
                "transaction",
                ("transactions", "items"),
                ("id", "transactionId"),
                ("createdAt", "updatedAt"),
                ("note", "counterpartyName", "status"),
                "api_token",
                **_HMAC,
                trust="authoritative",
            ),
            _profile(
                "quickbooks",
                ("backfill", "poll", "webhook"),
                "quickbooks.api.intuit.com",
                "/v3/company/query",
                "quickbooks:object",
                "object",
                ("QueryResponse", "items", "entities"),
                ("Id", "id"),
                ("MetaData.LastUpdatedTime", "time"),
                ("DisplayName", "DocNumber", "Name"),
                "oauth_access_token",
                webhook="hmac_sha256",
                webhook_header="intuit-signature",
                webhook_secret="webhook_signing_secret",
                trust="authoritative",
            ),
            _profile(
                "grafana",
                ("backfill", "poll", "webhook"),
                "grafana.com",
                "/api/annotations",
                "grafana:annotation",
                "annotation",
                ("items", "annotations"),
                ("id", "alertId"),
                ("timeEnd", "time", "startsAt"),
                ("text", "title", "message"),
                "service_account_token",
                webhook="hmac_sha256",
                webhook_header="x-grafana-alerting-signature",
                webhook_secret="webhook_signing_secret",
                trust="authoritative",
            ),
            _profile(
                "telegram",
                ("backfill", "gateway"),
                "api.telegram.org",
                "/bot/getUpdates",
                "telegram:message",
                "message",
                ("result", "items"),
                ("message_id", "update_id", "id"),
                ("date", "edit_date"),
                ("text", "caption"),
                "bot_token",
            ),
            _profile(
                "brex",
                ("backfill", "poll", "webhook"),
                "platform.brexapis.com",
                "/v2/transactions/card/primary",
                "brex:transaction",
                "transaction",
                ("items", "transactions"),
                ("id",),
                ("posted_at_date", "created_at"),
                ("description", "merchant_name", "status"),
                "access_token",
                **_HMAC,
                trust="authoritative",
            ),
            _profile(
                "ramp",
                ("backfill", "poll", "webhook"),
                "api.ramp.com",
                "/developer/v1/transactions",
                "ramp:transaction",
                "transaction",
                ("data", "items", "transactions"),
                ("id",),
                ("updated_at", "created_at"),
                ("merchant_name", "memo", "state"),
                "oauth_access_token",
                **_HMAC,
                trust="authoritative",
            ),
            _profile(
                "gusto",
                ("backfill", "poll", "webhook"),
                "api.gusto.com",
                "/v1/companies",
                "gusto:object",
                "object",
                ("items", "data", "companies"),
                ("uuid", "id"),
                ("updated_at", "created_at"),
                ("name", "title", "status"),
                "oauth_access_token",
                **_HMAC,
                trust="authoritative",
            ),
            _profile(
                "deel",
                ("backfill", "poll", "webhook"),
                "api.letsdeel.com",
                "/rest/v2/contracts",
                "deel:payment",
                "payment",
                ("data", "items"),
                ("id", "contract_id"),
                ("updated_at", "created_at"),
                ("title", "name", "status"),
                "api_token",
                **_HMAC,
                trust="authoritative",
            ),
            _profile(
                "fireflies",
                ("backfill", "poll", "webhook"),
                "api.fireflies.ai",
                "/graphql",
                "fireflies:transcript",
                "transcript",
                ("transcripts", "data", "items"),
                ("id", "transcript_id"),
                ("date", "created_at", "updated_at"),
                ("title", "summary", "sentence"),
                "api_key",
                **_HMAC,
            ),
            _profile(
                "signal",
                ("backfill", "gateway"),
                "chat.signal.org",
                "/v1/messages",
                "signal:message",
                "message",
                ("messages", "items"),
                ("timestamp", "id"),
                ("timestamp",),
                ("message", "body", "text"),
                "linked_device_token",
            ),
            _profile(
                "aws",
                ("backfill", "poll"),
                "cloudtrail.amazonaws.com",
                "/",
                "aws:event",
                "event",
                ("Events", "items"),
                ("EventId", "id"),
                ("EventTime", "eventTime"),
                ("EventName", "Username", "CloudTrailEvent"),
                "session_token",
                auth_scheme="AWS4-HMAC-SHA256",
                trust="authoritative",
            ),
            _profile(
                "miro",
                ("backfill", "poll", "webhook"),
                "api.miro.com",
                "/v2/boards",
                "miro:item",
                "item",
                ("data", "items"),
                ("id",),
                ("modifiedAt", "createdAt"),
                ("title", "content", "type"),
                "oauth_access_token",
                **_HMAC,
            ),
            _profile(
                "figma",
                ("backfill", "poll", "webhook"),
                "api.figma.com",
                "/v1/me",
                "figma:event",
                "event",
                ("items", "events", "versions"),
                ("id", "key"),
                ("created_at", "timestamp"),
                ("name", "message", "description"),
                "access_token",
                webhook="token",
                webhook_header="x-figma-passcode",
                webhook_secret="webhook_passcode",
            ),
            _profile(
                "carta",
                ("backfill", "poll"),
                "api.carta.com",
                "/v1/companies",
                "carta:object",
                "object",
                ("data", "items"),
                ("id", "uuid"),
                ("updated_at", "created_at"),
                ("name", "description", "status"),
                "oauth_access_token",
                trust="authoritative",
            ),
            _profile(
                "hibob",
                ("backfill", "poll", "webhook"),
                "api.hibob.com",
                "/v1/people",
                "hibob:object",
                "object",
                ("employees", "items"),
                ("id", "email"),
                ("updatedAt", "createdAt"),
                ("displayName", "title", "status"),
                "service_token",
                **_HMAC,
                trust="authoritative",
            ),
            _profile(
                "ashby",
                ("backfill", "poll", "webhook"),
                "api.ashbyhq.com",
                "/candidate.list",
                "ashby:object",
                "object",
                ("results", "items"),
                ("id",),
                ("updatedAt", "createdAt"),
                ("name", "title", "status"),
                "api_key",
                **_HMAC,
                trust="authoritative",
            ),
            _profile(
                "linkedin",
                ("backfill", "poll"),
                "api.linkedin.com",
                "/v2/organizationalEntityShareStatistics",
                "linkedin:object",
                "object",
                ("elements", "items"),
                ("id", "organizationalEntity"),
                ("lastModified", "created"),
                ("commentary", "name", "description"),
                "oauth_access_token",
            ),
        )
    }
)


def require_profile(source: str) -> SourceProfile:
    try:
        return FLEET_PROFILES[source]
    except KeyError as exc:
        raise ValueError(f"no native fleet profile is declared for {source!r}") from exc


__all__ = ["FLEET_PROFILES", "SourceProfile", "WebhookMode", "require_profile"]
