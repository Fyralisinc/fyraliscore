"""Dedicated REST/OAuth-family connector roots.

Each factory names one provider explicitly. Shared HTTP mechanics live in the
connector-local kit, while endpoint, pagination, authentication, identity,
webhook, and semantic choices remain owned here rather than a fleet registry.
"""

from __future__ import annotations

from services.ingest.connectors.fleet import build_http_connector
from services.ingest.connectors.provider_spec import SourceProfile
from services.ingest.connectors.standard_oauth import OAuthProviderSpec
from services.ingest.connectors.native import NativeSourceConnector


def _spec(
    source: str,
    origin: str,
    path: str,
    channel: str,
    native_type: str,
    record_keys: tuple[str, ...],
    identity_fields: tuple[str, ...],
    occurred_fields: tuple[str, ...],
    text_fields: tuple[str, ...],
    auth_slot: str,
    *,
    ingress: tuple[str, ...] = ("backfill", "poll"),
    auth_scheme: str = "Bearer",
    webhook_header: str | None = None,
    webhook_slot: str | None = None,
    webhook_mode: str | None = None,
    trust: str = "attested_agent",
    cursor: str = "cursor",
    limit: str = "limit",
    next_fields: tuple[str, ...] = (
        "next_cursor",
        "nextCursor",
        "next_page_token",
        "nextPageToken",
        "continuation_token",
        "paging.next",
        "page.next",
    ),
) -> SourceProfile:
    return SourceProfile(
        source=source,
        ingress_kinds=ingress,
        api_origin=origin,
        collection_path=path,
        channel=channel,
        native_type=native_type,
        record_keys=record_keys,
        identity_fields=identity_fields,
        occurred_fields=occurred_fields,
        text_fields=text_fields,
        auth_slot=auth_slot,
        auth_scheme=auth_scheme,
        webhook_mode=webhook_mode,  # type: ignore[arg-type]
        webhook_header=webhook_header,
        webhook_secret_slot=webhook_slot,
        trust_tier=trust,
        cursor_parameter=cursor,
        limit_parameter=limit,
        next_cursor_fields=next_fields,
    )


GITHUB = _spec(
    "github", "https://api.github.com", "/events", "github:webhook", "event",
    ("items", "events"), ("id", "node_id", "delivery_id"),
    ("created_at", "updated_at", "timestamp"), ("title", "body", "action"),
    "oauth_access_token", ingress=("backfill", "webhook"),
    webhook_header="x-hub-signature-256", webhook_slot="webhook_signing_secret",
    webhook_mode="hmac_sha256", trust="authoritative", cursor="page", limit="per_page",
    next_fields=("next",),
)
JIRA = _spec(
    "jira", "https://api.atlassian.com", "/rest/api/3/search", "jira:issue", "issue",
    ("issues",), ("id", "key"), ("fields.updated",),
    ("fields.summary", "fields.description", "webhookEvent"), "oauth_access_token",
    ingress=("backfill", "poll", "webhook"), webhook_header="x-hub-signature",
    webhook_slot="webhook_signing_secret", webhook_mode="hmac_sha256",
    trust="authoritative", cursor="startAt", limit="maxResults",
    next_fields=("nextPageToken",),
)
MERCURY = _spec(
    "mercury", "https://api.mercury.com", "/api/v1/transactions",
    "mercury:transaction", "transaction", ("transactions",), ("id", "transactionId"),
    ("createdAt", "updatedAt"), ("note", "counterpartyName", "status"), "api_token",
    ingress=("backfill", "poll", "webhook"), webhook_header="x-fyralis-signature",
    webhook_slot="webhook_signing_secret", webhook_mode="hmac_sha256",
    trust="authoritative",
)
QUICKBOOKS = _spec(
    "quickbooks", "https://quickbooks.api.intuit.com", "/v3/company/query",
    "quickbooks:object", "object", ("QueryResponse", "entities"), ("Id", "id"),
    ("MetaData.LastUpdatedTime",), ("DisplayName", "DocNumber", "Name"),
    "oauth_access_token", ingress=("backfill", "poll", "webhook"),
    webhook_header="intuit-signature", webhook_slot="webhook_signing_secret",
    webhook_mode="hmac_sha256", trust="authoritative", cursor="startposition",
    limit="maxresults",
)
QUICKBOOKS_OAUTH = OAuthProviderSpec(
    source="quickbooks",
    authorize_url="https://appcenter.intuit.com/connect/oauth2",
    token_url="https://oauth.platform.intuit.com/oauth2/v1/tokens/bearer",
    scopes=("com.intuit.quickbooks.accounting",),
    token_auth="basic",
    external_id_paths=("realmId",),
)
GRAFANA = _spec(
    "grafana", "https://grafana.com", "/api/annotations", "grafana:annotation",
    "annotation", ("annotations", "items"), ("id", "alertId"),
    ("timeEnd", "time", "startsAt"), ("text", "title", "message"),
    "service_account_token", ingress=("backfill", "poll", "webhook"),
    webhook_header="x-grafana-alerting-signature", webhook_slot="webhook_signing_secret",
    webhook_mode="hmac_sha256", trust="authoritative",
)
BREX = _spec(
    "brex", "https://platform.brexapis.com", "/v2/transactions/card/primary",
    "brex:transaction", "transaction", ("items",), ("id",),
    ("posted_at_date", "created_at"), ("description", "merchant_name", "status"),
    "access_token", ingress=("backfill", "poll", "webhook"),
    webhook_header="x-fyralis-signature", webhook_slot="webhook_signing_secret",
    webhook_mode="hmac_sha256", trust="authoritative",
)
RAMP = _spec(
    "ramp", "https://api.ramp.com", "/developer/v1/transactions", "ramp:transaction",
    "transaction", ("data",), ("id",), ("updated_at", "created_at"),
    ("merchant_name", "memo", "state"), "oauth_access_token",
    ingress=("backfill", "poll", "webhook"), webhook_header="x-fyralis-signature",
    webhook_slot="webhook_signing_secret", webhook_mode="hmac_sha256",
    trust="authoritative", cursor="page", limit="page_size",
)
GUSTO = _spec(
    "gusto", "https://api.gusto.com", "/v1/companies", "gusto:object", "object",
    ("data", "companies"), ("uuid", "id"), ("updated_at", "created_at"),
    ("name", "title", "status"), "oauth_access_token",
    ingress=("backfill", "poll", "webhook"), webhook_header="x-fyralis-signature",
    webhook_slot="webhook_signing_secret", webhook_mode="hmac_sha256",
    trust="authoritative", cursor="page", limit="per",
)
DEEL = _spec(
    "deel", "https://api.letsdeel.com", "/rest/v2/contracts", "deel:payment", "payment",
    ("data",), ("id", "contract_id"), ("updated_at", "created_at"),
    ("title", "name", "status"), "api_token", ingress=("backfill", "poll", "webhook"),
    webhook_header="x-fyralis-signature", webhook_slot="webhook_signing_secret",
    webhook_mode="hmac_sha256", trust="authoritative",
)
FIREFLIES = _spec(
    "fireflies", "https://api.fireflies.ai", "/graphql", "fireflies:transcript",
    "transcript", ("data", "transcripts"), ("id", "transcript_id"),
    ("date", "created_at", "updated_at"), ("title", "summary", "sentence"), "api_key",
    ingress=("backfill", "poll", "webhook"), webhook_header="x-fyralis-signature",
    webhook_slot="webhook_signing_secret", webhook_mode="hmac_sha256",
)
MIRO = _spec(
    "miro", "https://api.miro.com", "/v2/boards", "miro:item", "item", ("data",),
    ("id",), ("modifiedAt", "createdAt"), ("title", "content", "type"),
    "oauth_access_token", ingress=("backfill", "poll", "webhook"),
    webhook_header="x-fyralis-signature", webhook_slot="webhook_signing_secret",
    webhook_mode="hmac_sha256", cursor="cursor", limit="limit",
)
FIGMA = _spec(
    "figma", "https://api.figma.com", "/v1/me", "figma:event", "event",
    ("events", "versions", "items"), ("id", "key"), ("created_at", "timestamp"),
    ("name", "message", "description"), "access_token",
    ingress=("backfill", "poll", "webhook"), webhook_header="x-figma-passcode",
    webhook_slot="webhook_passcode", webhook_mode="token",
)
CARTA = _spec(
    "carta", "https://api.carta.com", "/v1/companies", "carta:object", "object",
    ("data",), ("id", "uuid"), ("updated_at", "created_at"),
    ("name", "description", "status"), "oauth_access_token", trust="authoritative",
)
HIBOB = _spec(
    "hibob", "https://api.hibob.com", "/v1/people", "hibob:object", "object",
    ("employees",), ("id", "email"), ("updatedAt", "createdAt"),
    ("displayName", "title", "status"), "service_token", auth_scheme="Basic",
    ingress=("backfill", "poll", "webhook"), webhook_header="x-fyralis-signature",
    webhook_slot="webhook_signing_secret", webhook_mode="hmac_sha256",
    trust="authoritative",
)
ASHBY = _spec(
    "ashby", "https://api.ashbyhq.com", "/candidate.list", "ashby:object", "object",
    ("results",), ("id",), ("updatedAt", "createdAt"), ("name", "title", "status"),
    "api_key", auth_scheme="Basic", ingress=("backfill", "poll", "webhook"),
    webhook_header="x-fyralis-signature", webhook_slot="webhook_signing_secret",
    webhook_mode="hmac_sha256", trust="authoritative", cursor="cursor", limit="limit",
    next_fields=("moreDataAvailable", "syncToken"),
)
LINKEDIN = _spec(
    "linkedin", "https://api.linkedin.com", "/v2/organizationalEntityShareStatistics",
    "linkedin:object", "object", ("elements",), ("id", "organizationalEntity"),
    ("lastModified", "created"), ("commentary", "name", "description"),
    "oauth_access_token", cursor="start", limit="count",
)


def build_github_connector() -> NativeSourceConnector: return build_http_connector(GITHUB)
def build_jira_connector() -> NativeSourceConnector: return build_http_connector(JIRA)
def build_mercury_connector() -> NativeSourceConnector: return build_http_connector(MERCURY)
def build_quickbooks_connector() -> NativeSourceConnector:
    return build_http_connector(QUICKBOOKS, oauth_spec=QUICKBOOKS_OAUTH)
def build_grafana_connector() -> NativeSourceConnector: return build_http_connector(GRAFANA)
def build_brex_connector() -> NativeSourceConnector: return build_http_connector(BREX)
def build_ramp_connector() -> NativeSourceConnector: return build_http_connector(RAMP)
def build_gusto_connector() -> NativeSourceConnector: return build_http_connector(GUSTO)
def build_deel_connector() -> NativeSourceConnector: return build_http_connector(DEEL)
def build_fireflies_connector() -> NativeSourceConnector: return build_http_connector(FIREFLIES)
def build_miro_connector() -> NativeSourceConnector: return build_http_connector(MIRO)
def build_figma_connector() -> NativeSourceConnector: return build_http_connector(FIGMA)
def build_carta_connector() -> NativeSourceConnector: return build_http_connector(CARTA)
def build_hibob_connector() -> NativeSourceConnector: return build_http_connector(HIBOB)
def build_ashby_connector() -> NativeSourceConnector: return build_http_connector(ASHBY)
def build_linkedin_connector() -> NativeSourceConnector: return build_http_connector(LINKEDIN)


__all__ = [name for name in globals() if name.startswith("build_")]
