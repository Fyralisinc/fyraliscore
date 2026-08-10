"""Self-contained Notion connector capabilities."""

from __future__ import annotations

import json
import hashlib
import hmac
import math
import os
from datetime import datetime, timezone
from typing import Any
from urllib.parse import quote

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
    EvidenceAccessPolicy,
    FetchRequest,
    FetchedPage,
    IdentityInput,
    NormalizationInput,
    ObservationDraft,
    PlanRequest,
    PlanResult,
    PollRequest,
    ReconciliationDecision,
    ReconciliationRequest,
    RepairShard,
    ShardPlan,
    SourceRecord,
    SourceObjectRef,
    VerifiedWebhookEvent,
    VerifiedWebhookResult,
)


_DATABASE_SHARD = "notion_database"
_PAGE_TREE_SHARD = "notion_page_tree"
_NOTION_VERSION = "2022-06-28"


def _payload(record: SourceRecord) -> dict[str, Any]:
    if not isinstance(record.payload, dict):
        raise PayloadRejectedError("Notion requires a JSON object payload")
    return record.payload


def notion_external_id(input: IdentityInput) -> str:
    payload = _payload(input.record)
    object_type = payload.get("object")
    object_id = payload.get("id")
    if not isinstance(object_type, str) or not isinstance(object_id, str):
        raise PayloadRejectedError("Notion identity requires object and id")
    return f"notion:{object_type}:{object_id}"


def _parse_time(value: Any, fallback: datetime) -> datetime:
    if not isinstance(value, str):
        return fallback
    candidate = value[:-1] + "+00:00" if value.endswith("Z") else value
    try:
        parsed = datetime.fromisoformat(candidate)
    except ValueError:
        return fallback
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _plain_text(value: Any) -> str:
    if not isinstance(value, list):
        return ""
    return "".join(
        str(item.get("plain_text"))
        for item in value
        if isinstance(item, dict) and isinstance(item.get("plain_text"), str)
    )


def _title(properties: Any) -> str:
    if isinstance(properties, dict):
        for value in properties.values():
            if isinstance(value, dict) and value.get("type") == "title":
                title = _plain_text(value.get("title"))
                if title:
                    return title
    return "(untitled)"


def _actor(value: dict[str, Any], key: str) -> str | None:
    actor = value.get(key)
    identifier = actor.get("id") if isinstance(actor, dict) else None
    return f"notion:{identifier}" if isinstance(identifier, str) else None


def _mentions(value: Any) -> tuple[dict[str, Any], ...]:
    result: list[dict[str, Any]] = []
    for item in value if isinstance(value, list) else ():
        if not isinstance(item, dict) or item.get("type") != "mention":
            continue
        mention = item.get("mention")
        if not isinstance(mention, dict):
            continue
        mention_type = mention.get("type")
        nested = mention.get(mention_type) if isinstance(mention_type, str) else None
        identifier = nested.get("id") if isinstance(nested, dict) else None
        if isinstance(identifier, str) and mention_type in {"user", "page"}:
            result.append(
                {
                    "type": f"notion_{mention_type}",
                    "id": identifier,
                }
            )
    return tuple(result)


class NotionNormalization:
    async def normalize(
        self, request: NormalizationInput, context: OperationContext
    ) -> tuple[ObservationDraft, ...]:
        value = _payload(request.record)
        object_type = value.get("object")
        now = context.services.clock.now()
        if object_type == "page":
            return (self._page(value, now),)
        if object_type == "block":
            return (self._block(value, now),)
        if object_type == "comment":
            return (self._comment(value, now),)
        raise PayloadRejectedError(
            "Notion object type is unsupported",
            details={"object_type": str(object_type)},
        )

    @staticmethod
    def _page(value: dict[str, Any], now: datetime) -> ObservationDraft:
        identifier = value.get("id")
        if not isinstance(identifier, str):
            raise PayloadRejectedError("Notion page is missing id")
        properties = value.get("properties") or {}
        parent = value.get("parent") or {}
        in_database = isinstance(parent, dict) and parent.get("type") == "database_id"
        database_id = parent.get("database_id") if in_database else None
        title = _title(properties)
        created_at = _parse_time(value.get("created_time"), now)
        recorded_at = _parse_time(
            value.get("last_edited_time") or value.get("created_time"), now
        )
        deleted = bool(value.get("archived") or value.get("in_trash"))
        operation = "delete" if deleted else (
            "update" if recorded_at != created_at else "create"
        )
        has_status = isinstance(properties, dict) and any(
            isinstance(item, dict) and item.get("type") in {"status", "select"}
            for item in properties.values()
        )
        entities: list[dict[str, Any]] = [{"type": "notion_page", "id": identifier}]
        if isinstance(database_id, str):
            entities.append({"type": "notion_database", "id": database_id})
        if isinstance(properties, dict):
            for name, prop in properties.items():
                if not isinstance(prop, dict) or prop.get("type") != "relation":
                    continue
                for relation in prop.get("relation") or ():
                    related = relation.get("id") if isinstance(relation, dict) else None
                    if isinstance(related, str):
                        entities.append(
                            {"type": "notion_page", "id": related, "relation": name}
                        )
        return ObservationDraft(
            source_channel="notion:object",
            content_text=(
                f"Notion page '{title}' in database {database_id}"
                if in_database
                else f"Notion page '{title}' in workspace"
            ),
            content={
                "object_type": "page",
                "page_id": identifier,
                "title": title,
                "in_database": in_database,
                "database_id": database_id,
                "url": value.get("url"),
                "properties": properties,
                "workspace_id": value.get("_fyralis_workspace_id"),
            },
            occurred_at=recorded_at,
            trust_tier="attested_agent",
            kind="state_change" if in_database and has_status else "signal",
            source_actor_ref=_actor(value, "last_edited_by"),
            external_id=f"notion:page:{identifier}",
            entities_hint=tuple(entities),
            raw_payload=value,
            source_object=SourceObjectRef(
                object_type="page",
                object_id=identifier,
                revision_id=str(
                    value.get("last_edited_time")
                    or value.get("created_time")
                    or recorded_at.isoformat()
                ),
                operation=operation,
                source_recorded_at=recorded_at,
                valid_from=created_at,
                parent_object_type=(
                    str(parent.get("type")) if isinstance(parent, dict) else None
                ),
                parent_object_id=(
                    str(parent.get(parent.get("type")))
                    if isinstance(parent, dict) and parent.get(parent.get("type"))
                    else None
                ),
                container_object_type="database" if in_database else "workspace",
                container_object_id=(
                    str(database_id)
                    if database_id is not None
                    else str(value.get("_fyralis_workspace_id") or "workspace")
                ),
                access_policy=EvidenceAccessPolicy(
                    visibility="unknown",
                    audience=(),
                    source_acl_version="not-captured",
                    resource_ref={"type": "notion_page", "id": identifier},
                    captured_at=recorded_at,
                ),
            ),
        )

    @staticmethod
    def _block(value: dict[str, Any], now: datetime) -> ObservationDraft:
        identifier = value.get("id")
        if not isinstance(identifier, str):
            raise PayloadRejectedError("Notion block is missing id")
        block_type = value.get("type") or "unknown"
        body = value.get(block_type) if isinstance(value.get(block_type), dict) else {}
        text = _plain_text(body.get("rich_text"))
        parent = value.get("parent") if isinstance(value.get("parent"), dict) else {}
        created_at = _parse_time(value.get("created_time"), now)
        recorded_at = _parse_time(
            value.get("last_edited_time") or value.get("created_time"), now
        )
        deleted = bool(value.get("archived") or value.get("in_trash"))
        operation = "delete" if deleted else (
            "update" if recorded_at != created_at else "create"
        )
        content: dict[str, Any] = {
            "object_type": "block",
            "block_id": identifier,
            "block_type": block_type,
            "text": text,
            "workspace_id": value.get("_fyralis_workspace_id"),
        }
        if value.get("_fyralis_truncated"):
            content["_truncated"] = value["_fyralis_truncated"]
        return ObservationDraft(
            source_channel="notion:object",
            content_text=(
                f"Notion {block_type}: {text[:200]}"
                if text
                else f"Notion {block_type} block"
            ),
            content=content,
            occurred_at=recorded_at,
            trust_tier="attested_agent",
            kind="signal",
            source_actor_ref=_actor(value, "last_edited_by"),
            external_id=f"notion:block:{identifier}",
            entities_hint=_mentions(body.get("rich_text")),
            raw_payload=value,
            source_object=SourceObjectRef(
                object_type="block",
                object_id=identifier,
                revision_id=str(
                    value.get("last_edited_time")
                    or value.get("created_time")
                    or recorded_at.isoformat()
                ),
                operation=operation,
                source_recorded_at=recorded_at,
                valid_from=created_at,
                parent_object_type=(
                    str(parent.get("type")) if parent.get("type") else None
                ),
                parent_object_id=(
                    str(parent.get(parent.get("type")))
                    if parent.get("type") and parent.get(parent.get("type"))
                    else None
                ),
                access_policy=EvidenceAccessPolicy(
                    visibility="unknown",
                    audience=(),
                    source_acl_version="not-captured",
                    resource_ref={"type": "notion_block", "id": identifier},
                    captured_at=recorded_at,
                ),
            ),
        )

    @staticmethod
    def _comment(value: dict[str, Any], now: datetime) -> ObservationDraft:
        identifier = value.get("id")
        if not isinstance(identifier, str):
            raise PayloadRejectedError("Notion comment is missing id")
        text = _plain_text(value.get("rich_text"))
        parent = value.get("parent") or {}
        parent_id = (
            parent.get("page_id") or parent.get("block_id")
            if isinstance(parent, dict)
            else None
        )
        created_at = _parse_time(value.get("created_time"), now)
        recorded_at = _parse_time(
            value.get("last_edited_time") or value.get("created_time"), now
        )
        entities = list(_mentions(value.get("rich_text")))
        if isinstance(parent_id, str):
            entities.append({"type": "notion_page", "id": parent_id})
        return ObservationDraft(
            source_channel="notion:object",
            content_text=f"Notion comment: {text[:200]}" if text else "Notion comment",
            content={
                "object_type": "comment",
                "comment_id": identifier,
                "text": text,
                "parent_id": parent_id,
                "discussion_id": value.get("discussion_id"),
                "workspace_id": value.get("_fyralis_workspace_id"),
            },
            occurred_at=recorded_at,
            trust_tier="attested_agent",
            kind="signal",
            source_actor_ref=_actor(value, "created_by"),
            external_id=f"notion:comment:{identifier}",
            entities_hint=tuple(entities),
            raw_payload=value,
            source_object=SourceObjectRef(
                object_type="comment",
                object_id=identifier,
                revision_id=str(
                    value.get("last_edited_time")
                    or value.get("created_time")
                    or recorded_at.isoformat()
                ),
                operation="update" if recorded_at != created_at else "create",
                source_recorded_at=recorded_at,
                valid_from=created_at,
                parent_object_type=(
                    str(parent.get("type")) if isinstance(parent, dict) else None
                ),
                parent_object_id=str(parent_id) if parent_id is not None else None,
                access_policy=EvidenceAccessPolicy(
                    visibility="unknown",
                    audience=(),
                    source_acl_version="not-captured",
                    resource_ref={"type": "notion_comment", "id": identifier},
                    captured_at=recorded_at,
                ),
            ),
        )


class NotionIngestion:
    def __init__(self, binding: BindingContext) -> None:
        self._binding = binding

    async def _external_installation_id(self) -> str:
        data = await self._binding.services.installation_store.read("provider")
        value = (data.values if data is not None else {}).get(
            "external_installation_id"
        )
        return str(value or self._binding.installation.id)

    async def _call(
        self,
        context: OperationContext,
        method: str,
        path: str,
        *,
        query: dict[str, str] | None = None,
        body: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        token = await self._binding.services.secrets.resolve(
            SlotId("oauth_access_token")
        )
        response = await context.services.http.send(
            GovernedHttpRequest(
                method=method,
                url=f"https://api.notion.com/v1/{path.lstrip('/')}",
                headers=(
                    ("authorization", f"Bearer {token.reveal_text()}"),
                    ("notion-version", _NOTION_VERSION),
                    ("content-type", "application/json"),
                ),
                query=tuple((query or {}).items()),
                body=json.dumps(body).encode() if body is not None else None,
            )
        )
        try:
            payload = json.loads(response.body)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise TransientSourceError("Notion returned malformed JSON") from exc
        if not isinstance(payload, dict):
            raise TransientSourceError("Notion returned an invalid response")
        if response.status_code == 429:
            raise RateLimitedError("Notion rate limit was reached")
        if response.status_code == 404:
            raise ResourceNotFoundError("Notion resource is no longer shared")
        if response.status_code in {401, 403}:
            raise AuthenticationRejectedError("Notion credential was rejected")
        if response.status_code >= 500:
            raise TransientSourceError("Notion is temporarily unavailable")
        if response.status_code >= 400:
            raise PayloadRejectedError(
                "Notion rejected the request",
                details={"code": str(payload.get("code") or response.status_code)},
            )
        return payload

    async def _search(
        self,
        context: OperationContext,
        object_filter: str,
        *,
        cursor: str | None = None,
        page_size: int = 100,
        sort: bool = False,
    ) -> dict[str, Any]:
        body: dict[str, Any] = {
            "filter": {"property": "object", "value": object_filter},
            "page_size": page_size,
        }
        if cursor:
            body["start_cursor"] = cursor
        if sort:
            body["sort"] = {
                "direction": "descending",
                "timestamp": "last_edited_time",
            }
        return await self._call(context, "POST", "search", body=body)

    async def plan(self, request: PlanRequest, context: OperationContext) -> PlanResult:
        workspace = await self._external_installation_id()
        selected = set(request.selected_resources)
        cursor: str | None = None
        shards: list[ShardPlan] = []
        while True:
            page = await self._search(context, "database", cursor=cursor)
            for database in page.get("results") or ():
                if not isinstance(database, dict) or not isinstance(
                    database.get("id"), str
                ):
                    continue
                identifier = database["id"]
                if selected and identifier not in selected:
                    continue
                shards.append(
                    ShardPlan(
                        kind=_DATABASE_SHARD,
                        identifier={
                            "shard_kind": _DATABASE_SHARD,
                            "database_id": identifier,
                            "workspace_id": workspace,
                        },
                        priority=self._recency(
                            database.get("last_edited_time"), context
                        ),
                    )
                )
            cursor = page.get("next_cursor")
            if not page.get("has_more") or not isinstance(cursor, str):
                break
        shards.append(
            ShardPlan(
                kind=_PAGE_TREE_SHARD,
                identifier={
                    "shard_kind": _PAGE_TREE_SHARD,
                    "workspace_id": workspace,
                },
            )
        )
        return PlanResult(shards=tuple(shards))

    @staticmethod
    def _recency(value: Any, context: OperationContext) -> float:
        parsed = _parse_time(value, context.services.clock.now())
        days = max(
            0.0,
            (context.services.clock.now() - parsed).total_seconds() / 86400.0,
        )
        return math.exp(-days / 7.0)

    async def fetch(
        self, request: FetchRequest, context: OperationContext
    ) -> FetchedPage:
        return await self._fetch_shard(request.shard, request.cursor, context)

    async def poll(
        self, request: PollRequest, context: OperationContext
    ) -> FetchedPage:
        workspace = await self._external_installation_id()
        return await self._fetch_shard(
            ShardPlan(
                kind=_PAGE_TREE_SHARD,
                identifier={
                    "shard_kind": _PAGE_TREE_SHARD,
                    "workspace_id": workspace,
                },
            ),
            request.cursor,
            context,
        )

    async def _fetch_shard(
        self,
        shard: ShardPlan,
        cursor: CursorState | None,
        context: OperationContext,
    ) -> FetchedPage:
        current = dict(cursor.payload) if cursor else {}
        stack = [
            dict(item) for item in current.get("stack") or () if isinstance(item, dict)
        ]
        if not current.get("seeded"):
            stack = [
                {"kind": "db_rows"}
                if shard.kind == _DATABASE_SHARD
                else {"kind": "loose_pages"}
            ]
        if not stack:
            return FetchedPage(end_of_data=True)
        item = stack.pop()
        workspace = shard.identifier.get("workspace_id")
        records: list[dict[str, Any]] = []
        try:
            page = await self._fetch_item(context, shard, item)
        except ResourceNotFoundError:
            page = {"results": [], "has_more": False, "next_cursor": None}
        for value in page.get("results") or ():
            if not isinstance(value, dict):
                continue
            if item["kind"] == "loose_pages" and self._is_database_row(value):
                continue
            value = {**value, "_fyralis_workspace_id": workspace}
            if item["kind"] == "page_blocks" and value.get("has_children"):
                depth = int(item.get("depth", 0))
                if depth + 1 < self._depth_cap():
                    if isinstance(value.get("id"), str):
                        stack.append(
                            {
                                "kind": "page_blocks",
                                "page_id": item.get("page_id"),
                                "block_id": value["id"],
                                "depth": depth + 1,
                            }
                        )
                else:
                    value["_fyralis_truncated"] = {
                        "reason": "depth_cap",
                        "depth": self._depth_cap(),
                    }
            records.append(value)
            if item["kind"] in {"db_rows", "loose_pages"} and isinstance(
                value.get("id"), str
            ):
                page_id = value["id"]
                stack.append({"kind": "page_comments", "page_id": page_id})
                stack.append(
                    {
                        "kind": "page_blocks",
                        "page_id": page_id,
                        "block_id": page_id,
                        "depth": 0,
                    }
                )
        if page.get("has_more") and isinstance(page.get("next_cursor"), str):
            stack.append({**item, "list_cursor": page["next_cursor"]})
        high_water = current.get("last_edited_at")
        for value in records:
            edited = value.get("last_edited_time") or value.get("created_time")
            if isinstance(edited, str) and (
                not isinstance(high_water, str) or edited > high_water
            ):
                high_water = edited
        end = not stack
        checkpoint = CursorState(
            schema_version=1,
            payload={
                "stack": stack,
                "items_seen": int(current.get("items_seen", 0)) + len(records),
                "last_edited_at": high_water,
                "seeded": True,
            },
        )
        return FetchedPage(
            records=tuple(
                SourceRecord(
                    native_type=str(value.get("object") or "record"), payload=value
                )
                for value in records
            ),
            next_cursor=None if end else checkpoint,
            checkpoint=checkpoint,
            end_of_data=end,
        )

    async def _fetch_item(
        self,
        context: OperationContext,
        shard: ShardPlan,
        item: dict[str, Any],
    ) -> dict[str, Any]:
        list_cursor = item.get("list_cursor")
        kind = item.get("kind")
        if kind == "db_rows":
            body = {"page_size": 100}
            if list_cursor:
                body["start_cursor"] = list_cursor
            database = quote(str(shard.identifier.get("database_id") or ""), safe="")
            return await self._call(
                context, "POST", f"databases/{database}/query", body=body
            )
        if kind == "loose_pages":
            return await self._search(context, "page", cursor=list_cursor)
        query = {"page_size": "100"}
        if list_cursor:
            query["start_cursor"] = str(list_cursor)
        if kind == "page_blocks":
            block = quote(str(item.get("block_id") or ""), safe="")
            return await self._call(
                context, "GET", f"blocks/{block}/children", query=query
            )
        if kind == "page_comments":
            query["block_id"] = str(item.get("page_id") or "")
            return await self._call(context, "GET", "comments", query=query)
        raise PayloadRejectedError("Notion cursor contains an unknown work item")

    @staticmethod
    def _is_database_row(value: dict[str, Any]) -> bool:
        parent = value.get("parent") or {}
        return isinstance(parent, dict) and parent.get("type") == "database_id"

    @staticmethod
    def _depth_cap() -> int:
        try:
            return max(1, int(os.environ.get("NOTION_BLOCK_DEPTH_CAP", "3")))
        except ValueError:
            return 3

    async def reconcile(
        self, request: ReconciliationRequest, context: OperationContext
    ) -> ReconciliationDecision:
        repairs: list[RepairShard] = []
        for summary in request.shards:
            if summary.state != "done" or summary.cursor is None:
                continue
            high_water = summary.cursor.payload.get("last_edited_at")
            if not isinstance(high_water, str):
                continue
            try:
                latest = await self._latest_edit(context, summary.shard)
            except ResourceNotFoundError:
                continue
            if latest is None or latest <= high_water:
                continue
            identifier = dict(summary.shard.identifier)
            identifier.update(
                {
                    "parent_shard_id": str(summary.shard_id),
                    "gap_baseline_edited_at": high_water,
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
            reason_code="newer_edits" if repairs else "clean",
            message=f"Notion reconciliation found {len(repairs)} gap(s)."
            if repairs
            else "",
            new_shards=tuple(repairs),
        )

    async def _latest_edit(
        self, context: OperationContext, shard: ShardPlan
    ) -> str | None:
        if shard.kind == _DATABASE_SHARD:
            database = quote(str(shard.identifier.get("database_id") or ""), safe="")
            payload = await self._call(
                context,
                "POST",
                f"databases/{database}/query",
                body={
                    "page_size": 1,
                    "sorts": [
                        {"timestamp": "last_edited_time", "direction": "descending"}
                    ],
                },
            )
        elif shard.kind == _PAGE_TREE_SHARD:
            payload = await self._search(context, "page", page_size=1, sort=True)
        else:
            return None
        first = next(
            (item for item in payload.get("results") or () if isinstance(item, dict)),
            None,
        )
        value = first.get("last_edited_time") if first else None
        return value if isinstance(value, str) else None


class NotionWebhook:
    """Verify a thin Notion event and fetch its canonical page record."""

    def __init__(self, binding: BindingContext) -> None:
        self._binding = binding
        self._ingestion = NotionIngestion(binding)

    async def verify_and_decode(
        self,
        request: BoundedWebhookRequest,
        context: OperationContext,
    ) -> VerifiedWebhookResult:
        secret = await self._binding.services.secrets.resolve(
            SlotId("webhook_verification_token")
        )
        headers = {key.lower(): value for key, value in request.headers.items()}
        supplied = headers.get("x-notion-signature", "")
        expected = hmac.new(
            secret.reveal_bytes(), request.body, hashlib.sha256
        ).hexdigest()
        if not (
            hmac.compare_digest(supplied, expected)
            or hmac.compare_digest(supplied, f"sha256={expected}")
        ):
            raise AuthenticationRejectedError("Notion webhook signature is invalid")
        try:
            payload = json.loads(request.body)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise PayloadRejectedError("Notion webhook body is not valid JSON") from exc
        if not isinstance(payload, dict):
            raise PayloadRejectedError("Notion webhook body must be an object")
        entity = payload.get("entity")
        entity_id = entity.get("id") if isinstance(entity, dict) else None
        entity_type = entity.get("type") if isinstance(entity, dict) else None
        workspace = payload.get("workspace_id")
        if entity_type != "page" or not isinstance(entity_id, str):
            return VerifiedWebhookResult(events=(), response_status_hint=202)
        page = await self._ingestion._call(
            context,
            "GET",
            f"pages/{quote(entity_id, safe='')}",
        )
        page["_fyralis_workspace_id"] = workspace
        external = (
            str(workspace)
            if workspace not in (None, "")
            else await self._ingestion._external_installation_id()
        )
        return VerifiedWebhookResult(
            events=(
                VerifiedWebhookEvent(
                    external_installation_id=external,
                    native_event_type=str(payload.get("type") or "page.updated"),
                    record=SourceRecord(native_type="page", payload=page),
                    verification_evidence={"scheme": "notion-hmac-sha256"},
                ),
            ),
        )
__all__ = [
    "NotionIngestion",
    "NotionNormalization",
    "NotionWebhook",
    "notion_external_id",
]
