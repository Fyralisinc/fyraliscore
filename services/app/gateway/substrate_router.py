"""Legacy substrate list endpoints."""
from __future__ import annotations

from typing import Any
from uuid import UUID

from fastapi import APIRouter, Request

from services.platform.access_control.audit import OverrideKind, record_override
from services.platform.access_control.checks import (
    AccessDecision,
    EntityKind,
    can_read,
)


def build_substrate_router() -> APIRouter:
    router = APIRouter(tags=["substrate"])

    @router.get("/observations")
    async def get_observations(
        request: Request, limit: int = 50, offset: int = 0, source: str | None = None
    ) -> dict[str, Any]:
        rows = await _visible_list(
            request,
            kind="observation",
            table="observations",
            output_columns=(
                "id",
                "kind",
                "source_channel",
                "occurred_at",
                "content_text",
            ),
            access_columns=("actor_id", "entities_mentioned", "source_actor_ref"),
            order_column="occurred_at",
            limit=limit,
            offset=offset,
            source_prefix=_source_prefix(source),
        )
        return {
            "items": [
                {
                    "id": str(r["id"]),
                    "kind": r["kind"],
                    "source_channel": r["source_channel"],
                    "occurred_at": r["occurred_at"].isoformat(),
                    "content_text": r["content_text"],
                }
                for r in rows
            ],
            "stub": False,
            "source": "substrate",
        }

    @router.get("/models")
    async def get_models(
        request: Request, limit: int = 50, offset: int = 0
    ) -> dict[str, Any]:
        return await _generic_list(
            request,
            kind="model",
            table="models",
            output_columns=(
                "id",
                "proposition",
                "confidence",
                "status",
                "created_at",
            ),
            access_columns=("visible_to_subjects", "scope_actors", "scope_entities"),
            limit=limit,
            offset=offset,
        )

    @router.get("/commitments")
    async def get_commitments(
        request: Request, limit: int = 50, offset: int = 0
    ) -> dict[str, Any]:
        return await _generic_list(
            request,
            kind="commitment",
            table="commitments",
            output_columns=(
                "id",
                "title",
                "state",
                "owner_id",
                "due_date",
                "created_at",
            ),
            access_columns=(),
            limit=limit,
            offset=offset,
        )

    @router.get("/goals")
    async def get_goals(
        request: Request, limit: int = 50, offset: int = 0
    ) -> dict[str, Any]:
        return await _generic_list(
            request,
            kind="goal",
            table="goals",
            output_columns=(
                "id",
                "title",
                "state",
                "altitude",
                "cached_health",
                "created_at",
            ),
            access_columns=(),
            limit=limit,
            offset=offset,
        )

    @router.get("/decisions")
    async def get_decisions(
        request: Request, limit: int = 50, offset: int = 0
    ) -> dict[str, Any]:
        return await _generic_list(
            request,
            kind="decision",
            table="decisions",
            output_columns=("id", "title", "state", "created_at"),
            access_columns=(),
            limit=limit,
            offset=offset,
        )

    @router.get("/resources")
    async def get_resources(
        request: Request, limit: int = 50, offset: int = 0
    ) -> dict[str, Any]:
        return await _generic_list(
            request,
            kind="resource",
            table="resources",
            output_columns=(
                "id",
                "kind",
                "identity",
                "utilization_state",
                "created_at",
            ),
            access_columns=("kind AS resource_kind", "metadata"),
            limit=limit,
            offset=offset,
        )

    return router


async def _generic_list(
    request: Request,
    *,
    kind: EntityKind,
    table: str,
    output_columns: tuple[str, ...],
    access_columns: tuple[str, ...],
    limit: int,
    offset: int,
) -> dict[str, Any]:
    rows = await _visible_list(
        request,
        kind=kind,
        table=table,
        output_columns=output_columns,
        access_columns=access_columns,
        order_column="created_at",
        limit=limit,
        offset=offset,
    )
    items: list[dict[str, Any]] = []
    for r in rows:
        item: dict[str, Any] = {}
        for c in output_columns:
            v = r[c]
            if hasattr(v, "isoformat"):
                v = v.isoformat()
            item[c] = str(v) if isinstance(v, UUID) else v
        items.append(item)
    return {"items": items, "stub": False, "source": "substrate"}


async def _visible_list(
    request: Request,
    *,
    kind: EntityKind,
    table: str,
    output_columns: tuple[str, ...],
    access_columns: tuple[str, ...],
    order_column: str,
    limit: int,
    offset: int,
    source_prefix: str | None = None,
) -> list[dict[str, Any]]:
    auth = request.state.auth
    deps = _deps(request)
    select_columns = _select_columns(output_columns, access_columns)
    source_filter = ""
    limit_arg = 2
    offset_arg = 3
    query_args: list[Any] = [auth.tenant_id]
    if source_prefix and table == "observations":
        source_filter = "AND source_channel LIKE $2 "
        limit_arg = 3
        offset_arg = 4
        query_args.append(f"{source_prefix}:%")
    query = (
        f"SELECT {select_columns} FROM {table} "
        "WHERE tenant_id = $1 "
        f"{source_filter}"
        f"ORDER BY {order_column} DESC "
        f"LIMIT ${limit_arg} OFFSET ${offset_arg}"
    )
    requested_limit = _clip(limit, 1, 500)
    visible_offset = max(offset, 0)
    batch_size = min(max(requested_limit + min(visible_offset, 500), 50), 500)
    async with deps.pool.acquire() as conn:
        visible: list[dict[str, Any]] = []
        visible_seen = 0
        raw_offset = 0
        while len(visible) < requested_limit:
            rows = await conn.fetch(
                query,
                *query_args,
                batch_size,
                raw_offset,
            )
            if not rows:
                break
            raw_offset += len(rows)
            for row in rows:
                item = dict(row)
                entity = _entity_for_access(kind, item, auth.tenant_id)
                decision: AccessDecision = await can_read(
                    auth.actor_id,
                    entity,
                    conn=conn,
                    tenant_id=auth.tenant_id,
                )
                if not decision.allowed:
                    continue
                if visible_seen < visible_offset:
                    visible_seen += 1
                    continue
                await _record_override_if_needed(
                    decision,
                    actor_id=auth.actor_id,
                    entity_type=kind,
                    entity_id=item.get("id"),
                    conn=conn,
                    tenant_id=auth.tenant_id,
                )
                visible.append(item)
                if len(visible) >= requested_limit:
                    break
            if len(rows) < batch_size:
                break
        return visible


def _source_prefix(source: str | None) -> str | None:
    if not source:
        return None
    normalized = source.strip().lower().replace("-", "_")
    if not normalized:
        return None
    return "".join(ch for ch in normalized if ch.isalnum() or ch == "_")


def _select_columns(
    output_columns: tuple[str, ...],
    access_columns: tuple[str, ...],
) -> str:
    columns = ["tenant_id", *output_columns, *access_columns]
    seen: set[str] = set()
    selected: list[str] = []
    for column in columns:
        output_name = _output_column_name(column)
        if output_name in seen:
            continue
        seen.add(output_name)
        selected.append(column)
    return ", ".join(selected)


def _output_column_name(column: str) -> str:
    lowered = column.lower()
    if " as " in lowered:
        return column[lowered.rindex(" as ") + 4 :].strip()
    return column.strip()


def _entity_for_access(
    kind: EntityKind,
    row: dict[str, Any],
    tenant_id: UUID,
) -> dict[str, Any]:
    entity = dict(row)
    entity["kind"] = kind
    entity["tenant_id"] = tenant_id
    return entity


async def _record_override_if_needed(
    decision: AccessDecision,
    *,
    actor_id: UUID,
    entity_type: str,
    entity_id: Any,
    conn: Any,
    tenant_id: UUID,
) -> None:
    if not decision.override_applied:
        return
    await record_override(
        actor_id,
        entity_type,
        entity_id if isinstance(entity_id, UUID) else UUID(str(entity_id)),
        _override_kind(decision.reason),
        conn=conn,
        tenant_id=tenant_id,
        reason=decision.reason,
    )


def _override_kind(reason: str) -> OverrideKind:
    if reason == "admin_override":
        return "admin"
    if reason == "leadership_override":
        return "leadership"
    if reason == "model_self_scope":
        return "first_person"
    return "system"


def _deps(request: Request) -> Any:
    deps = getattr(request.app.state, "deps", None)
    if deps is None:
        raise RuntimeError("Gateway deps not initialised (call lifespan startup)")
    return deps


def _clip(x: int, lo: int, hi: int) -> int:
    return max(lo, min(hi, int(x)))
