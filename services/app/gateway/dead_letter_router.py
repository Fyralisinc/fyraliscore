"""Gateway admin routes for durable dead-letter queues."""
from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Literal
from uuid import UUID

import asyncpg
from fastapi import APIRouter, Request, status
from fastapi.responses import JSONResponse

from lib.shared.ids import uuid7
from services.app.gateway.auth import AuthContext
from services.app.gateway.deps import get_gateway_deps
from services.domain.triggers import enqueue_model_reeval
from services.platform.access_control.roles import has_role


DeadLetterQueue = Literal["post_commit", "model_reeval", "think_trigger"]

_QUEUES: tuple[DeadLetterQueue, ...] = (
    "post_commit",
    "model_reeval",
    "think_trigger",
)
_MAX_LIMIT = 100
_ERROR_PREVIEW_CHARS = 500
_REASON_CHARS = 500


def build_dead_letter_admin_router() -> APIRouter:
    router = APIRouter(prefix="/api/admin/dead-letters", tags=["admin"])

    @router.get("")
    async def list_dead_letters(
        request: Request,
        queue: str = "all",
        limit: int = 50,
        include_quarantined: bool = False,
    ) -> JSONResponse:
        auth = _auth(request)
        if auth is None:
            return _unauth()
        queue_names = _parse_queues(queue)
        if queue_names is None:
            return _bad_request("invalid_queue")
        limit = _normalize_limit(limit)

        deps = get_gateway_deps(request)
        async with deps.pool.acquire() as conn:
            async with conn.transaction():
                if not await _is_admin(conn, auth):
                    return _forbidden()
                items = await _list_dead_letters(
                    conn,
                    tenant_id=auth.tenant_id,
                    queues=queue_names,
                    limit=limit,
                    include_quarantined=include_quarantined,
                )
                await _record_operator_action(
                    conn,
                    auth=auth,
                    action="dead_letter.list",
                    resource_type="dead_letter_collection",
                    resource_id=None,
                    metadata={
                        "queues": list(queue_names),
                        "limit": limit,
                        "include_quarantined": include_quarantined,
                        "item_count": len(items),
                    },
                )
        return JSONResponse(
            {
                "items": items,
                "queues": list(queue_names),
                "limit": limit,
                "include_quarantined": include_quarantined,
            }
        )

    @router.post("/{queue}/{item_id}/retry")
    async def retry_dead_letter(
        queue: str,
        item_id: UUID,
        request: Request,
    ) -> JSONResponse:
        auth = _auth(request)
        if auth is None:
            return _unauth()
        queue_name = _parse_queue(queue)
        if queue_name is None:
            return _bad_request("invalid_queue")
        body, body_error = await _optional_json_body(request)
        if body_error is not None:
            return _bad_request(body_error)
        reason = _bounded_text(body.get("reason") if isinstance(body, dict) else None)

        deps = get_gateway_deps(request)
        async with deps.pool.acquire() as conn:
            async with conn.transaction():
                if not await _is_admin(conn, auth):
                    return _forbidden()
                result = await _retry_dead_letter(
                    conn,
                    auth=auth,
                    queue=queue_name,
                    item_id=item_id,
                    reason=reason,
                )
                if result is None:
                    return _not_found()
                if result.get("error") == "dead_letter_quarantined":
                    return _conflict("dead_letter_quarantined")
                if result.get("error") == "dead_letter_already_retried":
                    return _conflict("dead_letter_already_retried")
                if result.get("error") == "not_dead_lettered":
                    return _conflict("not_dead_lettered")
        return JSONResponse(result)

    @router.post("/{queue}/{item_id}/quarantine")
    async def quarantine_dead_letter(
        queue: str,
        item_id: UUID,
        request: Request,
    ) -> JSONResponse:
        auth = _auth(request)
        if auth is None:
            return _unauth()
        queue_name = _parse_queue(queue)
        if queue_name is None:
            return _bad_request("invalid_queue")
        body, body_error = await _optional_json_body(request)
        if body_error is not None:
            return _bad_request(body_error)
        reason = _bounded_text(body.get("reason") if isinstance(body, dict) else None)
        if not reason:
            return _bad_request("quarantine_reason_required")

        deps = get_gateway_deps(request)
        async with deps.pool.acquire() as conn:
            async with conn.transaction():
                if not await _is_admin(conn, auth):
                    return _forbidden()
                result = await _quarantine_dead_letter(
                    conn,
                    auth=auth,
                    queue=queue_name,
                    item_id=item_id,
                    reason=reason,
                )
                if result is None:
                    return _not_found()
                if result.get("error") == "dead_letter_already_retried":
                    return _conflict("dead_letter_already_retried")
                if result.get("error") == "not_dead_lettered":
                    return _conflict("not_dead_lettered")
        return JSONResponse(result)

    return router


def _auth(request: Request) -> AuthContext | None:
    auth = getattr(request.state, "auth", None)
    return auth if isinstance(auth, AuthContext) else None


async def _is_admin(conn: asyncpg.Connection, auth: AuthContext) -> bool:
    return await has_role(
        auth.actor_id,
        "admin",
        conn=conn,
        tenant_id=auth.tenant_id,
    )


def _parse_queue(value: str) -> DeadLetterQueue | None:
    value = value.strip().lower()
    if value in _QUEUES:
        return value  # type: ignore[return-value]
    return None


def _parse_queues(value: str) -> tuple[DeadLetterQueue, ...] | None:
    value = value.strip().lower()
    if value == "all":
        return _QUEUES
    parsed = _parse_queue(value)
    return (parsed,) if parsed is not None else None


def _normalize_limit(limit: int) -> int:
    return max(1, min(_MAX_LIMIT, int(limit or 1)))


async def _optional_json_body(request: Request) -> tuple[dict[str, Any], str | None]:
    raw = await request.body()
    if not raw:
        return {}, None
    try:
        value = json.loads(raw)
    except json.JSONDecodeError:
        return {}, "invalid_json"
    if not isinstance(value, dict):
        return {}, "invalid_json"
    return value, None


def _bounded_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    return text[:_REASON_CHARS]


async def _list_dead_letters(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    queues: tuple[DeadLetterQueue, ...],
    limit: int,
    include_quarantined: bool,
) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    if "post_commit" in queues:
        items.extend(
            await _list_post_commit_dead_letters(
                conn,
                tenant_id=tenant_id,
                limit=limit,
                include_quarantined=include_quarantined,
            )
        )
    if "model_reeval" in queues:
        items.extend(
            await _list_model_reeval_dead_letters(
                conn,
                tenant_id=tenant_id,
                limit=limit,
                include_quarantined=include_quarantined,
            )
        )
    if "think_trigger" in queues:
        items.extend(
            await _list_think_trigger_dead_letters(
                conn,
                tenant_id=tenant_id,
                limit=limit,
                include_quarantined=include_quarantined,
            )
        )
    items.sort(key=lambda item: item.pop("_sort_at"), reverse=True)
    return items[:limit]


async def _list_post_commit_dead_letters(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    limit: int,
    include_quarantined: bool,
) -> list[dict[str, Any]]:
    quarantine_filter = "" if include_quarantined else "AND quarantined_at IS NULL"
    rows = await conn.fetch(
        f"""
        SELECT id, trigger_id, action_kind, attempts, created_at, scheduled_at,
               dead_lettered_at, last_error, quarantined_at, quarantine_reason
        FROM pending_post_commit_actions
        WHERE tenant_id = $1
          AND dead_lettered_at IS NOT NULL
          {quarantine_filter}
        ORDER BY dead_lettered_at DESC, created_at DESC
        LIMIT $2
        """,
        tenant_id,
        limit,
    )
    return [
        {
            "queue": "post_commit",
            "id": str(row["id"]),
            "state": _state(row["quarantined_at"]),
            "trigger_id": str(row["trigger_id"]),
            "action_kind": row["action_kind"],
            "attempts": row["attempts"],
            "created_at": _iso(row["created_at"]),
            "scheduled_at": _iso(row["scheduled_at"]),
            "dead_lettered_at": _iso(row["dead_lettered_at"]),
            "quarantined_at": _iso(row["quarantined_at"]),
            "quarantine_reason": row["quarantine_reason"],
            "error_preview": _error_preview(row["last_error"]),
            "_sort_at": row["dead_lettered_at"],
        }
        for row in rows
    ]


async def _list_model_reeval_dead_letters(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    limit: int,
    include_quarantined: bool,
) -> list[dict[str, Any]]:
    resolved_filter = (
        ""
        if include_quarantined
        else "AND quarantined_at IS NULL AND retried_at IS NULL"
    )
    rows = await conn.fetch(
        f"""
        SELECT id, original_queue_id, model_id, cause_model_id, cause_kind,
               attempts, last_error, enqueued_at, dead_lettered_at,
               quarantined_at, quarantine_reason, retried_at, retry_queue_id
        FROM model_reeval_dead_letter
        WHERE tenant_id = $1
          {resolved_filter}
        ORDER BY dead_lettered_at DESC
        LIMIT $2
        """,
        tenant_id,
        limit,
    )
    return [
        {
            "queue": "model_reeval",
            "id": str(row["id"]),
            "state": _model_reeval_state(row),
            "original_queue_id": str(row["original_queue_id"]),
            "model_id": str(row["model_id"]),
            "cause_model_id": (
                str(row["cause_model_id"]) if row["cause_model_id"] else None
            ),
            "cause_kind": row["cause_kind"],
            "attempts": row["attempts"],
            "enqueued_at": _iso(row["enqueued_at"]),
            "dead_lettered_at": _iso(row["dead_lettered_at"]),
            "quarantined_at": _iso(row["quarantined_at"]),
            "quarantine_reason": row["quarantine_reason"],
            "retried_at": _iso(row["retried_at"]),
            "retry_queue_id": (
                str(row["retry_queue_id"]) if row["retry_queue_id"] else None
            ),
            "error_preview": _error_preview(row["last_error"]),
            "_sort_at": row["dead_lettered_at"],
        }
        for row in rows
    ]


async def _list_think_trigger_dead_letters(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    limit: int,
    include_quarantined: bool,
) -> list[dict[str, Any]]:
    quarantine_filter = "" if include_quarantined else "AND quarantined_at IS NULL"
    rows = await conn.fetch(
        f"""
        SELECT id, trigger_kind, trigger_subkind, observation_id, model_id,
               attempts, enqueued_at, scheduled_for, completed_at, last_error,
               quarantined_at, quarantine_reason
        FROM think_trigger_queue
        WHERE tenant_id = $1
          AND completed_at IS NOT NULL
          AND last_error IS NOT NULL
          {quarantine_filter}
        ORDER BY completed_at DESC, enqueued_at DESC
        LIMIT $2
        """,
        tenant_id,
        limit,
    )
    return [
        {
            "queue": "think_trigger",
            "id": str(row["id"]),
            "state": _state(row["quarantined_at"]),
            "trigger_kind": row["trigger_kind"],
            "trigger_subkind": row["trigger_subkind"],
            "observation_id": (
                str(row["observation_id"]) if row["observation_id"] else None
            ),
            "model_id": str(row["model_id"]) if row["model_id"] else None,
            "attempts": row["attempts"],
            "enqueued_at": _iso(row["enqueued_at"]),
            "scheduled_for": _iso(row["scheduled_for"]),
            "completed_at": _iso(row["completed_at"]),
            "quarantined_at": _iso(row["quarantined_at"]),
            "quarantine_reason": row["quarantine_reason"],
            "error_preview": _error_preview(row["last_error"]),
            "_sort_at": row["completed_at"],
        }
        for row in rows
    ]


async def _retry_dead_letter(
    conn: asyncpg.Connection,
    *,
    auth: AuthContext,
    queue: DeadLetterQueue,
    item_id: UUID,
    reason: str | None,
) -> dict[str, Any] | None:
    if queue == "post_commit":
        return await _retry_post_commit(conn, auth=auth, item_id=item_id, reason=reason)
    if queue == "model_reeval":
        return await _retry_model_reeval(conn, auth=auth, item_id=item_id, reason=reason)
    return await _retry_think_trigger(conn, auth=auth, item_id=item_id, reason=reason)


async def _retry_post_commit(
    conn: asyncpg.Connection,
    *,
    auth: AuthContext,
    item_id: UUID,
    reason: str | None,
) -> dict[str, Any] | None:
    row = await conn.fetchrow(
        """
        SELECT id, dead_lettered_at, quarantined_at, attempts, action_kind
        FROM pending_post_commit_actions
        WHERE tenant_id = $1 AND id = $2
        FOR UPDATE
        """,
        auth.tenant_id,
        item_id,
    )
    if row is None:
        return None
    if row["dead_lettered_at"] is None:
        return {"error": "not_dead_lettered"}
    if row["quarantined_at"] is not None:
        return {"error": "dead_letter_quarantined"}
    await conn.execute(
        """
        UPDATE pending_post_commit_actions
        SET dead_lettered_at = NULL,
            scheduled_at = now(),
            attempts = 0,
            last_error = NULL,
            quarantined_at = NULL,
            quarantined_by = NULL,
            quarantine_reason = NULL
        WHERE tenant_id = $1 AND id = $2
        """,
        auth.tenant_id,
        item_id,
    )
    await _record_operator_action(
        conn,
        auth=auth,
        action="dead_letter.retry",
        resource_type="post_commit",
        resource_id=item_id,
        metadata={
            "previous_attempts": row["attempts"],
            "action_kind": row["action_kind"],
            "reason": reason,
        },
    )
    return {
        "status": "retry_scheduled",
        "queue": "post_commit",
        "id": str(item_id),
    }


async def _retry_model_reeval(
    conn: asyncpg.Connection,
    *,
    auth: AuthContext,
    item_id: UUID,
    reason: str | None,
) -> dict[str, Any] | None:
    row = await conn.fetchrow(
        """
        SELECT id, model_id, cause_model_id, cause_kind, attempts,
               quarantined_at, retried_at
        FROM model_reeval_dead_letter
        WHERE tenant_id = $1 AND id = $2
        FOR UPDATE
        """,
        auth.tenant_id,
        item_id,
    )
    if row is None:
        return None
    if row["quarantined_at"] is not None:
        return {"error": "dead_letter_quarantined"}
    if row["retried_at"] is not None:
        return {"error": "dead_letter_already_retried"}

    queued_id = await enqueue_model_reeval(
        conn,
        tenant_id=auth.tenant_id,
        model_id=row["model_id"],
        cause_model_id=row["cause_model_id"],
        cause_kind=row["cause_kind"],
    )
    await conn.execute(
        """
        UPDATE model_reeval_queue
        SET enqueued_at = now(),
            attempts = 0,
            last_error = NULL
        WHERE tenant_id = $1
          AND id = $2
          AND processed_at IS NULL
        """,
        auth.tenant_id,
        queued_id,
    )
    await conn.execute(
        """
        UPDATE model_reeval_dead_letter
        SET retried_at = now(),
            retried_by = $3,
            retry_queue_id = $4
        WHERE tenant_id = $1 AND id = $2
        """,
        auth.tenant_id,
        item_id,
        auth.actor_id,
        queued_id,
    )
    await _record_operator_action(
        conn,
        auth=auth,
        action="dead_letter.retry",
        resource_type="model_reeval",
        resource_id=item_id,
        metadata={
            "previous_attempts": row["attempts"],
            "retry_queue_id": str(queued_id),
            "reason": reason,
        },
    )
    return {
        "status": "retry_scheduled",
        "queue": "model_reeval",
        "id": str(item_id),
        "retry_queue_id": str(queued_id),
    }


async def _retry_think_trigger(
    conn: asyncpg.Connection,
    *,
    auth: AuthContext,
    item_id: UUID,
    reason: str | None,
) -> dict[str, Any] | None:
    row = await conn.fetchrow(
        """
        SELECT id, completed_at, last_error, quarantined_at, attempts,
               trigger_kind, trigger_subkind
        FROM think_trigger_queue
        WHERE tenant_id = $1 AND id = $2
        FOR UPDATE
        """,
        auth.tenant_id,
        item_id,
    )
    if row is None:
        return None
    if row["completed_at"] is None or row["last_error"] is None:
        return {"error": "not_dead_lettered"}
    if row["quarantined_at"] is not None:
        return {"error": "dead_letter_quarantined"}
    await conn.execute(
        """
        UPDATE think_trigger_queue
        SET completed_at = NULL,
            scheduled_for = now(),
            attempts = 0,
            locked_by = NULL,
            locked_at = NULL,
            last_error = NULL,
            quarantined_at = NULL,
            quarantined_by = NULL,
            quarantine_reason = NULL
        WHERE tenant_id = $1 AND id = $2
        """,
        auth.tenant_id,
        item_id,
    )
    await _record_operator_action(
        conn,
        auth=auth,
        action="dead_letter.retry",
        resource_type="think_trigger",
        resource_id=item_id,
        metadata={
            "previous_attempts": row["attempts"],
            "trigger_kind": row["trigger_kind"],
            "trigger_subkind": row["trigger_subkind"],
            "reason": reason,
        },
    )
    return {
        "status": "retry_scheduled",
        "queue": "think_trigger",
        "id": str(item_id),
    }


async def _quarantine_dead_letter(
    conn: asyncpg.Connection,
    *,
    auth: AuthContext,
    queue: DeadLetterQueue,
    item_id: UUID,
    reason: str,
) -> dict[str, Any] | None:
    if queue == "post_commit":
        return await _quarantine_post_commit(
            conn, auth=auth, item_id=item_id, reason=reason
        )
    if queue == "model_reeval":
        return await _quarantine_model_reeval(
            conn, auth=auth, item_id=item_id, reason=reason
        )
    return await _quarantine_think_trigger(
        conn, auth=auth, item_id=item_id, reason=reason
    )


async def _quarantine_post_commit(
    conn: asyncpg.Connection,
    *,
    auth: AuthContext,
    item_id: UUID,
    reason: str,
) -> dict[str, Any] | None:
    row = await conn.fetchrow(
        """
        SELECT id, dead_lettered_at, action_kind
        FROM pending_post_commit_actions
        WHERE tenant_id = $1 AND id = $2
        FOR UPDATE
        """,
        auth.tenant_id,
        item_id,
    )
    if row is None:
        return None
    if row["dead_lettered_at"] is None:
        return {"error": "not_dead_lettered"}
    await conn.execute(
        """
        UPDATE pending_post_commit_actions
        SET quarantined_at = COALESCE(quarantined_at, now()),
            quarantined_by = $3,
            quarantine_reason = $4
        WHERE tenant_id = $1 AND id = $2
        """,
        auth.tenant_id,
        item_id,
        auth.actor_id,
        reason,
    )
    await _record_operator_action(
        conn,
        auth=auth,
        action="dead_letter.quarantine",
        resource_type="post_commit",
        resource_id=item_id,
        metadata={"action_kind": row["action_kind"], "reason": reason},
    )
    return {"status": "quarantined", "queue": "post_commit", "id": str(item_id)}


async def _quarantine_model_reeval(
    conn: asyncpg.Connection,
    *,
    auth: AuthContext,
    item_id: UUID,
    reason: str,
) -> dict[str, Any] | None:
    row = await conn.fetchrow(
        """
        SELECT id, cause_kind, retried_at
        FROM model_reeval_dead_letter
        WHERE tenant_id = $1 AND id = $2
        FOR UPDATE
        """,
        auth.tenant_id,
        item_id,
    )
    if row is None:
        return None
    if row["retried_at"] is not None:
        return {"error": "dead_letter_already_retried"}
    await conn.execute(
        """
        UPDATE model_reeval_dead_letter
        SET quarantined_at = COALESCE(quarantined_at, now()),
            quarantined_by = $3,
            quarantine_reason = $4
        WHERE tenant_id = $1 AND id = $2
        """,
        auth.tenant_id,
        item_id,
        auth.actor_id,
        reason,
    )
    await _record_operator_action(
        conn,
        auth=auth,
        action="dead_letter.quarantine",
        resource_type="model_reeval",
        resource_id=item_id,
        metadata={"cause_kind": row["cause_kind"], "reason": reason},
    )
    return {"status": "quarantined", "queue": "model_reeval", "id": str(item_id)}


async def _quarantine_think_trigger(
    conn: asyncpg.Connection,
    *,
    auth: AuthContext,
    item_id: UUID,
    reason: str,
) -> dict[str, Any] | None:
    row = await conn.fetchrow(
        """
        SELECT id, completed_at, last_error, trigger_kind, trigger_subkind
        FROM think_trigger_queue
        WHERE tenant_id = $1 AND id = $2
        FOR UPDATE
        """,
        auth.tenant_id,
        item_id,
    )
    if row is None:
        return None
    if row["completed_at"] is None or row["last_error"] is None:
        return {"error": "not_dead_lettered"}
    await conn.execute(
        """
        UPDATE think_trigger_queue
        SET quarantined_at = COALESCE(quarantined_at, now()),
            quarantined_by = $3,
            quarantine_reason = $4
        WHERE tenant_id = $1 AND id = $2
        """,
        auth.tenant_id,
        item_id,
        auth.actor_id,
        reason,
    )
    await _record_operator_action(
        conn,
        auth=auth,
        action="dead_letter.quarantine",
        resource_type="think_trigger",
        resource_id=item_id,
        metadata={
            "trigger_kind": row["trigger_kind"],
            "trigger_subkind": row["trigger_subkind"],
            "reason": reason,
        },
    )
    return {"status": "quarantined", "queue": "think_trigger", "id": str(item_id)}


async def _record_operator_action(
    conn: asyncpg.Connection,
    *,
    auth: AuthContext,
    action: str,
    resource_type: str,
    resource_id: UUID | None,
    metadata: dict[str, Any],
) -> None:
    await conn.execute(
        """
        INSERT INTO operator_action_log (
            id, tenant_id, actor_id, action, resource_type, resource_id,
            metadata, occurred_at
        ) VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, now())
        """,
        uuid7(),
        auth.tenant_id,
        auth.actor_id,
        action,
        resource_type,
        resource_id,
        json.dumps(metadata, default=str, sort_keys=True),
    )


def _state(quarantined_at: datetime | None) -> str:
    return "quarantined" if quarantined_at is not None else "dead_lettered"


def _model_reeval_state(row: asyncpg.Record) -> str:
    if row["quarantined_at"] is not None:
        return "quarantined"
    if row["retried_at"] is not None:
        return "retried"
    return "dead_lettered"


def _iso(value: datetime | None) -> str | None:
    return value.isoformat() if value is not None else None


def _error_preview(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).replace("\n", " ").strip()
    return text[:_ERROR_PREVIEW_CHARS] if text else None


def _unauth() -> JSONResponse:
    return JSONResponse(
        {"error": "unauthorized"},
        status_code=status.HTTP_401_UNAUTHORIZED,
    )


def _forbidden() -> JSONResponse:
    return JSONResponse(
        {"error": "forbidden", "reason": "admin_required"},
        status_code=status.HTTP_403_FORBIDDEN,
    )


def _bad_request(reason: str) -> JSONResponse:
    return JSONResponse(
        {"error": "bad_request", "reason": reason},
        status_code=status.HTTP_400_BAD_REQUEST,
    )


def _not_found() -> JSONResponse:
    return JSONResponse(
        {"error": "dead_letter_not_found"},
        status_code=status.HTTP_404_NOT_FOUND,
    )


def _conflict(reason: str) -> JSONResponse:
    return JSONResponse(
        {"error": "conflict", "reason": reason},
        status_code=status.HTTP_409_CONFLICT,
    )


__all__ = ["build_dead_letter_admin_router"]
