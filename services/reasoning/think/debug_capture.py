"""services/reasoning/think/debug_capture.py — optional per-stage artifact capture.

Writes rows into `think_run_artifacts` when `DEBUG_ARTIFACT_CAPTURE=1`.
Each call is best-effort. Think attempts install a deferred-capture scope:
artifact payloads produced inside the mutation transaction are serialized
immediately, buffered in memory, and flushed on a fresh connection after
the transaction commits. Direct callers outside that scope still get the
old savepoint-wrapped insert behavior.

The /debug UI reads these rows to show the full processing log for
every observation (retrieval output, LLM prompt, LLM response, ops,
etc.). Prod topology should leave the flag off — prompts contain
every piece of substrate the LLM saw.
"""
from __future__ import annotations

import json
import os
from contextvars import ContextVar
from dataclasses import asdict, is_dataclass
from dataclasses import dataclass
from typing import Any
from uuid import UUID

import asyncpg
import structlog

from lib.shared.env import is_prod
from lib.shared.ids import uuid7


_log = structlog.get_logger("think.debug_capture")

_STAGES = (
    "trigger",
    "routing",
    "retrieval",
    "inquiry",
    "context_packet",
    "sufficiency",
    "prompt",
    "response",
    "validation",
    "apply",
    "post_commit",
    "cascade",
    "error",
)


_TRUE_VALUES = frozenset({"1", "true", "yes", "on"})
_FALSE_VALUES = frozenset({"0", "false", "no", "off"})


@dataclass(frozen=True)
class DebugArtifact:
    run_id: UUID
    tenant_id: UUID
    stage: str
    payload_json: str


_pending: ContextVar[list[DebugArtifact] | None] = ContextVar(
    "think_debug_capture_pending",
    default=None,
)


class _DeferredCaptureScope:
    def __init__(self) -> None:
        self._token: Any | None = None
        self.artifacts: list[DebugArtifact] = []

    def __enter__(self) -> "_DeferredCaptureScope":
        self._token = _pending.set(self.artifacts)
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        if self._token is not None:
            _pending.reset(self._token)
            self._token = None
        if exc is not None:
            self.artifacts.clear()


def defer_transactional_captures() -> _DeferredCaptureScope:
    return _DeferredCaptureScope()


def _flag_enabled(raw: str | None) -> bool:
    if raw is None or raw == "":
        return True
    value = raw.strip().lower()
    if value in _TRUE_VALUES:
        return True
    if value in _FALSE_VALUES:
        return False
    _log.warning("debug_capture.invalid_flag", value=raw)
    return False


def _enabled() -> bool:
    if is_prod():
        raw = os.environ.get("DEBUG_ARTIFACT_CAPTURE")
        if raw not in (None, "") and _flag_enabled(raw):
            _log.warning("debug_capture.disabled_in_production")
        return False
    return _flag_enabled(os.environ.get("DEBUG_ARTIFACT_CAPTURE"))


def _coerce(obj: Any) -> Any:
    """Best-effort JSON coercion for think pipeline objects."""
    if obj is None or isinstance(obj, (bool, int, float, str)):
        return obj
    if isinstance(obj, UUID):
        return str(obj)
    if isinstance(obj, (list, tuple, set, frozenset)):
        return [_coerce(x) for x in obj]
    if isinstance(obj, dict):
        return {str(k): _coerce(v) for k, v in obj.items()}
    if is_dataclass(obj) and not isinstance(obj, type):
        try:
            return _coerce(asdict(obj))
        except Exception:  # noqa: BLE001
            pass
    if hasattr(obj, "model_dump"):
        try:
            return _coerce(obj.model_dump(mode="python"))
        except Exception:  # noqa: BLE001
            pass
    if hasattr(obj, "__dict__"):
        try:
            return {k: _coerce(v) for k, v in vars(obj).items()
                    if not k.startswith("_")}
        except Exception:  # noqa: BLE001
            pass
    try:
        return repr(obj)
    except Exception:  # noqa: BLE001
        return "<unserializable>"


async def capture(
    conn: asyncpg.Connection,
    *,
    run_id: UUID,
    tenant_id: UUID,
    stage: str,
    payload: Any,
) -> None:
    payload_json = _prepare_artifact_payload(stage=stage, payload=payload)
    if payload_json is None:
        return
    pending = _pending.get()
    if pending is not None and conn.is_in_transaction():
        pending.append(
            DebugArtifact(
                run_id=run_id,
                tenant_id=tenant_id,
                stage=stage,
                payload_json=payload_json,
            )
        )
        return
    await _insert_artifact(
        conn,
        DebugArtifact(
            run_id=run_id,
            tenant_id=tenant_id,
            stage=stage,
            payload_json=payload_json,
        ),
    )


def _prepare_artifact_payload(*, stage: str, payload: Any) -> str | None:
    if not _enabled():
        return None
    if stage not in _STAGES:
        _log.warning("debug_capture.unknown_stage", stage=stage)
        return None
    try:
        return json.dumps(_coerce(payload), default=str)
    except Exception as e:  # noqa: BLE001
        _log.warning(
            "debug_capture.serialization_failed",
            stage=stage,
            error=str(e),
        )
        return None


async def _insert_artifact(conn: asyncpg.Connection, artifact: DebugArtifact) -> None:
    try:
        async with conn.transaction():
            await conn.execute(
                """
                INSERT INTO think_run_artifacts
                    (id, run_id, tenant_id, stage, payload, captured_at)
                VALUES ($1, $2, $3, $4, $5::jsonb, now())
                """,
                uuid7(),
                artifact.run_id,
                artifact.tenant_id,
                artifact.stage,
                artifact.payload_json,
            )
    except Exception as e:  # noqa: BLE001
        _log.warning(
            "debug_capture.insert_failed",
            stage=artifact.stage,
            error=str(e)[:200],
        )


async def flush_captures(
    pool: asyncpg.Pool,
    artifacts: list[DebugArtifact],
) -> None:
    if not artifacts:
        return
    try:
        async with pool.acquire() as conn:
            for artifact in artifacts:
                await _insert_artifact(conn, artifact)
    except Exception as e:  # noqa: BLE001
        _log.warning(
            "debug_capture.flush_failed",
            count=len(artifacts),
            error=str(e)[:200],
        )


async def capture_with_pool(
    pool: asyncpg.Pool,
    *,
    run_id: UUID,
    tenant_id: UUID,
    stage: str,
    payload: Any,
) -> None:
    if not _enabled():
        return
    try:
        async with pool.acquire() as conn:
            await capture(
                conn,
                run_id=run_id,
                tenant_id=tenant_id,
                stage=stage,
                payload=payload,
            )
    except Exception as e:  # noqa: BLE001
        _log.warning(
            "debug_capture.pool_acquire_failed",
            stage=stage, error=str(e)[:200],
        )


__all__ = [
    "DebugArtifact",
    "capture",
    "capture_with_pool",
    "defer_transactional_captures",
    "flush_captures",
]
