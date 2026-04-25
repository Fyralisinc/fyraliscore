"""services/think/debug_capture.py — optional per-stage artifact capture.

Writes rows into `think_run_artifacts` when `DEBUG_ARTIFACT_CAPTURE=1`.
Each call is best-effort: any DB error is swallowed so a capture bug
never breaks Think.

The /debug UI reads these rows to show the full processing log for
every observation (retrieval output, LLM prompt, LLM response, ops,
etc.). Prod topology should leave the flag off — prompts contain
every piece of substrate the LLM saw.
"""
from __future__ import annotations

import json
import os
from dataclasses import asdict, is_dataclass
from typing import Any
from uuid import UUID

import asyncpg
import structlog

from lib.shared.ids import uuid7


_log = structlog.get_logger("think.debug_capture")

_STAGES = (
    "trigger",
    "retrieval",
    "prompt",
    "response",
    "validation",
    "apply",
    "post_commit",
    "cascade",
    "error",
)


def _enabled() -> bool:
    return os.environ.get("DEBUG_ARTIFACT_CAPTURE", "1") == "1"


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
    if not _enabled():
        return
    if stage not in _STAGES:
        _log.warning("debug_capture.unknown_stage", stage=stage)
        return
    try:
        payload_json = json.dumps(_coerce(payload), default=str)
    except Exception as e:  # noqa: BLE001
        _log.warning(
            "debug_capture.serialization_failed",
            stage=stage, error=str(e),
        )
        return
    try:
        await conn.execute(
            """
            INSERT INTO think_run_artifacts
                (id, run_id, tenant_id, stage, payload, captured_at)
            VALUES ($1, $2, $3, $4, $5::jsonb, now())
            """,
            uuid7(), run_id, tenant_id, stage, payload_json,
        )
    except Exception as e:  # noqa: BLE001
        _log.warning(
            "debug_capture.insert_failed",
            stage=stage, error=str(e)[:200],
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


__all__ = ["capture", "capture_with_pool"]
