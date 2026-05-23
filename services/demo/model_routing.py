"""services/demo/model_routing.py — per-tenant LLM model overrides.

The Think / Render / EntityResolver paths read `LLM_MODEL` from env
today. For demo tenants we want Haiku-by-default so the demo runs cheap
and fast. The override resolution order:

    1. demo_configs.model_routing[<call_kind>]
    2. demo_configs.model_routing["default"]
    3. env LLM_MODEL
    4. provider's compiled-in default

Call sites grab a routing decision via `resolve_model(...)` and pass
the resulting model name into a per-call `LLMConfig`. Non-demo tenants
fall through to the env value.

The well-known short names ("haiku", "sonnet", "opus") expand to the
canonical Anthropic model id so the JSON is human-friendly.
"""
from __future__ import annotations

import os
from uuid import UUID

import asyncpg

from services.demo.repo import get_demo_config_by_id, get_tenant


# Short-name expansion. Keep current-rev defaults here so the demo
# JSON stays compact and operators can swap the rev in one place.
SHORTNAME_TO_MODEL: dict[str, dict[str, str]] = {
    "haiku":   {"anthropic": "claude-haiku-4-5-20251001",
                "openai": "gpt-4o-mini",
                "deepseek": "deepseek-chat"},
    "sonnet":  {"anthropic": "claude-sonnet-4-6",
                "openai": "gpt-4o",
                "deepseek": "deepseek-chat"},
    "opus":    {"anthropic": "claude-opus-4-7",
                "openai": "gpt-4o",
                "deepseek": "deepseek-reasoner"},
}


async def resolve_model(
    conn: asyncpg.Connection | asyncpg.Pool,
    *,
    tenant_id: UUID,
    call_kind: str,
    fallback_model: str | None = None,
) -> str:
    """Return the model name to use for `call_kind` on this tenant.

    `call_kind` is the bucket name the demo config keys against — the
    convention is the same as `services.demo.budget.flush(call_kind=)`:
    "think" | "render" | "entity_resolver" | "snapshot_gen" etc.
    """
    fallback = fallback_model or os.environ.get("LLM_MODEL") or "claude-opus-4-7"
    tenant = await get_tenant(conn, tenant_id)
    if tenant is None or not tenant.is_demo or tenant.demo_config_id is None:
        return fallback
    cfg = await get_demo_config_by_id(conn, tenant.demo_config_id)
    if cfg is None:
        return fallback
    routing = cfg.model_routing or {}
    raw = routing.get(call_kind) or routing.get("default") or fallback
    return _expand(raw)


def _expand(raw: str) -> str:
    """Expand a short-name to the canonical model id for the active
    provider. Unknown short-names pass through (already-canonical id)."""
    if not raw:
        return raw
    provider = (os.environ.get("LLM_PROVIDER") or "anthropic").lower()
    table = SHORTNAME_TO_MODEL.get(raw.lower())
    if table is None:
        return raw
    return table.get(provider, raw)


async def determinism_seed_for_tenant(
    conn: asyncpg.Connection | asyncpg.Pool,
    tenant_id: UUID,
) -> int | None:
    """Return the determinism seed to use for this tenant's Think calls,
    or None for non-demo tenants and demo tenants without a seed."""
    tenant = await get_tenant(conn, tenant_id)
    if tenant is None or not tenant.is_demo or tenant.demo_config_id is None:
        return None
    cfg = await get_demo_config_by_id(conn, tenant.demo_config_id)
    return cfg.determinism_seed if cfg else None


__all__ = [
    "SHORTNAME_TO_MODEL",
    "resolve_model",
    "determinism_seed_for_tenant",
]
