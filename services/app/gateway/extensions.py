"""services/app/gateway/extensions.py — gateway extension discovery seam.

Lets overlay packages (today: the demo overlay) contribute routers, startup
hooks, and unauthenticated path prefixes to the gateway **without core importing
them**. Core discovers whatever is installed via the
``company_os.gateway_extensions`` entry-point group; with no overlay installed
the gateway runs exactly as a bare core deployment.

An entry point resolves to either a :class:`GatewayExtension` instance or a
zero-argument callable returning one.

Declared by an overlay's ``pyproject.toml`` like::

    [project.entry-points."company_os.gateway_extensions"]
    demo = "fyralis_demo.gateway:extension"
"""
from __future__ import annotations

import importlib.metadata as importlib_metadata
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field

import asyncpg
from fastapi import APIRouter, FastAPI

from services.app.gateway.logging_config import get_logger

log = get_logger("gateway")

_ENTRY_POINT_GROUP = "company_os.gateway_extensions"

# Async hook run during gateway startup, receiving the app and runtime DB pool.
# The app is passed so hooks may mount routers/static that need live state
# (e.g. the simulation panel); pool covers the common seed-on-startup case.
StartupHook = Callable[["FastAPI", asyncpg.Pool], Awaitable[None]]


@dataclass
class GatewayExtension:
    """What an overlay may contribute to the running gateway."""

    name: str = "extension"
    routers: list[APIRouter] = field(default_factory=list)
    startup_hooks: list[StartupHook] = field(default_factory=list)
    production_enabled: bool = False
    # Path prefixes that bypass the bearer-session middleware (the overlay's
    # public, self-authenticating endpoints).
    public_path_prefixes: tuple[str, ...] = ()


_cache: list[GatewayExtension] | None = None


def discovered_extensions() -> list[GatewayExtension]:
    """Resolve installed gateway extensions once per process (cached)."""
    global _cache
    if _cache is not None:
        return _cache
    found: list[GatewayExtension] = []
    try:
        entry_points = importlib_metadata.entry_points(group=_ENTRY_POINT_GROUP)
    except Exception:  # noqa: BLE001 - discovery must never block startup
        log.warning("gateway_extension_discovery_failed", exc_info=True)
        _cache = found
        return found
    for ep in entry_points:
        try:
            obj = ep.load()
            ext = obj() if callable(obj) and not isinstance(obj, GatewayExtension) else obj
            if not isinstance(ext, GatewayExtension):
                log.error("gateway_extension_bad_type", source=ep.name)
                continue
            found.append(ext)
            log.info(
                "gateway_extension_discovered",
                source=ep.name,
                routers=len(ext.routers),
                startup_hooks=len(ext.startup_hooks),
                production_enabled=ext.production_enabled,
                public_prefixes=len(ext.public_path_prefixes),
            )
        except Exception:  # noqa: BLE001 - one bad overlay must not break others
            log.error("gateway_extension_load_failed", source=ep.name, exc_info=True)
    _cache = found
    return found


def _extension_allowed(ext: GatewayExtension, *, production: bool) -> bool:
    if production and not ext.production_enabled:
        log.warning(
            "gateway_extension_skipped_in_production",
            source=ext.name,
        )
        return False
    return True


def mount_extension_routers(app: FastAPI, *, production: bool = False) -> None:
    """Include every router contributed by discovered extensions."""
    for ext in discovered_extensions():
        if not _extension_allowed(ext, production=production):
            continue
        for router in ext.routers:
            app.include_router(router)


def extension_public_path_prefixes(*, production: bool = False) -> tuple[str, ...]:
    """All public path prefixes contributed by discovered extensions."""
    prefixes: list[str] = []
    for ext in discovered_extensions():
        if not _extension_allowed(ext, production=production):
            continue
        prefixes.extend(ext.public_path_prefixes)
    return tuple(prefixes)


async def run_extension_startup_hooks(
    app: FastAPI,
    pool: asyncpg.Pool,
    *,
    production: bool = False,
) -> None:
    """Run each extension's startup hooks.

    Dev/dogfood extensions are optional and keep the historical degraded-only
    behavior. A production-enabled extension is part of the runtime contract:
    if its startup hook fails in production, gateway startup must fail closed.
    """
    for ext in discovered_extensions():
        if not _extension_allowed(ext, production=production):
            continue
        for hook in ext.startup_hooks:
            try:
                await hook(app, pool)
            except Exception as exc:  # noqa: BLE001 - optional outside production
                log.exception("gateway_extension_startup_hook_failed", source=ext.name)
                if production:
                    raise RuntimeError(
                        f"gateway extension {ext.name!r} startup hook failed"
                    ) from exc


def reset_for_tests() -> None:
    """Force re-discovery (test isolation only)."""
    global _cache
    _cache = None
