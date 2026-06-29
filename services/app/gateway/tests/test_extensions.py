from __future__ import annotations

from typing import Any

import pytest
from fastapi import FastAPI

import services.app.gateway.extensions as gateway_extensions


@pytest.mark.asyncio
async def test_extension_startup_hooks_fail_closed_in_production(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def failing_hook(app: FastAPI, pool: Any) -> None:
        del app, pool
        raise RuntimeError("extension config missing")

    extension = gateway_extensions.GatewayExtension(
        name="customer-overlay",
        startup_hooks=[failing_hook],
        production_enabled=True,
    )
    monkeypatch.setattr(
        gateway_extensions,
        "discovered_extensions",
        lambda: [extension],
    )

    with pytest.raises(RuntimeError, match="customer-overlay"):
        await gateway_extensions.run_extension_startup_hooks(
            FastAPI(),
            object(),  # type: ignore[arg-type]
            production=True,
        )


@pytest.mark.asyncio
async def test_extension_startup_hooks_degrade_outside_production(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def failing_hook(app: FastAPI, pool: Any) -> None:
        del app, pool
        raise RuntimeError("extension config missing")

    extension = gateway_extensions.GatewayExtension(
        name="demo-overlay",
        startup_hooks=[failing_hook],
    )
    monkeypatch.setattr(
        gateway_extensions,
        "discovered_extensions",
        lambda: [extension],
    )

    await gateway_extensions.run_extension_startup_hooks(
        FastAPI(),
        object(),  # type: ignore[arg-type]
        production=False,
    )
