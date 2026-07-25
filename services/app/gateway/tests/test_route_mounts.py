from __future__ import annotations

import pytest
from fastapi import FastAPI

import services.app.gateway.route_mounts as route_mounts
from services.ingest.source_contract.catalog import CANONICAL_SOURCE_IDS


def test_native_connect_routers_are_mounted() -> None:
    app = FastAPI()

    route_mounts._mount_native_connect_routers(app, emit_mount_logs=False)

    paths = {route.path for route in app.routes}
    for source in CANONICAL_SOURCE_IDS:
        assert f"/integrations/{source}/connect/preflight" in paths
        assert f"/integrations/{source}/connect/finalize" in paths


def test_native_connect_router_mount_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    real_resolve = route_mounts.resolve_callable_reference

    def _missing_github(binding: str):
        if binding == "services.ingest.integrations.github.oauth:router":
            raise ImportError("missing github module")
        return real_resolve(binding)

    monkeypatch.setattr(
        route_mounts,
        "resolve_callable_reference",
        _missing_github,
    )

    with pytest.raises(RuntimeError, match="github.*is unavailable"):
        route_mounts._mount_native_connect_routers(
            FastAPI(),
            emit_mount_logs=False,
        )


def test_native_connect_router_requires_router_attribute(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        route_mounts,
        "SOURCE_DEFINITIONS",
        (route_mounts.SOURCE_DEFINITIONS[0],),
    )
    monkeypatch.setattr(
        route_mounts,
        "resolve_callable_reference",
        lambda _binding: object(),
    )

    with pytest.raises(RuntimeError, match="slack.*did not resolve"):
        route_mounts._mount_native_connect_routers(
            FastAPI(),
            emit_mount_logs=False,
        )


def test_gateway_mount_fails_on_invalid_runtime_contract(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def _invalid_bindings() -> None:
        raise RuntimeError("source binding missing")

    monkeypatch.setattr(
        route_mounts,
        "validate_runtime_bindings",
        _invalid_bindings,
    )

    with pytest.raises(RuntimeError, match="source binding missing"):
        route_mounts.mount_gateway_routes(
            FastAPI(),
            emit_mount_logs=False,
        )
