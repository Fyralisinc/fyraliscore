from __future__ import annotations

from types import SimpleNamespace

import pytest
from fastapi import HTTPException

from lib.shared.errors import DependencyUnavailableError
from services.product import error_contract
from services.product.decision_deltas import router as decision_delta_router
from services.product.forecasts import router as forecasts_router
from services.product.resolution_threads import router as resolution_threads_router


def test_product_data_plane_unavailable_is_explicitly_degraded() -> None:
    exc = error_contract.product_data_plane_unavailable()

    assert exc.status_code == 503
    assert exc.detail == {
        "error": "service_unavailable",
        "degraded": True,
        "degraded_reasons": ["product_data_plane_unavailable"],
        "dependency": "product_data_plane",
    }


def test_dependency_unavailable_detail_is_bounded_and_degraded() -> None:
    exc = DependencyUnavailableError(
        "rendering",
        "/conversation-turn",
        url="https://renderer.internal/render?token=secret",
    )

    assert error_contract.dependency_unavailable_detail(exc) == {
        "error": "dependency_unavailable",
        "degraded": True,
        "degraded_reasons": ["rendering_unavailable"],
        "dependency": "rendering",
        "operation": "conversation_turn",
    }


@pytest.mark.parametrize(
    "pool_getter",
    [
        decision_delta_router._pool,
        forecasts_router._pool,
        resolution_threads_router._pool,
    ],
)
def test_product_pool_helpers_report_degraded_data_plane(pool_getter) -> None:
    request = SimpleNamespace(
        app=SimpleNamespace(state=SimpleNamespace(deps=None)),
    )

    with pytest.raises(HTTPException) as exc_info:
        pool_getter(request)

    assert exc_info.value.status_code == 503
    assert exc_info.value.detail["degraded"] is True
    assert exc_info.value.detail["degraded_reasons"] == [
        "product_data_plane_unavailable",
    ]
