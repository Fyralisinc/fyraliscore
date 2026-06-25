"""Bounded product-facing error response helpers."""
from __future__ import annotations

import re
from typing import Any

from fastapi import HTTPException

from lib.shared.errors import DependencyUnavailableError


_SAFE_TOKEN_RE = re.compile(r"[^a-z0-9_]+")

PRODUCT_DATA_PLANE_DEGRADED_REASON = "product_data_plane_unavailable"


def _safe_token(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip().lower().replace("-", "_").replace("/", "_")
    text = _SAFE_TOKEN_RE.sub("_", text).strip("_")
    return text[:64] or None


def degraded_error_detail(
    *,
    error: str,
    reason: str,
    dependency: Any | None = None,
    operation: Any | None = None,
) -> dict[str, object]:
    detail: dict[str, object] = {
        "error": _safe_token(error) or "service_unavailable",
        "degraded": True,
        "degraded_reasons": [_safe_token(reason) or "dependency_unavailable"],
    }
    safe_dependency = _safe_token(dependency)
    safe_operation = _safe_token(operation)
    if safe_dependency:
        detail["dependency"] = safe_dependency
    if safe_operation:
        detail["operation"] = safe_operation
    return detail


def product_data_plane_unavailable() -> HTTPException:
    return HTTPException(
        status_code=503,
        detail=degraded_error_detail(
            error="service_unavailable",
            reason=PRODUCT_DATA_PLANE_DEGRADED_REASON,
            dependency="product_data_plane",
        ),
    )


def dependency_unavailable_detail(
    exc: DependencyUnavailableError,
) -> dict[str, object]:
    dependency = exc.context.get("dependency")
    operation = exc.context.get("operation")
    safe_dependency = _safe_token(dependency) or "dependency"
    return degraded_error_detail(
        error=exc.code,
        reason=f"{safe_dependency}_unavailable",
        dependency=safe_dependency,
        operation=operation,
    )


__all__ = [
    "PRODUCT_DATA_PLANE_DEGRADED_REASON",
    "degraded_error_detail",
    "dependency_unavailable_detail",
    "product_data_plane_unavailable",
]
