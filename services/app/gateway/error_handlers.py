from __future__ import annotations

import logging

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

from lib.shared.http_headers import redact_log_mapping


log = logging.getLogger(__name__)


def _request_id(request: Request) -> str | None:
    value = getattr(request.state, "request_id", None)
    return str(value) if value else None


async def unhandled_exception_handler(
    request: Request,
    exc: Exception,
) -> JSONResponse:
    request_id = _request_id(request)
    log.error(
        "gateway_unhandled_exception",
        extra={
            "request_id": request_id,
            "path": request.url.path,
            "error_type": type(exc).__name__,
        },
    )
    body: dict[str, str] = {"error": "internal_server_error"}
    if request_id:
        body["request_id"] = request_id
    response = JSONResponse(status_code=500, content=body)
    if request_id:
        response.headers["X-Request-Id"] = request_id
    return response


async def http_exception_handler(
    request: Request,
    exc: StarletteHTTPException,
) -> JSONResponse:
    request_id = _request_id(request)
    detail = redact_log_mapping({"detail": exc.detail}).get("detail")
    body: dict[str, object] = {"detail": detail}
    if request_id:
        body["request_id"] = request_id
    response = JSONResponse(status_code=exc.status_code, content=body)
    if request_id:
        response.headers["X-Request-Id"] = request_id
    return response


def _validation_issues(exc: RequestValidationError) -> list[dict[str, object]]:
    issues: list[dict[str, object]] = []
    for error in exc.errors()[:20]:
        raw_loc = error.get("loc", ())
        loc = [str(part) for part in raw_loc] if isinstance(raw_loc, tuple) else []
        issues.append(
            {
                "loc": loc,
                "type": str(error.get("type") or "validation_error"),
            }
        )
    return issues


async def request_validation_exception_handler(
    request: Request,
    exc: RequestValidationError,
) -> JSONResponse:
    request_id = _request_id(request)
    body: dict[str, object] = {
        "error": "request_validation_failed",
        "issues": _validation_issues(exc),
    }
    if request_id:
        body["request_id"] = request_id
    response = JSONResponse(status_code=422, content=body)
    if request_id:
        response.headers["X-Request-Id"] = request_id
    return response


def install_safe_error_handlers(app: FastAPI) -> None:
    app.add_exception_handler(StarletteHTTPException, http_exception_handler)
    app.add_exception_handler(
        RequestValidationError,
        request_validation_exception_handler,
    )
    app.add_exception_handler(Exception, unhandled_exception_handler)


__all__ = [
    "http_exception_handler",
    "install_safe_error_handlers",
    "request_validation_exception_handler",
    "unhandled_exception_handler",
]
