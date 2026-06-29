from __future__ import annotations

import pytest
from fastapi import Request, Response
from starlette.applications import Starlette

from services.app.gateway.middleware import RequestContextMiddleware


class RouteFailure(RuntimeError):
    pass


class RaisingLogger:
    def error(self, *args: object, **kwargs: object) -> None:
        raise RuntimeError("logger failed")


@pytest.mark.asyncio
async def test_request_context_preserves_original_exception_when_logging_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import services.app.gateway.middleware as middleware

    async def receive() -> dict[str, object]:
        return {"type": "http.request", "body": b"", "more_body": False}

    async def call_next(request: Request) -> Response:
        raise RouteFailure("route failed")

    scope = {
        "type": "http",
        "method": "GET",
        "path": "/boom",
        "raw_path": b"/boom",
        "query_string": b"",
        "headers": [],
        "client": ("127.0.0.1", 12345),
        "server": ("testserver", 80),
        "scheme": "http",
        "root_path": "",
    }
    request = Request(scope, receive)
    middleware_instance = RequestContextMiddleware(Starlette())
    monkeypatch.setattr(middleware, "log", RaisingLogger())

    with pytest.raises(RouteFailure, match="route failed"):
        await middleware_instance.dispatch(request, call_next)
