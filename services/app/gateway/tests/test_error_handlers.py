from __future__ import annotations

import logging
from uuid import UUID

import httpx
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from services.app.gateway.error_handlers import install_safe_error_handlers
from services.app.gateway.middleware import RequestContextMiddleware


class _ValidationBody(BaseModel):
    count: int
    name: str


async def test_unhandled_exception_response_is_safe() -> None:
    app = FastAPI()
    app.add_middleware(RequestContextMiddleware)
    install_safe_error_handlers(app)

    @app.get("/boom")
    async def boom() -> None:
        raise RuntimeError("password=secret traceback raw sql SELECT * FROM users")

    transport = httpx.ASGITransport(app=app, raise_app_exceptions=False)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://testserver",
    ) as client:
        response = await client.get("/boom")

    assert response.status_code == 500
    body = response.json()
    assert body["error"] == "internal_server_error"
    request_id = body["request_id"]
    assert response.headers["X-Request-Id"] == request_id
    UUID(request_id)

    rendered = response.text.lower()
    assert "password" not in rendered
    assert "secret" not in rendered
    assert "traceback" not in rendered
    assert "select * from users" not in rendered


async def test_unhandled_exception_does_not_log_fake_tokens(
    caplog,
) -> None:
    app = FastAPI()
    app.add_middleware(RequestContextMiddleware)
    install_safe_error_handlers(app)
    fake_bearer = "sk-fyralis-secret-response-log-probe"
    fake_refresh = "refresh_token=fyralis-refresh-secret-probe"

    @app.get("/boom")
    async def boom() -> None:
        raise RuntimeError(
            "provider failed with "
            f"Authorization=Bearer {fake_bearer} and {fake_refresh}"
        )

    caplog.set_level(logging.ERROR, logger="services.app.gateway.error_handlers")
    transport = httpx.ASGITransport(app=app, raise_app_exceptions=False)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://testserver",
    ) as client:
        response = await client.get("/boom")

    assert response.status_code == 500
    rendered_response = response.text
    assert fake_bearer not in rendered_response
    assert "fyralis-refresh-secret-probe" not in rendered_response

    rendered_logs = "\n".join(record.getMessage() for record in caplog.records)
    assert "gateway_unhandled_exception" in rendered_logs
    assert fake_bearer not in rendered_logs
    assert "fyralis-refresh-secret-probe" not in rendered_logs
    assert all(record.exc_info is None for record in caplog.records)


async def test_http_exception_detail_is_redacted() -> None:
    app = FastAPI()
    app.add_middleware(RequestContextMiddleware)
    install_safe_error_handlers(app)

    @app.get("/bad")
    async def bad() -> None:
        raise HTTPException(
            status_code=400,
            detail={
                "prompt": "raw customer prompt",
                "message": (
                    "provider failed for alice@example.com with "
                    "password=hunter2"
                ),
            },
        )

    transport = httpx.ASGITransport(app=app, raise_app_exceptions=False)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://testserver",
    ) as client:
        response = await client.get("/bad")

    assert response.status_code == 400
    body = response.json()
    assert body["detail"]["prompt"] == "[redacted]"
    rendered = response.text.lower()
    assert "raw customer prompt" not in rendered
    assert "alice@example.com" not in rendered
    assert "hunter2" not in rendered


async def test_request_validation_error_does_not_echo_raw_input() -> None:
    app = FastAPI()
    app.add_middleware(RequestContextMiddleware)
    install_safe_error_handlers(app)

    @app.post("/validate")
    async def validate(body: _ValidationBody) -> dict[str, object]:
        return body.model_dump()

    transport = httpx.ASGITransport(app=app, raise_app_exceptions=False)
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://testserver",
    ) as client:
        response = await client.post(
            "/validate",
            json={
                "count": "not-an-int password=hunter2",
                "name": "alice@example.com",
                "prompt": "raw customer prompt",
            },
        )

    assert response.status_code == 422
    body = response.json()
    assert body["error"] == "request_validation_failed"
    assert body["issues"] == [
        {"loc": ["body", "count"], "type": "int_parsing"}
    ]
    request_id = body["request_id"]
    assert response.headers["X-Request-Id"] == request_id
    UUID(request_id)

    rendered = response.text.lower()
    assert "hunter2" not in rendered
    assert "alice@example.com" not in rendered
    assert "raw customer prompt" not in rendered
    assert "not-an-int" not in rendered
