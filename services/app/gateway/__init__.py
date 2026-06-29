"""services/app/gateway — HTTP/WebSocket entry point (ARCHITECTURE §13).

Exports:
- `build_app()` — FastAPI app factory (for uvicorn + tests).
- `app` — default module-level app for `uvicorn services.app.gateway:app`.
"""
from __future__ import annotations

from typing import Any


def build_app(*args: Any, **kwargs: Any) -> Any:
    from services.app.gateway.main import build_app as _build_app

    return _build_app(*args, **kwargs)


def __getattr__(name: str) -> Any:
    if name == "app":
        from services.app.gateway.main import app as _app

        return _app
    raise AttributeError(name)

__all__ = ["app", "build_app"]
