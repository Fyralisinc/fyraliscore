"""services/app/gateway — HTTP/WebSocket entry point (ARCHITECTURE §13).

Exports:
- `build_app()` — FastAPI app factory (for uvicorn + tests).
- `app` — default module-level app for `uvicorn services.app.gateway:app`.
"""
from services.app.gateway.main import app, build_app  # noqa: F401

__all__ = ["app", "build_app"]
