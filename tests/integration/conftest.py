"""Integration-test fixtures shared by top-level end-to-end tests."""
from __future__ import annotations

from services.gateway.tests.conftest import (  # noqa: F401
    app_deps,
    client,
    gateway_pool,
    rate_limiter,
)
