"""tests/integration/conftest.py — re-exports gateway test fixtures so
the cross-service integration tests can construct an authed httpx
client. Same pattern as services/demo/tests/conftest.py.
"""
from __future__ import annotations

from services.gateway.tests.conftest import (  # noqa: F401
    SLACK_TEST_SECRET,
    _DeterministicEmbedder,
    app_deps,
    build_slack_payload,
    client,
    gateway_pool,
    rate_limiter,
    seeded_actor,
    seeded_actor_b,
    sign_slack,
    tenant_id,
    tenant_id_b,
    valid_session,
    valid_session_b,
)
