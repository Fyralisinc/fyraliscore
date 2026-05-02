"""services/demo/tests/conftest.py — re-exports gateway test fixtures
so demo router tests can construct an authed httpx client.

Most demo unit tests only need `fresh_db`; the gateway-fixture re-export
is used by the API-level tests that exercise /v1/demo/* endpoints.
"""
from __future__ import annotations

# Re-export the gateway fixtures (client, valid_session, gateway_pool,
# tenant_id, seeded_actor, ...) so router tests can authenticate via
# the same bootstrap flow that the recommendation tests use.
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
