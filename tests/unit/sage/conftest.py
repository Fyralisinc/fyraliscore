"""Shared fixtures for SAGE integration-flavored unit tests.

The SAGE tests use the gateway test database fixtures. Re-export the
fixture functions directly here; pytest 9 rejects nested ``pytest_plugins``
because they affect the full suite at collection time.
"""

from services.app.gateway.tests.conftest import (  # noqa: F401
    _deterministic_embedder_cls_fixture,
    app_deps,
    client,
    gateway_pool,
    rate_limiter,
    seeded_actor,
    seeded_actor_b,
    tenant_id,
    tenant_id_b,
    valid_session,
    valid_session_b,
)
