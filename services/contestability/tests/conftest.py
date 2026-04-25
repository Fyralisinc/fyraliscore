"""
Contestability tests — reuse the shared fixtures from
services/workers/calibration_updater/tests/conftest.py.
"""
from __future__ import annotations

from services.workers.calibration_updater.tests.conftest import (  # noqa: F401
    actor_id,
    born_from_event,
    db_pool,
    fresh_db,
    insert_actor,
    insert_model,
    insert_observation,
    make_embedding,
    other_tenant,
    similar_embedding,
    tenant,
    tx_conn,
)
