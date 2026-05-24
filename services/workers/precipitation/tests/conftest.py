"""Precipitation tests — reuse the calibration_updater conftest fixtures."""
from __future__ import annotations

import os
import uuid
from collections.abc import AsyncGenerator

import asyncpg
import pytest
import pytest_asyncio

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
