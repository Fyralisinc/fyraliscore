"""Projection-first retrieval helpers.

Compatibility wrappers over the domain projection repository. Retrieval should
consume projections; it should not own projection storage or Model hydration.
"""
from __future__ import annotations

from uuid import UUID

import asyncpg

from services.domain.projections.repo import ProjectionContext, ProjectionRepo


_PROJECTIONS = ProjectionRepo()


async def load_projection_context(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    projection_name: str,
    subject_key: str,
    projection_version: str = "v1",
    include_models: bool = True,
) -> ProjectionContext | None:
    return await _PROJECTIONS.get_context(
        conn,
        tenant_id=tenant_id,
        projection_name=projection_name,
        projection_version=projection_version,
        subject_key=subject_key,
        include_models=include_models,
    )


async def load_constraint_context(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    subject_key: str,
    projection_version: str = "v1",
    include_models: bool = True,
) -> ProjectionContext | None:
    return await load_projection_context(
        conn,
        tenant_id=tenant_id,
        projection_name="constraints",
        projection_version=projection_version,
        subject_key=subject_key,
        include_models=include_models,
    )


async def load_resource_context(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    subject_key: str,
    projection_version: str = "v1",
    include_models: bool = True,
) -> ProjectionContext | None:
    return await load_projection_context(
        conn,
        tenant_id=tenant_id,
        projection_name="resources",
        projection_version=projection_version,
        subject_key=subject_key,
        include_models=include_models,
    )

