from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import asyncpg

from lib.evaluation.company_learning_active_surfaces import (
    validate_active_learning_surfaces_artifact,
)
from scripts.run_company_learning_active_surfaces_db import (
    ARTIFACT_NAME,
    _google_drive_payload,
    run_active_surfaces_experiment,
)
from lib.shared.ids import uuid7
from services.domain.source_identity_bindings import SourceIdentityBindingRepo
from services.ingest.ingestion.core import ingest
from services.ingest.ingestion.handlers.google_drive import (
    handle_google_drive_file,
)
from services.workers.entity_resolver.context import build_context


async def test_active_learning_surfaces_produce_complete_postgres_evidence(
    gateway_pool: asyncpg.Pool,
    tmp_path: Path,
) -> None:
    evidence = await run_active_surfaces_experiment(
        pool=gateway_pool,
        output_dir=tmp_path,
        run_id="pytest-active-surfaces",
        system_version="pytest-system",
    )

    assert evidence.report.status == "observed"
    assert evidence.report.structured_identity.observed_case_count == 6
    assert evidence.report.structured_identity.violating_case_count == 0
    assert evidence.report.source_salience.observed_case_count == 5
    assert evidence.report.source_salience.violating_case_count == 0
    assert evidence.report.source_salience.salience_direction_rate.point_estimate == 1.0
    payload = json.loads((tmp_path / ARTIFACT_NAME).read_text())
    assert validate_active_learning_surfaces_artifact(payload) == evidence


async def test_foreign_only_exact_binding_cannot_authorize_local_observation(
    gateway_pool: asyncpg.Pool,
    tenant_id,
) -> None:
    foreign_tenant_id = uuid7()
    await gateway_pool.execute(
        "INSERT INTO tenants (id) VALUES ($1)",
        foreign_tenant_id,
    )
    payload = _google_drive_payload()
    draft = await handle_google_drive_file(payload, {})
    claim = draft.source_identity_claims[0]
    resource_id = uuid7()
    await gateway_pool.execute(
        """
        INSERT INTO resources (
          id, tenant_id, kind, identity, current_value, metadata
        ) VALUES (
          $1, $2, 'capacity', $3, '{}'::jsonb,
          '{"semantic_kind":"source_object"}'::jsonb
        )
        """,
        resource_id,
        foreign_tenant_id,
        claim.source_surface,
    )
    await SourceIdentityBindingRepo(gateway_pool).bind(
        tenant_id=foreign_tenant_id,
        source_system=claim.source_system,
        source_native_identifier=claim.source_native_identifier,
        source_identity_authority_ref="pytest:foreign-only-binding",
        canonical_ref={"type": "resource", "id": str(resource_id)},
        evidence_refs=("pytest:foreign-only-binding",),
        valid_from=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )

    result = await ingest(
        draft.source_channel,
        payload,
        pool=gateway_pool,
        tenant_id=tenant_id,
        embedder=None,
        enqueue_trigger=False,
    )
    attachment_count = await gateway_pool.fetchval(
        """
        SELECT count(*)
        FROM observation_source_identity_bindings
        WHERE tenant_id=$1 AND observation_id=$2
        """,
        tenant_id,
        result.observation.id,
    )
    local_context = await build_context(
        pool=gateway_pool,
        tenant_id=tenant_id,
        observation_id=result.observation.id,
        phrase=claim.source_surface,
    )
    foreign_context = await build_context(
        pool=gateway_pool,
        tenant_id=foreign_tenant_id,
        observation_id=result.observation.id,
        phrase=claim.source_surface,
    )

    assert attachment_count == 0
    assert local_context.source_identity_binding is None
    assert foreign_context.source_identity_binding is None
