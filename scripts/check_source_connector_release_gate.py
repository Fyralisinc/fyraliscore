#!/usr/bin/env python3
"""Fail CI when connector manifests, evidence, factories, or defaults drift."""

from __future__ import annotations

from uuid import UUID

from services.ingest.connector_platform.catalog import CONNECTOR_CATALOG
from services.ingest.connector_platform.pilots import (
    build_runtime_candidates,
    default_migrated_routing_policy,
    release_evidence_catalog,
)
from services.ingest.connector_runtime.artifacts import connector_artifact_sha256
from services.ingest.connector_runtime.policy import ExecutionMode, RouteRequest


def main() -> int:
    candidates = build_runtime_candidates()
    expected_ids = {entry.connector_id for entry in CONNECTOR_CATALOG}
    actual_ids = {candidate.manifest.connector_id for candidate in candidates}
    if actual_ids != expected_ids or len(candidates) != len(CONNECTOR_CATALOG):
        raise SystemExit("connector candidate catalog does not match source inventory")
    release_evidence_catalog().validate(candidates)
    policy = default_migrated_routing_policy()
    for candidate in candidates:
        manifest = candidate.manifest
        connector_artifact_sha256(manifest)
        decision = policy.resolve(
            RouteRequest(
                tenant_id=UUID(int=0),
                connector_id=manifest.connector_id,
                source=manifest.source,
                capability=manifest.capability_refs[0].id,
            )
        )
        if decision.mode is not ExecutionMode.LEGACY:
            raise SystemExit(
                f"{manifest.connector_id} is connector-authoritative by default"
            )
    print(
        f"source connector release gate passed: {len(candidates)} candidates, "
        "independent evidence, measured modules, legacy-safe defaults"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
