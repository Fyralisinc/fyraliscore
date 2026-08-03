#!/usr/bin/env python3
"""Fail CI when connector manifests, evidence, factories, or defaults drift."""

from __future__ import annotations

import asyncio
from uuid import UUID

from services.ingest.connector_platform.catalog import CONNECTOR_CATALOG
from services.ingest.connector_platform.pilots import (
    build_runtime_candidates,
    default_migrated_routing_policy,
    release_evidence_catalog,
)
from services.ingest.connector_runtime.artifacts import connector_artifact_sha256
from services.ingest.connector_runtime.policy import ExecutionMode, RouteRequest
from services.ingest.connectors.behavior import run_pilot_behavioral_conformance


def main() -> int:
    candidates = build_runtime_candidates()
    expected_ids = {entry.connector_id for entry in CONNECTOR_CATALOG}
    actual_ids = {candidate.manifest.connector_id for candidate in candidates}
    if actual_ids != expected_ids or len(candidates) != len(CONNECTOR_CATALOG):
        raise SystemExit("connector candidate catalog does not match source inventory")
    release_evidence_catalog().validate(candidates)
    reports = asyncio.run(run_pilot_behavioral_conformance())
    for connector_id, report in reports.items():
        if not report.passed:
            raise SystemExit(f"behavioral conformance failed for {connector_id}")
        evidence = release_evidence_catalog().require(
            connector_id, report.connector_version
        )
        if report.fingerprint != evidence.behavioral_fingerprint:
            raise SystemExit(
                f"behavioral release evidence drifted for {connector_id}: "
                f"expected {evidence.behavioral_fingerprint}, got {report.fingerprint}"
            )
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
        "structural + behavioral evidence, measured modules, legacy-safe defaults"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
