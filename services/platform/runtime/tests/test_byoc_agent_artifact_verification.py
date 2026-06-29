from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from services.platform.runtime.byoc_agent_apply_plan import build_apply_revision_plan
from services.platform.runtime.byoc_agent_artifact_verification import (
    build_artifact_verification_evidence,
    validate_artifact_verification_contract,
)
from services.platform.runtime.byoc_agent_control_plane import (
    ByocAgentDesiredStateResponse,
)
from services.platform.runtime.byoc_bootstrap_bundle import load_byoc_bootstrap_bundle
from services.platform.runtime.byoc_contract import load_byoc_manifest


ROOT = Path(__file__).resolve().parents[4]
MANIFEST = load_byoc_manifest(ROOT / "deploy/byoc/dataplane.example.yaml")
BUNDLE = load_byoc_bootstrap_bundle(ROOT / "deploy/byoc/bootstrap-bundle.example.yaml")
NEXT_BUNDLE = load_byoc_bootstrap_bundle(
    ROOT / "deploy/byoc/bootstrap-bundle.next.example.yaml"
)


def _desired_state() -> ByocAgentDesiredStateResponse:
    return ByocAgentDesiredStateResponse(
        schema_version="fyralis.byoc.agent.desired_state.v1",
        status="accepted",
        deployment_id=MANIFEST.deployment_id,
        customer_id=MANIFEST.customer_id,
        agent_id="agt_artifact001",
        current_revision=MANIFEST.artifact_revision,
        desired_revision=NEXT_BUNDLE.artifact_revision,
        rollout_action="apply_revision",
        config_epoch=8,
        config_scope="metadata_only",
        heartbeat_interval_seconds=MANIFEST.connectivity.heartbeat_interval_seconds,
        poll_after_seconds=MANIFEST.connectivity.agent_poll_interval_seconds,
        telemetry_contract=MANIFEST.telemetry.contract,
        evidence_package_required=False,
        accepted_at=datetime(2026, 6, 26, 12, 0, tzinfo=UTC),
        stored_scope="sanitized_agent_metadata_only",
    )


def test_artifact_verification_evidence_maps_desired_revision_to_bundle() -> None:
    plan = build_apply_revision_plan(MANIFEST, _desired_state())
    evidence = build_artifact_verification_evidence(
        plan,
        NEXT_BUNDLE,
        MANIFEST,
        verify_local_files=True,
        repo_root=ROOT,
    )
    serialized = json.dumps(evidence.model_dump(mode="json"), sort_keys=True)

    assert validate_artifact_verification_contract(
        evidence,
        plan=plan,
        bundle=NEXT_BUNDLE,
        manifest=MANIFEST,
        verify_local_files=True,
        repo_root=ROOT,
    ) == []
    assert evidence.schema_version == (
        "fyralis.byoc.agent.artifact_verification_evidence.v1"
    )
    assert evidence.desired_revision == NEXT_BUNDLE.artifact_revision
    assert evidence.bundle_artifact_revision == NEXT_BUNDLE.artifact_revision
    assert evidence.artifact_count == len(NEXT_BUNDLE.artifacts)
    assert evidence.digest_pinned_artifact_count == len(NEXT_BUNDLE.artifacts)
    assert evidence.local_digest_checked_count == 1
    assert {artifact.role for artifact in evidence.artifacts} >= {
        "gateway_image",
        "worker_image",
        "data_plane_agent_image",
        "helm_chart",
        "iam_bootstrap_template",
        "source_sbom",
        "image_sbom",
    }
    assert "://" not in serialized
    assert "signature" not in serialized.lower()
    assert "sigstore" not in serialized.lower()
    assert "bundle_ref" not in serialized.lower()
    assert "payload" not in serialized.lower()
    assert MANIFEST.connectivity.control_plane_url not in serialized


def test_artifact_verification_rejects_bundle_revision_mismatch() -> None:
    plan = build_apply_revision_plan(MANIFEST, _desired_state())
    evidence = build_artifact_verification_evidence(plan, BUNDLE, MANIFEST)

    assert {
        violation.code
        for violation in validate_artifact_verification_contract(
            evidence,
            plan=plan,
            bundle=BUNDLE,
            manifest=MANIFEST,
        )
    } >= {"desired_revision_mismatch"}


def test_artifact_verification_rejects_digest_count_drift() -> None:
    plan = build_apply_revision_plan(MANIFEST, _desired_state())
    evidence = build_artifact_verification_evidence(
        plan,
        NEXT_BUNDLE,
        MANIFEST,
    ).model_copy(update={"digest_pinned_artifact_count": 0})

    assert {
        violation.code
        for violation in validate_artifact_verification_contract(
            evidence,
            plan=plan,
            bundle=NEXT_BUNDLE,
            manifest=MANIFEST,
        )
    } >= {"digest_pin_count_mismatch"}
