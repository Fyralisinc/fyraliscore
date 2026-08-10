from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone

from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from services.ingest.connector_runtime.artifacts import (
    ArtifactAttestation,
    ArtifactDeploymentPolicy,
    connector_artifact_sha256,
    manifest_sha256,
)
from services.ingest.connector_runtime.tests.helpers import (
    make_candidate,
    make_manifest,
)


def _candidate():
    candidate, _ = make_candidate()
    return replace(candidate, conformance_fingerprint="a" * 64)


def _attestation(candidate, key):
    manifest = candidate.manifest
    unsigned = ArtifactAttestation(
        connector_id=manifest.connector_id,
        connector_version=manifest.metadata.version,
        artifact_sha256="b" * 64,
        manifest_sha256=manifest_sha256(manifest),
        conformance_fingerprint=candidate.conformance_fingerprint,
        signer_key_id="release-2026",
        builder_id="fyralis-ci",
        source_revision="deadbeef",
        built_at=datetime(2026, 8, 3, tzinfo=timezone.utc),
        signature="pending",
    )
    return unsigned.with_signature(key)


def test_valid_signed_artifact_is_admitted() -> None:
    candidate = _candidate()
    key = Ed25519PrivateKey.generate()
    artifact = _attestation(candidate, key)
    policy = ArtifactDeploymentPolicy(
        {"release-2026": key.public_key()},
        allowed_builders=frozenset({"fyralis-ci"}),
    )

    admission = policy.admit(
        (candidate,),
        {(artifact.connector_id, artifact.connector_version): artifact},
        measured_artifacts={
            (
                artifact.connector_id,
                artifact.connector_version,
            ): artifact.artifact_sha256
        },
    )

    assert admission.candidates == (candidate,)
    assert not admission.quarantined


def test_tampered_or_missing_artifacts_are_quarantined() -> None:
    candidate = _candidate()
    key = Ed25519PrivateKey.generate()
    artifact = replace(_attestation(candidate, key), artifact_sha256="c" * 64)
    policy = ArtifactDeploymentPolicy({"release-2026": key.public_key()})

    decision = policy.evaluate(
        candidate,
        artifact,
        measured_artifact_sha256=artifact.artifact_sha256,
    )
    missing = policy.admit((candidate,), {})

    assert not decision.admitted
    assert decision.reason == "signature_invalid"
    assert missing.quarantined == {
        candidate.manifest.connector_id: "attestation_missing"
    }


def test_signed_attestation_must_match_the_running_artifact() -> None:
    candidate = _candidate()
    key = Ed25519PrivateKey.generate()
    artifact = _attestation(candidate, key)
    policy = ArtifactDeploymentPolicy({"release-2026": key.public_key()})

    decision = policy.evaluate(
        candidate,
        artifact,
        measured_artifact_sha256="c" * 64,
    )

    assert not decision.admitted
    assert decision.reason == "artifact_digest_mismatch"


def test_artifact_measurement_binds_running_module_and_exact_manifest() -> None:
    manifest = make_manifest().model_copy(
        update={
            "spec": make_manifest().spec.model_copy(
                update={
                    "implementation": (
                        "services.ingest.connector_runtime.tests.helpers:"
                        "build_example_connector"
                    )
                }
            )
        }
    )
    changed = manifest.model_copy(
        update={"metadata": manifest.metadata.model_copy(update={"version": "1.0.1"})}
    )

    assert connector_artifact_sha256(manifest) == connector_artifact_sha256(manifest)
    assert connector_artifact_sha256(manifest) != connector_artifact_sha256(changed)
