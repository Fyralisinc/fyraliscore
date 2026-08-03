from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone

from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from services.ingest.connector_runtime.artifacts import (
    ArtifactAttestation,
    ArtifactDeploymentPolicy,
    manifest_sha256,
)
from services.ingest.connector_runtime.tests.helpers import make_candidate


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
    )

    assert admission.candidates == (candidate,)
    assert not admission.quarantined


def test_tampered_or_missing_artifacts_are_quarantined() -> None:
    candidate = _candidate()
    key = Ed25519PrivateKey.generate()
    artifact = replace(_attestation(candidate, key), artifact_sha256="c" * 64)
    policy = ArtifactDeploymentPolicy({"release-2026": key.public_key()})

    decision = policy.evaluate(candidate, artifact)
    missing = policy.admit((candidate,), {})

    assert not decision.admitted
    assert decision.reason == "signature_invalid"
    assert missing.quarantined == {candidate.manifest.connector_id: "attestation_missing"}
