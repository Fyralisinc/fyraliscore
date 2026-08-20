"""Signed connector artifact provenance and deployment admission policy."""

from __future__ import annotations

import base64
import hashlib
import importlib.util
import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import StrEnum
from pathlib import Path
from types import MappingProxyType

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives.asymmetric.ed25519 import (
    Ed25519PrivateKey,
    Ed25519PublicKey,
)

from services.ingest.connector_runtime.definitions import ConnectorCandidate
from services.ingest.source_contract.manifest import ConnectorManifest


ATTESTATION_SCHEMA = "sources.fyralis.io/artifact-attestation/v1"


def manifest_sha256(manifest: ConnectorManifest) -> str:
    payload = json.dumps(
        manifest.model_dump(mode="json", by_alias=True),
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    return hashlib.sha256(payload).hexdigest()


def connector_artifact_sha256(manifest: ConnectorManifest) -> str:
    """Measure every declared implementation module and the exact manifest."""

    module_name, _attribute = manifest.spec.implementation.split(":", 1)
    module_names = tuple(sorted({module_name, *manifest.spec.artifact_modules}))
    modules: list[tuple[str, bytes]] = []
    for measured_module in module_names:
        spec = importlib.util.find_spec(measured_module)
        if spec is None or spec.origin is None:
            raise ValueError(
                f"connector implementation module {measured_module!r} "
                "cannot be measured"
            )
        try:
            modules.append((measured_module, Path(spec.origin).read_bytes()))
        except OSError as exc:
            raise ValueError(
                f"connector implementation module {measured_module!r} cannot be read"
            ) from exc
    manifest_bytes = json.dumps(
        manifest.model_dump(mode="json", by_alias=True),
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    digest = hashlib.sha256()
    digest.update(b"fyralis-in-process-connector-artifact-v2\0")
    digest.update(manifest_bytes)
    digest.update(b"\0")
    for measured_module, module_bytes in modules:
        digest.update(b"\0module\0")
        digest.update(measured_module.encode())
        digest.update(b"\0")
        digest.update(module_bytes)
    return digest.hexdigest()


class DeploymentStatus(StrEnum):
    DISABLED = "disabled"
    ENABLED = "enabled"
    QUARANTINED = "quarantined"
    RETIRED = "retired"


@dataclass(frozen=True)
class ArtifactAttestation:
    connector_id: str
    connector_version: str
    artifact_sha256: str
    manifest_sha256: str
    conformance_fingerprint: str
    signer_key_id: str
    builder_id: str
    source_revision: str
    built_at: datetime
    signature: str
    deployment_status: DeploymentStatus = DeploymentStatus.ENABLED
    quarantine_reason: str | None = None

    def __post_init__(self) -> None:
        for name in (
            "artifact_sha256",
            "manifest_sha256",
            "conformance_fingerprint",
        ):
            value = getattr(self, name)
            if len(value) != 64 or any(
                char not in "0123456789abcdef" for char in value
            ):
                raise ValueError(f"{name} must be a lowercase SHA-256")
        if self.built_at.tzinfo is None:
            raise ValueError("artifact build time must be timezone-aware")
        if (self.deployment_status is DeploymentStatus.QUARANTINED) != (
            self.quarantine_reason is not None
        ):
            raise ValueError("only quarantined artifacts carry a quarantine reason")

    def payload(self) -> bytes:
        value = {
            "schema": ATTESTATION_SCHEMA,
            "connector_id": self.connector_id,
            "connector_version": self.connector_version,
            "artifact_sha256": self.artifact_sha256,
            "manifest_sha256": self.manifest_sha256,
            "conformance_fingerprint": self.conformance_fingerprint,
            "signer_key_id": self.signer_key_id,
            "builder_id": self.builder_id,
            "source_revision": self.source_revision,
            "built_at": self.built_at.astimezone(timezone.utc).isoformat(),
        }
        return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()

    def with_signature(self, private_key: Ed25519PrivateKey) -> "ArtifactAttestation":
        signature = base64.b64encode(private_key.sign(self.payload())).decode()
        return ArtifactAttestation(**{**self.__dict__, "signature": signature})


@dataclass(frozen=True)
class ArtifactDecision:
    admitted: bool
    reason: str


@dataclass(frozen=True)
class ArtifactAdmission:
    candidates: tuple[ConnectorCandidate, ...]
    quarantined: Mapping[str, str]

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "quarantined", MappingProxyType(dict(self.quarantined))
        )


class ArtifactDeploymentPolicy:
    def __init__(
        self,
        trusted_signers: Mapping[str, Ed25519PublicKey],
        *,
        allowed_builders: frozenset[str] = frozenset(),
        require_signed: bool = True,
    ) -> None:
        self._trusted_signers = MappingProxyType(dict(trusted_signers))
        self._allowed_builders = frozenset(allowed_builders)
        self._require_signed = require_signed

    def evaluate(
        self,
        candidate: ConnectorCandidate,
        attestation: ArtifactAttestation | None,
        *,
        measured_artifact_sha256: str | None = None,
    ) -> ArtifactDecision:
        if attestation is None:
            return ArtifactDecision(not self._require_signed, "attestation_missing")
        if attestation.deployment_status is not DeploymentStatus.ENABLED:
            return ArtifactDecision(
                False, f"artifact_{attestation.deployment_status.value}"
            )
        manifest = candidate.manifest
        if attestation.connector_id != manifest.connector_id:
            return ArtifactDecision(False, "connector_id_mismatch")
        if attestation.connector_version != manifest.metadata.version:
            return ArtifactDecision(False, "connector_version_mismatch")
        if measured_artifact_sha256 is None:
            return ArtifactDecision(False, "artifact_measurement_missing")
        if attestation.artifact_sha256 != measured_artifact_sha256:
            return ArtifactDecision(False, "artifact_digest_mismatch")
        if attestation.manifest_sha256 != manifest_sha256(manifest):
            return ArtifactDecision(False, "manifest_digest_mismatch")
        if candidate.conformance_fingerprint is None:
            return ArtifactDecision(False, "conformance_evidence_missing")
        if attestation.conformance_fingerprint != candidate.conformance_fingerprint:
            return ArtifactDecision(False, "conformance_evidence_mismatch")
        if (
            self._allowed_builders
            and attestation.builder_id not in self._allowed_builders
        ):
            return ArtifactDecision(False, "builder_not_allowed")
        public_key = self._trusted_signers.get(attestation.signer_key_id)
        if public_key is None:
            return ArtifactDecision(False, "signer_not_trusted")
        try:
            signature = base64.b64decode(attestation.signature, validate=True)
            public_key.verify(signature, attestation.payload())
        except (InvalidSignature, ValueError):
            return ArtifactDecision(False, "signature_invalid")
        return ArtifactDecision(True, "admitted")

    def admit(
        self,
        candidates: Sequence[ConnectorCandidate],
        attestations: Mapping[tuple[str, str], ArtifactAttestation],
        *,
        measured_artifacts: Mapping[tuple[str, str], str] | None = None,
    ) -> ArtifactAdmission:
        admitted: list[ConnectorCandidate] = []
        quarantined: dict[str, str] = {}
        for candidate in candidates:
            key = (
                candidate.manifest.connector_id,
                candidate.manifest.metadata.version,
            )
            decision = self.evaluate(
                candidate,
                attestations.get(key),
                measured_artifact_sha256=(measured_artifacts or {}).get(key),
            )
            if decision.admitted:
                admitted.append(candidate)
            else:
                quarantined[candidate.manifest.connector_id] = decision.reason
        return ArtifactAdmission(tuple(admitted), quarantined)


__all__ = [
    "ATTESTATION_SCHEMA",
    "ArtifactAdmission",
    "ArtifactAttestation",
    "ArtifactDecision",
    "ArtifactDeploymentPolicy",
    "DeploymentStatus",
    "connector_artifact_sha256",
    "manifest_sha256",
]
