"""Checked-in connector conformance evidence used for release admission."""

from __future__ import annotations

import json
import re
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType

from services.ingest.connector_runtime.definitions import ConnectorCandidate


RELEASE_EVIDENCE_SCHEMA = "sources.fyralis.io/release-evidence/v1"


@dataclass(frozen=True)
class ConnectorReleaseEvidence:
    connector_id: str
    connector_version: str
    structural_fingerprint: str

    def __post_init__(self) -> None:
        if re.fullmatch(r"[0-9a-f]{64}", self.structural_fingerprint) is None:
            raise ValueError("release evidence fingerprint must be a lowercase SHA-256")

    @property
    def key(self) -> tuple[str, str]:
        return self.connector_id, self.connector_version


class ReleaseEvidenceCatalog:
    def __init__(self, records: Sequence[ConnectorReleaseEvidence]) -> None:
        by_key = {record.key: record for record in records}
        if len(by_key) != len(records):
            raise ValueError("release evidence contains duplicate connector versions")
        self._by_key = MappingProxyType(by_key)

    @property
    def approved_fingerprints(self) -> frozenset[str]:
        return frozenset(
            record.structural_fingerprint for record in self._by_key.values()
        )

    def require(
        self, connector_id: str, connector_version: str
    ) -> ConnectorReleaseEvidence:
        try:
            return self._by_key[(connector_id, connector_version)]
        except KeyError as exc:
            raise ValueError(
                f"release evidence is missing for {connector_id}@{connector_version}"
            ) from exc

    def validate(self, candidates: Sequence[ConnectorCandidate]) -> None:
        candidate_keys: set[tuple[str, str]] = set()
        for candidate in candidates:
            manifest = candidate.manifest
            key = (manifest.connector_id, manifest.metadata.version)
            candidate_keys.add(key)
            expected = self.require(*key).structural_fingerprint
            if candidate.conformance_fingerprint != expected:
                raise ValueError(
                    f"release evidence mismatch for {key[0]}@{key[1]}: "
                    f"expected {expected}, got {candidate.conformance_fingerprint}"
                )
        stale = set(self._by_key) - candidate_keys
        if stale:
            formatted = ", ".join(f"{item[0]}@{item[1]}" for item in sorted(stale))
            raise ValueError(f"release evidence contains stale entries: {formatted}")


def load_release_evidence(path: str | Path) -> ReleaseEvidenceCatalog:
    evidence_path = Path(path)
    try:
        payload = json.loads(evidence_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError(f"release evidence {evidence_path} is not valid JSON") from exc
    if (
        not isinstance(payload, dict)
        or payload.get("schema") != RELEASE_EVIDENCE_SCHEMA
    ):
        raise ValueError(f"release evidence {evidence_path} has an unsupported schema")
    raw_records = payload.get("connectors")
    if not isinstance(raw_records, list):
        raise ValueError(f"release evidence {evidence_path} must contain connectors")
    records: list[ConnectorReleaseEvidence] = []
    for item in raw_records:
        if not isinstance(item, dict) or set(item) != {
            "connectorId",
            "connectorVersion",
            "structuralFingerprint",
        }:
            raise ValueError(
                f"release evidence {evidence_path} contains an invalid record"
            )
        records.append(
            ConnectorReleaseEvidence(
                connector_id=str(item["connectorId"]),
                connector_version=str(item["connectorVersion"]),
                structural_fingerprint=str(item["structuralFingerprint"]),
            )
        )
    return ReleaseEvidenceCatalog(records)


__all__ = [
    "ConnectorReleaseEvidence",
    "RELEASE_EVIDENCE_SCHEMA",
    "ReleaseEvidenceCatalog",
    "load_release_evidence",
]
