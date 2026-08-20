from __future__ import annotations

import json
from dataclasses import replace

import pytest

from services.ingest.connector_runtime.release_evidence import load_release_evidence
from services.ingest.connector_runtime.tests.helpers import make_candidate


def test_release_evidence_is_independent_and_matches_candidates(tmp_path) -> None:
    candidate, _ = make_candidate()
    candidate = replace(candidate, conformance_fingerprint="a" * 64)
    path = tmp_path / "release-evidence.json"
    path.write_text(
        json.dumps(
            {
                "schema": "sources.fyralis.io/release-evidence/v2",
                "connectors": [
                    {
                        "connectorId": candidate.manifest.connector_id,
                        "connectorVersion": candidate.manifest.metadata.version,
                        "structuralFingerprint": "a" * 64,
                    }
                ],
            }
        )
    )

    catalog = load_release_evidence(path)
    catalog.validate((candidate,))

    with pytest.raises(ValueError, match="mismatch"):
        catalog.validate((replace(candidate, conformance_fingerprint="b" * 64),))


def test_release_evidence_rejects_stale_entries(tmp_path) -> None:
    candidate, _ = make_candidate()
    candidate = replace(candidate, conformance_fingerprint="b" * 64)
    path = tmp_path / "release-evidence.json"
    path.write_text(
        json.dumps(
            {
                "schema": "sources.fyralis.io/release-evidence/v2",
                "connectors": [
                    {
                        "connectorId": candidate.manifest.connector_id,
                        "connectorVersion": candidate.manifest.metadata.version,
                        "structuralFingerprint": "b" * 64,
                    },
                    {
                        "connectorId": "fyralis/stale",
                        "connectorVersion": "1.0.0",
                        "structuralFingerprint": "a" * 64,
                    },
                ],
            }
        )
    )

    with pytest.raises(ValueError, match="stale"):
        load_release_evidence(path).validate((candidate,))
