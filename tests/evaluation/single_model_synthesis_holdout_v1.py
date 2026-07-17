"""Frozen untouched corpus for strict single-Model synthesis."""

from __future__ import annotations

import hashlib
import json

SUBJECTS_V1 = ("quartz", "raven", "solstice")
FACETS_V1 = {
    subject: tuple(f"{subject}_signal_{index}" for index in range(1, 7))
    for subject in SUBJECTS_V1
}
BATCHES_V1 = tuple(
    tuple((subject, facet) for facet in FACETS_V1[subject][offset:offset + 3])
    for subject in SUBJECTS_V1
    for offset in (0, 3)
)
MANIFEST_V1 = {
    "schema_version": "company-model-synthesis-manifest-v1",
    "experiment_id": "single-model-synthesis-holdout-v1",
    "hidden_patterns": [
        {"thesis_id": subject, "required_facets": list(FACETS_V1[subject])}
        for subject in SUBJECTS_V1
    ],
}


def _digest(value: object) -> str:
    payload = json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(payload).hexdigest()


MANIFEST_DIGEST_V1 = _digest(MANIFEST_V1)
CORPUS_DIGEST_V1 = _digest(BATCHES_V1)

