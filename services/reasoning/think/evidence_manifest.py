"""Authorization checks for compiler-proposed claim evidence manifests."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any
from uuid import UUID

from lib.contracts.kernel import canonical_sha256
from lib.shared.errors import InvariantViolation


def authorize_compiler_evidence_manifest(
    *,
    selected_observation_ids: Sequence[UUID],
    manifest: Any,
    persisted_observations: Sequence[Mapping[str, Any]],
) -> None:
    """Prove selected support is a non-tampered subset of compiler evidence."""

    if not isinstance(manifest, list):
        raise InvariantViolation(
            "THINK_TRUTH_EVIDENCE_MANIFEST_MISSING",
            "compiler-bound claims require an evidence observation manifest",
        )

    authorized: dict[UUID, dict[str, Any]] = {}
    for raw in manifest:
        if not isinstance(raw, dict):
            raise InvariantViolation(
                "THINK_TRUTH_EVIDENCE_MANIFEST_INVALID",
                "evidence manifest rows must be objects",
            )
        try:
            observation_id = UUID(str(raw.get("observation_id") or ""))
        except (TypeError, ValueError) as exc:
            raise InvariantViolation(
                "THINK_TRUTH_EVIDENCE_MANIFEST_INVALID",
                "evidence manifest observation IDs must be UUIDs",
            ) from exc
        if observation_id in authorized:
            raise InvariantViolation(
                "THINK_TRUTH_EVIDENCE_MANIFEST_DUPLICATE",
                "evidence manifest observation IDs must be unique",
                observation_id=str(observation_id),
            )
        body = raw.get("body")
        content_digest = raw.get("content_digest")
        if not isinstance(body, str) or not body:
            raise InvariantViolation(
                "THINK_TRUTH_EVIDENCE_MANIFEST_INVALID",
                "evidence manifest rows require an exact non-empty body",
                observation_id=str(observation_id),
            )
        if content_digest is not None and (
            not isinstance(content_digest, str)
            or len(content_digest) != 64
            or any(char not in "0123456789abcdef" for char in content_digest)
        ):
            raise InvariantViolation(
                "THINK_TRUTH_EVIDENCE_MANIFEST_INVALID",
                "evidence manifest content digests must be lowercase sha256",
                observation_id=str(observation_id),
            )
        authorized[observation_id] = raw

    selected = tuple(dict.fromkeys(selected_observation_ids))
    foreign = [value for value in selected if value not in authorized]
    if foreign:
        raise InvariantViolation(
            "THINK_TRUTH_EVIDENCE_MANIFEST_SUBSET_VIOLATION",
            "redistributed evidence cannot exceed compiler-authorized observations",
            foreign_observation_ids=[str(value) for value in foreign],
        )

    persisted = {
        row["id"]: str(row["content_text"] or "")
        for row in persisted_observations
    }
    missing = [value for value in selected if value not in persisted]
    if missing:
        raise InvariantViolation(
            "THINK_TRUTH_EVIDENCE_NOT_FOUND",
            "manifest-authorized observations must exist in the same tenant",
            missing=[str(value) for value in missing],
        )

    for observation_id in selected:
        row = authorized[observation_id]
        body = persisted[observation_id]
        if row["body"] != body:
            raise InvariantViolation(
                "THINK_TRUTH_EVIDENCE_MANIFEST_BODY_MISMATCH",
                "manifest body does not match the persisted observation",
                observation_id=str(observation_id),
            )
        expected_digest = row.get("content_digest")
        if expected_digest is not None and expected_digest != canonical_sha256(body):
            raise InvariantViolation(
                "THINK_TRUTH_EVIDENCE_MANIFEST_DIGEST_MISMATCH",
                "manifest digest does not match the persisted observation",
                observation_id=str(observation_id),
            )


__all__ = ["authorize_compiler_evidence_manifest"]
