"""Gold-blind semantic fingerprints for core fast-path runtime receipts.

The scorer receipt deliberately contains execution-local identifiers.  This
module removes those identities while retaining the observable structure that
must be stable across clean executions: multiplicity, evidence and lifecycle
topology, relation endpoints, retrieval choices, and commit equivalence.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Mapping, Sequence
from typing import Any
from uuid import NAMESPACE_URL, UUID, uuid5

from lib.contracts.kernel import canonical_sha256


def _items(value: Any) -> list[Mapping[str, Any]]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        return []
    return [item for item in value if isinstance(item, Mapping)]


def _strings(value: Any) -> list[str]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        return []
    return [str(item) for item in value]


def _multiset(values: Sequence[Any]) -> list[dict[str, Any]]:
    """Encode an unordered collection without collapsing duplicates."""

    encoded = [canonical_sha256(value) for value in values]
    return [
        {"value_digest": digest, "count": count}
        for digest, count in sorted(Counter(encoded).items())
    ]


def semantic_replay_projection(receipt: Mapping[str, Any]) -> dict[str, Any]:
    """Return the execution-identity-free behavioral projection of a receipt."""

    batches = sorted(
        _items(receipt.get("batches")), key=lambda row: int(row.get("batch_number") or 0),
    )
    tenant = str(receipt.get("tenant_id") or "")
    observation_to_signal: dict[str, str] = {}
    if tenant:
        try:
            tenant_id = UUID(tenant)
        except ValueError:
            tenant_id = None
        if tenant_id is not None:
            for batch in batches:
                for signal_id in _strings(batch.get("input_signal_ids")):
                    observation_to_signal[str(uuid5(
                        NAMESPACE_URL, f"p6-think:{tenant_id}:{signal_id}",
                    ))] = signal_id
    for batch in batches:
        for atomic in _items(batch.get("atomics")):
            signal_id = str(atomic.get("signal_id") or "")
            observation_id = str(atomic.get("observation_id") or "")
            if signal_id and observation_id:
                observation_to_signal[observation_id] = signal_id

    model_by_version: dict[str, Mapping[str, Any]] = {}
    for batch in batches:
        for model in _items(batch.get("accepted_models")):
            version_id = str(model.get("version_id") or "")
            if version_id:
                model_by_version[version_id] = model

    fingerprints: dict[str, str] = {}
    visiting: set[str] = set()

    def model_fingerprint(version_id: str) -> str:
        if version_id in fingerprints:
            return fingerprints[version_id]
        model = model_by_version.get(version_id)
        if model is None:
            return canonical_sha256({"unresolved_model_version": True})
        if version_id in visiting:
            return canonical_sha256({"cyclic_model_reference": True})
        visiting.add(version_id)
        prior = str(
            model.get("prior_version_id")
            or model.get("supersedes_version_id")
            or ""
        )
        descriptor = {
            "source_signal_id": model.get("source_signal_id"),
            "proposition": model.get("proposition"),
            "abstraction_level": model.get("abstraction_level"),
            "claim_role": model.get("claim_role"),
            "lifecycle": model.get("lifecycle"),
            "scope_refs": sorted(_strings(model.get("scope_refs"))),
            "evidence_signal_ids": sorted(_strings(model.get("evidence_signal_ids"))),
            "supporting_versions": sorted(
                model_fingerprint(value)
                for value in _strings(model.get("supporting_model_version_ids"))
            ),
            "prior_version": model_fingerprint(prior) if prior else None,
            "history_retained": model.get("history_retained") is True,
        }
        visiting.remove(version_id)
        fingerprints[version_id] = canonical_sha256(descriptor)
        return fingerprints[version_id]

    for version_id in model_by_version:
        model_fingerprint(version_id)

    projected_batches: list[dict[str, Any]] = []
    for batch in batches:
        model_descriptors: list[dict[str, Any]] = []
        relation_descriptors: list[dict[str, Any]] = []
        commit_members: dict[str, list[dict[str, str]]] = {}
        missing_commit_count = 0

        for model in _items(batch.get("accepted_models")):
            version_id = str(model.get("version_id") or "")
            fingerprint = model_fingerprint(version_id)
            model_descriptors.append({"semantic_version": fingerprint})
            commit_id = str(model.get("commit_id") or "")
            if commit_id:
                commit_members.setdefault(commit_id, []).append({
                    "kind": "model", "semantic_version": fingerprint,
                })
            else:
                missing_commit_count += 1

        for relation in _items(batch.get("accepted_relations")):
            descriptor = {
                "kind": relation.get("kind"),
                "lifecycle": relation.get("lifecycle"),
                # Participant order is meaningful when the runtime preserves
                # role/ordinal order; do not sort it.
                "participant_versions": [
                    model_fingerprint(value)
                    for value in _strings(relation.get("participant_model_version_ids"))
                ],
            }
            relation_fingerprint = canonical_sha256(descriptor)
            relation_descriptors.append({"semantic_relation": relation_fingerprint})
            commit_id = str(relation.get("commit_id") or "")
            if commit_id:
                commit_members.setdefault(commit_id, []).append({
                    "kind": "relation", "semantic_relation": relation_fingerprint,
                })
            else:
                missing_commit_count += 1

        commit_groups = [
            {
                "members": _multiset(members),
                "member_count": len(members),
            }
            for members in commit_members.values()
        ]
        retrieval = batch.get("retrieval")
        if not isinstance(retrieval, Mapping):
            retrieval = {}
        atomics = [
            {
                "signal_id": row.get("signal_id"),
                "evidence_bound": row.get("evidence_bound") is True,
                "tenant_isolated": str(row.get("tenant_id") or "") == tenant,
            }
            for row in _items(batch.get("atomics"))
        ]
        groundings = [
            {
                "signal_id": row.get("signal_id"),
                "canonical_ref": row.get("canonical_ref"),
                "surface": row.get("surface"),
                "authority": row.get("authority"),
            }
            for row in _items(batch.get("groundings"))
        ]
        barrier = batch.get("barrier")
        if not isinstance(barrier, Mapping):
            barrier = {}
        projected_batches.append({
            "batch_number": batch.get("batch_number"),
            "input_signal_ids": sorted(_strings(batch.get("input_signal_ids"))),
            "processed_signal_ids": sorted(
                _strings(batch.get("processed_signal_ids")),
            ),
            "unbatched_signal_count": batch.get("unbatched_signal_count"),
            "groundings": _multiset(groundings),
            "atomics": _multiset(atomics),
            "retrieval": {
                "model_versions": sorted(
                    model_fingerprint(value)
                    for value in _strings(retrieval.get("accepted_model_version_ids"))
                ),
                "observation_signals": sorted(
                    observation_to_signal.get(value, "unresolved_observation")
                    for value in _strings(retrieval.get("observation_ids"))
                ),
            },
            "models": _multiset(model_descriptors),
            "relations": _multiset(relation_descriptors),
            "commit_groups": _multiset(commit_groups),
            "missing_commit_identity_count": missing_commit_count,
            "barrier": {
                key: barrier.get(key) for key in (
                    "snapshot_validated", "expected_head_count",
                    "matched_head_count", "stale_head_count", "missing_head_count",
                )
            },
        })

    return {
        "population_digest": receipt.get("population_digest"),
        "batches": projected_batches,
    }


def semantic_replay_digest(receipt: Mapping[str, Any]) -> str:
    """Return a canonical digest comparable across clean tenant executions."""

    return canonical_sha256(semantic_replay_projection(receipt))


__all__ = ["semantic_replay_digest", "semantic_replay_projection"]
