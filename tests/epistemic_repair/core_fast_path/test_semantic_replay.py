from __future__ import annotations

from copy import deepcopy
from uuid import NAMESPACE_URL, uuid4, uuid5

from services.evaluation.epistemic_repair.core_fast_path_semantic_replay import (
    semantic_replay_digest,
)


def _receipt(*, split_commit: bool = False, duplicate_model: bool = False):
    tenant = uuid4()
    observation_a = uuid5(NAMESPACE_URL, f"p6-think:{tenant}:signal-a")
    observation_b = uuid5(NAMESPACE_URL, f"p6-think:{tenant}:signal-b")
    version_a, version_b, version_s = uuid4(), uuid4(), uuid4()
    commit = str(uuid4())
    models = [
        {
            "model_id": str(uuid4()), "version_id": str(version_a),
            "source_signal_id": "signal-a", "proposition": "A",
            "natural_text": "A",
            "lifecycle": "active", "scope_refs": ["scope:x"],
            "evidence_signal_ids": ["signal-a"],
            "supporting_model_version_ids": [], "commit_id": str(uuid4()),
            "prior_version_id": None, "supersedes_version_id": None,
            "history_retained": False,
        },
        {
            "model_id": str(uuid4()), "version_id": str(version_b),
            "source_signal_id": "signal-b", "proposition": "B",
            "natural_text": "B",
            "lifecycle": "active", "scope_refs": ["scope:x"],
            "evidence_signal_ids": ["signal-b"],
            "supporting_model_version_ids": [], "commit_id": str(uuid4()),
            "prior_version_id": None, "supersedes_version_id": None,
            "history_retained": False,
        },
        {
            "model_id": str(uuid4()), "version_id": str(version_s),
            "source_signal_id": None, "proposition": "A therefore B",
            "natural_text": "A therefore B",
            "lifecycle": "active", "scope_refs": ["scope:x"],
            "evidence_signal_ids": ["signal-a", "signal-b"],
            "supporting_model_version_ids": [str(version_a), str(version_b)],
            "commit_id": commit, "prior_version_id": None,
            "supersedes_version_id": None, "history_retained": False,
        },
    ]
    if duplicate_model:
        duplicate = deepcopy(models[0])
        duplicate["model_id"], duplicate["version_id"] = str(uuid4()), str(uuid4())
        models.append(duplicate)
    relation_commit = str(uuid4()) if split_commit else commit
    return {
        "population_digest": "population-v1", "tenant_id": str(tenant),
        "execution_id": str(uuid4()),
        "batches": [{
            "batch_number": 1,
            "input_signal_ids": ["signal-a", "signal-b"],
            "processed_signal_ids": ["signal-a", "signal-b"],
            "unbatched_signal_count": 0,
            "groundings": [{
                "signal_id": "signal-a", "canonical_ref": "scope:x",
                "surface": "A", "authority": "resolved_for_consumer",
            }],
            "atomics": [{
                "signal_id": "signal-a", "observation_id": str(observation_a),
                "evidence_bound": True, "tenant_id": str(tenant),
            }],
            "retrieval": {
                "accepted_model_version_ids": [str(version_a)],
                "observation_ids": [str(observation_b)],
            },
            "accepted_models": models,
            "accepted_relations": [{
                "relation_id": str(uuid4()), "relation_version_id": str(uuid4()),
                "kind": "dependency_constraint", "lifecycle": "active",
                "participant_model_version_ids": [str(version_s), str(version_b)],
                "commit_id": relation_commit,
            }],
            "barrier": {
                "snapshot_validated": True, "expected_head_count": 4,
                "matched_head_count": 4, "stale_head_count": 0,
                "missing_head_count": 0,
            },
        }],
    }


def test_digest_ignores_execution_identity_but_retains_semantics() -> None:
    assert semantic_replay_digest(_receipt()) == semantic_replay_digest(_receipt())


def test_digest_detects_support_and_relation_topology_change() -> None:
    original = _receipt()
    changed = deepcopy(original)
    synthesis = changed["batches"][0]["accepted_models"][2]
    synthesis["supporting_model_version_ids"] = synthesis["supporting_model_version_ids"][:1]
    assert semantic_replay_digest(original) != semantic_replay_digest(changed)


def test_digest_preserves_commit_equivalence_and_multiplicity() -> None:
    assert semantic_replay_digest(_receipt()) != semantic_replay_digest(
        _receipt(split_commit=True),
    )
    assert semantic_replay_digest(_receipt()) != semantic_replay_digest(
        _receipt(duplicate_model=True),
    )


def test_digest_detects_natural_text_change() -> None:
    original = _receipt()
    changed = deepcopy(original)
    changed["batches"][0]["accepted_models"][2]["natural_text"] = "stale text"
    assert semantic_replay_digest(original) != semantic_replay_digest(changed)


def test_digest_detects_relation_fate_change_without_execution_ids() -> None:
    original = _receipt()
    relation = original["batches"][0]["accepted_relations"][0]
    original["batches"][0]["relation_fates"] = [{
        "relation_id": relation["relation_id"],
        "relation_version_id": str(uuid4()),
        "prior_relation_version_id": relation["relation_version_id"],
        "kind": relation["kind"],
        "lifecycle": "retired",
        "prior_active_head_absent": True,
    }]
    same_semantics = deepcopy(original)
    same_semantics["batches"][0]["relation_fates"][0].update({
        "relation_id": str(uuid4()),
        "relation_version_id": str(uuid4()),
    })
    changed = deepcopy(original)
    changed["batches"][0]["relation_fates"][0]["prior_active_head_absent"] = False

    assert semantic_replay_digest(original) == semantic_replay_digest(same_semantics)
    assert semantic_replay_digest(original) != semantic_replay_digest(changed)
