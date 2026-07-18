from __future__ import annotations

import json
from datetime import datetime, timezone
from uuid import NAMESPACE_URL, uuid4, uuid5

import pytest

from lib.contracts.kernel import canonical_sha256
from lib.evaluation.epistemic_repair.core_fast_path_gold import (
    build_core_fast_path_gold,
)
from lib.evaluation.epistemic_repair.core_fast_path_population import (
    build_core_fast_path_population,
)
from services.evaluation.epistemic_repair import core_fast_path_receipt
from services.evaluation.epistemic_repair.core_fast_path_receipt import (
    _barrier_digest,
    build_core_fast_path_runtime_receipt,
)
from services.evaluation.epistemic_repair.core_fast_path_scorer import (
    score_core_fast_path,
)


pytestmark = pytest.mark.asyncio


class _Connection:
    def __init__(self, *, tenant_id, observation_id, signal_id) -> None:
        model_id, version_id = uuid4(), uuid4()
        observation_occurred_at = datetime(
            2026, 7, 1, tzinfo=timezone.utc,
        )
        observation_text = "Delta handoff is open."
        self.model_id = model_id
        self.version_id = version_id
        self.relation_id, self.relation_version_id = uuid4(), uuid4()
        self.rows = {
            "observations": [{
                "id": observation_id,
                "occurred_at": observation_occurred_at,
                "content_text": observation_text,
            }],
            "grounding_traces": [{
                "source_observation_id": observation_id,
                "phrase": "Delta handoff",
                "current_fate": "resolved_for_consumer",
                "selected_referent": {"id": "workstream:delta-handoff"},
            }],
            "model_truth_versions": [{
                "model_id": model_id, "version_id": version_id, "version": 1,
                "natural_text": "Delta handoff is open.",
                "proposition": {
                    "assertion": "Delta handoff is open.",
                    "scope_ref": "workstream:delta-handoff",
                    "abstraction_level": "atomic",
                    "evidence_event_ids": [str(observation_id)],
                    "evidence_contract": {"evidence_status": "evidence_bound"},
                },
                "lifecycle": "active", "supersedes_version_id": None,
                "created_at": None, "supporting_model_ids": [],
                "visible_to_subjects": True,
                "born_from_event_id": observation_id, "supporting_event_ids": [],
            }],
            "model_truth_evidence_references": [{
                "model_version_id": version_id,
                "evidence_kind": "observation",
                "evidence_id": str(observation_id),
                "evidence_version": 1,
                "evidence_digest": canonical_sha256(observation_text),
                "evidence_role": "support",
                "source_object_id": str(observation_id),
                "source_revision": "1",
                "field_path": "content_text",
                "occurred_at": observation_occurred_at,
            }],
            "relation_truth_versions": [{
                "id": self.relation_id,
                "relation_version_id": self.relation_version_id,
                "relation_kind": "dependency_constraint",
                "lifecycle": "active",
                "think_run_id": None,
                "instance_status": "accepted",
                "admission_disposition": "accepted",
            }],
            "relation_truth_participants": [{
                "relation_version_id": self.relation_version_id,
                "model_id": model_id,
                "model_version_id": version_id,
                "role": "source", "ordinal": 0,
            }],
            "company_learning_barriers": [],
            "company_learning_context_decisions": [],
            "think_runs": [],
            "applied_triggers": [],
        }

    async def fetch(self, query, *_args):
        for table, rows in self.rows.items():
            if table in query:
                return rows
        raise AssertionError(query)

    async def fetchval(self, query, *_args):
        if "count(*)" in query:
            return 0
        return []


async def test_runtime_receipt_is_json_safe_and_scorer_consumable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tenant_id = uuid4()
    population = build_core_fast_path_population()
    signal = population.batches[0].signals[0]
    observation_id = uuid5(
        NAMESPACE_URL, f"p6-think:{tenant_id}:{signal.signal_id}",
    )
    artifact = {
        "tenant_id": str(tenant_id),
        "population_digest": population.population_digest,
        "waves": [
            {
                "batch_number": number, "status": "success",
                "execution": {
                    "trigger_id": str(uuid4()),
                    "run": {"id": str(uuid4()), "status": "success"},
                },
                "barrier_receipt": {
                    "reopened_exactly": True,
                    "truth_critical_pending_count": 0,
                    "expected_model_version_ids": [],
                    "expected_relation_version_ids": [],
                },
            }
            for number in range(1, 5)
        ],
    }
    run_id = artifact["waves"][0]["execution"]["run"]["id"]
    connection = _Connection(
        tenant_id=tenant_id,
        observation_id=observation_id,
        signal_id=signal.signal_id,
    )
    connection.rows["think_runs"] = [{
        "id": run_id,
        "trigger_id": artifact["waves"][0]["execution"]["trigger_id"],
        "status": "success", "error": None,
        "ops_applied": {"context_use": {
            "selected_trigger_observation_ids": [str(observation_id)],
        }},
    }]
    connection.rows["company_learning_context_decisions"] = [{
        "batch_id": artifact["waves"][1]["execution"]["trigger_id"],
        "context_item_kind": "accepted_model",
        "context_item_id": str(connection.model_id),
        "retrieved": True,
    }]
    async def prove_queue(_conn, **kwargs):
        return tuple(sorted(kwargs["expected_observation_ids"], key=str))

    monkeypatch.setattr(
        core_fast_path_receipt, "proven_batch_observation_ids", prove_queue,
    )
    prior_id = None
    for number in range(1, 5):
        barrier_id = uuid4()
        row = {
            "barrier_id": barrier_id,
            "tenant_id": tenant_id,
            "batch_id": f"p6-batch-{number}",
            "barrier_version": number,
            "prior_barrier_id": prior_id,
            "expected_model_version_ids": [connection.version_id],
            "expected_relation_version_ids": (
                [] if number == 1 else [connection.relation_version_id]
            ),
            "invalidated_model_version_ids": [],
            "truth_critical_pending_count": 0,
            "status": "complete",
            "completed_at": datetime(2026, 7, 18, number, tzinfo=timezone.utc),
        }
        row["receipt_digest"] = _barrier_digest(row)
        connection.rows["company_learning_barriers"].append(row)
        artifact["waves"][number - 1]["barrier_receipt"] = {
            **{
                key: row[key] for key in (
                    "barrier_id", "batch_id", "barrier_version",
                    "prior_barrier_id", "expected_model_version_ids",
                    "expected_relation_version_ids",
                    "invalidated_model_version_ids",
                    "truth_critical_pending_count", "receipt_digest",
                    "completed_at",
                )
            },
            "reopened_exactly": True,
        }
        artifact["waves"][number - 1]["snapshot"] = {
            "accepted_models": [
                {"truth_version_id": str(value)}
                for value in row["expected_model_version_ids"]
            ],
            "accepted_relations": [
                {"truth_relation_version_id": str(value)}
                for value in row["expected_relation_version_ids"]
            ],
            "accepted_relation_count": len(
                row["expected_relation_version_ids"]
            ),
        }
        prior_id = barrier_id

    receipt = await build_core_fast_path_runtime_receipt(
        connection,
        artifact,
    )

    json.dumps(receipt)
    assert len(receipt["batches"]) == 4
    assert receipt["batches"][0]["groundings"][0]["signal_id"] == signal.signal_id
    assert receipt["batches"][0]["atomics"][0]["evidence_bound"] is True
    assert len(receipt["batches"][0]["processed_signal_ids"]) == 25
    assert receipt["batches"][0]["unbatched_signal_count"] == 0
    assert "commit_id" not in receipt["batches"][0]["accepted_models"][0]
    assert receipt["batches"][0]["barrier"] == {
        "snapshot_validated": True,
        "expected_head_count": 1,
        "matched_head_count": 1,
        "stale_head_count": 0,
        "missing_head_count": 0,
    }
    assert [len(batch["accepted_models"]) for batch in receipt["batches"]] == [
        1, 0, 0, 0,
    ]
    assert [len(batch["accepted_relations"]) for batch in receipt["batches"]] == [
        0, 1, 0, 0,
    ]
    assert receipt["batches"][1]["retrieval"][
        "accepted_model_version_ids"
    ] == [str(connection.version_id)]
    report = score_core_fast_path(receipt, gold=build_core_fast_path_gold())
    assert report["schema_version"] == "core-fast-path-score-v1"


async def test_tampered_historical_artifact_receipt_fails_validation() -> None:
    tenant_id = uuid4()
    population = build_core_fast_path_population()
    signal = population.batches[0].signals[0]
    observation_id = uuid5(
        NAMESPACE_URL, f"p6-think:{tenant_id}:{signal.signal_id}",
    )
    connection = _Connection(
        tenant_id=tenant_id, observation_id=observation_id,
        signal_id=signal.signal_id,
    )
    # Proposition JSON still claims evidence-bound status; canonical evidence
    # is deliberately absent and must therefore receive no atomic credit.
    connection.rows["model_truth_evidence_references"] = []
    barrier_id = uuid4()
    barrier = {
        "barrier_id": barrier_id, "tenant_id": tenant_id,
        "batch_id": "p6-batch-1", "barrier_version": 1,
        "prior_barrier_id": None,
        "expected_model_version_ids": [connection.version_id],
        "expected_relation_version_ids": [],
        "invalidated_model_version_ids": [],
        "truth_critical_pending_count": 0, "status": "complete",
        "completed_at": datetime(2026, 7, 18, 1, tzinfo=timezone.utc),
    }
    barrier["receipt_digest"] = _barrier_digest(barrier)
    connection.rows["company_learning_barriers"] = [barrier]
    artifact_barrier = {
        key: barrier[key] for key in (
            "barrier_id", "batch_id", "barrier_version", "prior_barrier_id",
            "expected_model_version_ids", "expected_relation_version_ids",
            "invalidated_model_version_ids", "truth_critical_pending_count",
            "receipt_digest", "completed_at",
        )
    }
    artifact_barrier.update({
        "reopened_exactly": True,
        "receipt_digest": "0" * 64,
    })
    artifact = {
        "tenant_id": str(tenant_id),
        "population_digest": population.population_digest,
        "waves": [{
            "batch_number": 1,
            "status": "success",
            "execution": {"run": {"id": str(uuid4())}},
            "barrier_receipt": artifact_barrier,
            "snapshot": {
                "accepted_models": [{
                    "truth_version_id": str(connection.version_id),
                }],
                "accepted_relations": [],
                "accepted_relation_count": 0,
            },
        }],
    }

    receipt = await build_core_fast_path_runtime_receipt(connection, artifact)

    assert receipt["batches"][0]["barrier"] == {
        "snapshot_validated": False,
        "expected_head_count": 1,
        "matched_head_count": 0,
        "stale_head_count": 0,
        "missing_head_count": 1,
    }
    assert receipt["batches"][0]["atomics"] == []


async def test_barrier_snapshot_fails_when_expected_canonical_heads_are_absent() -> None:
    tenant_id = uuid4()
    population = build_core_fast_path_population()
    signal = population.batches[0].signals[0]
    observation_id = uuid5(
        NAMESPACE_URL, f"p6-think:{tenant_id}:{signal.signal_id}",
    )
    connection = _Connection(
        tenant_id=tenant_id, observation_id=observation_id,
        signal_id=signal.signal_id,
    )
    missing_model_version_id = uuid4()
    missing_relation_version_id = uuid4()
    barrier = {
        "barrier_id": uuid4(), "tenant_id": tenant_id,
        "batch_id": "p6-batch-1", "barrier_version": 1,
        "prior_barrier_id": None,
        "expected_model_version_ids": [missing_model_version_id],
        "expected_relation_version_ids": [missing_relation_version_id],
        "invalidated_model_version_ids": [],
        "truth_critical_pending_count": 0, "status": "complete",
        "completed_at": datetime(2026, 7, 18, 1, tzinfo=timezone.utc),
    }
    barrier["receipt_digest"] = _barrier_digest(barrier)
    connection.rows["company_learning_barriers"] = [barrier]
    artifact_barrier = {
        key: barrier[key] for key in (
            "barrier_id", "batch_id", "barrier_version", "prior_barrier_id",
            "expected_model_version_ids", "expected_relation_version_ids",
            "invalidated_model_version_ids", "truth_critical_pending_count",
            "receipt_digest", "completed_at",
        )
    }
    artifact_barrier["reopened_exactly"] = True
    artifact = {
        "tenant_id": str(tenant_id),
        "population_digest": population.population_digest,
        "waves": [{
            "batch_number": 1, "status": "success",
            "execution": {"run": {"id": str(uuid4())}},
            "barrier_receipt": artifact_barrier,
            "snapshot": {
                "accepted_models": [{
                    "truth_version_id": str(missing_model_version_id),
                }],
                "accepted_relations": [{
                    "truth_relation_version_id": str(missing_relation_version_id),
                }],
                "accepted_relation_count": 1,
            },
        }],
    }

    receipt = await build_core_fast_path_runtime_receipt(connection, artifact)

    assert receipt["batches"][0]["barrier"] == {
        "snapshot_validated": False,
        "expected_head_count": 2,
        "matched_head_count": 0,
        "stale_head_count": 0,
        "missing_head_count": 2,
    }


async def test_dangling_observation_evidence_receives_no_atomic_credit() -> None:
    tenant_id = uuid4()
    population = build_core_fast_path_population()
    signal = population.batches[0].signals[0]
    dangling_observation_id = uuid5(
        NAMESPACE_URL, f"p6-think:{tenant_id}:{signal.signal_id}",
    )
    connection = _Connection(
        tenant_id=tenant_id, observation_id=dangling_observation_id,
        signal_id=signal.signal_id,
    )
    connection.rows["observations"] = []
    barrier = {
        "barrier_id": uuid4(), "tenant_id": tenant_id,
        "batch_id": "p6-batch-1", "barrier_version": 1,
        "prior_barrier_id": None,
        "expected_model_version_ids": [connection.version_id],
        "expected_relation_version_ids": [],
        "invalidated_model_version_ids": [],
        "truth_critical_pending_count": 0, "status": "complete",
        "completed_at": datetime(2026, 7, 18, 1, tzinfo=timezone.utc),
    }
    barrier["receipt_digest"] = _barrier_digest(barrier)
    connection.rows["company_learning_barriers"] = [barrier]
    artifact_barrier = {
        key: barrier[key] for key in (
            "barrier_id", "batch_id", "barrier_version", "prior_barrier_id",
            "expected_model_version_ids", "expected_relation_version_ids",
            "invalidated_model_version_ids", "truth_critical_pending_count",
            "receipt_digest", "completed_at",
        )
    }
    artifact_barrier["reopened_exactly"] = True
    artifact = {
        "tenant_id": str(tenant_id),
        "population_digest": population.population_digest,
        "waves": [{
            "batch_number": 1, "status": "success",
            "execution": {"run": {"id": str(uuid4())}},
            "barrier_receipt": artifact_barrier,
            "snapshot": {
                "accepted_models": [{
                    "truth_version_id": str(connection.version_id),
                }],
                "accepted_relations": [],
                "accepted_relation_count": 0,
            },
        }],
    }

    receipt = await build_core_fast_path_runtime_receipt(connection, artifact)

    assert receipt["batches"][0]["atomics"] == []


async def test_shared_commit_requires_exact_durable_run_envelope(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tenant_id = uuid4()
    population = build_core_fast_path_population()
    signal = population.batches[0].signals[0]
    observation_id = uuid5(
        NAMESPACE_URL, f"p6-think:{tenant_id}:{signal.signal_id}",
    )
    connection = _Connection(
        tenant_id=tenant_id, observation_id=observation_id,
        signal_id=signal.signal_id,
    )
    composite_model_id, composite_version_id = uuid4(), uuid4()
    connection.rows["model_truth_versions"].append({
        "model_id": composite_model_id,
        "version_id": composite_version_id,
        "version": 1,
        "natural_text": "The workstream is blocked.",
        "proposition": {
            "situation": "The workstream is blocked.",
            "abstraction_level": "composite",
            "scope_ref": "workstream:delta-handoff",
            "member_model_ids": [str(connection.model_id)],
            "evidence_event_ids": [str(observation_id)],
        },
        "lifecycle": "active", "supersedes_version_id": None,
        "created_at": None,
    })
    connection.rows["model_truth_evidence_references"].extend([
        {
            "model_version_id": composite_version_id,
            "evidence_kind": "observation",
            "evidence_id": str(observation_id),
            "evidence_version": 1,
            "evidence_digest": canonical_sha256(
                connection.rows["observations"][0]["content_text"]
            ),
            "evidence_role": "support",
            "source_object_id": str(observation_id),
            "source_revision": "1",
            "field_path": "content_text",
            "occurred_at": connection.rows["observations"][0]["occurred_at"],
        },
        {
            "model_version_id": composite_version_id,
            "evidence_kind": "model_version",
            "evidence_id": str(connection.version_id),
            "evidence_version": 1,
            "evidence_role": "derivation",
            "source_object_id": str(connection.version_id),
            "field_path": "semantic_digest",
        },
    ])
    run_id, trigger_id = uuid4(), uuid4()
    relation = connection.rows["relation_truth_versions"][0]
    relation["think_run_id"] = run_id
    diff_hash = "a" * 64
    connection.rows["think_runs"] = [{
        "id": run_id, "trigger_id": trigger_id, "status": "success",
        "error": None,
        "ops_applied": {
            "diff_hash": diff_hash,
            "apply_dropped_op_count": 0,
            "applied_model_ids": [str(composite_model_id)],
            "claim_ops": [{
                "op": "insert", "quality_decision": "accept",
                "abstraction_level": "composite",
                "model_id": str(composite_model_id),
            }],
            "relation_claim_ops": [{
                "status": "accepted",
                "relation_instance_id": str(connection.relation_id),
                "canonical_relation_version_id": str(
                    connection.relation_version_id
                ),
            }],
        },
    }]
    connection.rows["applied_triggers"] = [{
        "trigger_id": trigger_id,
        "diff_hash": diff_hash,
        "outcome": "success",
    }]
    barrier = {
        "barrier_id": uuid4(), "tenant_id": tenant_id,
        "batch_id": "p6-batch-1", "barrier_version": 1,
        "prior_barrier_id": None,
        "expected_model_version_ids": [
            connection.version_id, composite_version_id,
        ],
        "expected_relation_version_ids": [connection.relation_version_id],
        "invalidated_model_version_ids": [],
        "truth_critical_pending_count": 0,
        "status": "complete",
        "completed_at": datetime(2026, 7, 18, 1, tzinfo=timezone.utc),
    }
    barrier["receipt_digest"] = _barrier_digest(barrier)
    connection.rows["company_learning_barriers"] = [barrier]
    artifact_barrier = {
        key: barrier[key] for key in (
            "barrier_id", "batch_id", "barrier_version", "prior_barrier_id",
            "expected_model_version_ids", "expected_relation_version_ids",
            "invalidated_model_version_ids", "truth_critical_pending_count",
            "receipt_digest", "completed_at",
        )
    }
    artifact_barrier["reopened_exactly"] = True
    artifact = {
        "tenant_id": str(tenant_id),
        "population_digest": population.population_digest,
        "waves": [{
            "batch_number": 1, "status": "success",
            "execution": {
                "trigger_id": str(trigger_id),
                "run": {"id": str(run_id), "status": "success"},
            },
            "barrier_receipt": artifact_barrier,
            "snapshot": {
                "accepted_models": [
                    {"truth_version_id": str(value)}
                    for value in barrier["expected_model_version_ids"]
                ],
                "accepted_relations": [{
                    "truth_relation_version_id": str(
                        connection.relation_version_id
                    ),
                }],
                "accepted_relation_count": 1,
            },
        }],
    }

    async def prove_queue(_conn, **kwargs):
        return tuple(sorted(kwargs["expected_observation_ids"], key=str))

    monkeypatch.setattr(
        core_fast_path_receipt, "proven_batch_observation_ids", prove_queue,
    )
    receipt = await build_core_fast_path_runtime_receipt(connection, artifact)

    composite = next(
        model for model in receipt["batches"][0]["accepted_models"]
        if model["model_id"] == str(composite_model_id)
    )
    admitted_relation = receipt["batches"][0]["accepted_relations"][0]
    assert composite["commit_id"] == str(run_id)
    assert admitted_relation["commit_id"] == str(run_id)
