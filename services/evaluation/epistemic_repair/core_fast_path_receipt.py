"""Gold-blind receipt adapter for completed core fast-path executions."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from typing import Any
from uuid import NAMESPACE_URL, UUID, uuid5

import asyncpg

from lib.contracts.kernel import canonical_sha256
from lib.evaluation.epistemic_repair.core_fast_path_population import (
    build_core_fast_path_population,
)
from services.evaluation.epistemic_repair.core_fast_path_queue_evidence import (
    proven_batch_observation_ids,
)
from services.evaluation.epistemic_repair.core_fast_path_semantic_replay import (
    semantic_replay_digest,
)


def _json(value: Any) -> Any:
    if isinstance(value, str):
        import json
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return value
    return value


def _barrier_digest(row: Mapping[str, Any]) -> str:
    completed_at = row["completed_at"]
    if isinstance(completed_at, datetime):
        completed_at = completed_at.isoformat()
    return canonical_sha256({
        "barrier_id": str(row["barrier_id"]),
        "tenant_id": str(row["tenant_id"]),
        "batch_id": row["batch_id"],
        "barrier_version": int(row["barrier_version"]),
        "prior_barrier_id": (
            str(row["prior_barrier_id"]) if row["prior_barrier_id"] else None
        ),
        "expected_model_version_ids": sorted(map(
            str, row["expected_model_version_ids"] or (),
        )),
        "expected_relation_version_ids": sorted(map(
            str, row["expected_relation_version_ids"] or (),
        )),
        "invalidated_model_version_ids": sorted(map(
            str, row["invalidated_model_version_ids"] or (),
        )),
        "truth_critical_pending_count": int(row["truth_critical_pending_count"]),
        "completed_at": completed_at,
    })


def _timestamp(value: Any) -> str:
    if isinstance(value, datetime):
        return value.isoformat()
    text = str(value or "")
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).isoformat()
    except ValueError:
        return text


def _artifact_barrier_matches(
    row: Mapping[str, Any], receipt: Any,
) -> bool:
    """Bind the saved artifact to the exact durable barrier receipt."""

    if (
        not isinstance(receipt, Mapping)
        or receipt.get("reopened_exactly") is not True
    ):
        return False
    scalar_pairs = (
        ("barrier_id", str),
        ("batch_id", str),
        ("barrier_version", int),
        ("truth_critical_pending_count", int),
        ("receipt_digest", str),
    )
    for key, cast in scalar_pairs:
        try:
            if cast(row[key]) != cast(receipt.get(key)):
                return False
        except (KeyError, TypeError, ValueError):
            return False
    prior = str(row["prior_barrier_id"]) if row["prior_barrier_id"] else None
    artifact_prior = (
        str(receipt.get("prior_barrier_id"))
        if receipt.get("prior_barrier_id") else None
    )
    if prior != artifact_prior or _timestamp(row["completed_at"]) != _timestamp(
        receipt.get("completed_at")
    ):
        return False
    for key in (
        "expected_model_version_ids",
        "expected_relation_version_ids",
        "invalidated_model_version_ids",
    ):
        if {str(value) for value in row[key] or ()} != {
            str(value) for value in receipt.get(key) or ()
        }:
            return False
    return True


def _artifact_snapshot_matches(
    snapshot: Any,
    *,
    expected_model_ids: set[str],
    expected_relation_ids: set[str],
) -> bool:
    """Require the post-barrier snapshot to expose the complete exact heads."""

    if not isinstance(snapshot, Mapping):
        return False
    models = snapshot.get("accepted_models")
    relations = snapshot.get("accepted_relations")
    if not isinstance(models, list) or not isinstance(relations, list):
        return False
    snapshot_models = {
        str(row.get("truth_version_id"))
        for row in models if isinstance(row, Mapping)
        and row.get("truth_version_id")
    }
    snapshot_relations = {
        str(row.get("truth_relation_version_id"))
        for row in relations if isinstance(row, Mapping)
        and row.get("truth_relation_version_id")
    }
    return (
        snapshot_models == expected_model_ids
        and snapshot_relations == expected_relation_ids
        and int(snapshot.get("accepted_relation_count") or 0)
        == len(expected_relation_ids)
    )


def _uuid(value: Any) -> UUID | None:
    try:
        return UUID(str(value))
    except (TypeError, ValueError, AttributeError):
        return None


def _canonical_observation_evidence_id(
    row: Mapping[str, Any],
    *,
    observations_by_coordinate: Mapping[tuple[UUID, Any], Mapping[str, Any]],
    observation_to_signal: Mapping[UUID, str],
) -> UUID | None:
    """Resolve a truth reference to one exact tenant-owned observation revision."""

    evidence_id = _uuid(row.get("evidence_id"))
    if evidence_id is None or evidence_id not in observation_to_signal:
        return None
    observation = observations_by_coordinate.get((
        evidence_id, row.get("occurred_at"),
    ))
    if observation is None:
        return None
    if (
        row.get("evidence_kind") != "observation"
        or row.get("evidence_role") not in {
            "support", "counterevidence", "authority",
        }
        or int(row.get("evidence_version") or 0) != 1
        or str(row.get("source_object_id")) != str(evidence_id)
        or str(row.get("source_revision")) != "1"
        or row.get("field_path") != "content_text"
        or row.get("evidence_digest") != canonical_sha256(
            str(observation.get("content_text") or "")
        )
    ):
        return None
    return evidence_id


async def build_core_fast_path_runtime_receipt(
    conn: asyncpg.Connection, artifact: Mapping[str, Any],
) -> dict[str, Any]:
    """Project saved runtime and database truth into the scorer contract."""

    tenant_id = UUID(str(artifact["tenant_id"]))
    population = build_core_fast_path_population()
    observation_to_signal = {
        uuid5(
            NAMESPACE_URL, f"p6-think:{tenant_id}:{signal.signal_id}",
        ): signal.signal_id
        for signal in population.signals
    }
    observation_ids = list(observation_to_signal)
    grounding_rows = await conn.fetch(
        """SELECT g.source_observation_id,g.phrase,g.current_fate,
                  g.selected_referent
             FROM grounding_traces g
             JOIN entity_grounding_work_items w
               ON w.tenant_id=g.tenant_id AND w.current_trace_id=g.id
            WHERE g.tenant_id=$1 AND g.source_observation_id=ANY($2::uuid[])""",
        tenant_id, observation_ids,
    )
    observation_rows = await conn.fetch(
        """SELECT id,occurred_at,content_text FROM observations
            WHERE tenant_id=$1 AND id=ANY($2::uuid[])
            ORDER BY id,occurred_at""",
        tenant_id, observation_ids,
    )
    model_rows = await conn.fetch(
        """SELECT v.model_id,v.version_id,v.version,v.natural_text,v.proposition,
                  v.lifecycle,v.supersedes_version_id,v.created_at
             FROM model_truth_versions v
            WHERE v.tenant_id=$1 ORDER BY v.created_at,v.version_id""",
        tenant_id,
    )
    evidence_rows = await conn.fetch(
        """SELECT model_version_id,evidence_kind,evidence_id,
                  evidence_version,evidence_digest,evidence_role,
                  source_object_id,source_revision,field_path,occurred_at
             FROM model_truth_evidence_references
            WHERE tenant_id=$1
            ORDER BY model_version_id,evidence_kind,evidence_id,evidence_role""",
        tenant_id,
    )
    relation_rows = await conn.fetch(
        """SELECT v.relation_id AS id,v.relation_version_id,
                  v.relation_kind,v.lifecycle,
                  v.supersedes_relation_version_id,
                  h.relation_version_id AS head_relation_version_id,
                  h.lifecycle AS head_lifecycle,i.think_run_id,
                  i.status AS instance_status,
                  d.disposition AS admission_disposition
             FROM relation_truth_versions v
             JOIN relation_truth_heads h
               ON h.tenant_id=v.tenant_id AND h.relation_id=v.relation_id
             JOIN relation_instances i
               ON i.tenant_id=v.tenant_id AND i.id=v.relation_id
             JOIN relation_truth_admission_decisions d
               ON d.tenant_id=v.tenant_id
              AND d.decision_id=v.admission_decision_id
            WHERE v.tenant_id=$1""",
        tenant_id,
    )
    participant_rows = await conn.fetch(
        """SELECT relation_version_id,model_id,model_version_id,role,ordinal
             FROM relation_truth_participants WHERE tenant_id=$1
             ORDER BY relation_version_id,ordinal""",
        tenant_id,
    )
    barrier_rows = await conn.fetch(
        """SELECT barrier_id,tenant_id,batch_id,barrier_version,
                  prior_barrier_id,expected_model_version_ids,
                  expected_relation_version_ids,invalidated_model_version_ids,
                  truth_critical_pending_count,status,receipt_digest,completed_at
             FROM company_learning_barriers WHERE tenant_id=$1
             ORDER BY barrier_version""",
        tenant_id,
    )
    decision_rows = await conn.fetch(
        """SELECT batch_id,context_item_kind,context_item_id,retrieved
             FROM company_learning_context_decisions
            WHERE tenant_id=$1 AND retrieved IS TRUE
            ORDER BY decided_at,decision_id""",
        tenant_id,
    )
    run_rows = await conn.fetch(
        """SELECT id,trigger_id,status,error,ops_applied FROM think_runs
            WHERE tenant_id=$1 AND trigger_kind='T1:event_batch'
              AND status='success'""",
        tenant_id,
    )
    applied_rows = await conn.fetch(
        """SELECT trigger_id,diff_hash,outcome FROM applied_triggers
            WHERE tenant_id=$1""",
        tenant_id,
    )
    cross_tenant = int(await conn.fetchval(
        """SELECT
             (SELECT count(*) FROM observations
               WHERE id=ANY($1::uuid[]) AND tenant_id<>$2)
             +
             (SELECT count(*)
                FROM model_truth_evidence_references e
                JOIN observations o
                  ON e.evidence_kind='observation'
                 AND o.id::text=e.evidence_id
               WHERE e.tenant_id=$2 AND o.tenant_id<>$2)
             +
             (SELECT count(*)
                FROM model_truth_evidence_references e
                JOIN model_truth_versions v
                  ON e.evidence_kind='model_version'
                 AND v.version_id::text=e.evidence_id
               WHERE e.tenant_id=$2 AND v.tenant_id<>$2)
             +
             (SELECT count(*)
                FROM grounding_traces g
                JOIN observations o ON o.id=g.source_observation_id
               WHERE g.tenant_id=$2 AND o.tenant_id<>$2)
             +
             (SELECT count(*)
                FROM relation_truth_participants p
                JOIN model_truth_versions v
                  ON v.version_id=p.model_version_id
               WHERE p.tenant_id=$2 AND v.tenant_id<>$2)""",
        observation_ids, tenant_id,
    ))

    groundings_by_signal: dict[str, list[dict[str, Any]]] = {}
    for row in grounding_rows:
        signal_id = observation_to_signal.get(row["source_observation_id"])
        selected = _json(row["selected_referent"]) or {}
        if signal_id and isinstance(selected, Mapping):
            groundings_by_signal.setdefault(signal_id, []).append({
                "signal_id": signal_id,
                "canonical_ref": selected.get("id"),
                "surface": row["phrase"],
                "authority": row["current_fate"],
            })

    waves = {int(w["batch_number"]): w for w in artifact.get("waves", [])}
    barriers = {str(row["batch_id"]): row for row in barrier_rows}
    model_row_by_version = {str(row["version_id"]): row for row in model_rows}
    observations_by_coordinate = {
        (row["id"], row["occurred_at"]): row for row in observation_rows
    }
    canonical_model_ids = set(model_row_by_version)
    canonical_relation_ids = {
        str(row["relation_version_id"]) for row in relation_rows
    }
    evidence_by_version: dict[str, list[Mapping[str, Any]]] = {}
    for row in evidence_rows:
        evidence_by_version.setdefault(
            str(row["model_version_id"]), [],
        ).append(row)
    participants_by_relation: dict[str, list[str]] = {}
    for row in participant_rows:
        participants_by_relation.setdefault(
            str(row["relation_version_id"]), [],
        ).append(str(row["model_version_id"]))
    runs_by_id = {str(row["id"]): row for row in run_rows}
    applied_by_trigger = {
        str(row["trigger_id"]): row for row in applied_rows
    }
    batches: list[dict[str, Any]] = []
    prior_model_heads: set[str] = set()
    prior_relation_heads: set[str] = set()
    prior_barrier_id: str | None = None
    prior_barrier_version = 0
    for source_batch in population.batches:
        number = source_batch.batch_number
        wave = waves.get(number, {})
        run = (wave.get("execution") or {}).get("run") or {}
        run_id = str(run.get("id") or "")
        trigger_id = str((wave.get("execution") or {}).get("trigger_id") or "")
        artifact_barrier = wave.get("barrier_receipt") or {}
        barrier = barriers.get(f"p6-batch-{number}") or {}
        signal_ids = [signal.signal_id for signal in source_batch.signals]
        batch_observation_ids = {
            observation_id for observation_id, signal_id in observation_to_signal.items()
            if signal_id in signal_ids
        }
        run_uuid = _uuid(run_id)
        processed_ids: tuple[UUID, ...] = ()
        if (
            run_uuid is not None
            and wave.get("status") == "success"
            and run.get("status") == "success"
        ):
            processed_ids = await proven_batch_observation_ids(
                conn,
                tenant_id=tenant_id,
                run_id=run_uuid,
                expected_observation_ids=batch_observation_ids,
                batch_label=f"p6-batch-{number}",
            )
        expected_model_ids = {
            str(value) for value in barrier.get("expected_model_version_ids") or ()
        }
        expected_relation_ids = {
            str(value)
            for value in barrier.get("expected_relation_version_ids") or ()
        }
        delta_model_ids = expected_model_ids - prior_model_heads
        delta_relation_ids = expected_relation_ids - prior_relation_heads
        batch_models: list[dict[str, Any]] = []
        batch_atomics: list[dict[str, Any]] = []
        for version_id in sorted(delta_model_ids):
            row = model_row_by_version.get(version_id)
            if row is None or row["lifecycle"] != "active":
                continue
            proposition = _json(row["proposition"]) or {}
            canonical_evidence = evidence_by_version.get(version_id, [])
            canonical_observations = {
                evidence_id
                for item in canonical_evidence
                if (evidence_id := _canonical_observation_evidence_id(
                    item,
                    observations_by_coordinate=observations_by_coordinate,
                    observation_to_signal=observation_to_signal,
                )) is not None
            }
            prior_id = row["supersedes_version_id"]
            prior_observations = {
                evidence_id
                for item in evidence_by_version.get(str(prior_id), [])
                if (evidence_id := _canonical_observation_evidence_id(
                    item,
                    observations_by_coordinate=observations_by_coordinate,
                    observation_to_signal=observation_to_signal,
                )) is not None
            } if prior_id else set()
            evidence_ids = sorted(
                canonical_observations - prior_observations,
                key=str,
            )
            evidence_signals = [
                observation_to_signal[value] for value in evidence_ids
            ]
            source_signal = evidence_signals[0] if len(evidence_signals) == 1 else None
            supporting_versions = [
                str(item["evidence_id"])
                for item in canonical_evidence
                if item["evidence_kind"] == "model_version"
                and item["evidence_role"] == "derivation"
                and str(item["source_object_id"]) == str(item["evidence_id"])
                and str(item["evidence_id"]) in model_row_by_version
                and int(item["evidence_version"]) == int(
                    model_row_by_version[str(item["evidence_id"])]["version"]
                )
            ]
            batch_models.append({
                "model_id": str(row["model_id"]),
                "version_id": str(row["version_id"]),
                "source_signal_id": source_signal,
                "proposition": (
                    proposition.get("assertion")
                    or proposition.get("summary")
                    or proposition.get("situation")
                    or row["natural_text"]
                ),
                "natural_text": row["natural_text"],
                "abstraction_level": proposition.get("abstraction_level"),
                "claim_role": proposition.get("claim_role"),
                "lifecycle": row["lifecycle"],
                "scope_refs": (
                    [proposition.get("scope_ref")]
                    if proposition.get("scope_ref") else []
                ),
                "evidence_signal_ids": evidence_signals,
                "supporting_model_version_ids": supporting_versions,
                "prior_version_id": str(prior_id) if prior_id else None,
                "supersedes_version_id": str(prior_id) if prior_id else None,
                "history_retained": bool(prior_id and str(prior_id) in model_row_by_version),
            })
            if (
                proposition.get("abstraction_level") == "atomic"
                and len(evidence_ids) == 1
            ):
                batch_atomics.append({
                    "signal_id": evidence_signals[0],
                    "observation_id": str(evidence_ids[0]),
                    "evidence_bound": (
                        (proposition.get("evidence_contract") or {}).get(
                            "evidence_status"
                        ) == "evidence_bound"
                    ),
                    "tenant_id": str(tenant_id),
                })
        batch_relations: list[dict[str, Any]] = []
        relation_fates: list[dict[str, Any]] = []
        relation_row_by_version: dict[str, Mapping[str, Any]] = {}
        for row in relation_rows:
            relation_version_id = str(row["relation_version_id"])
            if relation_version_id not in delta_relation_ids:
                continue
            relation_row_by_version[relation_version_id] = row
            batch_relations.append({
                "relation_id": str(row["id"]),
                "relation_version_id": relation_version_id,
                "kind": row["relation_kind"],
                "lifecycle": row["lifecycle"],
                "participant_model_version_ids": participants_by_relation.get(
                    relation_version_id, [],
                ),
            })
        retrieved_models: list[str] = []
        retrieved_observations: list[str] = []
        prior_version_for_model = {
            str(model_row_by_version[version_id]["model_id"]): version_id
            for version_id in prior_model_heads
            if version_id in model_row_by_version
        }
        for row in decision_rows:
            if str(row["batch_id"]) not in {run_id, trigger_id}:
                continue
            item_id = str(row["context_item_id"])
            if row["context_item_kind"] == "accepted_model":
                version_id = (
                    item_id if item_id in prior_model_heads
                    else prior_version_for_model.get(item_id)
                )
                if version_id is not None:
                    retrieved_models.append(version_id)
            elif row["context_item_kind"] in {
                "current_episode", "historical_observation",
            } and _uuid(item_id) in observation_to_signal:
                retrieved_observations.append(item_id)
        expected_count = len(expected_model_ids) + len(expected_relation_ids)
        matched_count = (
            len(expected_model_ids & canonical_model_ids)
            + len(expected_relation_ids & canonical_relation_ids)
        )
        missing_count = (
            len(expected_model_ids - canonical_model_ids)
            + len(expected_relation_ids - canonical_relation_ids)
        )
        canonical_heads_exist = missing_count == 0
        exact_chain = bool(barrier) and (
            int(barrier["barrier_version"]) == prior_barrier_version + 1
            and (
                str(barrier["prior_barrier_id"])
                if barrier["prior_barrier_id"] else None
            ) == prior_barrier_id
        )
        receipt_valid = bool(barrier) and (
            barrier["status"] == "complete"
            and int(barrier["truth_critical_pending_count"]) == 0
            and barrier["receipt_digest"] == _barrier_digest(barrier)
            and _artifact_barrier_matches(barrier, artifact_barrier)
            and _artifact_snapshot_matches(
                wave.get("snapshot"),
                expected_model_ids=expected_model_ids,
                expected_relation_ids=expected_relation_ids,
            )
            and canonical_heads_exist
            and exact_chain
        )
        reported_matched_count = matched_count if receipt_valid else 0
        reported_missing_count = expected_count - reported_matched_count

        # Relation lifecycle credit must be caused by this batch's durable
        # apply envelope.  A successor observed between barriers is not enough:
        # it may have been advanced by unrelated work in the same tenant.
        db_run = runs_by_id.get(run_id)
        run_ops = _json(db_run["ops_applied"]) if db_run is not None else {}
        run_ops = run_ops if isinstance(run_ops, Mapping) else {}
        diff_hash = str(run_ops.get("diff_hash") or "")
        db_trigger_id = str(db_run["trigger_id"]) if db_run is not None else ""
        applied = applied_by_trigger.get(db_trigger_id)
        retirement_envelope_valid = bool(
            receipt_valid
            and db_run is not None
            and db_run["status"] == "success"
            and db_run["error"] is None
            and db_trigger_id == trigger_id
            and diff_hash
            and applied is not None
            and applied["outcome"] == "success"
            and applied["diff_hash"] == diff_hash
            and int(run_ops.get("apply_dropped_op_count") or 0) == 0
        )
        applied_retired_relation_ids = {
            str(relation_id)
            for op in run_ops.get("relation_claim_ops") or ()
            if isinstance(op, Mapping) and op.get("status") == "retired"
            for relation_id in op.get("retired_relation_ids") or ()
        }
        for row in relation_rows:
            prior_relation_version_id = row["supersedes_relation_version_id"]
            if (
                prior_relation_version_id is None
                or str(prior_relation_version_id) not in prior_relation_heads
                or str(row["head_relation_version_id"])
                != str(row["relation_version_id"])
                or (
                    number == 4
                    and (
                        not retirement_envelope_valid
                        or str(row["id"]) not in applied_retired_relation_ids
                    )
                )
            ):
                continue
            relation_fates.append({
                "relation_id": str(row["id"]),
                "relation_version_id": str(row["relation_version_id"]),
                "prior_relation_version_id": str(prior_relation_version_id),
                "kind": row["relation_kind"],
                "lifecycle": row["head_lifecycle"],
                "prior_active_head_absent": (
                    receipt_valid
                    and str(prior_relation_version_id) not in expected_relation_ids
                ),
            })

        # A Think run is used as a shared transaction-envelope coordinate only
        # when durable apply, composite, relation, and barrier evidence all
        # agree. It is not relabeled as either canonical truth command ID.
        envelope_valid = bool(
            receipt_valid
            and db_run is not None
            and db_run["status"] == "success"
            and db_run["error"] is None
            and db_trigger_id == trigger_id
            and diff_hash
            and applied is not None
            and applied["outcome"] == "success"
            and applied["diff_hash"] == diff_hash
            and int(run_ops.get("apply_dropped_op_count") or 0) == 0
        )
        applied_model_ids = {
            str(value) for value in run_ops.get("applied_model_ids") or ()
        }
        composite_model_ids = {
            str(op.get("model_id"))
            for op in run_ops.get("claim_ops") or ()
            if isinstance(op, Mapping)
            and op.get("op") == "insert"
            and op.get("quality_decision") == "accept"
            and op.get("abstraction_level") == "composite"
            and str(op.get("model_id") or "") in applied_model_ids
        }
        emitted_composite_ids = {
            str(model_row_by_version[version_id]["model_id"])
            for version_id in delta_model_ids
            if version_id in model_row_by_version
            and (_json(model_row_by_version[version_id]["proposition"]) or {}).get(
                "abstraction_level"
            ) == "composite"
        }
        exact_composite_ids = composite_model_ids & emitted_composite_ids
        relation_ops = {
            str(op.get("canonical_relation_version_id")): str(
                op.get("relation_instance_id")
            )
            for op in run_ops.get("relation_claim_ops") or ()
            if isinstance(op, Mapping)
            and op.get("status") == "accepted"
            and op.get("canonical_relation_version_id")
            and op.get("relation_instance_id")
        }
        exact_relation_versions = {
            version_id
            for version_id, relation_id in relation_ops.items()
            if version_id in relation_row_by_version
            and str(relation_row_by_version[version_id]["id"]) == relation_id
            and str(relation_row_by_version[version_id]["think_run_id"]) == run_id
            and relation_row_by_version[version_id]["instance_status"] in {
                "accepted", "active",
            }
            and relation_row_by_version[version_id]["admission_disposition"]
            == "accepted"
        }
        if envelope_valid and exact_composite_ids and exact_relation_versions:
            for model in batch_models:
                if str(model["model_id"]) in exact_composite_ids:
                    model["commit_id"] = run_id
            for relation in batch_relations:
                if str(relation["relation_version_id"]) in exact_relation_versions:
                    relation["commit_id"] = run_id
        batches.append({
            "batch_number": number, "input_signal_ids": signal_ids,
            "processed_signal_ids": [
                observation_to_signal[value] for value in processed_ids
            ],
            "unbatched_signal_count": len(signal_ids) - len(processed_ids),
            "groundings": [
                item for signal_id in signal_ids
                for item in groundings_by_signal.get(signal_id, ())
            ],
            "atomics": batch_atomics,
            "retrieval": {
                "accepted_model_version_ids": retrieved_models,
                "observation_ids": retrieved_observations,
            },
            "accepted_models": batch_models,
            "accepted_relations": batch_relations,
            "relation_fates": relation_fates,
            "barrier": {
                "snapshot_validated": receipt_valid,
                "expected_head_count": expected_count,
                "matched_head_count": reported_matched_count,
                "stale_head_count": 0,
                "missing_head_count": reported_missing_count,
            },
        })
        if receipt_valid:
            prior_barrier_id = str(barrier["barrier_id"])
            prior_barrier_version = int(barrier["barrier_version"])
            prior_model_heads = expected_model_ids
            prior_relation_heads = expected_relation_ids
    provenance = artifact.get("run_provenance")
    provenance = provenance if isinstance(provenance, Mapping) else {}
    expected_llm = artifact.get("expected_llm_configuration")
    expected_llm = expected_llm if isinstance(expected_llm, Mapping) else {}
    provider_telemetry = artifact.get("provider_telemetry")
    provider_telemetry = (
        provider_telemetry if isinstance(provider_telemetry, Mapping) else {}
    )
    blindness_proven = bool(
        artifact.get("gold_visible_during_execution") is False
        and provenance.get("gold_visible_during_execution") is False
        and provenance.get("population_digest") == artifact.get(
            "population_digest"
        )
        and provenance.get("runtime_identity") == "cf2-provider-free-v1"
        and artifact.get("mixed_llm_attempt_count") == 0
        and expected_llm.get("provider") == "cf2_provider_free"
        and expected_llm.get("model") == "cf2-provider-free-v1"
        and expected_llm.get("transport") == "in_process_provider_free"
        and provider_telemetry.get("provider") == expected_llm.get("provider")
        and provider_telemetry.get("model") == expected_llm.get("model")
    )
    execution_id = canonical_sha256({
        "tenant_id": str(tenant_id),
        "run_ids": [
            str(((waves.get(number, {}).get("execution") or {}).get("run") or {}).get(
                "id"
            ) or "")
            for number in range(1, 5)
        ],
    })
    receipt = {
        "population_digest": artifact.get("population_digest"),
        "execution_id": execution_id,
        "tenant_id": str(tenant_id),
        "batches": batches,
        "contamination": {
            "gold_fields_seen": (
                0 if blindness_proven else ["runtime_blindness_unverified"]
            ),
            "cross_tenant_row_count": cross_tenant,
            "oracle_imported": False if blindness_proven else None,
        },
        "replay_digests": [],
    }
    receipt["replay_digests"] = [semantic_replay_digest(receipt)]
    return receipt


__all__ = ["build_core_fast_path_runtime_receipt"]
