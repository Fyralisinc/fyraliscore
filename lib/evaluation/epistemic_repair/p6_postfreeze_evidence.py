"""Gold-blind extraction of immutable P6 evaluation facts after execution.

This module is not imported by the production runner.  It receives only the
tenant and the preregistered signal-ID manifest, reconstructs deterministic
observation coordinates, and freezes generic database facts for the pure
post-freeze scorer.
"""

from __future__ import annotations

import json
from typing import Any, Iterable
from uuid import NAMESPACE_URL, UUID, uuid5

from lib.contracts.kernel import canonical_sha256


def _as_value(value: Any) -> Any:
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, dict):
        return {str(key): _as_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_as_value(item) for item in value]
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return value


def _rows(rows: Iterable[Any]) -> list[dict[str, Any]]:
    return [_as_value(dict(row)) for row in rows]


def _json_list(value: Any) -> list[dict[str, Any]]:
    if isinstance(value, str):
        value = json.loads(value)
    return [dict(item) for item in value or () if isinstance(item, dict)]


def _signal_fate_rows(
    observation_to_signal: dict[str, str],
    *,
    observed_ids: set[str],
    boundary_by_signal: dict[str, dict[str, Any]],
    mention_rows: list[dict[str, Any]],
    claims: list[dict[str, Any]],
    context_items: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    dispositions = {
        str(row["source_signal_ids"][0]): row
        for row in context_items
        if row.get("context_item_kind") == "candidate"
        and row.get("decision_fate") == "justified_noop"
        and row.get("result_object_kind") in {
            "open_question",
            "clarification_residual",
        }
        and len(row.get("source_signal_ids") or ()) == 1
    }
    fates = []
    for observation_id, signal_id in observation_to_signal.items():
        canonical = any(
            signal_id in claim.get("evidence_signal_ids", ()) for claim in claims
        )
        disposition = dispositions.get(signal_id)
        fates.append({
            "signal_id": signal_id,
            "observation_id": observation_id,
            "boundary_fate": "assigned" if signal_id in boundary_by_signal else None,
            "mention_fate": "mention" if any(
                str(row.get("source_observation_id")) == observation_id
                and row.get("fate") == "detected"
                for row in mention_rows
            ) else "no_mention" if observation_id in observed_ids else None,
            "mutation_fate": (
                "canonical_mutation" if canonical
                else str(disposition["result_object_kind"]) if disposition
                else "no_mutation" if observation_id in observed_ids else None
            ),
            "mutation_reason": (
                "nonassertable_signal_retained_outside_truth"
                if disposition else None
            ),
            "disposition_decision_id": (
                disposition.get("decision_id") if disposition else None
            ),
        })
    return fates, list(dispositions.values())


async def extract_p6_postfreeze_evidence(
    conn: Any, *, tenant_id: UUID, signal_ids: tuple[str, ...],
    boundary_decisions: tuple[dict[str, Any], ...] = (),
) -> dict[str, Any]:
    """Extract evaluation facts without importing sealed P6 gold."""

    observation_to_signal = {
        str(uuid5(NAMESPACE_URL, f"p6-think:{tenant_id}:{signal_id}")): signal_id
        for signal_id in signal_ids
    }
    observations = _rows(await conn.fetch(
        """SELECT id,occurred_at,source_channel,content_text,entities_mentioned
           FROM observations WHERE tenant_id=$1 AND id=ANY($2::uuid[])
           ORDER BY occurred_at,id""",
        tenant_id, list(map(UUID, observation_to_signal)),
    ))
    observed_ids = {str(row["id"]) for row in observations}

    mention_rows = _rows(await conn.fetch(
        """SELECT detection.id,detection.source_observation_id,
                  detection.candidate_surface,detection.fate,detection.mention,
                  trace.current_fate AS grounding_fate,trace.selected_referent
           FROM entity_mention_detections detection
           JOIN entity_mention_detection_heads head
             ON head.tenant_id=detection.tenant_id
            AND head.current_detection_id=detection.id
           LEFT JOIN grounding_traces trace
             ON trace.tenant_id=detection.tenant_id
            AND trace.source_observation_id=detection.source_observation_id
            AND trace.phrase=detection.candidate_surface
           WHERE detection.tenant_id=$1
             AND detection.source_observation_id=ANY($2::uuid[])
           ORDER BY detection.source_observation_id,detection.id""",
        tenant_id, list(map(UUID, observation_to_signal)),
    ))
    mentions = []
    for row in mention_rows:
        mention = row.get("mention") or {}
        if isinstance(mention, str):
            mention = json.loads(mention)
        referent = row.get("selected_referent") or {}
        if isinstance(referent, str):
            referent = json.loads(referent)
        if row.get("fate") == "detected":
            anchor = mention.get("primary_anchor") or {}
            coordinate = anchor.get("coordinate") or {}
            grounding_fate = row.get("grounding_fate") or mention.get("grounding_fate")
            resolved = grounding_fate in {"resolved", "resolved_for_consumer"}
            resolved_ref = (
                referent.get("canonical_ref")
                or referent.get("id")
                or referent.get("canonical_referent_id")
            ) if resolved else None
            mentions.append({
                "id": row["id"],
                "signal_id": observation_to_signal.get(str(row["source_observation_id"])),
                "surface": mention.get("surface") or row.get("candidate_surface"),
                "span_start": coordinate.get("span_start"),
                "span_end": coordinate.get("span_end"),
                "entity_type": mention.get("provisional_entity_type"),
                "canonical_ref": resolved_ref
                or mention.get("provisional_canonical_ref"),
                "canonical_ref_status": "resolved" if resolved_ref
                else mention.get("canonical_ref_status"),
                "normalization_version": mention.get("normalization_version"),
                "grounding_fate": grounding_fate,
            })

    claim_rows = _rows(await conn.fetch(
        """SELECT model.id,model.truth_version_id,model.natural_text,
                  model.proposition,model.confidence,model.truth_lifecycle,
                  COALESCE((SELECT jsonb_agg(jsonb_build_object(
                    'kind',ref.evidence_kind,'id',ref.evidence_id,
                    'role',ref.evidence_role,'reference_id',ref.reference_id)
                    ORDER BY ref.reference_id)
                    FROM model_truth_evidence_references ref
                    WHERE ref.tenant_id=model.tenant_id
                      AND ref.model_version_id=model.truth_version_id),'[]'::jsonb)
                    AS evidence,
                  COALESCE((SELECT jsonb_agg(jsonb_build_object(
                    'id',binding.subject_id,'type',binding.subject_kind,
                    'role',binding.scope_role,
                    'canonical_ref',COALESCE(binding.canonical_ref, CASE
                      WHEN actor.id IS NOT NULL THEN 'actor:' || actor.id::text
                      WHEN resource.id IS NOT NULL THEN 'resource:' || resource.id::text
                      ELSE NULL END),
                    'display_label',binding.display_label,
                    'canonical_ref_status',binding.canonical_ref_status,
                    'normalization_version',binding.normalization_version)
                    ORDER BY binding.binding_id)
                    FROM model_truth_scope_bindings binding
                    LEFT JOIN actors actor
                      ON actor.tenant_id=binding.tenant_id
                     AND actor.id=binding.subject_id
                     AND binding.subject_kind='person'
                    LEFT JOIN resources resource
                      ON resource.tenant_id=binding.tenant_id
                     AND resource.id=binding.subject_id
                    WHERE binding.tenant_id=model.tenant_id
                      AND binding.model_version_id=model.truth_version_id),'[]'::jsonb)
                    AS scope_entities
           FROM accepted_current_models model WHERE model.tenant_id=$1
           ORDER BY model.truth_advanced_at,model.id""",
        tenant_id,
    ))
    claims = []
    version_to_claim: dict[str, str] = {}
    for row in claim_rows:
        evidence = _json_list(row.pop("evidence"))
        row["scope_entities"] = _json_list(row.get("scope_entities"))
        evidence_signal_ids = [
            observation_to_signal[str(item.get("id"))]
            for item in evidence
            if item.get("kind") == "observation"
            and str(item.get("id")) in observation_to_signal
        ]
        claim = {
            **row,
            "evidence_signal_ids": evidence_signal_ids,
            "evidence_references": evidence,
        }
        claims.append(claim)
        version_to_claim[str(row["truth_version_id"])] = str(row["id"])

    relation_rows = _rows(await conn.fetch(
        """SELECT relation.id,relation.truth_relation_version_id,
                  relation.truth_relation_kind AS relation_kind,
                  relation.truth_rationale AS rationale,
                  COALESCE((SELECT jsonb_agg(jsonb_build_object(
                    'claim_id',participant.model_id,'model_version_id',
                    participant.model_version_id,'role',participant.role,
                    'ordinal',participant.ordinal) ORDER BY participant.ordinal)
                    FROM relation_truth_participants participant
                    WHERE participant.tenant_id=relation.tenant_id
                      AND participant.relation_version_id=
                          relation.truth_relation_version_id),'[]'::jsonb)
                    AS participants
           FROM accepted_current_relations relation WHERE relation.tenant_id=$1
           ORDER BY relation.truth_advanced_at,relation.id""",
        tenant_id,
    ))
    for row in relation_rows:
        row["participants"] = _json_list(row.get("participants"))

    lifecycle = _rows(await conn.fetch(
        """SELECT event.lifecycle_event_id AS id,event.model_id,event.transition AS action,
                  event.to_version_id,event.occurred_at,
                  COALESCE((SELECT array_agg(ref.evidence_id ORDER BY ref.evidence_id)
                    FROM model_truth_evidence_references ref
                    WHERE ref.tenant_id=event.tenant_id
                      AND ref.model_version_id=event.to_version_id
                      AND ref.evidence_kind='observation'),'{}'::text[])
                    AS evidence_observation_ids
           FROM model_truth_lifecycle_events event WHERE event.tenant_id=$1
           ORDER BY event.occurred_at,event.lifecycle_event_id""",
        tenant_id,
    ))
    for row in lifecycle:
        row["evidence_signal_ids"] = [
            observation_to_signal[str(value)]
            for value in row.pop("evidence_observation_ids") or ()
            if str(value) in observation_to_signal
        ]

    decisions = _rows(await conn.fetch(
        """SELECT decision.decision_id,decision.batch_id,decision.route_id,
                  decision.context_item_kind,
                  context_item_id,retrieved,selected,included,referenced,
                  necessary_background,historical_reopen_reason,decision_fate,
                  result_object_kind,result_object_id,evidence_lineage,decided_at,
                  trigger.payload AS trigger_payload,
                  run.id AS think_run_id,
                  artifact.payload -> 'context_use' AS context_use
           FROM company_learning_context_decisions decision
           LEFT JOIN think_trigger_queue trigger
             ON trigger.tenant_id=decision.tenant_id
            AND trigger.id::text=decision.batch_id
           LEFT JOIN think_runs run
             ON run.tenant_id=trigger.tenant_id AND run.trigger_id=trigger.id
           LEFT JOIN LATERAL (
             SELECT candidate.payload
             FROM think_run_artifacts candidate
             WHERE candidate.tenant_id=run.tenant_id
               AND candidate.run_id=run.id AND candidate.stage='apply'
             ORDER BY candidate.captured_at DESC,candidate.id DESC LIMIT 1
           ) artifact ON TRUE
           WHERE decision.tenant_id=$1
           ORDER BY decision.decided_at,decision.decision_id""",
        tenant_id,
    ))
    context_items = []
    claim_sources = {
        str(claim["id"]): list(claim.get("evidence_signal_ids") or ())
        for claim in claims
    }
    expected_context_opportunities: set[tuple[str, str]] = set()
    emitted_context_opportunities: set[tuple[str, str]] = set()
    context_manifest_present = bool(decisions)
    for row in decisions:
        source_signal_id = observation_to_signal.get(str(row.get("context_item_id")))
        item_kind = (
            "observation" if "observation" in str(row["context_item_kind"])
            or row["context_item_kind"] == "current_episode"
            else "model" if row["context_item_kind"] == "accepted_model"
            else row["context_item_kind"]
        )
        source_ids = (
            [source_signal_id] if source_signal_id
            else claim_sources.get(str(row.get("context_item_id")), [])
        )
        payload = row.pop("trigger_payload", None) or {}
        context_use = row.pop("context_use", None) or {}
        think_run_id = str(row.pop("think_run_id", None) or "")
        if isinstance(payload, str):
            payload = json.loads(payload)
        if isinstance(context_use, str):
            context_use = json.loads(context_use)
        target_ids = [
            observation_to_signal[str(value)]
            for value in payload.get("observation_ids") or ()
            if str(value) in observation_to_signal
        ]
        selected_ids = {
            str(value)
            for key in (
                "selected_observation_ids", "selected_model_ids",
                "graph_selected_model_ids",
            )
            for value in context_use.get(key) or ()
        }
        if not selected_ids:
            context_manifest_present = False
        expected_context_opportunities.update(
            (think_run_id, selected_id) for selected_id in selected_ids
        )
        if row.get("selected"):
            emitted_context_opportunities.add(
                (think_run_id, str(row["context_item_id"]))
            )
        output_evidence_signal_ids = claim_sources.get(
            str(row.get("result_object_id")), []
        )
        context_items.append({
            **row,
            "think_run_id": think_run_id,
            "input_signal_ids": target_ids,
            "source_signal_ids": source_ids,
            "output_evidence_signal_ids": output_evidence_signal_ids,
            "batch_number": (
                int(target_ids[0].split("-")[1][1:]) if target_ids else None
            ),
            "context_item_kind": item_kind,
        })

    refresh_events = _rows(await conn.fetch(
        """SELECT id,projection_name,projection_version,subject_key,status,
                  attempts,processed_at,created_at
           FROM projection_refresh_jobs WHERE tenant_id=$1
           ORDER BY created_at,id""",
        tenant_id,
    ))
    for row in refresh_events:
        row["refresh_key"] = ":".join(map(str, (
            row.get("projection_name"), row.get("projection_version"),
            row.get("subject_key"),
        )))

    resolved_rows = _rows(await conn.fetch(
        """SELECT version.model_id,version.confidence,version.resolution_outcome,
                  ref.evidence_id AS outcome_observation_id
           FROM model_truth_versions version
           JOIN model_truth_evidence_references ref
             ON ref.tenant_id=version.tenant_id
            AND ref.model_version_id=version.version_id
            AND ref.evidence_kind='observation'
           WHERE version.tenant_id=$1
             AND version.resolution_outcome IS NOT NULL
             AND version.confidence IS NOT NULL
           ORDER BY version.model_id,version.version,ref.evidence_id""",
        tenant_id,
    ))
    resolved_outcomes = [{
        "model_id": row["model_id"],
        "confidence": float(row["confidence"]),
        "resolution_outcome": bool(row["resolution_outcome"]),
        "outcome_signal_id": observation_to_signal.get(
            str(row["outcome_observation_id"])
        ),
    } for row in resolved_rows if str(row["outcome_observation_id"])
       in observation_to_signal]

    hg_counts = dict(await conn.fetchrow(
        """SELECT
          (SELECT count(*) FROM accepted_current_models model
             WHERE model.tenant_id=$1 AND NOT EXISTS (
               SELECT 1 FROM model_truth_evidence_references ref
               WHERE ref.tenant_id=model.tenant_id
                 AND ref.model_version_id=model.truth_version_id))::int
            AS accepted_models_without_evidence,
          (SELECT count(*) FROM accepted_current_relations relation
             WHERE relation.tenant_id=$1 AND (
               SELECT count(*) FROM relation_truth_participants participant
               WHERE participant.tenant_id=relation.tenant_id
                 AND participant.relation_version_id=
                     relation.truth_relation_version_id) < 2)::int
            AS accepted_relations_without_participants,
          (SELECT count(*) FROM truth_repair_obligations obligation
             WHERE obligation.tenant_id=$1
               AND obligation.status IN ('pending','in_progress'))::int
            AS open_truth_repair_obligations,
          (SELECT count(*) FROM think_trigger_queue trigger
             WHERE trigger.tenant_id=$1 AND trigger.completed_at IS NULL)::int
            AS pending_truth_triggers""",
        tenant_id,
    ))
    hg_gates = {key: int(value) == 0 for key, value in hg_counts.items()}

    active_candidates = _rows(await conn.fetch(
        """SELECT id,proposed_at FROM pattern_candidates
           WHERE tenant_id=$1 AND promoted_at IS NULL AND rejected_at IS NULL""",
        tenant_id,
    ))
    active_reviews = _rows(await conn.fetch(
        "SELECT id,created_at FROM entity_review_queue WHERE tenant_id=$1 AND resolved_at IS NULL",
        tenant_id,
    ))

    causal_credits = _rows(await conn.fetch(
        """SELECT outcome_link_id AS id,decision_id,outcome_object_kind,
                  outcome_object_id,evidence_lineage
           FROM company_learning_outcome_links WHERE tenant_id=$1
           ORDER BY observed_at,outcome_link_id""",
        tenant_id,
    ))

    boundary_by_signal = {
        str(row.get("signal_id")): dict(row) for row in boundary_decisions
        if row.get("signal_id")
    }
    scope_coordinates = [
        entity
        for claim in claims
        for entity in claim.get("scope_entities") or ()
    ]
    resolved_scope_statuses = {"resolved", "accepted"}
    resolved_scope_coordinates = [
        entity for entity in scope_coordinates
        if entity.get("canonical_ref")
        and str(entity.get("canonical_ref_status") or "").lower()
        in resolved_scope_statuses
    ]
    provisional_scope_coordinates = [
        entity for entity in scope_coordinates
        if entity.get("canonical_ref")
        and str(entity.get("canonical_ref_status") or "").lower()
        == "provisional"
    ]
    typed_scope_coordinates = [
        entity for entity in scope_coordinates
        if entity.get("canonical_ref")
        and entity.get("type")
        and str(entity.get("canonical_ref_status") or "").lower()
        in (resolved_scope_statuses | {"provisional"})
    ]
    extracted_scope_complete = bool(scope_coordinates) and (
        len(typed_scope_coordinates) == len(scope_coordinates)
    )
    extracted_scope_status = (
        "complete" if extracted_scope_complete
        else "partial" if typed_scope_coordinates
        else "missing"
    )
    signal_fates, uncertainty_dispositions = _signal_fate_rows(
        observation_to_signal,
        observed_ids=observed_ids,
        boundary_by_signal=boundary_by_signal,
        mention_rows=mention_rows,
        claims=claims,
        context_items=context_items,
    )
    evidence = {
        "schema_version": "epistemic-repair-p6-postfreeze-evidence-v1",
        "tenant_id": str(tenant_id),
        "observation_signal_map": observation_to_signal,
        "observed_source_ids": sorted(observed_ids),
        "signal_fates": signal_fates,
        "uncertainty_dispositions": uncertainty_dispositions,
        "boundaries": list(boundary_by_signal.values()),
        "mentions": mentions,
        "claims": claims,
        "relations": relation_rows,
        "lifecycle_events": lifecycle,
        "context_items": context_items,
        "context_opportunities_complete": (
            context_manifest_present
            and bool(expected_context_opportunities)
            and emitted_context_opportunities == expected_context_opportunities
        ),
        "context_opportunity_counts": {
            "expected": len(expected_context_opportunities),
            "emitted": len(emitted_context_opportunities),
            "missing": len(expected_context_opportunities - emitted_context_opportunities),
            "unexpected": len(emitted_context_opportunities - expected_context_opportunities),
        },
        "scope_coordinates_canonical": bool(scope_coordinates) and (
            len(resolved_scope_coordinates) == len(scope_coordinates)
        ),
        "extracted_scope_coordinates_complete": extracted_scope_complete,
        "extracted_scope_coordinates_status": extracted_scope_status,
        "extracted_scope_coordinate_counts": {
            "total": len(scope_coordinates),
            "typed": len(typed_scope_coordinates),
            "resolved": len(resolved_scope_coordinates),
            "provisional": len(provisional_scope_coordinates),
            "incomplete": len(scope_coordinates) - len(typed_scope_coordinates),
        },
        "scope_coordinate_counts": {
            "total": len(scope_coordinates),
            "resolved": len(resolved_scope_coordinates),
            "provisional": len(provisional_scope_coordinates),
            "unresolved": (
                len(scope_coordinates)
                - len(resolved_scope_coordinates)
                - len(provisional_scope_coordinates)
            ),
        },
        "refresh_events": refresh_events,
        "active_candidates": active_candidates,
        "active_reviews": active_reviews,
        "causal_credits": causal_credits,
        "resolved_outcomes": resolved_outcomes,
        "hg_gates": hg_gates,
    }
    evidence["query_receipts"] = [
        {
            "query_name": name,
            "row_count": len(value) if isinstance(value, list) else len(value),
            "result_digest": canonical_sha256(value),
        }
        for name, value in (
            ("observations", observations), ("mentions", mention_rows),
            ("claims", claims), ("relations", relation_rows),
            ("lifecycle", lifecycle), ("context_decisions", decisions),
            ("refresh_events", refresh_events),
            ("resolved_outcomes", resolved_outcomes), ("hg_counts", hg_counts),
            ("active_candidates", active_candidates), ("active_reviews", active_reviews),
            ("causal_credits", causal_credits),
        )
    ]
    payload = {
        **evidence,
        "source_digest": canonical_sha256(evidence),
    }
    return payload


__all__ = ["extract_p6_postfreeze_evidence"]
