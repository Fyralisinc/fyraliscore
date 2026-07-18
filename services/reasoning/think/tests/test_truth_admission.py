from __future__ import annotations

import os
import json
from datetime import datetime, timezone
from uuid import uuid4

import asyncpg
import pytest
from pydantic import ValidationError as PydanticValidationError

from lib.shared.errors import InvariantViolation, ValidationError
from lib.shared.types import ModelCreate
from lib.contracts.truth_evidence import (
    ClaimScopeBinding, ClaimScopeRole, ScopeSubjectKind,
)
from lib.contracts.truth_admission import ModelTruthLifecycle, ModelTruthTransition
from services.domain.models.repo import ModelsRepo
from services.reasoning.think.applier import (
    _prepare_claim_insert_model,
    _with_claim_evidence_defaults,
)
from services.reasoning.think.diff_schema import ClaimOp
from services.reasoning.think.truth_admission import (
    _scope_canonical_ref,
    _scope_display_label,
    _synthesis_member_ids,
    _synthesis_member_evidence,
    _validate_synthesis_relation,
    admit_validated_think_claim,
    advance_validated_think_model,
)
from services.reasoning.think.reconciler import _synthesis_scope_identical
from services.reasoning.think.validator import _validate_claim_op


def test_candidate_scope_canonical_provenance_survives_uuid_derivation() -> None:
    entity = {"type": "commitment", "id": "commitment:cobalt-renewal"}
    proposition = {
        "scope_ref": "commitment:cobalt-renewal",
        "scope_label": "Cobalt renewal",
    }

    assert _scope_canonical_ref(entity) == "commitment:cobalt-renewal"
    assert _scope_display_label(entity, proposition) == "Cobalt renewal"


def test_synthesis_scope_match_is_exact_and_fail_closed() -> None:
    atlas = {
        "scope_entities": [{
            "type": "workstream", "id": "workstream:atlas-release",
            "canonical_ref": "workstream:atlas-release",
        }],
        "proposition": {"scope_ref": "workstream:atlas-release"},
    }
    beacon = {
        "scope_entities": [{
            "type": "workstream", "id": "workstream:beacon-migration",
            "canonical_ref": "workstream:beacon-migration",
        }],
        "proposition": {"scope_ref": "workstream:beacon-migration"},
    }
    assert _synthesis_scope_identical(atlas, dict(atlas)) is True
    assert _synthesis_scope_identical(atlas, beacon) is False
    assert _synthesis_scope_identical(atlas, {"scope_entities": []}) is False
    scopes = (
        "workstream:atlas-release", "workstream:beacon-migration",
        "commitment:cobalt-renewal", "workstream:delta-handoff",
    )
    entries = [{
        "scope_entities": [{"canonical_ref": scope}],
        "proposition": {"scope_ref": scope},
    } for scope in scopes]
    for left_index, left in enumerate(entries):
        for right_index, right in enumerate(entries):
            assert _synthesis_scope_identical(left, right) is (
                left_index == right_index
            )


def test_synthesis_requires_uuid_members_and_structured_mechanism() -> None:
    members = [uuid4(), uuid4()]
    assert _synthesis_member_ids({
        "member_model_ids": [str(value) for value in members]
    }) == tuple(members)
    _validate_synthesis_relation({
        "member_model_ids": [str(value) for value in members],
        "supported_relation": {
            "kind": "dependency_constraint",
            "mechanism": "approval gates downstream completion",
            "source_model_id": str(members[0]),
            "target_model_id": str(members[1]),
            "source_model_version_id": str(uuid4()),
            "target_model_version_id": str(uuid4()),
        }
    })
    with pytest.raises(InvariantViolation, match="supported_relation"):
        _validate_synthesis_relation({
            "shared_mechanism": "these things appear together"
        })


@pytest.mark.asyncio
async def test_synthesis_members_resolve_exact_active_versions_as_lineage() -> None:
    tenant_id, synthesis_id = uuid4(), uuid4()
    members, versions = [uuid4(), uuid4()], [uuid4(), uuid4()]
    now = datetime.now(timezone.utc)

    class Connection:
        async def fetch(self, _query, _tenant_id, member_ids):
            assert _tenant_id == tenant_id
            assert member_ids == members
            return [{
                "model_id": member_id, "version_id": version_id,
                "version": index + 2, "semantic_digest": "a" * 64,
                "created_at": now, "canonical_refs": ["workstream:atlas"],
            } for index, (member_id, version_id) in enumerate(zip(members, versions))]

    refs = await _synthesis_member_evidence(
        Connection(), tenant_id=tenant_id,
        proposition={
            "member_model_ids": [str(value) for value in members],
            "supported_relation": {
                "kind": "causal_influence",
                "mechanism": "ownership churn delays completion",
                "source_model_id": str(members[0]),
                "target_model_id": str(members[1]),
                "source_model_version_id": str(versions[0]),
                "target_model_version_id": str(versions[1]),
            },
        },
        scope_refs=frozenset({"workstream:atlas"}),
        model_id=synthesis_id, admitted_at=now,
    )
    assert [ref.evidence_id for ref in refs] == list(map(str, versions))
    assert [ref.evidence_version for ref in refs] == [2, 3]
    assert all(ref.kind.value == "model_version" for ref in refs)
    with pytest.raises(InvariantViolation, match="exact active member heads"):
        await _synthesis_member_evidence(
            Connection(), tenant_id=tenant_id,
            proposition={
                "member_model_ids": [str(value) for value in members],
                "supported_relation": {
                    "kind": "causal_influence",
                    "mechanism": "ownership churn delays completion",
                    "source_model_id": str(members[0]),
                    "target_model_id": str(members[1]),
                    "source_model_version_id": str(uuid4()),
                    "target_model_version_id": str(versions[1]),
                },
            },
            scope_refs=frozenset({"workstream:atlas"}),
            model_id=synthesis_id, admitted_at=now,
        )


def _claim(*, supporting_event_ids=(), born_from_event_id=None) -> ClaimOp:
    entry = {
        "tenant_id": str(uuid4()),
        "proposition": {"kind": "state", "subject": "Atlas", "assertion": "blocked"},
        "natural": "Atlas is blocked",
        "supporting_event_ids": list(supporting_event_ids),
    }
    if born_from_event_id is not None:
        entry["born_from_event_id"] = born_from_event_id
    return ClaimOp(op="insert", entry=entry)


def test_multi_signal_trigger_is_never_inherited_as_claim_evidence() -> None:
    trigger_ids = [uuid4(), uuid4()]
    result = _with_claim_evidence_defaults(
        _claim(), trigger_cause_event_id=None,
        trigger_supporting_event_ids=trigger_ids,
    )
    assert result.entry is not None
    assert result.entry["supporting_event_ids"] == []
    assert "born_from_event_id" not in result.entry


def test_explicit_claim_local_evidence_is_not_widened_to_batch() -> None:
    local_id, unrelated_id, synthetic_born_id = uuid4(), uuid4(), uuid4()
    result = _with_claim_evidence_defaults(
        _claim(
            supporting_event_ids=[local_id],
            born_from_event_id=synthetic_born_id,
        ),
        trigger_cause_event_id=None,
        trigger_supporting_event_ids=[local_id, unrelated_id],
    )
    assert result.entry is not None
    assert result.entry["supporting_event_ids"] == [local_id]
    assert result.entry["born_from_event_id"] == synthetic_born_id


def test_manifest_bound_atomic_cannot_resurrect_stale_sibling_evidence() -> None:
    tenant_id = uuid4()
    observation_ids = [uuid4() for _ in range(12)]

    for local_id in observation_ids:
        entry = {
            "tenant_id": str(tenant_id),
            "proposition": {
                "kind": "state",
                "subject": "Cobalt renewal",
                "assertion": f"Atomic signal {local_id}",
                # Deliberately emulate the stale pre-split proposition state.
                "evidence_event_ids": [str(value) for value in observation_ids],
                "evidence_observation_manifest": [{
                    "observation_id": str(local_id),
                    "body": f"Atomic signal {local_id}",
                    "source_channel": "test",
                }],
                "contextual_frame": {
                    "observation_ids": [], "source_channels": [],
                },
                "evidence_contract": {
                    "evidence_status": "needs_evidence",
                    "supporting_event_count": 0,
                },
            },
            "natural": f"Atomic signal {local_id}",
            "supporting_event_ids": [str(local_id)],
            "evidence_observation_manifest": [{
                "observation_id": str(local_id),
                "body": f"Atomic signal {local_id}",
                "source_channel": "test",
            }],
            "scope_actors": [],
            "scope_entities": [],
            "scope_temporal": {},
            "confidence": 0.58,
            "confidence_at_assertion": 0.58,
        }
        op = ClaimOp(op="insert", entry=entry)

        prepared = _prepare_claim_insert_model(
            op,
            tenant_id,
            cause_event_id=None,
            trigger_supporting_event_ids=observation_ids,
        )

        assert prepared.supporting_event_ids == [local_id]
        assert prepared.proposition["evidence_event_ids"] == [str(local_id)]
        assert prepared.proposition["contextual_frame"]["observation_ids"] == [
            str(local_id)
        ]
        assert prepared.proposition["contextual_frame"]["source_channels"] == [
            "test"
        ]
        assert prepared.proposition["evidence_contract"]["evidence_status"] == (
            "evidence_bound"
        )
        assert prepared.proposition["evidence_contract"][
            "supporting_event_count"
        ] == 1


def test_single_signal_trigger_may_supply_evidence_fallback() -> None:
    only_id = uuid4()
    result = _with_claim_evidence_defaults(
        _claim(), trigger_cause_event_id=None,
        trigger_supporting_event_ids=[only_id],
    )
    assert result.entry is not None
    assert result.entry["supporting_event_ids"] == [only_id]
    assert result.entry["born_from_event_id"] == only_id


@pytest.mark.asyncio
async def test_governed_admission_persists_exact_claim_local_evidence() -> None:
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        pytest.skip("DATABASE_URL is required for the PostgreSQL admission proof")
    conn = await asyncpg.connect(dsn)
    transaction = conn.transaction()
    await transaction.start()
    try:
        tenant_id, observation_id = uuid4(), uuid4()
        await conn.execute(
            "INSERT INTO tenants (id,name) VALUES ($1,$2)", tenant_id, "think-admission-proof",
        )
        await conn.execute(
            """
            INSERT INTO observations
              (id,tenant_id,occurred_at,kind,source_channel,content,content_text,
               embedding,embedding_pending,trust_tier)
            VALUES ($1,$2,now(),'signal','test','{}'::jsonb,$3,$4,FALSE,'authoritative')
            """,
            observation_id, tenant_id, "Atlas release certificate is blocked",
            "[" + ",".join(["0"] * 768) + "]",
        )
        proposed = ModelCreate(
            tenant_id=tenant_id, born_from_event_id=observation_id,
            proposition={"kind": "state", "subject": "Atlas", "assertion": "blocked"},
            natural="Atlas is blocked on its release certificate",
            embedding=[0.0] * 768, scope_temporal={}, confidence=0.7,
            confidence_at_assertion=0.7, supporting_event_ids=[observation_id],
        )
        synthetic_born_id, missing_id = uuid4(), uuid4()
        valid_op = ClaimOp(op="insert", entry={
            **proposed.model_dump(mode="json"),
            "born_from_event_id": str(synthetic_born_id),
            "supporting_event_ids": [str(observation_id)],
        })
        validated_op = await _validate_claim_op(
            valid_op, None, conn, tenant_id=tenant_id,
        )
        assert validated_op.entry is not None
        assert validated_op.entry["born_from_event_id"] == str(synthetic_born_id)
        invalid_entry = dict(valid_op.entry or {})
        invalid_entry["supporting_event_ids"] = [
            str(observation_id), str(missing_id),
        ]
        with pytest.raises(ValidationError, match=str(missing_id)):
            await _validate_claim_op(
                ClaimOp(op="insert", entry=invalid_entry), None, conn,
                tenant_id=tenant_id,
            )
        row = await admit_validated_think_claim(
            conn, proposed=proposed, evidence_observation_ids=(observation_id,),
            models_repo=ModelsRepo(None, embedder=None),
        )
        assert await conn.fetchval(
            "SELECT count(*) FROM accepted_current_models WHERE tenant_id=$1 AND id=$2",
            tenant_id, row.id,
        ) == 1
        claim_evidence_ref = await conn.fetchval(
            """
            SELECT evidence.reference_id
            FROM model_truth_evidence_references evidence
            JOIN model_truth_heads head
              ON head.tenant_id=evidence.tenant_id
             AND head.version_id=evidence.model_version_id
            WHERE head.tenant_id=$1 AND head.model_id=$2
            """, tenant_id, row.id,
        )
        scoped_subject = uuid4()
        revised_proposition = {
            "kind": "state", "subject": "Atlas", "assertion": "blocked",
            "qualifier": "release certificate",
        }
        revised_natural = "Atlas release is blocked by its certificate."
        revised_falsifier = {"kind": "observation_pattern", "pattern": "certificate clears"}
        supporting_model_id = uuid4()
        resolved_at = datetime(2026, 7, 10, 10, 0, tzinfo=timezone.utc)
        revised_scope = (ClaimScopeBinding(
            subject_id=scoped_subject, subject_kind=ScopeSubjectKind.PROJECT,
            role=ClaimScopeRole.SUBJECT,
            claim_local_evidence_refs=(claim_evidence_ref,),
        ),)
        revised_temporal_scope = {"valid_from": "2026-07-10T09:00:00+00:00"}
        first_command = await advance_validated_think_model(
            conn, tenant_id=tenant_id, model_id=row.id, confidence=0.61,
            evidence_observation_ids=(observation_id,), reason_code="focused-proof",
            proposition=revised_proposition, falsifier=revised_falsifier,
            natural=revised_natural,
            evidential_weight=0.73, supporting_model_ids=(supporting_model_id,),
            visible_to_subjects=False, resolution_outcome=False,
            resolved_at=resolved_at, scope=revised_scope,
            temporal_scope=revised_temporal_scope,
        )
        replay_command = await advance_validated_think_model(
            conn, tenant_id=tenant_id, model_id=row.id, confidence=0.61,
            evidence_observation_ids=(observation_id,), reason_code="focused-proof",
            proposition=revised_proposition, falsifier=revised_falsifier,
            natural=revised_natural,
            evidential_weight=0.73, supporting_model_ids=(supporting_model_id,),
            visible_to_subjects=False, resolution_outcome=False,
            resolved_at=resolved_at, scope=revised_scope,
            temporal_scope=revised_temporal_scope,
        )
        assert replay_command == first_command
        assert await conn.fetchval(
            "SELECT count(*) FROM model_truth_versions WHERE tenant_id=$1 AND model_id=$2",
            tenant_id, row.id,
        ) == 2
        assert await conn.fetchval(
            "SELECT confidence FROM accepted_current_models WHERE tenant_id=$1 AND id=$2",
            tenant_id, row.id,
        ) == pytest.approx(0.61)
        projected = await conn.fetchrow(
            """
            SELECT proposition,"natural",falsifier,evidential_weight,visible_to_subjects,
                   supporting_model_ids,resolution_outcome,resolved_at,
                   scope_entities,scope_temporal
            FROM models WHERE tenant_id=$1 AND id=$2
            """, tenant_id, row.id,
        )
        assert json.loads(projected["proposition"]) == revised_proposition
        assert projected["natural"] == revised_natural
        assert await conn.fetchval(
            """SELECT natural_text FROM model_truth_versions
               WHERE tenant_id=$1 AND model_id=$2 AND version=2""",
            tenant_id,
            row.id,
        ) == revised_natural
        assert json.loads(projected["falsifier"]) == revised_falsifier
        assert float(projected["evidential_weight"]) == pytest.approx(0.73)
        assert projected["visible_to_subjects"] is False
        assert list(projected["supporting_model_ids"]) == [supporting_model_id]
        assert projected["resolution_outcome"] is False
        assert projected["resolved_at"] == resolved_at
        assert str(scoped_subject) in str(projected["scope_entities"])
        assert "valid_from" in str(projected["scope_temporal"])
        assert await conn.fetchval(
            "SELECT confidence FROM models WHERE tenant_id=$1 AND id=$2",
            tenant_id, row.id,
        ) == pytest.approx(0.61)
        # Every accepted semantic compatibility field is command-guarded.  The
        # truth-kernel projection above is the only permitted write path.
        forbidden_mutations = (
            "proposition = '{}'::jsonb", "\"natural\" = 'bypass'",
            f"scope_actors = ARRAY['{scoped_subject}']::uuid[]",
            "scope_entities = '[]'::jsonb",
            "scope_temporal = '{}'::jsonb", "confidence = 0.62",
            "falsifier = '{}'::jsonb", "supporting_event_ids = ARRAY[]::uuid[]",
            "supporting_model_ids = ARRAY[]::uuid[]", "evidential_weight = 0.74",
            "visible_to_subjects = TRUE", "resolution_outcome = TRUE",
            "resolved_at = NULL", "status = 'archived'",
            "archived_at = now()", "archive_reason = 'bypass'",
        )
        for mutation in forbidden_mutations:
            savepoint = conn.transaction()
            await savepoint.start()
            with pytest.raises(asyncpg.RaiseError, match="truth-kernel command"):
                await conn.execute(
                    f"UPDATE models SET {mutation} WHERE tenant_id=$1 AND id=$2",
                    tenant_id, row.id,
                )
            await savepoint.rollback()
        assert await conn.fetchval(
            """
            SELECT count(*)
            FROM model_truth_evidence_references evidence
            JOIN model_truth_versions version
              ON version.tenant_id=evidence.tenant_id
             AND version.version_id=evidence.model_version_id
            JOIN model_truth_heads head
              ON head.tenant_id=version.tenant_id
             AND head.version_id=version.version_id
            WHERE evidence.tenant_id=$1 AND version.model_id=$2
              AND evidence.evidence_id=$3
            """,
            tenant_id, row.id, str(observation_id),
        ) == 1
        missing_id = uuid4()
        with pytest.raises(InvariantViolation, match="same tenant") as raised:
            await admit_validated_think_claim(
                conn, proposed=proposed.model_copy(update={"id": uuid4()}),
                evidence_observation_ids=(missing_id,),
                models_repo=ModelsRepo(None, embedder=None),
            )
        assert raised.value.context["missing"] == [str(missing_id)]
        assert raised.value.context["found"] == []
    finally:
        await transaction.rollback()
        await conn.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("transition,expected", [
    (ModelTruthTransition.CONTEST, ModelTruthLifecycle.DISPUTED),
    (ModelTruthTransition.FALSIFY, ModelTruthLifecycle.FALSIFIED),
    (ModelTruthTransition.SUPERSEDE, ModelTruthLifecycle.SUPERSEDED),
    (ModelTruthTransition.ARCHIVE, ModelTruthLifecycle.ARCHIVED),
])
async def test_postgres_lifecycle_matrix_hides_nonactive_truth_and_blocks_terminals(
    transition: ModelTruthTransition, expected: ModelTruthLifecycle,
) -> None:
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        pytest.skip("DATABASE_URL is required for the PostgreSQL lifecycle proof")
    conn = await asyncpg.connect(dsn)
    transaction = conn.transaction()
    await transaction.start()
    try:
        tenant_id, support_id, challenge_id = uuid4(), uuid4(), uuid4()
        await conn.execute(
            "INSERT INTO tenants (id,name) VALUES ($1,$2)",
            tenant_id, f"truth-lifecycle-{transition.value}",
        )
        for observation_id, text in (
            (support_id, "Atlas is blocked"),
            (challenge_id, "Atlas is no longer blocked"),
        ):
            await conn.execute(
                """
                INSERT INTO observations
                  (id,tenant_id,occurred_at,kind,source_channel,content,content_text,
                   embedding,embedding_pending,trust_tier)
                VALUES ($1,$2,now(),'signal','test','{}'::jsonb,$3,$4,FALSE,'authoritative')
                """, observation_id, tenant_id, text,
                "[" + ",".join(["0"] * 768) + "]",
            )
        proposed = ModelCreate(
            tenant_id=tenant_id, born_from_event_id=support_id,
            proposition={"kind": "state", "subject": "Atlas", "assertion": "blocked"},
            natural="Atlas is blocked", embedding=[0.0] * 768,
            scope_temporal={}, confidence=0.7, confidence_at_assertion=0.7,
            supporting_event_ids=[support_id],
        )
        row = await admit_validated_think_claim(
            conn, proposed=proposed, evidence_observation_ids=(support_id,),
            models_repo=ModelsRepo(None, embedder=None),
        )
        await advance_validated_think_model(
            conn, tenant_id=tenant_id, model_id=row.id, confidence=0.4,
            evidence_observation_ids=(challenge_id,), transition=transition,
            reason_code=f"matrix-{transition.value}",
        )
        head = await conn.fetchrow(
            "SELECT lifecycle,version FROM model_truth_heads WHERE tenant_id=$1 AND model_id=$2",
            tenant_id, row.id,
        )
        assert head["lifecycle"] == expected.value
        assert head["version"] == 2
        assert await conn.fetchval(
            "SELECT count(*) FROM accepted_current_models WHERE tenant_id=$1 AND id=$2",
            tenant_id, row.id,
        ) == 0
        assert await conn.fetchval(
            """
            SELECT count(*) FROM model_truth_evidence_references evidence
            JOIN model_truth_heads head ON head.tenant_id=evidence.tenant_id
              AND head.version_id=evidence.model_version_id
            WHERE head.tenant_id=$1 AND head.model_id=$2
              AND evidence.evidence_role='counterevidence'
            """, tenant_id, row.id,
        ) == 1
        if expected.terminal:
            with pytest.raises(
                (PydanticValidationError, ValidationError, InvariantViolation),
                match="terminal",
            ):
                await advance_validated_think_model(
                    conn, tenant_id=tenant_id, model_id=row.id, confidence=0.8,
                    evidence_observation_ids=(), transition=ModelTruthTransition.CONFIRM,
                    reason_code=f"matrix-resurrect-{transition.value}",
                )
    finally:
        await transaction.rollback()
        await conn.close()
