from __future__ import annotations

import inspect
import json
import os
from types import SimpleNamespace

import asyncpg
import pytest

from lib.shared.ids import uuid7
from lib.shared.types import ModelCreate
from services.domain.models.repo import ModelsRepo
from services.reasoning.think import run_pipeline
from services.reasoning.think.representation_contract import (
    _maybe_classify_lifecycle_phase,
)
from services.reasoning.think.splitter import (
    _atomic_proposition,
    _operational_atomic_proposition,
)
from services.reasoning.think.truth_admission import admit_validated_think_claim


SCOPE = [{"type": "workstream", "canonical_ref": "project:atlas"}]


def _observation(text: str, *, content: dict | None = None):
    return SimpleNamespace(id=uuid7(), content_text=text, content=content or {})


def _prior(scope=SCOPE):
    return SimpleNamespace(
        id=uuid7(), status="active", scope_entities=scope,
        proposition={"claim_role": "situation"},
    )


@pytest.mark.parametrize(("text", "phase"), (
    ("A second independent source confirms the earlier ownership state.", "corroboration"),
    ("The dashboard says complete, but the owner says the review is still open.", "contradiction"),
    ("The owner corrected the timestamp; the prior completion was wrong.", "correction"),
    ("The final audit shows the migration completed without the prior delay.", "external_outcome"),
))
def test_phase_producer_uses_explicit_semantics_and_same_scope(
    text: str, phase: str,
) -> None:
    prop = {"kind": "belief", "claim_role": "fact"}
    obs = _observation(text)
    _maybe_classify_lifecycle_phase(
        {"scope_entities": SCOPE}, prop, [obs], [_prior()]
    )
    assert prop["lifecycle_phase"] == phase
    assert prop["lifecycle_phase_basis"]["exact_observation_ids"] == [str(obs.id)]
    assert prop["lifecycle_phase_basis"]["compared_model_ids"]
    assert prop["lifecycle_phase_basis"]["classifier_version"] == (
        "explicit-scope-semantics-v1"
    )


def test_phase_producer_initial_structured_priority_and_fail_closed_cases() -> None:
    initial = {"kind": "belief", "claim_role": "fact"}
    _maybe_classify_lifecycle_phase(
        {"scope_entities": SCOPE}, initial, [_observation("Owner is unclear.")], []
    )
    assert initial["lifecycle_phase"] == "weak_initial"

    explicit = {"kind": "belief", "claim_role": "fact"}
    _maybe_classify_lifecycle_phase(
        {"scope_entities": SCOPE}, explicit,
        [_observation("Routine update.", content={"lifecycle_phase": "correction"})],
        [_prior()],
    )
    assert explicit["lifecycle_phase"] == "correction"
    assert explicit["lifecycle_phase_basis"]["semantic_cues"] == [
        "explicit_source_semantics"
    ]

    generic = {"kind": "belief", "claim_role": "fact"}
    _maybe_classify_lifecycle_phase(
        {"scope_entities": SCOPE}, generic,
        [_observation("The status changed this afternoon.")], [_prior()],
    )
    assert "lifecycle_phase" not in generic

    invalid = {"kind": "belief", "claim_role": "fact"}
    _maybe_classify_lifecycle_phase(
        {"scope_entities": SCOPE}, invalid,
        [_observation("Owner is unclear.", content={"lifecycle_phase": "batch_7"})],
        [],
    )
    assert "lifecycle_phase" not in invalid

    cross_scope = {"kind": "belief", "claim_role": "fact"}
    _maybe_classify_lifecycle_phase(
        {"scope_entities": SCOPE}, cross_scope,
        [_observation("A second source confirms the state.")],
        [_prior([{"type": "project", "canonical_ref": "project:beacon"}])],
    )
    assert cross_scope["lifecycle_phase"] == "weak_initial"
    assert cross_scope["lifecycle_phase"] != "corroboration"


def test_splitters_preserve_phase_and_auditable_basis() -> None:
    basis = {
        "classifier_version": "explicit-scope-semantics-v1",
        "exact_observation_ids": [str(uuid7())],
    }
    original = {
        "subject": "Atlas", "lifecycle_phase": "correction",
        "lifecycle_phase_basis": basis,
    }
    operational = _operational_atomic_proposition(
        [{"summary": "Owner corrected", "evidence": "timestamp revised"}],
        original,
    )
    ordinary = _atomic_proposition("The timestamp was revised", "state", original)
    for proposition in (operational, ordinary):
        assert proposition["lifecycle_phase"] == "correction"
        assert proposition["lifecycle_phase_basis"] == basis
        assert proposition["lifecycle_phase_basis"] is not basis


def test_pipeline_runs_phase_producer_before_synthesis_evolution() -> None:
    source = inspect.getsource(run_pipeline)
    assert source.index("enrich_raw_diff_representation(raw_diff") < source.index(
        "maybe_inject_synthesis_evolution_obligations(raw_diff"
    )


@pytest.mark.asyncio
async def test_lifecycle_phase_round_trips_through_canonical_admission() -> None:
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        pytest.skip("DATABASE_URL required")
    conn = await asyncpg.connect(dsn)
    tx = conn.transaction()
    await tx.start()
    try:
        tenant_id, observation_id = uuid7(), uuid7()
        await conn.execute(
            "INSERT INTO tenants(id,name) VALUES($1,$2)", tenant_id,
            f"phase-roundtrip-{tenant_id}",
        )
        await conn.execute(
            """INSERT INTO observations(
                 id,tenant_id,occurred_at,kind,source_channel,content,content_text,
                 embedding_pending,trust_tier
               ) VALUES($1,$2,now(),'signal','test','{}','Owner corrected the timestamp',
                        TRUE,'authoritative')""",
            observation_id, tenant_id,
        )
        basis = {
            "classifier_version": "explicit-scope-semantics-v1",
            "exact_observation_ids": [str(observation_id)],
            "semantic_cues": ["corrected"],
            "compared_model_ids": [],
        }
        proposed = ModelCreate(
            tenant_id=tenant_id, born_from_event_id=observation_id,
            proposition={
                "kind": "belief", "claim_role": "fact", "subject": "Atlas",
                "assertion": "Owner corrected the timestamp",
                "lifecycle_phase": "correction", "lifecycle_phase_basis": basis,
            },
            natural="Owner corrected the Atlas timestamp",
            embedding=[0.0] * 768, scope_temporal={}, confidence=0.75,
            confidence_at_assertion=0.75,
            supporting_event_ids=[observation_id],
        )
        row = await admit_validated_think_claim(
            conn, proposed=proposed,
            evidence_observation_ids=(observation_id,),
            models_repo=ModelsRepo(None, embedder=None),
        )
        proposition = await conn.fetchval(
            "SELECT proposition FROM accepted_current_models WHERE tenant_id=$1 AND id=$2",
            tenant_id, row.id,
        )
        if isinstance(proposition, str):
            proposition = json.loads(proposition)
        assert proposition["lifecycle_phase"] == "correction"
        assert proposition["lifecycle_phase_basis"] == basis
    finally:
        await tx.rollback()
        await conn.close()
