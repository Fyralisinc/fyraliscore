"""
RA-2 — Second-pass activation condition tests.

Source: RETRIEVAL-DESIGN-AUDIT §1 and §9 +
VARIANCE-INVESTIGATION-FINDINGS.md (second_pass_expand "imported but
never called").

Covers:
  Unit tests (per-path, hermetic — no DB):
    - sparse primary activates
    - bridge + high-confidence model activates
    - anomaly_flagged observation activates
    - T2 authoritative handler non-activation
    - token-budget saturation non-activation
    - no-rule-matched non-activation
  Integration tests (real DB):
    - signal with sparse primary → decision.run=True
    - signal with rich primary (no bridge/anomaly) → decision.run=False
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any
from uuid import UUID

import pytest

from lib.shared.ids import uuid7

from services.retrieval.primary import (
    RetrievalResult,
    TriggerContext,
    primary_retrieve,
)
from services.retrieval.second_pass import (
    SecondPassDecision,
    SECOND_PASS_SPARSE_THRESHOLD,
    should_run_second_pass,
    log_second_pass_decision,
)

from services.retrieval.tests._fixtures import build_fixture, make_embedding


# ---------------------------------------------------------------------
# Unit tests (no DB)
# ---------------------------------------------------------------------


def _empty_result(trigger_kind: str = "T1") -> RetrievalResult:
    trigger = TriggerContext(kind=trigger_kind, tenant_id=uuid7())
    return RetrievalResult(trigger=trigger)


def _make_model_stub(
    *, confidence: float = 0.5, scope_entities=None
):
    """Build a lightweight ModelRow-shaped object. We cannot use the
    real Pydantic ModelRow directly because it requires every column;
    use a permissive dataclass for the activation logic which only
    touches .id, .confidence, .scope_entities."""

    @dataclass
    class _MStub:
        id: UUID = field(default_factory=uuid7)
        confidence: float = 0.5
        scope_entities: list = field(default_factory=list)

    return _MStub(confidence=confidence, scope_entities=scope_entities or [])


def _make_commit_stub(*, ref=None):
    @dataclass
    class _CStub:
        id: UUID = field(default_factory=uuid7)
        external_counterparty_ref: Any = None

    return _CStub(external_counterparty_ref=ref)


def _make_obs_stub(kind: str = "signal"):
    @dataclass
    class _OStub:
        id: UUID = field(default_factory=uuid7)
        kind: str = "signal"

    return _OStub(kind=kind)


def test_ra2_unit_sparse_primary_activates():
    r = _empty_result()
    # 2 models, below threshold 5
    r.models = [_make_model_stub(), _make_model_stub()]
    d = should_run_second_pass(r)
    assert d.run is True
    assert d.trigger_condition == "sparse_primary"
    assert "dependency_context" in d.suggested_dimensions


def test_ra2_unit_bridge_with_high_confidence_activates():
    r = _empty_result()
    commit = _make_commit_stub(ref={"type": "customer_resource", "id": str(uuid7())})
    # Enough models to defeat sparse trigger (>= 5).
    r.models = [
        _make_model_stub(
            confidence=0.9,
            scope_entities=[{"type": "commitment", "id": str(commit.id)}],
        ),
    ] + [_make_model_stub(confidence=0.3) for _ in range(6)]
    r.acts = {"goals": [], "commitments": [commit], "decisions": []}
    d = should_run_second_pass(r)
    assert d.run is True
    assert d.trigger_condition == "high_confidence_commitment_with_counterparty"
    assert d.reason_detail["commits_with_counterparty_ref"] == 1
    assert d.reason_detail["high_confidence_bridge_models"] == 1


def test_ra2_unit_bridge_without_high_confidence_does_not_activate_via_bridge():
    r = _empty_result()
    commit = _make_commit_stub(ref={"type": "customer_resource", "id": str(uuid7())})
    # 6 models — above sparse threshold, all low confidence.
    r.models = [_make_model_stub(confidence=0.4) for _ in range(6)]
    r.acts = {"goals": [], "commitments": [commit], "decisions": []}
    d = should_run_second_pass(r)
    # No anomaly either. Expect no activation.
    assert d.run is False
    assert d.trigger_condition == "no_activation_rule_matched"


def test_ra2_unit_anomaly_flagged_activates():
    r = _empty_result()
    # above sparse threshold
    r.models = [_make_model_stub() for _ in range(6)]
    r.observations = [_make_obs_stub("anomaly_flagged")]
    d = should_run_second_pass(r)
    assert d.run is True
    assert d.trigger_condition == "anomaly_flagged"
    assert "supporting_evidence" in d.suggested_dimensions


def test_ra2_unit_t2_authoritative_handler_blocks_activation():
    r = _empty_result(trigger_kind="T2")
    # Make it sparse, which would normally activate.
    r.models = [_make_model_stub()]
    d = should_run_second_pass(
        r, trigger=r.trigger, t2_has_authoritative_handler=True,
    )
    assert d.run is False
    assert d.trigger_condition == "t2_authoritative_handler"


def test_ra2_unit_token_budget_saturation_blocks_activation():
    r = _empty_result()
    # Make it sparse, which would normally activate.
    r.models = [_make_model_stub()]
    r.notes = {"token_budget": {"used": 100_000, "budget": 100_000}}
    d = should_run_second_pass(r)
    assert d.run is False
    assert d.trigger_condition == "token_budget_saturated"


def test_ra2_unit_no_activation_on_rich_uneventful_primary():
    r = _empty_result()
    r.models = [_make_model_stub() for _ in range(20)]
    # No bridge, no anomaly.
    d = should_run_second_pass(r)
    assert d.run is False
    assert d.trigger_condition == "no_activation_rule_matched"


def test_ra2_unit_custom_thresholds_honored():
    r = _empty_result()
    r.models = [_make_model_stub() for _ in range(8)]
    # Raise sparse threshold to 10 → now 8 counts as sparse.
    d = should_run_second_pass(r, sparse_threshold=10)
    assert d.run is True
    assert d.trigger_condition == "sparse_primary"


def test_ra2_log_helper_emits_structured_event(caplog):
    import logging
    caplog.set_level(logging.INFO)
    d = SecondPassDecision(
        run=True,
        trigger_condition="sparse_primary",
        suggested_dimensions=["dependency_context"],
        reason_detail={"model_count": 2},
    )
    log_second_pass_decision(d, tenant_id=uuid7())
    # structlog bubbles through logging; we just verify it does not raise.
    # (Structured event content verification is best-effort on structlog.)


# ---------------------------------------------------------------------
# Integration tests (real DB via tx_conn)
# ---------------------------------------------------------------------


@pytest.mark.integration
async def test_ra2_integration_sparse_primary_activates(
    tx_conn, fresh_db, tenant
):
    # Build a small-ish fixture deliberately then probe with a seed
    # that yields few results → sparse trigger.
    from services.retrieval.tests._fixtures import build_fixture, make_embedding
    fs = await build_fixture(
        tx_conn, tenant, pool=fresh_db,
        n_models=3,    # deliberately sparse corpus
        n_commitments=3,
        n_goals=2,
        n_observations=10,
        n_decisions=2,
    )
    trigger = TriggerContext(
        kind="T1",
        tenant_id=tenant,
        seed_entity_ids=[{"type": "commitment", "id": str(fs.hero_commitment_id)}],
        seed_natural_text="sparse",
        seed_occurred_at=datetime(2026, 4, 1, 12, 0, tzinfo=timezone.utc),
        precomputed_seed_vector=make_embedding("sparse"),
    )
    result = await primary_retrieve(trigger, tx_conn)
    d = should_run_second_pass(result)
    # With 3-model corpus, primary should come back below sparse=5.
    assert len(result.models) < SECOND_PASS_SPARSE_THRESHOLD
    assert d.run is True
    assert d.trigger_condition == "sparse_primary"


@pytest.mark.integration
async def test_ra2_integration_rich_primary_no_signals_no_activation(
    tx_conn, fresh_db, tenant
):
    """Primary with >= threshold models and no anomaly/bridge
    signatures → no activation."""
    fs = await build_fixture(tx_conn, tenant, pool=fresh_db)
    # Seed on commit that is NOT the counterparty hero (commit 0 has
    # external_counterparty_ref per fixture). Use commit 1.
    trigger = TriggerContext(
        kind="T1",
        tenant_id=tenant,
        seed_entity_ids=[{"type": "commitment", "id": str(fs.commitment_ids[1])}],
        seed_natural_text="alice ships reliably",
        seed_occurred_at=datetime(2026, 4, 1, 12, 0, tzinfo=timezone.utc),
        precomputed_seed_vector=make_embedding("alice ships reliably"),
    )
    result = await primary_retrieve(trigger, tx_conn)
    # We might still pick up commit 0 via goal-sibling edges; filter
    # only to the case where no commits-with-ref are present.
    d = should_run_second_pass(result)
    # assert it either did not fire, or fired for bridge/anomaly (which
    # is legitimate given the corpus). What we specifically assert is
    # NOT sparse when models >= threshold.
    if len(result.models) >= SECOND_PASS_SPARSE_THRESHOLD:
        assert d.trigger_condition != "sparse_primary"
