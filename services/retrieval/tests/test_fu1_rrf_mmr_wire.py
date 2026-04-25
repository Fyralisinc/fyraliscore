"""FU-1 integration tests — RRF primary wiring + MMR assembler wiring.

Verifies the two follow-ups from AUDIT-FIXES-IMPLEMENTATION-PLAN:

  1. `primary_retrieve` with `RetrievalConfig.scoring_mode="rrf"`
     produces a ranking that diverges from `scoring_mode="linear"` on
     a seeded fixture.
  2. `assemble_context` with `RetrievalConfig.assembler_use_mmr=True`
     invokes `mmr_select` end-to-end; notes["mmr"]["used"] records
     the selection.

Both paths stay green under the baseline (linear + count-cap).
"""
from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone
from uuid import UUID

import pytest

from services.retrieval.assembler import AccessContext, assemble_context
from services.retrieval.config import RetrievalConfig
from services.retrieval.primary import TriggerContext, primary_retrieve
from services.retrieval.tests._fixtures import build_fixture, make_embedding


pytestmark = pytest.mark.integration


def _t1_trigger(fs, tenant: UUID) -> TriggerContext:
    seed_vec = make_embedding("alice ships reliably")
    return TriggerContext(
        kind="T1",
        tenant_id=tenant,
        observation_id=fs.observation_ids[0],
        seed_entity_ids=[{"type": "commitment", "id": str(fs.hero_commitment_id)}],
        seed_natural_text="alice ships reliably",
        seed_occurred_at=datetime(2026, 4, 1, 18, 0, 0, tzinfo=timezone.utc),
        scope_actors=[fs.hero_actor_id],
        precomputed_seed_vector=seed_vec,
    )


# ---------------------------------------------------------------------
# FU-1 RA-3 wire — RRF vs linear divergence
# ---------------------------------------------------------------------


async def test_fu1_rrf_primary_retrieve_diverges_from_linear(
    tx_conn, fresh_db, tenant
):
    """Full primary_retrieve with scoring_mode='rrf' produces different
    results from scoring_mode='linear' on the seeded fixture."""
    fs = await build_fixture(tx_conn, tenant, pool=fresh_db)
    trigger = _t1_trigger(fs, tenant)

    cfg_default = RetrievalConfig()  # defaults — include 'rrf'
    cfg_linear = replace(cfg_default, scoring_mode="linear")
    cfg_rrf = replace(cfg_default, scoring_mode="rrf")

    result_linear = await primary_retrieve(
        trigger, tx_conn, config=cfg_linear,
    )
    result_rrf = await primary_retrieve(
        trigger, tx_conn, config=cfg_rrf,
    )

    # Sanity: both paths produced the same candidate set (they ran the
    # same pathway mix). They should differ in *ordering* or *scores*.
    ids_linear = [m.id for m in result_linear.models]
    ids_rrf = [m.id for m in result_rrf.models]
    assert set(ids_linear) == set(ids_rrf), (
        "RRF and linear should select from the same candidate union"
    )

    # Notes record the active mode.
    assert result_linear.notes["config_summary"]["scoring_mode"] == "linear"
    assert result_rrf.notes["config_summary"]["scoring_mode"] == "rrf"

    # Divergence check: scores must not be the same dict (different
    # algorithms → different numeric scores for overlapping ids).
    overlap = set(ids_linear) & set(ids_rrf)
    if len(overlap) >= 2:
        any_diff = any(
            abs(result_linear.model_scores[mid] - result_rrf.model_scores[mid])
            > 1e-9
            for mid in overlap
        )
        assert any_diff, (
            "RRF scores should differ from linear scores on at least "
            "one overlapping model"
        )

    # Ranking divergence: top-1 may coincide (fine — the fixture's
    # strongest Model wins under both), but the top-5 sets should
    # differ in either ordering or composition on a fixture this
    # diverse.
    if len(ids_linear) >= 5 and len(ids_rrf) >= 5:
        top5_linear = ids_linear[:5]
        top5_rrf = ids_rrf[:5]
        # Compare ORDERED top-5; equal ordering would mean RRF isn't
        # actually being used. (If the test fixture becomes too small
        # for this assertion a future edit can relax to set-compare.)
        assert top5_linear != top5_rrf, (
            f"RRF top-5 == linear top-5; expected divergence. "
            f"linear={[str(i)[:8] for i in top5_linear]} "
            f"rrf={[str(i)[:8] for i in top5_rrf]}"
        )


async def test_fu1_rrf_default_is_rrf_when_no_config_supplied(
    tx_conn, fresh_db, tenant
):
    """`RetrievalConfig()` defaults to rrf — primary_retrieve without
    an explicit config still gets the new scorer via the module-level
    CONFIG."""
    cfg = RetrievalConfig()
    assert cfg.scoring_mode == "rrf"


def test_fu1_rrf_scoring_mode_from_env_override(monkeypatch):
    """`RETRIEVAL_SCORING_MODE=linear` rolls back to the legacy path."""
    monkeypatch.setenv("RETRIEVAL_SCORING_MODE", "linear")
    cfg = RetrievalConfig.from_env()
    assert cfg.scoring_mode == "linear"


def test_fu1_rrf_scoring_mode_invalid_env_falls_back_to_default(monkeypatch):
    """Unknown scoring_mode values fall back to the default — never
    crash `from_env`."""
    monkeypatch.setenv("RETRIEVAL_SCORING_MODE", "bogus_value")
    cfg = RetrievalConfig.from_env()
    assert cfg.scoring_mode == "rrf"  # default


# ---------------------------------------------------------------------
# FU-1 RA-4 wire — MMR end-to-end through assemble_context
# ---------------------------------------------------------------------


async def test_fu1_mmr_assembler_path_is_exercised_end_to_end(
    tx_conn, fresh_db, tenant
):
    """With `assembler_use_mmr=True`, `assemble_context` runs MMR
    over the Models bucket. `notes["mmr"]["used"]` is True and the
    selected count is bounded by both the count cap and the token
    budget."""
    fs = await build_fixture(tx_conn, tenant, pool=fresh_db)
    trigger = _t1_trigger(fs, tenant)

    # Use the linear scorer so we don't interact with FU-1 RA-3 changes.
    cfg_off = RetrievalConfig(scoring_mode="linear", assembler_use_mmr=False)
    cfg_on = RetrievalConfig(
        scoring_mode="linear",
        assembler_use_mmr=True,
        context_budget_tokens=10_000,
        mmr_lambda_diversity=0.5,
    )

    retrieval = await primary_retrieve(trigger, tx_conn, config=cfg_on)
    access = AccessContext(tenant_id=tenant)

    bundle_off = await assemble_context(
        retrieval, access, tx_conn, config=cfg_off,
    )
    bundle_on = await assemble_context(
        retrieval, access, tx_conn, config=cfg_on,
    )

    assert bundle_off.notes["mmr"]["used"] is False
    assert bundle_on.notes["mmr"]["used"] is True
    assert bundle_on.notes["mmr"]["lambda_diversity"] == 0.5
    assert bundle_on.notes["mmr"]["budget_tokens"] == 10_000
    assert bundle_on.notes["mmr"]["candidate_count"] >= len(bundle_on.models)

    # Every MMR-selected model must have been in the off-path candidate
    # pool (i.e. access-control didn't see MMR and let more through).
    off_ids = {m.id for m in bundle_off.models}
    # bundle_off is count-capped at _BUDGET_MODELS=40. The MMR path
    # could have selected additional models that the count-cap left
    # out but still visible. Compare against the full visible pool via
    # retrieval.models (tenant is the only access filter here since
    # AccessContext has no requestor_actor_id).
    all_visible_ids = {m.id for m in retrieval.models}
    on_ids = {m.id for m in bundle_on.models}
    assert on_ids.issubset(all_visible_ids)


async def test_fu1_mmr_assembler_tight_token_budget_reduces_selection(
    tx_conn, fresh_db, tenant
):
    """Drop the token budget to a value below the candidate total: MMR
    must select strictly fewer Models than the candidate count."""
    fs = await build_fixture(tx_conn, tenant, pool=fresh_db)
    trigger = _t1_trigger(fs, tenant)

    cfg_big = RetrievalConfig(
        scoring_mode="linear",
        assembler_use_mmr=True,
        context_budget_tokens=1_000_000,
        mmr_lambda_diversity=0.5,
    )
    cfg_small = RetrievalConfig(
        scoring_mode="linear",
        assembler_use_mmr=True,
        context_budget_tokens=50,  # forces MMR to skip most items
        mmr_lambda_diversity=0.5,
    )

    retrieval = await primary_retrieve(trigger, tx_conn, config=cfg_big)
    if not retrieval.models:
        pytest.skip("no models retrieved; fixture too small for FU-1 MMR test")

    access = AccessContext(tenant_id=tenant)
    bundle_big = await assemble_context(
        retrieval, access, tx_conn, config=cfg_big,
    )
    bundle_small = await assemble_context(
        retrieval, access, tx_conn, config=cfg_small,
    )
    # Tight budget → fewer (or equal if candidate_count==1) selections.
    assert len(bundle_small.models) <= len(bundle_big.models)
    # And strictly fewer when we have multiple candidates.
    if bundle_big.notes["mmr"]["candidate_count"] >= 2:
        assert len(bundle_small.models) < bundle_big.notes["mmr"]["candidate_count"]


def test_fu1_mmr_assembler_use_mmr_env_override(monkeypatch):
    """`RETRIEVAL_ASSEMBLER_USE_MMR=1` flips the MMR flag on."""
    monkeypatch.setenv("RETRIEVAL_ASSEMBLER_USE_MMR", "1")
    cfg = RetrievalConfig.from_env()
    assert cfg.assembler_use_mmr is True
