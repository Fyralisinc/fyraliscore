import json

import pytest

from scripts.run_integrated_company_learning_vertical import (
    INTEGRATED_BATCHES,
    _IntegratedDecisionProvider,
)


def test_integrated_vertical_has_five_learning_batches_and_no_singletons():
    assert len(INTEGRATED_BATCHES) == 5
    assert all(len(batch) >= 2 for batch in INTEGRATED_BATCHES)


@pytest.mark.asyncio
async def test_same_subject_model_materially_changes_synthesis():
    base = """[FACET subject=mercury value=current_risk]
<candidate>
candidate_id: MDC_1
proposed_text: mercury synthesis
</candidate>"""
    provider = _IntegratedDecisionProvider()
    without_prior = json.loads(await provider._raw_call(
        system="bounded", user=base, temperature=0, max_tokens=1000,
        schema_hint=None,
    ))
    with_prior = json.loads(await provider._raw_call(
        system="bounded",
        user=base + "\n- id=00000000-0000-0000-0000-000000000001 natural=Mercury is blocked.",
        temperature=0, max_tokens=1000, schema_hint=None,
    ))
    assert "prior_blocked" not in without_prior["decisions"][0]["claim_text"]
    assert "prior_blocked" in with_prior["decisions"][0]["claim_text"]
    assert "00000000-0000-0000-0000-000000000001" in with_prior["reasoning_trace"]
