import json

import pytest

from lib.shared.ids import uuid7
from scripts.run_bounded_company_model_ablation_db import (
    BATCHES,
    MANIFEST,
    FacetCompressionProvider,
)


def test_hidden_truth_is_not_present_in_producer_signal_population() -> None:
    signal_text = " ".join(
        f"[FACET subject={subject} value={facet}]"
        for batch in BATCHES
        for subject, facet in batch
    ).lower()
    assert len(BATCHES) == 3
    assert all(len(batch) == 6 for batch in BATCHES)
    for thesis in MANIFEST["hidden_theses"]:
        assert thesis["truth"].lower() not in signal_text


@pytest.mark.asyncio
async def test_producer_compresses_only_facets_visible_in_runtime_context() -> None:
    provider = FacetCompressionProvider(
        trigger_id=uuid7(), tenant_id=uuid7(), event_id=uuid7(), actor_id=uuid7()
    )
    value = await provider._raw_call(
        system="runtime producer without hidden truth",
        user=(
            "[FACET subject=atlas value=audit] "
            "[FACET subject=atlas value=usage_drop]"
        ),
        temperature=0.0,
        max_tokens=100,
        schema_hint=None,
    )
    payload = json.loads(value)
    assert len(payload["claim_ops"]) == 1
    natural = payload["claim_ops"][0]["entry"]["natural"]
    assert natural == "atlas evidence facets: audit, usage_drop"
    assert "procurement_wait" not in natural
    assert MANIFEST["hidden_theses"][0]["truth"] not in natural


@pytest.mark.asyncio
async def test_v4_producer_generically_consumes_and_cites_selected_model_summary() -> None:
    selected_id = uuid7()
    provider = FacetCompressionProvider(
        trigger_id=uuid7(), tenant_id=uuid7(), event_id=uuid7(), actor_id=uuid7(),
        consume_model_summaries=True,
    )
    value = await provider._raw_call(
        system="runtime producer without hidden truth",
        user=(
            f"- id={selected_id} detail=full kind=belief role=fact "
            "retrieval=selected natural=omega evidence facets: audit, security\n"
            "[FACET subject=omega value=usage_drop]"
        ),
        temperature=0.0,
        max_tokens=100,
        schema_hint=None,
    )
    payload = json.loads(value)
    entry = payload["claim_ops"][0]["entry"]

    assert entry["natural"] == "omega evidence facets: audit, security, usage_drop"
    assert entry["supporting_model_ids"] == [str(selected_id)]
    assert str(selected_id) in payload["reasoning_trace"]
