import json

import pytest

from scripts.compiled_facet_decision_provider import CompiledFacetDecisionProvider
from services.reasoning.think.compiled_reasoning import BatchMemoryDecisionSet


@pytest.mark.asyncio
async def test_generic_provider_satisfies_active_compiled_contract_and_consumes_models():
    provider = CompiledFacetDecisionProvider()
    raw = await provider._raw_call(
        system="closed world candidate adjudication",
        user="""
        [FACET subject=omega value=usage_drop]
        <candidate>
          candidate_id: "MDC_1"
          proposed_text: "compress omega evidence"
        </candidate>
        <allowed_model_cards>
          - id=00000000-0000-0000-0000-000000000001 natural=omega evidence facets: audit, security
        </allowed_model_cards>
        """,
        temperature=0, max_tokens=1200, schema_hint=None,
    )
    parsed = BatchMemoryDecisionSet.model_validate(json.loads(raw))
    assert len(parsed.decisions) == 1
    assert parsed.decisions[0].operation == "claim"
    assert parsed.decisions[0].confidence >= 0.55
    assert parsed.decisions[0].claim_text == (
        "omega evidence facets: audit, security, usage_drop"
    )
    assert "00000000-0000-0000-0000-000000000001" in parsed.reasoning_trace
