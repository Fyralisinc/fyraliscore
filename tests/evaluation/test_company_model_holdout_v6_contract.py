from tests.evaluation.company_model_holdout_v6 import BATCHES_V6, MANIFEST_V6


def test_v6_is_small_distinct_and_has_five_hidden_theses():
    assert len(BATCHES_V6) == 5
    assert len(MANIFEST_V6["hidden_theses"]) == 5
    assert all(len(batch) == 10 for batch in BATCHES_V6)
    assert {row["thesis_id"] for row in MANIFEST_V6["hidden_theses"]} == {
        "orion", "prairie", "quartz", "rivet", "solace"
    }
