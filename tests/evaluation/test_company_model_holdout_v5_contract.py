from lib.contracts.kernel import canonical_sha256
from tests.evaluation.company_model_holdout_v5 import (
    BATCHES_V5, CORPUS_DIGEST_V5, MANIFEST_DIGEST_V5, MANIFEST_V5,
)


def test_v5_holdout_is_new_batched_and_frozen_before_execution():
    assert len(MANIFEST_V5["hidden_theses"]) == 5
    assert len(BATCHES_V5) == 5
    assert all(len(batch) == 10 for batch in BATCHES_V5)
    assert len({signal for batch in BATCHES_V5 for signal in batch}) == 50
    assert MANIFEST_DIGEST_V5 == canonical_sha256(MANIFEST_V5)
    assert CORPUS_DIGEST_V5 == canonical_sha256({"batches": BATCHES_V5})
    subjects = {thesis["thesis_id"] for thesis in MANIFEST_V5["hidden_theses"]}
    assert subjects == {"helios", "juniper", "kestrel", "lumen", "mosaic"}


def test_v5_hidden_truth_is_absent_from_producer_corpus():
    corpus = " ".join(f"{subject} {facet}" for batch in BATCHES_V5
                      for subject, facet in batch).casefold()
    for thesis in MANIFEST_V5["hidden_theses"]:
        assert thesis["truth"].casefold() not in corpus
