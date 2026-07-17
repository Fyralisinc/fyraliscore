import json
from pathlib import Path

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


def test_v5_receipt_metadata_preserves_one_shot_failure_without_overclaim():
    metadata = json.loads(
        (Path(__file__).with_name("company_model_holdout_v5_receipt_metadata.json"))
        .read_text()
    )
    assert metadata["run_attempts"] == 1
    assert metadata["rerun_performed"] is False
    assert metadata["producer_tuned_after_result"] is False
    assert metadata["status"] == "inconclusive_runtime_contract_failure"
    assert metadata["generalization_claim"] == "unproven"
    assert metadata["manifest_digest"] == MANIFEST_DIGEST_V5
    assert metadata["corpus_digest"] == CORPUS_DIGEST_V5
