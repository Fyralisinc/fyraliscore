from tests.evaluation.company_model_holdout_v7 import BATCHES_V7, MANIFEST_V7


def test_v7_requires_cross_batch_evidence_for_every_thesis():
    assert len(BATCHES_V7) == 5
    theses = MANIFEST_V7["hidden_theses"]
    assert len(theses) == 5
    for thesis in theses:
        subject = thesis["thesis_id"]
        appearances = [batch for batch in BATCHES_V7 if any(row[0] == subject for row in batch)]
        assert len(appearances) == 2
        assert all(sum(row[0] == subject for row in batch) == 5 for batch in appearances)
        assert len(thesis["required_groups"]) == 10
