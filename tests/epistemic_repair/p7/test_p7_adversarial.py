import pytest

from lib.contracts.kernel import canonical_sha256
from lib.evaluation.epistemic_repair.p7_runner import P7Artifact, build_p7_artifact


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("hidden_model_access_count", 1),
        ("forbidden_mutation_count", 1),
        ("hidden_label_access_count", 1),
        ("budget_asymmetry_count", 1),
    ),
)
def test_rehashed_guard_violation_cannot_keep_mechanical_success(field: str, value: int) -> None:
    artifact = build_p7_artifact()
    payload = artifact.model_dump(mode="json", exclude={"content_digest"})
    payload["guards"][0][field] = value
    payload["content_digest"] = canonical_sha256(payload)
    with pytest.raises(ValueError):
        P7Artifact.model_validate(payload)


def test_rehashed_corruption_persistence_cannot_keep_success() -> None:
    artifact = build_p7_artifact()
    payload = artifact.model_dump(mode="json", exclude={"content_digest"})
    corrupted = next(row for row in payload["member_results"] if row["arm_id"] == "corrupted")
    corrupted["unsafe_corrupted_persistence"] = 1
    payload["content_digest"] = canonical_sha256(payload)
    with pytest.raises(ValueError):
        P7Artifact.model_validate(payload)
