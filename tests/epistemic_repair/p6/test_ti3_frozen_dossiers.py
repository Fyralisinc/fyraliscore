from __future__ import annotations

import json

from lib.contracts.kernel import canonical_sha256
from lib.evaluation.epistemic_repair.ti3_frozen_dossiers import (
    build_fixture_manifest,
    build_frozen_dossier_cases,
    write_frozen_dossier_artifacts,
)


def test_three_cases_have_stable_independent_digests() -> None:
    cases = build_frozen_dossier_cases()
    assert [case.case_id for case in cases] == [
        "atlas_positive_v1",
        "cobalt_positive_v1",
        "null_adversarial_v1",
    ]
    assert [case.dossier_digest for case in cases] == [
        "0feda62cb418388d6baab2d8bd95ab6063c5895ee240ebd6a598e70c23b30222",
        "283a521fb191c725f6a22ee03fd8e0c976120058da2dd1ffed3c3ede5d080bcd",
        "a439451996c955046c1d5b7cd78c17b112cb4ef4a69aec9da30f40262f2e88ee",
    ]
    assert [case.case_digest for case in cases] == [
        "47e07df8b5588236dc77dea0db5e88980cdb2eb510e1e039929171dcec831439",
        "a123c2b2b33b36e3aaf431896d47328795b091d688e0cccd16c467b79148058a",
        "ee5e48285086db106f1ae6ba478830c1ef96bd5e1eb02c3d26a5736f73317323",
    ]
    assert len({case.dossier_digest for case in cases}) == 3
    assert len({case.gold_digest for case in cases}) == 3
    assert len({case.case_digest for case in cases}) == 3


def test_provider_payloads_contain_no_gold_or_canonical_identity() -> None:
    forbidden_keys = {
        "gold",
        "expected_decision",
        "required_mechanism_facets",
        "required_direction",
        "acceptable_abstention_reasons",
        "forbidden_claims",
        "storyline_id",
        "canonical_ref",
        "model_id",
        "truth_version_id",
        "observation_id",
        "threshold",
        "score",
    }
    for case in build_frozen_dossier_cases():
        payload_text = json.dumps(case.provider_payload, sort_keys=True)
        payload_keys = _all_keys(case.provider_payload)
        assert not forbidden_keys & payload_keys
        assert case.gold.expected_decision not in case.provider_payload
        assert "expected_" not in payload_text
        assert "required_" not in payload_text


def test_positive_cases_are_structurally_distinct_and_null_is_insufficient() -> None:
    atlas, cobalt, null = build_frozen_dossier_cases()

    assert atlas.gold.expected_decision == cobalt.gold.expected_decision == "synthesis"
    assert atlas.provider_payload["candidate_mechanism_slots"] == {
        "causes": ["M1", "M2"],
        "conditions": ["O1"],
        "outcomes": ["O3"],
    }
    assert cobalt.provider_payload["candidate_mechanism_slots"] == {
        "causes": ["O1"],
        "conditions": ["M1"],
        "outcomes": ["O3"],
    }
    assert atlas.gold.required_direction != cobalt.gold.required_direction
    assert null.gold.expected_decision == "abstain"
    assert null.provider_payload["candidate_mechanism_slots"]["causes"] == []
    assert null.provider_payload["open_uncertainty"] == ["U1"]
    assert null.gold.required_counterevidence_handles == ("O3",)


def test_every_provider_handle_and_reference_is_closed() -> None:
    for case in build_frozen_dossier_cases():
        payload = case.provider_payload
        handles = {item["handle"] for item in payload["handles"]}
        assert len(handles) == len(payload["handles"])
        references = set(payload["event_order"])
        references.update(payload["accepted_model_heads"])
        references.update(payload["direct_observations"])
        references.update(payload["open_uncertainty"])
        references.update(payload["discriminating_missing_evidence"])
        for group in (
            "supporting_evidence",
            "contradictory_evidence",
            "auxiliary_evidence",
        ):
            references.update(item["object_handle"] for item in payload[group])
        for values in payload["candidate_mechanism_slots"].values():
            references.update(values)
        assert references <= handles


def test_artifact_writer_keeps_provider_and_gold_files_separate(tmp_path) -> None:
    manifest = write_frozen_dossier_artifacts(tmp_path)
    reopened_manifest = json.loads((tmp_path / "manifest.json").read_text())
    assert reopened_manifest == manifest
    assert manifest["case_count"] == 3
    body = dict(manifest)
    digest = body.pop("manifest_digest")
    assert digest == canonical_sha256(body)

    for entry in manifest["cases"]:
        case_id = entry["case_id"]
        dossier = json.loads((tmp_path / f"{case_id}.dossier.json").read_text())
        gold = json.loads((tmp_path / f"{case_id}.gold.json").read_text())
        assert canonical_sha256(dossier) == entry["dossier_digest"]
        assert canonical_sha256(gold) == entry["gold_digest"]
        assert "expected_decision" not in dossier
        assert "expected_decision" in gold


def test_manifest_is_reproducible_and_binds_all_case_parts() -> None:
    first = build_fixture_manifest()
    second = build_fixture_manifest()
    assert first == second
    assert first["manifest_digest"] == (
        "92de6fbdb9676f73376a268a09376bbeb48c3e9ce51ad6d146aba0b82a7d266f"
    )
    assert all(
        set(entry) == {"case_id", "dossier_digest", "gold_digest", "case_digest"}
        for entry in first["cases"]
    )


def _all_keys(value: object) -> set[str]:
    if isinstance(value, dict):
        return set(value) | {
            key for nested in value.values() for key in _all_keys(nested)
        }
    if isinstance(value, list):
        return {key for nested in value for key in _all_keys(nested)}
    return set()
