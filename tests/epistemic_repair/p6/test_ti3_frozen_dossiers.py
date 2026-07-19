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
        "7703e20677499e057ca83134f9f89eb850b6e0a63e1f3102f3cd15780815b0f9",
        "1db969580b4c9ec49dc10c96ed84c296323b100635da9da9c5f4a0a3afde7143",
        "551225920d3d194fdb00e43b5a9ab1ef9f4e346dcd62ee769b96d0be9bcef0c8",
    ]
    assert [case.case_digest for case in cases] == [
        "00ad39a6bf4f962699b91d6b538fbf82972b09c033dc8236d8a546b6602a4d0c",
        "d8ef469d13fcc66482abdfc41866dcfc435e8a4fc2a12255986d4a6ea10e3baa",
        "2642647b51973625cea1d5bb38e00d3c8b0db953018fd829f39f974c19a493f9",
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


def test_provider_visible_identifiers_are_opaque_to_case_and_gold_labels() -> None:
    forbidden_identifier_values = {
        "atlas_positive",
        "cobalt_positive",
        "null_adversarial",
        "null_v1",
    }
    dossier_ids = []
    for case in build_frozen_dossier_cases():
        dossier_id = str(case.provider_payload["dossier_id"])
        dossier_ids.append(dossier_id)
        normalized = dossier_id.casefold()
        assert normalized.startswith("dos_")
        assert all(value not in normalized for value in forbidden_identifier_values)
        assert case.case_id.casefold() not in normalized
        company_label = str(case.provider_payload["scope"]["display_label"]).split()[0]
        assert company_label.casefold() not in normalized
        assert case.case_id not in json.dumps(case.provider_payload, sort_keys=True)
    assert len(dossier_ids) == len(set(dossier_ids)) == 3


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
        "94d5a81b981b12adb18e565813ed2172de3139fee8a6ee785eae5a6a34230bac"
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
