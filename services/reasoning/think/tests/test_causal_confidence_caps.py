from services.reasoning.think.validator import (
    _confidence_cap_for_causal_evidence,
)


def _entry(
    role: str,
    evidence_ids: list[str] | None = None,
    source_channels: list[str] | None = None,
) -> dict:
    proposition = {
        "kind": "belief",
        "claim_role": role,
    }
    if evidence_ids is not None:
        proposition["evidence_event_ids"] = evidence_ids
    if source_channels is not None:
        proposition["contextual_frame"] = {"source_channels": source_channels}
    return {"proposition": proposition}


def test_batched_causal_claims_earn_confidence_only_from_cited_evidence() -> None:
    entries = [
        _entry("hypothesis"),
        _entry("situation", []),
        _entry("situation", ["obs-1", "obs-2"]),
        _entry(
            "situation",
            ["obs-1", "obs-2", "obs-3"],
            ["slack:message", "jira:issue"],
        ),
    ]

    assert [
        _confidence_cap_for_causal_evidence(entry)
        for entry in entries
    ] == [0.60, 0.68, 0.74, None]


def test_duplicate_evidence_does_not_inflate_causal_confidence() -> None:
    entry = _entry("situation", ["obs-1", "obs-1", "obs-1"])

    assert _confidence_cap_for_causal_evidence(entry) == 0.68


def test_same_source_repetition_does_not_remove_causal_confidence_cap() -> None:
    entry = _entry(
        "situation",
        ["obs-1", "obs-2", "obs-3"],
        ["slack:message"],
    )

    assert _confidence_cap_for_causal_evidence(entry) == 0.74


def test_entry_level_supporting_events_count_toward_causal_evidence() -> None:
    entry = _entry("situation", source_channels=["slack:message", "jira:issue"])
    entry["supporting_event_ids"] = ["obs-1", "obs-2", "obs-3"]

    assert _confidence_cap_for_causal_evidence(entry) is None


def test_noncausal_claims_keep_existing_calibration_behavior() -> None:
    entries = [
        _entry("fact"),
        _entry("concern"),
        _entry("prediction"),
        {"proposition": {"kind": "observation", "claim_role": "fact"}},
    ]

    assert all(
        _confidence_cap_for_causal_evidence(entry) is None
        for entry in entries
    )
