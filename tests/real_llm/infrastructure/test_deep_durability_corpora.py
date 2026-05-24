"""Structural checks for the deep durability real-LLM corpora.

These tests keep the large synthetic-company corpora useful over time without
touching Postgres, Ollama, or a paid LLM. They assert that each corpus is large,
referentially coherent, channel-diverse, and intentionally hostile to naive
retrieval.
"""
from __future__ import annotations

from collections import Counter
from typing import Any

import pytest

from tests.real_llm.infrastructure.scenario_loader import load_scenario


DEEP_CORPORA = {
    "industrial_ops": {
        "min_actors": 12,
        "min_customers": 6,
        "min_goals": 5,
        "min_commitments": 12,
        "min_decisions": 5,
        "min_customer_commitments": 8,
        "min_sequences": 6,
        "min_signals": 40,
        "alias_terms": ("TFI", "MRA", "Harbor"),
        "business_terms": (
            "furnace telemetry",
            "penalty",
            "safety",
            "supplier",
            "forecast",
            "regulator",
            "revenue",
        ),
    },
    "fintech_risk": {
        "min_actors": 12,
        "min_customers": 6,
        "min_goals": 5,
        "min_commitments": 12,
        "min_decisions": 5,
        "min_customer_commitments": 8,
        "min_sequences": 6,
        "min_signals": 40,
        "alias_terms": ("ACS", "BRCU"),
        "business_terms": (
            "ledger",
            "KYC",
            "fraud",
            "reconciliation",
            "network",
            "regulatory",
            "replay",
        ),
    },
}


@pytest.mark.parametrize("scenario_id, requirements", DEEP_CORPORA.items())
def test_deep_corpus_loads_with_production_shaped_volume(
    scenario_id: str,
    requirements: dict[str, Any],
) -> None:
    scenario = load_scenario(scenario_id)
    foundation = scenario.foundation

    assert scenario.scenario_id == scenario_id
    assert len(foundation.get("actors") or []) >= requirements["min_actors"]
    assert len(foundation.get("customers") or []) >= requirements["min_customers"]
    assert len(foundation.get("goals") or []) >= requirements["min_goals"]
    assert len(foundation.get("commitments") or []) >= requirements["min_commitments"]
    assert len(foundation.get("decisions") or []) >= requirements["min_decisions"]
    assert (
        len(foundation.get("customer_commitments") or [])
        >= requirements["min_customer_commitments"]
    )
    assert len(scenario.signal_sequences) >= requirements["min_sequences"]
    assert _signal_count(scenario.signal_sequences) >= requirements["min_signals"]
    assert all(len(signals) >= 5 for signals in scenario.signal_sequences.values())


@pytest.mark.parametrize("scenario_id", DEEP_CORPORA)
def test_deep_corpus_references_known_foundation_entities(scenario_id: str) -> None:
    scenario = load_scenario(scenario_id)
    foundation = scenario.foundation

    allowed_criticalities = {"must_have", "high", "medium", "low"}
    actor_names = {a["name"] for a in foundation.get("actors") or []}
    customer_names = {c["name"] for c in foundation.get("customers") or []}
    goal_titles = {g["title"] for g in foundation.get("goals") or []}
    commitment_titles = {c["title"] for c in foundation.get("commitments") or []}

    for goal in foundation.get("goals") or []:
        success_criteria = goal.get("success_criteria")
        if success_criteria is not None:
            assert isinstance(success_criteria, dict), goal["title"]

    for commitment in foundation.get("commitments") or []:
        owner = commitment.get("owner")
        assert owner in actor_names, commitment["title"]
        for goal_title in _as_list(commitment.get("contributes_to_goal")):
            assert goal_title in goal_titles, commitment["title"]

    for decision in foundation.get("decisions") or []:
        scope = decision.get("scope")
        revisit_triggers = decision.get("revisit_triggers")
        if scope is not None:
            assert isinstance(scope, dict), decision["title"]
        if revisit_triggers is not None:
            assert isinstance(revisit_triggers, dict), decision["title"]

    for link in foundation.get("customer_commitments") or []:
        assert link["customer"] in customer_names, link
        assert link["commitment"] in commitment_titles, link
        assert link.get("criticality", "medium") in allowed_criticalities, link

    for sequence_name, signals in scenario.signal_sequences.items():
        last_delay = -1.0
        for index, signal in enumerate(signals):
            assert signal.get("channel"), (sequence_name, index)
            assert signal.get("content") or signal.get("content_dict"), (
                sequence_name,
                index,
            )
            assert signal["channel"] not in {
                "slack:message",
                "email:inbound",
                "calendar:sync",
                "github:webhook",
                "linear:webhook",
            }, (sequence_name, index, signal["channel"])

            delay = float(signal.get("delay_minutes", 0))
            assert delay >= last_delay, (sequence_name, index)
            last_delay = delay

            actor = signal.get("actor")
            if actor and ":" not in actor:
                assert actor in actor_names, (sequence_name, index, actor)

            if "thread_of" in signal:
                assert int(signal["thread_of"]) < index, (sequence_name, index)


@pytest.mark.parametrize("scenario_id, requirements", DEEP_CORPORA.items())
def test_deep_corpus_has_retrieval_and_alias_stressors(
    scenario_id: str,
    requirements: dict[str, Any],
) -> None:
    scenario = load_scenario(scenario_id)
    signals = [
        signal
        for sequence in scenario.signal_sequences.values()
        for signal in sequence
    ]
    text = "\n".join(str(signal.get("content") or "") for signal in signals)
    channel_families = Counter(
        str(signal["channel"]).split(":", 1)[0] for signal in signals
    )
    trust_tiers = Counter(
        signal.get("trust_tier", "inferential") for signal in signals
    )

    assert len(channel_families) >= 7
    assert channel_families["slack"] >= 8
    assert channel_families["email"] >= 2
    assert channel_families["github"] >= 2
    assert channel_families["linear"] >= 3
    assert trust_tiers["authoritative"] >= 5
    assert trust_tiers["authoritative_external"] >= 4

    for phrase in requirements["alias_terms"]:
        assert phrase in text

    for concept in requirements["business_terms"]:
        assert concept.lower() in text.lower()

    assert any(
        noise_word in text.lower()
        for noise_word in ("ambiguous", "noise", "contradict", "not urgent")
    )
    assert any(
        edge_word in text.lower()
        for edge_word in ("connect", "hidden", "converge", "together")
    )


def _signal_count(sequences: dict[str, list[dict[str, Any]]]) -> int:
    return sum(len(sequence) for sequence in sequences.values())


def _as_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        result: list[str] = []
        for entry in value:
            if isinstance(entry, dict):
                result.append(str(entry["title"]))
            else:
                result.append(str(entry))
        return result
    if isinstance(value, dict):
        return [str(value["title"])]
    return [str(value)]
