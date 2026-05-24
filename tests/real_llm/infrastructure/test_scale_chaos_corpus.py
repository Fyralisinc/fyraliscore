"""Structural checks for the scale-chaos real-LLM scenario corpus.

These tests intentionally avoid Postgres, Ollama, and any real LLM call. Their
job is to make the large corpus behave like a maintained test asset: loadable,
referentially coherent, diverse enough to stress retrieval, and rich enough to
exercise the production-shaped scenario loader.
"""
from __future__ import annotations

from collections import Counter
from typing import Any

from tests.real_llm.infrastructure.scenario_loader import load_scenario


def test_scale_chaos_corpus_loads_with_production_shaped_volume() -> None:
    scenario = load_scenario("scale_chaos_b2b")

    foundation = scenario.foundation
    assert scenario.scenario_id == "scale_chaos_b2b"
    assert len(foundation.get("actors") or []) >= 20
    assert len(foundation.get("customers") or []) >= 10
    assert len(foundation.get("goals") or []) >= 8
    assert len(foundation.get("commitments") or []) >= 20
    assert len(foundation.get("decisions") or []) >= 8
    assert len(foundation.get("customer_commitments") or []) >= 10
    assert len(scenario.signal_sequences) >= 10
    assert sum(len(seq) for seq in scenario.signal_sequences.values()) >= 100


def test_scale_chaos_corpus_references_known_foundation_entities() -> None:
    scenario = load_scenario("scale_chaos_b2b")
    foundation = scenario.foundation

    allowed_criticalities = {"must_have", "high", "medium", "low"}
    actor_names = {a["name"] for a in foundation.get("actors") or []}
    customer_names = {c["name"] for c in foundation.get("customers") or []}
    goal_titles = {g["title"] for g in foundation.get("goals") or []}
    commitment_titles = {
        c["title"] for c in foundation.get("commitments") or []
    }

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
            assert signal.get("content"), (sequence_name, index)
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


def test_scale_chaos_corpus_has_retrieval_stressors() -> None:
    scenario = load_scenario("scale_chaos_b2b")
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

    for family in (
        "slack",
        "email",
        "github",
        "linear",
        "support",
        "salesforce",
        "datadog",
        "pagerduty",
        "calendar",
    ):
        assert channel_families[family] >= 1

    assert trust_tiers["authoritative"] >= 15
    assert trust_tiers["authoritative_external"] >= 8
    assert any("thread_of" in signal for signal in signals)
    assert any("content_dict" in signal for signal in signals)
    assert any("entities_hint" in signal for signal in signals)

    for phrase in (
        "Nimbus Bank",
        "NBI",
        "Axion Robotics Inc",
        "AXN Robotics",
        "Northstar Insurance",
        "North Star Insurance",
        "Meridian Energy Corp",
        "ParcelPilot",
        "Kestrel Labs",
        "Clearpath Logistics LLC",
    ):
        assert phrase in text

    for concept in (
        "revenue-at-risk",
        "audit export",
        "SAML",
        "duplicate CRM",
        "billing",
        "data residency",
        "falsifier",
        "priority lane",
        "stale replays",
        "alias",
    ):
        assert concept.lower() in text.lower()


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
