"""Five sealed, evaluator-owned P8 characterization populations."""

from __future__ import annotations

from dataclasses import asdict, dataclass

from lib.contracts.kernel import canonical_sha256


@dataclass(frozen=True, slots=True)
class CharacterizationCase:
    case_id: str
    runtime_text: str
    source_kind: str
    maturity: str | None
    evaluator_labels: tuple[str, ...]
    runtime_candidate_refs: tuple[str, ...] = ()
    runtime_source_metadata: tuple[tuple[str, str], ...] = ()

    def runtime_payload(self) -> dict[str, object]:
        # This is the only payload production adapters receive.
        return {"case_id": self.case_id, "text": self.runtime_text,
                "source_kind": self.source_kind, "maturity": self.maturity,
                "candidate_refs": self.runtime_candidate_refs,
                "source_metadata": self.runtime_source_metadata}


@dataclass(frozen=True, slots=True)
class SealedPopulation:
    name: str
    version: str
    unit: str
    cases: tuple[CharacterizationCase, ...]
    runtime_digest: str
    gold_digest: str


def _seal(
    name: str, unit: str, cases: list[CharacterizationCase], *, version: str = "1",
) -> SealedPopulation:
    runtime = [case.runtime_payload() for case in cases]
    gold = [{"case_id": case.case_id, "labels": case.evaluator_labels} for case in cases]
    return SealedPopulation(name, version, unit, tuple(cases), canonical_sha256(runtime), canonical_sha256(gold))


def build_boundary_population() -> SealedPopulation:
    cases = []
    challenge_minima = {
        "reply_thread_edit": 200, "discourse_reference": 120, "topic_drift": 100,
        "split_merge": 80, "temporal_distractor": 80, "quote_link": 60,
        "incomplete_topology": 60, "cross_source_object_link": 100,
    }
    for episode in range(240):
        source = "structured" if episode < 60 else "conversational" if episode < 180 else "cross_source"
        for position in range(5):
            index = episode * 5 + position
            labels = [source]
            for label, minimum in challenge_minima.items():
                if (index * 37) % 1200 < minimum:
                    labels.append(label)
            gold_episode = (episode + 1) % 240 if "topic_drift" in labels else episode
            labels.append(f"episode:{gold_episode:03d}")
            metadata = (
                (("object_id", f"structured:{episode:03d}"),)
                if source == "structured"
                else (
                    ("channel", "C-p8-boundary"),
                    ("ts", f"{episode:03d}.{position:03d}"),
                    *(((("thread_ts", f"{episode:03d}.000"),) if position else ())),
                )
                if source == "conversational"
                else (("linked_object_id", f"cross:{episode:03d}"),)
            )
            # Topic-drift gold is observable in the runtime signal: the
            # message explicitly references the new business episode while
            # retaining its original source container/thread metadata.
            cases.append(CharacterizationCase(
                f"boundary-{index:04d}",
                f"Harbor episode {gold_episode} update {position} references the prior status.",
                source, None, tuple(labels), (), metadata,
            ))
    return _seal("boundary_discovery", "normalized_observation", cases, version="2")


def build_context_population() -> SealedPopulation:
    composition = (
        ("topology_sufficient", 200), ("temporal_combined_expansion", 150),
        ("semantically_unstable_multi_context", 100), ("needs_expansion", 75),
        ("needs_clarification", 50), ("budget_exhausted", 25),
    )
    cases, index = [], 0
    for label, count in composition:
        for _ in range(count):
            cases.append(CharacterizationCase(
                f"context-{index:04d}", f"This update requires {label.replace('_', ' ')}.",
                "slack" if index % 2 else "document", None, (label,),
            ))
            index += 1
    return _seal("context_selection", "frozen_decision", cases)


def build_entity_population() -> SealedPopulation:
    roles = (("explicit", 1200), ("discourse_deictic", 600),
             ("open_world_none_known", 300), ("negative", 300))
    challenges = {"ambiguous_alias": 300, "near_name_collision": 200,
                  "cross_customer_trap": 120, "novel_referent": 100,
                  "merge_split_correction": 80}
    cases, index = [], 0
    for role, count in roles:
        for _ in range(count):
            phrase = f"Entity-{index:04d}"
            text = f"{phrase} owns the Harbor follow-up." if role != "negative" else "No named owner appears in this update."
            labels = [role]
            for label, minimum in challenges.items():
                if index < minimum:
                    labels.append(label)
            runtime_refs: tuple[str, ...] = ()
            if role not in {"negative", "open_world_none_known"}:
                runtime_refs = (f"project:{index:04d}",)
                if any(label in labels for label in ("ambiguous_alias", "near_name_collision", "cross_customer_trap", "merge_split_correction")):
                    runtime_refs += (f"customer:{index:04d}",)
            cases.append(CharacterizationCase(
                f"entity-{index:04d}", text, "mixed", None, tuple(labels), runtime_refs,
            ))
            index += 1
    return _seal("entity_grounding", "mention_opportunity", cases)


def build_retrieval_population() -> SealedPopulation:
    kinds = (("supporting_equivalent", 150), ("contradiction_lifecycle", 150),
             ("multi_hop_relation", 120), ("sparse_no_match_raw_reopen", 90),
             ("noise_noop", 90))
    cases, index = [], 0
    expanded = [label for label, count in kinds for _ in range(count)]
    for maturity in ("cold", "intermediate", "mature"):
        for offset in range(200):
            label = expanded[index]
            text = {
                "supporting_equivalent": "What accepted Harbor status supports this update?",
                "contradiction_lifecycle": "What accepted Harbor status contradicts this update?",
                "multi_hop_relation": "How does Harbor depend on the certificate work?",
                "sparse_no_match_raw_reopen": "No known model matches; reopen source evidence.",
                "noise_noop": "Unrelated social chatter needs no company-memory retrieval.",
            }[label]
            cases.append(CharacterizationCase(
                f"retrieval-{index:04d}", text,
                "mixed", maturity, (label, maturity),
            ))
            index += 1
    return _seal("retrieval", "claim_local_decision", cases)


def build_feedback_population() -> SealedPopulation:
    outcomes = (("later_confirmed", 120), ("revised", 80), ("falsified", 60),
                ("justified_noop", 40), ("entity_human_correction", 30),
                ("no_observable_outcome_control", 30))
    cases, index = [], 0
    for outcome, count in outcomes:
        for _ in range(count):
            cases.append(CharacterizationCase(
                f"feedback-{index:04d}", f"Harbor feedback outcome {index}.",
                "mixed", None, (outcome, "paired_route_policies"),
            ))
            index += 1
    return _seal("feedback", "base_decision_two_policies", cases)


def build_all_characterization_populations() -> tuple[SealedPopulation, ...]:
    return (build_boundary_population(), build_context_population(), build_entity_population(),
            build_retrieval_population(), build_feedback_population())


def population_manifest(population: SealedPopulation) -> dict[str, object]:
    counts: dict[str, int] = {}
    for case in population.cases:
        for label in case.evaluator_labels:
            counts[label] = counts.get(label, 0) + 1
    return {"name": population.name, "version": population.version,
            "unit": population.unit, "size": len(population.cases),
            "label_counts": counts, "runtime_digest": population.runtime_digest,
            "gold_digest": population.gold_digest,
            "source_digest": canonical_sha256(asdict(population))}
