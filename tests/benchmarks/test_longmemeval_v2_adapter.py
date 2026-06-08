from __future__ import annotations

import json

from benchmarks.adapters.longmemeval_v2_adapter import LongMemEvalV2Adapter
from benchmarks.fyralis_eval.metrics import longmemeval_v2_score
from benchmarks.runners.core import BenchmarkRunConfig, run_benchmark


def test_longmemeval_v2_adapter_maps_dataset_root(tmp_path):
    data_root = _write_lme_v2_fixture(tmp_path)
    adapter = LongMemEvalV2Adapter(data_root, max_cases=2, haystack_tier="small")

    observations = list(adapter.iter_observations())
    queries = list(adapter.iter_queries())

    assert len(queries) == 2
    assert len(observations) == 4
    assert {query.tenant_id for query in queries} == {queries[0].tenant_id}
    assert queries[0].query_id == "q_incident_filters"
    assert queries[0].query_type == "dynamic-environment"
    assert queries[0].gold_answer == "Incident Mobile, Incident Portal"
    assert queries[0].metadata["eval_function"].startswith("norm_phrase_set_match")
    assert queries[0].metadata["haystack_trajectory_count"] == 2

    relevant = [
        observation
        for observation in observations
        if observation.metadata["trajectory_id"] == "traj_filters"
        and observation.metadata["state_index"] == 1
    ][0]
    assert "Goal: Inspect incident filters" in relevant.content
    assert "Action: Open Filters dropdown" in relevant.content
    assert "Incident Mobile" in relevant.content
    assert relevant.source == "benchmark_longmemeval_v2"
    transition = [
        observation
        for observation in observations
        if observation.metadata.get("observation_kind") == "state_transition"
    ][0]
    assert "Operational memory record: web_agent_state_transition" in transition.content
    assert "Transition: state 0 -> state 1" in transition.content
    assert "Action taken: Open Filters dropdown" in transition.content
    assert "Newly visible after action" in transition.content
    assert "Incident Mobile" in transition.content
    assert transition.source == "benchmark_longmemeval_v2_transition"

    gold = adapter.gold("q_incident_filters")
    assert gold.answer == "Incident Mobile, Incident Portal"
    assert gold.evidence_ids == []
    assert gold.metadata["haystack_tier"] == "small"


def test_longmemeval_v2_only_loads_selected_question_haystack(tmp_path):
    data_root = _write_lme_v2_fixture(tmp_path)
    adapter = LongMemEvalV2Adapter(data_root, max_cases=1, haystack_tier="small")

    observations = list(adapter.iter_observations())

    assert {item.metadata["trajectory_id"] for item in observations} == {
        "traj_filters",
        "traj_noise",
    }
    assert "traj_unused" not in {item.metadata["trajectory_id"] for item in observations}


def test_longmemeval_v2_bm25_retrieval_smoke(tmp_path):
    data_root = _write_lme_v2_fixture(tmp_path)
    adapter = LongMemEvalV2Adapter(data_root, max_cases=1, haystack_tier="small")

    result = run_benchmark(
        adapter,
        config=BenchmarkRunConfig(
            benchmark=adapter.benchmark_name,
            system_name="bm25_session",
            top_k=1,
            score_answers=False,
        ),
    )

    assert result.metrics_summary["queries"] == 1
    assert result.metrics_summary["evidence_recall_at_10"] is None
    assert result.metrics_summary["longmemeval_v2_packet_answer_support"] == 1.0
    assert result.results[0].debug["retrieved_evidence_ids"] == [
        result.retrieval_traces[0]["retrieved_evidence"][0]["observation_id"]
    ]
    assert (
        "traj_filters:state:1" in result.results[0].debug["retrieved_evidence_ids"][0]
        or "traj_filters:transition:0->1"
        in result.results[0].debug["retrieved_evidence_ids"][0]
    )


def test_longmemeval_v2_deterministic_eval_specs():
    assert longmemeval_v2_score(
        "\\boxed{Incident Portal and Incident Mobile}",
        "Incident Mobile, Incident Portal",
        "norm_phrase_set_match|lower=true|normalize_hyphen=true|strip_punct=true|separators=,;|require_non_empty=true",
    ) == 1.0
    assert longmemeval_v2_score(
        "\\boxed{Problems; Reports}",
        "Reports;Problems",
        "norm_phrase_set_match_ordered|lower=true|separators=;|require_non_empty=true",
    ) == 0.0
    assert longmemeval_v2_score(
        "\\boxed{option b}",
        "B",
        "mc_choice_match|strip_chars=.",
    ) == 1.0
    assert longmemeval_v2_score(
        "\\boxed{A and C}",
        "CA",
        "mc_choice_set_match",
    ) == 1.0
    assert longmemeval_v2_score(
        "\\boxed{UNKNOWN}",
        "The premise is wrong.",
        "llm_abstention_checker",
    ) is None


def _write_lme_v2_fixture(tmp_path):
    root = tmp_path / "longmemeval-v2"
    (root / "haystacks").mkdir(parents=True)
    questions = [
        {
            "id": "q_incident_filters",
            "domain": "enterprise",
            "environment": "workarena",
            "question_type": "dynamic-environment",
            "question": "Which filter option labels contain Incident?",
            "image": None,
            "answer": "Incident Mobile, Incident Portal",
            "eval_function": "norm_phrase_set_match|lower=true|separators=,;",
        },
        {
            "id": "q_module_order",
            "domain": "enterprise",
            "environment": "workarena",
            "question_type": "procedure",
            "question": "Which modules are used in order for reassignment?",
            "image": None,
            "answer": "Reports;Problems",
            "eval_function": "norm_phrase_set_match_ordered|lower=true|separators=;",
        },
    ]
    _write_jsonl(root / "questions.jsonl", questions)
    (root / "haystacks" / "lme_v2_small.json").write_text(
        json.dumps({
            "q_incident_filters": ["traj_filters", "traj_noise"],
            "q_module_order": ["traj_filters", "traj_noise"],
        }),
        encoding="utf-8",
    )
    (root / "haystacks" / "lme_v2_medium.json").write_text(
        json.dumps({
            "q_incident_filters": ["traj_filters", "traj_noise", "traj_unused"],
            "q_module_order": ["traj_filters", "traj_noise", "traj_unused"],
        }),
        encoding="utf-8",
    )
    trajectories = [
        {
            "id": "traj_filters",
            "domain": "enterprise",
            "environment": "workarena",
            "goal": "Inspect incident filters",
            "outcome": "success",
            "start_url": "https://example.invalid/incidents",
            "states": [
                {
                    "state_index": 0,
                    "step": 0,
                    "url": "https://example.invalid/incidents",
                    "action": None,
                    "thought": "Open the incidents list.",
                    "accessibility_tree": "Incidents list page",
                    "screenshot": "screenshots/traj_filters/0.png",
                },
                {
                    "state_index": 1,
                    "step": 1,
                    "url": "https://example.invalid/incidents",
                    "action": "Open Filters dropdown",
                    "thought": "Read the available Incident filter labels.",
                    "accessibility_tree": (
                        "Filters menu with Incident Mobile, Incident Portal, "
                        "My Open Incidents, and Edit personal filters"
                    ),
                    "screenshot": "screenshots/traj_filters/1.png",
                },
            ],
        },
        {
            "id": "traj_noise",
            "domain": "enterprise",
            "environment": "workarena",
            "goal": "Check unrelated catalog item",
            "outcome": "failure",
            "start_url": "https://example.invalid/catalog",
            "states": [
                {
                    "state_index": 0,
                    "step": 0,
                    "url": "https://example.invalid/catalog",
                    "action": None,
                    "thought": "Look for laptop options.",
                    "accessibility_tree": "Catalog item Dell laptop with SSD upgrade",
                    "screenshot": "screenshots/traj_noise/0.png",
                }
            ],
        },
        {
            "id": "traj_unused",
            "domain": "enterprise",
            "environment": "workarena",
            "goal": "Unused medium-only trajectory",
            "outcome": "success",
            "start_url": "https://example.invalid/unused",
            "states": [
                {
                    "state_index": 0,
                    "step": 0,
                    "url": "https://example.invalid/unused",
                    "action": None,
                    "thought": "Unused.",
                    "accessibility_tree": "Unused state",
                    "screenshot": "screenshots/traj_unused/0.png",
                }
            ],
        },
    ]
    _write_jsonl(root / "trajectories.jsonl", trajectories)
    return root


def _write_jsonl(path, records):
    path.write_text(
        "\n".join(json.dumps(record) for record in records) + "\n",
        encoding="utf-8",
    )
