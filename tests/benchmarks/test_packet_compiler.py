from __future__ import annotations

from benchmarks.adapters.base import BenchmarkQuery
from benchmarks.fyralis_eval.packet_compiler import ContextPacketCompiler
from benchmarks.fyralis_eval.reader import RetrievedEvidence


def test_packet_compiler_focuses_long_session_on_query_terms():
    query = BenchmarkQuery(
        query_id="q1",
        tenant_id="t1",
        query_text="How long is my daily commute to work?",
    )
    content = "\n".join([
        "assistant: " + ("general productivity advice " * 300),
        "user: I've been listening to audiobooks during my daily commute, which takes 45 minutes each way.",
        "assistant: " + ("more general audiobook advice " * 300),
    ])
    compiler = ContextPacketCompiler(max_chars_per_evidence=600)

    retrieval = compiler.compile(
        query,
        [
            RetrievedEvidence(
                observation_id="obs1",
                content=content,
                score=1.0,
                occurred_at="2026-01-01T00:00:00+00:00",
            )
        ],
        latency_ms=1,
        retrieval_calls=1,
    )

    packet_content = retrieval.context_packet["evidence"][0]["content"]
    assert "45 minutes each way" in packet_content
    assert len(packet_content) <= 600


def test_packet_compiler_keeps_dynamic_transition_lines():
    query = BenchmarkQuery(
        query_id="q_dynamic",
        tenant_id="t1",
        query_text=(
            "When I open the Filters dropdown, which filter option labels "
            "contain the substring Incident?"
        ),
        query_type="dynamic-environment",
    )
    content = "\n".join([
        "Before state key UI labels: " + ("Filters Incidents list " * 80),
        "After state key UI labels: " + ("Filters Incidents toolbar " * 80),
        (
            "Newly visible after action: Edit personal filters; "
            "Incident Mobile; Incident Portal; My Open Incidents"
        ),
        "Transition summary: the dropdown opened after the filter control action.",
    ])
    compiler = ContextPacketCompiler(max_chars_per_evidence=500)

    retrieval = compiler.compile(
        query,
        [
            RetrievedEvidence(
                observation_id="obs_dynamic",
                content=content,
                score=1.0,
                occurred_at="2026-01-01T00:00:00+00:00",
            )
        ],
        latency_ms=1,
        retrieval_calls=1,
    )

    packet_content = retrieval.context_packet["evidence"][0]["content"]
    assert "Incident Mobile" in packet_content
    assert "Incident Portal" in packet_content
    assert "My Open Incidents" in packet_content
    assert len(packet_content) <= 500


def test_packet_compiler_gates_structured_ui_facts_by_query_intent():
    evidence = RetrievedEvidence(
        observation_id="obs_popup",
        content="Operational memory record: web_agent_state\nState summary: user opened a lookup.",
        score=1.0,
        occurred_at="2026-01-01T00:00:00+00:00",
        metadata={
            "structured_ui_facts": [
                "autocomplete popup title: Recent selections; field=Caller; options: Abraham Lincoln",
                "table summary row Total: value=-",
            ]
        },
    )
    compiler = ContextPacketCompiler(max_chars_per_evidence=350)

    popup_retrieval = compiler.compile(
        BenchmarkQuery(
            query_id="q_popup",
            tenant_id="t1",
            query_text="What title appeared in the search box popup?",
        ),
        [evidence],
        latency_ms=1,
        retrieval_calls=1,
    )
    ordinary_retrieval = compiler.compile(
        BenchmarkQuery(
            query_id="q_ordinary",
            tenant_id="t1",
            query_text="What route did the user open?",
        ),
        [evidence],
        latency_ms=1,
        retrieval_calls=1,
    )

    popup_content = popup_retrieval.context_packet["evidence"][0]["content"]
    ordinary_content = ordinary_retrieval.context_packet["evidence"][0]["content"]
    assert "Recent selections" in popup_content
    assert "table summary row" not in popup_content
    assert "Recent selections" not in ordinary_content


def test_packet_compiler_appends_relevant_sort_field_metadata():
    compiler = ContextPacketCompiler(max_chars_per_evidence=260)
    retrieval = compiler.compile(
        BenchmarkQuery(
            query_id="q_sort",
            tenant_id="t1",
            query_text=(
                "Before selecting the target field, what default sort field "
                "is initially shown in the sort row?"
            ),
        ),
        [
            RetrievedEvidence(
                observation_id="obs_sort",
                content="Operational state. " + ("boilerplate " * 80),
                score=1.0,
                occurred_at="2026-01-01T00:00:00+00:00",
                metadata={"sort_fields": ["Acquisition method"]},
            )
        ],
        latency_ms=1,
        retrieval_calls=1,
    )

    packet_content = retrieval.context_packet["evidence"][0]["content"]
    assert "sort fields visible: Acquisition method" in packet_content
    assert len(packet_content) <= 260


def test_packet_compiler_appends_relevant_form_value_facts():
    compiler = ContextPacketCompiler(max_chars_per_evidence=420)
    retrieval = compiler.compile(
        BenchmarkQuery(
            query_id="q_priority",
            tenant_id="t1",
            query_text=(
                'If I change the "Impact" field to "Low" and the "Urgency" '
                'field to "Low", what value does the "Priority" field '
                "automatically change to?"
            ),
        ),
        [
            RetrievedEvidence(
                observation_id="obs_priority",
                content="After action form controls visible: option 3 - Low selected=true.",
                score=1.0,
                occurred_at="2026-01-01T00:00:00+00:00",
                metadata={
                    "structured_ui_facts": [
                        (
                            "editable form fields: Impact=3 - Low; "
                            "Urgency=3 - Low"
                        ),
                        "disabled/read-only form fields: Priority=5 - Planning",
                    ]
                },
            )
        ],
        latency_ms=1,
        retrieval_calls=1,
    )

    packet_content = retrieval.context_packet["evidence"][0]["content"]
    assert "Impact = 3 - Low" in packet_content
    assert "Urgency = 3 - Low" in packet_content
    assert "Priority = 5 - Planning" in packet_content


def test_packet_compiler_spells_pipeline_stage_count_for_answer_support():
    compiler = ContextPacketCompiler(max_chars_per_evidence=420)
    retrieval = compiler.compile(
        BenchmarkQuery(
            query_id="q_pipeline",
            tenant_id="t1",
            query_text=(
                "After you place an order for a MacBook, how many stages "
                "remain, excluding in-progress ones, before the order pipeline "
                "is fully complete?"
            ),
        ),
        [
            RetrievedEvidence(
                observation_id="obs_pipeline",
                content="Order status page.",
                score=1.0,
                occurred_at="2026-01-01T00:00:00+00:00",
                metadata={
                    "stage_chains": [
                        (
                            "Waiting for Approval (In progress); "
                            "Completed (Pending - has not started); "
                            "remaining_excluding_in_progress_count=7"
                        )
                    ]
                },
            )
        ],
        latency_ms=1,
        retrieval_calls=1,
    )

    packet_content = retrieval.context_packet["evidence"][0]["content"]
    assert "remaining_excluding_in_progress_count=7 (seven)" in packet_content


def test_packet_compiler_derives_checkbox_comparison_choice():
    compiler = ContextPacketCompiler(max_chars_per_evidence=600)
    query = BenchmarkQuery(
        query_id="q_laptops",
        tenant_id="t1",
        query_text=(
            "Which page has the largest number of optional software checkbox choices?\n\n"
            "A. Standard Laptop\n"
            "B. Developer Laptop (Mac)\n"
            "C. Sales Laptop\n"
            "D. Standard Laptop and Developer Laptop (Mac) are tied"
        ),
    )

    retrieval = compiler.compile(
        query,
        [
            _checkbox_page_evidence("standard", "Standard Laptop", 2),
            _checkbox_page_evidence("developer", "Developer Laptop (Mac)", 3),
            _checkbox_page_evidence("sales", "Sales Laptop", 4),
        ],
        latency_ms=1,
        retrieval_calls=1,
    )

    derived = retrieval.context_packet["evidence"][0]
    assert derived["metadata"]["derived_kind"] == "checkbox_comparison"
    assert "Sales Laptop: 4 checkbox choices" in derived["content"]
    assert "Final derived answer: \\boxed{C}" in derived["content"]


def test_packet_compiler_warns_when_required_tool_surface_is_missing():
    query = BenchmarkQuery(
        query_id="q_repo",
        tenant_id="t1",
        query_text="Clone the repository and inspect the exact file.",
        metadata={
            "requires_external_tool_surface": True,
            "required_tool_surfaces": ["repository", "filesystem"],
        },
    )
    compiler = ContextPacketCompiler()

    retrieval = compiler.compile(
        query,
        [
            RetrievedEvidence(
                observation_id="obs_breadcrumb",
                content="The team said someone needs to inspect the repository.",
                score=1.0,
                occurred_at="2026-01-01T00:00:00+00:00",
                metadata={"observation_kind": "timeline_event"},
            )
        ],
        latency_ms=1,
        retrieval_calls=1,
    )

    assert retrieval.omission_ledger == [
        {
            "reason": "external_tool_surface_not_materialized",
            "severity": "warning",
            "required_tool_surfaces": ["repository", "filesystem"],
        }
    ]
    requirement_kinds = {
        item["kind"] for item in retrieval.context_packet["answer_requirements"]
    }
    assert {"specificity", "external_tool_surface"} <= requirement_kinds
    assert retrieval.context_packet["sufficiency"]["has_external_tool_result"] is False


def test_packet_compiler_accepts_materialized_tool_surface():
    query = BenchmarkQuery(
        query_id="q_repo",
        tenant_id="t1",
        query_text="Clone the repository and inspect the exact file.",
        metadata={
            "requires_external_tool_surface": True,
            "required_tool_surfaces": ["repository"],
        },
    )
    compiler = ContextPacketCompiler()

    retrieval = compiler.compile(
        query,
        [
            RetrievedEvidence(
                observation_id="obs_repo",
                content="Repository snapshot: src/app.py line 42.",
                score=1.0,
                occurred_at="2026-01-01T00:00:00+00:00",
                metadata={"observation_kind": "repository_snapshot"},
            )
        ],
        latency_ms=1,
        retrieval_calls=1,
    )

    assert retrieval.omission_ledger == []
    assert retrieval.context_packet["sufficiency"]["has_external_tool_result"] is True


def test_packet_compiler_warns_when_finality_roles_are_missing():
    query = BenchmarkQuery(
        query_id="q_final",
        tenant_id="t1",
        query_text="After the rollback, what was the final solution?",
        query_type="timeline",
    )
    compiler = ContextPacketCompiler()

    retrieval = compiler.compile(
        query,
        [
            RetrievedEvidence(
                observation_id="obs_assessment",
                content="Impact assessment: the system was affected after rollback.",
                score=1.0,
                occurred_at="2026-01-01T00:00:00+00:00",
                metadata={
                    "observation_kind": "timeline_event",
                    "event_index": 4,
                    "hybrid_roles": ["temporal_anchor", "transition"],
                },
            )
        ],
        latency_ms=1,
        retrieval_calls=1,
    )

    reasons = {item["reason"] for item in retrieval.omission_ledger}
    assert "incomplete_composition_role_coverage" in reasons
    assert "missing_finality_evidence" in reasons
    assert retrieval.context_packet["sufficiency"]["missing_roles"] == [
        "decision",
        "final_outcome",
    ]


def _checkbox_page_evidence(
    observation_id: str,
    label: str,
    count: int,
) -> RetrievedEvidence:
    return RetrievedEvidence(
        observation_id=observation_id,
        content=(
            f"Newly visible after action: Create favorite for {label}; "
            f"{label} | ServiceNow. "
            f"Relevant structured UI facts: checkbox choice group visible: "
            f"count = {count}; choices = A; B; C"
        ),
        score=1.0,
        occurred_at="2026-01-01T00:00:00+00:00",
    )
