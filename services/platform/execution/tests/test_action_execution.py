from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from types import SimpleNamespace
from uuid import uuid4

import pytest

from services.platform.execution import action_execution, inquiry, retrieval_actions
from services.platform.execution.config import InquiryConfig
from services.platform.execution.types import InquiryQuestion, RetrievalAction
from services.reasoning.retrieval.pathways import ModelCandidateHit, PathwayResult
from services.reasoning.retrieval.primary import TriggerContext


def _question(question_id: str = "Q1") -> InquiryQuestion:
    return InquiryQuestion(
        question_id=question_id,
        question="Which dependency blocks launch?",
        primitive="DEPENDENCY",
        tests_hypotheses=("H1",),
        expected_value=0.8,
        expected_cost=0.2,
        retrieval_target="blocker",
        stop_condition="dependency found",
        score=0.7,
    )


def _trigger() -> TriggerContext:
    return TriggerContext(
        kind="T1",
        tenant_id=uuid4(),
        seed_entity_ids=[],
        scope_actors=[],
        seed_natural_text="Launch dependency status",
        seed_occurred_at=datetime(2026, 6, 17, 8, 0, tzinfo=timezone.utc),
    )


class _AcquireConn:
    def __init__(self, conn: object) -> None:
        self._conn = conn

    async def __aenter__(self) -> object:
        return self._conn

    async def __aexit__(
        self,
        exc_type: object,
        exc: object,
        tb: object,
    ) -> None:
        return None


class _ReadPool:
    def __init__(self, conn: object, *, max_size: int = 4) -> None:
        self._conn = conn
        self._max_size = max_size
        self.acquires = 0

    def get_max_size(self) -> int:
        return self._max_size

    def acquire(self) -> _AcquireConn:
        self.acquires += 1
        return _AcquireConn(self._conn)


def test_inquiry_private_aliases_point_to_action_execution_module() -> None:
    assert inquiry._execute_action is action_execution._execute_action
    assert (
        inquiry._execute_question_retrieval_actions
        is action_execution._execute_question_retrieval_actions
    )
    assert inquiry._action_timing_note is action_execution._action_timing_note
    assert inquiry._QuestionRetrievalPlan is action_execution._QuestionRetrievalPlan
    assert inquiry._ActionExecutionRecord is action_execution._ActionExecutionRecord


def test_action_timing_note_includes_cache_and_motif_details() -> None:
    action = RetrievalAction(
        "Q1",
        "semantic",
        "constraint_evidence",
        filters={
            "_motif_id": str(uuid4()),
            "_motif_stage": 2,
            "_motif_match_score": 0.8,
            "_motif_utility_score": 1.2,
            "_bound_scope": {"model_count": 1},
        },
    )
    result = PathwayResult(
        models=[SimpleNamespace()],
        observations=[SimpleNamespace()],
        resources=[SimpleNamespace()],
        source_pathway="B",
    )

    note = action_execution._action_timing_note(
        action,
        result,
        elapsed_ms=12,
        cache_hit=True,
    )

    assert note["question_id"] == "Q1"
    assert note["models"] == 1
    assert note["source_pathway"] == "B"
    assert note["cache_hit"] is True
    assert note["timing_kind"] == "cache_hit"
    assert note["in_flight_wait"] is False
    assert note["work_elapsed_ms"] == 0
    assert note["wait_elapsed_ms"] == 0
    assert note["motif_stage"] == 2
    assert note["bound_scope"] == {"model_count": 1}


def test_action_timing_note_includes_reconstruction_details() -> None:
    action = RetrievalAction(
        "Q1",
        "semantic",
        "owner_evidence",
        filters={
            "_reconstruction_stage": 2,
            "_reconstruction_round": 3,
            "_reconstruction_active_cues": ["owner", "audit"],
            "_reconstruction_cue_count": 2,
            "_bound_scope": {"model_count": 2},
        },
    )

    note = action_execution._action_timing_note(
        action,
        PathwayResult(source_pathway="B"),
        elapsed_ms=4,
        cache_hit=False,
    )

    assert action_execution._action_stage(action) == 2
    assert note["reconstruction_stage"] == 2
    assert note["reconstruction_round"] == 3
    assert note["reconstruction_cue_count"] == 2
    assert note["reconstruction_active_cues"] == ["owner", "audit"]
    assert note["bound_scope"] == {"model_count": 2}


def test_action_timing_note_includes_sage_policy_details() -> None:
    action = RetrievalAction(
        "Q1",
        "semantic",
        "owner_evidence",
        filters={
            "_sage_policy_stage": 2,
            "_sage_policy_mode": "probe",
            "_sage_policy_reason": "semantic_probe_after_structural_first_actions",
        },
    )

    note = action_execution._action_timing_note(
        action,
        PathwayResult(source_pathway="B"),
        elapsed_ms=5,
        cache_hit=False,
    )

    assert action_execution._action_stage(action) == 2
    assert note["sage_policy_stage"] == 2
    assert note["sage_policy_mode"] == "probe"
    assert (
        note["sage_policy_reason"]
        == "semantic_probe_after_structural_first_actions"
    )


def test_action_timing_note_includes_sage_route_utility_details() -> None:
    action = RetrievalAction(
        "Q1",
        "semantic",
        "owner_evidence",
        filters={
            "_sage_policy_stage": 2,
            "_sage_policy_mode": "skip",
            "_sage_policy_reason": "negative_route_utility",
            "_sage_route_utility_score": -0.62,
            "_sage_route_utility_confidence": 0.5,
            "_sage_route_utility_match": 0.84,
            "_sage_route_utility_skip": True,
        },
    )

    note = action_execution._action_timing_note(
        action,
        None,
        elapsed_ms=0,
        cache_hit=False,
    )

    assert note["sage_route_utility_score"] == -0.62
    assert note["sage_route_utility_confidence"] == 0.5
    assert note["sage_route_utility_match"] == 0.84
    assert note["sage_route_utility_skip"] is True


def test_action_timing_note_includes_focused_index_diagnostics() -> None:
    action = RetrievalAction("Q1", "focused_index", "question_answerability_scope")
    model_id = uuid4()
    result = PathwayResult(
        models=[SimpleNamespace(id=model_id)],
        source_pathway="focused_index",
        notes={
            "scan_timeouts": {
                "answerability_index": False,
                "scope_sparse": True,
                "direct_scope": False,
            },
            "top_hits": [
                {
                    "model_id": str(model_id),
                    "sources": ["answerability_index", "scope_sparse"],
                }
            ],
        },
    )

    note = action_execution._action_timing_note(
        action,
        result,
        elapsed_ms=17,
        cache_hit=False,
    )

    assert note["source_set"] == ["answerability_index", "scope_sparse"]
    assert note["source_count"] == 2
    assert note["scan_timeouts"] == {
        "answerability_index": False,
        "scope_sparse": True,
        "direct_scope": False,
    }
    assert note["bounded_lookup_timeout_count"] == 1


def test_action_timing_note_includes_semantic_substrate_subtimings() -> None:
    action = RetrievalAction("Q1", "semantic", "constraint_evidence")
    result = PathwayResult(
        source_pathway="B",
        notes={
            "semantic_substrate_timings_ms": {
                "dense_ms": 12,
                "semantic_terms_ms": 3,
                "merge_hydrate_ms": 2,
            }
        },
    )

    note = action_execution._action_timing_note(
        action,
        result,
        elapsed_ms=20,
        cache_hit=False,
    )

    assert note["semantic_substrate_timings_ms"] == {
        "dense_ms": 12,
        "semantic_terms_ms": 3,
        "merge_hydrate_ms": 2,
    }


def test_action_timing_note_includes_temporal_lane_and_subtimings() -> None:
    action = RetrievalAction("Q1", "temporal", "nearby_counterevidence")
    result = PathwayResult(
        source_pathway="C",
        notes={
            "temporal_action": {
                "lane": "nearby",
                "window_days": 2,
                "broad_fallback": False,
            },
            "temporal_timings_ms": {
                "observations_query_ms": 5,
                "observations_hydrate_ms": 1,
                "models_query_ms": 7,
                "models_hydrate_ms": 2,
            },
        },
    )

    note = action_execution._action_timing_note(
        action,
        result,
        elapsed_ms=20,
        cache_hit=False,
    )

    assert note["temporal_lane"] == "nearby"
    assert note["temporal_window_days"] == 2
    assert note["temporal_broad_fallback"] is False
    assert note["temporal_timings_ms"] == {
        "observations_query_ms": 5,
        "observations_hydrate_ms": 1,
        "models_query_ms": 7,
        "models_hydrate_ms": 2,
    }


def test_question_retrieval_plan_defaults_to_empty_action_lists() -> None:
    plan = action_execution._QuestionRetrievalPlan(question=_question())

    assert plan.actions_to_run == []
    assert plan.skipped_timing_notes == []
    assert plan.learned_motif is None


@pytest.mark.asyncio
async def test_execute_action_dispatches_semantic_terms_action(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    expected = PathwayResult(models=[SimpleNamespace(id=uuid4())], source_pathway="L")
    calls: list[tuple[str, int]] = []

    async def fake_execute_semantic_terms_action(
        action: RetrievalAction,
        *_args: object,
        model_limit: int,
        **_kwargs: object,
    ) -> PathwayResult:
        calls.append((action.path, model_limit))
        return expected

    monkeypatch.setattr(
        action_execution,
        "_execute_semantic_terms_action",
        fake_execute_semantic_terms_action,
    )

    result = await action_execution._execute_action(
        RetrievalAction("Q1", "semantic_terms", "constraint_semantic_terms", budget=7),
        _trigger(),
        object(),  # type: ignore[arg-type]
        None,
        InquiryConfig(action_model_budget_limit=5),
    )

    assert result is expected
    assert calls == [("semantic_terms", 5)]


@pytest.mark.asyncio
async def test_execute_action_passes_temporal_scope_strategy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[dict[str, object]] = []

    async def fake_pathway_c_temporal(
        *_args: object,
        include_entity_mentions: bool,
        scope_filter_strategy: str,
        **_kwargs: object,
    ) -> PathwayResult:
        calls.append(
            {
                "include_entity_mentions": include_entity_mentions,
                "scope_filter_strategy": scope_filter_strategy,
            }
        )
        return PathwayResult(
            source_pathway="C",
            notes={
                "include_entity_mentions": include_entity_mentions,
                "temporal_scope_filter_strategy": scope_filter_strategy,
            },
        )

    monkeypatch.setattr(
        action_execution,
        "pathway_c_temporal",
        fake_pathway_c_temporal,
    )

    result = await action_execution._execute_action(
        RetrievalAction(
            "Q1",
            "temporal",
            "nearby_counterevidence",
            filters={
                "window_days": 2,
                "_temporal_lane": "nearby",
                "_temporal_scope_filter_strategy": "time_prefilter",
                "_temporal_include_entity_mentions": False,
            },
        ),
        _trigger(),
        object(),  # type: ignore[arg-type]
        None,
        InquiryConfig(),
    )

    assert result is not None
    assert calls == [
        {
            "include_entity_mentions": False,
            "scope_filter_strategy": "time_prefilter",
        }
    ]
    assert result.notes["temporal_action"]["scope_filter_strategy"] == "time_prefilter"


@pytest.mark.asyncio
async def test_duplicate_semantic_actions_across_question_plans_share_execution(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    semantic_result = PathwayResult(
        models=[SimpleNamespace(id=uuid4())],
        source_pathway="B",
    )
    calls: list[str] = []

    async def fake_pathway_b_semantic(
        query_text: str,
        *_args: object,
        **_kwargs: object,
    ) -> PathwayResult:
        calls.append(query_text)
        return semantic_result

    async def fake_semantic_term_rescue(
        *_args: object,
        **_kwargs: object,
    ) -> tuple[list[ModelCandidateHit], dict[str, object]]:
        return [], {"enabled": True, "used": False}

    async def fake_representation_tag_rescue(
        *_args: object,
        **_kwargs: object,
    ) -> tuple[list[ModelCandidateHit], dict[str, object]]:
        return [], {"enabled": True, "used": False}

    async def fake_lexical_rescue(
        *_args: object,
        **_kwargs: object,
    ) -> tuple[list[tuple[SimpleNamespace, int]], dict[str, object]]:
        return [], {"enabled": False, "used": False, "lexical_count": 0}

    monkeypatch.setattr(
        retrieval_actions,
        "pathway_b_semantic",
        fake_pathway_b_semantic,
    )
    monkeypatch.setattr(
        retrieval_actions,
        "_semantic_term_candidate_rescue",
        fake_semantic_term_rescue,
    )
    monkeypatch.setattr(
        retrieval_actions,
        "_representation_tag_candidate_rescue",
        fake_representation_tag_rescue,
    )
    monkeypatch.setattr(
        retrieval_actions,
        "_semantic_hybrid_lexical_rescue",
        fake_lexical_rescue,
    )

    plans = [
        action_execution._QuestionRetrievalPlan(
            question=_question("Q1"),
            actions_to_run=[
                RetrievalAction(
                    "Q1",
                    "semantic",
                    "constraint_evidence",
                    query="same blocked launch dependency",
                    budget=5,
                )
            ],
        ),
        action_execution._QuestionRetrievalPlan(
            question=_question("Q2"),
            actions_to_run=[
                RetrievalAction(
                    "Q2",
                    "semantic",
                    "constraint_evidence",
                    query="same blocked launch dependency",
                    budget=5,
                )
            ],
        ),
    ]
    read_pool = _ReadPool(object())

    records_by_qid = await action_execution._execute_question_retrieval_actions(
        plans,
        _trigger(),
        object(),  # type: ignore[arg-type]
        None,
        InquiryConfig(
            question_action_parallel_enabled=True,
            question_action_parallelism=4,
            semantic_hybrid_lexical_enabled=False,
        ),
        {},
        read_pool=read_pool,  # type: ignore[arg-type]
    )

    assert calls == ["same blocked launch dependency"]
    assert records_by_qid["Q1"][0].path_result is semantic_result
    assert records_by_qid["Q2"][0].path_result is semantic_result
    assert records_by_qid["Q1"][0].timing_note["cache_hit"] is False
    assert records_by_qid["Q2"][0].timing_note["cache_hit"] is True
    assert records_by_qid["Q1"][0].timing_note["timing_kind"] == "owner_work"
    assert records_by_qid["Q2"][0].timing_note["timing_kind"] == "cache_hit"


@pytest.mark.asyncio
async def test_staged_question_actions_run_same_stage_across_plans_in_parallel(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    started: list[str] = []
    all_started = asyncio.Event()

    async def fake_execute_action(
        action: RetrievalAction,
        *_args: object,
        **_kwargs: object,
    ) -> PathwayResult:
        started.append(action.question_id)
        if len(started) == 2:
            all_started.set()
        await asyncio.wait_for(all_started.wait(), timeout=0.5)
        return PathwayResult(
            models=[SimpleNamespace(id=uuid4())],
            source_pathway="B",
        )

    monkeypatch.setattr(action_execution, "_execute_action", fake_execute_action)

    plans = [
        action_execution._QuestionRetrievalPlan(
            question=_question("Q1"),
            actions_to_run=[
                RetrievalAction(
                    "Q1",
                    "semantic",
                    "stage_one_a",
                    query="stage one a",
                    filters={"_motif_stage": 1},
                )
            ],
        ),
        action_execution._QuestionRetrievalPlan(
            question=_question("Q2"),
            actions_to_run=[
                RetrievalAction(
                    "Q2",
                    "semantic",
                    "stage_one_b",
                    query="stage one b",
                    filters={"_motif_stage": 1},
                )
            ],
        ),
    ]

    records_by_qid = await action_execution._execute_question_retrieval_actions(
        plans,
        _trigger(),
        object(),  # type: ignore[arg-type]
        None,
        InquiryConfig(
            question_action_parallel_enabled=True,
            question_action_parallelism=2,
        ),
        {},
        read_pool=_ReadPool(object()),  # type: ignore[arg-type]
    )

    assert started == ["Q1", "Q2"]
    assert records_by_qid["Q1"][0].timing_note["cache_hit"] is False
    assert records_by_qid["Q2"][0].timing_note["cache_hit"] is False
    assert records_by_qid["Q1"][0].timing_note["timing_kind"] == "owner_work"
    assert records_by_qid["Q2"][0].timing_note["timing_kind"] == "owner_work"


@pytest.mark.asyncio
async def test_staged_duplicate_actions_share_in_flight_execution(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    result = PathwayResult(models=[SimpleNamespace(id=uuid4())], source_pathway="B")
    calls = 0

    async def fake_execute_action(
        action: RetrievalAction,
        *_args: object,
        **_kwargs: object,
    ) -> PathwayResult:
        nonlocal calls
        calls += 1
        await asyncio.sleep(0)
        return result

    monkeypatch.setattr(action_execution, "_execute_action", fake_execute_action)

    action_one = RetrievalAction(
        "Q1",
        "semantic",
        "shared_stage_action",
        query="shared stage query",
        filters={"_motif_stage": 1},
    )
    action_two = RetrievalAction(
        "Q2",
        "semantic",
        "shared_stage_action",
        query="shared stage query",
        filters={"_motif_stage": 1},
    )
    plans = [
        action_execution._QuestionRetrievalPlan(
            question=_question("Q1"),
            actions_to_run=[action_one],
        ),
        action_execution._QuestionRetrievalPlan(
            question=_question("Q2"),
            actions_to_run=[action_two],
        ),
    ]

    records_by_qid = await action_execution._execute_question_retrieval_actions(
        plans,
        _trigger(),
        object(),  # type: ignore[arg-type]
        None,
        InquiryConfig(
            question_action_parallel_enabled=True,
            question_action_parallelism=2,
        ),
        {},
        read_pool=_ReadPool(object()),  # type: ignore[arg-type]
    )

    cache_hits = [
        records_by_qid["Q1"][0].timing_note["cache_hit"],
        records_by_qid["Q2"][0].timing_note["cache_hit"],
    ]
    assert calls == 1
    assert sorted(cache_hits) == [False, True]
    assert sorted(
        [
            records_by_qid["Q1"][0].timing_note["timing_kind"],
            records_by_qid["Q2"][0].timing_note["timing_kind"],
        ]
    ) == ["in_flight_wait", "owner_work"]
    assert records_by_qid["Q1"][0].path_result is result
    assert records_by_qid["Q2"][0].path_result is result


@pytest.mark.asyncio
async def test_parallel_structural_action_uses_shared_read_budget_for_nested_fanout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    nested_denied: list[bool] = []

    async def fake_pathway_a_structural(*_args: object, **kwargs: object):
        read_fanout_budget = kwargs["read_fanout_budget"]
        assert read_fanout_budget is not None
        async with read_fanout_budget.connection_if_available() as nested_conn:
            nested_denied.append(nested_conn is None)
        return PathwayResult(source_pathway="A")

    monkeypatch.setattr(
        action_execution,
        "pathway_a_structural",
        fake_pathway_a_structural,
    )

    read_pool = _ReadPool(object(), max_size=1)
    records_by_qid = await asyncio.wait_for(
        action_execution._execute_question_retrieval_actions(
            [
                action_execution._QuestionRetrievalPlan(
                    question=_question("Q1"),
                    actions_to_run=[
                        RetrievalAction(
                            "Q1",
                            "structural",
                            "dependency_scope",
                            filters={
                                "seed_entities": [
                                    {"type": "commitment", "id": str(uuid4())}
                                ]
                            },
                            budget=5,
                        )
                    ],
                )
            ],
            _trigger(),
            object(),  # type: ignore[arg-type]
            None,
            InquiryConfig(
                question_action_parallel_enabled=True,
                question_action_parallelism=2,
                structural_read_fanout_enabled=True,
                structural_read_fanout_min_seeds=1,
                structural_read_fanout_chunk_size=1,
            ),
            {},
            read_pool=read_pool,  # type: ignore[arg-type]
        ),
        timeout=1.0,
    )

    assert nested_denied == [True]
    assert read_pool.acquires == 1
    assert records_by_qid["Q1"][0].path_result is not None
    assert records_by_qid["Q1"][0].path_result.source_pathway == "A"


@pytest.mark.asyncio
async def test_staged_bound_scope_remains_per_plan_under_parallel_execution(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    commitment_one = uuid4()
    commitment_two = uuid4()
    model_one = uuid4()
    model_two = uuid4()
    stage_two_filters: list[tuple[str, dict[str, object]]] = []

    async def fake_execute_action(
        action: RetrievalAction,
        *_args: object,
        **_kwargs: object,
    ) -> PathwayResult:
        if int(action.filters.get("_motif_stage") or 1) == 1:
            if action.question_id == "Q1":
                model_id = model_one
                commitment_id = commitment_one
            else:
                model_id = model_two
                commitment_id = commitment_two
            return PathwayResult(
                models=[
                    SimpleNamespace(
                        id=model_id,
                        scope_entities=[
                            {"type": "commitment", "id": str(commitment_id)}
                        ],
                    )
                ],
                source_pathway="B",
            )
        stage_two_filters.append((action.question_id, dict(action.filters)))
        return PathwayResult(models=[], source_pathway="B")

    monkeypatch.setattr(action_execution, "_execute_action", fake_execute_action)

    def plan(question_id: str, stage_one_query: str) -> (
        action_execution._QuestionRetrievalPlan
    ):
        return action_execution._QuestionRetrievalPlan(
            question=_question(question_id),
            actions_to_run=[
                RetrievalAction(
                    question_id,
                    "semantic",
                    "stage_one",
                    query=stage_one_query,
                    filters={"_motif_stage": 1},
                ),
                RetrievalAction(
                    question_id,
                    "semantic",
                    "stage_two",
                    query="same follow-up query",
                    filters={"_motif_stage": 2, "_bind_previous_scope": True},
                ),
            ],
        )

    records_by_qid = await action_execution._execute_question_retrieval_actions(
        [plan("Q1", "stage one one"), plan("Q2", "stage one two")],
        _trigger(),
        object(),  # type: ignore[arg-type]
        None,
        InquiryConfig(
            question_action_parallel_enabled=True,
            question_action_parallelism=4,
        ),
        {},
        read_pool=_ReadPool(object()),  # type: ignore[arg-type]
    )

    assert len(stage_two_filters) == 2
    by_qid = {qid: filters for qid, filters in stage_two_filters}
    assert by_qid["Q1"]["_bound_scope"] == {"model_count": 1, "entity_count": 1}
    assert by_qid["Q2"]["_bound_scope"] == {"model_count": 1, "entity_count": 1}
    assert by_qid["Q1"]["seed_model_ids"] == [str(model_one)]
    assert by_qid["Q2"]["seed_model_ids"] == [str(model_two)]
    assert by_qid["Q1"]["seed_entities"] == [
        {"type": "commitment", "id": str(commitment_one)}
    ]
    assert by_qid["Q2"]["seed_entities"] == [
        {"type": "commitment", "id": str(commitment_two)}
    ]
    assert len(records_by_qid["Q1"]) == 2
    assert len(records_by_qid["Q2"]) == 2
    assert records_by_qid["Q1"][1].timing_note["cache_hit"] is False
    assert records_by_qid["Q2"][1].timing_note["cache_hit"] is False


@pytest.mark.asyncio
async def test_dense_semantic_fallback_skips_when_semantic_terms_are_sufficient(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []
    models = [SimpleNamespace(id=uuid4()) for _ in range(3)]

    async def fake_execute_action(
        action: RetrievalAction,
        *_args: object,
        **_kwargs: object,
    ) -> PathwayResult:
        calls.append(action.path)
        if action.path == "semantic_terms":
            return PathwayResult(models=models, source_pathway="L")
        return PathwayResult(models=[SimpleNamespace(id=uuid4())], source_pathway="B")

    monkeypatch.setattr(action_execution, "_execute_action", fake_execute_action)

    plan = action_execution._QuestionRetrievalPlan(
        question=_question("Q1"),
        actions_to_run=[
            RetrievalAction(
                "Q1",
                "semantic_terms",
                "constraint_semantic_terms",
                filters={"_sage_policy_stage": 1},
            ),
            RetrievalAction(
                "Q1",
                "semantic",
                "constraint_evidence",
                filters={
                    "_sage_policy_stage": 2,
                    "_semantic_fallback_after_terms": True,
                    "_fallback_min_semantic_terms_models": 3,
                },
            ),
        ],
    )

    records_by_qid = await action_execution._execute_question_retrieval_actions(
        [plan],
        _trigger(),
        object(),  # type: ignore[arg-type]
        None,
        InquiryConfig(question_action_parallel_enabled=False),
        {},
        read_pool=None,
    )

    assert calls == ["semantic_terms"]
    assert len(records_by_qid["Q1"]) == 2
    skipped = records_by_qid["Q1"][1].timing_note
    assert skipped["path"] == "semantic"
    assert skipped["skipped"] is True
    assert skipped["skip_reason"] == "semantic_terms_sufficient:3>=3"


@pytest.mark.asyncio
async def test_sage_route_utility_skip_admission_prevents_action_execution(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []

    async def fake_execute_action(
        action: RetrievalAction,
        *_args: object,
        **_kwargs: object,
    ) -> PathwayResult:
        calls.append(action.path)
        return PathwayResult(models=[SimpleNamespace(id=uuid4())], source_pathway="B")

    monkeypatch.setattr(action_execution, "_execute_action", fake_execute_action)

    plan = action_execution._QuestionRetrievalPlan(
        question=_question("Q1"),
        actions_to_run=[
            RetrievalAction(
                "Q1",
                "semantic",
                "constraint_evidence",
                filters={
                    "_sage_policy_stage": 2,
                    "_sage_policy_mode": "skip",
                    "_sage_policy_reason": "negative_route_utility",
                    "_sage_route_utility_skip": True,
                },
            ),
        ],
    )

    records_by_qid = await action_execution._execute_question_retrieval_actions(
        [plan],
        _trigger(),
        object(),  # type: ignore[arg-type]
        None,
        InquiryConfig(question_action_parallel_enabled=False),
        {},
        read_pool=None,
    )

    assert calls == []
    skipped = records_by_qid["Q1"][0].timing_note
    assert skipped["path"] == "semantic"
    assert skipped["skipped"] is True
    assert skipped["skip_reason"].startswith("sage_route_utility_skip:")


@pytest.mark.asyncio
async def test_dense_semantic_fallback_skips_when_cheap_context_is_sufficient(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []
    models = [SimpleNamespace(id=uuid4()) for _ in range(6)]

    async def fake_execute_action(
        action: RetrievalAction,
        *_args: object,
        **_kwargs: object,
    ) -> PathwayResult:
        calls.append(action.path)
        if action.path == "focused_index":
            return PathwayResult(models=models, source_pathway="focused_index")
        return PathwayResult(models=[SimpleNamespace(id=uuid4())], source_pathway="B")

    monkeypatch.setattr(action_execution, "_execute_action", fake_execute_action)

    plan = action_execution._QuestionRetrievalPlan(
        question=_question("Q1"),
        actions_to_run=[
            RetrievalAction(
                "Q1",
                "focused_index",
                "question_answerability_scope",
                filters={"_sage_policy_stage": 1},
            ),
            RetrievalAction(
                "Q1",
                "semantic",
                "constraint_evidence",
                filters={
                    "_sage_policy_stage": 2,
                    "_semantic_fallback_after_terms": True,
                    "_fallback_min_semantic_terms_models": 3,
                    "_fallback_min_cheap_context_models": 6,
                },
            ),
        ],
    )

    records_by_qid = await action_execution._execute_question_retrieval_actions(
        [plan],
        _trigger(),
        object(),  # type: ignore[arg-type]
        None,
        InquiryConfig(question_action_parallel_enabled=False),
        {},
        read_pool=None,
    )

    assert calls == ["focused_index"]
    skipped = records_by_qid["Q1"][1].timing_note
    assert skipped["path"] == "semantic"
    assert skipped["skipped"] is True
    assert skipped["skip_reason"] == "cheap_context_sufficient:6>=6"


@pytest.mark.asyncio
async def test_broad_temporal_fallback_skips_when_nearby_is_sufficient(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []
    model = SimpleNamespace(id=uuid4())
    observation = SimpleNamespace(id=uuid4())

    async def fake_execute_action(
        action: RetrievalAction,
        *_args: object,
        **_kwargs: object,
    ) -> PathwayResult:
        calls.append(action.target)
        return PathwayResult(
            models=[model],
            observations=[observation],
            source_pathway="C",
        )

    monkeypatch.setattr(action_execution, "_execute_action", fake_execute_action)

    plan = action_execution._QuestionRetrievalPlan(
        question=_question("Q1"),
        actions_to_run=[
            RetrievalAction(
                "Q1",
                "temporal",
                "nearby_counterevidence",
                filters={"_sage_policy_stage": 1, "_temporal_lane": "nearby"},
            ),
            RetrievalAction(
                "Q1",
                "temporal",
                "recent_counterevidence",
                filters={
                    "_sage_policy_stage": 2,
                    "_temporal_lane": "broad",
                    "_temporal_broad_fallback_after_nearby": True,
                    "_fallback_min_temporal_records": 2,
                },
            ),
        ],
    )

    records_by_qid = await action_execution._execute_question_retrieval_actions(
        [plan],
        _trigger(),
        object(),  # type: ignore[arg-type]
        None,
        InquiryConfig(question_action_parallel_enabled=False),
        {},
        read_pool=None,
    )

    assert calls == ["nearby_counterevidence"]
    skipped = records_by_qid["Q1"][1].timing_note
    assert skipped["path"] == "temporal"
    assert skipped["skipped"] is True
    assert skipped["skip_reason"] == "temporal_nearby_sufficient:2>=2"


@pytest.mark.asyncio
async def test_nearby_temporal_skips_when_cheap_context_is_sufficient(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[str] = []
    focused_models = [SimpleNamespace(id=uuid4()) for _ in range(8)]
    semantic_models = focused_models[:3]

    async def fake_execute_action(
        action: RetrievalAction,
        *_args: object,
        **_kwargs: object,
    ) -> PathwayResult:
        calls.append(action.path)
        if action.path == "focused_index":
            return PathwayResult(models=focused_models, source_pathway="focused_index")
        if action.path == "semantic_terms":
            return PathwayResult(models=semantic_models, source_pathway="L")
        return PathwayResult(source_pathway="C")

    monkeypatch.setattr(action_execution, "_execute_action", fake_execute_action)

    plan = action_execution._QuestionRetrievalPlan(
        question=_question("Q1"),
        actions_to_run=[
            RetrievalAction(
                "Q1",
                "focused_index",
                "question_answerability_scope",
                filters={"_sage_policy_stage": 1},
            ),
            RetrievalAction(
                "Q1",
                "semantic_terms",
                "counterevidence_semantic_terms",
                filters={"_sage_policy_stage": 1},
            ),
            RetrievalAction(
                "Q1",
                "temporal",
                "nearby_counterevidence",
                filters={
                    "_sage_policy_stage": 2,
                    "_temporal_lane": "nearby",
                    "_temporal_nearby_fallback_after_cheap_context": True,
                    "_fallback_min_temporal_cheap_context_models": 8,
                    "_fallback_min_temporal_semantic_terms_models": 3,
                },
            ),
        ],
    )

    records_by_qid = await action_execution._execute_question_retrieval_actions(
        [plan],
        _trigger(),
        object(),  # type: ignore[arg-type]
        None,
        InquiryConfig(question_action_parallel_enabled=False),
        {},
        read_pool=None,
    )

    assert calls == ["focused_index", "semantic_terms"]
    skipped = records_by_qid["Q1"][2].timing_note
    assert skipped["path"] == "temporal"
    assert skipped["skipped"] is True
    assert skipped["skip_reason"] == (
        "temporal_cheap_context_sufficient:cheap=8>=8,semantic_terms=3>=3"
    )
    assert skipped["admission_coverage"] == {
        "semantic_terms_models": 3,
        "cheap_context_models": 8,
        "temporal_records": 0,
    }


@pytest.mark.asyncio
async def test_shared_action_execution_session_coalesces_concurrent_plan_calls(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    result = PathwayResult(models=[SimpleNamespace(id=uuid4())], source_pathway="B")
    calls = 0

    async def fake_execute_action(
        action: RetrievalAction,
        *_args: object,
        **_kwargs: object,
    ) -> PathwayResult:
        nonlocal calls
        calls += 1
        await asyncio.sleep(0)
        return result

    monkeypatch.setattr(action_execution, "_execute_action", fake_execute_action)
    session = action_execution.ActionExecutionSession(parallelism=2)
    trigger = _trigger()
    cfg = InquiryConfig(
        question_action_parallel_enabled=True,
        question_action_parallelism=2,
    )
    action_one = RetrievalAction(
        "Q1",
        "focused_index",
        "shared_target",
        query="shared action query",
        budget=5,
    )
    action_two = RetrievalAction(
        "Q2",
        "focused_index",
        "shared_target",
        query="shared action query",
        budget=5,
    )

    records_one, records_two = await asyncio.gather(
        action_execution._execute_question_retrieval_actions(
            [
                action_execution._QuestionRetrievalPlan(
                    question=_question("Q1"),
                    actions_to_run=[action_one],
                )
            ],
            trigger,
            object(),  # type: ignore[arg-type]
            None,
            cfg,
            {},
            read_pool=_ReadPool(object()),  # type: ignore[arg-type]
            execution_session=session,
        ),
        action_execution._execute_question_retrieval_actions(
            [
                action_execution._QuestionRetrievalPlan(
                    question=_question("Q2"),
                    actions_to_run=[action_two],
                )
            ],
            trigger,
            object(),  # type: ignore[arg-type]
            None,
            cfg,
            {},
            read_pool=_ReadPool(object()),  # type: ignore[arg-type]
            execution_session=session,
        ),
    )

    assert calls == 1
    cache_hits = [
        records_one["Q1"][0].timing_note["cache_hit"],
        records_two["Q2"][0].timing_note["cache_hit"],
    ]
    assert sorted(cache_hits) == [False, True]
    assert sorted(
        [
            records_one["Q1"][0].timing_note["timing_kind"],
            records_two["Q2"][0].timing_note["timing_kind"],
        ]
    ) == ["in_flight_wait", "owner_work"]
    assert records_one["Q1"][0].path_result is result
    assert records_two["Q2"][0].path_result is result
