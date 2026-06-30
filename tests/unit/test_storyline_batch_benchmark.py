import argparse
import asyncio
import json
from types import SimpleNamespace
from uuid import uuid4

import pytest

from scripts import run_storyline_batch_benchmark as benchmark
from scripts.run_storyline_batch_benchmark import (
    _LATENT_BRIDGE_STORYLINE_ID,
    _PRODUCT_VALUE_EVAL_KEYS,
    STORYLINES,
    StorylineScore,
    _accumulate_edge_ops_stats,
    _benchmark_summary,
    _company_intelligence_scorecard,
    _empty_edge_ops_stats,
    _future_wave_trigger_ids,
    _latent_pattern_assessment,
    _noise_noop_score,
    _retrieval_context_budget_fit_score,
    _render_benchmark_markdown,
    _render_variance_markdown,
    _story_id_from_external_id,
    _storyline_calibration_report,
    build_variance_report,
    build_storyline_scenario,
)


class _StopAfterMigration(Exception):
    pass


class _TimedOutRows(list):
    timed_out = True


class _FakeAcquire:
    async def __aenter__(self):
        return object()

    async def __aexit__(self, exc_type, exc, tb):
        return False


class _FakePool:
    def acquire(self):
        return _FakeAcquire()


class _FakeSeedOnlyConn:
    async def fetchval(self, query, *args):
        if "FROM models" in query:
            return 42
        if "FROM observations" in query:
            return 7
        return None


class _FakeSeedOnlyAcquire:
    async def __aenter__(self):
        return _FakeSeedOnlyConn()

    async def __aexit__(self, exc_type, exc, tb):
        return False


class _FakeSeedOnlyPool:
    def acquire(self):
        return _FakeSeedOnlyAcquire()


class _EmptyProbeTransaction:
    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return False


class _EmptyProbeConn:
    async def fetchval(self, *_args, **_kwargs):
        return None

    async def fetch(self, *_args, **_kwargs):
        return []

    async def execute(self, *_args, **_kwargs):
        return None

    def transaction(self):
        return _EmptyProbeTransaction()


class _ReadinessProbeConn:
    tables = {
        "model_scope_entities",
        "model_sparse_terms",
        "model_answerability_index",
        "model_representation_tag_postings",
        "model_scope_actors",
        "model_semantic_term_postings",
        "model_operational_role_postings",
    }
    status_tables = {
        "model_sparse_terms",
        "model_answerability_index",
        "model_representation_tag_postings",
        "model_semantic_term_postings",
        "model_operational_role_postings",
    }

    def __init__(
        self,
        *,
        active_models: int = 100,
        sparse_posted_models: int = 98,
        sparse_rows: int = 1200,
        sparse_terms: int = 25,
    ) -> None:
        self.active_models = active_models
        self.metrics = {
            "model_scope_entities": {
                "row_count": 300,
                "active_row_count": 300,
                "active_model_hit_count": 95,
                "orphan_row_count": 0,
                "distinct_entities": 6,
            },
            "model_sparse_terms": {
                "row_count": sparse_rows,
                "active_row_count": sparse_rows,
                "active_model_hit_count": sparse_posted_models,
                "orphan_row_count": 0,
                "distinct_terms": sparse_terms,
            },
            "model_answerability_index": {
                "row_count": 1000,
                "active_row_count": 1000,
                "active_model_hit_count": 98,
                "orphan_row_count": 0,
                "distinct_terms": 22,
                "probe_primitives": 3,
            },
            "model_representation_tag_postings": {
                "row_count": 300,
                "active_row_count": 300,
                "active_model_hit_count": 100,
                "orphan_row_count": 0,
                "distinct_tags": 8,
            },
            "model_scope_actors": {
                "row_count": 0,
                "active_row_count": 0,
                "active_model_hit_count": 0,
                "orphan_row_count": 0,
            },
            "model_semantic_term_postings": {
                "row_count": 0,
                "active_row_count": 0,
                "active_model_hit_count": 0,
                "orphan_row_count": 0,
            },
            "model_operational_role_postings": {
                "row_count": 0,
                "active_row_count": 0,
                "active_model_hit_count": 0,
                "orphan_row_count": 0,
            },
        }

    def _table_from_query(self, query: str) -> str:
        for table in self.tables:
            if table in query:
                return table
        return ""

    async def fetchval(self, query: str, *args: object, **_kwargs: object):
        if "to_regclass" in query:
            table = str(args[0]).removeprefix("public.")
            return args[0] if table in self.tables else None
        if "information_schema.columns" in query:
            table, column = str(args[0]), str(args[1])
            return column == "status" and table in self.status_tables
        if "FROM models" in query and "status = 'active'" in query:
            return self.active_models

        table = self._table_from_query(query)
        metrics = self.metrics.get(table, {})
        if "entity_type" in query:
            return metrics.get("distinct_entities", 0)
        if "primitive = ANY" in query:
            return metrics.get("probe_primitives", 0)
        if "DISTINCT tag_type" in query:
            return metrics.get("distinct_tags", 0)
        if "DISTINCT term" in query:
            return metrics.get("distinct_terms", 0)
        return None

    async def fetchrow(self, query: str, *_args: object, **_kwargs: object):
        table = self._table_from_query(query)
        return dict(self.metrics.get(table, {}))


def test_prepare_benchmark_tenant_uses_warn_migration_policy(monkeypatch) -> None:
    calls: list[dict] = []

    async def _fake_apply_migrations_dir(conn, migrations_dir, *, on_error="stop"):
        calls.append({
            "conn": conn,
            "migrations_dir": migrations_dir,
            "on_error": on_error,
        })
        raise _StopAfterMigration

    monkeypatch.setattr(
        benchmark,
        "apply_migrations_dir",
        _fake_apply_migrations_dir,
    )
    args = argparse.Namespace(skip_migrations=False)

    try:
        asyncio.run(
            benchmark._prepare_storyline_benchmark_tenant(
                args,
                pool=_FakePool(),
                scenario=None,
                append_context=None,
                run_id="unit-migration-policy",
                horizon_start_batch=0,
            )
        )
    except _StopAfterMigration:
        pass
    else:  # pragma: no cover - this helper must stop before tenant seeding.
        raise AssertionError("expected migration sentinel")

    assert calls
    assert calls[0]["on_error"] == "warn"


def test_parse_args_accepts_seed_only_mode(monkeypatch) -> None:
    monkeypatch.setattr(
        "sys.argv",
        [
            "run_storyline_batch_benchmark.py",
            "--mode",
            "seed-only",
            "--run-id",
            "unit-seed-baseline",
        ],
    )

    args = benchmark.parse_args()

    assert args.mode == "seed-only"
    assert args.target_t1_batches == 0


def test_parse_args_rejects_seed_only_append(monkeypatch) -> None:
    monkeypatch.setattr(
        "sys.argv",
        [
            "run_storyline_batch_benchmark.py",
            "--mode",
            "seed-only",
            "--append-to-run-id",
            "base",
        ],
    )

    with pytest.raises(SystemExit, match="seed-only cannot be combined"):
        benchmark.parse_args()


def test_parse_args_accepts_retrieval_probe_with_append_run(monkeypatch) -> None:
    monkeypatch.setattr(
        "sys.argv",
        [
            "run_storyline_batch_benchmark.py",
            "--mode",
            "retrieval-probe",
            "--append-to-run-id",
            "unit-seed-baseline",
            "--retrieval-probe-max-ms",
            "750",
        ],
    )

    args = benchmark.parse_args()

    assert args.mode == "retrieval-probe"
    assert args.append_to_run_id == "unit-seed-baseline"
    assert args.retrieval_probe_max_ms == 750
    assert args.target_t1_batches == 0


def test_parse_args_rejects_retrieval_probe_without_existing_tenant(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        "sys.argv",
        ["run_storyline_batch_benchmark.py", "--mode", "retrieval-probe"],
    )

    with pytest.raises(SystemExit, match="retrieval-probe requires"):
        benchmark.parse_args()


def test_seed_only_outputs_write_append_ready_summary(tmp_path) -> None:
    scenario, _gold = build_storyline_scenario(
        run_id="unit-seed-baseline",
        signals_per_storyline=2,
        noise_signals=0,
    )
    tenant_id = uuid4()

    summary = asyncio.run(
        benchmark._write_seed_only_outputs(
            argparse.Namespace(cleanup=False),
            pool=_FakeSeedOnlyPool(),
            tenant_id=tenant_id,
            scenario=scenario,
            run_id="unit-seed-baseline",
            report_dir=tmp_path,
            run_config={"mode": "seed-only", "target_t1_batches": 0},
            seed_status={"models": 42, "requested_models": 42},
            started=0.0,
        )
    )

    run_summary = json.loads((tmp_path / "run_summary.json").read_text())
    storyline_scores = json.loads((tmp_path / "storyline_scores.json").read_text())

    assert summary["tenant_id"] == str(tenant_id)
    assert summary["append_ready"] is True
    assert summary["active_model_count"] == 42
    assert summary["processed_signal_count"] == 0
    assert run_summary["tenant_id"] == str(tenant_id)
    assert storyline_scores["tenant_id"] == str(tenant_id)
    assert "append-to-run-id unit-seed-baseline" in summary["append_example"]


def test_load_append_context_accepts_seed_only_report(tmp_path) -> None:
    base_dir = tmp_path / "seed-baseline"
    base_dir.mkdir()
    tenant_id = uuid4()
    (base_dir / "run_config.json").write_text(
        json.dumps({"mode": "seed-only", "target_t1_batches": 0})
    )
    (base_dir / "run_summary.json").write_text(
        json.dumps(
            {
                "tenant_id": str(tenant_id),
                "seed_status": {"models": 15000, "families": 120},
            }
        )
    )
    (base_dir / "storyline_scores.json").write_text(json.dumps({}))

    context = benchmark._load_append_context(
        argparse.Namespace(
            append_to_run_id="seed-baseline",
            append_tenant_id=None,
            report_root=tmp_path,
            horizon_start_batch=None,
            signals_per_storyline=20,
            target_t1_batches=5,
        )
    )

    assert context is not None
    assert context["tenant_id"] == str(tenant_id)
    assert context["horizon_start_batch"] == 0
    assert context["base_seed_status"] == {"models": 15000, "families": 120}
    assert context["additional_t1_batches"] == 5


def test_maybe_seed_storyline_models_skips_seed_for_append_context() -> None:
    seed_status = {"models": 0, "skipped": "append_to_existing_tenant"}

    result = asyncio.run(
        benchmark._maybe_seed_storyline_models(
            argparse.Namespace(seed_models=15000, seed_families=120),
            pool=object(),
            tenant_id=uuid4(),
            append_context={"tenant_id": str(uuid4())},
            seed_status=seed_status,
        )
    )

    assert result is seed_status


def test_analyze_post_seed_lookup_tables_checks_and_analyzes_existing_tables() -> None:
    class FakeConn:
        def __init__(self) -> None:
            self.checked: list[str] = []
            self.executed: list[str] = []

        async def fetchval(self, _query: str, table: str) -> str | None:
            self.checked.append(table)
            if table.endswith("model_operational_role_postings"):
                return None
            return table

        async def execute(self, query: str) -> None:
            self.executed.append(query)

    class FakeAcquire:
        def __init__(self, conn: FakeConn) -> None:
            self.conn = conn

        async def __aenter__(self) -> FakeConn:
            return self.conn

        async def __aexit__(self, *_args: object) -> None:
            return None

    class FakePool:
        def __init__(self) -> None:
            self.conn = FakeConn()

        def acquire(self) -> FakeAcquire:
            return FakeAcquire(self.conn)

    pool = FakePool()

    result = asyncio.run(benchmark._analyze_post_seed_lookup_tables(pool))  # type: ignore[arg-type]

    assert "models" in result["tables"]
    assert "model_operational_role_postings" not in result["tables"]
    assert "public.model_answerability_index" in pool.conn.checked
    assert "ANALYZE model_answerability_index" in pool.conn.executed
    assert "ANALYZE model_operational_role_postings" not in pool.conn.executed
    assert "model_answerability_index" in result["table_timings_ms"]
    assert "model_operational_role_postings" not in result["table_timings_ms"]


def test_retrieval_probe_discovers_seed_scope_and_dynamic_term_cases() -> None:
    entity_id = uuid4()

    class FakeConn:
        async def fetchval(self, _query: str, table: str) -> str | None:
            return table

        async def fetch(self, query: str, *_args: object) -> list[dict[str, object]]:
            if "FROM model_scope_entities" in query:
                return [
                    {
                        "entity_type": "customer_resource",
                        "entity_id": entity_id,
                        "model_count": 20,
                    }
                ]
            if "FROM model_sparse_terms" in query:
                return [
                    {"term": "customer_resource", "df": 15000},
                    {"term": "execution", "df": 14000},
                ]
            if "FROM model_answerability_index" in query:
                return [
                    {"term": "goal_impact", "df": 15000},
                    {"term": "predicate", "df": 14000},
                ]
            return []

    raw_entities, seed_pairs = asyncio.run(
        benchmark._retrieval_probe_seed_pairs(
            FakeConn(),  # type: ignore[arg-type]
            tenant_id=uuid4(),
        )
    )
    cases = asyncio.run(
        benchmark._retrieval_probe_term_cases(
            FakeConn(),  # type: ignore[arg-type]
            tenant_id=uuid4(),
        )
    )

    assert raw_entities == [{"type": "customer_resource", "id": str(entity_id)}]
    assert {kind for kind, _raw_id in seed_pairs} == {
        "customer",
        "customer_resource",
        "resource",
    }
    case_names = {case["name"] for case in cases}
    assert "background_noise" in case_names
    assert "top_sparse_terms" in case_names
    assert "top_answerability_terms" in case_names


def test_retrieval_probe_sidecar_preflight_passes_ready_core_surfaces() -> None:
    summary = asyncio.run(
        benchmark._retrieval_probe_sidecar_preflight(
            _ReadinessProbeConn(),  # type: ignore[arg-type]
            tenant_id=uuid4(),
        )
    )

    assert summary["status"] == "passed"
    assert summary["active_model_count"] == 100
    assert summary["tables"]["model_sparse_terms"]["active_model_hit_count"] == 98
    assert summary["tables"]["model_answerability_index"]["probe_primitive_count"] == 3
    assert summary["failures"] == []


def test_retrieval_probe_sidecar_preflight_fails_low_sparse_coverage() -> None:
    summary = asyncio.run(
        benchmark._retrieval_probe_sidecar_preflight(
            _ReadinessProbeConn(
                sparse_posted_models=40,
                sparse_rows=200,
                sparse_terms=3,
            ),  # type: ignore[arg-type]
            tenant_id=uuid4(),
        )
    )

    assert summary["status"] == "failed"
    assert any("model_sparse_terms" in item for item in summary["failures"])
    assert any("coverage too low" in item for item in summary["failures"])
    assert any("term variety too low" in item for item in summary["failures"])


def test_render_retrieval_probe_markdown_marks_failures() -> None:
    rendered = benchmark._render_retrieval_probe_markdown(
        {
            "tenant_id": str(uuid4()),
            "status": "failed",
            "max_ms": 1000.0,
            "seed_pair_count": 3,
            "results": [
                {
                    "label": "focused_answerability/background_noise",
                    "elapsed_ms": 1501.25,
                    "row_count": 0,
                    "min_rows": 1,
                    "coverage_passed": False,
                    "source_count": 1,
                    "source_set": ["answerability_index"],
                    "min_sources": 2,
                    "source_passed": False,
                    "passed": False,
                },
                {
                    "label": "focused_scope_sparse/background_noise",
                    "elapsed_ms": 4.25,
                    "row_count": 1,
                    "min_rows": 0,
                    "max_rows": 0,
                    "coverage_passed": True,
                    "excess_passed": False,
                    "timed_out": True,
                    "passed": False,
                }
            ],
            "failures": [
                {
                    "label": "focused_answerability/background_noise",
                    "elapsed_ms": 1501.25,
                    "row_count": 0,
                    "min_rows": 1,
                    "coverage_passed": False,
                    "source_count": 1,
                    "source_set": ["answerability_index"],
                    "min_sources": 2,
                    "source_passed": False,
                },
                {
                    "label": "focused_scope_sparse/background_noise",
                    "elapsed_ms": 4.25,
                    "row_count": 1,
                    "min_rows": 0,
                    "max_rows": 0,
                    "coverage_passed": True,
                    "excess_passed": False,
                    "timed_out": True,
                }
            ],
            "bounded_lookup_timeout_count": 1,
            "bounded_lookup_timeout_paths": [
                "focused_scope_sparse/background_noise"
            ],
            "coverage_failures": ["no model_scope_entities seed pairs found"],
            "sidecar_readiness": {
                "status": "failed",
                "active_model_count": 100,
                "tables": {
                    "model_sparse_terms": {
                        "required": True,
                        "active_row_count": 200,
                        "active_model_hit_count": 40,
                        "active_model_ratio": 0.4,
                    }
                },
                "failures": [
                    "required retrieval sidecar coverage too low: "
                    "model_sparse_terms 40.0% < 98.0%"
                ],
                "warnings": [],
            },
        }
    )

    assert "Status: `failed`" in rendered
    assert "focused_answerability/background_noise" in rendered
    assert "1501.250ms" in rendered
    assert "0 / min 1" in rendered
    assert "1 / max 0" in rendered
    assert "1 / min 2 (answerability_index)" in rendered
    assert "timeout" in rendered
    assert "Bounded Lookup Timeouts" in rendered
    assert "focused_scope_sparse/background_noise" in rendered
    assert "below min 1" in rendered
    assert "rows 1 above max 0" in rendered
    assert "bounded lookup timed out" in rendered
    assert "sources 1 below min 2" in rendered
    assert "Sidecar Readiness" in rendered
    assert "model_sparse_terms" in rendered
    assert "40 (40.0%)" in rendered
    assert "Readiness Failures" in rendered
    assert "Coverage Failures" in rendered
    assert "no model_scope_entities seed pairs found" in rendered


def test_focused_action_probe_requires_multiple_sources() -> None:
    model_id = uuid4()

    async def action_call() -> SimpleNamespace:
        return SimpleNamespace(
            models=[SimpleNamespace(id=model_id)],
            notes={
                "answerability_hits": 1,
                "scoped_sparse_hits": 0,
                "direct_scope_hits": 0,
                "merged_hits": 1,
                "returned_models": 1,
                "top_hits": [
                    {
                        "model_id": str(model_id),
                        "sources": ["answerability_index"],
                    }
                ],
            },
        )

    result = asyncio.run(
        benchmark._time_focused_action_probe_call(
            label="focused_action/common_generic",
            max_ms=1000,
            min_rows=1,
            min_sources=2,
            call=action_call(),
        )
    )

    assert result["latency_passed"] is True
    assert result["coverage_passed"] is True
    assert result["source_passed"] is False
    assert result["passed"] is False
    assert result["row_count"] == 1
    assert result["source_count"] == 1
    assert result["source_set"] == ["answerability_index"]
    assert result["notes"]["source_set"] == ["answerability_index"]


def test_rollback_focused_action_probe_rolls_back_transaction(monkeypatch) -> None:
    from services.platform.execution import retrieval_actions

    class FakeTransaction:
        def __init__(self) -> None:
            self.started = False
            self.rolled_back = False

        async def start(self) -> None:
            self.started = True

        async def rollback(self) -> None:
            self.rolled_back = True

    class FakeConn:
        def __init__(self) -> None:
            self.tx = FakeTransaction()

        def transaction(self) -> FakeTransaction:
            return self.tx

    calls: list[dict[str, object]] = []

    async def fake_execute_focused_index_action(
        action,
        trigger,
        conn,
        _cfg,
        *,
        model_limit: int,
    ) -> SimpleNamespace:
        calls.append(
            {
                "action": action,
                "trigger": trigger,
                "conn": conn,
                "model_limit": model_limit,
            }
        )
        return SimpleNamespace(models=[], notes={})

    monkeypatch.setattr(
        retrieval_actions,
        "execute_focused_index_action",
        fake_execute_focused_index_action,
    )

    tenant_id = uuid4()
    seed_id = uuid4()
    conn = FakeConn()
    result = asyncio.run(
        benchmark._rollback_focused_action_probe(
            conn,  # type: ignore[arg-type]
            tenant_id=tenant_id,
            raw_seed_entities=[{"type": "customer", "id": str(seed_id)}],
            terms=["capacity", "billing"],
            model_limit=5,
        )
    )

    assert result.models == []
    assert conn.tx.started is True
    assert conn.tx.rolled_back is True
    assert len(calls) == 1
    action = calls[0]["action"]
    trigger = calls[0]["trigger"]
    assert action.filters["terms"] == ["capacity", "billing"]
    assert action.filters["seed_entities"] == [
        {"type": "customer", "id": str(seed_id)}
    ]
    assert trigger.tenant_id == tenant_id
    assert calls[0]["conn"] is conn
    assert calls[0]["model_limit"] == 5


def test_retrieval_probe_marks_fast_empty_positive_case_as_failure() -> None:
    async def fast_empty_call() -> list[object]:
        return []

    result = asyncio.run(
        benchmark._time_retrieval_probe_call(
            label="focused_answerability/common_generic",
            max_ms=1000,
            min_rows=1,
            call=fast_empty_call(),
        )
    )

    assert result["latency_passed"] is True
    assert result["coverage_passed"] is False
    assert result["passed"] is False
    assert result["row_count"] == 0
    assert result["min_rows"] == 1


def test_retrieval_probe_marks_bounded_lookup_timeout_as_failure() -> None:
    async def timed_out_call() -> list[object]:
        return _TimedOutRows()

    result = asyncio.run(
        benchmark._time_retrieval_probe_call(
            label="focused_scope_sparse/common_generic",
            max_ms=1000,
            min_rows=0,
            call=timed_out_call(),
        )
    )

    assert result["timed_out"] is True
    assert result["timeout_passed"] is False
    assert result["passed"] is False
    assert result["row_count"] == 0


def test_retrieval_probe_marks_noisy_scoped_rows_as_failure() -> None:
    async def fast_noise_call() -> list[object]:
        return [object()]

    result = asyncio.run(
        benchmark._time_retrieval_probe_call(
            label="focused_scope_sparse/background_noise",
            max_ms=1000,
            min_rows=0,
            max_rows=0,
            call=fast_noise_call(),
        )
    )

    assert result["latency_passed"] is True
    assert result["coverage_passed"] is True
    assert result["excess_passed"] is False
    assert result["passed"] is False
    assert result["row_count"] == 1
    assert result["max_rows"] == 0


def test_retrieval_probe_summary_fails_when_scope_missing() -> None:
    summary = asyncio.run(
        benchmark._run_retrieval_hot_path_probe(
            _EmptyProbeConn(),  # type: ignore[arg-type]
            tenant_id=uuid4(),
            max_ms=1000,
            model_limit=8,
            require_scope=True,
        )
    )

    assert summary["status"] == "failed"
    assert summary["failures"]
    assert summary["coverage_failures"]


def test_storyline_scenario_builds_expected_batch_waves() -> None:
    scenario, gold = build_storyline_scenario(
        run_id="unit-storyline-benchmark",
        signals_per_storyline=20,
        noise_signals=5,
        future_validation_signals_per_storyline=3,
    )

    assert len(gold) == len(STORYLINES)
    assert len(scenario.signal_sequences) == len(STORYLINES) + 2
    assert sum(len(v) for v in scenario.signal_sequences.values()) == (
        len(STORYLINES) * 20 + len(STORYLINES) * 3 + 5
    )
    assert len(scenario.signal_sequences["future_validation"]) == (len(STORYLINES) * 3)

    for story in STORYLINES:
        wave = scenario.signal_sequences[f"{story.id}_wave"]
        assert len(wave) == 20
        assert {
            _story_id_from_external_id(signal.get("external_id")) for signal in wave
        } == {story.id}
        assert all(
            "storyline_id" not in (signal.get("content_dict") or {}) for signal in wave
        )
        assert all(
            "storyline_title" not in (signal.get("content_dict") or {})
            for signal in wave
        )


def test_storyline_scenario_builds_long_horizon_400_t1_batches() -> None:
    scenario, gold = build_storyline_scenario(
        run_id="unit-storyline-long-horizon",
        signals_per_storyline=25,
        noise_signals=0,
        future_validation_signals_per_storyline=3,
        target_t1_batches=400,
    )

    assert len(gold) == len(STORYLINES)
    assert len(scenario.signal_sequences) == 400
    assert sum(len(v) for v in scenario.signal_sequences.values()) == 10000
    assert {len(v) for v in scenario.signal_sequences.values()} == {25}
    assert any(
        name.startswith("future_validation_wave_") for name in scenario.signal_sequences
    )
    assert any(
        name.startswith("capability_probe_wave_")
        for name in scenario.signal_sequences
    )
    assert any(
        name.startswith("background_noise_wave_") for name in scenario.signal_sequences
    )
    assert (scenario.raw or {})["scenario_mode"] == "long_horizon"
    assert (scenario.raw or {})["target_t1_batches"] == 400


def test_storyline_scenario_builds_long_horizon_10_t1_batches_with_validation() -> None:
    scenario, gold = build_storyline_scenario(
        run_id="unit-storyline-long-horizon-10",
        signals_per_storyline=5,
        noise_signals=0,
        future_validation_signals_per_storyline=3,
        target_t1_batches=10,
    )

    assert len(gold) == len(STORYLINES)
    assert len(scenario.signal_sequences) == 10
    assert sum(len(v) for v in scenario.signal_sequences.values()) == 50
    assert any(
        name.startswith("future_validation_wave_") for name in scenario.signal_sequences
    )
    probe_waves = {
        name: signals
        for name, signals in scenario.signal_sequences.items()
        if name.startswith("capability_probe_wave_")
    }
    assert probe_waves
    probe_kinds = {
        kind
        for signals in probe_waves.values()
        for signal in signals
        for kind in signal["content_dict"]["capability_probe_kinds"]
    }
    assert probe_kinds == {
        "prediction",
        "resource",
        "ontology_gap",
        "archive",
        "evidence_attachment",
        "question_policy",
    }
    assert (scenario.raw or {})["scenario_mode"] == "long_horizon"
    assert (scenario.raw or {})["warmup_batches"] < 10


def test_lifecycle_obligation_measurement_helpers_explain_conversion() -> None:
    opportunity_counts = benchmark._lifecycle_opportunity_counts_from_texts(
        [
            "Forecast says launch will slip by Friday unless approval clears.",
            "Compliance capacity is down to two hours and owner is unclear.",
            "Yesterday's review felt rough around the launch narrative.",
            "The old launch-readiness memory is stale and may be replaced.",
            "Alias ambiguity: Acme and Acme Enterprise may not be the same customer.",
            "Capability probe. capability_probe=true prediction lifecycle.",
        ]
    )
    injected = benchmark._lifecycle_injected_kinds_from_trace(
        "reasoning\nlifecycle_obligations: injected prediction, resource, "
        "question_policy\nmore"
    )
    injected_counts = {"prediction": 1, "resource": 1, "question_policy": 1}
    persisted_counts = {"prediction": 1, "resource": 0, "question_policy": 1}

    assert opportunity_counts["prediction"] == 1
    assert opportunity_counts["resource"] == 1
    assert opportunity_counts["question_policy"] == 1
    assert opportunity_counts["evidence_attachment"] == 1
    assert opportunity_counts["staleness_review"] == 1
    assert opportunity_counts["ambiguity_review"] == 1
    assert injected == ["prediction", "resource", "question_policy"]
    assert benchmark._lifecycle_conversion_rates(
        numerator=persisted_counts,
        denominator=injected_counts,
    )["resource"] == 0.0
    assert any(
        "resource operations" in note and "none persisted" in note
        for note in benchmark._lifecycle_bottleneck_notes(
            opportunities=opportunity_counts,
            injected=injected_counts,
            persisted=persisted_counts,
        )
    )


def test_storyline_scenario_builds_append_horizon_without_reused_signal_ids() -> None:
    base, _gold = build_storyline_scenario(
        run_id="unit-storyline-long-horizon",
        signals_per_storyline=25,
        noise_signals=0,
        future_validation_signals_per_storyline=3,
        target_t1_batches=400,
    )
    append, _gold = build_storyline_scenario(
        run_id="unit-storyline-long-horizon-plus-200",
        foundation_namespace="unit-storyline-long-horizon",
        signals_per_storyline=25,
        noise_signals=0,
        future_validation_signals_per_storyline=3,
        target_t1_batches=200,
        horizon_start_batch=400,
    )

    assert len(append.signal_sequences) == 200
    assert sum(len(v) for v in append.signal_sequences.values()) == 5000
    assert next(iter(append.signal_sequences)).endswith("_wave_401")
    assert (append.raw or {})["horizon_start_batch"] == 400
    assert (append.raw or {})["horizon_end_batch"] == 600
    assert (append.raw or {})["foundation_namespace"] == ("unit-storyline-long-horizon")

    base_external_ids = {
        signal["external_id"]
        for signals in base.signal_sequences.values()
        for signal in signals
    }
    append_external_ids = {
        signal["external_id"]
        for signals in append.signal_sequences.values()
        for signal in signals
    }
    assert not (base_external_ids & append_external_ids)

    first_signal = next(iter(append.signal_sequences.values()))[0]
    assert first_signal["content_dict"]["signal_index"] == 10000
    assert first_signal["content_dict"]["horizon_wave_index"] == 401


def test_latent_bridge_storyline_has_sensor_gap_without_initial_hallway_leak() -> None:
    scenario, _gold = build_storyline_scenario(
        run_id="unit-storyline-benchmark",
        signals_per_storyline=20,
        noise_signals=0,
        future_validation_signals_per_storyline=3,
    )

    bridge_wave = scenario.signal_sequences[f"{_LATENT_BRIDGE_STORYLINE_ID}_wave"]
    bridge_text = "\n".join(signal["content"].lower() for signal in bridge_wave)
    future_bridge_text = "\n".join(
        signal["content"].lower()
        for signal in scenario.signal_sequences["future_validation"]
        if _story_id_from_external_id(signal.get("external_id"))
        == _LATENT_BRIDGE_STORYLINE_ID
    )

    assert "sensor trail has a gap" in bridge_text
    assert "before and after states" in bridge_text
    assert "hallway" not in bridge_text
    assert "hallway" in future_bridge_text


def test_future_wave_trigger_ids_mark_future_edge_evolution() -> None:
    waves = [
        {
            "sequence": "atlas_wave_001",
            "t1_batch": {"trigger_id": "ordinary-trigger"},
        },
        {
            "sequence": "future_validation_wave_005",
            "t1_batch": {"trigger_id": "future-trigger"},
            "downstream": [{"trigger_id": "future-downstream"}],
        },
    ]
    future_trigger_ids = _future_wave_trigger_ids(waves)
    assert future_trigger_ids == {"future-trigger", "future-downstream"}

    stats = _empty_edge_ops_stats()
    _accumulate_edge_ops_stats(
        stats,
        {
            "edge_ops": [
                {
                    "op": "add",
                    "edge_kind": "weakens",
                    "review_status": "accepted",
                }
            ],
            "relation_frame_ops": [
                {
                    "status": "accepted",
                    "write_policy": "project_edges",
                    "relation_kind": "blocked_workstream",
                    "projected_edge_count": 3,
                }
            ],
        },
        is_future="future-trigger" in future_trigger_ids,
    )

    assert stats["future_edge_ops"] == 1
    assert stats["future_relation_frame_ops"] == 1


def test_storyline_signal_metadata_does_not_persist_gold_answers() -> None:
    scenario, _gold = build_storyline_scenario(
        run_id="unit-storyline-benchmark",
        signals_per_storyline=20,
        noise_signals=0,
    )

    forbidden_metadata_keys = {
        "expected_term",
        "expected_action",
        "expected_relationship",
        "storyline_id",
        "storyline_title",
    }
    forbidden_text = (
        "Important term:",
        "Likely operating implication:",
        "Potential relationship shape:",
    )
    for signals in scenario.signal_sequences.values():
        for signal in signals:
            content = signal.get("content_dict") or {}
            assert content["benchmark"] == "storyline_batch"
            assert not (forbidden_metadata_keys & set(content))
        assert all(marker not in signal["content"] for marker in forbidden_text)


def test_story_id_from_external_id_accepts_run_prefixed_ids() -> None:
    assert _story_id_from_external_id("storyline:atlas:001") == "atlas"
    assert _story_id_from_external_id("capability-400:storyline:atlas:001") == "atlas"
    assert (
        _story_id_from_external_id(
            "capability-400-plus-200:storyline:northstar_gap:future:004"
        )
        == "northstar_gap"
    )


def test_t1_batch_retryability_classifies_transient_provider_failures() -> None:
    assert benchmark._is_retryable_t1_batch_run(
        {"status": "failed", "error": "ConnectTimeout: "}
    )
    assert benchmark._is_retryable_t1_batch_run(
        {"status": "failed", "error": "HTTP 503 service unavailable"}
    )
    assert not benchmark._is_retryable_t1_batch_run(
        {"status": "failed", "error": "ValidationError: out of region"}
    )
    assert not benchmark._is_retryable_t1_batch_run({"status": "success"})


def test_downstream_drain_includes_all_t2_t3_t4_root_triggers() -> None:
    class FakeConn:
        def __init__(self) -> None:
            self.fetchval_queries: list[str] = []
            self.execute_queries: list[str] = []
            self.pending_calls = 0

        async def fetchval(self, query: str, *_args: object) -> int:
            self.fetchval_queries.append(query)
            self.pending_calls += 1
            return 1 if self.pending_calls == 1 else 0

        async def execute(self, query: str, *_args: object) -> None:
            self.execute_queries.append(query)

        async def fetch(self, *_args: object) -> list[dict[str, object]]:
            return []

    class FakeAcquire:
        def __init__(self, conn: FakeConn) -> None:
            self.conn = conn

        async def __aenter__(self) -> FakeConn:
            return self.conn

        async def __aexit__(self, *_args: object) -> None:
            return None

    class FakePool:
        def __init__(self) -> None:
            self.conn = FakeConn()

        def acquire(self) -> FakeAcquire:
            return FakeAcquire(self.conn)

    class FakeWorker:
        def __init__(self) -> None:
            self._in_flight: set[object] = set()
            self.polls = 0

        async def _poll_and_dispatch(self) -> None:
            self.polls += 1

    pool = FakePool()
    worker = FakeWorker()

    steps = asyncio.run(
        benchmark._drain_downstream_limited(
            pool,  # type: ignore[arg-type]
            worker,  # type: ignore[arg-type]
            tenant_id=uuid4(),
            steps=2,
            force_window_elapsed_s=1.0,
        )
    )

    assert len(steps) == 1
    assert worker.polls == 1
    assert pool.conn.execute_queries
    selector_sql = "\n".join(pool.conn.execute_queries)
    assert "trigger_kind IN ('T2', 'T3', 'T4')" in selector_sql
    assert "open_question_search" not in selector_sql
    assert "latent_relationship_candidate" not in selector_sql


def test_think_cost_profile_classifies_t4_family_as_background(
    monkeypatch,
) -> None:
    class FakeConn:
        async def fetch(self, *_args: object) -> list[dict[str, object]]:
            return [
                {
                    "trigger_kind": "T1:event_batch",
                    "trigger_subkind": "event_batch",
                    "runs": 10,
                    "llm_calls": 10,
                    "cost_usd": 1.0,
                },
                {
                    "trigger_kind": "T2:belief_updated",
                    "trigger_subkind": "belief_updated",
                    "runs": 6,
                    "llm_calls": 6,
                    "cost_usd": 0.6,
                },
                {
                    "trigger_kind": "T3:missing_transition",
                    "trigger_subkind": "missing_transition",
                    "runs": 8,
                    "llm_calls": 5,
                    "cost_usd": 0.5,
                },
                {
                    "trigger_kind": "T4:open_question_search",
                    "trigger_subkind": "open_question_search",
                    "runs": 4,
                    "llm_calls": 4,
                    "cost_usd": 0.4,
                },
                {
                    "trigger_kind": "T4:latent_relationship_candidate",
                    "trigger_subkind": "latent_relationship_candidate",
                    "runs": 6,
                    "llm_calls": 6,
                    "cost_usd": 0.6,
                },
                {
                    "trigger_kind": "T4:representation_repair",
                    "trigger_subkind": "representation_repair",
                    "runs": 1,
                    "llm_calls": 1,
                    "cost_usd": 0.1,
                },
            ]

    class FakeAcquire:
        async def __aenter__(self) -> FakeConn:
            return FakeConn()

        async def __aexit__(self, *_args: object) -> None:
            return None

    class FakePool:
        def acquire(self) -> FakeAcquire:
            return FakeAcquire()

    async def fake_table_exists(_conn: object, table: str) -> bool:
        return table == "think_run_costs"

    monkeypatch.setattr(benchmark, "_table_exists", fake_table_exists)

    profile = asyncio.run(
        benchmark._collect_think_cost_profile(FakePool(), tenant_id=uuid4())
    )

    assert profile["product_path"] == {
        "runs": 24,
        "llm_calls": 21,
        "cost_usd": 2.1,
    }
    assert profile["background_maintenance"] == {
        "runs": 11,
        "llm_calls": 11,
        "cost_usd": 1.1,
    }
    assert (
        profile["by_kind"]["T4:open_question_search:open_question_search"][
            "trigger_family"
        ]
        == "T4"
    )


def test_benchmark_summary_fails_when_required_t1_batch_stays_failed() -> None:
    failed_wave = json.loads(json.dumps(_sample_success_wave()))
    failed_wave["sequence"] = "cobalt_security_packet_horizon_wave_003"
    failed_wave["t1_batch"]["trigger_id"] = "trigger-cobalt"
    failed_wave["t1_batch"]["run"] = {
        "status": "failed",
        "error": "ConnectTimeout: ",
        "validation_error_count": None,
        "retrieval_model_count": None,
        "retrieval_observation_count": None,
    }
    model_summary = _sample_model_summary()
    model_summary["think_runs_success"] = 0
    model_summary["think_runs_failed"] = 1

    summary = _benchmark_summary(
        model_summary=model_summary,
        storyline_scores=[_sample_storyline_score()],
        waves=[failed_wave],
        elapsed_seconds=45.0,
    )

    assert summary["status"] == "failed"
    assert summary["required_run_failures"] == [
        "required T1 batch failed: cobalt_security_packet_horizon_wave_003 "
        "trigger=trigger-cobalt error=ConnectTimeout: "
    ]
    rendered = _render_benchmark_markdown(summary)
    assert "Status: `failed`" in rendered
    assert "Required Run Failures" in rendered


def test_benchmark_summary_passes_when_t1_batch_recovers_after_retry() -> None:
    recovered_wave = json.loads(json.dumps(_sample_success_wave()))
    recovered_wave["sequence"] = "cobalt_security_packet_horizon_wave_003"
    recovered_wave["t1_batch"]["trigger_id"] = "trigger-cobalt"
    recovered_wave["t1_batch"]["retry_count"] = 1
    recovered_wave["t1_batch"]["attempt_history"] = [
        {
            "attempt": 1,
            "status": "failed",
            "error": "ConnectTimeout: ",
            "retryable": True,
        },
        {"attempt": 2, "status": "success", "error": None, "retryable": False},
    ]
    recovered_wave["t1_batch"]["run"]["retry_count"] = 1
    recovered_wave["t1_batch"]["run"]["recovered_after_retry"] = True
    model_summary = _sample_model_summary()
    model_summary["think_runs_success"] = 1
    model_summary["think_runs_failed"] = 1

    summary = _benchmark_summary(
        model_summary=model_summary,
        storyline_scores=[_sample_storyline_score()],
        waves=[recovered_wave],
        elapsed_seconds=90.0,
    )

    assert summary["status"] == "passed"
    assert summary["required_run_failures"] == []
    assert summary["t1_retry"]["recovered_t1_batches"] == 1
    assert summary["t1_retry"]["retry_attempts"] == 1


def test_benchmark_summary_fails_below_min_efficiency_score() -> None:
    model_summary = _sample_model_summary()
    model_summary["run_config"] = {"min_efficiency_score": 1.0}

    summary = _benchmark_summary(
        model_summary=model_summary,
        storyline_scores=[_sample_storyline_score()],
        waves=[_sample_success_wave()],
        elapsed_seconds=30.0,
    )

    assert summary["status"] == "failed"
    assert any(
        item.startswith("efficiency score below required floor:")
        for item in summary["required_run_failures"]
    )


def test_benchmark_efficiency_uses_product_path_cost_profile() -> None:
    model_summary = _sample_model_summary()
    model_summary["signal_count"] = 200
    model_summary["think_runs_success"] = 31
    model_summary["cost"] = {"llm_calls": 20, "cost_usd": 2.207248}
    model_summary["think_cost_profile"] = {
        "available": True,
        "efficiency_scope": "product_path_excludes_t4_background_maintenance",
        "product_path": {
            "runs": 24,
            "llm_calls": 13,
            "cost_usd": 1.626488,
        },
        "background_maintenance": {
            "runs": 7,
            "llm_calls": 7,
            "cost_usd": 0.58076,
        },
    }
    model_summary["run_config"] = {"min_efficiency_score": 0.5}

    summary = _benchmark_summary(
        model_summary=model_summary,
        storyline_scores=[_sample_storyline_score()],
        waves=[_sample_success_wave()],
        elapsed_seconds=30.0,
    )

    efficiency = summary["company_intelligence_scorecard"]["dimensions"]["efficiency"]
    metrics = efficiency["metrics"]
    assert summary["status"] == "passed"
    assert summary["required_run_failures"] == []
    assert efficiency["score"] >= 0.5
    assert metrics["think_runs_per_signal"] == pytest.approx(24 / 200)
    assert metrics["llm_calls_per_signal"] == pytest.approx(13 / 200)
    assert metrics["cost_per_signal_usd"] == pytest.approx(0.0081)
    assert metrics["background_maintenance_think_runs"] == 7
    assert metrics["background_maintenance_llm_calls"] == 7
    assert metrics["background_maintenance_cost_usd"] == pytest.approx(0.5808)


def test_main_run_mode_returns_nonzero_for_failed_required_run(
    monkeypatch,
) -> None:
    async def fake_run_benchmark(_args):
        return {"status": "failed", "required_run_failures": ["required wave failed"]}

    monkeypatch.setattr(benchmark, "run_benchmark", fake_run_benchmark)
    monkeypatch.setattr(
        "sys.argv",
        [
            "run_storyline_batch_benchmark.py",
            "--mode",
            "run",
            "--target-t1-batches",
            "1",
        ],
    )

    assert asyncio.run(benchmark.main()) == 1


def test_main_run_mode_allows_degraded_exit_zero_when_explicit(
    monkeypatch,
) -> None:
    async def fake_run_benchmark(_args):
        return {"status": "failed", "required_run_failures": ["required wave failed"]}

    monkeypatch.setattr(benchmark, "run_benchmark", fake_run_benchmark)
    monkeypatch.setattr(
        "sys.argv",
        [
            "run_storyline_batch_benchmark.py",
            "--mode",
            "run",
            "--target-t1-batches",
            "1",
            "--allow-degraded-exit-zero",
        ],
    )

    assert asyncio.run(benchmark.main()) == 0


def test_storyline_signals_do_not_leak_hidden_thesis() -> None:
    scenario, _gold = build_storyline_scenario(
        run_id="unit-storyline-benchmark",
        signals_per_storyline=20,
        noise_signals=0,
    )

    thesis_by_story = {story.id: story.thesis for story in STORYLINES}
    for signals in scenario.signal_sequences.values():
        for signal in signals:
            story_id = _story_id_from_external_id(signal.get("external_id"))
            thesis = thesis_by_story.get(story_id)
            if thesis:
                assert thesis not in signal["content"]


def test_latent_pattern_assessment_scores_concrete_model_coverage() -> None:
    story = STORYLINES[0]
    model = {
        "natural": (
            "Atlas renewal risk is driven by missing security evidence, "
            "usage drop, and procurement waiting on approval."
        ),
        "proposition": {
            "claim_role": "situation",
            "summary": "Security evidence, usage decay, and procurement wait combine.",
        },
    }

    assessment = _latent_pattern_assessment(model, story)

    assert assessment["coverage"] == 1.0
    assert assessment["missing"] == []


def test_noise_noop_score_credits_explicit_empty_diff_without_noop_grade() -> None:
    assert (
        _noise_noop_score(
            [
                {
                    "sequence": "background_noise_wave_010",
                    "t1_batch": {
                        "run": {
                            "validation_error_count": 0,
                            "ops_applied": {
                                "claim_ops": [],
                                "relation_claim_ops": [],
                                "relation_frame_ops": [],
                                "edge_ops": [],
                                "act_ops": [],
                                "resource_ops": [],
                                "ontology_gap_ops": [],
                                "state_changes_emitted": 0,
                                "context_use": {
                                    "context_use_grade": "unused_selected_context"
                                },
                                "reasoning_trace": (
                                    "Empty diff; "
                                    "relation_lifecycle_kernel="
                                    "packet_obligations_skipped:explicit_noop"
                                ),
                                "synthesis_decisions": [
                                    {
                                        "bucket": "diff",
                                        "decision": "discard_as_noise",
                                    }
                                ],
                            },
                        }
                    },
                }
            ]
        )
        == 1.0
    )


def test_noise_noop_score_rejects_zero_state_change_relation_writes() -> None:
    assert (
        _noise_noop_score(
            [
                {
                    "sequence": "background_noise_wave_010",
                    "t1_batch": {
                        "run": {
                            "validation_error_count": 0,
                            "ops_applied": {
                                "claim_ops": [],
                                "relation_claim_ops": [
                                    {"edge_kind": "weakens", "op": "upsert"}
                                ],
                                "relation_frame_ops": [],
                                "edge_ops": [],
                                "act_ops": [],
                                "resource_ops": [],
                                "ontology_gap_ops": [],
                                "state_changes_emitted": 0,
                                "reasoning_trace": "No durable diff emitted.",
                            },
                        }
                    },
                }
            ]
        )
        == 0.0
    )


def test_retrieval_context_budget_fit_score_rewards_efficient_band() -> None:
    assert _retrieval_context_budget_fit_score(0) == 0.0
    assert _retrieval_context_budget_fit_score(3) == 0.5
    assert _retrieval_context_budget_fit_score(8) == 1.0
    assert _retrieval_context_budget_fit_score(16) == 1.0
    assert _retrieval_context_budget_fit_score(22) == 0.5
    assert _retrieval_context_budget_fit_score(28) == 0.0


def test_company_intelligence_scorecard_penalizes_over_budget_context() -> None:
    scorecard = _company_intelligence_scorecard(
        model_summary=_sample_model_summary(),
        storyline_scores=[_sample_storyline_score()],
        waves=[_sample_success_wave()],
        retrieval_model_counts=[24],
        retrieval_observation_counts=[29],
        validation_errors=0,
    )

    retrieval = scorecard["dimensions"]["retrieval_usefulness"]

    assert retrieval["metrics"]["retrieval_budget_fit_score"] < 0.5
    assert scorecard["proof_coverage"]["retrieval_budget_fit_score"] < 0.5
    assert any(
        "Selected Model context is above the efficient batch budget" in gap
        for gap in scorecard["proof_gaps"]
    )


def test_company_intelligence_scorecard_reports_dimensions_and_gaps() -> None:
    scorecard = _company_intelligence_scorecard(
        model_summary=_sample_model_summary(),
        storyline_scores=[_sample_storyline_score()],
        waves=[_sample_success_wave()],
        retrieval_model_counts=[22],
        retrieval_observation_counts=[29],
        validation_errors=0,
    )

    assert 0.0 <= scorecard["overall_score"] <= 1.0
    assert {
        "memory_truth",
        "compression",
        "retrieval_usefulness",
        "reasoning_value",
        "temporal_improvement",
        "edge_intelligence",
        "adaptive_lifecycle",
        "robustness",
        "efficiency",
    } == set(scorecard["dimensions"])
    assert any("No future validation events" in gap for gap in scorecard["proof_gaps"])
    assert any(
        "Resource/action-resource operations are untested" in gap
        for gap in scorecard["proof_gaps"]
    )


def test_company_intelligence_scorecard_distinguishes_probed_from_untested() -> None:
    model_summary = _sample_model_summary()
    model_summary["capability_probe_counts"] = {
        "prediction": 1,
        "resource": 1,
        "ontology_gap": 1,
        "archive": 1,
        "evidence_attachment": 1,
        "question_policy": 1,
    }

    scorecard = _company_intelligence_scorecard(
        model_summary=model_summary,
        storyline_scores=[_sample_storyline_score()],
        waves=[_sample_success_wave()],
        retrieval_model_counts=[22],
        retrieval_observation_counts=[29],
        validation_errors=0,
    )

    gaps = "\n".join(scorecard["proof_gaps"])
    assert "Resource/action-resource probe ran" in gaps
    assert "Ontology-gap probe ran" in gaps
    assert "Archive probe ran" in gaps
    assert "Evidence probe ran" in gaps
    assert "Question-policy probe ran" in gaps
    assert "Resource/action-resource operations are untested" not in gaps
    assert "Ontology-gap write path is untested" not in gaps


def test_company_intelligence_scorecard_flags_topology_missing_model_skips() -> None:
    model_summary = _sample_model_summary()
    model_summary["topology_optimizer_metric_totals"] = {
        **model_summary["topology_optimizer_metric_totals"],
        "shortcut_missing_model_skips": 2,
        "structural_missing_model_skips": 1,
    }

    scorecard = _company_intelligence_scorecard(
        model_summary=model_summary,
        storyline_scores=[_sample_storyline_score()],
        waves=[_sample_success_wave()],
        retrieval_model_counts=[22],
        retrieval_observation_counts=[29],
        validation_errors=0,
    )

    assert (
        scorecard["dimensions"]["robustness"]["metrics"]["topology_missing_model_skips"]
        == 3.0
    )
    assert any(
        "Topology optimizer skipped missing model references" in gap
        for gap in scorecard["proof_gaps"]
    )


def test_company_intelligence_scorecard_scores_future_validation_evidence() -> None:
    model_summary = _sample_model_summary()
    model_summary["future_validation_events"] = 24

    scorecard = _company_intelligence_scorecard(
        model_summary=model_summary,
        storyline_scores=[_sample_storyline_score()],
        waves=[_sample_success_wave(), _sample_future_validation_wave()],
        retrieval_model_counts=[22, 24],
        retrieval_observation_counts=[29, 28],
        validation_errors=0,
    )

    temporal = scorecard["dimensions"]["temporal_improvement"]
    retrieval = scorecard["dimensions"]["retrieval_usefulness"]

    assert temporal["metrics"]["future_validation_events"] == 24
    assert temporal["metrics"]["future_validation_success_rate"] == 1.0
    assert (
        temporal["metrics"]["future_validation_model_or_graph_context_use_score"] == 1.0
    )
    assert temporal["score"] > 0.55
    assert retrieval["metrics"]["avg_historical_observations_per_t1_batch"] == 3.5
    assert not any(
        "No future validation events" in gap for gap in scorecard["proof_gaps"]
    )


def test_company_intelligence_scorecard_reports_adaptive_lifecycle() -> None:
    model_summary = _sample_model_summary()
    model_summary["future_validation_events"] = 24
    model_summary["discovery_layer_counts"] = {
        "negative_memory": 2,
        "question_policy_stats": 1,
    }
    model_summary["topology_optimizer_metric_totals"] = {
        **model_summary["topology_optimizer_metric_totals"],
        "shortcut_creates_or_bumps": 12,
        "affordance_reinforces": 10,
        "negative_memory_inserts": 1,
        "question_policy_updates": 1,
    }
    model_summary["post_commit_status"] = {
        "processed": 5,
        "failed": 0,
        "dead_lettered": 0,
        "iterations": 2,
    }

    scorecard = _company_intelligence_scorecard(
        model_summary=model_summary,
        storyline_scores=[_sample_storyline_score()],
        waves=[_sample_success_wave(), _sample_future_validation_wave()],
        retrieval_model_counts=[22, 24],
        retrieval_observation_counts=[29, 28],
        validation_errors=0,
    )

    adaptive = scorecard["dimensions"]["adaptive_lifecycle"]

    assert adaptive["score"] > 0.65
    assert adaptive["metrics"]["policy_feedback_score"] == 1.0
    assert adaptive["metrics"]["negative_learning_score"] == 1.0
    assert adaptive["metrics"]["temporal_closure_score"] > 0.0
    assert (
        scorecard["proof_coverage"]["adaptive_lifecycle"]
        == adaptive["metrics"]
    )
    assert not any(
        "Adaptive temporal closure is weak" in gap
        for gap in scorecard["proof_gaps"]
    )


def test_company_intelligence_scorecard_reports_edge_intelligence() -> None:
    model_summary = _sample_model_summary()
    model_summary["edge_kind_distribution"] = {
        "supports": 2,
        "early_warning_for": 1,
        "blocks": 1,
        "weakens": 1,
        "explains": 1,
        "contributes_to_resolution": 1,
    }
    model_summary["relationship_candidates"] = 2
    model_summary["relationship_candidate_status_distribution"] = {"accepted": 2}
    model_summary["edge_lifecycle"] = {
        "total_edges": 7,
        "accepted_edges": 7,
        "accepted_edge_kind_distribution": {
            "supports": 2,
            "early_warning_for": 1,
            "blocks": 1,
            "weakens": 1,
            "explains": 1,
            "contributes_to_resolution": 1,
        },
        "reconfirmed_edges": 1,
        "reconfirmation_events": 2,
        "retired_or_inert_edges": 1,
        "ontology_proposals": 0,
    }

    scorecard = _company_intelligence_scorecard(
        model_summary=model_summary,
        storyline_scores=[_sample_storyline_score()],
        waves=[_sample_success_wave(), _sample_future_validation_wave()],
        retrieval_model_counts=[22, 24],
        retrieval_observation_counts=[29, 28],
        validation_errors=0,
    )

    edge = scorecard["dimensions"]["edge_intelligence"]

    assert edge["metrics"]["required_registered_edge_kind_coverage"] == 1.0
    assert edge["metrics"]["precise_required_edge_kind_coverage"] == 1.0
    assert edge["metrics"]["future_validation_edge_ops"] == 1
    assert edge["metrics"]["reconfirmation_events"] == 2.0
    assert edge["metrics"]["graph_relation_contract_score"] == 1.0
    assert "missing_registered_edge_kinds" in scorecard["proof_coverage"]


def test_company_intelligence_scorecard_counts_relation_frames_as_structure() -> None:
    model_summary = _sample_model_summary()
    model_summary["edge_kind_distribution"] = {"supports": 1}
    model_summary["edge_lifecycle"] = {
        **model_summary["edge_lifecycle"],
        "accepted_edge_kind_distribution": {"supports": 1},
    }
    model_summary["relation_frame_lifecycle"] = {
        "available": True,
        "total_relation_frames": 1,
        "accepted_relation_frames": 1,
        "projectable_relation_frames": 1,
        "bound_relation_frames": 1,
        "relation_participants": 5,
        "relation_edge_projections": 3,
        "relation_frame_kind_distribution": {"blocked_workstream": 1},
        "relation_projection_kind_distribution": {
            "blocks": 1,
            "early_warning_for": 1,
            "contributes_to_resolution": 1,
        },
    }
    story = _sample_storyline_score()
    story.scoped_edge_count = 0
    story.relation_frame_count = 1
    story.accepted_relation_frame_count = 1
    story.relation_frame_projection_count = 3
    story.relation_frame_kind_hits = ["blocked_workstream"]

    scorecard = _company_intelligence_scorecard(
        model_summary=model_summary,
        storyline_scores=[story],
        waves=[_sample_success_wave_with_relation_frame()],
        retrieval_model_counts=[22],
        retrieval_observation_counts=[29],
        validation_errors=0,
    )

    edge = scorecard["dimensions"]["edge_intelligence"]
    proof = scorecard["proof_coverage"]

    assert edge["metrics"]["relation_frame_score"] > 0
    assert edge["metrics"]["storyline_edge_presence"] == 1.0
    assert edge["metrics"]["relation_frame_ops"] == 1
    assert edge["metrics"]["relation_frame_projected_edges_from_ops"] == 3
    assert proof["accepted_relation_frames"] == 1.0
    assert "blocks" in proof["structural_edge_kinds_observed"]
    assert not any(
        "N-ary relation frames were not exercised" in gap
        for gap in scorecard["proof_gaps"]
    )


def test_company_intelligence_scorecard_flags_graph_relation_contract_failure() -> None:
    model_summary = _sample_model_summary()
    model_summary["context_use_relation_contract"] = {
        "context_use_runs": 4,
        "graph_selected_runs": 4,
        "graph_relation_op_runs": 1,
        "graph_no_edge_rationale_runs": 1,
        "graph_selected_without_relation_ops_runs": 3,
        "graph_relation_contract_satisfied_runs": 2,
        "graph_relation_contract_failed_runs": 2,
    }

    scorecard = _company_intelligence_scorecard(
        model_summary=model_summary,
        storyline_scores=[_sample_storyline_score()],
        waves=[_sample_success_wave()],
        retrieval_model_counts=[22],
        retrieval_observation_counts=[29],
        validation_errors=0,
    )

    retrieval = scorecard["dimensions"]["retrieval_usefulness"]
    edge = scorecard["dimensions"]["edge_intelligence"]

    assert retrieval["metrics"]["graph_relation_contract_score"] == 0.5
    assert edge["metrics"]["graph_relation_contract_failed_runs"] == 2
    assert any(
        "Graph-selected context failed the relationship contract" in gap
        for gap in scorecard["proof_gaps"]
    )


def test_company_intelligence_scorecard_reports_product_value_evals() -> None:
    scorecard = _company_intelligence_scorecard(
        model_summary=_sample_model_summary(),
        storyline_scores=[_sample_storyline_score()],
        waves=[_sample_success_wave()],
        retrieval_model_counts=[22],
        retrieval_observation_counts=[29],
        validation_errors=0,
    )

    product_value = scorecard["product_value_evals"]

    assert 0.0 <= product_value["overall_score"] <= 1.0
    assert set(product_value["evals"]) == set(_PRODUCT_VALUE_EVAL_KEYS)
    assert scorecard["proof_coverage"]["product_value_eval_keys"] == list(
        _PRODUCT_VALUE_EVAL_KEYS
    )
    assert (
        product_value["evals"]["negative_learning"]["metrics"][
            "negative_learning_events"
        ]
        == 0
    )
    assert any("Negative learning eval" in gap for gap in product_value["proof_gaps"])
    assert any("Question policy eval" in gap for gap in product_value["proof_gaps"])
    assert any("Customer value eval" in gap for gap in product_value["proof_gaps"])
    assert any(
        "Latent bridge inference eval" in gap for gap in product_value["proof_gaps"]
    )


def test_product_value_gaps_distinguish_probed_from_untested() -> None:
    model_summary = _sample_model_summary()
    model_summary["capability_probe_counts"] = {
        "resource": 1,
        "archive": 1,
        "evidence_attachment": 1,
        "question_policy": 1,
    }

    scorecard = _company_intelligence_scorecard(
        model_summary=model_summary,
        storyline_scores=[_sample_storyline_score()],
        waves=[_sample_success_wave()],
        retrieval_model_counts=[22],
        retrieval_observation_counts=[29],
        validation_errors=0,
    )

    gaps = "\n".join(scorecard["product_value_evals"]["proof_gaps"])
    assert "resource/action-resource probe" in gaps
    assert "archive probe" in gaps
    assert "evidence probe" in gaps
    assert "Question policy eval ran a probe" in gaps
    assert "Question policy eval did not exercise" not in gaps


def test_company_intelligence_scorecard_scores_latent_bridge_inference() -> None:
    scorecard = _company_intelligence_scorecard(
        model_summary=_sample_model_summary(),
        storyline_scores=[
            _sample_storyline_score(),
            _sample_latent_bridge_storyline_score(),
        ],
        waves=[_sample_success_wave(), _sample_future_validation_wave()],
        retrieval_model_counts=[22, 24],
        retrieval_observation_counts=[29, 28],
        validation_errors=0,
    )

    bridge = scorecard["product_value_evals"]["evals"]["latent_bridge_inference"]

    assert bridge["metrics"]["inferred_bridge_model_count"] == 1
    assert bridge["metrics"]["transition_supported_bridge_model_count"] == 1
    assert bridge["metrics"]["future_confirmed_bridge_model_count"] == 1
    assert bridge["metrics"]["unsupported_specific_claim_count"] == 0
    assert bridge["score"] > 0.8
    assert not any(
        "Latent bridge inference eval did not create" in gap
        for gap in scorecard["product_value_evals"]["proof_gaps"]
    )


def test_benchmark_summary_renders_company_intelligence_scorecard() -> None:
    summary = _benchmark_summary(
        model_summary=_sample_model_summary(),
        storyline_scores=[_sample_storyline_score_with_calibration()],
        waves=[_sample_success_wave()],
        elapsed_seconds=12.0,
    )
    markdown = _render_benchmark_markdown(summary)

    assert "company_intelligence_scorecard" in summary
    assert summary["calibration"]["n"] == 2
    assert summary["calibration"]["expected_calibration_error"] is not None
    assert "## Company Intelligence Scorecard" in markdown
    assert "## Calibration" in markdown
    assert "### Product Value Evals" in markdown
    assert "### Proof Gaps" in markdown


def test_storyline_calibration_report_bins_future_validation_samples() -> None:
    report = _storyline_calibration_report(
        [
            _sample_storyline_score_with_calibration(),
        ]
    )

    assert report["n"] == 2
    assert report["positive_outcomes"] == 1
    assert report["negative_outcomes"] == 1
    assert 0.0 <= report["expected_calibration_error"] <= 1.0
    assert any(bucket["n"] for bucket in report["bins"])


def test_storyline_calibration_report_is_empty_without_future_validation_samples() -> (
    None
):
    report = _storyline_calibration_report([_sample_storyline_score()])

    assert report["n"] == 0
    assert report["expected_calibration_error"] is None


def test_variance_report_summarizes_scores_and_judged_rate(tmp_path) -> None:
    report_root = tmp_path / "runs"
    _write_run_artifact(
        report_root,
        "run-a",
        average=0.70,
        company=0.80,
        product=0.60,
        thesis_average=0.75,
        thesis_correct=7,
        thesis_incorrect=2,
    )
    _write_run_artifact(
        report_root,
        "run-b",
        average=0.76,
        company=0.84,
        product=0.63,
        thesis_average=0.80,
        thesis_correct=8,
        thesis_incorrect=1,
    )
    report = build_variance_report(report_root, ["run-a", "run-b"])
    markdown = _render_variance_markdown(report)

    average = report["metrics"]["average_storyline_score"]
    thesis_rate = report["judged_rates"]["thesis_recovery_correct_rate"]

    assert average["n"] == 2
    assert average["mean"] == 0.73
    assert average["min"] == 0.70
    assert average["max"] == 0.76
    assert average["stddev"] > 0
    assert thesis_rate["n"] == 18
    assert thesis_rate["correct"] == 15
    assert thesis_rate["wilson_95_ci"]["low"] < thesis_rate["rate"]
    assert "Wilson 95% CI" in markdown


def _sample_storyline_score() -> StorylineScore:
    return StorylineScore(
        storyline_id="atlas_renewal_risk",
        title="Atlas renewal risk is really security plus usage decay",
        signal_count=25,
        relevant_model_count=8,
        evidence_supported_model_count=4,
        keyword_hits=["atlas", "renewal", "security"],
        missing_keywords=[],
        situation_model_count=1,
        recommendation_model_count=1,
        scoped_edge_count=2,
        edge_kind_hits=["supports", "early_warning_for"],
        missing_edge_kinds=[],
        review_candidate_count=1,
        accepted_candidate_count=1,
        needs_review_candidate_count=0,
        latent_pattern_score=0.9,
        latent_pattern_model_count=1,
        latent_pattern_evidence_supported_model_count=1,
        latent_pattern_best_coverage=1.0,
        latent_pattern_group_hits=["security/evidence", "renewal/risk"],
        missing_latent_pattern_groups=[],
        latent_pattern_model_ids=["model-1"],
        score=0.85,
    )


def _sample_storyline_score_with_calibration() -> StorylineScore:
    score = _sample_storyline_score()
    score.calibration_samples = [
        {
            "storyline_id": score.storyline_id,
            "model_id": "model-1",
            "confidence": 0.82,
            "outcome": 1.0,
            "basis": "future_validation_wave_proxy",
        },
        {
            "storyline_id": score.storyline_id,
            "model_id": "model-2",
            "confidence": 0.74,
            "outcome": 0.0,
            "basis": "future_validation_wave_proxy",
        },
    ]
    return score


def _write_run_artifact(
    report_root,
    run_id: str,
    *,
    average: float,
    company: float,
    product: float,
    thesis_average: float,
    thesis_correct: int,
    thesis_incorrect: int,
) -> None:
    run_dir = report_root / run_id
    run_dir.mkdir(parents=True)
    (run_dir / "storyline_scores.json").write_text(
        json.dumps(
            {
                "run_id": run_id,
                "signals": 225,
                "storyline_count": 9,
                "elapsed_seconds": 12.0,
                "average_storyline_score": average,
                "company_intelligence_scorecard": {
                    "overall_score": company,
                    "product_value_evals": {"overall_score": product},
                },
                "thesis_recovery_judge": {
                    "enabled": True,
                    "n": thesis_correct + thesis_incorrect,
                    "average_score": thesis_average,
                    "correct_count": thesis_correct,
                    "incorrect_count": thesis_incorrect,
                },
            }
        )
    )
    (run_dir / "run_config.json").write_text(
        json.dumps(
            {
                "run_id": run_id,
                "cache_bypass_env": {"LLM_CACHE_BYPASS": "1"},
            }
        )
    )


def _sample_latent_bridge_storyline_score() -> StorylineScore:
    return StorylineScore(
        storyline_id=_LATENT_BRIDGE_STORYLINE_ID,
        title="Northstar pricing shift implies an unobserved decision bridge",
        signal_count=23,
        relevant_model_count=4,
        evidence_supported_model_count=3,
        keyword_hits=[
            "northstar",
            "pricing",
            "discount",
            "exception",
            "before",
            "after",
            "inferred",
            "unobserved",
            "confidence",
        ],
        missing_keywords=[],
        situation_model_count=1,
        recommendation_model_count=1,
        scoped_edge_count=2,
        edge_kind_hits=["explains", "early_warning_for"],
        missing_edge_kinds=[],
        review_candidate_count=1,
        accepted_candidate_count=1,
        needs_review_candidate_count=0,
        latent_pattern_score=0.88,
        latent_pattern_model_count=1,
        latent_pattern_evidence_supported_model_count=1,
        latent_pattern_best_coverage=1.0,
        latent_pattern_group_hits=[
            "before/after/state/transition",
            "unobserved/inferred/missing/gap",
            "discount/exception/pricing/policy",
        ],
        missing_latent_pattern_groups=[],
        latent_pattern_model_ids=["bridge-model-1"],
        score=0.9,
        inferred_bridge_model_count=1,
        inferred_bridge_transition_supported_model_count=1,
        inferred_bridge_future_confirmed_model_count=1,
        unsupported_bridge_specific_claim_count=0,
        bridge_epistemic_marker_hits=["confidence", "gap", "inferred"],
    )


def _sample_model_summary() -> dict:
    return {
        "run_id": "unit-scorecard",
        "tenant_id": "tenant",
        "signal_count": 25,
        "think_runs_success": 1,
        "think_runs_failed": 0,
        "pending_triggers": 0,
        "active_models": 15005,
        "archived_models": 0,
        "model_edges": 3,
        "relationship_candidates": 1,
        "relationship_candidate_status_distribution": {"accepted": 1},
        "capability_probe_counts": {},
        "model_kind_distribution": {"belief": 15005},
        "context_use_distribution": {"graph_context_used": 1},
        "context_use_relation_contract": {
            "context_use_runs": 1,
            "graph_selected_runs": 1,
            "graph_relation_op_runs": 1,
            "graph_no_edge_rationale_runs": 0,
            "graph_selected_without_relation_ops_runs": 0,
            "graph_relation_contract_satisfied_runs": 1,
            "graph_relation_contract_failed_runs": 0,
        },
        "edge_kind_distribution": {"supports": 2, "early_warning_for": 1},
        "edge_lifecycle": {
            "total_edges": 3,
            "active_edges": 3,
            "accepted_edges": 3,
            "accepted_edge_kind_distribution": {
                "supports": 2,
                "early_warning_for": 1,
            },
            "candidate_edges": 0,
            "needs_review_edges": 0,
            "retired_or_inert_edges": 0,
            "reconfirmed_edges": 0,
            "reconfirmation_events": 0,
            "distinct_edge_kinds": 2,
            "ontology_proposals": 0,
        },
        "graph_health": {"exact_duplicate_natural_groups": 0},
        "discovery_layer_counts": {
            "negative_memory": 0,
            "question_policy_stats": 0,
        },
        "topology_optimizer_metric_totals": {
            "shortcut_creates_or_bumps": 3,
            "affordance_reinforces": 2,
            "negative_memory_inserts": 0,
            "question_policy_updates": 0,
        },
        "post_commit_status": {"dead_lettered": 0},
        "cost": {"llm_calls": 1, "cost_usd": 0.01},
    }


def _sample_success_wave() -> dict:
    return {
        "sequence": "atlas_renewal_risk_wave",
        "signals": 25,
        "t1_batch": {
            "elapsed_s": 30.0,
            "observation_count": 25,
            "run": {
                "status": "success",
                "validation_error_count": 0,
                "retrieval_model_count": 22,
                "retrieval_observation_count": 29,
                "ops_applied": {
                    "claim_ops": [{}, {}, {}],
                    "edge_ops": [
                        {
                            "op": "add",
                            "edge_kind": "supports",
                            "review_status": "accepted",
                        },
                        {
                            "op": "add",
                            "edge_kind": "early_warning_for",
                            "review_status": "accepted",
                        },
                    ],
                    "act_ops": [{}],
                    "resource_ops": [],
                    "ontology_gap_ops": [],
                    "state_changes_emitted": 6,
                    "memory_aggregation": {
                        "model_inserts": 3,
                        "model_updates": 1,
                        "situation_model_updates": 1,
                    },
                    "context_use": {
                        "context_use_grade": "graph_context_used",
                        "selected_trigger_observation_count": 25,
                        "selected_historical_observation_count": 4,
                    },
                },
            },
        },
    }


def _sample_success_wave_with_relation_frame() -> dict:
    wave = json.loads(json.dumps(_sample_success_wave()))
    ops = wave["t1_batch"]["run"]["ops_applied"]
    ops["relation_claim_ops"] = [
        {
            "op": "upsert",
            "relation_kind": "blocked_workstream",
            "status": "accepted",
        }
    ]
    ops["relation_frame_ops"] = [
        {
            "op": "upsert",
            "relation_kind": "blocked_workstream",
            "status": "accepted",
            "write_policy": "project_edges",
            "participant_count": 5,
            "projected_edge_count": 3,
        }
    ]
    return wave


def _sample_future_validation_wave() -> dict:
    return {
        "sequence": "future_validation",
        "signals": 24,
        "t1_batch": {
            "elapsed_s": 34.0,
            "observation_count": 24,
            "run": {
                "status": "success",
                "validation_error_count": 0,
                "retrieval_model_count": 24,
                "retrieval_observation_count": 28,
                "ops_applied": {
                    "claim_ops": [{"op": "update"}],
                    "edge_ops": [
                        {
                            "op": "add",
                            "edge_kind": "blocks",
                            "review_status": "accepted",
                        }
                    ],
                    "act_ops": [],
                    "resource_ops": [],
                    "ontology_gap_ops": [],
                    "memory_aggregation": {
                        "model_updates": 2,
                        "evidence_attachments": 1,
                    },
                    "context_use": {
                        "context_use_grade": "model_context_used",
                        "selected_trigger_observation_count": 24,
                        "selected_historical_observation_count": 3,
                    },
                },
            },
        },
    }


def test_latency_breakdown_composes_wave_think_and_inquiry_timings() -> None:
    waves = [
        {
            "wave": 1,
            "sequence": "atlas_wave",
            "t1_batch": {
                "trigger_id": str(uuid4()),
                "elapsed_s": 2.0,
                "run": {
                    "id": str(uuid4()),
                    "status": "success",
                    "llm_latency_ms": 500,
                    "retrieval_model_count": 16,
                    "retrieval_observation_count": 12,
                    "validation_error_count": 0,
                },
            },
        }
    ]
    think_rows = [
        {
            "trigger_kind": "T1:event_batch",
            "status": "success",
            "elapsed_ms": 2100,
            "llm_latency_ms": 500,
        },
        {
            "trigger_kind": "T3:missing_transition",
            "status": "success",
            "elapsed_ms": 250,
            "llm_latency_ms": 0,
        },
    ]
    inquiry_rows = [
        {
            "notes": {
                "retrieval_runtime": {
                    "total_ms": 1200,
                    "retrieval_action_timings_ms_total": 700,
                    "retrieval_action_work_timings_ms_total": 600,
                    "retrieval_action_wait_timings_ms_total": 100,
                    "retrieval_stage_timings_ms_total": 300,
                    "measured_ms_total": 1000,
                    "non_wait_measured_ms_total": 900,
                    "parallel_wait_overcount_ms": 100,
                    "unaccounted_ms": 200,
                    "work_unaccounted_ms": 300,
                },
                "retrieval_action_timings": [
                    {
                        "path": "sage_reader",
                        "elapsed_ms": 300,
                        "work_elapsed_ms": 300,
                        "wait_elapsed_ms": 0,
                    },
                    {
                        "path": "focused_index",
                        "elapsed_ms": 400,
                        "work_elapsed_ms": 300,
                        "wait_elapsed_ms": 100,
                        "cache_hit": True,
                    },
                ],
                "retrieval_stage_timings": [
                    {"stage": "context_packet_compile", "elapsed_ms": 50},
                    {"stage": "answer_sufficiency", "elapsed_ms": 250},
                ],
                "question_planning": [{"mode": "llm"}],
            }
        }
    ]

    report = benchmark._compose_latency_breakdown(
        waves=waves,
        think_rows=think_rows,
        inquiry_rows=inquiry_rows,
    )

    assert report["critical_path_summary"]["t1_wall_ms_total"] == 2000
    assert report["critical_path_summary"]["t1_llm_ms_total"] == 500
    assert report["critical_path_summary"]["t1_non_llm_residual_ms_total"] == 1500
    assert report["critical_path_summary"]["t1_unclassified_or_failed_ms_total"] == 0
    assert report["critical_path_summary"]["adaptive_inquiry_runtime_ms_total"] == 1200
    assert report["critical_path_summary"]["main_llm_share_of_t1_wall"] == 0.25
    assert report["t1_wave_wall_clock"]["waves"][0]["non_llm_residual_ms"] == 1500
    assert report["think_runs"]["by_trigger_kind"]["T1:event_batch"]["count"] == 1
    assert report["think_runs"]["non_llm_residual_ms"]["total_ms"] == 1850
    inquiry = report["adaptive_inquiry"]
    assert inquiry["sessions_with_runtime"] == 1
    assert inquiry["runtime_totals"]["retrieval_action_wait_timings_ms_total"] == 100
    assert inquiry["action_timings_by_path"]["focused_index"]["cache_hits"] == 1
    assert inquiry["action_timings_by_path"]["sage_reader"]["work_ms_total"] == 300
    assert inquiry["stage_timings_by_stage"]["answer_sufficiency"]["elapsed_ms_total"] == 250
    assert inquiry["question_planning_modes"] == {"llm": 1}


def test_render_benchmark_markdown_includes_latency_breakdown() -> None:
    latency = benchmark._compose_latency_breakdown(
        waves=[
            {
                "wave": 1,
                "sequence": "atlas_wave",
                "t1_batch": {
                    "elapsed_s": 2.0,
                    "run": {
                        "status": "success",
                        "llm_latency_ms": 500,
                        "retrieval_model_count": 16,
                        "retrieval_observation_count": 12,
                    },
                },
            }
        ],
        think_rows=[],
        inquiry_rows=[
            {
                "notes": {
                    "retrieval_runtime": {
                        "total_ms": 1000,
                        "retrieval_action_timings_ms_total": 600,
                        "retrieval_stage_timings_ms_total": 200,
                        "unaccounted_ms": 200,
                    },
                    "retrieval_action_timings": [
                        {"path": "sage_reader", "elapsed_ms": 600}
                    ],
                    "retrieval_stage_timings": [
                        {"stage": "context_packet_compile", "elapsed_ms": 200}
                    ],
                }
            }
        ],
    )
    summary = {
        "run_id": "latency-test",
        "status": "passed",
        "tenant_id": str(uuid4()),
        "signals": 25,
        "storyline_count": 0,
        "average_storyline_score": 0,
        "latent_pattern_fitness": {"average_latent_pattern_score": 0},
        "thesis_recovery_judge": {"average_score": 0, "n": 0},
        "calibration": {"expected_calibration_error": 0, "n": 0},
        "run_amplification": {"think_runs_per_signal": 0, "pending_triggers": 0},
        "question_planner_reflective_report": {},
        "latency_breakdown": latency,
        "storyline_scores": [],
        "company_intelligence_scorecard": {
            "overall_score": 0,
            "interpretation": "test",
            "dimensions": {},
            "product_value_evals": {"evals": {}, "proof_gaps": []},
            "proof_coverage": {},
            "proof_gaps": [],
        },
    }

    rendered = _render_benchmark_markdown(summary)

    assert "## Latency Breakdown" in rendered
    assert "| T1 wall-clock total | 2.000s |" in rendered
    assert "| Main Think LLM share of T1 wall | 25.0% |" in rendered
    assert "atlas_wave" in rendered
    assert "sage_reader" in rendered
    assert "context_packet_compile" in rendered
