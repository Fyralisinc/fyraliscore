"""LayerSixEvaluator: decision-support quality (Phase 6a + 6b)."""

from __future__ import annotations

from collections import Counter
from typing import Any

from lsob_contracts import (
    Corpus,
    DiffOp,
    EvalResult,
    EvaluationContext,
    Trigger,
)

from lsob_evaluator_l6.judge import LLMJudge, MockJudge
from lsob_evaluator_l6.metrics import (
    confidence_alignment_rate,
    falsifier_adequacy_rate,
    is_over_split,
    is_under_split,
    state_transition_accuracy,
)
from lsob_evaluator_l6.sampling import sample_uniform

_DEFAULT_MAX_JUDGE_CALLS = 500

_PHASE6A_METRIC_NAMES = (
    "state_transition_accuracy",
    "confidence_alignment_rate",
    "falsifier_adequacy_rate",
    "over_splitting_rate",
    "under_splitting_rate",
)

_PHASE6B_METRIC_NAMES = (
    "pairwise_win_rate",
    "pairwise_tie_rate",
    "pairwise_loss_rate",
)


def _extract_reference_pairs(
    corpus: Corpus,
) -> list[tuple[Trigger, DiffOp]]:
    """Pull (trigger, reference_diff) tuples from ground_truth[].reference_diffs.

    The field is optional. When absent we simply return an empty list and the
    evaluator emits a single `layer6_no_reference` EvalResult.
    """
    pairs: list[tuple[Trigger, DiffOp]] = []
    for gt in corpus.ground_truth:
        ref_diffs: list[dict[str, Any]] = []
        # The reference_diffs field isn't declared on GroundTruth yet; use
        # getattr so we keep compatibility with older fixtures. `_Base` uses
        # extra="forbid" which would normally strip unknown fields, but in
        # practice fixtures load the attribute through model_extra when
        # present. Fall back to model_extra for robustness.
        raw = getattr(gt, "reference_diffs", None)
        if raw is None and getattr(gt, "model_extra", None):
            raw = gt.model_extra.get("reference_diffs") if gt.model_extra else None
        if raw:
            ref_diffs = list(raw)
        for entry in ref_diffs:
            trig_raw = entry.get("trigger")
            if trig_raw is None:
                # Older shape: just a trigger_id + diff. Synthesize a minimal
                # Trigger so the judge has something to echo back.
                trigger_id = entry.get("trigger_id")
                if trigger_id is None:
                    continue
                trig = Trigger(
                    trigger_id=trigger_id,
                    kind=entry.get("trigger_kind", "unknown"),
                    payload=entry.get("trigger_payload", {}),
                    timestamp=gt.timestamp,
                )
            else:
                trig = Trigger.model_validate(trig_raw)
            diff = DiffOp.model_validate(entry["diff"])
            pairs.append((trig, diff))
    return pairs


class LayerSixEvaluator:
    """Top-level Layer 6 evaluator. Implements the `Evaluator` Protocol."""

    layer_id: int = 6
    metric_names: list[str] = [
        *_PHASE6A_METRIC_NAMES,
        *_PHASE6B_METRIC_NAMES,
    ]

    def __init__(
        self,
        judge: LLMJudge | None = None,
        max_judge_calls: int = _DEFAULT_MAX_JUDGE_CALLS,
        sampling_seed: int = 0,
    ) -> None:
        self._judge = judge
        self._max_judge_calls = max_judge_calls
        self._sampling_seed = sampling_seed

    async def evaluate(
        self, ctx: EvaluationContext
    ) -> list[EvalResult]:
        pairs = _extract_reference_pairs(ctx.corpus)
        if not pairs:
            return [
                EvalResult(
                    layer_id=6,
                    metric_name="layer6_no_reference",
                    value=0.0,
                    confidence_interval=None,
                    breakdown_by={
                        "reason": "corpus has no ground_truth[].reference_diffs",
                    },
                    run_id=ctx.run_id,
                )
            ]

        # Phase 6a: produce the SUT diff for every reference trigger.
        produced: list[tuple[Trigger, DiffOp, DiffOp]] = []
        for trig, ref_diff in pairs:
            sut_diff = await ctx.sut.produce_diff_for_trigger(trig)
            produced.append((trig, ref_diff, sut_diff))

        results: list[EvalResult] = []
        results.extend(self._phase_6a_results(produced, run_id=ctx.run_id))

        enable_judge = bool(ctx.extras.get("enable_llm_judge", False))
        if not enable_judge:
            results.append(
                EvalResult(
                    layer_id=6,
                    metric_name="layer6b_skipped",
                    value=0.0,
                    confidence_interval=None,
                    breakdown_by={"reason": "enable_llm_judge=False"},
                    run_id=ctx.run_id,
                )
            )
            return results

        results.extend(
            await self._phase_6b_results(produced, run_id=ctx.run_id)
        )
        return results

    def _phase_6a_results(
        self,
        produced: list[tuple[Trigger, DiffOp, DiffOp]],
        run_id: str,
    ) -> list[EvalResult]:
        transition_scores: list[float] = []
        alignment_scores: list[float] = []
        falsifier_scores: list[float] = []
        over_flags: list[bool] = []
        under_flags: list[bool] = []

        for _trig, ref_diff, sut_diff in produced:
            transition_scores.append(
                state_transition_accuracy(sut_diff, ref_diff)
            )
            alignment_scores.append(
                confidence_alignment_rate(sut_diff, ref_diff)
            )
            falsifier_scores.append(falsifier_adequacy_rate(sut_diff))
            over_flags.append(is_over_split(sut_diff, ref_diff))
            under_flags.append(is_under_split(sut_diff, ref_diff))

        def _mean(xs: list[float]) -> float:
            return sum(xs) / len(xs) if xs else 0.0

        def _rate(xs: list[bool]) -> float:
            return sum(1 for x in xs if x) / len(xs) if xs else 0.0

        n = len(produced)
        breakdown_common = {"n_triggers": n}

        return [
            EvalResult(
                layer_id=6,
                metric_name="state_transition_accuracy",
                value=_mean(transition_scores),
                breakdown_by=breakdown_common,
                run_id=run_id,
            ),
            EvalResult(
                layer_id=6,
                metric_name="confidence_alignment_rate",
                value=_mean(alignment_scores),
                breakdown_by=breakdown_common,
                run_id=run_id,
            ),
            EvalResult(
                layer_id=6,
                metric_name="falsifier_adequacy_rate",
                value=_mean(falsifier_scores),
                breakdown_by=breakdown_common,
                run_id=run_id,
            ),
            EvalResult(
                layer_id=6,
                metric_name="over_splitting_rate",
                value=_rate(over_flags),
                breakdown_by=breakdown_common,
                run_id=run_id,
            ),
            EvalResult(
                layer_id=6,
                metric_name="under_splitting_rate",
                value=_rate(under_flags),
                breakdown_by=breakdown_common,
                run_id=run_id,
            ),
        ]

    async def _phase_6b_results(
        self,
        produced: list[tuple[Trigger, DiffOp, DiffOp]],
        run_id: str,
    ) -> list[EvalResult]:
        judge = self._judge or LLMJudge(judge_client=MockJudge())
        sampled_indices = sample_uniform(
            n_total=len(produced),
            cap=self._max_judge_calls,
            seed=self._sampling_seed,
        )
        sampled = [produced[i] for i in sampled_indices]

        outcomes: list[str] = []
        ordering_counter: Counter[str] = Counter()
        ref_score_totals: Counter[str] = Counter()
        sut_score_totals: Counter[str] = Counter()
        ref_score_n: Counter[str] = Counter()
        sut_score_n: Counter[str] = Counter()

        for trig, ref_diff, sut_diff in sampled:
            outcome = await judge.compare(trig, ref_diff, sut_diff)
            outcomes.append(outcome.winner)
            ordering_counter[outcome.ordering] += 1
            for k, v in outcome.scores_reference.items():
                ref_score_totals[k] += v
                ref_score_n[k] += 1
            for k, v in outcome.scores_sut.items():
                sut_score_totals[k] += v
                sut_score_n[k] += 1

        n = len(outcomes) or 1
        win_rate = outcomes.count("sut") / n
        tie_rate = outcomes.count("tie") / n
        loss_rate = outcomes.count("reference") / n

        shared_breakdown: dict[str, Any] = {
            "n_judge_comparisons": len(outcomes),
            "n_triggers_total": len(produced),
            "ordering_counts": dict(ordering_counter),
            "prompt_hash": judge.prompt_hash,
            "mean_scores_reference": {
                k: ref_score_totals[k] / ref_score_n[k]
                for k in ref_score_totals
                if ref_score_n[k]
            },
            "mean_scores_sut": {
                k: sut_score_totals[k] / sut_score_n[k]
                for k in sut_score_totals
                if sut_score_n[k]
            },
        }

        return [
            EvalResult(
                layer_id=6,
                metric_name="pairwise_win_rate",
                value=win_rate,
                breakdown_by=shared_breakdown,
                run_id=run_id,
            ),
            EvalResult(
                layer_id=6,
                metric_name="pairwise_tie_rate",
                value=tie_rate,
                breakdown_by=shared_breakdown,
                run_id=run_id,
            ),
            EvalResult(
                layer_id=6,
                metric_name="pairwise_loss_rate",
                value=loss_rate,
                breakdown_by=shared_breakdown,
                run_id=run_id,
            ),
        ]


__all__ = ["LayerSixEvaluator"]
