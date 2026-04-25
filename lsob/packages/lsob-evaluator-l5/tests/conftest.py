"""Shared fixtures for L5 tests.

Provides a helper that builds a minimal `Corpus` with monthly ground-truth
checkpoints, suitable for exercising each sub-evaluator in isolation.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

import pytest
from lsob_contracts import Corpus, CorpusMeta, GroundTruth


def _month_step(base: datetime, i: int) -> datetime:
    # Use 30-day steps so stability/latency math never crosses month boundaries
    # in an ambiguous way.
    return base + timedelta(days=30 * i)


def build_corpus(
    *,
    months: int = 6,
    start: datetime | None = None,
    commitments: list[dict[str, Any]] | None = None,
    customers_per_checkpoint: list[list[dict[str, Any]]] | None = None,
    patterns_per_checkpoint: list[list[dict[str, Any]]] | None = None,
    predictions_per_checkpoint: list[list[dict[str, Any]]] | None = None,
    extra_meta: dict[str, Any] | None = None,
) -> Corpus:
    start = start or datetime(2024, 1, 15, tzinfo=timezone.utc)
    gts: list[GroundTruth] = []
    for i in range(months):
        ts = _month_step(start, i)
        gt = GroundTruth(
            timestamp=ts,
            commitments=(commitments or [])[:]
            if commitments is not None
            else [],
            customers=(customers_per_checkpoint[i]
                       if customers_per_checkpoint and i < len(customers_per_checkpoint)
                       else []),
            patterns=(patterns_per_checkpoint[i]
                      if patterns_per_checkpoint and i < len(patterns_per_checkpoint)
                      else []),
            predictions_that_will_resolve=(
                predictions_per_checkpoint[i]
                if predictions_per_checkpoint and i < len(predictions_per_checkpoint)
                else []
            ),
        )
        gts.append(gt)
    meta = CorpusMeta(
        corpus_id="l5-test",
        company_id="co-test",
        months_simulated=months,
        seed=42,
        config_hash="test",
        start_date=start,
        end_date=_month_step(start, months - 1) if months else start,
    )
    corpus = Corpus(meta=meta, signals=[], ground_truth=gts)
    if extra_meta:
        for k, v in extra_meta.items():
            object.__setattr__(corpus, k, v)
    return corpus


@pytest.fixture
def make_corpus():
    return build_corpus
