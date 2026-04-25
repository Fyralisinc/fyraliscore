"""Resume should refuse to continue if the corpus file hash changed."""

from __future__ import annotations

import json
import shutil
from pathlib import Path

import pytest

from lsob_contracts import AblationConfig

from lsob_harness.checkpoint import CorpusHashMismatch
from lsob_harness.mocks import MockEvaluatorRegistry, MockSUT
from lsob_harness.runner import RunRequest, resume_run, run_once


@pytest.mark.asyncio
async def test_resume_fails_on_modified_corpus(
    tmp_path: Path, mini_corpus_a: Path
) -> None:
    runs_root = tmp_path / "runs"
    # Copy the corpus so we can mutate it without touching the fixture.
    local_corpus = tmp_path / "mini_corpus_a.json"
    shutil.copy(mini_corpus_a, local_corpus)

    sut = MockSUT(name="mock")
    evaluators = MockEvaluatorRegistry.construct_for_layers([1, 2])
    req = RunRequest(
        corpus_path=local_corpus,
        sut_name="mock",
        layers=[1, 2],
        ablation=AblationConfig(name="none"),
        runs_root=runs_root,
        sut_override=sut,
        evaluators_override=evaluators,
    )
    outcome = await run_once(req)

    # Mutate the corpus in place (flip a signal's text).
    raw = json.loads(local_corpus.read_text())
    raw["signals"][0]["content_text"] = "mutated-for-mismatch"
    local_corpus.write_text(json.dumps(raw))

    with pytest.raises(CorpusHashMismatch):
        await resume_run(outcome.run_id, runs_root)
