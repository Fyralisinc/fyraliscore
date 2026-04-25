"""Prompt hash is identical across processes and imports."""

from __future__ import annotations

import subprocess
import sys

from lsob_evaluator_l6.llm_judge import load_prompt_template, prompt_hash


def test_prompt_hash_is_deterministic_in_process():
    a = prompt_hash()
    b = prompt_hash()
    assert a == b
    assert len(a) == 64


def test_prompt_hash_matches_explicit_template():
    template = load_prompt_template()
    assert prompt_hash(template) == prompt_hash()


def test_prompt_hash_stable_across_processes():
    local = prompt_hash()
    # Recompute in a fresh subprocess to ensure the hash does not depend on
    # any global mutable state (random seeds, env vars, import order, etc.).
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "from lsob_evaluator_l6.llm_judge import prompt_hash; print(prompt_hash())",
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    remote = result.stdout.strip()
    assert remote == local


def test_legacy_import_path_agrees():
    from lsob_evaluator_l6.judge import prompt_hash as legacy_prompt_hash

    assert legacy_prompt_hash() == prompt_hash()
