# Fyralis Benchmarks

This directory implements the Phase 1 benchmark foundation from
`fyralis-benchmark-implementation-plan.md`.

The current runnable target is a deterministic toy memory benchmark:

```bash
python -m benchmarks.run_benchmark --benchmark toy
```

It exercises:

- dataset adapter contracts
- normalized observations and queries
- in-memory ingestion
- lexical retrieval baseline
- context packet compilation
- fixed extractive answerer
- evaluation metrics
- JSONL, JSON, trace, and Markdown artifacts

Generated reports land under `benchmarks/reports/generated/` by default.

LongMemEval cleaned JSON files can also be run in retrieval-only mode:

```bash
python -m benchmarks.run_benchmark \
  --benchmark longmemeval \
  --data benchmarks/datasets/raw/longmemeval_s_cleaned.json \
  --system bm25_session \
  --top-k 10 \
  --max-cases 50 \
  --out benchmarks/reports/generated/longmemeval_bm25
```

By default this skips answer scoring for public runs. That keeps the
reported numbers to retrieval metrics until a fixed model answerer or the
official LongMemEval judge is wired in.

HotpotQA distractor/fullwiki JSON files can be run the same way:

```bash
python benchmarks/scripts/download_hotpotqa_validation.py
python -m benchmarks.run_benchmark \
  --benchmark hotpotqa \
  --data benchmarks/datasets/raw/hotpotqa_distractor_validation.json \
  --system bm25_session \
  --top-k 10 \
  --out benchmarks/reports/generated/hotpotqa_bm25
```

HaluMem-Medium memory-QA retrieval can be run with:

```bash
bash benchmarks/scripts/download_halumem_medium.sh
python -m benchmarks.run_benchmark \
  --benchmark halumem \
  --data benchmarks/datasets/raw/HaluMem-Medium.jsonl \
  --system bm25_session \
  --top-k 5 \
  --out benchmarks/reports/generated/halumem_bm25
```

LongMemEval-V2 small can be run against the product Ask Fyralis path.
This materializes the public trajectories into the Fyralis database,
asks the real `AskOrchestrator` for each question, and scores the
returned Ask answer via the benchmark evaluator:

```bash
export DATABASE_URL=postgresql://company_os:company_os@localhost:5432/company_os

uv run python -m benchmarks.run_benchmark \
  --benchmark longmemeval_v2 \
  --data benchmarks/datasets/raw/longmemeval-v2 \
  --haystack-tier small \
  --system fyralis_ask_current \
  --embedding-mode hash \
  --apply-migrations \
  --top-k 20 \
  --evidence-k 20 \
  --score-answers \
  --out benchmarks/reports/generated/lme_v2_small_fyralis_ask_current
```

`fyralis_ask_current` uses a passthrough answerer by default, so the
reported answer is the actual Ask Fyralis product answer rather than a
second fixed model answering from the packet.

MEMTRACK has two intentionally separate lanes:

- Retrieval-support runs leave `--score-answers` off and report whether
  the retrieved packet contains answer-supporting evidence.
- Public end-to-end correctness runs enable final answer generation and
  an LLM judge. This is closer to the paper's correctness metric, but it
  is still limited by the public archive surface unless the full
  dockerized Slack/Linear/Git/filesystem environment is available.

Example judged public run:

```bash
python -m benchmarks.run_benchmark \
  --benchmark memtrack \
  --data benchmarks/datasets/raw/memtrack/Memtrak \
  --system fyralis_sage_hybrid \
  --embedding-mode ollama \
  --top-k 20 \
  --evidence-k 20 \
  --score-answers \
  --answerer codex \
  --judge-answers \
  --judge codex \
  --out benchmarks/reports/generated/memtrack_fyralis_sage_hybrid_public_judged
```
