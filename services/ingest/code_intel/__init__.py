"""services/ingest/code_intel — code-comprehension subsystem (GitHub Intelligence Layer, Part A).

A living, commit-sha-versioned code graph per repo: files -> symbols ->
edges (contains/imports/references) + per-symbol code-RAG embeddings. Powers
the "blast radius" question (given changed files/symbols, who depends on them?)
that the github_intel enrichment ties to each GitHub signal.

Provider-agnostic by design: GitHub is only a *fetch source*. The indexer is
language-pluggable (see `parsing.py`); the shipped backbone is a precise,
zero-dependency Python `ast` indexer. tree-sitter / SCIP indexers are additive
behind the same `LanguageIndexer` Protocol (Phase 11).
"""
