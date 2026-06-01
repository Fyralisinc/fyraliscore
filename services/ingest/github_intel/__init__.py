"""services/ingest/github_intel — GitHub state + signal-enrichment engine (Part B).

A dedicated GitHub-state subsystem fed by the existing `github:webhook`
observations. It (1) maintains FSMs for the repo's live state (PR lifecycle +
CI, branches, issues, checks), and (2) produces the layer's primary output:
causal context tied to every GitHub signal — written inline into the same
observation row's `content["intelligence"]` (raw-on-failure), and persisted to
the structured `github_signal_enrichment` system-of-record.

It uses `services.ingest.code_intel` for the "what code does this touch?" blast radius
and (optionally, flag-gated) an LLM for the causal "why".
"""
