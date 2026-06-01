"""services/github_intel/config.py — feature flags + tunables."""
from __future__ import annotations

import os

# Per-tenant feature flags (read via services.ingestion.feature_flags.TenantFlags).
GITHUB_INTEL_ENABLED = "github_intel.enabled"          # gate the whole subsystem
GITHUB_INTEL_LLM_ENABLED = "github_intel.llm_enabled"  # gate only the LLM step
CODE_INTEL_ENABLED = "code_intel.enabled"              # gate code-graph fetch/index

# Bounds the inline enrichment before it degrades to the raw signal.
INLINE_TIMEOUT_MS = int(os.environ.get("GITHUB_INTEL_INLINE_TIMEOUT_MS", "1500"))

# Blast-radius traversal depth.
MAX_BLAST_HOPS = int(os.environ.get("CODE_INTEL_MAX_BLAST_HOPS", "3"))

# Default repo full-name when a tenant maps to a single repo (demo convenience).
DEFAULT_REPO = os.environ.get("GITHUB_INTEL_DEFAULT_REPO", "")
