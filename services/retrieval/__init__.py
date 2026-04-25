"""
services/retrieval — primary retrieval, second-pass expansion, context
assembler, and background relationship maintenance.

Public API is curated in submodules:

  - pathways.py   : pathway_a_structural / pathway_b_semantic /
                    pathway_c_temporal / pathway_d_pattern + PathwayResult
  - primary.py    : TriggerContext, RetrievalResult, primary_retrieve
  - second_pass.py: second_pass_expand
  - assembler.py  : AccessContext, ContextBundle, assemble_context
  - maintenance.py: MaintenanceReport, background_relationship_maintenance

Retrieval is READ-ONLY except for two write side-effects:
  1. Reconsolidation via ModelsRepo.retrieve(ids) — bumps activation,
     retrieval_count, last_retrieved_at. Confidence is NOT touched.
  2. relationship_maintenance_log writes from the background worker.

See BUILD-PLAN §4 Prompt 3.A and ARCHITECTURE-FINAL.md §8, §9, §10, §26.
"""
from __future__ import annotations
