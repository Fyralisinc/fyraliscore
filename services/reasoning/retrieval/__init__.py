"""
services/reasoning/retrieval — primary retrieval, second-pass expansion, context
assembler, and background relationship maintenance.

Public API is curated in submodules:

  - pathways.py   : pathway_a_structural / pathway_b_semantic /
                    pathway_c_temporal / pathway_d_pattern + PathwayResult
  - primary.py    : TriggerContext, RetrievalResult, primary_retrieve
  - second_pass.py: second_pass_expand
  - assembler.py  : AccessContext, ContextBundle, assemble_context
  - maintenance.py: MaintenanceReport, background_relationship_maintenance
  - projection_context.py: load_constraint_context / load_projection_context
  - projection_pathway.py: projection-first Model candidates

Retrieval is canonical-truth read-only with two operational side-effects:
  1. ModelsRepo.retrieve(ids) records activation, retrieval_count, and
     last_retrieved_at in model_activity_sidecar. Model truth is untouched.
  2. relationship_maintenance_log writes from the background worker.

See BUILD-PLAN §4 Prompt 3.A and ARCHITECTURE-FINAL.md §8, §9, §10, §26.
"""
from __future__ import annotations
