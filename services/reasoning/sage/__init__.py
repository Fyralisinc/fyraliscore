"""services.reasoning.sage — adaptive company-specific policy memory.

SAGE is not a canonical truth store and must not be treated as a second Models
layer. It learns how this tenant/company tends to behave by updating retrieval
utility, question utility, salience, negative memory, structural features,
residuals, and latent pattern priors. Explicit company truth still belongs in
Models and typed edges.

Promotion bridge:
  SAGE latent signal -> Think semantic judgment -> Pattern/Situation Model.

Retrieval motifs, discovery shortcuts, route utilities, company profiles, and
latent pattern candidates are optimization memory. They may change what Fyralis
retrieves, asks, or prioritizes, but they do not assert company facts. Only
normal Think validation/application can promote a useful, stable, explainable,
falsifiable, action-shaping regularity into explicit Model memory.

Glossary:
  * latent SAGE memory — mutable policy/salience/utility memory that adapts
    future behavior and carries `canonical_write=false`.
  * explicit Model memory — canonical tenant truth in Models/model_edges/etc.,
    subject to Model grammar, authority, validation, lifecycle, and audit.

Sub-packages:
  * inquiry_traces — Phase 1 gap-filler tables (retrieval_plans,
    omitted_evidence, inquiry_outcome_events) used to make every
    inquiry session inspectable end-to-end. See
    fyralis-sage-synthesis-self-evolution.md Phase 1 + §15.1.
  * structural_features — Phase 5 structural feature store
    (per-Model + per-edge topological properties). See
    fyralis-sage-synthesis-self-evolution.md §10 / Phase 5.
  * reader — Phases 2-8 query-conditioned Synthesis Reader
    (cue extraction, intent inference, soft activation, gating,
    subgraph selection, evidence projection).
  * experience — the small contract that makes SAGE's role explicit:
    outcome events become policy effects that can change future behavior.
  * model_residuals — persistent model-metabolism residual evidence for
    compression debt that is not canonical truth.
  * latent_gaps — non-canonical hypotheses born only from measured residual
    clusters.
  * patterns — surface-independent structural signatures, bounded global
    scouts, counterexamples, and Think-review promotion assessments for latent
    pattern learning. These never write canonical Models.
  * company_profile — compact tenant/company learning digest assembled from
    SAGE optimization surfaces. This is policy memory, not explicit truth.
"""
