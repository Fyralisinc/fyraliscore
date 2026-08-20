# Next-Generation Entity Resolution for Fyralis

**Architecture review / implementation recommendation**

**Status:** Proposed

**Date:** 2026-08-03
**Audience:** Fyralis architecture, ingestion, domain, reasoning, data, and security owners

## 1. Executive Summary

Entity Resolution (ER) is the semantic identity boundary between an observation and Fyralis's durable model of an organization. It answers a narrower question than extraction—“which real organizational entity does this source identity or mention denote?”—but its errors have unusually broad consequences. A false merge combines evidence, behavior, commitments, permissions, and history that belong to different entities. A false split fragments organizational memory. In Fyralis, a false merge should normally be treated as more expensive than a missed link.

The current implementation has the correct macro-shape but an unsafe decision core. It already uses a low-latency, tenant-scoped alias lookup during ingestion; persists observations even when identity is unresolved; sends difficult phrases to an asynchronous worker; learns aliases; emits late-resolution state changes; and provides human clarification and an open-world substrate promotion path. These are valuable foundations and should be retained.

The present resolver is nevertheless an alias lookup plus an LLM adjudicator, not yet a complete ER system. Its hot path accepts any unambiguous alias regardless of stored confidence or temporal validity. Its worker asks an LLM to emit an arbitrary `{"type", "id"}` reference and treats the model's self-reported confidence above `0.8` as sufficient for an authoritative update. It has no calibrated pair model, no durable candidate/evidence/decision ledger, no negative links, no cluster versioning, no merge or split semantics, and no bitemporal identity history. Candidate extraction is ASCII-only, capped at 50 one-to-three-grams, and the existing vector index is not used for retrieval. Actor mapping is especially urgent: `actor_identity_mappings` is globally keyed by source channel and source reference rather than tenant, and actor-identity clarification options are created but are not handled by the clarification side-effect dispatcher.

The recommended design is a **multi-stage, open-world, temporal identity system**:

1. **Persist first.** Store the canonical observation and its source facts without waiting on remote models or broad candidate searches.
2. **Resolve deterministic identities during ingestion.** Only source-scoped identifiers and already-confirmed, temporally valid, tenant-scoped aliases may take the synchronous fast path. This stage annotates; it never destructively merges canonical entities.
3. **Resolve the long tail asynchronously.** Generate candidates through a union of exact, lexical, phonetic, semantic, temporal, organizational, and graph blocking lanes. Score them with a calibrated, type-specific model and explicit constraints.
4. **Abstain as a first-class outcome.** A decision policy selects `LINK`, `REVIEW`, `NEW/PROVISIONAL`, or `NO_MATCH`. Thresholds are set from the business cost of false merges, not from raw model output.
5. **Record evidence and decisions, not just the answer.** Mentions, source identities, aliases, candidate scores, negative evidence, model/rule versions, decisions, memberships, and corrections remain queryable and replayable.
6. **Treat identity as bitemporal and reversible.** A durable identity has event-time validity and system-time knowledge. Merges create lineage and redirects; splits produce new membership versions and repair events. Historical queries reproduce what was valid at the event time and what Fyralis believed at a selected system time.
7. **Use relationships carefully.** Organizational graph evidence should re-rank ambiguous candidates and detect inconsistent clusters. It must not turn a weak pairwise edge into an automatic transitive merge.
8. **Constrain LLMs to bounded assistance.** LLMs may extract mentions, propose normalization features, compare a supplied short list, and explain evidence. They must never invent an identifier, search the tenant unrestricted, set operational probability by self-report, or mutate identity state directly.
9. **Turn human review into supervised data.** Reviewer actions must create durable positive or negative labels, corrected aliases, cluster constraints, and impact-aware training examples. Model changes should be evaluated offline and deployed by version, never learned immediately from a single action.

The recommended source of truth remains PostgreSQL for the first implementation: an identity registry and append-only decision ledger fit the current stack, while `pg_trgm`, GIN, and pgvector/HNSW can support the first candidate indexes. Kafka should carry resolution and repair events. A graph database, a distributed ANN service, or a Flink-style stateful resolver should be introduced only when measured scale or traversal latency justifies the additional consistency boundary. The architecture does not depend on postponing those components; it defines stable contracts so they can replace projections later.

The first release must be **safety and measurement**, not a new neural matcher. Tenant-scope actor mappings, validate all canonical references, stop trusting LLM self-confidence, complete actor clarification actions, preserve unresolved outcomes, and instrument a labelled shadow pipeline. The next releases add the identity ledger and hybrid candidates, then calibrated ranking and review, and finally temporal collective resolution, merge/split tooling, and selective infrastructure scale-out.

### Architecture decision

Adopt a versioned identity ledger and hybrid resolver in which:

- immutable source observations and resolution projections are separate;
- synchronous resolution is limited to authoritative identifiers and confirmed aliases;
- asynchronous decisions are candidate-constrained, calibrated, explainable, and reversible;
- canonical identity and domain state remain distinct—the ER layer identifies an actor, customer, system, repository, project, document, commitment, or goal but does not collapse their domain lifecycles;
- uncertainty is persisted on evidence and decisions, while downstream consumers receive an explicit resolution status and identity-version reference.

## 2. Formal Definition of Entity Resolution

### 2.1 Objects and objective

Let an incoming organizational fragment be

\[
r_i = (x_i, s_i, t_i, c_i, g_i),
\]

where `x` is observed content and attributes, `s` is source and source-specific identity, `t` is event time, `c` is local conversational/organizational context, and `g` is observed relationship evidence. Let \(E_t\) be the canonical entities known to the tenant at event time \(t\), plus a distinguished `NIL` outcome for “no known entity.” Entity linking estimates

\[
P(z_i=e \mid r_i, E_t, H_t), \quad e \in E_t \cup \{NIL\},
\]

where \(H_t\) is the tenant's evidence and decision history. Open-world ER must also decide whether `NIL` means a genuine non-entity, insufficient evidence, or a new entity candidate.

For record linkage or deduplication, the primitive target is an equivalence relation:

\[
y_{ij}=1 \iff r_i \text{ and } r_j \text{ denote the same real-world entity}.
\]

A pair model estimates \(p_{ij}=P(y_{ij}=1\mid \phi(r_i,r_j))\), where \(\phi\) contains lexical, identifier, source, temporal, graph, and organizational comparison features. A valid entity partition is not merely a set of independent positive edges. It must be reflexive, symmetric, and transitive, and it may be subject to domain constraints such as source-local uniqueness, incompatible entity types, non-overlapping employment, or explicitly asserted `DIFFERENT_FROM` links.

The operational decision minimizes expected cost rather than maximizing pair accuracy:

\[
a^* = \arg\min_{a \in \{link, review, new, no\_match\}}
\sum_y C(a,y)P(y\mid r_i,E_t,H_t).
\]

For Fyralis, `C(false_merge)` is generally much greater than `C(false_split)`: a mistaken merge contaminates graph traversal and reasoning across many observations, whereas a split usually loses recall and can be repaired later. Costs vary by type and action. Linking two spelling variants of a low-impact project label is not equivalent to merging two employees, customers, or legal entities.

### 2.2 Four concepts that must not be conflated

| Concept | Fyralis example | Correct representation |
|---|---|---|
| Mention | “The Bank” in one Slack message | A span/phrase in a specific observation with offsets, context, type hypotheses, and extraction provenance |
| Source identity | Slack user `U123`, GitHub login `nbi-bot`, email `a@x.com` | A tenant- and source-scoped identifier with validity and provenance |
| Alias assertion | “NBI” is a name for Nimbus Bank | Evidence-backed, time-bounded mapping from a normalized surface form to a canonical identity |
| Canonical entity | The durable Nimbus Bank customer or legal entity | Stable Fyralis identity envelope pointing to a typed domain object and its versioned lineage |

Named-entity recognition finds mention spans and types. Coreference groups mentions in a discourse. Entity linking assigns mentions to a known entity or `NIL`. Record linkage/deduplication groups source records. Data fusion chooses or preserves conflicting attribute values after identity is established. These can exchange evidence, but combining them into one opaque “resolve” call makes correction, evaluation, and security analysis impossible.

### 2.3 Fyralis-specific definition

For Fyralis, ER is:

> The tenant-scoped, time-aware, evidence-preserving process that maps source identities and observation mentions to stable organizational identities; detects new identities; maintains merge/split lineage; and exposes calibrated uncertainty so downstream perception can distinguish facts, hypotheses, and unresolved references.

It serves five purposes in the ingestion flow:

- **continuity:** connect a new observation to prior observations about the same entity;
- **cross-source unification:** connect Slack, email, GitHub, HR, CRM, and internal-system identifiers;
- **disambiguation:** keep identically named people, teams, systems, and companies separate;
- **context routing:** attach observations to the correct model scopes and trigger targeted reasoning;
- **correction and audit:** allow later evidence or a human to revise identity without rewriting source history.

ER must not decide the truth of every attribute, equate a person with their role, equate a product brand with a legal company, or turn a mention into a new domain object without an explicit open-world creation policy.


No single technique dominates all of ER. Candidate generation and final matching are different problems: the blocker must achieve very high recall cheaply; the matcher must reject hard negatives and be calibrated. Production systems usually cascade several families.

### 3.1 Comparative assessment

In the table, cost is per relevant operation after ordinary indexing; “incremental” means whether new records can be incorporated without full retraining or reclustering.

| Family | Strengths | Weaknesses | Cost and scale | Streaming / incremental | Explainability | Production maturity |
|---|---|---|---|---|---|---|
| Exact identifiers | Highest precision when the identifier is authoritative, scoped, and non-reassigned | Shared, recycled, malformed, or incorrectly scoped IDs create catastrophic merges | Hash/B-tree lookup, approximately O(1) or O(log n) | Excellent | Excellent | Very high |
| Deterministic rules | Encodes domain invariants, vetoes, and trusted composite keys | Rule interactions grow; brittle under drift; apparent transitivity can overmerge | Usually low; depends on join/block | Excellent if rules are versioned | Excellent | Very high |
| Alias dictionaries | Fast, learns repeated language, easy to curate | Same alias can denote several entities; staleness and popularity bias; no context alone | Indexed lookup; very scalable | Excellent | High | Very high |
| Fellegi–Sunter | Principled likelihood ratio from agreement/disagreement; handles missing fields and rare values; supports link/review/non-link | Conditional-independence assumptions; requires good comparison levels and parameter estimates | Linear in candidate pairs | Good; parameters update in batches | High: feature weights are inspectable | Very high in statistical linkage |
| General probabilistic linkage | Produces comparable probabilities and cost-sensitive decisions; can incorporate priors | Calibration and distribution shift are real; probabilities depend on candidate sampling | Low to medium after blocking | Good | Medium to high | High |
| Bayesian linkage/partitioning | Represents posterior uncertainty and global constraints; can leave cases unresolved; coherent cluster inference | Computationally heavier; modeling and diagnostics require expertise | Often expensive MCMC/variational inference; best for batches or high-value subsets | Limited to moderate, though incremental variants exist | High at model level | Medium in specialized domains |
| Logistic/linear ML | Strong baseline, cheap, calibratable, stable with modest labels | Needs engineered features; misses complex interactions | Very low inference cost | Excellent | High | Very high |
| Gradient-boosted trees | Excellent on mixed tabular similarity features, missingness, and nonlinear interactions | Candidate-distribution leakage; raw probabilities need calibration; model-specific explanations are approximate | Low inference cost; scales well | Excellent inference, periodic retraining | Medium/high with feature contributions | Very high |
| Random forests | Robust baseline, less tuning, nonlinear features | Larger/slower than boosted alternatives at similar quality; probabilities often poorly calibrated | Low to medium | Good | Medium | High |
| Deep pair encoders | Learn field and token interactions with less manual engineering | Label/data hungry, domain drift, latency, opaque errors | Medium to high, GPU-friendly in batches | Moderate; model updates are batched | Low to medium | High for text-rich matching |
| Bi-encoders | Encode mention and entity separately; enable ANN retrieval over millions of candidates | Compress interaction into one vector; weaker on subtle contradictions | O(log n)-like ANN search after offline embeddings | Strong for streaming retrieval; index updates required | Low | High for entity linking |
| Cross-encoders | Rich joint attention over mention and candidate; strong reranking | Cannot score the full catalog; high latency/cost | O(k) expensive model calls for top-k only | Moderate as a late cascade | Low/medium with evidence extraction | High as reranker |
| Generic embedding similarity | Cheap semantic recall and multilingual robustness | Similar meaning is not identity; hubness and model drift; thresholds are domain-specific | ANN makes large catalogs practical | Good, with versioned embeddings and indexes | Low | High for retrieval, unsafe alone for merge |
| GNN/link prediction | Uses relational topology and can detect non-obvious identity structure | Homophily is not identity; hubs, leakage, cold start, retraining, and explanation challenges | High offline training; sampled inference | Moderate; commonly asynchronous | Low | Medium |
| Collective ER | Enforces coherence across related mentions/records and can repair locally ambiguous pairs | Circular confirmation and error propagation; global inference can be expensive | Medium/high on candidate subgraphs | Best as background refinement | Medium if contributing relations are retained | Medium/high in knowledge systems |
| Generative/LLM matching | Strong zero/few-shot semantic comparison and explanation; useful for sparse organizational language | Non-deterministic, costly, prompt-sensitive, uncalibrated, vulnerable to injected content, may invent IDs | High per pair; poor for broad retrieval | Limited to bounded late-stage use | Superficially high; generated rationale is not proof | Emerging |
| Hybrid cascades | Combines deterministic precision, statistical calibration, semantic recall, and review | More components and version contracts to operate | Cost controlled by gates and top-k budgets | Excellent when event-driven | High if evidence is preserved | State-of-practice |
| Human-in-the-loop | Resolves novel/high-impact ambiguity; creates labels and domain rules | Expensive, inconsistent, slow; reviewers need context and impact preview | Queue and reviewer constrained | Asynchronous by nature | Highest when UI is well designed | Very high |

### 3.2 Classical probabilistic linkage remains relevant

Fellegi and Sunter formalized linkage as a likelihood-ratio decision with three actions—link, possible link, and non-link—under bounded error rates ([original paper](https://www.tandfonline.com/doi/abs/10.1080/01621459.1969.10501049)). Modern implementations add string comparators, missingness, term-frequency corrections, and learned parameters. The United Kingdom's Census 2021 linkage used a production combination of deterministic matching, probabilistic linkage, blocking, and clerical resolution rather than a single opaque model ([ONS methodology](https://www.ons.gov.uk/peoplepopulationandcommunity/populationandmigration/populationestimates/methodologies/linkagemethodsforcensus2021inenglandandwales)).

The important lesson for Fyralis is not to reproduce a census matcher. It is to preserve the likelihood decomposition: rare agreement is stronger than agreement on a common value; missing data differs from disagreement; and the output must support an abstention band. Splink operationalizes this family with EM-estimated `m`/`u` probabilities, term-frequency adjustments, explicit blocking, and model/link/cluster evaluation ([training rationale](https://moj-analytical-services.github.io/splink/topic_guides/training/training_rationale.html), [evaluation overview](https://moj-analytical-services.github.io/splink/topic_guides/evaluation/overview.html)).

Bayesian models go further. Sadinle's bipartite record linkage represents assignment uncertainty and a “no link” outcome under one-to-one constraints ([paper](https://arxiv.org/abs/1601.06630)); Bayesian partition models make transitive entity clusters the latent object rather than repairing inconsistent independent pairs afterward ([paper](https://arxiv.org/abs/1407.8219)). Fyralis should borrow their separation of uncertainty, constraints, and decisions, but not put MCMC on the ingestion path.

### 3.3 Learned text matching and retrieval

DeepMatcher demonstrated field-aware neural matching across structured records ([SIGMOD paper](https://doi.org/10.1145/3183713.3196926)). Ditto used pretrained language models plus domain knowledge and data augmentation, showing the value of semantic transfer for difficult textual pairs ([paper](https://arxiv.org/abs/2004.00584)). These systems improve pair classification but do not remove candidate generation, calibration, cluster consistency, or operational correction.

The dominant large-catalog entity-linking pattern is retrieve then rerank. Meta's BLINK independently embeds mention context and entity descriptions with a bi-encoder, retrieves through FAISS, and applies a cross-encoder to a small candidate list ([BLINK](https://github.com/facebookresearch/BLINK)). A bi-encoder is therefore a candidate lane, not a final identity oracle. Generic embedding similarity is especially dangerous for organizational names: “payments API” and “billing service” can be semantically close yet operationally distinct, while an employee and contractor with identical names may be unrelated.

### 3.4 Graph and collective resolution

Collective methods score a set of assignments together. Early relational ER demonstrated that co-occurrence and relational evidence can improve ambiguous identity decisions ([Bhattacharya and Getoor](https://linqs.org/assets/resources/bhattacharya-tkdd07.pdf)). Google's multi-focal attention work selectively used strong knowledge-base relations rather than assuming document-wide coherence ([paper](https://research.google/pubs/collective-entity-resolution-with-multi-focal-attention/)). Microsoft's web-scale ER work showed that relationship evidence and global constraints can outperform isolated pair decisions ([paper](https://www.microsoft.com/en-us/research/wp-content/uploads/2011/03/paper.pdf)).

For Fyralis, graph signals are unusually rich—shared repositories, calendar attendance, reporting relationships, Slack channels, assignments, ownership, and commitment participation—but most express association, not identity. Graph evidence belongs in a bounded candidate subgraph and should usually adjust a pair score or trigger contradiction review. Connected components over thresholded weak edges are not a safe merge algorithm.

### 3.5 LLM-based matching is an augmentation, not a foundation

Recent LLM ER studies show useful zero/few-shot reasoning, but also prompt sensitivity, cost, and global-consistency limitations. Match–Compare–Select improves over asking independent binary questions by comparing candidates, while noting that pairwise decisions ignore global consistency ([paper](https://arxiv.org/abs/2405.16884)). A 2025 EDBT study found material prompt effects and no universally best prompt for LLM entity matching ([paper](https://www.openproceedings.org/2025/conf/edbt/paper-81.pdf)). Structured output constrains syntax, not factual correctness: OpenAI explicitly notes that schema-conformant output can still contain wrong values ([Structured Outputs](https://openai.com/index/introducing-structured-outputs-in-the-api/)); Anthropic similarly offers grammar-constrained tool inputs and JSON output ([strict tool use](https://platform.claude.com/docs/en/agents-and-tools/tool-use/strict-tool-use)).

Accordingly, an LLM may compare `NBI` against three supplied, validated candidate profiles and return evidence fields. It may not return an arbitrary UUID and an operationally trusted probability. Its output must be validated, converted into model features or a bounded vote, calibrated on Fyralis labels, and subjected to deterministic constraints.

## 4. Research Findings

### 4.1 Placement in a streaming ingestion architecture

ER should occur at multiple stages because latency, available evidence, and reversibility differ by stage.

| Stage | Allowed work | Why it belongs here | What it must not do |
|---|---|---|---|
| Connector/pre-ingest | Preserve native IDs, source timestamps, account/workspace/installation scope, raw display values, and authoritative source links | Lost source scope cannot be reconstructed reliably later | Global identity inference or destructive deduplication |
| Canonicalization before persistence | Parse schema, normalize representations without discarding raw values, extract deterministic source identity keys | Makes persistence queryable and idempotent | Wait for LLM/ANN/network services |
| Persistence transaction | Persist observation, mentions/source identities, and confirmed exact annotations; enqueue resolution event atomically | Guarantees the observation survives ambiguity and replay | Broad search or canonical merge |
| Near-line asynchronous | Candidate generation, feature calculation, calibrated ranking, decision policy, alias learning, review creation | More context and compute are available without ingest latency | Unvalidated free-form identifiers or unlogged decisions |
| Background collective | Temporal reconciliation, graph consistency, duplicate cluster detection, merge/split proposals, index rebuilds | Uses accumulating history and can revisit old decisions | Silent rewriting of historical truth |
| Query/reasoning time | Read a selected identity projection and its confidence; optionally request on-demand re-resolution | Lets consumers choose as-of time and risk tolerance | Invent an identity differently for each query |

This is a stream/table duality: raw observations and append-only identity events are the durable log; current aliases, memberships, profiles, and caches are rebuildable projections. Stateful stream processors can maintain partitioned keyed state and recover it by checkpoint plus replay ([Flink state model](https://nightlies.apache.org/flink/flink-docs-stable/docs/concepts/stateful-stream-processing/)), but Fyralis should establish correct identity event semantics before adopting a distributed state runtime.

### 4.2 Signals and their relative value

Signal strength is conditional on entity type, source authority, time, and uniqueness. No global ordering is valid, but the following is a practical Fyralis hierarchy.

| Signal family | Examples | Typical value | Principal hazards |
|---|---|---|---|
| Authoritative source identity | Slack workspace + user ID; GitHub installation + node ID; HR system + employee ID; registry jurisdiction + company number | Very high within its declared scope | Tenant/scope omission, ID recycling, sandbox/production collisions, bad upstream mapping |
| Cross-source verified identifier | Verified corporate email, SCIM external ID, LEI, repository node ID, validated domain ownership | High | Shared mailboxes, email reassignment, aliases, subsidiaries, contractors, domain changes |
| Explicit human assertion | Curator says “same” or “different,” with effective time and scope | High | Reviewer mistake, stale decision, insufficient context, broad application of a narrow assertion |
| Composite lexical/attribute | Normalized name + domain + country; username + organization; system name + environment | High when jointly rare | Common names, transliteration, abbreviations, missingness, source formatting |
| Temporal compatibility | Attribute validity overlap, employment interval, repository lifetime, event time | Often a veto or strong modifier | Late data, imprecise dates, legitimate concurrent roles |
| Organizational context | Tenant, legal hierarchy, customer/vendor relationship, team, environment, source account | Strong candidate gate | Organizations reorganize; one person can cross boundaries |
| Relationship topology | Reports to, works with, owns, assigned, commits to, attends, channel/repo membership | Good disambiguator for hard cases | Hubs, bots, public channels, circular inference, copied data |
| Conversation/document context | Nearby resolved mentions, thread, sender, project vocabulary | Good mention linker | Context can discuss an external namesake; prompt injection if sent to LLM |
| Behavioral history | Login/commit rhythm, interaction patterns, authored systems | Supporting evidence | Privacy, fairness, shift over time; similarity is not identity |
| Lexical similarity | Edit distance, token/Jaro-Winkler, acronyms, transliteration, character n-grams | Essential recall feature | Short-name collisions, common suffixes, multilingual loss |
| Semantic embeddings | Mention plus context vs. entity profile; multimodal document/image representations | Broad recall and zero-shot semantics | Semantic relatedness mistaken for identity, drift, opaque neighborhoods |
| Source reliability and freshness | Connector trust tier, field provenance, observation recency, extraction method | Essential weight/modifier | Must be learned per field and use case, not one source-wide scalar |

Strong identifiers should be normalized conservatively and stored in raw and canonical forms. The SCIM specification is a useful model: the service-issued `id` is stable, while `externalId` is explicitly scoped to the provisioning domain; its privacy guidance recommends tenant/client-specific identifiers to prevent unintended correlation ([RFC 7643](https://www.rfc-editor.org/info/rfc7643/)). Legal organizations require jurisdiction and registry identity, not name alone; the LEI standard includes official register and registry identifier among core identifying attributes ([GLEIF](https://www.gleif.org/content/4_lei-data/1_access-and-use-lei-data/2_level-1-data-lei-cdf-3-1-format/lei-cdf_version_3.1-documentation.html)).

Multimodal signals can help resolve scanned documents, logos, screenshots, and slide decks, but should be decomposed into attributable evidence—OCR text, document metadata, visual identifier, surrounding entities—rather than one uninspectable multimodal similarity score. Their highest near-term value is mention extraction and candidate recall, not authoritative merge.

### 4.3 Blocking and candidate generation

An exhaustive comparison is quadratic: \(n(n-1)/2\) for deduplication or \(nm\) for two catalogs. Blocking produces a candidate set \(C\) much smaller than all possible pairs \(P\). It must be evaluated separately from matching:

- pair completeness \(PC=|C\cap M|/|M|\), the recall of true pairs;
- pair quality \(PQ=|C\cap M|/|C|\), the precision of the candidate set;
- reduction ratio \(RR=1-|C|/|P|\).

The blocker is a recall system. A perfect matcher cannot recover a true entity that never enters its candidate set. Splink describes blocking as the main accuracy/performance trade-off and unions multiple rules for prediction ([blocking guide](https://moj-analytical-services.github.io/splink/demos/tutorials/03_Blocking.html)); modern supervised contrastive blocking experiments commonly hold pair completeness near 99.5% before comparing downstream matchers ([SC-Block](https://arxiv.org/abs/2303.03132)).

Fyralis should use a **typed union of bounded lanes**:

| Lane | Mechanism | Best use | Operational control |
|---|---|---|---|
| Source-key | Exact tenant + connector installation/account + native ID | Actor, repository, document, CRM/HR records | Unique index; authority and reassignment policy |
| Normalized exact/composite | Exact normalized email/domain/name; type-specific composite keys | Precision and cheap repeated aliases | Versioned normalizer; stop-word and suffix policy by type/language |
| Lexical inverted index | pg_trgm/BM25, character n-grams, token overlap, acronym expansion | Misspellings, aliases, company/system names | Top-k per field and minimum score |
| Sorted neighborhood | Sort on several keys and compare within a moving window | Batch backfills with moderately dirty keys | Multiple passes and bounded window |
| Phonetic/transliteration | Double Metaphone or locale-specific phonetics; transliterated keys | People and multilingual names | Locale/type gating; never final evidence alone |
| MinHash/LSH | Approximate Jaccard over token/character shingles | Long names, addresses, documents, catalog descriptions | Banding parameters; high-recall ensemble |
| Canopy clustering | Cheap loose metric creates overlapping canopies, expensive scoring inside | Batch, high-dimensional text | Loose/tight thresholds; overlap caps |
| ANN | HNSW initially; IVF/product quantization at larger scale | Mention/entity profile embedding retrieval | Filter tenant/type/time first; top-k and model-version indexes |
| Graph neighborhood | Shared rare neighbor, two-hop typed paths, source-local co-membership | Ambiguous people, teams, repos, systems | Degree correction, relation allowlist, bounded expansion |
| Temporal/organizational | Validity overlap, tenant/org/environment, legal jurisdiction | Eligibility filter and contradiction veto | Bitemporal query, explicit unknown handling |

MinHash originates from estimating resemblance of sets ([Broder](https://www.cs.princeton.edu/courses/archive/spr05/cos598E/bib/broder97resemblance.pdf)); HNSW provides efficient navigable small-world ANN search ([paper](https://arxiv.org/abs/1603.09320)); FAISS offers exact and approximate dense-vector indexes at scale ([Meta engineering](https://engineering.fb.com/2017/03/29/data-infrastructure/faiss-a-library-for-efficient-similarity-search/)). None provides an identity decision by itself.

Candidate explosion is controlled by mandatory tenant and type partitioning, time eligibility, per-lane top-k, per-mention total budgets, deduplication across lanes, common-value suppression, heavy-block splitting, and progressive widening. A typical request first checks authoritative IDs and exact aliases, then lexical/composite lanes, then ANN, and only invokes graph or expensive rerankers if top candidates remain close. Every response records which lanes retrieved each candidate. “No candidate” is observable blocker failure, not matcher certainty.

### 4.4 Confidence calibration and uncertainty

Fyralis must distinguish six values that are commonly—and incorrectly—called confidence:

1. a raw model score;
2. a calibrated pair probability on a specified population;
3. candidate-set uncertainty (top score, runner-up margin, entropy, and whether the true entity may be absent);
4. evidence reliability and freshness;
5. cluster compatibility after constraints;
6. decision confidence under a versioned cost policy.

Neural scores are often poorly calibrated; temperature scaling can substantially improve calibration without changing ranking ([Guo et al.](https://proceedings.mlr.press/v70/guo17a.html)). Calibration must be evaluated by entity type, source family, language, and novelty cohort using reliability plots, Brier/log loss, expected calibration error, and risk–coverage curves. A probability calibrated on blocked hard candidates is not automatically a population probability; candidate sampling and class priors must be documented.

Recommended decision record:

```text
raw_score
calibrated_p_match
calibration_model_version + cohort
candidate_rank + top_two_margin
retrieval_lanes and retrieval_scores
positive_evidence[] + contradiction_evidence[]
source_reliability + temporal_compatibility
constraint_result
decision_policy_version + action
```

Automatic linking requires more than `p > threshold`: the candidate must exist and be tenant/type eligible; no hard contradiction may fire; the top-two margin must be adequate; and the accepted risk for that action/type must be met. Thresholds should be selected to achieve a measured false-merge target—for example a very high precision target for people and customers—rather than copied as `0.8`. Medium-confidence cases go to review, plausible novel cases become provisional identities, and low-information cases remain unresolved. Conformal prediction is promising for calibrated candidate sets, but its exchangeability assumptions and subgroup coverage must be monitored; it is an optional later improvement, not a prerequisite.

Uncertainty should persist in the identity graph, but not as `SAME_AS(probability)` with unrestricted transitive semantics. Store scored candidate edges and versioned decisions separately. Downstream queries default to accepted current membership while privileged/debug views can inspect alternatives. OWL `sameAs` means true identity, not approximate similarity ([W3C OWL](https://www.w3.org/TR/owl-ref/)); Fyralis should use explicit `candidate_for`, `resolved_to`, `different_from`, `merged_into`, and `superseded_by` semantics.

### 4.5 Incremental and temporal ER

Incremental ER must update only affected neighborhoods while remaining reproducible. A new fragment creates or changes blocking keys, candidates, evidence edges, and perhaps one or more cluster memberships. Its impact frontier includes the new fragment, retrieved candidates, their current cluster members, and clusters linked by newly introduced contradictions. Incremental approaches reduce work by exploiting document references, LSH/blocking, and affected subgraphs rather than rerunning the entire corpus ([incremental linked-document ER](https://arxiv.org/abs/1402.4417), [incremental blocking](https://researchportal.tuni.fi/en/publications/incremental-blocking-for-entity-resolution-over-web-streaming-dat/)). Periodic full or sampled audits remain necessary to catch accumulated path dependence.

Time has two axes:

- **valid/event time:** when the identity assertion was true in the organization;
- **system/transaction time:** when Fyralis learned, corrected, or superseded it.

Every source identity, alias assertion, relationship, profile attribute, membership decision, merge, and split should therefore have `valid_from/valid_to` and `recorded_at/superseded_at` (or equivalent range types). Event-time processing produces reproducible results despite late arrival; processing-time-only systems smear present identity onto old events ([Flink event-time rationale](https://nightlies.apache.org/flink/flink-docs-stable/docs/learn-flink/streaming_analytics/)). A 2026 Google publication describes “state smearing” at two-billion-fragment daily scale and uses temporal graph resolution plus checkpoints to support historical reconstruction ([Google temporal ER](https://research.google/pubs/operationalized-temporal-entity-resolution-at-scale-a-2b-entity-architecture-for-cloud-native/)).

A rename normally extends one entity with a new alias interval. A new job changes a person's employment/role relation, not their identity. A company legal merger creates an organizational lineage event and may or may not collapse operational customer accounts. Reassigned emails and usernames close one source-identity binding before another begins. Unknown dates remain intervals or uncertainty, never fabricated points.

Merge and split must be events:

- A **merge** selects a surviving identity envelope or creates a new parent, closes prior current memberships, opens new versioned memberships, records all evidence and decision policy, and emits a repair event. Loser IDs remain resolvable through redirects and lineage.
- A **split** does not erase the prior decision. It records the contradiction, closes affected membership versions, partitions the evidence graph under hard negative constraints, assigns new/current identities, and emits downstream repair/recompute events. Historical “as believed then” queries still reproduce the old projection.

### 4.6 Organization identity is not generic customer matching

Fyralis must model several identity boundaries explicitly:

- **Person vs. account vs. role:** an employee may have multiple accounts; a shared/bot account may have multiple operators; employee, contractor, and customer-contact roles change over time.
- **Organization vs. brand vs. account:** a legal entity, trading name, parent group, subsidiary, CRM account, and Fyralis customer resource may be related without being identical.
- **System vs. service vs. deployment:** “payments” may denote a business capability, service, repository, runtime deployment, or team. Environment and lifecycle matter.
- **Repository vs. project/initiative:** repositories can be renamed, transferred, archived, forked, or shared by initiatives; project language is often informal.
- **Team vs. channel:** membership and channel names are evidence, not identity. Reorganizations create successor teams.
- **Document vs. version:** storage-provider object IDs, copies, revisions, near-duplicate content, and attachments require content lineage separate from identity.
- **Commitment/goal/decision:** semantically similar statements can be distinct commitments with different owners, due dates, customers, and validity. Embedding similarity must not deduplicate obligations.

Identity policies and models must therefore be type-specific. A globally uniform “name similarity” model would systematically conflate association with sameness.

### 4.7 Human-in-the-loop and continuous learning

Review should optimize expected information and organizational harm, not merely surface all scores between two constants. Queue priority should combine uncertainty, predicted downstream impact, novelty, cluster size, contradiction severity, recurrence, and reviewer cost. CrowdER and subsequent work show the value of using machines to narrow candidates and humans for ambiguous pairs rather than crowd-checking the Cartesian product ([CrowdER](https://arxiv.org/abs/1208.1927)).

A review UI must show raw source facts, time, candidate comparisons, positive and conflicting evidence, affected cluster members, predicted downstream impact, and the exact action. Supported answers include `same`, `different`, `choose candidate`, `new entity`, `not an entity`, `valid only during interval`, `merge`, and `split`. Each answer produces:

- a durable label and reviewer/audit metadata;
- an alias or source-identity assertion when applicable;
- a hard negative constraint for `different`;
- a corrected membership/lineage event;
- replayable repair notifications;
- a training example placed in a held-out-aware dataset.

Feedback must not update a production model online after one action. Labels are quality-checked, deduplicated, weighted by expertise and provenance, split by time/entity/source to prevent leakage, used to train a candidate model, calibrated, compared in shadow mode, and deployed under a new version. Active learning should sample high-utility cases while retaining random and common-case samples so evaluation is not biased to the review distribution.

### 4.8 Scalability and operational evaluation

At millions of observations and aliases, the central scale controls are append-only ingestion, indexed exact fast paths, bounded candidate sets, tenant/type sharding, incremental feature/profile updates, batched model inference, idempotent events, and backpressure. Expensive models run only on uncertain top-k candidates. Large tenants can be partitioned further by entity type and stable block hashes; hot/common keys are isolated and sampled rather than broadcast. ANN and lexical indexes are versioned and rebuilt side-by-side. Cross-tenant retrieval is prohibited at the query plan and key level, not filtered after search.

Evaluation must cover the whole pipeline:

| Layer | Metrics |
|---|---|
| Mention extraction | Span/type precision, recall, F1; missed-mention rate by source/language/type |
| Blocking | Pair completeness, pair quality, reduction ratio, candidate count p50/p95/p99, no-candidate rate, per-lane marginal recall |
| Pair ranking | Precision/recall/PR-AUC on realistic hard candidates; top-k recall; MRR; `NIL` precision/recall |
| Calibration/abstention | Brier score, log loss, ECE/reliability, risk vs. coverage, review yield, false-merge risk at auto-link threshold |
| Clustering | Pairwise precision/recall/F1; B-cubed; CEAF; MUC; split/lump errors; cluster-size-stratified metrics |
| Organizational slices | Actor/customer/system/repository/project/document/commitment; source pair; language; tenure; common names; contractors/bots/shared accounts; head vs. long tail |
| Temporal | As-of accuracy, alias-interval accuracy, reassignment/reorg cases, late-event corrections, historical replay equivalence |
| Operational | Ingest added latency, resolution freshness, queue age, worker throughput, retry/DLQ rates, candidate fanout, index freshness, cost per resolved mention, cache hit rate |
| Correction impact | Merge/split rate, contested decisions, time to correction, observations/models repaired, rollback success, repeated-review rate |

Pairwise metrics overweight large clusters and can hide singleton behavior. B-cubed computes precision and recall per item from its predicted and reference cluster; CEAF aligns predicted and reference clusters one-to-one; MUC measures the links needed to connect clusters and is insensitive to some singleton errors. No one metric is sufficient. CEAF's constrained alignment is defined as a maximum bipartite matching ([Luo](https://aclanthology.org/H05-1004/)); MUC's model-theoretic link score is documented by Vilain et al. ([paper](https://aclanthology.org/M95-1005.pdf)). Modern ER evaluation recommends entity-centric samples and joint pair/cluster error analysis because true matches are needles in an enormous non-match population ([Binette et al.](https://arxiv.org/abs/2404.05622)).

Ground truth should combine source-authoritative IDs, adjudicated difficult clusters, temporal scenario suites, synthetic corruptions, and sampled production reviews. Splits must be by identity and time, not random pairs, to prevent aliases of the same entity leaking across train and test. Every release needs regression gates on both aggregate and high-risk slices.

## 5. Production System Patterns

Public evidence varies sharply. Some entries below document deployed ER systems; others are product capabilities or research architectures from the named company. The distinction matters—an attractive quickstart is not proof of a production identity service.

| System | Publicly documented pattern | Implication for Fyralis |
|---|---|---|
| Google | Knowledge Vault fused noisy web extractions and prior knowledge with supervised models that produced calibrated fact-correctness probabilities ([paper](https://research.google/pubs/knowledge-vault-a-web-scale-approach-to-probabilistic-knowledge-fusion/)). Google's collective EL research uses selective relational coherence, and its 2026 temporal ER paper uses deterministic dynamic graph resolution and validity checkpoints at very large scale. | Separate identity linkage from fact fusion; retain probabilities/provenance; make temporal state a first-class graph property. Public work should not be read as the private Google Knowledge Graph's full architecture. |
| Microsoft | A deployed web-scale entity linker explicitly models out-of-KB entities and sparse tail signals, reporting a high-precision operating point ([WSDM paper](https://www.microsoft.com/en-us/research/publication/entity-linking-at-the-tail-sparse-signals-unknown-entities-and-phrase-models/)). Other production research enforces global one-to-one constraints ([paper](https://www.microsoft.com/en-us/research/wp-content/uploads/2011/08/msr-report-1to1.pdf)). Dynamics Customer Insights uses per-table deduplication, ordered cross-table rules, normalization/fuzzy conditions, winner policies, aliases, and always/never overrides ([matching rules](https://learn.microsoft.com/en-us/dynamics365/customer-insights/data/data-unification-match-tables)). | Model `NIL`; use global/domain constraints after pair scoring; support explicit exceptions and deterministic source precedence. |
| Meta | Graph Search described high-confidence text segments being sent to a retrieval/ranking service for entity resolution ([engineering post](https://engineering.fb.com/2013/04/29/web/under-the-hood-the-natural-language-interface-of-graph-search/)). BLINK demonstrates bi-encoder retrieval, FAISS, and cross-encoder reranking. FAISS separates vector search infrastructure from identity policy. | Adopt retrieve/rerank as a lane; do not infer that Meta uses BLINK unchanged in production or that vector proximity is a merge decision. |
| LinkedIn | Organization ER exposes online and Spark/offline modes and decomposes requests into handling, inverted-index candidate retrieval, feature computation, and candidate ranking. Name and website are strongest, with industry/location/phone for disambiguation; ML and business heuristics are combined ([engineering post](https://www.linkedin.com/blog/engineering/economic-graph/matching-external-companies-to-linkedin-s-economic-graph-at-scal)). LinkedIn's knowledge-graph work uses canonical names, synonyms, co-occurrence vectors, clustering, external sources, and human taxonomists ([post](https://www.linkedin.com/blog/engineering/knowledge/building-the-linkedin-knowledge-graph)). | This is the closest public organizational pattern: typed features, online/offline parity, separate retrieval/ranking, graph context, and human curation. |
| Palantir | Foundry distinguishes the data layer from an ontology of objects, links, properties, and actions; actions write decisions back into operational workflows ([concepts](https://www.palantir.com/docs/foundry/getting-started/introductory-concepts)). Public material emphasizes provenance and object/action governance but does not publish a general matcher architecture. | Keep source data and operational identity objects separate; make human/system changes explicit actions with provenance. Do not claim undocumented Palantir matching internals. |
| Snowflake | Cortex Search supports high-throughput batch retrieval for ER/deduplication ([docs](https://docs.snowflake.com/en/user-guide/snowflake-cortex/cortex-search/batch-cortex-search)). Snowflake quickstarts combine embeddings, vector similarity, AI classification/LLM validation, review apps, and audit tables ([hybrid guide](https://www.snowflake.com/en/developers/guides/data-harmonization/)). | Useful platform recipe for retrieval and review, not a turnkey identity ledger. Treat LLM validation and reported demo accuracy cautiously. |
| Databricks | Its AML reference uses Splink blocking and probabilistic match scores, persists resolved output in Delta, and emphasizes avoiding all-pairs comparison ([notebook](https://www.databricks.com/notebooks/aml/03_aml_entity_resolution.html)). | Strong batch/backfill and model-audit pattern. Streaming identity semantics and merges/splits still belong to the application. |
| OpenAlex | OpenAlex publicly reports deterministic name parsing, 106.7M author embeddings, 718M authorship similarities, and splitting roughly 3.2M overmerged profiles using ORCID conflicts; it also warns that treating ORCID as perfect allowed a minority of errors to spread ([2026 update](https://blog.openalex.org/category/uncategorized/)). | Authoritative IDs are high-weight evidence, not infallible truth. Contradiction-driven splits and curator corrections are production necessities. |
| OpenCorporates | Its reconciliation API maps names to legal entities; search uses prior names, normalization of punctuation/stop words/company types, jurisdiction, identifiers, and provenance/confidence ([API reference](https://api.opencorporates.com/documentation/API-Reference?source=post_page---------------------------), [reconciliation API](https://api.opencorporates.com/documentation/Open-Refine-Reconciliation-API)). | Organization resolution must prefer jurisdiction + registry number and preserve alternative names and provenance. A search score is a candidate score, not a guaranteed match. |
| Neo4j | Neo4j models identifiers and behavior as graph nodes/edges, uses deterministic queries, node similarity or supervised link prediction, then derives resolved IDs from connected components ([GDS overview](https://neo4j.com/blog/graph-data-science/graph-data-science-use-cases-entity-resolution/), [supervised example](https://neo4j.com/blog/developer/exploring-supervised-entity-resolution-in-neo4j/)). | Graph projections are useful, but naive weakly connected components require strong accepted edges and contradiction controls. A graph database is optional infrastructure, not the algorithm. |
| Elastic / Elasticsearch | Elastic Security creates resolution groups with a primary entity and aliases; automatic matching currently uses shared email and manual links override automation. Its own docs warn shared-email automation can produce false positives ([docs](https://www.elastic.co/docs/solutions/security/advanced-entity-analytics/entity-resolution)). | Primary/alias grouping and manual override are practical; the warning illustrates why one shared identifier cannot be universal identity proof. |
| AWS | AWS Entity Resolution offers rule-based exact/fuzzy, ML, and provider workflows, emits Match IDs, supports staged workflows and automatic incremental rule matching, and exposes consistent vs. eventual Match-ID processing ([workflow docs](https://docs.aws.amazon.com/entityresolution/latest/userguide/create-matching-workflow.html), [service overview](https://docs.aws.amazon.com/entityresolution/latest/userguide/what-is-service.html)). | Separate match workflow, stable ID, output, cadence, and consistency choice. Fyralis needs richer temporal/graph semantics than the managed batch abstraction exposes. |
| Azure / Dynamics | Customer Insights ingests first, deduplicates within each table, then matches tables in priority order and creates unified profiles. Changed source/rules can split a previously unified profile; Delta inputs enable incremental unification ([overview](https://learn.microsoft.com/en-us/dynamics365/customer-insights/data/data-unification), [data sources](https://learn.microsoft.com/en-us/dynamics365/customer-insights/data/data-sources)). | Source-local dedupe before cross-source linking, explicit precedence, and split-on-recompute are useful patterns, though Fyralis requires finer event-time lineage. |
| OpenAI | OpenAI publishes embeddings for search/clustering and schema-constrained outputs, not a public production ER product architecture ([embeddings](https://openai.com/index/introducing-text-and-code-embeddings/), [Structured Outputs](https://openai.com/index/introducing-structured-outputs-in-the-api/)). | OpenAI models may implement semantic candidate and bounded extraction/reranking components. Schema validity does not validate entity identity. |
| Anthropic | Anthropic publishes strict JSON/tool schemas and explicitly states that it does not offer its own embedding model ([tool docs](https://platform.claude.com/docs/en/agents-and-tools/tool-use/overview), [embedding docs](https://platform.claude.com/docs/en/build-with-claude/embeddings)). No public Anthropic ER architecture was found. | Claude may be a provider-neutral bounded adjudication/explanation component. Vendor API features are not an identity architecture. |
| Academic systems | Magellan treats ER as an end-to-end workflow with data exploration, blocking, matching, debugging, and iteration rather than a single classifier ([paper](https://www.vldb.org/pvldb/vol9/p1581-konda.pdf)). Swoosh formalizes generic match/merge operators; DeepMatcher and Ditto advance pair models; Splink operationalizes probabilistic linkage; Bayesian work represents assignment/partition uncertainty. | The durable lesson is system decomposition, explicit assumptions, and evaluation at blocker, pair, cluster, and workflow levels. |

The cross-system pattern is consistent: preserve source identity, reduce candidates before expensive scoring, combine deterministic and learned evidence, explicitly represent unknowns, provide stable match identifiers, separate online from broader offline processing, and retain human or rule overrides. No credible system supports unrestricted LLM-to-canonical-merge as the primary architecture.

## 6. Evaluation of Fyralis's Current Design

This assessment is based on the repository at the report date. Code paths, not earlier design intent, are treated as authoritative.

### 6.1 Current flow

```text
Connector / gateway
        |
        v
ObservationDraft -- connector entities_hint -----------------------+
        |                                                          |
        +-- actor source ref --> actor_identity_mappings exact -----+-- resolved refs
        |                         miss -> unresolved actor ref       |
        |                                                          |
        +-- content_text --> ASCII 1/2/3-grams (max 50)             |
                              |                                    |
                              +--> tenant exact alias batch --------+
                              |       ambiguity => no answer        |
                              +--> capital/hyphen heuristic          |
                                      -> unresolved phrases          |
        |                                                          |
        v                                                          v
persist Observation + entities_mentioned + unresolved metadata + enqueue T1
        |
        +--> actor_identity clarification (on miss)
        |
        +--> entity resolver worker (unresolved phrase)
                 |
                 +--> recent channel observations / models / aliases
                 +--> LLM returns arbitrary canonical_ref,
                 |    self-reported confidence, reasoning
                 |
                 +--> >0.8: alias + mutate observation annotation
                 |          + state_change + selected T1 re-enqueue
                 +--> 0.5..0.8: review queue + clarification
                 +--> lower/null: remove phrase and drop
```

During ingestion, actor and phrase resolution run before the observation insert (`services/ingest/ingestion/core.py:384-409`). Actor resolution prefixes a missing channel, then performs an exact source-reference lookup (`core.py:432-448`). Phrase generation is a case-preserving ASCII tokenizer that produces one-, two-, and three-word n-grams, capped at 50 (`core.py:76-118`). Connector-provided `entities_hint` values are accepted first; a single batched alias query adds unambiguous refs; capitalized or hyphenated misses become `_unresolved_phrases` (`core.py:451-473`, `core.py:793-804`).

The alias repository uses Python Unicode `casefold` plus whitespace collapse at its API boundary, while SQL lookup uses `lower` plus `regexp_replace`. It returns a ref only when every row for a normalized alias points to the same JSONB value (`services/domain/entity_aliases/repo.py:68-82`, `101-204`). `entity_aliases` is tenant-scoped and already has text, GIN, and HNSW indexes plus confidence, use/contest counts, timestamps, source observation, and optional actor (`db/migrations/0001_foundation.sql:341-366`); a later functional index supports normalized lookup (`0060_entity_aliases_normalized_index.sql:49-53`).

The asynchronous worker sends up to 20 previous same-channel observations, recent active models, exact prior aliases, and a lexical ranking of at most 200 high-confidence/popular aliases to the LLM (`services/workers/entity_resolver/context.py:125-285`). It does not query the alias HNSW index. The output schema permits any JSON object as `canonical_ref`; the prompt says not to invent IDs but no referential or type validation enforces that instruction (`services/workers/entity_resolver/worker.py:90-99`, `338-366`). Its hard-coded policy is automatic resolution above `0.8`, review from `0.5` through `0.8`, and drop otherwise (`worker.py:255-332`). Automatic resolution inserts/uses an alias, appends the ref to the source observation, emits `entity_late_resolution`, and re-enqueues T1 only for customer, commitment, or goal (`worker.py:394-442`).

Medium cases create a durable `entity_review_queue` row and clarification. The answer path can accept, reject, or create an actor/resource/customer/vendor/system/workstream/commitment; acceptance inserts a manual alias, marks the review, updates the observation annotation, emits a state change, and may enqueue reasoning (`services/app/gateway/clarifications_router.py:540-740`). This is a real closed loop for phrase resolution.

Actor identity is a parallel but incomplete path. `actor_identity_mappings` has primary key `(source_channel, source_actor_ref)` and no `tenant_id` (`db/migrations/0001_foundation.sql:40-48`). Its exact repository lookup is therefore global. Ingestion opens an `actor_identity` clarification with map/create/ignore actions (`core.py:646-724`), but the side-effect dispatcher handles only `substrate_candidate_resolution` and `entity_resolution` (`clarifications_router.py:514-548`); no handler applies the actor actions.

Substrate promotion is adjacent open-world entity creation, not merely alias matching. Think deterministically extracts and upserts typed candidates, requests clarification when needed, or auto-promotes (`services/reasoning/think/substrate_builder.py:258-352`). Promotion creates actors, mappings, resources or commitments, inserts entity aliases, and backfills model scopes; default promotion floors are `0.72` and `0.78` for commitments (`services/domain/substrate_promotion.py:1145-1175`). This path should be unified with the identity ledger while retaining its domain-specific creation policies.

### 6.2 What should be kept

- **Non-blocking persistence:** observations are not lost because identity is uncertain. This is the most important architectural property.
- **A small deterministic hot path:** a batched indexed lookup avoids remote calls and bounds ingest latency.
- **Tenant-scoped phrase aliases:** aliases are organizational vocabulary and should remain tenant-local by default.
- **Ambiguity abstention:** multiple refs for the same normalized phrase produce no fast-path answer.
- **Connector hints:** authoritative structured connectors often know more than text extraction, provided hints are validated and provenance is recorded.
- **Asynchronous late resolution:** new evidence can enrich already persisted observations and request downstream recomputation.
- **Alias learning and human clarification:** repeated language becomes cheap and reviewer actions can correct the long tail.
- **Rate limiting, retry/backoff, and production worker wiring:** cost and failure isolation are already considered.
- **Substrate candidates:** open-world discovery is necessary because an Organizational Perception Engine must recognize entities that do not yet have domain rows.

### 6.3 Findings and severity

| Severity | Finding | Consequence | Required disposition |
|---|---|---|---|
| Critical | Actor source mappings are not tenant-scoped in schema or lookup | The same connector/source ref can resolve across tenants or collide globally; it violates identity isolation | Add tenant and connector-account/installation scope to the key and all reads/writes; backfill and audit collisions before enabling automatic actor linking |
| Critical | LLM output can name an arbitrary JSONB ref, and self-reported `confidence > 0.8` authorizes mutation | Hallucinated, cross-type, nonexistent, or wrong refs can become learned aliases and affect reasoning | Restrict to a server-generated candidate ID enum, validate registry existence/tenant/type, remove self-confidence as policy input, and require calibrated evidence |
| High | Actor clarification actions have no side-effect handler | Users can answer a clarification that does not repair mapping or observation state | Implement and test map/create/ignore action semantics in the identity workflow before relying on the UI |
| High | A unique alias resolves regardless of its confidence, contested count, provenance, status, or time | One low-quality stale alias becomes deterministic truth on every later observation | Fast path must query only active confirmed assertions meeting type-specific policy and event-time validity; contradictions veto it |
| High | Canonical refs are unconstrained polymorphic JSONB | Dangling IDs, invalid types, and cross-tenant refs cannot be prevented by a foreign key | Introduce a tenant-scoped entity registry and reference its ID from resolution tables; domain objects remain typed payloads |
| High | No mention/source-identity/evidence/decision/membership separation | It is impossible to explain or replay why a link exists, evaluate candidates, or safely undo a merge | Add an append-only identity ledger and versioned projections |
| High | No merge, split, negative link, or lineage semantics | Transitive duplicates accumulate; corrections require ad hoc rewrites and cannot preserve historical belief | Add merge/split events, `different_from` constraints, versioned memberships, redirects, and repair events |
| High | Phrase extraction is ASCII, case-biased, 1–3 grams, and capped before linguistic prioritization | Lowercase systems, long organizations, non-Latin names, handles, emails, and later-document mentions are missed; early tokens crowd out later ones | Replace as the long-tail extractor with Unicode-aware NER/structured parsers; retain bounded exact matching over connector-provided and extracted mentions |
| High | Worker retrieves top popular aliases then lexically ranks in process; HNSW is unused | Recall degrades with millions of aliases and biases toward popular entities; database work grows | Implement typed lexical and ANN candidate services with per-lane top-k and measured pair completeness |
| High | Worker confidence is neither calibrated nor decomposed | `0.8` has no empirical false-merge meaning, and confidence cannot be compared across sources/types/models | Store raw features/scores, calibrate per cohort, use risk-based policy versions and margin/constraint checks |
| Medium | Python `casefold` and PostgreSQL `lower` are not identical normalizers | Non-ASCII lookup can silently disagree with index/documented invariants | Materialize a versioned normalized form from one implementation or use compatible database collation/function and migration tests over multilingual cases |
| Medium | `UNIQUE (tenant_id, alias_text, actor_id)` does not prevent duplicate rows when `actor_id IS NULL` in PostgreSQL | Duplicate alias assertions and inconsistent update behavior are possible | Make identity assertion uniqueness explicit with null-safe constraints and include normalized text, target, scope, and validity semantics |
| Medium | Low/null LLM results are removed from unresolved phrases | A transient weak context becomes permanent drop and will not benefit from new entities/evidence | Persist `UNRESOLVED/NO_MATCH/NOT_ENTITY` as distinct versioned decisions with reconsideration triggers |
| Medium | Late resolution mutates `observations.entities_mentioned` | The observation row mixes source-canonical facts with current derived belief; historical reconstruction is difficult | Preserve connector hints/source annotations immutably and put ER membership in a versioned projection; retain state-change compatibility during migration |
| Medium | Graph evidence, temporal validity, source reliability, and organization constraints are absent from scoring | Common-name and lifecycle ambiguity remains unresolved or is guessed by the LLM | Add explicit typed features and hard constraint evaluation before collective refinement |
| Medium | Automatic alias insertion, observation update, state change, and trigger are orchestrated across repository calls | Partial failures can leave projections inconsistent unless every path shares a transaction/idempotency contract | Drive application from an idempotent identity decision event and reconciliation worker; transactionally update local projections/outbox |
| Medium | Two open-world paths use unrelated thresholds and representations | Substrate promotion and phrase ER can create or resolve the same concept inconsistently | Put both behind one registry, evidence ledger, and decision policy while keeping separate type-specific creation rules |

### 6.4 Overall assessment

The implementation is a credible Wave-2 foundation: it learned the right operational lesson that ER cannot block ingestion. It should not be discarded. Its core abstraction, however, is still “phrase → JSON ref,” whereas Fyralis now needs “evidence over time → versioned organizational identity decision.” The recommended design is therefore an evolutionary replacement of the decision and data model around the existing flow, not a rewrite of connectors or observation ingestion.

## 7. Recommended Architecture

### 7.1 Logical architecture

```text
 Sources / connectors
 (native IDs, raw values, source time, account/installation scope)
                         |
                         v
 +---------------- Canonical ingest transaction ----------------+
 | validate + normalize + extract structured source identities   |
 | persist immutable Observation + source annotations/mentions   |
 | confirmed-ID/alias fast path -> provisional resolution refs   |
 | transactional outbox -> identity.fragment.created             |
 +---------------------------------------------------------------+
              |                                      |
              v                                      v
     Observation store                       Exact identity cache
              |                              (tenant/type/version)
              +------------------+-------------------+
                                 |
                                 v
                 +------- Candidate service --------+
                 | source key / exact / trigram-BM25 |
                 | phonetic-MinHash / ANN / temporal |
                 | graph neighborhood (bounded)      |
                 +----------------+------------------+
                                  | top-k + provenance
                                  v
                 +--------- Feature service ---------+
                 | pair + source + time + org + graph|
                 | contradictions / cluster profile  |
                 +----------------+------------------+
                                  v
                 +---------- Scoring cascade --------+
                 | hard rules -> cheap GBT/logistic  |
                 | -> cross-encoder -> bounded LLM*  |
                 | -> calibrated probability         |
                 +----------------+------------------+
                                  v
                 +---------- Decision policy --------+
                 | LINK | REVIEW | PROVISIONAL/NEW    |
                 | NO_MATCH | NOT_ENTITY | CONTESTED  |
                 +---------+-----------+-------------+
                           |           |
                 accepted  |           | uncertain/high impact
                           v           v
                    Identity ledger   Review/clarification
                           ^           |
                           +-----------+ human labels/constraints
                           |
             +-------------+-----------------------------+
             | current projections / canonical registry |
             | aliases, memberships, redirects, profiles|
             +--------+------------------+---------------+
                      |                  |
                      v                  v
              reasoning triggers   graph/temporal audit workers
              repair/replay events merge/split proposals + calibration

 * LLM is optional and sees only validated, bounded candidates.
```

The architecture has one authoritative write surface: the identity ledger. Match services propose decisions; a policy service validates and commits them. Current aliases, memberships, entity profiles, observation annotations, graph views, and caches are projections. This prevents an LLM, index, or reviewer UI from silently becoming a second source of truth.

### 7.2 End-to-end pipeline and contracts

1. **Connector preservation.** Emit `tenant_id`, connector definition/installation/account ID, native entity ID and kind, event time, raw display values, and source evidence. Stable source identity is `(tenant, connector_instance, source_kind, native_id)`, not a concatenated channel string.
2. **Normalization and mention extraction.** Store raw and versioned normalized values. Extract emails, handles, URLs, repo coordinates, ticket/project keys, registry identifiers, and Unicode mention spans. Connector hints carry `assertion_kind`, provenance, and authority; they are not bare JSON refs.
3. **Immutable persistence.** Write the observation, source-annotation rows, mention rows, and an outbox event in one transaction. The observation content is never rewritten to represent a later belief.
4. **Synchronous deterministic annotation.** Consult only tenant/type/source-scoped current mappings and aliases that satisfy the fast-path policy at the observation's event time. Return a decision ID and identity version, not just a target ref. A miss is normal.
5. **Candidate generation.** Union bounded typed lanes, validate target registry rows, deduplicate candidates, and record retrieval provenance. A candidate response has a deterministic request/input version and deadline.
6. **Feature materialization.** Compute field agreements, rarity, missingness, source authority, temporal overlap, entity-profile compatibility, and bounded graph features. Store a feature snapshot or reproducible feature references.
7. **Scoring cascade.** Apply hard allow/veto rules; a cheap type-specific logistic/GBT matcher rejects obvious negatives; a cross-encoder handles text-rich close candidates; an optional LLM produces structured comparison features for a very small ambiguous set. Calibrate the final score by cohort.
8. **Cluster/constraint check.** Test the proposed link against explicit negatives, source uniqueness, type compatibility, temporal validity, and cluster profile. One-to-one assignment can be solved globally where the source guarantees one record per entity; it must not be imposed where duplicates are legitimate.
9. **Decision.** Execute a versioned cost policy. Auto-link only at a validated risk/coverage point; create review, provisional identity, no-match, or not-entity decisions otherwise.
10. **Apply and publish.** Append the decision and membership/lineage event, update projections transactionally or through an outbox, invalidate versioned caches, and emit `identity.resolution.changed` with affected observations/entities and required downstream recomputations.
11. **Learn and revisit.** Human labels, new source IDs, contradiction detectors, changed entity profiles, model/rule deployments, and merge/split events create bounded re-resolution jobs. Periodic shadow backfills measure path dependence and missed links.

All handlers are idempotent on `(tenant_id, fragment_id, resolver_input_version, policy_version)`. At-least-once delivery may repeat computation but cannot duplicate an accepted decision or alias assertion.

### 7.3 State machines

Mention/source-fragment resolution state:

```text
DISCOVERED
   |
   v
CANDIDATES_READY -- none plausible --> UNRESOLVED or NOT_ENTITY
   |
   +-- strong known candidate ------> AUTO_LINKED --> CONFIRMED
   |
   +-- plausible new entity --------> PROVISIONAL --> PROMOTED
   |
   +-- ambiguous/high impact -------> IN_REVIEW --+--> CONFIRMED
   |                                                +--> DIFFERENT/UNRESOLVED
   |                                                +--> PROVISIONAL
   v
LINKED -- contradiction/new evidence --> CONTESTED --> IN_REVIEW/RECOMPUTE

Any terminal state -- new eligible evidence/model policy --> SUPERSEDED
SUPERSEDED retains a pointer to the replacing decision.
```

Canonical identity lifecycle:

```text
PROVISIONAL -> ACTIVE -> DEPRECATED/MERGED_INTO
                   |               |
                   +-> CONTESTED <-+
                           |
                           +-> SPLIT_PENDING -> ACTIVE descendants
                   |
                   +-> INACTIVE (real-world lifecycle; identity retained)
```

`INACTIVE` is not deletion. `MERGED_INTO` is a redirect and lineage relation. A split changes membership projections; it does not pretend the prior decision never existed.

### 7.4 Data model

Names are illustrative; exact migrations belong in a separate design/implementation review.

| Relation | Essential fields and invariant |
|---|---|
| `entity_registry` | `tenant_id, entity_id, entity_type, domain_ref, lifecycle_status, created_at`; common FK target for every resolvable entity; unique tenant/type/domain object |
| `source_identities` | `source_identity_id, tenant_id, connector_instance_id, source_kind, native_id, raw_attributes, valid_range, system_range`; source identifiers are never global |
| `entity_mentions` | `mention_id, observation_id, tenant_id, span offsets/phrase, mention_type hypotheses, extractor/version, event_time`; immutable extraction facts |
| `identity_fragments` | Common resolvable unit referencing a mention, source identity, imported record, or substrate candidate; carries event time and current resolution state |
| `alias_assertions` | Raw and normalized alias, target `entity_id`, language/type/source scope, validity/system ranges, provenance, assertion status; several conflicting assertions may coexist |
| `candidate_runs` / `entity_candidates` | Input/index versions, lane, rank, retrieval score, candidate entity; required for blocker evaluation and replay |
| `feature_snapshots` | Typed feature vector or immutable feature references, feature definition version, missingness, source and time context |
| `pair_scores` | Model/raw score, calibrated probability, cohort/calibrator version, LLM/rule contributions, runner-up margin |
| `identity_constraints` | `SAME`, `DIFFERENT`, `ONE_TO_ONE_SCOPE`, `TYPE_INCOMPATIBLE`, temporal constraint; authority, provenance, validity, status |
| `resolution_decisions` | Fragment, candidate or `NIL`, action, reason codes, policy/model/rule versions, uncertainty fields, actor/system, recorded/superseded times |
| `entity_memberships` | Fragment/source identity -> entity, valid range, system range, decision ID; exclusion constraints prevent conflicting current membership where policy requires |
| `identity_lineage_events` | `MERGE`, `SPLIT`, `REDIRECT`, `REACTIVATE`, affected identities/memberships, evidence, impact plan, approval, idempotency key |
| `entity_profiles` | Rebuildable current feature/profile projection by entity and version; field values retain provenance rather than blind winner overwrite |
| `review_tasks` / `review_labels` | Candidate comparison, impact, priority, assignment, answer, reviewer, timing, and resulting decision/constraint IDs |
| `source_reliability` | Field- and source-specific learned/configured reliability with time/model version; not one universal trust tier |

The registry solves the current JSONB-reference problem without forcing all domain objects into one table. An actor, customer resource, system resource, repository, document, commitment, or goal remains in its domain model; `entity_registry.domain_ref` provides a validated identity envelope. A trigger can still expose the familiar `{"type","id"}` shape at API boundaries, derived from a registry row.

W3C PROV provides a useful vocabulary—entities, activities, agents, derivation, generation, and responsibility—for the audit model ([PROV-O](https://www.w3.org/TR/prov-o/)). Fyralis need not adopt RDF storage, but every resolution should answer which observations/identifiers/features were used, which software/model/rule made it, who approved it, and which later event superseded it.

### 7.5 Deterministic rule engine and scoring policy

Rules are data with code-reviewed definitions, versions, effective dates, entity-type scope, and tests. They produce one of:

- **authoritative link:** exact source key under a uniqueness and reassignment contract;
- **positive feature:** e.g. verified domain agreement;
- **hard veto:** cross-tenant target, incompatible type, explicit different-person assertion, non-overlapping exclusive source identity;
- **eligibility filter:** candidate must share tenant, kind family, jurisdiction, environment, or temporal range;
- **review mandate:** high-impact cluster or contradictory authoritative identifiers;
- **creation rule:** sufficient evidence to propose/promote a new typed entity.

Avoid rule waterfalls that hide later evidence. Candidate rules generally union; hard constraints filter; the scorer combines remaining evidence; the policy selects an action. Each outcome lists fired rules. Common-value rarity belongs in features: agreement on `john@...` or “payments” is not equivalent to agreement on a rare registry ID.

The first learned matcher should be a type-specific logistic model or gradient-boosted tree over explicit comparison features. It will be easier to label, calibrate, operate, and debug than an end-to-end neural model. Add bi-encoder retrieval and a cross-encoder where labelled error analysis proves text semantics is the limiting factor. Use graph and LLM features only after the baseline and candidate recall are measurable.

### 7.6 Merge, split, and collective graph policy

Accepted pair links form an evidence graph, but clusters are produced under constraints. Automatic cluster merge requires every hard constraint to pass and a cluster-level compatibility check; a single weak bridge must not join two large components. Suitable strategies include constrained union-find for authoritative edges, correlation clustering/agglomeration for probabilistic edges, and complete/average-link safeguards for risky entity types. Store the pair edges that justify each membership.

Graph features should include typed common-neighbor counts weighted by inverse degree; compatible manager/team/repository/customer neighborhoods; temporal co-participation; and path motifs such as source account → verified email → actor. High-degree channels, bots, company-wide calendars, and popular repositories receive little or negative information value. Features use only evidence available before the decision's as-of time to prevent temporal leakage.

A background collective worker may re-score a bounded ambiguous subgraph when several related mentions arrive. It creates a proposal; it does not directly overwrite memberships. Contradiction workers detect impossible source-local duplicates, concurrent exclusive assignments, divergent authoritative IDs, excessive cluster size/growth, and low internal edge connectivity. They open `CONTESTED` or `SPLIT_PENDING` events with an impact preview.

Splitting removes or supersedes the minimum set of weak/invalid membership edges subject to hard negatives, recomputes affected components, proposes descendant identities, and lists downstream observations/models/materializations to repair. High-impact splits require a curator. Repair consumers are idempotent and can rebuild from ledger checkpoints.

### 7.7 Human review and LLM boundaries

The clarification workflow should be one identity-review system for actor mapping, phrase linking, and substrate creation. Type-specific panels differ, but actions share ledger semantics. A reviewer sees why each candidate was retrieved, field-by-field agreements/disagreements, timeline, selected graph neighborhood, aliases/source IDs, score calibration cohort, and cluster impact. Review cannot expose other tenants, and PII fields are redacted unless the reviewer role requires them.

LLMs may:

- extract Unicode/multilingual mentions and possible types from unstructured content;
- identify acronym/alias relations or normalize organization-specific wording as features;
- compare a server-supplied list of typically 2–10 validated candidates;
- return structured evidence such as “same domain,” “name is acronym,” or “role conflicts”;
- summarize a candidate comparison for the reviewer;
- propose, but not apply, a new substrate candidate;
- help generate labelled test cases and deterministic parsers, followed by conventional validation.

LLMs must never:

- invent or accept a canonical ID outside the supplied candidate enumeration;
- scan arbitrary tenant or cross-tenant data;
- treat prompt text as instructions; observation content is untrusted data;
- provide the sole numeric confidence used for an operational merge;
- override tenant/type/temporal/negative-link constraints;
- mutate aliases, observations, clusters, or domain objects directly;
- silently resolve `NIL` to a famous public entity;
- learn immediately from their own prior outputs.

Use strict structured output, candidate tokens rather than raw UUID generation, input/output schema validation, content delimiting, minimal context, provider data-retention controls, timeouts, cost budgets, model/version logging, and a kill switch. Re-evaluate provider/model changes against a frozen ER suite.

### 7.8 Storage, indexing, caching, and workers

**System of record:** PostgreSQL, with append-only/immutable decision rows, range indexes for temporal validity, tenant-prefixed primary/unique keys, and transactional outbox. Partition large candidate/score/event tables by time and/or tenant hash. Object storage holds reproducible training snapshots, large feature exports, and index artifacts.

**Indexes:** B-tree for source keys and current memberships; partial indexes for active aliases/reviews; GIN/`pg_trgm` for normalized lexical retrieval; GIN for structured attributes; pgvector HNSW for the first typed ANN profiles. HNSW is appropriate for dynamic online updates but has memory/build and worst-case limitations; at much larger scale, a service using sharded HNSW or IVF/PQ/FAISS can replace the ANN projection without changing contracts. Maintain separate indexes by tenant class/entity type/embedding model where practical; every hit reports index version.

**Cache:** an in-process L1 for confirmed exact aliases/source IDs and a distributed L2 only if measured need justifies it. Keys contain tenant, entity type, normalizer version, event-time bucket when needed, and identity projection version. Identity events invalidate or advance version namespaces. Negative cache entries have short TTLs because new entities and aliases arrive continuously. Never cache an ambiguous result as a permanent no-match.

**Workers:**

- mention/source-fragment extractor and index writer;
- candidate generator and feature materializer;
- cheap scorer and expensive reranker pools;
- policy/ledger applier;
- review task and clarification integrator;
- entity profile/alias/cache projector;
- graph collective and contradiction auditor;
- merge/split impact planner and repair/replay worker;
- calibration, evaluation, drift, and training pipelines;
- dead-letter/reconciliation worker that compares ledger to projections.

Workers partition primarily by tenant plus fragment/entity hash. Per-tenant quotas, weighted fair scheduling, batch inference, backpressure, and circuit breakers prevent one tenant or hot alias from starving the fleet. A replay job pins normalizer, feature, index snapshot, model, calibrator, and policy versions so results are reproducible.

### 7.9 Observability, SLOs, and failure behavior

Dashboards must follow a fragment from ingest through candidates, score, decision, projection, and downstream repair using correlation IDs. In addition to Section 4.8 metrics, alert on cross-tenant lookup attempts, invalid target refs, sudden auto-link coverage changes, calibration drift, contested-cluster growth, common-key fanout, index lag, review backlog age, LLM use/cost/parse failures, projection/ledger divergence, and repair lag.

Initial SLOs should be measured before numeric commitments, then set separately:

- synchronous ER added latency p95/p99 and availability;
- observation durability independent of resolver availability;
- exact-source/confirmed-alias resolution freshness;
- near-line long-tail resolution freshness;
- review queue service levels by impact;
- ledger-to-projection and identity-change-to-reasoning propagation lag;
- measured false-merge risk at automatic coverage.

Failure defaults are conservative:

| Failure | Behavior |
|---|---|
| Exact cache/index unavailable | Persist unresolved; enqueue retry; do not guess |
| Candidate lane timeout | Score available lanes only if policy allows and mark incomplete; otherwise retry/review |
| Model/LLM timeout or rate limit | Retry with bounded backoff or fall back to cheaper calibrated model; never lower threshold |
| Invalid/dangling/cross-tenant candidate | Reject, security-log, and quarantine producing component |
| Ledger write succeeds, projection fails | Replay idempotently from outbox; ledger remains truth |
| Conflicting authoritative IDs | Mark contested and require policy/human resolution |
| Oversized block/hub graph | Split/suppress lane, cap expansion, and emit blocker health event |
| Calibration or drift gate fails | Disable auto-link for affected cohort; continue review/unresolved operation |
| Merge/split repair partially fails | Keep lineage event and retry each consumer from repair manifest |

### 7.10 Security and privacy

ER increases privacy risk because linking identifiers reveals more than either source alone. Security requirements are architectural:

- tenant and connector-instance scope are present in every key, index, cache, queue message, and authorization check;
- RLS is defense in depth, not a substitute for scoped query predicates and constraints;
- encrypt source identifiers and sensitive attributes, with keyed hashes or tokenization for equality search where appropriate;
- restrict and audit unmasked email, HR IDs, behavior, and relationship features;
- minimize LLM/provider context, exclude secrets and unrelated PII, honor residency/retention policy, and support provider disablement;
- rate-limit enumeration and candidate APIs; do not expose similarity neighborhoods as a side channel;
- protect review actions with least privilege, reason codes, dual approval for high-impact merges/splits, and tamper-evident audit;
- propagate deletion/retention/legal-hold rules through aliases, features, training sets, index artifacts, and lineage without erasing required decision audit;
- test for demographic/language/source disparities in mention, blocking, ranking, calibration, and abstention; behavioral similarity must not encode protected-class proxies without review;
- treat all observation text as hostile input to parsers and LLMs.

The actor mapping defect should be handled as a security boundary issue, not merely a schema cleanup.

## 8. Migration Strategy

Migration should preserve observation ingestion and introduce the new ledger beside current projections. No phase should require a flag day.

### Phase 0 — Contain current risk and establish a baseline

- Add tenant plus connector-instance/account scope to actor mapping keys and queries; detect and adjudicate existing collisions.
- Complete actor clarification side effects with authorization, idempotency, and end-to-end tests.
- Validate every target against tenant, allowed type, and existing domain row before alias insertion or observation annotation.
- Disable LLM-self-confidence auto-resolution, or temporarily require review for worker-only candidates until a calibrated policy exists.
- Change low/null results from destructive drop to durable unresolved/not-entity decisions eligible for reconsideration.
- Instrument mention yield, exact hits, ambiguous hits, unresolved backlog, worker actions, review outcomes, and downstream changes by tenant/type/source.
- Freeze a representative, identity-disjoint, time-aware gold set from source-authoritative IDs and adjudicated reviews.

**Exit gate:** no known cross-tenant mapping path; all clarification actions have tested effects; invalid refs are rejected; current precision/coverage/cost are measured.

### Phase 1 — Introduce the registry and identity ledger

- Create the entity registry, source identity, mention/fragment, alias assertion, candidate-run, decision, membership, constraint, review-label, and lineage-event contracts.
- Backfill registry rows for actors and supported domain objects; validate all current alias refs and quarantine dangling/ambiguous rows.
- Dual-write new observations to immutable mention/source-annotation tables while keeping `entities_mentioned` as a compatibility projection.
- Project accepted ledger decisions into current aliases and observation-facing views. Reconcile counts and target sets continuously.
- Materialize one versioned normalizer and migrate exact alias indexes; retain raw values.

**Exit gate:** the new ledger can rebuild current compatible entity annotations for a sampled/full tenant with explainable provenance and no material divergence.

### Phase 2 — Hybrid candidate generation in shadow mode

- Implement typed exact/composite and trigram/BM25 lanes first; add ANN profile retrieval where it provides measured marginal recall.
- Record candidates and retrieval lanes without changing production decisions.
- Build explicit type-specific comparison features, rarity statistics, temporal eligibility, and hard constraints.
- Measure pair completeness, reduction ratio, fanout, latency, and marginal recall by lane. Tune caps on realistic long-tail and multilingual data.

**Exit gate:** candidate recall meets the agreed high-recall target in every critical slice, with bounded p99 fanout and no cross-tenant retrieval.

### Phase 3 — Calibrated ranking, policy, and unified review

- Train an interpretable baseline matcher by entity type; calibrate by source/type/language/novelty cohort.
- Run it shadowed against exact/legacy/LLM decisions and adjudicate disagreements, emphasizing false-merge analysis.
- Deploy `LINK/REVIEW/PROVISIONAL/NO_MATCH/NOT_ENTITY` policy versions gradually by type and tenant, initially with auto-link disabled except authoritative source IDs.
- Unify actor, phrase, and substrate clarification behind identity decisions and constraints. Add active-learning priority and impact previews.
- Add cross-encoder or bounded LLM comparison only when controlled ablations show net value after cost and calibration.

**Exit gate:** automatic coverage meets an explicit false-merge risk target on held-out temporal/entity-disjoint data; online calibration and review-yield stay within guardrails.

### Phase 4 — Temporal membership, merge/split, and downstream repair

- Backfill validity ranges conservatively and expose current/as-of/system-time query APIs.
- Implement redirects, merge proposals, hard negative links, split planning, lineage, and repair manifests.
- Stop mutating immutable observation source annotations; serve resolution through versioned projections while maintaining the legacy field until consumers migrate.
- Make reasoning/model-scope consumers respond idempotently to identity change and split/merge repair events.

**Exit gate:** rehearsed merge and split can be previewed, approved, applied, replayed, and rolled back as a new superseding event; historical queries remain reproducible.

### Phase 5 — Collective refinement and measured scale-out

- Add graph features and contradiction detectors in shadow mode, with temporal leakage and hub controls.
- Run bounded collective inference on ambiguous/high-impact subgraphs; retain pair and cluster ablations.
- Scale lexical/ANN/stateful processing out of PostgreSQL only where SLO and cost evidence requires it. Preserve ledger and service contracts.
- Schedule periodic replay/backfill audits and model/index revalidation.

**Exit gate:** graph/scale components improve defined error slices or SLOs without unacceptable calibration, operational, privacy, or explainability regression.

### 8.1 Current-to-recommended disposition

| Subsystem | Current design | Recommended design | Action and justification |
|---|---|---|---|
| Observation persistence | Persist even when unresolved | Immutable source observation plus separate identity projection | **Keep/improve:** preserve non-blocking ingest and separate later belief from source fact |
| Connector hints | Bare `entities_hint` refs | Validated source assertions with connector-instance provenance/authority | **Improve:** structured sources are strong but not implicitly trusted |
| Actor source key | `(source_channel, source_actor_ref)` global | `(tenant, connector_instance, source_kind, native_id)` with validity | **Replace immediately:** isolation and ID lifecycle require full scope |
| Actor exact lookup | Exact repository read | Authoritative source-identity fast path through registry | **Keep/improve:** correct fast-path family, unsafe current key |
| Actor clarification | Options exist; actions not dispatched | Unified identity review writes mapping/constraint/creation decision | **Replace wiring:** current user action is incomplete |
| Phrase tokenizer | ASCII 1–3 grams, max 50 | Unicode/structured mention extraction plus bounded exact lookup | **Replace for long tail; keep only compatibility fast lookup:** current recall is structurally limited |
| Normalization | Python casefold vs SQL lower/regex | Materialized versioned normalizer per type/language with raw value | **Replace invariant:** one reproducible implementation and migration path |
| Alias table | Phrase → JSONB ref with counts/confidence | Temporal evidence-backed alias assertions → registry ID | **Evolve/replace schema:** keep learned vocabulary, add validity/provenance/constraints |
| Exact alias query | Batched tenant lookup; ambiguity abstains | Query only confirmed/current/type-eligible assertions; return decision/version | **Keep/improve:** efficient and safe once quality gates exist |
| Alias confidence | Stored but ignored by fast path | One evidence component plus calibrated decision policy | **Replace semantics:** raw stored confidence is not operational probability |
| Alias vector | HNSW column/index unused | Versioned typed entity-profile ANN candidate lane | **Postpone use until evaluated, then repurpose:** embeddings are retrieval, not final match |
| Unresolved storage | JSON in observation content | Durable fragment decision state and retry/reconsideration events | **Replace:** queryable lifecycle is required |
| Worker context | Recent same-channel observations/models and top aliases | Typed source/time/org/graph feature bundle plus validated candidates | **Improve:** keep local context, remove unbounded popularity bias |
| Worker LLM | Emits arbitrary ref and self-confidence | Optional bounded candidate comparator returning validated features | **Replace role:** semantic help without authority |
| Thresholds | Global `>0.8`, `0.5..0.8` | Calibrated type/cohort risk policy with margin and constraints | **Replace:** thresholds must have measured error meaning |
| Low-confidence action | Drop/remove phrase | `UNRESOLVED`, `NO_MATCH`, or `NOT_ENTITY`, versioned and revisitable | **Remove destructive drop:** future evidence must repair uncertainty |
| Review queue | One candidate; accept/reject/create | Ranked candidates, evidence/impact/timeline, same/different/new/interval/merge/split | **Improve:** make decisions useful and learnable |
| Manual alias learning | Insert alias on accepted review | Add label, assertion/constraint, decision, membership, and training example | **Keep/expand:** feedback must propagate safely |
| Automatic apply | Alias + observation mutation + state change + selected trigger | Append ledger decision, project annotations, emit universal identity-change/repair event | **Replace orchestration:** one truth and idempotent consumers |
| Canonical refs | Arbitrary polymorphic JSONB | Tenant entity registry FK with derived typed API ref | **Replace:** enforce existence, type, and tenancy |
| Clustering | None beyond same alias target | Constraint-aware membership clusters with versioned edges | **Add:** pair links alone do not maintain identity |
| Negative evidence | Reject dismisses a review | Durable `DIFFERENT_FROM`/veto constraint with scope/time | **Add:** prevents recurring false candidates and unsafe transitivity |
| Merge/split | None | Lineage events, redirects, bitemporal memberships, repair manifests | **Add:** correctness requires reversibility |
| Temporal model | first/last alias usage only | Valid and system time on assertions, memberships, profiles, relationships | **Add:** avoid state smearing and ID reassignment errors |
| Graph resolution | No explicit relationship features | Bounded degree-corrected features and background collective proposals | **Postpone until pair baseline:** high potential, high propagation risk |
| Substrate promotion | Separate deterministic candidates and thresholds | Shared registry/ledger/policy, type-specific promotion rules retained | **Integrate/improve:** preserve open-world strength, remove parallel truth |
| Storage | PostgreSQL/JSONB/pgvector | PostgreSQL ledger and projections initially; object snapshots; replaceable search/graph projections | **Keep/improve:** adequate current scale and transactional fit |
| Caching | Database-centric | Versioned tenant/type exact caches with event invalidation and short negative TTL | **Add when measured:** latency without stale identity truth |
| Observability | Structured worker decisions, limited quality metrics | End-to-end blocker/ranker/calibration/cluster/temporal/operational dashboards | **Replace/expand:** production ER is managed by error slices, not log volume |

## 9. Risks and Trade-offs

| Risk/trade-off | Consequence | Mitigation / accepted position |
|---|---|---|
| False merge vs. false split | High precision lowers automatic coverage and increases review/unresolved volume | Explicitly prefer false splits for people/customers/commitments; tune by action impact and expose unresolved state to reasoning |
| Architecture complexity | Ledger, projections, versions, and repair workflows add operational surface | Complexity reflects real correction/time requirements; introduce in phases and keep PostgreSQL as one truth boundary |
| Historical bitemporality | More storage and harder queries | Provide current projection for normal consumers and as-of API for audit/reasoning; partition/compact feature artifacts, never identity audit |
| Candidate recall vs. cost | Wider lanes improve recall but increase scoring and hot blocks | Measure per-lane marginal recall, progressive widening, caps, common-key suppression, and high-impact on-demand expansion |
| Calibration drift | A safe threshold becomes unsafe as sources/entities/model change | Cohort monitoring, temporal tests, auto-link kill switch, periodic recalibration, policy version pinning |
| Graph error propagation | One bad edge can join many entities | Hard negatives, degree correction, cluster checks, proposal-only collective worker, impact-aware human review |
| LLM variability and injection | Wrong evidence, nondeterminism, data leakage, provider cost | Bounded candidates, strict schema, no direct writes/self-confidence, hostile-content delimiting, eval pinning, provider controls |
| Human inconsistency | Labels and corrections can encode reviewer error | Evidence-rich UI, expertise routing, dual approval for high impact, disagreement audits, reversible decisions |
| New entity proliferation | Conservative linking produces many provisional duplicates | Periodic duplicate candidate runs and safe merge workflow; do not solve proliferation with aggressive automatic merges |
| Source “authority” errors | Upstream HR/CRM/ORCID-like errors metastasize | Authority is scoped and defeasible; preserve provenance, contradiction detection, and explicit correction |
| Training leakage/bias | Inflated metrics and unequal quality for names/languages/roles | Identity/time/source-disjoint splits, slice metrics, random audits, privacy/fairness review of behavioral features |
| Projection inconsistency | Reasoning sees stale or half-applied identity | Ledger/outbox truth, idempotent projection, reconciliation checks, version-aware reads, repair lag SLO |
| PostgreSQL bottleneck | Large candidate/score volumes or ANN memory affect OLTP | Partition, isolate worker pools/indexes, batch, archive artifacts; scale search/state projections out behind stable contracts when proven |
| Domain boundary disputes | Legal company, CRM customer, brand, and group may be incorrectly treated as one concept | Registry types and explicit relations; domain-specific sameness policies and curator ownership |

The principal trade-off is deliberate: Fyralis will carry explicit unresolved and provisional state in exchange for protecting organizational memory from silent false merges. That is the correct bias for a perception engine whose conclusions compound over time.

## 10. References

### Foundational and survey literature

- Fellegi, I. and Sunter, A. “A Theory for Record Linkage,” 1969. [Journal page](https://www.tandfonline.com/doi/abs/10.1080/01621459.1969.10501049).
- Papadakis et al. “The Four Generations of Entity Resolution,” and related end-to-end survey. [End-to-End Entity Resolution for Big Data](https://arxiv.org/abs/1905.06397).
- Christen, P. *Data Matching: Concepts and Techniques for Record Linkage, Entity Resolution, and Duplicate Detection*, Springer, 2012.
- Binette, O. and Steorts, R. “(Almost) All of Entity Resolution.” [Science Advances/PMC](https://pmc.ncbi.nlm.nih.gov/articles/PMC11636688/).
- Sadinle, M. “Bayesian Estimation of Bipartite Matchings for Record Linkage.” [arXiv](https://arxiv.org/abs/1601.06630).
- Steorts, R., Hall, R., and Fienberg, S. “A Bayesian Approach to Graphical Record Linkage and Deduplication.” [arXiv](https://arxiv.org/abs/1407.8219).
- Konda et al. “Magellan: Toward Building Entity Matching Management Systems.” [PVLDB](https://www.vldb.org/pvldb/vol9/p1581-konda.pdf).
- Mudgal et al. “Deep Learning for Entity Matching.” [DeepMatcher paper](https://doi.org/10.1145/3183713.3196926).
- Li et al. “Deep Entity Matching with Pre-Trained Language Models” (Ditto). [arXiv](https://arxiv.org/abs/2004.00584).
- Bhattacharya and Getoor. “Collective Entity Resolution in Relational Data.” [Paper](https://linqs.org/assets/resources/bhattacharya-tkdd07.pdf).
- Guo et al. “On Calibration of Modern Neural Networks.” [PMLR](https://proceedings.mlr.press/v70/guo17a.html).
- Binette et al. “How to Evaluate Entity Resolution Systems.” [arXiv](https://arxiv.org/abs/2404.05622).
- Luo, X. “On Coreference Resolution Performance Metrics” (CEAF). [ACL Anthology](https://aclanthology.org/H05-1004/).
- Vilain et al. “A Model-Theoretic Coreference Scoring Scheme” (MUC). [ACL Anthology](https://aclanthology.org/M95-1005.pdf).

### Blocking, retrieval, incremental, and LLM work

- Papadakis et al. “A Survey of Blocking and Filtering Techniques for Entity Resolution.” [arXiv](https://arxiv.org/abs/1905.06167).
- Broder, A. “On the Resemblance and Containment of Documents” (MinHash). [PDF](https://www.cs.princeton.edu/courses/archive/spr05/cos598E/bib/broder97resemblance.pdf).
- Malkov and Yashunin. “Efficient and Robust Approximate Nearest Neighbor Search Using HNSW.” [arXiv](https://arxiv.org/abs/1603.09320).
- Johnson, Douze, and Jégou. “Billion-scale Similarity Search with GPUs” / FAISS. [arXiv](https://arxiv.org/abs/1702.08734).
- Gruenheid et al. “Incremental Record Linkage.” [Linked-document ER](https://arxiv.org/abs/1402.4417).
- Wadhwa et al. “Entity Resolution with LLMs: Prompting, Fine-Tuning, and Active Learning.” [arXiv](https://arxiv.org/abs/2401.03426).
- Peeters and Bizer. “Entity Matching Using Large Language Models.” [EDBT 2025](https://www.openproceedings.org/2025/conf/edbt/paper-81.pdf).

### Production systems, platforms, and standards

- LinkedIn. “Matching external companies to LinkedIn's Economic Graph at scale.” [Engineering](https://www.linkedin.com/blog/engineering/economic-graph/matching-external-companies-to-linkedin-s-economic-graph-at-scal).
- Google. “Knowledge Vault: A Web-Scale Approach to Probabilistic Knowledge Fusion.” [Google Research](https://research.google/pubs/knowledge-vault-a-web-scale-approach-to-probabilistic-knowledge-fusion/).
- Google. “Operationalized Temporal Entity Resolution at Scale.” [Google Research](https://research.google/pubs/operationalized-temporal-entity-resolution-at-scale-a-2b-entity-architecture-for-cloud-native/).
- Microsoft. “Entity Linking at the Tail.” [Microsoft Research](https://www.microsoft.com/en-us/research/publication/entity-linking-at-the-tail-sparse-signals-unknown-entities-and-phrase-models/).
- Meta Research. BLINK and FAISS. [BLINK](https://github.com/facebookresearch/BLINK), [FAISS](https://github.com/facebookresearch/faiss).
- AWS. AWS Entity Resolution documentation. [User guide](https://docs.aws.amazon.com/entityresolution/latest/userguide/what-is-service.html).
- Microsoft. Dynamics 365 Customer Insights data unification. [Overview](https://learn.microsoft.com/en-us/dynamics365/customer-insights/data/data-unification).
- Splink. Probabilistic data linkage documentation. [Documentation](https://moj-analytical-services.github.io/splink/).
- OpenCorporates. Reconciliation and API documentation. [Reconciliation](https://api.opencorporates.com/documentation/Open-Refine-Reconciliation-API).
- OpenAlex. Public author-disambiguation updates. [OpenAlex blog](https://blog.openalex.org/category/uncategorized/).
- W3C. PROV-O and OWL identity semantics. [PROV-O](https://www.w3.org/TR/prov-o/), [OWL Reference](https://www.w3.org/TR/owl-ref/).
- IETF. SCIM Core Schema, RFC 7643. [RFC Editor](https://www.rfc-editor.org/info/rfc7643/).
- GLEIF. LEI Common Data File and organizational identity documentation. [LEI-CDF](https://www.gleif.org/content/4_lei-data/1_access-and-use-lei-data/2_level-1-data-lei-cdf-3-1-format/lei-cdf_version_3.1-documentation.html).

### Evidence note

Vendor entries intentionally rely on official research, engineering, product documentation, or maintained repositories. Where a company does not publish its internal matcher—especially Palantir, OpenAI, and Anthropic—the report says so and uses only the architectural capability that is publicly supported. Product quickstarts are treated as examples, not independent evidence of accuracy or scale.
