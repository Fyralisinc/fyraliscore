# Read Authority Plane Plan

Date: 2026-06-23

## North Star

Fyralis Core should enforce authority-based reads over company state.

The system is correct only when no actor can learn company state they lack
read authority for, whether through raw rows, Models, summaries, Ask evidence,
cached cards, realtime events, projections, debug routes, exports, or indirect
prompt attacks.

Finance is only one concrete example. The larger invariant is:

```text
principal + purpose + object/provenance -> authorized view
```

Product code should not decide this directly. Product code should ask the
authority plane for an authorized view, and the authority plane should decide
from object kind, ownership, scope, labels, provenance, delegation, and purpose.

## First Principles

1. Read authority is about learning, not just fetching.
   If a user can learn a restricted fact through a summary, Model, Ask answer,
   cached card, debug route, or realtime event, the system has leaked it.

2. Derived state inherits source authority.
   A public-looking Model derived from restricted source data remains restricted
   unless an explicit declassification rule produces a safe projection.

3. Access control should happen before retrieval, ranking, prompting, rendering,
   caching, and evidence expansion.
   Filtering after an LLM or evidence projector has summarized restricted
   sources is too late.

4. All user-facing reads need an explicit principal.
   `AccessContext(requestor_actor_id=None)` is acceptable for internal workers,
   but should never be the default for human-facing query paths.

5. Delegation is first-class.
   If a user needs access, authority should be delegated by someone who has the
   right to grant it, for a scope, purpose, duration, and audit reason.

## Current Repo Signals

- `services/platform/access_control/checks.py` already contains a useful
  `can_read(actor, entity)` core with tenant isolation, role checks, resource
  kind checks, and Model visibility.
- The newer `services/product/ask` path passes a viewer id and filters Models
  and Observations through `can_read`.
- The older `/view/ceo/ask` query path can still default to a system-like
  `AccessContext` unless wired with a request actor.
- The retrieval assembler currently filters Models more strongly than
  Observations, resources, acts, customer context, and projected evidence.
- Models default to `visible_to_subjects = TRUE`, which is unsafe for derived
  memory that may contain restricted source facts.
- Extension reads already use a cleaner capability-scoped reader pattern. Human
  product reads should get the same architectural discipline.

## Implementation Plan

## Implementation Progress

As of 2026-06-24:

- Phase 0 first bypass fix is implemented for legacy `/view/ceo/ask`:
  `AnswerQueryRequest` now carries a `viewer_id`, `QueryHandler` refuses
  human query reads without a viewer, `/view/ceo/ask` resolves the actor from
  bearer auth or explicit dogfood defaults, and query-prefetch cache keys are
  viewer-scoped.
- In-card conversation Ask now forwards `actor_id` into `QueryHandler`, so it
  no longer falls back to system-style access context.
- Phase 1/2 foundation exists in code and schema:
  `db/migrations/0171_read_authority_plane_foundation.sql` adds
  `object_access_labels`, `object_provenance_edges`, `access_grant_epochs`, and
  `read_authority_grants`; `services/platform/access_control/authority.py`
  defines `Principal`, `Purpose`, `ObjectRef`, `AccessLabel`,
  `AuthorityDecision`, `authorize_read`, and `authorized_reader`.
- Hermetic tests cover restricted-label denial, label delegation, object
  delegation, finance-role label access, provenance-source denial, and the
  initial `AuthorizedReader` facade.
- `/v1/ask` now uses `authorize_read` for retrieved Models, Observations,
  projected evidence refs, and omitted projection refs. Projected evidence
  without an authorized source ref is dropped before ranking, answer
  composition, evidence persistence, or state-contract compilation.
- Query-prefetch cache keys now include an authority fingerprint derived from
  tenant, actor, purpose, role set, active grant epoch, scope hash, and policy
  version; role or grant changes therefore miss older prefetched answers.
- `/v1/ask/evidence/expand` now receives the authenticated viewer and re-checks
  persisted evidence against live authority before returning it. Stored model,
  observation, omitted-model, projected, and composed-chain evidence is returned
  only when its source refs still authorize for the current viewer.
- Persisted Ask evidence now records authority provenance and inherited labels
  as `evidence` derived artifacts at write time, with migration
  `0175_backfill_ask_evidence_authority.sql` backfilling existing persisted
  Ask evidence from direct source refs, projected source refs, and composed
  chain observation refs.
- Accepted Ask-answer writeback now re-checks persisted supporting evidence
  against live authority before enqueuing memory writeback. Denied evidence is
  excluded from writeback provenance, and writeback is suppressed when no
  authorized supporting evidence remains.
- Ask sessions/scopes and answers now persist compact authority snapshots:
  fingerprint, tenant, viewer, purpose, role-set hash, grant epoch, scope hash,
  policy version, and capture time. Migration `0172_ask_authority_snapshots.sql`
  adds `ask_answers.authority_snapshot` plus audit indexes.
- `ModelsRepo.insert` and `ModelsRepo.insert_many` now record read-authority
  provenance edges from born-from observations, supporting observations,
  supporting Models, and contributing Models to each created Model, so derived
  Model reads can inherit source authority.
- Migration `0173_backfill_model_authority_provenance.sql` backfills the same
  provenance edges for existing Models from their evidence arrays.
- Product and gateway model-trace reads now accept the authenticated viewer
  principal and filter trace chains plus one-hop supports/depends-on results
  through `authorize_read(..., purpose="model_trace", object_kind="model")`.
  Internal trace callers can still omit a principal for system reads.
- Raw debug observation/model/act endpoints now require an authenticated actor
  and filter listed rows, single-object reads, and nested model support rows
  through `authorize_read(..., purpose="debug")`. Unauthorized single-object
  debug reads return 404 to avoid confirming existence.
- `/debug/cache` no longer returns cached payloads to tenant-header-only
  callers; it requires an authenticated principal with `admin` or `leadership`
  authority until cache rows have stable object refs for per-object checks.
- Core source objects now receive authority labels at write time:
  resource creation records internal/resource-kind/domain labels; observation
  inserts and state-change emission record internal/channel labels, including
  duplicate-observation repair paths.
- Observation provenance is now written for cause links, and state-change
  observations record provenance to both their cause observation and mutated
  core entity where available.
- Model inserts and bulk inserts now copy inherited source labels from their
  recorded provenance refs, so newly derived Models carry the source authority
  labels that Ask/model-trace authorization can evaluate.
- Migration `0174_backfill_access_labels.sql` backfills existing resource and
  observation labels, observation/state-change provenance edges, and inherited
  labels for existing derived objects using recorded provenance.
- Derived artifacts with first-class provenance, including persisted evidence,
  can now be authorized through their source objects. Unsupported derived
  artifacts without provenance still fail closed.
- Legacy Today and `/v1/recommendations` now load the authenticated viewer's
  `Principal` and filter recommendation Models, target objects, card evidence
  observations, supporting Models, financial resource metrics, just-learned
  Models, recent-signal observations, and authority-sensitive counts through
  `authorize_read(..., purpose="today")`.
- Active v2 `/today` decision-delta reads now use the authenticated viewer's
  `Principal` and filter delta list/detail/evidence/mutation paths through the
  strongest available source refs: promoted source recommendation Models and
  target node refs. Summary exposure and next-delta selection use the same
  filter, and unauthorized deltas return 404 on direct reads/mutations.

Still remaining:

The detailed resume checklist is saved in
`docs/plans/read-authority-remaining-work.md`.

- Complete label/provenance coverage for projections, caches, decision-delta
  evidence rows, direct/unbacked decision deltas, non-Model derived artifacts,
  and source areas where domain labels cannot yet be inferred conservatively.
- Enforce provenance-derived labels beyond Models and core source objects:
  projections, caches, replay/read APIs, and other derived read artifacts still
  need object-level authority refs and checks.
- Finish moving retrieval, remaining raw substrate, fine-grained debug cache
  object refs, realtime, and export reads onto the authority API.
- Make Ask answer replay/read behavior explicit in APIs and audits if a replay
  endpoint is introduced, and require live authority at replay time.
- Finish grantor authorization, approval workflow, revocation invalidation,
  and runtime/replay tests across every scenario below.

### Phase 0 - Stop Known User-Read Bypasses

1. Wire `/view/ceo/ask` with the authenticated actor.
   `QueryHandler` should receive an `access_context_builder` that builds
   `AccessContext(tenant_id=auth.tenant_id, requestor_actor_id=auth.actor_id)`.

2. Remove system access defaults from human-facing paths.
   Keep system identity available only through an explicit internal worker
   context, not as a fallback.

3. Add immediate regression tests:
   - non-authorized actor cannot retrieve restricted private Models through
     `/view/ceo/ask`;
   - `/view/ceo/ask` and `/v1/ask` produce the same access outcome;
   - cache hits do not bypass actor-specific access.

### Phase 1 - Define Authority Primitives

Add a compact authority vocabulary under `services/platform/access_control`.

Suggested primitives:

- `Principal`: tenant id, actor id, roles, active grants, grant epoch.
- `Purpose`: ask, today, model_trace, debug, export, realtime, extension,
  internal_reasoning.
- `ObjectRef`: tenant id, object kind, object id.
- `AccessLabel`: domain, channel, subject, scope, resource kind, classification.
- `AuthorityDecision`: allowed, reason, labels considered, provenance considered,
  override/delegation flag, audit requirement.

Keep the public API small:

```text
authorize_read(principal, purpose, object_ref) -> AuthorityDecision
authorized_reader(principal, purpose) -> AuthorizedReader
```

### Phase 2 - Add Labels And Provenance

Add idempotent migrations:

```text
object_access_labels(
  tenant_id,
  object_kind,
  object_id,
  label,
  source,
  created_at
)

object_provenance_edges(
  tenant_id,
  derived_kind,
  derived_id,
  source_kind,
  source_id,
  derivation_kind,
  created_at
)

access_grant_epochs(
  tenant_id,
  epoch,
  updated_at
)
```

Initial label sources:

- resource kinds: financial, capacity, relational, ip, infrastructure,
  regulatory;
- channels: finance integrations, HR, legal, incident, customer, engineering;
- scope: subject actor, manager chain, customer/account owner, goal, commitment,
  decision, vendor, system;
- explicit classification labels: public, internal, restricted, confidential.

Backfill strategy:

- label resources from `resources.kind`;
- label observations from `source_channel`, actor, and entities mentioned;
- label Models from `born_from_event_id`, `supporting_event_ids`,
  `supporting_model_ids`, and `scope_entities`;
- conservative fallback: unknown provenance means restricted or needs review,
  not public.

### Phase 3 - Propagate Authority At Write Time

Think validation and apply should enforce provenance-derived labels.

For Model inserts and updates:

1. collect source refs from born-from event, supporting events, supporting
   Models, referenced resources, and source scope;
2. compute the union or max sensitivity of source labels;
3. write provenance edges;
4. write access labels;
5. force `visible_to_subjects = false` when restricted labels are present unless
   an explicit declassification rule produced the object;
6. reject or rewrite LLM attempts to make restricted-derived memory public.

For Ask evidence and answers:

1. store source refs and source labels;
2. attach authority snapshot or fingerprint;
3. prevent projected evidence from entering the answer if its source is not
   already authorized for the viewer.

For product projections and caches:

1. record provenance;
2. record labels;
3. include authority fingerprint in cache keys.

### Phase 4 - Build The Authorized Reader

Create `services/platform/access_control/authorized_reader.py`.

It should expose safe, typed read methods only:

```text
models.search(...)
models.get(...)
observations.search(...)
observations.get(...)
resources.list(...)
resources.get(...)
acts.list(...)
evidence.expand(...)
projection.get(...)
```

Each method applies:

- tenant binding;
- principal and purpose;
- object-level `can_read`;
- label grants;
- provenance restrictions;
- optional projection/redaction;
- audit capture for overrides and delegated reads.

Human-facing product code should use this reader instead of raw SQL reads.

Move these paths onto it:

- `/v1/ask`;
- `/view/ceo/ask`;
- retrieval assembler;
- Today/cards;
- model trace;
- substrate list routes;
- evidence expansion;
- realtime delivery;
- debug and export routes.

### Phase 5 - Make Retrieval Authority-Safe

Retrieval should not retrieve restricted material and then hope the assembler
drops it.

Required changes:

1. pass `Principal` and `Purpose` into retrieval;
2. constrain lexical, structural, semantic, temporal, graph, and SAGE retrieval
   to authorized candidates;
3. ensure projected evidence is built only from authorized source refs;
4. count denied candidates without leaking restricted object ids to the user;
5. expose operator-only access-redaction telemetry.

### Phase 6 - Cache And Session Safety

Every user-facing cached artifact needs an authority fingerprint:

```text
tenant_id
actor_id
purpose
role_set_hash
active_grant_epoch
scope_hash
policy_version
```

Rules:

- cache hits require exact fingerprint match;
- grants and revocations increment tenant grant epoch;
- Ask sessions store viewer id and authority snapshot;
- evidence expansion re-checks live access;
- revocation affects future reads immediately;
- old answers remain auditable but should not allow expanding newly unauthorized
  evidence.

### Phase 7 - Delegation And Access Requests

Add explicit access workflow:

1. actor requests authority over a label, object, or scope;
2. system resolves eligible grantors;
3. grantor approves or denies;
4. grant stores scope, purpose, expiry, reason, granted_by, and audit trail;
5. revocation increments grant epoch and invalidates future cache hits.

Delegation should be enforceable without product-specific exceptions.

### Phase 8 - RLS And Restricted Roles For Human Reads

The extension subsystem already has a restricted-reader pattern. Human reads
should get the same defense in depth:

- run human-facing reads in a tenant-bound transaction;
- set actor and purpose session variables when practical;
- use a restricted DB role for read paths;
- fail closed when the tenant or actor context is missing in production;
- keep service/system lanes explicit and auditable.

## Objective Test Scenarios

### Core Authority

1. Actor can read an object they own.
2. Actor cannot read an object outside their authority.
3. Tenant admin or leadership can read through an override where policy allows.
4. Override reads write an audit event.
5. Cross-tenant reads are denied even for admins.
6. Revoked grants deny immediately on the next read.
7. Expired grants deny after expiry.
8. Delegated access works only for the delegated scope and purpose.
9. Delegation by a non-authoritative actor is rejected.
10. Denial response does not leak restricted object content.

### Domain Coverage

11. Finance state is invisible to non-finance actors.
12. HR state is invisible outside subject, HR authority, or permitted manager
    scope.
13. Legal state is invisible outside legal or explicit grant.
14. Security/incident state is invisible outside incident authority.
15. Customer account state is visible to account owners and authorized leaders.
16. Engineering work state is visible to owners, contributors, and relevant
    managers.
17. Board or executive-only material is not visible to general employees.
18. Private actor-scoped Models are visible to the subject where policy allows.
19. Resource-kind rules are enforced consistently across resources and derived
    Models.
20. Channel sensitivity labels are applied consistently during ingest.

### Derived State And Data Laundering

21. A Model derived from restricted observation inherits restricted labels.
22. A Model derived from restricted resource inherits restricted labels.
23. A Model derived from public plus restricted sources remains restricted.
24. A same-topic public Model does not grant access to restricted sibling
    evidence.
25. LLM-proposed `visible_to_subjects=true` on restricted-derived memory is
    rejected or rewritten.
26. Reconciliation merge between public and restricted Models preserves the
    restricted label.
27. Model edges do not leak restricted node existence to unauthorized users.
28. Situation/composite Models inherit authority from all member Models.
29. State-change observations derived from restricted operations are labeled.
30. Declassified projections expose only the explicitly safe projection, not the
    source facts.

### Ask And Query

31. `/v1/ask` answers only from authorized Models and Observations.
32. `/view/ceo/ask` has the same authority behavior as `/v1/ask`.
33. Ask projected evidence cannot summarize unauthorized source refs.
34. Ask omitted evidence does not reveal restricted ids or object existence.
35. Evidence expansion re-checks live authority.
36. Follow-up questions cannot use conversation history to reveal restricted
    evidence after revocation.
37. Ask refuses or redirects when the user asks for state outside authority.
38. Ask answer distinguishes lack of authority from lack of data without leaking
    the restricted fact.
39. Prompt attacks such as "do not show numbers, just tell me which customer" do
    not bypass authority.
40. Prompt attacks asking for "summarize what the CFO is worried about" do not
    bypass authority.

### Product Surfaces

41. Today/cards do not include restricted state for unauthorized viewers.
42. Model trace does not reveal restricted Models or provenance to unauthorized
    viewers.
43. Forecast/history routes respect authority over underlying facts.
44. Recommendations do not reveal restricted proposed changes.
45. Decision deltas do not reveal restricted evidence.
46. Raw substrate list routes use the authorized reader.
47. Debug routes are unavailable in production and authority-filtered in
    dev/test where appropriate.
48. Export routes require purpose-specific authority.
49. Card conversations inherit the viewer's authority.
50. Product-rendered summaries preserve source authority.

### Realtime And Caching

51. Realtime delivers authorized events only.
52. Realtime drops restricted events for unauthorized subscribers.
53. Realtime revocation removes future delivery.
54. CEO cache cannot be served to an engineer.
55. Two actors with different grants get different cache fingerprints.
56. Grant epoch change invalidates relevant cache hits.
57. Cached Ask answers cannot be replayed across actors.
58. Cached cards cannot include stale restricted facts after revocation.
59. Authority fingerprint includes purpose, not just actor and tenant.
60. Cache telemetry records authority misses without leaking content.

### Extensions And Integrations

61. Extension with no capability cannot read restricted state.
62. Extension with granted capability can read only its granted kinds/channels.
63. First-party fallback does not grant access unless host-trusted.
64. Extension egress applies both capability and label rules.
65. Extension redaction cannot loosen tenant/user policy.

### Migration And Backfill

66. Existing resources receive labels from kind.
67. Existing observations receive labels from channel and subject.
68. Existing Models receive labels from provenance when available.
69. Existing Models with unknown provenance default conservative.
70. Backfill is idempotent.
71. Label indexes support dense-tenant reads without per-row explosions.
72. Schema drift checks include authority tables and policies.
73. Import boundaries keep access-control platform code out of product-specific
    cycles.

## Definition Of Done

This work is complete when:

1. every user-facing read path consumes an authorized reader or equivalent
   authority gate;
2. derived objects carry provenance and inherited authority labels;
3. Ask and query cannot answer from unauthorized evidence;
4. summaries, caches, realtime, debug, and exports cannot bypass read authority;
5. delegation and revocation are first-class and audited;
6. objective tests above pass at unit, integration, and selected end-to-end
   levels;
7. validation reports state the boundary of what was proven.
