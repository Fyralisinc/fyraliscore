# P0-A Authority Writer/Reader Inventory

The JSON companion is the machine-readable source. This document records the
interpretation and proof boundary.

## Result

All current production writer families have named owners, but canonical truth
does not have a single authority. Model state is directly mutated from domain,
reasoning, product, and maintenance code. Identity has a primary repository but
also a maintenance deletion path. Relation instances are the intended semantic
truth while accepted binary edges remain directly writable and widely read.

The inventory therefore satisfies P0 discovery, not HG-02, HG-08, or HG-10.
Those gates remain deliberately open for P2.

## Highest-risk authority splits

1. Model admission and lifecycle are split among `ModelsRepo`, Think's applier,
   contestability, product handlers, decay, and correction paths.
2. `model_edges` still has an accepted-write API despite the target rule that
   only canonical relation instances carry business relation truth.
3. Entity aliases have several connection-level mutation functions plus a
   maintenance deletion path; authority and provenance are not mandatory on
   every mutation.
4. Semantic fields and retrieval/control fields share the `models` row.
5. SAGE and projections are physically separate today, but their inability to
   mutate truth is a convention rather than one registered database authority.

## Proof boundary

The scan covers production Python SQL under `services/`. It excludes tests,
evaluation verticals, migrations, and benchmark scaffolding. SQL hidden behind
database functions, dynamically constructed table names, or external processes
is not proven absent. P2 must replace static completeness with registered
write-authority enforcement.

## Reproduction

Run the focused characterization test. It verifies that every production file
containing a direct canonical-table SQL mutation appears in the JSON inventory
and that derived policy/projection writers do not declare canonical tables.
