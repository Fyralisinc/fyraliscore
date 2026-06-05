# ADR-0002: Main Is The Single Integration Trunk

- **Status:** Accepted
- **Date:** 2026-06-03
- **Deciders:** Fyralis engineering
- **Related:** [CODEBASE-MANAGEMENT.md](https://github.com/Fyralisinc/fyraliscore/blob/main/CODEBASE-MANAGEMENT.md), [CONTRIBUTING.md](https://github.com/Fyralisinc/fyraliscore/blob/main/CONTRIBUTING.md)

## Context

The repository previously carried multiple long-lived lines that each behaved
like the source of truth: `main`, `cannonical`, `production`, `demo-deploy`, and
feature/integration branches. That made it unclear where new work should land,
which branch represented the current product, and which branch should be used
for cleanup and release work.

The `cannonical` line has now been merged into `main`, together with the newer
Sage work that had landed on `main`. The codebase needs one durable integration
branch so cleanup, CI, release, and branch-protection rules all point at the
same place.

## Decision

We will treat `main` as the only integration trunk.

Feature and cleanup work branches from `main` and returns to `main` through a
pull request with CI. `production` and `demo-deploy` are release/deployment
branches cut from `main`; they are not independent development trunks.

`cannonical` is retired as an integration line. It may remain temporarily as a
historical branch, but new work must not target it.

Direct pushes to `main` are emergency-only. Branch protection should require PRs
and checks without routine administrator bypass.

Rejected alternatives:

- Keep `cannonical` as the working trunk: rejected because the misspelled branch
  name and divergent `main` made ownership and release intent unclear.
- Keep multiple integration branches: rejected because it recreates the drift
  that forced the consolidation merge.

## Consequences

The branch model is simpler: new work has one base and one return path. CI and
release policy can now be written against `main` without special cases.

The cost is that existing long-lived branches must be merged, rebased, or
closed. Any branch with unique work now needs an explicit owner and disposition
instead of being treated as another quiet source of truth.
