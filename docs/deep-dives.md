# Subsystem deep-dives

These pages are **pre-existing internal deep-dives** that predate this site,
ported in unchanged. They go deeper than the curated [architecture](architecture/index.md)
pages — especially on **ingestion** (end-to-end pipeline + a page per source) and
the **GitHub intelligence** layer.

!!! note "How these differ from the rest of the site"
    - They were written as GitHub-relative docs, so their links to **source files**
      (e.g. `services/.../core.py`) open the file on GitHub
      (`github.com/Fyralisinc/fyraliscore`, `cannonical` branch) rather than a page
      in this site. That rewrite happens at build time (see `docs_hooks.py`); the
      markdown on disk is unchanged.
    - They reflect the state at the time each was written and may lag the code.
      When a deep-dive and a curated architecture page disagree, trust the code,
      then the [architecture](architecture/index.md) page.

## What's here

- **Ingestion** — the canonical ingestion architecture, source isolation, the
  finance sources + API map, and a page for each connected source (Slack, GitHub,
  Discord, Gmail, Notion, Google Calendar/Drive, Jira). See also the curated
  [Ingest layer](architecture/ingest.md) page.
- **GitHub Intelligence** — the formalized spec/plan, implementation status, the
  read API, and the browser UI for the GitHub enrichment layer.

> A few other point-in-time artifacts (`docs/testing/`, `docs/history/`,
> `docs/mockups/`, `docs/hardening-backlog.md`) remain in the repo but are **not**
> built into this site.
